"""Shared reactive query coordinator with two-tier keys and 3-tier execution.

This module owns Component 4 of the streaming and analytical diff architecture:
- Two-tier sharing keys (`LiveQueryKey` vs `ComparisonKey`) so users comparing
  different baselines reuse one shared live aggregation state under RLS isolation
- `IncrementalStateAdmissionPolicy` governed by `ResourceGovernor` leases
- Query-scoped dynamic pivot column discovery across both `old_rows` and `new_rows`
- 3-Tier execution routing:
  - Tier A: Exact sufficient-statistics incremental updates (`ExactMeasureEngine`)
  - Tier B: Revision-stamped targeted dirty-group SQL recomputation
  - Tier C: Full authoritative SQL query (`AuthoritativeQueryResult`)
- Blocker 1A contract: `AuthoritativeQueryResult` stamps `node.applied_revision = R_query`
  so concurrent commits already included in `R_query` are never applied twice
- In-flight `schema_version` evolution detection and `query_epoch` invalidation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import json
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from enum import StrEnum
except ImportError:  # Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        __str__ = str.__str__
        __format__ = str.__format__

from ..diff.baseline_comparison_service import BaselineComparisonResult, BaselineComparisonService
from ..diff.snapshot_provider import SnapshotDescriptor, SnapshotProvider
from ..resource_governor import (
    ResourceExhaustedError,
    ResourceGovernor,
    ResourceKind,
    ResourceLease,
    SYSTEM_ACCOUNT,
    WorkloadAccount,
    account_for_principal,
    get_worker_resource_governor,
)
from ..types.pivot_spec import Measure, PivotSpec
from .exact_measure_engine import (
    CanonicalPathCodec,
    DependencyScope,
    ExactMeasureEngine,
    GRAND_TOTAL_CANONICAL_PATH,
    GRAND_TOTAL_DISPLAY_PATH,
    GroupSufficientStats,
    MeasureAlgebraClass,
    SufficientStatsProjector,
    encode_wire_value,
    normalize_numeric,
    values_equal,
)
from .versioned_source_adapter import CommittedChangeBatch, VersionedSourceAdapter


class ExecutionStrategy(StrEnum):
    INCREMENTAL_INITIALIZED = "incremental_initialized"
    QUERY_ONLY_DIFF = "query_only_diff"


@dataclass(frozen=True)
class LiveQueryKey:
    """Tier-1 shared key: independent of baseline so live state is computed once."""

    source_id: str
    live_spec_fingerprint: str
    auth_scope_fingerprint: str
    schema_version: str
    policy_version: str = "v1"


@dataclass(frozen=True)
class ComparisonKey:
    """Tier-2 shared key: joins one SharedLiveQuery with one immutable baseline."""

    live_query_key: LiveQueryKey
    baseline_id: str
    comparison_spec_fingerprint: str


@dataclass(frozen=True)
class AuthoritativeQueryResult:
    """Blocker 1A: Revision-stamped authoritative replacement from Tier B or Tier C."""

    groups: Dict[str, GroupSufficientStats]
    col_keys: Tuple[str, ...]
    source_revision: int
    schema_version: str


@dataclass(frozen=True)
class LiveQueryDelta:
    """Structured result of applying one CommittedChangeBatch to a SharedLiveQuery."""

    source_revision: int
    schema_version: str
    strategy_used: str  # "tier_a_incremental" | "tier_b_targeted_sql" | "tier_c_authoritative"
    dirty_paths: Tuple[str, ...]
    added_paths: Tuple[str, ...]
    removed_paths: Tuple[str, ...]
    dirty_cells: Tuple[Tuple[str, str], ...]
    grand_total_changed: bool
    grand_total_dependents_dirty: bool
    columns_changed: bool
    schema_changed: bool
    query_epoch: int


def build_live_spec_fingerprint(spec: PivotSpec) -> str:
    payload = {
        "table": spec.table,
        "rows": list(spec.rows or []),
        "columns": list(spec.columns or []),
        "measures": [
            {
                "field": m.field,
                "agg": m.agg,
                "alias": m.alias,
                "weight": getattr(m, "weighted_field", None),
                "formula": getattr(m, "formula", None),
                "window": getattr(m, "window_func", None),
                "filter": getattr(m, "filter_condition", None),
            }
            for m in (spec.measures or [])
        ],
        "filters": spec.filters or [],
        "custom_dimensions": getattr(spec, "custom_dimensions", []) or [],
        "formula_ast_version": 1,
        "custom_dim_ast_version": 1,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]


def _row_matches_filter_clause(row: Mapping[str, Any], clause: Mapping[str, Any]) -> bool:
    if not isinstance(clause, Mapping):
        return True
    if ("op" in clause or "operator" in clause) and isinstance(clause.get("conditions"), list):
        op = str(clause.get("op") or clause.get("operator") or "AND").upper()
        subs = clause["conditions"]
        if not subs:
            return True
        if op == "OR":
            return any(_row_matches_filter_clause(row, sub) for sub in subs)
        return all(_row_matches_filter_clause(row, sub) for sub in subs)

    field = clause.get("field")
    if not field:
        return True
    op = str(clause.get("op") or clause.get("type") or "=").strip().lower()
    target = clause.get("value")
    val = row.get(field)

    if op in {"is null", "is_null", "isnull"}:
        return val is None or (isinstance(val, str) and not val.strip())
    if op in {"is not null", "is_not_null", "isnotnull"}:
        return val is not None and not (isinstance(val, str) and not val.strip())
    if val is None:
        return False

    case_sensitive = bool(clause.get("caseSensitive", False))
    try:
        if op in {"=", "==", "eq", "equals"}:
            if not case_sensitive and isinstance(val, str) and isinstance(target, str):
                return val.lower() == target.lower()
            return val == target
        if op in {"!=", "<>", "ne", "not_eq", "not_equals"}:
            if not case_sensitive and isinstance(val, str) and isinstance(target, str):
                return val.lower() != target.lower()
            return val != target
        if op in {">", "gt"}:
            return val > target
        if op in {">=", "gte"}:
            return val >= target
        if op in {"<", "lt"}:
            return val < target
        if op in {"<=", "lte"}:
            return val <= target
        if op == "in":
            if not isinstance(target, (list, tuple, set)):
                return val == target
            if not case_sensitive and isinstance(val, str):
                return val.lower() in {str(t).lower() for t in target if t is not None}
            return val in target
        if op in {"not in", "not_in", "notin"}:
            if not isinstance(target, (list, tuple, set)):
                return val != target
            if not case_sensitive and isinstance(val, str):
                return val.lower() not in {str(t).lower() for t in target if t is not None}
            return val not in target
        if op == "between" and isinstance(target, (list, tuple)) and len(target) == 2:
            return target[0] <= val <= target[1]
        if op in {"contains", "starts_with", "startswith", "ends_with", "endswith"}:
            s_val = str(val)
            s_tgt = str(target)
            if not case_sensitive:
                s_val = s_val.lower()
                s_tgt = s_tgt.lower()
            if op == "contains":
                return s_tgt in s_val
            if op in {"starts_with", "startswith"}:
                return s_val.startswith(s_tgt)
            return s_val.endswith(s_tgt)
    except TypeError:
        return False
    return True


def _row_matches_filters(row: Mapping[str, Any], filters: Sequence[Mapping[str, Any]]) -> bool:
    if not filters:
        return True
    return all(_row_matches_filter_clause(row, f) for f in filters)


def _rls_filters_from_scope(row_scope: Any) -> List[Dict[str, Any]]:
    if row_scope is None:
        return []
    applied = {}
    apply_fn = getattr(row_scope, "apply_to_filters", None)
    if callable(apply_fn):
        res = apply_fn({})
        if isinstance(res, Mapping):
            applied = dict(res)
    out: List[Dict[str, Any]] = []
    for field_name, spec in applied.items():
        if isinstance(spec, Mapping):
            if "field" in spec:
                out.append(dict(spec))
            elif "conditions" in spec:
                out.append(dict(spec))
            else:
                out.append({"field": field_name, **dict(spec)})
        elif isinstance(spec, (list, tuple, set)):
            out.append({"field": field_name, "op": "in", "value": list(spec)})
        else:
            out.append({"field": field_name, "op": "=", "value": spec})
    return out


class SharedLiveQuery:
    """Shared live analytical state for one LiveQueryKey across all subscribers."""

    def __init__(
        self,
        *,
        live_key: LiveQueryKey,
        spec: PivotSpec,
        controller: Any,
        source_adapter: VersionedSourceAdapter,
        comparison_service: BaselineComparisonService,
        resource_governor: ResourceGovernor,
        account: WorkloadAccount = SYSTEM_ACCOUNT,
        row_scope: Any = None,
        max_incremental_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        self.live_key = live_key
        self.spec = spec
        self._controller = controller
        self._source_adapter = source_adapter
        self._comparison_service = comparison_service
        self._governor = resource_governor
        self._account = account
        self._row_scope = row_scope
        self._max_incremental_bytes = max(0, int(max_incremental_bytes))

        self.engine = ExactMeasureEngine(list(spec.measures or []))
        self._lock = threading.RLock()
        self.ref_count: int = 0
        self.query_epoch: int = 1
        self.schema_version: str = live_key.schema_version
        self.last_applied_revision: int = 0
        self.strategy: ExecutionStrategy = (
            ExecutionStrategy.INCREMENTAL_INITIALIZED
            if self.engine.supports_incremental
            else ExecutionStrategy.QUERY_ONLY_DIFF
        )
        self._lease: Optional[ResourceLease] = None

        # Authoritative in-memory group state (when strategy == INCREMENTAL_INITIALIZED)
        self.groups: Dict[str, GroupSufficientStats] = {}
        self.col_keys: Tuple[str, ...] = ()
        # Issue 8: Query-scoped pivot column counts across filtered/authorized rows
        self.pivot_col_row_counts: Dict[str, int] = {}

        # Effective filters = user filters + server-owned RLS filters
        self._effective_filters: List[Dict[str, Any]] = list(spec.filters or []) + _rls_filters_from_scope(row_scope)
        self._has_custom_dims: bool = bool(getattr(spec, "custom_dimensions", None))

    def _effective_spec(self) -> PivotSpec:
        copied = self.spec.copy() if hasattr(self.spec, "copy") else self.spec
        copied.filters = list(self._effective_filters)
        return copied

    def _estimate_state_bytes(self, group_count: int, col_count: int) -> int:
        measure_count = max(1, len(self.spec.measures or []))
        return max(4096, group_count * max(1, col_count) * measure_count * 96)

    def _admit_incremental_lease_locked(self, group_count: int, col_count: int) -> bool:
        if not self.engine.supports_incremental:
            return False
        est_bytes = self._estimate_state_bytes(group_count, col_count)
        if est_bytes > self._max_incremental_bytes:
            self._release_lease_locked()
            self.strategy = ExecutionStrategy.QUERY_ONLY_DIFF
            return False
        try:
            if self._lease is None:
                self._lease = self._governor.reserve(
                    ResourceKind.CDC, est_bytes, account=self._account
                )
            elif est_bytes > self._lease.reserved_bytes:
                self._lease.grow(est_bytes - self._lease.reserved_bytes)
            self.strategy = ExecutionStrategy.INCREMENTAL_INITIALIZED
            return True
        except ResourceExhaustedError:
            self._release_lease_locked()
            self.strategy = ExecutionStrategy.QUERY_ONLY_DIFF
            return False

    def _release_lease_locked(self) -> None:
        if self._lease is not None:
            self._lease.release()
            self._lease = None

    def close(self) -> None:
        with self._lock:
            self.groups.clear()
            self.pivot_col_row_counts.clear()
            self._release_lease_locked()

    def _execute_authoritative_full_locked(self) -> AuthoritativeQueryResult:
        """Execute a coherent Tier C SQL query returning revision-stamped result."""
        table_name = self.spec.table
        with self._source_adapter._execution_locks.lock_for(table_name):
            rev, schema_ver = self._source_adapter.read_coherent_revision(table_name)
            planning_lock = getattr(self._controller, "planning_lock", None)
            if planning_lock is not None:
                with planning_lock:
                    tbl = self._controller.resolve_table(table_name)
            else:
                tbl = self._controller.resolve_table(table_name)
            groups, col_keys = self._comparison_service.aggregate_relation_hierarchy(
                tbl, self._effective_spec(), source_revision=rev
            )
            return AuthoritativeQueryResult(
                groups=groups,
                col_keys=col_keys,
                source_revision=rev,
                schema_version=schema_ver,
            )

    def _execute_authoritative_dirty_paths(
        self, dirty_paths: Sequence[Tuple[Any, ...]]
    ) -> AuthoritativeQueryResult:
        """Execute a coherent Tier B SQL query for specific dirty group paths."""
        table_name = self.spec.table
        row_dims = list(self.spec.rows or [])
        col_dims = list(self.spec.columns or [])
        measures = list(self.spec.measures or [])
        from contextlib import nullcontext

        from ..transport.core_bridge import core_records

        with self._source_adapter._execution_locks.lock_for(table_name):
            rev, schema_ver = self._source_adapter.read_coherent_revision(table_name)
            planning_lock = getattr(self._controller, "planning_lock", None)
            lock_ctx = planning_lock if planning_lock is not None else nullcontext()

            # Group dirty paths by depth so we run at most one batched SQL query per depth.
            by_depth: Dict[int, List[Tuple[Any, ...]]] = {}
            for pvals in dirty_paths:
                by_depth.setdefault(len(pvals), []).append(pvals)

            recomputed: Dict[str, GroupSufficientStats] = {}
            seen_cols: Set[str] = set()

            def _extract_ck(rdict: Mapping[str, Any]) -> str:
                if not col_dims:
                    return ""
                return "|".join("" if rdict.get(cd) is None else str(rdict.get(cd)) for cd in col_dims)

            with lock_ctx:
                base_tbl = self._controller.resolve_table(table_name)
                filtered = core_records(base_tbl, self.spec, self._effective_filters)

                for depth, paths_at_depth in by_depth.items():
                    level_dims = row_dims[:depth]
                    sub_tbl = filtered
                    if depth > 0:
                        or_filter = None
                        for pvals in paths_at_depth:
                            and_filter = None
                            for dim, val in zip(level_dims, pvals):
                                cond = sub_tbl[dim].isnull() if val is None else (sub_tbl[dim] == val)
                                and_filter = cond if and_filter is None else (and_filter & cond)
                            if and_filter is not None:
                                or_filter = and_filter if or_filter is None else (or_filter | and_filter)
                        if or_filter is not None:
                            sub_tbl = sub_tbl.filter(or_filter)

                    projections = SufficientStatsProjector.build_projections(
                        sub_tbl, measures
                    )
                    group_cols = level_dims + col_dims
                    expr = sub_tbl.group_by(group_cols).aggregate(projections) if group_cols else sub_tbl.aggregate(projections)
                    rows_out = expr.to_pyarrow().to_pylist()

                    # Pre-seed requested paths as empty (row_count=0) in case all members were deleted.
                    for pvals in paths_at_depth:
                        cpath = CanonicalPathCodec.encode(pvals)
                        recomputed[cpath] = GroupSufficientStats(
                            canonical_path=cpath,
                            path_values=pvals,
                            applied_revision=rev,
                            row_count=0,
                            initialized=True,
                        )

                    for rdict in rows_out:
                        pvals = tuple(rdict.get(d) for d in level_dims)
                        cpath = CanonicalPathCodec.encode(pvals)
                        ck = _extract_ck(rdict)
                        if ck:
                            seen_cols.add(ck)
                        existing = recomputed.get(cpath)
                        if existing is not None and existing.row_count == 0 and not existing.cells:
                            existing = None
                        recomputed[cpath] = self.engine.init_group_from_projected_row(
                            rdict,
                            path_values=pvals,
                            source_revision=rev,
                            col_key=ck,
                            existing=existing,
                        )

            return AuthoritativeQueryResult(
                groups=recomputed,
                col_keys=tuple(sorted(seen_cols)),
                source_revision=rev,
                schema_version=schema_ver,
            )

    def _rebuild_pivot_col_counts_locked(self) -> None:
        self.pivot_col_row_counts.clear()
        gt = self.groups.get(GRAND_TOTAL_CANONICAL_PATH)
        if gt is None:
            return
        for ck in self.col_keys:
            # Estimate row count per pivot column from any initialized cell's nn_count or >=1
            max_cnt = 0
            for (cell_ck, _alias), cell in gt.cells.items():
                if cell_ck == ck:
                    max_cnt = max(max_cnt, cell.nn_count)
            self.pivot_col_row_counts[ck] = max(1, max_cnt)

    def install_authoritative_result(
        self,
        result: AuthoritativeQueryResult,
        *,
        partial: bool = False,
    ) -> None:
        """Blocker 1A: Atomically install revision-stamped result without double-apply."""
        with self._lock:
            if not partial:
                if result.schema_version != self.schema_version:
                    self.schema_version = result.schema_version
                    self.query_epoch += 1
                self.groups = dict(result.groups)
                self.col_keys = tuple(result.col_keys)
                self.last_applied_revision = max(self.last_applied_revision, result.source_revision)
                self._rebuild_pivot_col_counts_locked()
                self._admit_incremental_lease_locked(len(self.groups), len(self.col_keys))
                return

            # Partial Tier B replacement: reject stale schema mismatch -> force full resync.
            if result.schema_version != self.schema_version:
                full = self._execute_authoritative_full_locked()
                self.install_authoritative_result(full, partial=False)
                return

            for cpath, replacement in result.groups.items():
                existing = self.groups.get(cpath)
                if existing is not None and existing.applied_revision > result.source_revision:
                    continue
                if replacement.row_count <= 0 and cpath != GRAND_TOTAL_CANONICAL_PATH:
                    self.groups.pop(cpath, None)
                else:
                    self.groups[cpath] = replacement
            if result.col_keys:
                merged_cols = sorted(set(self.col_keys) | set(result.col_keys))
                self.col_keys = tuple(merged_cols)
            self.last_applied_revision = max(self.last_applied_revision, result.source_revision)

    def initialize(self) -> AuthoritativeQueryResult:
        """Bootstrap the shared live query at a coherent authoritative revision."""
        with self._lock:
            result = self._execute_authoritative_full_locked()
            self.install_authoritative_result(result, partial=False)
            return result

    def _extract_col_key_from_row(self, row: Mapping[str, Any]) -> str:
        col_dims = list(self.spec.columns or [])
        if not col_dims:
            return ""
        return "|".join("" if row.get(cd) is None else str(row.get(cd)) for cd in col_dims)

    def _extract_path_tuple(self, row: Mapping[str, Any]) -> Tuple[Any, ...]:
        return tuple(row.get(d) for d in (self.spec.rows or []))

    def apply_batch(
        self,
        batch: CommittedChangeBatch,
        *,
        on_before_tier_b_sql: Optional[Callable[[], None]] = None,
    ) -> LiveQueryDelta:
        """Apply one committed batch via Tier A, Tier B (revision-stamped), or Tier C."""
        with self._lock:
            # Schema evolution check: invalidate state, bump queryEpoch, re-init!
            if batch.schema_changed or batch.schema_version != self.schema_version:
                self.schema_version = batch.schema_version
                self.query_epoch += 1
                self.groups.clear()
                full = self._execute_authoritative_full_locked()
                self.install_authoritative_result(full, partial=False)
                return LiveQueryDelta(
                    source_revision=full.source_revision,
                    schema_version=full.schema_version,
                    strategy_used="tier_c_authoritative",
                    dirty_paths=tuple(self.groups.keys()),
                    added_paths=(),
                    removed_paths=(),
                    dirty_cells=(),
                    grand_total_changed=True,
                    grand_total_dependents_dirty=self.engine.graph.has_grand_total_dependents,
                    columns_changed=True,
                    schema_changed=True,
                    query_epoch=self.query_epoch,
                )

            if batch.source_revision <= self.last_applied_revision:
                return LiveQueryDelta(
                    source_revision=self.last_applied_revision,
                    schema_version=self.schema_version,
                    strategy_used="tier_a_incremental",
                    dirty_paths=(),
                    added_paths=(),
                    removed_paths=(),
                    dirty_cells=(),
                    grand_total_changed=False,
                    grand_total_dependents_dirty=False,
                    columns_changed=False,
                    schema_changed=False,
                    query_epoch=self.query_epoch,
                )

            # Non-incremental fallback (QUERY_ONLY_DIFF, missing row deltas, custom dimensions, or replace)
            if (
                self.strategy != ExecutionStrategy.INCREMENTAL_INITIALIZED
                or not batch.row_deltas_complete
                or self._has_custom_dims
                or batch.mode == "replace"
                or not self.groups
            ):
                prev_paths = set(self.groups.keys())
                prev_cols = self.col_keys
                full = self._execute_authoritative_full_locked()
                self.install_authoritative_result(full, partial=False)
                next_paths = set(self.groups.keys())
                return LiveQueryDelta(
                    source_revision=full.source_revision,
                    schema_version=full.schema_version,
                    strategy_used="tier_c_authoritative",
                    dirty_paths=tuple(next_paths | prev_paths),
                    added_paths=tuple(next_paths - prev_paths),
                    removed_paths=tuple(prev_paths - next_paths),
                    dirty_cells=(),
                    grand_total_changed=True,
                    grand_total_dependents_dirty=self.engine.graph.has_grand_total_dependents,
                    columns_changed=(prev_cols != self.col_keys),
                    schema_changed=False,
                    query_epoch=self.query_epoch,
                )

            # Tier A: Exact Sufficient-Statistics Incremental Update
            row_dims = list(self.spec.rows or [])
            col_dims = list(self.spec.columns or [])
            dirty_paths: Set[str] = set()
            added_paths: Set[str] = set()
            removed_paths: Set[str] = set()
            dirty_cells: Set[Tuple[str, str]] = set()
            tier_b_needed_paths: Dict[str, Tuple[Any, ...]] = {}
            columns_changed = False

            # Track old/new filtered transitions
            transitions: List[Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = []
            if batch.changes:
                for ch in batch.changes:
                    old_r = ch.old_row if (ch.old_row is not None and _row_matches_filters(ch.old_row, self._effective_filters)) else None
                    new_r = ch.new_row if (ch.new_row is not None and _row_matches_filters(ch.new_row, self._effective_filters)) else None
                    if old_r is None and new_r is None:
                        continue
                    transitions.append((old_r, new_r))
            else:
                for old_r in batch.old_rows:
                    if _row_matches_filters(old_r, self._effective_filters):
                        transitions.append((old_r, None))
                for new_r in batch.new_rows:
                    if _row_matches_filters(new_r, self._effective_filters):
                        transitions.append((None, new_r))

            def _apply_one_side(
                row: Mapping[str, Any],
                *,
                is_add: bool,
                target_rev: int,
            ) -> None:
                nonlocal columns_changed
                ck = self._extract_col_key_from_row(row)
                if col_dims:
                    prev_cnt = self.pivot_col_row_counts.get(ck, 0)
                    next_cnt = prev_cnt + 1 if is_add else max(0, prev_cnt - 1)
                    if is_add and prev_cnt == 0 and ck not in self.col_keys:
                        columns_changed = True
                        self.col_keys = tuple(sorted(set(self.col_keys) | {ck}))
                    if next_cnt == 0:
                        self.pivot_col_row_counts.pop(ck, None)
                        if not is_add and prev_cnt > 0:
                            columns_changed = True
                    else:
                        self.pivot_col_row_counts[ck] = next_cnt

                full_vals = self._extract_path_tuple(row)
                for depth in range(0, len(row_dims) + 1):
                    pvals = full_vals[:depth]
                    cpath = CanonicalPathCodec.encode(pvals)
                    grp = self.groups.get(cpath)
                    if grp is None:
                        if not is_add:
                            continue
                        grp = GroupSufficientStats(
                            canonical_path=cpath,
                            path_values=pvals,
                            applied_revision=target_rev - 1,
                            row_count=0,
                            initialized=True,
                        )
                        self.groups[cpath] = grp
                        if cpath != GRAND_TOTAL_CANONICAL_PATH:
                            added_paths.add(cpath)
                            removed_paths.discard(cpath)

                    # Blocker 1A: If a prior Tier B SQL result already reflects `target_rev`, skip!
                    if target_rev <= grp.applied_revision:
                        continue

                    needs_b, changed_cells = self.engine.apply_row_transition(
                        grp,
                        col_key=ck,
                        old_row=None if is_add else row,
                        new_row=row if is_add else None,
                        source_revision=target_rev,
                    )
                    dirty_paths.add(cpath)
                    for cell_ck, alias in changed_cells:
                        col_id = f"{cell_ck}_{alias}" if cell_ck else alias
                        dirty_cells.add((cpath, col_id))
                    if needs_b:
                        tier_b_needed_paths[cpath] = pvals

            # Pair same-(path, col_key) old/new updates so monotonic min/max/sum see atomic transitions;
            # split cross-group or cross-pivot-column moves into exact (-old, +new).
            for old_r, new_r in transitions:
                same_coords = (
                    old_r is not None
                    and new_r is not None
                    and self._extract_path_tuple(old_r) == self._extract_path_tuple(new_r)
                    and self._extract_col_key_from_row(old_r) == self._extract_col_key_from_row(new_r)
                )
                if same_coords:
                    assert old_r is not None and new_r is not None
                    ck = self._extract_col_key_from_row(new_r)
                    full_vals = self._extract_path_tuple(new_r)
                    for depth in range(0, len(row_dims) + 1):
                        pvals = full_vals[:depth]
                        cpath = CanonicalPathCodec.encode(pvals)
                        grp = self.groups.get(cpath)
                        if grp is None:
                            tier_b_needed_paths[cpath] = pvals
                            continue
                        if batch.source_revision <= grp.applied_revision:
                            continue
                        needs_b, changed_cells = self.engine.apply_row_transition(
                            grp,
                            col_key=ck,
                            old_row=old_r,
                            new_row=new_r,
                            source_revision=batch.source_revision,
                        )
                        dirty_paths.add(cpath)
                        for cell_ck, alias in changed_cells:
                            col_id = f"{cell_ck}_{alias}" if cell_ck else alias
                            dirty_cells.add((cpath, col_id))
                        if needs_b:
                            tier_b_needed_paths[cpath] = pvals
                else:
                    if old_r is not None:
                        _apply_one_side(old_r, is_add=False, target_rev=batch.source_revision)
                    if new_r is not None:
                        _apply_one_side(new_r, is_add=True, target_rev=batch.source_revision)

            # Stamp applied_revision on Tier-A updated groups and prune emptied non-root groups.
            for cpath in list(dirty_paths):
                if cpath in tier_b_needed_paths:
                    continue
                grp = self.groups.get(cpath)
                if grp is None:
                    continue
                if grp.applied_revision < batch.source_revision:
                    grp.applied_revision = batch.source_revision
                if grp.row_count <= 0 and cpath != GRAND_TOTAL_CANONICAL_PATH:
                    self.groups.pop(cpath, None)
                    removed_paths.add(cpath)
                    added_paths.discard(cpath)

            # Re-verify pivot column catalog if a column may have emptied.
            if columns_changed and col_dims:
                self.col_keys = tuple(sorted(self.pivot_col_row_counts.keys()))

            strategy_used = "tier_a_incremental"
            final_rev = batch.source_revision

            # Tier B: Targeted dirty-group SQL recomputation (with Blocker 1A revision stamp!).
            if tier_b_needed_paths:
                strategy_used = "tier_b_targeted_sql"
                if on_before_tier_b_sql is not None:
                    # Release internal lock around hook so concurrency tests can commit rev+1!
                    self._lock.release()
                    try:
                        on_before_tier_b_sql()
                    finally:
                        self._lock.acquire()
                auth_result = self._execute_authoritative_dirty_paths(
                    list(tier_b_needed_paths.values())
                )
                self.install_authoritative_result(auth_result, partial=True)
                final_rev = max(final_rev, auth_result.source_revision)
                for cpath in tier_b_needed_paths:
                    dirty_paths.add(cpath)
                    if cpath not in self.groups and cpath != GRAND_TOTAL_CANONICAL_PATH:
                        removed_paths.add(cpath)

            # Check admission budget after group growth
            self._admit_incremental_lease_locked(len(self.groups), len(self.col_keys))

            self.last_applied_revision = max(self.last_applied_revision, final_rev)
            gt_changed = GRAND_TOTAL_CANONICAL_PATH in dirty_paths
            gt_dependents_dirty = gt_changed and self.engine.graph.has_grand_total_dependents

            return LiveQueryDelta(
                source_revision=self.last_applied_revision,
                schema_version=self.schema_version,
                strategy_used=strategy_used,
                dirty_paths=tuple(sorted(dirty_paths)),
                added_paths=tuple(sorted(added_paths)),
                removed_paths=tuple(sorted(removed_paths)),
                dirty_cells=tuple(sorted(dirty_cells)),
                grand_total_changed=gt_changed,
                grand_total_dependents_dirty=gt_dependents_dirty,
                columns_changed=columns_changed,
                schema_changed=False,
                query_epoch=self.query_epoch,
            )

    def materialize_hierarchy_rows(
        self,
        *,
        sort_specs: Optional[Sequence[Mapping[str, Any]]] = None,
        expanded_paths: Optional[Iterable[str]] = None,
        frozen_row_order: Optional[Sequence[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Materialize ordered hierarchy row dicts + Grand Total from current live state."""
        with self._lock:
            row_dims = list(self.spec.rows or [])
            effective_col_keys = self.col_keys if self.col_keys else ("",)
            gt_group = self.groups.get(GRAND_TOTAL_CANONICAL_PATH)

            grand_total_dict: Optional[Dict[str, Any]] = None
            if gt_group is not None:
                gt_scalars = self.engine.materialize_row_scalars(
                    gt_group, col_keys=effective_col_keys, grand_total_group=gt_group
                )
                grand_total_dict = {
                    "_canonicalPath": GRAND_TOTAL_CANONICAL_PATH,
                    "_path": GRAND_TOTAL_DISPLAY_PATH,
                    "_id": "Grand Total",
                    "_isTotal": True,
                    "depth": 0,
                    **gt_scalars,
                }

            rows_by_path: Dict[str, Dict[str, Any]] = {}
            children_by_parent: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
            for cpath, grp in self.groups.items():
                if cpath == GRAND_TOTAL_CANONICAL_PATH or grp.row_count <= 0:
                    continue
                scalars = self.engine.materialize_row_scalars(
                    grp, col_keys=effective_col_keys, grand_total_group=gt_group
                )
                row_dict: Dict[str, Any] = {
                    "_canonicalPath": cpath,
                    "_path": grp.display_path(),
                    "_id": "" if not grp.path_values else ("" if grp.path_values[-1] is None else str(grp.path_values[-1])),
                    "_isTotal": False,
                    "depth": max(0, len(grp.path_values) - 1),
                    "_has_children": len(grp.path_values) < len(row_dims),
                    **scalars,
                }
                for idx, dim in enumerate(row_dims):
                    if idx < len(grp.path_values):
                        row_dict[dim] = grp.path_values[idx]
                rows_by_path[cpath] = row_dict
                parent_path = (
                    GRAND_TOTAL_CANONICAL_PATH
                    if len(grp.path_values) <= 1
                    else CanonicalPathCodec.encode(grp.path_values[:-1])
                )
                children_by_parent.setdefault(parent_path, []).append((cpath, row_dict))

            # Order siblings: either frozen_row_order (in_place Sort Lock) or live_sort
            frozen_rank = (
                {cpath: idx for idx, cpath in enumerate(frozen_row_order)}
                if frozen_row_order is not None
                else None
            )

            def _sort_children(
                items: List[Tuple[str, Dict[str, Any]]]
            ) -> List[Tuple[str, Dict[str, Any]]]:
                if frozen_rank is not None:
                    # Surviving rows keep frozen order; newly added rows append stably.
                    return sorted(
                        items,
                        key=lambda pair: (
                            0 if pair[0] in frozen_rank else 1,
                            frozen_rank.get(pair[0], 0),
                            pair[1]["_path"],
                        ),
                    )
                if sort_specs:
                    first = sort_specs[0]
                    s_field = str(first.get("field") or first.get("id") or "")
                    desc = str(first.get("order") or ("desc" if first.get("desc") else "asc")).lower() == "desc"

                    def _cmp_key(pair: Tuple[str, Dict[str, Any]]):
                        val = normalize_numeric(pair[1].get(s_field))
                        if val is None:
                            return (1, 0.0, "", pair[1]["_path"])
                        if isinstance(val, (int, float, Decimal)) and not isinstance(val, bool):
                            fval = float(val)
                            return (0, -fval if desc else fval, "", pair[1]["_path"])
                        sval = str(val).lower()
                        # For string desc we invert ordinals cleanly via helper
                        return (0, 0.0, sval, pair[1]["_path"])

                    ordered = sorted(items, key=_cmp_key)
                    # Handle string descending cleanly
                    if desc and ordered:
                        sample = normalize_numeric(ordered[0][1].get(s_field))
                        if isinstance(sample, str):
                            ordered = sorted(items, key=lambda p: (p[1].get(s_field) is None, str(p[1].get(s_field) or "").lower()), reverse=True)
                    return ordered
                return sorted(items, key=lambda pair: pair[1]["_path"])

            expanded_set = set(expanded_paths) if expanded_paths is not None else None
            if expanded_set is not None and "__ALL__" in expanded_set:
                expanded_set = None
            ordered_rows: List[Dict[str, Any]] = []

            def _walk(parent_path: str) -> None:
                for cpath, row_dict in _sort_children(children_by_parent.get(parent_path, [])):
                    ordered_rows.append(row_dict)
                    if expanded_set is None or cpath in expanded_set or row_dict["_path"] in expanded_set:
                        _walk(cpath)

            _walk(GRAND_TOTAL_CANONICAL_PATH)
            return ordered_rows, grand_total_dict


class ReactiveQueryCoordinator:
    """Component 4: Registry of shared live queries & baseline comparison views."""

    def __init__(
        self,
        controller: Any,
        source_adapter: VersionedSourceAdapter,
        snapshot_provider: SnapshotProvider,
        comparison_service: Optional[BaselineComparisonService] = None,
        *,
        resource_governor: Optional[ResourceGovernor] = None,
        max_incremental_bytes_per_query: int = 16 * 1024 * 1024,
    ) -> None:
        self._controller = controller
        self._source_adapter = source_adapter
        self._snapshot_provider = snapshot_provider
        self._comparison_service = comparison_service or BaselineComparisonService(
            controller, snapshot_provider
        )
        self._governor = (
            resource_governor
            or getattr(controller, "resource_governor", None)
            or get_worker_resource_governor()
        )
        self._max_incremental_bytes_per_query = max_incremental_bytes_per_query
        self._lock = threading.RLock()
        self._live_queries: Dict[LiveQueryKey, SharedLiveQuery] = {}
        self._comparison_refs: Dict[ComparisonKey, int] = {}

    def make_live_key(
        self,
        spec: PivotSpec,
        *,
        row_scope: Any = None,
        schema_version: Optional[str] = None,
    ) -> LiveQueryKey:
        resolved_schema = schema_version
        if resolved_schema is None:
            _, resolved_schema = self._source_adapter.read_coherent_revision(spec.table)
        scope_fp = str(getattr(row_scope, "scope_fingerprint", None) or "global")
        policy_ver = str(getattr(row_scope, "policy_version", None) or "v1")
        return LiveQueryKey(
            source_id=str(spec.table),
            live_spec_fingerprint=build_live_spec_fingerprint(spec),
            auth_scope_fingerprint=scope_fp,
            schema_version=resolved_schema,
            policy_version=policy_ver,
        )

    def acquire_live_query(
        self,
        spec: PivotSpec,
        *,
        principal: Any = None,
        row_scope: Any = None,
    ) -> SharedLiveQuery:
        """Return or initialize one shared live query for `(spec, auth_scope)`."""
        live_key = self.make_live_key(spec, row_scope=row_scope)
        with self._lock:
            existing = self._live_queries.get(live_key)
            if existing is not None:
                existing.ref_count += 1
                return existing
            query = SharedLiveQuery(
                live_key=live_key,
                spec=spec,
                controller=self._controller,
                source_adapter=self._source_adapter,
                comparison_service=self._comparison_service,
                resource_governor=self._governor,
                account=account_for_principal(principal),
                row_scope=row_scope,
                max_incremental_bytes=self._max_incremental_bytes_per_query,
            )
            query.ref_count = 1
            self._live_queries[live_key] = query
        query.initialize()
        return query

    def release_live_query(self, live_key: LiveQueryKey) -> None:
        with self._lock:
            query = self._live_queries.get(live_key)
            if query is None:
                return
            query.ref_count = max(0, query.ref_count - 1)
            if query.ref_count == 0:
                self._live_queries.pop(live_key, None)
                query.close()

    def active_live_query_count(self) -> int:
        with self._lock:
            return len(self._live_queries)

    def queries_for_table(self, table: str) -> List[SharedLiveQuery]:
        with self._lock:
            return [q for k, q in self._live_queries.items() if k.source_id == table]

    def apply_committed_batch(self, batch: CommittedChangeBatch) -> Dict[LiveQueryKey, LiveQueryDelta]:
        deltas: Dict[LiveQueryKey, LiveQueryDelta] = {}
        for query in self.queries_for_table(batch.table):
            deltas[query.live_key] = query.apply_batch(batch)
        return deltas

    def compute_comparison_for_live_query(
        self,
        live_query: SharedLiveQuery,
        *,
        baseline_id: str,
        principal: Any = None,
        row_scope: Any = None,
        changed_only: bool = False,
        sort_by_variance: Optional[Tuple[str, str, bool]] = None,
        sort_specs: Optional[Sequence[Mapping[str, Any]]] = None,
        frozen_row_order: Optional[Sequence[str]] = None,
        expanded_paths: Optional[Iterable[str]] = None,
    ) -> BaselineComparisonResult:
        """Join shared live state against cached immutable T0 baseline without duplicating live work."""
        base_groups, base_col_keys, descriptor = self._comparison_service.get_or_compute_baseline_state(
            baseline_id,
            live_query._effective_spec(),
            principal=principal,
            row_scope=row_scope,
        )
        with live_query._lock:
            cur_groups = {k: v.clone() for k, v in live_query.groups.items()}
            cur_cols = live_query.col_keys
            cur_rev = live_query.last_applied_revision
        return self._comparison_service.compare_states(
            spec=live_query.spec,
            current_groups=cur_groups,
            current_col_keys=cur_cols,
            current_revision=cur_rev,
            baseline_groups=base_groups,
            baseline_col_keys=base_col_keys,
            snapshot_descriptor=descriptor,
            changed_only=changed_only,
            sort_by_variance=sort_by_variance,
            sort_specs=sort_specs,
            frozen_row_order=frozen_row_order,
            expanded_paths=expanded_paths,
        )

    def close(self) -> None:
        with self._lock:
            for query in list(self._live_queries.values()):
                query.close()
            self._live_queries.clear()
            self._comparison_refs.clear()
