"""Full analytical baseline comparison engine (T1 Current vs. T0 Baseline).

This module owns Component 3B of the streaming and analytical diff architecture:
- Caches immutable T0 aggregates per `(snapshot_id, live_spec_fingerprint,
  auth_scope_fingerprint, schema_version)` so live ticks never rescan T0
- Evaluates AST formulas and window ratios independently on T1 and T0 before
  computing delta and %delta (protecting non-additive ratios like Margin %)
- Aligns multi-level hierarchy nodes via `CanonicalPathCodec`, synthesizing
  `_diffStatus: "added" | "removed" | "modified" | "unchanged"`
- Applies exact extensive vs. intensive removed-group semantics
- Computes Top-3 child variance attribution (`_varianceDrivers`) for PnL Explain
- Supports full-dataset variance sorting (`delta` / `pct_delta`) and `changedOnly`
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import json
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from ..streaming.exact_measure_engine import (
    CanonicalPathCodec,
    ExactMeasureEngine,
    GRAND_TOTAL_CANONICAL_PATH,
    GRAND_TOTAL_DISPLAY_PATH,
    GroupSufficientStats,
    MeasureDiffResult,
    SufficientStatsProjector,
    compute_measure_diff,
    encode_wire_value,
    normalize_numeric,
)
from ..types.pivot_spec import Measure, PivotSpec
from .snapshot_provider import SnapshotDescriptor, SnapshotProvider


@dataclass(frozen=True)
class BaselineComparisonRow:
    """One aligned hierarchy row combining T1 values, T0 values, deltas, and drivers."""

    canonical_path: str
    display_path: str
    path_values: Tuple[Any, ...]
    depth: int
    diff_status: str  # "unchanged" | "modified" | "added" | "removed"
    current_values: Dict[str, Any]
    baseline_values: Dict[str, Any]
    diffs: Dict[str, MeasureDiffResult]
    variance_drivers: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    is_grand_total: bool = False

    def to_row_dict(self, row_fields: Sequence[str]) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "_canonicalPath": self.canonical_path,
            "_path": GRAND_TOTAL_DISPLAY_PATH if self.is_grand_total else self.display_path,
            "depth": self.depth,
            "_isTotal": self.is_grand_total,
            "_diffStatus": self.diff_status,
            "_baseline": {k: encode_wire_value(v) for k, v in self.baseline_values.items()},
            "_delta": {k: encode_wire_value(d.delta) for k, d in self.diffs.items()},
            "_pctDelta": {k: d.pct_delta for k, d in self.diffs.items()},
            "_deltaUnit": {k: d.delta_unit for k, d in self.diffs.items()},
        }
        if self.is_grand_total:
            row["_id"] = "Grand Total"
        else:
            row["_id"] = (
                "" if not self.path_values else ("" if self.path_values[-1] is None else str(self.path_values[-1]))
            )
            for idx, field_name in enumerate(row_fields):
                if idx < len(self.path_values):
                    row[field_name] = self.path_values[idx]
        for col_id, diff_res in self.diffs.items():
            row[col_id] = diff_res.current
        if self.variance_drivers:
            row["_varianceDrivers"] = self.variance_drivers
        return row


@dataclass(frozen=True)
class BaselineComparisonResult:
    """Complete aligned hierarchy diff at `(current_revision, baseline_descriptor)`."""

    snapshot_descriptor: SnapshotDescriptor
    current_revision: int
    rows: List[BaselineComparisonRow]
    grand_total: Optional[BaselineComparisonRow]
    total_rows: int
    changed_rows: int
    added_rows: int
    removed_rows: int
    col_keys: Tuple[str, ...]


def _measure_lookup_for_col(
    col_id: str, measures: Sequence[Measure], col_keys: Sequence[str]
) -> Optional[Measure]:
    for m in measures:
        if col_id == m.alias:
            return m
    for ck in col_keys:
        if not ck:
            continue
        prefix = f"{ck}_"
        if col_id.startswith(prefix):
            suffix = col_id[len(prefix):]
            for m in measures:
                if suffix == m.alias:
                    return m
    return None


def _numeric_sort_key(value: Any, *, descending: bool = False) -> Tuple[int, float]:
    norm = normalize_numeric(value)
    if norm is None or isinstance(norm, bool):
        return (1, 0.0)
    try:
        num = float(norm)
    except (TypeError, ValueError):
        return (1, 0.0)
    return (0, -num if descending else num)


class BaselineComparisonService:
    """Component 3B: Evaluates cached T0 aggregates and aligns T1 vs T0 hierarchies."""

    def __init__(
        self,
        controller: Any,
        snapshot_provider: SnapshotProvider,
        *,
        max_cached_baselines: int = 32,
    ) -> None:
        self._controller = controller
        self._snapshot_provider = snapshot_provider
        self._lock = threading.RLock()
        self._max_cached_baselines = max(4, int(max_cached_baselines))
        # Cache immutable T0 aggregates keyed by (snapshot_id, spec_fp, scope_fp, schema_ver)
        self._baseline_agg_cache: "OrderedDict[str, Tuple[Dict[str, GroupSufficientStats], Tuple[str, ...]]]" = OrderedDict()

    @staticmethod
    def _baseline_cache_key(
        snapshot_id: str,
        spec: PivotSpec,
        auth_scope_fingerprint: str,
        schema_version: str,
    ) -> str:
        payload = {
            "snapshot_id": snapshot_id,
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
            "auth_scope": auth_scope_fingerprint,
            "schema_version": schema_version,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:32]

    def aggregate_relation_hierarchy(
        self,
        ibis_table: Any,
        spec: PivotSpec,
        *,
        source_revision: int,
    ) -> Tuple[Dict[str, GroupSufficientStats], Tuple[str, ...]]:
        """Aggregate an Ibis relation into multi-level GroupSufficientStats via single-pass SQL."""
        from contextlib import nullcontext

        planning_lock = getattr(self._controller, "planning_lock", None)
        lock_ctx = planning_lock if planning_lock is not None else nullcontext()

        measures = list(spec.measures or [])
        engine = ExactMeasureEngine(measures)
        row_dims = list(spec.rows or [])
        col_dims = list(spec.columns or [])

        groups: Dict[str, GroupSufficientStats] = {}
        discovered_col_keys: List[str] = []
        seen_col_keys = set()

        def _extract_col_key(row_dict: Mapping[str, Any]) -> str:
            if not col_dims:
                return ""
            parts = ["" if row_dict.get(cd) is None else str(row_dict.get(cd)) for cd in col_dims]
            return "|".join(parts)

        with lock_ctx:
            from ..transport.core_bridge import core_records

            filtered = core_records(ibis_table, spec, list(spec.filters or []))
            projections = SufficientStatsProjector.build_projections(filtered, measures)

            # Level 0 (Grand Total) + Level 1..D (each hierarchy prefix)
            for depth in range(0, len(row_dims) + 1):
                level_row_dims = row_dims[:depth]
                group_cols = level_row_dims + col_dims
                if group_cols:
                    expr = filtered.group_by(group_cols).aggregate(projections)
                else:
                    expr = filtered.aggregate(projections)
                arrow_res = expr.to_pyarrow()
                for rdict in arrow_res.to_pylist():
                    path_vals = tuple(rdict.get(d) for d in level_row_dims)
                    cpath = CanonicalPathCodec.encode(path_vals)
                    ck = _extract_col_key(rdict)
                    if ck and ck not in seen_col_keys:
                        seen_col_keys.add(ck)
                        discovered_col_keys.append(ck)
                    existing = groups.get(cpath)
                    groups[cpath] = engine.init_group_from_projected_row(
                        rdict,
                        path_values=path_vals,
                        source_revision=source_revision,
                        col_key=ck,
                        existing=existing,
                    )

        discovered_col_keys.sort()
        return groups, tuple(discovered_col_keys)

    def get_or_compute_baseline_state(
        self,
        snapshot_id: str,
        spec: PivotSpec,
        *,
        principal: Any = None,
        row_scope: Any = None,
        connection: Optional[Any] = None,
    ) -> Tuple[Dict[str, GroupSufficientStats], Tuple[str, ...], SnapshotDescriptor]:
        """Compute and cache immutable T0 sufficient stats so live ticks never rescan T0."""
        snap_table, descriptor = self._snapshot_provider.resolve_snapshot_table(
            snapshot_id,
            principal=principal,
            row_scope=row_scope,
            connection=connection,
        )
        scope_fp = getattr(row_scope, "scope_fingerprint", None) or "global"
        cache_key = self._baseline_cache_key(
            snapshot_id, spec, str(scope_fp), descriptor.schema_version
        )
        with self._lock:
            cached = self._baseline_agg_cache.get(cache_key)
            if cached is not None:
                self._baseline_agg_cache.move_to_end(cache_key)
                return cached[0], cached[1], descriptor

        groups, col_keys = self.aggregate_relation_hierarchy(
            snap_table,
            spec,
            source_revision=descriptor.source_revision,
        )
        with self._lock:
            self._baseline_agg_cache[cache_key] = (groups, col_keys)
            while len(self._baseline_agg_cache) > self._max_cached_baselines:
                self._baseline_agg_cache.popitem(last=False)
        return groups, col_keys, descriptor

    def compare_states(
        self,
        *,
        spec: PivotSpec,
        current_groups: Mapping[str, GroupSufficientStats],
        current_col_keys: Sequence[str],
        current_revision: int,
        baseline_groups: Mapping[str, GroupSufficientStats],
        baseline_col_keys: Sequence[str],
        snapshot_descriptor: SnapshotDescriptor,
        changed_only: bool = False,
        sort_by_variance: Optional[Tuple[str, str, bool]] = None,  # (col_id, "delta"|"pct_delta"|"abs_delta", desc)
        sort_specs: Optional[Sequence[Mapping[str, Any]]] = None,
        frozen_row_order: Optional[Sequence[str]] = None,
        expanded_paths: Optional[Iterable[str]] = None,
        include_removed: bool = True,
    ) -> BaselineComparisonResult:
        """Align T1 and T0 hierarchies, evaluate formulas independently, and compute drivers."""
        measures = list(spec.measures or [])
        engine = ExactMeasureEngine(measures)
        union_col_keys = tuple(sorted(set(current_col_keys) | set(baseline_col_keys)))
        effective_col_keys = union_col_keys if union_col_keys else ("",)

        cur_gt = current_groups.get(GRAND_TOTAL_CANONICAL_PATH)
        base_gt = baseline_groups.get(GRAND_TOTAL_CANONICAL_PATH)

        all_paths = set(current_groups.keys()) | (set(baseline_groups.keys()) if include_removed else set())
        all_paths.discard(GRAND_TOTAL_CANONICAL_PATH)

        def _build_comparison_row(cpath: str, is_gt: bool = False) -> BaselineComparisonRow:
            cg = current_groups.get(cpath)
            bg = baseline_groups.get(cpath)
            if cg is not None and cg.row_count <= 0:
                cg = None
            if bg is not None and bg.row_count <= 0:
                bg = None

            path_vals = (
                ()
                if is_gt
                else (cg.path_values if cg is not None else (bg.path_values if bg is not None else CanonicalPathCodec.decode(cpath)))
            )
            depth = 0 if is_gt else max(0, len(path_vals) - 1)

            cur_scalars = (
                engine.materialize_row_scalars(cg, col_keys=effective_col_keys, grand_total_group=cur_gt)
                if cg is not None
                else {}
            )
            base_scalars = (
                engine.materialize_row_scalars(bg, col_keys=effective_col_keys, grand_total_group=base_gt)
                if bg is not None
                else {}
            )

            if cg is not None and bg is None:
                status = "added"
            elif cg is None and bg is not None:
                status = "removed"
            else:
                status = "unchanged"

            all_col_ids = list(dict.fromkeys(list(cur_scalars.keys()) + list(base_scalars.keys())))
            diffs: Dict[str, MeasureDiffResult] = {}
            any_changed = status in {"added", "removed"}
            for col_id in all_col_ids:
                m = _measure_lookup_for_col(col_id, measures, effective_col_keys)
                agg = (m.agg if m is not None else "sum") or "sum"
                fmt = (
                    "percent"
                    if (m is not None and (getattr(m, "format", None) == "percent" or getattr(m, "ratio_format", None) == "percentage"))
                    else getattr(m, "format", None)
                )
                d_res = compute_measure_diff(
                    cur_scalars.get(col_id),
                    base_scalars.get(col_id),
                    agg=agg,
                    diff_status=status,
                    value_format=fmt,
                )
                diffs[col_id] = d_res
                if d_res.changed:
                    any_changed = True
            if status == "unchanged" and any_changed:
                status = "modified"

            return BaselineComparisonRow(
                canonical_path=cpath,
                display_path=GRAND_TOTAL_DISPLAY_PATH if is_gt else CanonicalPathCodec.display_path(path_vals),
                path_values=path_vals,
                depth=depth,
                diff_status=status,
                current_values=cur_scalars,
                baseline_values=base_scalars,
                diffs=diffs,
                is_grand_total=is_gt,
            )

        grand_total_row: Optional[BaselineComparisonRow] = None
        if cur_gt is not None or base_gt is not None:
            grand_total_row = _build_comparison_row(GRAND_TOTAL_CANONICAL_PATH, is_gt=True)

        rows_by_path: Dict[str, BaselineComparisonRow] = {}
        children_by_parent: Dict[str, List[BaselineComparisonRow]] = {}
        for cpath in all_paths:
            row = _build_comparison_row(cpath, is_gt=False)
            rows_by_path[cpath] = row
            parent_path = (
                GRAND_TOTAL_CANONICAL_PATH
                if len(row.path_values) <= 1
                else CanonicalPathCodec.encode(row.path_values[:-1])
            )
            children_by_parent.setdefault(parent_path, []).append(row)

        # Compute Top-3 child variance attribution ("PnL Explain") for parent rows.
        def _attach_drivers(parent_row: BaselineComparisonRow) -> BaselineComparisonRow:
            kids = children_by_parent.get(parent_row.canonical_path)
            if not kids:
                return parent_row
            drivers: Dict[str, List[Dict[str, Any]]] = {}
            for col_id, parent_diff in parent_row.diffs.items():
                p_delta = parent_diff.delta
                candidates: List[Tuple[float, BaselineComparisonRow, Any]] = []
                for kid in kids:
                    kd = kid.diffs.get(col_id)
                    if kd is None or kd.delta is None:
                        continue
                    try:
                        mag = abs(float(kd.delta))
                    except (TypeError, ValueError):
                        continue
                    if mag > 0:
                        candidates.append((mag, kid, kd.delta))
                if not candidates:
                    continue
                candidates.sort(key=lambda item: item[0], reverse=True)
                p_abs = abs(float(p_delta)) if isinstance(p_delta, (int, float, Decimal)) and p_delta != 0 else 0.0
                top_list: List[Dict[str, Any]] = []
                for _, kid, k_delta in candidates[:3]:
                    share = (float(k_delta) / float(p_delta)) if p_abs > 0 else None
                    top_list.append(
                        {
                            "path": kid.display_path,
                            "canonicalPath": kid.canonical_path,
                            "label": "" if not kid.path_values else str(kid.path_values[-1]),
                            "delta": encode_wire_value(k_delta),
                            "pctShare": round(share, 4) if share is not None else None,
                            "status": kid.diff_status,
                        }
                    )
                if top_list:
                    drivers[col_id] = top_list
            if not drivers:
                return parent_row
            return BaselineComparisonRow(
                canonical_path=parent_row.canonical_path,
                display_path=parent_row.display_path,
                path_values=parent_row.path_values,
                depth=parent_row.depth,
                diff_status=parent_row.diff_status,
                current_values=parent_row.current_values,
                baseline_values=parent_row.baseline_values,
                diffs=parent_row.diffs,
                variance_drivers=drivers,
                is_grand_total=parent_row.is_grand_total,
            )

        if grand_total_row is not None:
            grand_total_row = _attach_drivers(grand_total_row)
        for cpath in list(rows_by_path.keys()):
            rows_by_path[cpath] = _attach_drivers(rows_by_path[cpath])

        # Rebuild children_by_parent with attached drivers.
        children_by_parent.clear()
        for row in rows_by_path.values():
            parent_path = (
                GRAND_TOTAL_CANONICAL_PATH
                if len(row.path_values) <= 1
                else CanonicalPathCodec.encode(row.path_values[:-1])
            )
            children_by_parent.setdefault(parent_path, []).append(row)

        frozen_rank = (
            {cpath: idx for idx, cpath in enumerate(frozen_row_order)}
            if frozen_row_order is not None
            else None
        )

        def _sort_siblings(siblings: List[BaselineComparisonRow]) -> List[BaselineComparisonRow]:
            if sort_by_variance is not None:
                col_id, metric, desc = sort_by_variance
                def _key(r: BaselineComparisonRow):
                    d = r.diffs.get(col_id)
                    if d is None:
                        val = None
                    elif metric == "pct_delta":
                        val = d.pct_delta
                    elif metric == "abs_delta":
                        val = abs(d.delta) if d.delta is not None else None
                    else:
                        val = d.delta
                    return (_numeric_sort_key(val, descending=desc), r.display_path)
                return sorted(siblings, key=_key)
            if frozen_rank is not None:
                return sorted(
                    siblings,
                    key=lambda r: (
                        0 if r.canonical_path in frozen_rank else 1,
                        frozen_rank.get(r.canonical_path, 0),
                        r.display_path,
                    ),
                )
            if sort_specs:
                first = sort_specs[0]
                s_field = str(first.get("field") or first.get("id") or "")
                desc = str(first.get("order") or ("desc" if first.get("desc") else "asc")).lower() == "desc"
                row_fields = list(spec.rows or [])

                def _row_sort_val(r: BaselineComparisonRow) -> Any:
                    if s_field in r.current_values:
                        return r.current_values.get(s_field)
                    if s_field in row_fields:
                        idx = row_fields.index(s_field)
                        if idx < len(r.path_values):
                            return r.path_values[idx]
                    return r.display_path if s_field == "hierarchy" else None

                return sorted(
                    siblings,
                    key=lambda r: (_numeric_sort_key(_row_sort_val(r), descending=desc), r.display_path),
                )
            return sorted(siblings, key=lambda r: r.display_path)

        expanded_set = set(expanded_paths) if expanded_paths is not None else None
        if expanded_set is not None and "__ALL__" in expanded_set:
            expanded_set = None
        ordered_rows: List[BaselineComparisonRow] = []

        def _walk(parent_path: str) -> None:
            kids = _sort_siblings(children_by_parent.get(parent_path, []))
            for kid in kids:
                if changed_only and kid.diff_status == "unchanged":
                    continue
                ordered_rows.append(kid)
                if expanded_set is None or kid.canonical_path in expanded_set or kid.display_path in expanded_set:
                    _walk(kid.canonical_path)

        _walk(GRAND_TOTAL_CANONICAL_PATH)

        changed_cnt = sum(1 for r in ordered_rows if r.diff_status != "unchanged")
        added_cnt = sum(1 for r in ordered_rows if r.diff_status == "added")
        removed_cnt = sum(1 for r in ordered_rows if r.diff_status == "removed")

        return BaselineComparisonResult(
            snapshot_descriptor=snapshot_descriptor,
            current_revision=current_revision,
            rows=ordered_rows,
            grand_total=grand_total_row,
            total_rows=len(ordered_rows),
            changed_rows=changed_cnt,
            added_rows=added_cnt,
            removed_rows=removed_cnt,
            col_keys=union_col_keys,
        )

    def compare_with_snapshot(
        self,
        *,
        spec: PivotSpec,
        snapshot_id: str,
        current_table: Any,
        current_revision: int,
        principal: Any = None,
        row_scope: Any = None,
        changed_only: bool = False,
        sort_by_variance: Optional[Tuple[str, str, bool]] = None,
        expanded_paths: Optional[Iterable[str]] = None,
        connection: Optional[Any] = None,
    ) -> BaselineComparisonResult:
        """End-to-end coherent comparison of current relation vs. retained snapshot."""
        cur_groups, cur_col_keys = self.aggregate_relation_hierarchy(
            current_table, spec, source_revision=current_revision
        )
        base_groups, base_col_keys, descriptor = self.get_or_compute_baseline_state(
            snapshot_id,
            spec,
            principal=principal,
            row_scope=row_scope,
            connection=connection,
        )
        return self.compare_states(
            spec=spec,
            current_groups=cur_groups,
            current_col_keys=cur_col_keys,
            current_revision=current_revision,
            baseline_groups=base_groups,
            baseline_col_keys=base_col_keys,
            snapshot_descriptor=descriptor,
            changed_only=changed_only,
            sort_by_variance=sort_by_variance,
            expanded_paths=expanded_paths,
        )
