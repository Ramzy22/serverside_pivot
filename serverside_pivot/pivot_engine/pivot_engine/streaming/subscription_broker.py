"""Subscription broker with 6-state bootstrap, ATOMIC_HANDOFF, and ACK conflation.

This module owns Component 5 of the streaming and analytical diff architecture:
- 6-state linearized bootstrap:
  REGISTERING -> CAPTURING_SNAPSHOT -> CATCHING_UP -> ATOMIC_HANDOFF -> LIVE
  with O(1) lock-only pointer handoff (<5µs) and inbox overflow -> RESYNC
- Orthogonal coordinates: `sourceRevision`, `eventSeq` (`baseSeq -> seq`),
  `queryEpoch`, and `serverEpochId`
- Application-level `lastAckSeq` tracking with bounded byte/age conflation
  and structural upgrade to authoritative `SNAPSHOT`
- Explicit `Pause Live` (visual hold) vs. `Freeze Snapshot` (retained snapshot)
- Mid-stream authorization re-validation emitting `AUTH_REVOKED` control frames
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
import json
import threading
import time
import uuid
from typing import Any, AsyncIterator, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from enum import StrEnum
except ImportError:  # Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        __str__ = str.__str__
        __format__ = str.__format__

from ..diff.baseline_comparison_service import BaselineComparisonService
from ..diff.snapshot_provider import (
    SnapshotDescriptor,
    SnapshotExpiredError,
    SnapshotProvider,
)
from ..types.pivot_spec import PivotSpec
from .exact_measure_engine import (
    GRAND_TOTAL_CANONICAL_PATH,
    GRAND_TOTAL_DISPLAY_PATH,
    encode_wire_value,
    normalize_numeric,
    values_equal,
)
from .reactive_query_coordinator import (
    LiveQueryDelta,
    LiveQueryKey,
    ReactiveQueryCoordinator,
    SharedLiveQuery,
)
from .versioned_source_adapter import (
    CommittedChangeBatch,
    SubscriptionInbox,
    VersionedSourceAdapter,
)

PROTOCOL_VERSION_V2 = 2
_DEFAULT_MAX_UNACKED_BYTES = 2 * 1024 * 1024  # 2 MiB
_DEFAULT_MAX_UNACKED_AGE_SECONDS = 15.0
_DEFAULT_SUBSCRIPTION_TTL_SECONDS = 60.0


class SubscriptionState(StrEnum):
    REGISTERING = "REGISTERING"
    CAPTURING_SNAPSHOT = "CAPTURING_SNAPSHOT"
    CATCHING_UP = "CATCHING_UP"
    ATOMIC_HANDOFF = "ATOMIC_HANDOFF"
    LIVE = "LIVE"
    RESYNC = "RESYNC"
    CLOSED = "CLOSED"


class PauseMode(StrEnum):
    ACTIVE = "active"
    PAUSE_LIVE = "pause_live"
    FREEZE_SNAPSHOT = "freeze_snapshot"


@dataclass
class ViewportWindow:
    start_row: int = 0
    end_row: int = 47
    col_start: Optional[int] = None
    col_end: Optional[int] = None
    expanded_paths: Optional[Tuple[str, ...]] = None
    sort_specs: Tuple[Dict[str, Any], ...] = ()
    sort_mode: str = "in_place"  # "in_place" | "live_sort"
    changed_only: bool = False
    sort_by_variance: Optional[Tuple[str, str, bool]] = None  # (col_id, metric, desc)


@dataclass
class _UnackedFrameRecord:
    seq: int
    byte_size: int
    sent_at: float
    had_structural_ops: bool


def _wire_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, dict):
            out[k] = {sk: encode_wire_value(sv) for sk, sv in v.items()}
        elif isinstance(v, list):
            out[k] = v
        else:
            out[k] = encode_wire_value(v)
    return out


def _numeric_delta(new_val: Any, old_val: Any) -> Tuple[Any, Optional[float], Optional[str]]:
    nv = normalize_numeric(new_val)
    ov = normalize_numeric(old_val)
    if (
        isinstance(nv, (int, float, Decimal))
        and isinstance(ov, (int, float, Decimal))
        and not isinstance(nv, bool)
        and not isinstance(ov, bool)
    ):
        if isinstance(nv, Decimal) or isinstance(ov, Decimal):
            dn = nv if isinstance(nv, Decimal) else Decimal(str(nv))
            do = ov if isinstance(ov, Decimal) else Decimal(str(ov))
            diff = dn - do
            direction = "up" if diff > 0 else ("down" if diff < 0 else None)
            pct = float(diff / abs(do)) if do != 0 else None
            return diff, pct, direction
        diff_f = nv - ov
        direction = "up" if diff_f > 0 else ("down" if diff_f < 0 else None)
        pct = (diff_f / abs(ov)) if ov != 0 else None
        return diff_f, pct, direction
    return None, None, None


class ClientViewportSubscription:
    """Per-client stateful viewport projection, bootstrap machine, and ACK tracker."""

    def __init__(
        self,
        *,
        subscription_id: str,
        table: str,
        spec: PivotSpec,
        viewport: ViewportWindow,
        principal: Any,
        row_scope: Any,
        live_query: SharedLiveQuery,
        inbox: SubscriptionInbox,
        baseline_id: Optional[str] = None,
        max_unacked_bytes: int = _DEFAULT_MAX_UNACKED_BYTES,
        max_unacked_age_seconds: float = _DEFAULT_MAX_UNACKED_AGE_SECONDS,
    ) -> None:
        self.subscription_id = subscription_id
        self.table = table
        self.spec = spec
        self.viewport = viewport
        self.principal = principal
        self.row_scope = row_scope
        self.live_query = live_query
        self.inbox = inbox
        self.baseline_id = baseline_id

        self.state: SubscriptionState = SubscriptionState.REGISTERING
        self.pause_mode: PauseMode = PauseMode.ACTIVE
        self.frozen_snapshot_id: Optional[str] = None
        self.buffered_updates_while_paused: int = 0

        self.query_epoch: int = live_query.query_epoch
        self.window_seq: int = 1
        self.event_seq: int = 0
        self.last_ack_seq: int = 0
        self.last_delivered_revision: int = 0

        # Reference frames for exact cell & structural diffing
        self.last_emitted_rows: List[Dict[str, Any]] = []
        self.last_emitted_by_path: Dict[str, Dict[str, Any]] = {}
        self.last_emitted_grand_total: Optional[Dict[str, Any]] = None
        self.last_emitted_col_keys: Tuple[str, ...] = ()
        self.frozen_row_order: Optional[List[str]] = None

        # Acknowledged baseline for safe conflation when lastAckSeq < eventSeq
        self.acked_rows_by_path: Dict[str, Dict[str, Any]] = {}
        self.acked_grand_total: Optional[Dict[str, Any]] = None
        self._unacked_frames: Deque[_UnackedFrameRecord] = deque()
        self._unacked_bytes: int = 0
        self._max_unacked_bytes = max(4096, int(max_unacked_bytes))
        self._max_unacked_age_seconds = max(0.5, float(max_unacked_age_seconds))

        self.created_at: float = time.time()
        self.last_activity_at: float = self.created_at
        self._lock = threading.RLock()

    def record_sent_frame(self, frame: Dict[str, Any], *, had_structural_ops: bool = False) -> None:
        seq = int(frame.get("seq") or 0)
        encoded = json.dumps(frame, default=str, separators=(",", ":")).encode("utf-8")
        byte_size = len(encoded)
        self._unacked_frames.append(
            _UnackedFrameRecord(
                seq=seq,
                byte_size=byte_size,
                sent_at=time.time(),
                had_structural_ops=had_structural_ops,
            )
        )
        self._unacked_bytes += byte_size

    def acknowledge(self, ack_seq: int) -> None:
        with self._lock:
            self.last_activity_at = time.time()
            if ack_seq <= self.last_ack_seq:
                return
            self.last_ack_seq = min(int(ack_seq), self.event_seq)
            while self._unacked_frames and self._unacked_frames[0].seq <= self.last_ack_seq:
                rec = self._unacked_frames.popleft()
                self._unacked_bytes = max(0, self._unacked_bytes - rec.byte_size)
            if self.last_ack_seq == self.event_seq:
                self.acked_rows_by_path = {
                    k: dict(v) for k, v in self.last_emitted_by_path.items()
                }
                self.acked_grand_total = (
                    dict(self.last_emitted_grand_total)
                    if self.last_emitted_grand_total is not None
                    else None
                )

    def requires_snapshot_due_to_unacked(self, *, incoming_structural: bool) -> bool:
        if not self._unacked_frames:
            return False
        if incoming_structural:
            return True
        if any(rec.had_structural_ops for rec in self._unacked_frames):
            return True
        if self._unacked_bytes >= self._max_unacked_bytes:
            return True
        oldest_age = time.time() - self._unacked_frames[0].sent_at
        if oldest_age >= self._max_unacked_age_seconds:
            return True
        return False


class SubscriptionBroker:
    """Component 5: Orchestrates subscriptions, linearized handoff, diffs, and auth."""

    def __init__(
        self,
        *,
        source_adapter: VersionedSourceAdapter,
        snapshot_provider: SnapshotProvider,
        coordinator: ReactiveQueryCoordinator,
        authorizer: Optional[Callable[[Any, str], Any]] = None,
        subscription_ttl_seconds: float = _DEFAULT_SUBSCRIPTION_TTL_SECONDS,
        max_unacked_bytes: int = _DEFAULT_MAX_UNACKED_BYTES,
        max_unacked_age_seconds: float = _DEFAULT_MAX_UNACKED_AGE_SECONDS,
    ) -> None:
        self._source_adapter = source_adapter
        self._snapshot_provider = snapshot_provider
        self._coordinator = coordinator
        self._authorizer = authorizer
        self._subscription_ttl_seconds = max(5.0, float(subscription_ttl_seconds))
        self._max_unacked_bytes = max_unacked_bytes
        self._max_unacked_age_seconds = max_unacked_age_seconds
        self._lock = threading.RLock()
        self._subscriptions: Dict[str, ClientViewportSubscription] = {}

    def _revalidate_authorization(self, sub: ClientViewportSubscription) -> Tuple[bool, Optional[str]]:
        if self._authorizer is None:
            return True, None
        try:
            new_scope = self._authorizer(sub.principal, sub.table)
            old_fp = getattr(sub.row_scope, "scope_fingerprint", None) or "global"
            new_fp = getattr(new_scope, "scope_fingerprint", None) or "global"
            if old_fp != new_fp:
                return False, "authorization_scope_changed"
            return True, None
        except Exception:
            return False, "authorization_revoked"

    def _materialize_subscription_rows(
        self, sub: ClientViewportSubscription
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, Tuple[str, ...], int]:
        """Materialize full ordered rows + Grand Total for `sub` (live, baseline, or frozen)."""
        if sub.pause_mode == PauseMode.FREEZE_SNAPSHOT and sub.frozen_snapshot_id:
            snap_table, desc = self._snapshot_provider.resolve_snapshot_table(
                sub.frozen_snapshot_id,
                principal=sub.principal,
                row_scope=sub.row_scope,
            )
            groups, col_keys = self._coordinator._comparison_service.aggregate_relation_hierarchy(
                snap_table, sub.live_query._effective_spec(), source_revision=desc.source_revision
            )
            # Build temporary live view over the frozen snapshot groups.
            row_dims = list(sub.spec.rows or [])
            effective_col_keys = col_keys if col_keys else ("",)
            gt_grp = groups.get(GRAND_TOTAL_CANONICAL_PATH)
            gt_dict = None
            if gt_grp is not None:
                gt_scalars = sub.live_query.engine.materialize_row_scalars(
                    gt_grp, col_keys=effective_col_keys, grand_total_group=gt_grp
                )
                gt_dict = {
                    "_canonicalPath": GRAND_TOTAL_CANONICAL_PATH,
                    "_path": GRAND_TOTAL_DISPLAY_PATH,
                    "_id": "Grand Total",
                    "_isTotal": True,
                    "depth": 0,
                    **gt_scalars,
                }
            rows_list: List[Dict[str, Any]] = []
            for cpath, grp in sorted(groups.items()):
                if cpath == GRAND_TOTAL_CANONICAL_PATH or grp.row_count <= 0:
                    continue
                scalars = sub.live_query.engine.materialize_row_scalars(
                    grp, col_keys=effective_col_keys, grand_total_group=gt_grp
                )
                rd: Dict[str, Any] = {
                    "_canonicalPath": cpath,
                    "_path": grp.display_path(),
                    "_id": "" if not grp.path_values else str(grp.path_values[-1]),
                    "_isTotal": False,
                    "depth": max(0, len(grp.path_values) - 1),
                    **scalars,
                }
                for idx, dim in enumerate(row_dims):
                    if idx < len(grp.path_values):
                        rd[dim] = grp.path_values[idx]
                rows_list.append(rd)
            return rows_list, gt_dict, desc.source_revision, col_keys, len(rows_list)

        frozen_order = sub.frozen_row_order if sub.viewport.sort_mode == "in_place" else None
        if sub.baseline_id:
            comp = self._coordinator.compute_comparison_for_live_query(
                sub.live_query,
                baseline_id=sub.baseline_id,
                principal=sub.principal,
                row_scope=sub.row_scope,
                changed_only=sub.viewport.changed_only,
                sort_by_variance=sub.viewport.sort_by_variance,
                sort_specs=sub.viewport.sort_specs,
                frozen_row_order=frozen_order,
                expanded_paths=sub.viewport.expanded_paths,
            )
            row_fields = list(sub.spec.rows or [])
            rows_list = [r.to_row_dict(row_fields) for r in comp.rows]
            gt_dict = comp.grand_total.to_row_dict(row_fields) if comp.grand_total is not None else None
            return rows_list, gt_dict, comp.current_revision, comp.col_keys, comp.total_rows

        rows_list, gt_dict = sub.live_query.materialize_hierarchy_rows(
            sort_specs=sub.viewport.sort_specs,
            expanded_paths=sub.viewport.expanded_paths,
            frozen_row_order=frozen_order,
        )
        return (
            rows_list,
            gt_dict,
            sub.live_query.last_applied_revision,
            sub.live_query.col_keys,
            len(rows_list),
        )

    def _build_snapshot_frame(
        self,
        sub: ClientViewportSubscription,
        *,
        all_rows: List[Dict[str, Any]],
        grand_total: Optional[Dict[str, Any]],
        source_revision: int,
        col_keys: Tuple[str, ...],
        total_rows: int,
        strategy_used: str = "authoritative_snapshot",
    ) -> Dict[str, Any]:
        start = max(0, int(sub.viewport.start_row))
        end = max(start, int(sub.viewport.end_row))
        window_rows = all_rows[start : end + 1]

        sub.event_seq += 1
        sub.last_delivered_revision = source_revision
        sub.last_emitted_rows = [dict(r) for r in window_rows]
        sub.last_emitted_by_path = {str(r["_canonicalPath"]): dict(r) for r in window_rows}
        sub.last_emitted_grand_total = dict(grand_total) if grand_total is not None else None
        sub.last_emitted_col_keys = col_keys
        if sub.viewport.sort_mode == "in_place":
            sub.frozen_row_order = [str(r["_canonicalPath"]) for r in all_rows]
        if sub.last_ack_seq == 0:
            sub.acked_rows_by_path = {k: dict(v) for k, v in sub.last_emitted_by_path.items()}
            sub.acked_grand_total = dict(grand_total) if grand_total is not None else None

        caps = self._source_adapter.get_source_capabilities(sub.table)
        frame: Dict[str, Any] = {
            "type": "SNAPSHOT",
            "protocolVersion": PROTOCOL_VERSION_V2,
            "subscriptionId": sub.subscription_id,
            "serverEpochId": self._source_adapter.server_epoch_id,
            "table": sub.table,
            "queryEpoch": sub.query_epoch,
            "windowSeq": sub.window_seq,
            "seq": sub.event_seq,
            "sourceRevision": source_revision,
            "dataRevision": source_revision,
            "schemaVersion": sub.live_query.schema_version,
            "baselineId": sub.baseline_id,
            "frozenSnapshotId": sub.frozen_snapshot_id,
            "pauseMode": sub.pause_mode.value,
            "freshnessMode": caps.tier.value,
            "timestamp": time.time(),
            "dataOffset": start,
            "rowCount": total_rows,
            "colKeys": list(col_keys),
            "rows": [_wire_row(r) for r in window_rows],
            "grandTotal": _wire_row(grand_total) if grand_total is not None else None,
            "summary": {
                "strategy": strategy_used,
                "changedCells": 0,
                "addedRows": 0,
                "removedRows": 0,
            },
        }
        sub.record_sent_frame(frame, had_structural_ops=False)
        return frame

    def _build_diff_frame(
        self,
        sub: ClientViewportSubscription,
        *,
        all_rows: List[Dict[str, Any]],
        grand_total: Optional[Dict[str, Any]],
        source_revision: int,
        col_keys: Tuple[str, ...],
        total_rows: int,
        strategy_used: str,
        columns_changed: bool = False,
        schema_changed: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if schema_changed or columns_changed or col_keys != sub.last_emitted_col_keys:
            return self._build_snapshot_frame(
                sub,
                all_rows=all_rows,
                grand_total=grand_total,
                source_revision=source_revision,
                col_keys=col_keys,
                total_rows=total_rows,
                strategy_used=strategy_used,
            )

        start = max(0, int(sub.viewport.start_row))
        end = max(start, int(sub.viewport.end_row))
        next_window = all_rows[start : end + 1]
        next_by_path = {str(r["_canonicalPath"]): r for r in next_window}

        prev_paths = [str(r["_canonicalPath"]) for r in sub.last_emitted_rows]
        next_paths = [str(r["_canonicalPath"]) for r in next_window]
        prev_set = set(prev_paths)
        next_set = set(next_paths)

        added_paths = [p for p in next_paths if p not in prev_set]
        removed_paths = [p for p in prev_paths if p not in next_set]
        structural_change = prev_paths != next_paths

        # Adaptive threshold: >20% structural churn or unacked structural state -> SNAPSHOT
        window_len = max(1, len(next_window), len(sub.last_emitted_rows))
        moved_or_changed_rows = len(added_paths) + len(removed_paths)
        if structural_change and not added_paths and not removed_paths:
            moved_or_changed_rows = sum(1 for a, b in zip(prev_paths, next_paths) if a != b)

        if (
            (structural_change and (moved_or_changed_rows / window_len) > 0.20)
            or sub.requires_snapshot_due_to_unacked(incoming_structural=structural_change)
        ):
            return self._build_snapshot_frame(
                sub,
                all_rows=all_rows,
                grand_total=grand_total,
                source_revision=source_revision,
                col_keys=col_keys,
                total_rows=total_rows,
                strategy_used=strategy_used,
            )

        # Reference map for prevValue/tickDelta (use acked frame when unacked frames exist)
        ref_by_path = (
            sub.acked_rows_by_path
            if sub.last_ack_seq < sub.event_seq and sub.acked_rows_by_path
            else sub.last_emitted_by_path
        )
        ref_gt = (
            sub.acked_grand_total
            if sub.last_ack_seq < sub.event_seq and sub.acked_grand_total is not None
            else sub.last_emitted_grand_total
        )

        reserved_keys = {
            "_canonicalPath",
            "_path",
            "_id",
            "_isTotal",
            "depth",
            "_has_children",
            "_diffStatus",
            "_baseline",
            "_delta",
            "_pctDelta",
            "_deltaUnit",
            "_varianceDrivers",
        }
        row_dim_set = set(sub.spec.rows or [])

        cell_patches: List[Dict[str, Any]] = []
        for row in next_window:
            cpath = str(row["_canonicalPath"])
            if cpath not in prev_set:
                continue
            prev_row = ref_by_path.get(cpath) or {}
            baseline_map = row.get("_baseline") if isinstance(row.get("_baseline"), Mapping) else {}
            delta_map = row.get("_delta") if isinstance(row.get("_delta"), Mapping) else {}
            pct_map = row.get("_pctDelta") if isinstance(row.get("_pctDelta"), Mapping) else {}
            unit_map = row.get("_deltaUnit") if isinstance(row.get("_deltaUnit"), Mapping) else {}
            drivers_map = row.get("_varianceDrivers") if isinstance(row.get("_varianceDrivers"), Mapping) else {}

            for col_id, new_val in row.items():
                if col_id in reserved_keys or col_id in row_dim_set:
                    continue
                old_val = prev_row.get(col_id)
                status_lifecycle_changed = row.get("_diffStatus") in {"new", "removed"} and row.get("_diffStatus") != prev_row.get("_diffStatus")
                if values_equal(new_val, old_val) and not status_lifecycle_changed:
                    continue
                t_delta, t_pct, t_dir = _numeric_delta(new_val, old_val)
                patch_item: Dict[str, Any] = {
                    "kind": "cell",
                    "rowId": row["_path"],
                    "canonicalPath": cpath,
                    "colId": col_id,
                    "columnId": col_id,
                    "value": encode_wire_value(new_val),
                    "prevValue": encode_wire_value(old_val),
                    "tickDelta": encode_wire_value(t_delta),
                    "tickPct": t_pct,
                    "tickDir": t_dir,
                    "diffStatus": row.get("_diffStatus"),
                }
                if col_id in baseline_map or col_id in delta_map:
                    patch_item["baselineValue"] = baseline_map.get(col_id)
                    patch_item["baselineDelta"] = delta_map.get(col_id)
                    patch_item["baselinePct"] = pct_map.get(col_id)
                    patch_item["deltaUnit"] = unit_map.get(col_id, "abs")
                if col_id in drivers_map:
                    patch_item["varianceDrivers"] = drivers_map.get(col_id)
                cell_patches.append(patch_item)

        # Build rowOps if small structural update (<= 20% of window)
        row_ops: List[Dict[str, Any]] = []
        if structural_change:
            prev_idx = {p: i for i, p in enumerate(prev_paths)}
            for rp in removed_paths:
                prev_r = sub.last_emitted_by_path.get(rp)
                row_ops.append(
                    {
                        "op": "remove",
                        "canonicalPath": rp,
                        "rowId": prev_r["_path"] if prev_r else rp,
                        "fromIndex": start + prev_idx.get(rp, 0),
                    }
                )
            for idx, np in enumerate(next_paths):
                if np in added_paths:
                    row_ops.append(
                        {
                            "op": "insert",
                            "canonicalPath": np,
                            "rowId": next_by_path[np]["_path"],
                            "toIndex": start + idx,
                            "rowData": _wire_row(next_by_path[np]),
                        }
                    )
                elif prev_idx.get(np) != idx:
                    row_ops.append(
                        {
                            "op": "move",
                            "canonicalPath": np,
                            "rowId": next_by_path[np]["_path"],
                            "fromIndex": start + prev_idx[np],
                            "toIndex": start + idx,
                            "rankShift": prev_idx[np] - idx,
                        }
                    )

        # Contract D: Grand Total patch even when zero leaf rows in viewport changed!
        gt_patch: Dict[str, Any] = {}
        if grand_total is not None:
            prev_gt = ref_gt or {}
            gt_base_map = grand_total.get("_baseline") if isinstance(grand_total.get("_baseline"), Mapping) else {}
            gt_delta_map = grand_total.get("_delta") if isinstance(grand_total.get("_delta"), Mapping) else {}
            gt_pct_map = grand_total.get("_pctDelta") if isinstance(grand_total.get("_pctDelta"), Mapping) else {}
            for col_id, new_val in grand_total.items():
                if col_id in reserved_keys or col_id in row_dim_set:
                    continue
                old_val = prev_gt.get(col_id)
                if values_equal(new_val, old_val):
                    continue
                t_delta, t_pct, t_dir = _numeric_delta(new_val, old_val)
                item: Dict[str, Any] = {
                    "value": encode_wire_value(new_val),
                    "prevValue": encode_wire_value(old_val),
                    "tickDelta": encode_wire_value(t_delta),
                    "tickPct": t_pct,
                    "tickDir": t_dir,
                }
                if col_id in gt_base_map or col_id in gt_delta_map:
                    item["baselineValue"] = gt_base_map.get(col_id)
                    item["baselineDelta"] = gt_delta_map.get(col_id)
                    item["baselinePct"] = gt_pct_map.get(col_id)
                gt_patch[col_id] = item

        # Zero-noise guarantee: emit nothing if viewport & Grand Total are identical!
        if not cell_patches and not row_ops and not gt_patch:
            sub.last_delivered_revision = source_revision
            return None

        base_seq = sub.event_seq
        sub.event_seq += 1
        sub.last_delivered_revision = source_revision
        sub.last_emitted_rows = [dict(r) for r in next_window]
        sub.last_emitted_by_path = {str(r["_canonicalPath"]): dict(r) for r in next_window}
        sub.last_emitted_grand_total = dict(grand_total) if grand_total is not None else None
        sub.last_emitted_col_keys = col_keys
        if sub.viewport.sort_mode == "in_place":
            sub.frozen_row_order = [str(r["_canonicalPath"]) for r in all_rows]

        caps = self._source_adapter.get_source_capabilities(sub.table)
        frame = {
            "type": "PATCH",
            "protocolVersion": PROTOCOL_VERSION_V2,
            "subscriptionId": sub.subscription_id,
            "serverEpochId": self._source_adapter.server_epoch_id,
            "table": sub.table,
            "queryEpoch": sub.query_epoch,
            "windowSeq": sub.window_seq,
            "baseSeq": base_seq,
            "seq": sub.event_seq,
            "sourceRevision": source_revision,
            "dataRevision": source_revision,
            "schemaVersion": sub.live_query.schema_version,
            "baselineId": sub.baseline_id,
            "freshnessMode": caps.tier.value,
            "timestamp": time.time(),
            "dataOffset": start,
            "rowCount": total_rows,
            "cellPatches": cell_patches,
            "rowOps": row_ops,
            "grandTotalPatch": gt_patch or None,
            "summary": {
                "strategy": strategy_used,
                "changedCells": len(cell_patches),
                "addedRows": len(added_paths),
                "removedRows": len(removed_paths),
            },
        }
        sub.record_sent_frame(frame, had_structural_ops=bool(row_ops))
        return frame

    def subscribe(
        self,
        *,
        spec: PivotSpec,
        viewport: ViewportWindow,
        principal: Any = None,
        row_scope: Any = None,
        baseline_id: Optional[str] = None,
        known_revision: Optional[int] = None,
        on_notify: Optional[Callable[[], None]] = None,
    ) -> Tuple[ClientViewportSubscription, Dict[str, Any]]:
        """Execute the 6-state linearized bootstrap with <5µs ATOMIC_HANDOFF."""
        table = spec.table
        if self._authorizer is not None:
            row_scope = self._authorizer(principal, table)

        # Phase 1: REGISTERING — Register inbox cursor BEFORE any snapshot query!
        inbox = self._source_adapter.register_inbox_cursor(table, on_notify=on_notify)
        try:
            if baseline_id:
                self._snapshot_provider.acquire_snapshot(
                    baseline_id, principal=principal, row_scope=row_scope
                )

            # Phase 2: CAPTURING_SNAPSHOT — Acquire/initialize SharedLiveQuery at R_snap.
            live_query = self._coordinator.acquire_live_query(
                spec, principal=principal, row_scope=row_scope
            )
            sub_id = f"sub_{uuid.uuid4().hex[:16]}"
            sub = ClientViewportSubscription(
                subscription_id=sub_id,
                table=table,
                spec=spec,
                viewport=viewport,
                principal=principal,
                row_scope=row_scope,
                live_query=live_query,
                inbox=inbox,
                baseline_id=baseline_id,
                max_unacked_bytes=self._max_unacked_bytes,
                max_unacked_age_seconds=self._max_unacked_age_seconds,
            )
            sub.state = SubscriptionState.CAPTURING_SNAPSHOT

            # If SharedLiveQuery was re-acquired from cache or client rendered at known_revision
            # and commits happened before inbox registration, catch up via ring buffer or re-init.
            if live_query.last_applied_revision < inbox.floor_revision:
                replay = self._source_adapter.get_replay_batches(
                    table, live_query.last_applied_revision
                )
                if replay is None:
                    live_query.initialize()
                else:
                    for b in replay:
                        if b.source_revision > live_query.last_applied_revision:
                            live_query.apply_batch(b)
            elif known_revision is not None and known_revision < live_query.last_applied_revision:
                replay = self._source_adapter.get_replay_batches(table, known_revision)
                if replay is None:
                    live_query.initialize()

            # Phase 3: CATCHING_UP (Outside `_publish_lock`!)
            sub.state = SubscriptionState.CATCHING_UP
            while True:
                if inbox.overflowed:
                    sub.state = SubscriptionState.RESYNC
                    self._source_adapter.unregister_inbox_cursor(inbox)
                    inbox = self._source_adapter.register_inbox_cursor(table, on_notify=on_notify)
                    sub.inbox = inbox
                    live_query.initialize()
                    continue

                item = self._source_adapter.pop_inbox_item(inbox)
                if item is None:
                    break
                if item.batch.source_revision > live_query.last_applied_revision:
                    live_query.apply_batch(item.batch)

            # Phase 4: ATOMIC_HANDOFF (Blocker 1B: <5µs pointer flip inside `_publish_lock`!)
            sub.state = SubscriptionState.ATOMIC_HANDOFF
            handoff_rev = self._source_adapter.seal_atomic_handoff(
                inbox, live_query.last_applied_revision
            )

            # Drain any final pre-handoff items up to `is_handoff_seal` OUTSIDE `_publish_lock`.
            if handoff_rev > live_query.last_applied_revision:
                while True:
                    item = self._source_adapter.pop_inbox_item(inbox)
                    if item is None:
                        break
                    if item.batch.source_revision > live_query.last_applied_revision:
                        live_query.apply_batch(item.batch)
                    if item.is_handoff_seal or item.batch.source_revision >= handoff_rev:
                        break

            # Seal initial SNAPSHOT at exact `R_handoff`.
            all_rows, gt_dict, rev, col_keys, total_rows = self._materialize_subscription_rows(sub)
            snapshot_frame = self._build_snapshot_frame(
                sub,
                all_rows=all_rows,
                grand_total=gt_dict,
                source_revision=rev,
                col_keys=col_keys,
                total_rows=total_rows,
                strategy_used="atomic_handoff_snapshot",
            )
            sub.state = SubscriptionState.LIVE
            with self._lock:
                self._subscriptions[sub_id] = sub
            return sub, snapshot_frame
        except Exception:
            self._source_adapter.unregister_inbox_cursor(inbox)
            raise

    def poll_updates(
        self,
        subscription_id: str,
        *,
        ack_seq: Optional[int] = None,
        client_server_epoch_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Drain pending inbox revisions for `subscription_id` and return next WireFrameV2."""
        if (
            client_server_epoch_id is not None
            and client_server_epoch_id != self._source_adapter.server_epoch_id
        ):
            return {
                "type": "INVALIDATE",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "subscriptionId": subscription_id,
                "serverEpochId": self._source_adapter.server_epoch_id,
                "reason": "server_epoch_changed",
                "timestamp": time.time(),
            }

        with self._lock:
            sub = self._subscriptions.get(subscription_id)
        if sub is None:
            return {
                "type": "INVALIDATE",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "subscriptionId": subscription_id,
                "serverEpochId": self._source_adapter.server_epoch_id,
                "reason": "unknown_subscription",
                "timestamp": time.time(),
            }

        # Re-validate authorization before delivering any frame!
        authorized, auth_reason = self._revalidate_authorization(sub)
        if not authorized:
            self.unsubscribe(subscription_id)
            return {
                "type": "AUTH_REVOKED",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "subscriptionId": subscription_id,
                "code": auth_reason or "authorization_revoked",
                "timestamp": time.time(),
            }

        if ack_seq is not None:
            sub.acknowledge(ack_seq)

        with sub._lock:
            sub.last_activity_at = time.time()

            # Pause Live: accumulate update count without applying visual diffs.
            if sub.pause_mode == PauseMode.PAUSE_LIVE:
                drained_cnt = 0
                while True:
                    item = self._source_adapter.pop_inbox_item(sub.inbox)
                    if item is None:
                        break
                    drained_cnt += 1
                    if item.batch.source_revision > sub.live_query.last_applied_revision:
                        sub.live_query.apply_batch(item.batch)
                sub.buffered_updates_while_paused += drained_cnt
                return {
                    "type": "HEARTBEAT",
                    "protocolVersion": PROTOCOL_VERSION_V2,
                    "subscriptionId": sub.subscription_id,
                    "serverEpochId": self._source_adapter.server_epoch_id,
                    "queryEpoch": sub.query_epoch,
                    "seq": sub.event_seq,
                    "sourceRevision": sub.last_delivered_revision,
                    "pauseMode": sub.pause_mode.value,
                    "paused": True,
                    "bufferedUpdates": sub.buffered_updates_while_paused,
                    "timestamp": time.time(),
                }

            # Freeze Snapshot: validate snapshot hasn't expired; live ticks do not mutate view.
            if sub.pause_mode == PauseMode.FREEZE_SNAPSHOT:
                if sub.frozen_snapshot_id:
                    # Verify snapshot is still valid (raises SnapshotExpiredError if expired).
                    self._snapshot_provider.get_descriptor(
                        sub.frozen_snapshot_id,
                        principal=sub.principal,
                        row_scope=sub.row_scope,
                        require_unexpired=True,
                    )
                return {
                    "type": "HEARTBEAT",
                    "protocolVersion": PROTOCOL_VERSION_V2,
                    "subscriptionId": sub.subscription_id,
                    "serverEpochId": self._source_adapter.server_epoch_id,
                    "queryEpoch": sub.query_epoch,
                    "seq": sub.event_seq,
                    "sourceRevision": sub.last_delivered_revision,
                    "pauseMode": sub.pause_mode.value,
                    "paused": False,
                    "frozenSnapshotId": sub.frozen_snapshot_id,
                    "timestamp": time.time(),
                }

            # Handle inbox overflow -> RESYNC via fresh authoritative snapshot.
            if sub.inbox.overflowed:
                sub.state = SubscriptionState.RESYNC
                self._source_adapter.unregister_inbox_cursor(sub.inbox)
                sub.inbox = self._source_adapter.register_inbox_cursor(sub.table)
                sub.live_query.initialize()
                all_rows, gt_dict, rev, col_keys, total_rows = self._materialize_subscription_rows(sub)
                sub.state = SubscriptionState.LIVE
                return self._build_snapshot_frame(
                    sub,
                    all_rows=all_rows,
                    grand_total=gt_dict,
                    source_revision=rev,
                    col_keys=col_keys,
                    total_rows=total_rows,
                    strategy_used="overflow_resync_snapshot",
                )

            # Drain all pending committed batches from inbox.
            drained_batches: List[CommittedChangeBatch] = []
            while True:
                item = self._source_adapter.pop_inbox_item(sub.inbox)
                if item is None:
                    break
                drained_batches.append(item.batch)

            strategy_used = "tier_a_incremental"
            columns_changed = False
            schema_changed = False

            for batch in drained_batches:
                delta = sub.live_query.apply_batch(batch)
                if delta.strategy_used == "tier_c_authoritative":
                    strategy_used = "tier_c_authoritative"
                elif delta.strategy_used == "tier_b_targeted_sql" and strategy_used != "tier_c_authoritative":
                    strategy_used = "tier_b_targeted_sql"
                if delta.columns_changed:
                    columns_changed = True
                if delta.schema_changed:
                    schema_changed = True

            # Check if SharedLiveQuery advanced via another subscriber on the same LiveQueryKey!
            if (
                not drained_batches
                and sub.live_query.last_applied_revision <= sub.last_delivered_revision
                and sub.live_query.query_epoch == sub.query_epoch
            ):
                return {
                    "type": "HEARTBEAT",
                    "protocolVersion": PROTOCOL_VERSION_V2,
                    "subscriptionId": sub.subscription_id,
                    "serverEpochId": self._source_adapter.server_epoch_id,
                    "queryEpoch": sub.query_epoch,
                    "seq": sub.event_seq,
                    "sourceRevision": sub.last_delivered_revision,
                    "timestamp": time.time(),
                }

            if sub.live_query.query_epoch != sub.query_epoch:
                sub.query_epoch = sub.live_query.query_epoch
                schema_changed = True

            all_rows, gt_dict, rev, col_keys, total_rows = self._materialize_subscription_rows(sub)
            diff_frame = self._build_diff_frame(
                sub,
                all_rows=all_rows,
                grand_total=gt_dict,
                source_revision=rev,
                col_keys=col_keys,
                total_rows=total_rows,
                strategy_used=strategy_used,
                columns_changed=columns_changed,
                schema_changed=schema_changed,
            )
            if diff_frame is None:
                return {
                    "type": "HEARTBEAT",
                    "protocolVersion": PROTOCOL_VERSION_V2,
                    "subscriptionId": sub.subscription_id,
                    "serverEpochId": self._source_adapter.server_epoch_id,
                    "queryEpoch": sub.query_epoch,
                    "seq": sub.event_seq,
                    "sourceRevision": rev,
                    "timestamp": time.time(),
                }
            return diff_frame

    def modify_viewport(
        self,
        subscription_id: str,
        viewport: ViewportWindow,
        *,
        baseline_id: Optional[str] = None,
        update_baseline: bool = False,
    ) -> Dict[str, Any]:
        """Update viewport window/sort/baseline and return a fresh authoritative SNAPSHOT."""
        with self._lock:
            sub = self._subscriptions.get(subscription_id)
        if sub is None:
            return {
                "type": "INVALIDATE",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "subscriptionId": subscription_id,
                "reason": "unknown_subscription",
            }
        authorized, auth_reason = self._revalidate_authorization(sub)
        if not authorized:
            self.unsubscribe(subscription_id)
            return {
                "type": "AUTH_REVOKED",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "subscriptionId": subscription_id,
                "code": auth_reason or "authorization_revoked",
            }

        with sub._lock:
            if update_baseline and baseline_id != sub.baseline_id:
                old_base = sub.baseline_id
                if baseline_id:
                    self._snapshot_provider.acquire_snapshot(
                        baseline_id, principal=sub.principal, row_scope=sub.row_scope
                    )
                if old_base:
                    self._snapshot_provider.release_snapshot(old_base)
                sub.baseline_id = baseline_id
                sub.query_epoch += 1

            old_viewport = sub.viewport
            if (
                viewport.expanded_paths != old_viewport.expanded_paths
                or viewport.sort_specs != old_viewport.sort_specs
                or viewport.changed_only != old_viewport.changed_only
                or viewport.sort_by_variance != old_viewport.sort_by_variance
            ):
                sub.frozen_row_order = None

            sub.viewport = viewport
            sub.window_seq += 1
            sub.last_activity_at = time.time()

            # Pause Live scrolling to a new viewport follows explicit freshness policy:
            # clear buffered count and emit authoritative SNAPSHOT at current state.
            if sub.pause_mode == PauseMode.PAUSE_LIVE:
                sub.pause_mode = PauseMode.ACTIVE
                sub.buffered_updates_while_paused = 0

            if sub.pause_mode == PauseMode.ACTIVE:
                while True:
                    item = self._source_adapter.pop_inbox_item(sub.inbox)
                    if item is None:
                        break
                    if item.batch.source_revision > sub.live_query.last_applied_revision:
                        sub.live_query.apply_batch(item.batch)

            all_rows, gt_dict, rev, col_keys, total_rows = self._materialize_subscription_rows(sub)
            return self._build_snapshot_frame(
                sub,
                all_rows=all_rows,
                grand_total=gt_dict,
                source_revision=rev,
                col_keys=col_keys,
                total_rows=total_rows,
                strategy_used="viewport_change_snapshot",
            )

    def set_pause_mode(
        self,
        subscription_id: str,
        mode: str,
        *,
        ttl_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Switch between active, pause_live (visual hold), and freeze_snapshot (retained)."""
        with self._lock:
            sub = self._subscriptions.get(subscription_id)
        if sub is None:
            return {
                "type": "INVALIDATE",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "subscriptionId": subscription_id,
                "reason": "unknown_subscription",
            }

        target_mode = PauseMode(str(mode or "active").strip().lower())
        with sub._lock:
            prev_frozen = sub.frozen_snapshot_id
            if target_mode == PauseMode.PAUSE_LIVE:
                sub.pause_mode = PauseMode.PAUSE_LIVE
                sub.buffered_updates_while_paused = 0
                return {
                    "type": "HEARTBEAT",
                    "protocolVersion": PROTOCOL_VERSION_V2,
                    "subscriptionId": sub.subscription_id,
                    "pauseMode": sub.pause_mode.value,
                    "paused": True,
                    "bufferedUpdates": sub.buffered_updates_while_paused,
                    "sourceRevision": sub.last_delivered_revision,
                    "seq": sub.event_seq,
                    "timestamp": time.time(),
                }

            if target_mode == PauseMode.FREEZE_SNAPSHOT:
                desc = self._snapshot_provider.capture_snapshot(
                    sub.table,
                    principal=sub.principal,
                    row_scope=sub.row_scope,
                    kind="freeze_snapshot",
                    ttl_seconds=ttl_seconds,
                )
                self._snapshot_provider.acquire_snapshot(
                    desc.snapshot_id, principal=sub.principal, row_scope=sub.row_scope
                )
                if prev_frozen:
                    self._snapshot_provider.release_snapshot(prev_frozen)
                sub.frozen_snapshot_id = desc.snapshot_id
                sub.pause_mode = PauseMode.FREEZE_SNAPSHOT
                sub.buffered_updates_while_paused = 0
                all_rows, gt_dict, rev, col_keys, total_rows = self._materialize_subscription_rows(sub)
                return self._build_snapshot_frame(
                    sub,
                    all_rows=all_rows,
                    grand_total=gt_dict,
                    source_revision=rev,
                    col_keys=col_keys,
                    total_rows=total_rows,
                    strategy_used="freeze_snapshot_pinned",
                )

            # Resuming to ACTIVE: release any frozen snapshot and resync to latest state.
            if prev_frozen:
                self._snapshot_provider.release_snapshot(prev_frozen)
                sub.frozen_snapshot_id = None
            sub.pause_mode = PauseMode.ACTIVE
            sub.buffered_updates_while_paused = 0
            # Drain any pending inbox items so resume shows latest committed state.
            while True:
                item = self._source_adapter.pop_inbox_item(sub.inbox)
                if item is None:
                    break
                if item.batch.source_revision > sub.live_query.last_applied_revision:
                    sub.live_query.apply_batch(item.batch)
            all_rows, gt_dict, rev, col_keys, total_rows = self._materialize_subscription_rows(sub)
            return self._build_snapshot_frame(
                sub,
                all_rows=all_rows,
                grand_total=gt_dict,
                source_revision=rev,
                col_keys=col_keys,
                total_rows=total_rows,
                strategy_used="resume_resync_snapshot",
            )

    def acknowledge(self, subscription_id: str, ack_seq: int) -> bool:
        with self._lock:
            sub = self._subscriptions.get(subscription_id)
        if sub is None:
            return False
        sub.acknowledge(ack_seq)
        return True

    def unsubscribe(self, subscription_id: str) -> bool:
        with self._lock:
            sub = self._subscriptions.pop(subscription_id, None)
        if sub is None:
            return False
        with sub._lock:
            sub.state = SubscriptionState.CLOSED
            self._source_adapter.unregister_inbox_cursor(sub.inbox)
            if sub.baseline_id:
                self._snapshot_provider.release_snapshot(sub.baseline_id)
                sub.baseline_id = None
            if sub.frozen_snapshot_id:
                self._snapshot_provider.release_snapshot(sub.frozen_snapshot_id)
                sub.frozen_snapshot_id = None
            self._coordinator.release_live_query(sub.live_query.live_key)
        return True

    def get_subscription(self, subscription_id: str) -> Optional[ClientViewportSubscription]:
        with self._lock:
            return self._subscriptions.get(subscription_id)

    def cleanup_expired_subscriptions(self) -> int:
        now = time.time()
        expired_ids: List[str] = []
        with self._lock:
            for sub_id, sub in self._subscriptions.items():
                if (now - sub.last_activity_at) > self._subscription_ttl_seconds:
                    expired_ids.append(sub_id)
        for sub_id in expired_ids:
            self.unsubscribe(sub_id)
        return len(expired_ids)

    async def stream_events(
        self,
        subscription_id: str,
        *,
        poll_interval_seconds: float = 0.1,
        heartbeat_interval_seconds: float = 5.0,
        is_disconnected: Optional[Callable[[], Any]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Async generator for FastAPI SSE / WebSocket delivery with clean disconnect."""
        last_heartbeat = time.monotonic()
        try:
            while True:
                if is_disconnected is not None:
                    disc = is_disconnected()
                    if asyncio.iscoroutine(disc):
                        disc = await disc
                    if disc:
                        break
                frame = self.poll_updates(subscription_id)
                ftype = frame.get("type")
                if ftype in {"AUTH_REVOKED", "INVALIDATE"}:
                    yield frame
                    break
                if ftype in {"SNAPSHOT", "PATCH"}:
                    last_heartbeat = time.monotonic()
                    yield frame
                elif (time.monotonic() - last_heartbeat) >= heartbeat_interval_seconds:
                    last_heartbeat = time.monotonic()
                    yield frame
                await asyncio.sleep(max(0.02, poll_interval_seconds))
        finally:
            self.unsubscribe(subscription_id)

    def close(self) -> None:
        with self._lock:
            sub_ids = list(self._subscriptions.keys())
        for sub_id in sub_ids:
            self.unsubscribe(sub_id)
