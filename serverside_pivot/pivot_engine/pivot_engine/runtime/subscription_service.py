"""Runtime capability facade for live streaming subscriptions and baseline diffs.

Keeps `PivotRuntimeService`, `RuntimeRequestRouter`, `RuntimeHttpGateway`, and
Dash callbacks thin by encapsulating all subscription & snapshot lifecycle logic
in one dedicated runtime service module (respecting the one-way dependency
boundaries enforced by `test_runtime_capability_architecture.py`).
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from ..diff.baseline_comparison_service import BaselineComparisonService
from .dataset_authorization import RuntimeDatasetAuthorizationError
from ..diff.snapshot_provider import (
    SnapshotAuthorizationError,
    SnapshotDescriptor,
    SnapshotError,
    SnapshotExpiredError,
    SnapshotProvider,
)
from ..streaming.reactive_query_coordinator import ReactiveQueryCoordinator
from ..streaming.subscription_broker import (
    PROTOCOL_VERSION_V2,
    SubscriptionBroker,
    ViewportWindow,
)
from ..streaming.versioned_source_adapter import (
    CommittedChangeBatch,
    SourceMutationError,
    VersionedSourceAdapter,
)
from ..types.pivot_spec import Measure, PivotSpec


def _coerce_measures(val_configs: Sequence[Mapping[str, Any]]) -> List[Measure]:
    measures: List[Measure] = []
    for cfg in val_configs or []:
        if not isinstance(cfg, Mapping):
            continue
        field = str(cfg.get("field") or "")
        agg = str(cfg.get("agg") or ("formula" if cfg.get("formula") else "sum")).strip().lower()
        alias = str(cfg.get("alias") or cfg.get("id") or (field if agg == "formula" else f"{field}_{agg}"))
        measures.append(
            Measure(
                field=field,
                agg=agg,
                alias=alias,
                weighted_field=cfg.get("weightField") or cfg.get("weighted_field"),
                order_by=cfg.get("orderBy") or cfg.get("order_by"),
                window_func=cfg.get("windowFn") or cfg.get("window_func"),
                expression=cfg.get("formula") or cfg.get("expression"),
                ratio_format="percentage" if str(cfg.get("format") or "").strip().lower() == "percent" else "decimal",
                filter_condition=cfg.get("filterCondition") or cfg.get("filter_condition"),
            )
        )
    return measures


def _map_ui_filter_op(raw_op: Any) -> str:
    op = str(raw_op or "eq").strip().lower()
    mapping = {
        "eq": "=",
        "equals": "=",
        "=": "=",
        "==": "=",
        "neq": "!=",
        "ne": "!=",
        "not_equals": "!=",
        "!=": "!=",
        "<>": "!=",
        "gt": ">",
        ">": ">",
        "gte": ">=",
        ">=": ">=",
        "lt": "<",
        "<": "<",
        "lte": "<=",
        "<=": "<=",
        "in": "in",
        "not_in": "not_in",
        "contains": "contains",
        "not_contains": "not_contains",
        "starts_with": "starts_with",
        "ends_with": "ends_with",
        "between": "between",
        "is_null": "is_null",
        "is_not_null": "is_not_null",
    }
    return mapping.get(op, op)


def _coerce_filters(raw_filters: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_filters, list):
        return [dict(item) for item in raw_filters if isinstance(item, Mapping)]
    if isinstance(raw_filters, Mapping):
        out: List[Dict[str, Any]] = []
        for field_name, spec in raw_filters.items():
            if field_name == "global" or spec is None:
                continue
            if isinstance(spec, Mapping):
                if "conditions" in spec and isinstance(spec.get("conditions"), (list, tuple)):
                    sub_conds: List[Dict[str, Any]] = []
                    for cond in spec.get("conditions") or []:
                        if not isinstance(cond, Mapping):
                            continue
                        sub_conds.append(
                            {
                                "field": str(cond.get("field") or field_name),
                                "op": _map_ui_filter_op(cond.get("op") or cond.get("type") or "eq"),
                                "value": cond.get("value"),
                                "caseSensitive": bool(cond.get("caseSensitive", False)),
                            }
                        )
                    if sub_conds:
                        out.append(
                            {
                                "op": str(spec.get("operator") or spec.get("op") or "AND").upper(),
                                "conditions": sub_conds,
                            }
                        )
                elif "field" in spec and "op" in spec:
                    out.append(dict(spec))
                else:
                    out.append(
                        {
                            "field": str(spec.get("field") or field_name),
                            "op": _map_ui_filter_op(spec.get("op") or spec.get("type") or "eq"),
                            "value": spec.get("value"),
                            "caseSensitive": bool(spec.get("caseSensitive", False)),
                        }
                    )
            elif isinstance(spec, (list, tuple, set)):
                out.append({"field": field_name, "op": "in", "value": list(spec)})
            else:
                out.append({"field": field_name, "op": "=", "value": spec})
        return out
    return []


def build_pivot_spec_from_subscription_state(
    table: str,
    state: Mapping[str, Any],
) -> PivotSpec:
    row_fields = [str(x) for x in (state.get("rowFields") or []) if x]
    col_fields = [str(x) for x in (state.get("colFields") or []) if x]
    measures = _coerce_measures(state.get("valConfigs") or [])
    filters = _coerce_filters(state.get("filters"))
    sorting = [dict(s) for s in (state.get("sorting") or []) if isinstance(s, Mapping)]
    custom_dims = [dict(c) for c in (state.get("customDimensions") or []) if isinstance(c, Mapping)]
    return PivotSpec(
        table=table,
        rows=row_fields,
        columns=col_fields,
        measures=measures,
        filters=filters,
        sort=sorting,
        totals=bool(state.get("showColTotals", True)),
        custom_dimensions=custom_dims,
    )


def build_viewport_window_from_payload(
    payload: Mapping[str, Any],
    state: Mapping[str, Any],
) -> ViewportWindow:
    start_row = int(payload.get("start", payload.get("startRow", 0)) or 0)
    end_row = int(payload.get("end", payload.get("endRow", start_row + 47)) or (start_row + 47))
    col_start = payload.get("colStart", payload.get("col_start"))
    col_end = payload.get("colEnd", payload.get("col_end"))
    raw_expanded = state.get("expanded", payload.get("expanded"))
    expanded_paths: Optional[Tuple[str, ...]] = ()
    if raw_expanded is True:
        expanded_paths = None  # expand all
    elif isinstance(raw_expanded, Mapping):
        if bool(raw_expanded.get("__ALL__")):
            expanded_paths = None
        else:
            expanded_paths = tuple(str(k) for k, v in raw_expanded.items() if v and k != "__ALL__")
    elif isinstance(raw_expanded, (list, tuple)):
        flat_items = [str(x) for x in raw_expanded]
        if "__ALL__" in flat_items or "['__ALL__']" in flat_items:
            expanded_paths = None
        else:
            expanded_paths = tuple(flat_items)

    sorting = tuple(dict(s) for s in (state.get("sorting") or []) if isinstance(s, Mapping))
    sort_mode = str(payload.get("sortMode") or ("in_place" if payload.get("lockSortOnStream", True) else "live_sort")).strip().lower()
    if sort_mode not in {"in_place", "live_sort"}:
        sort_mode = "in_place"

    changed_only = bool(payload.get("changedOnly", False))
    sort_by_var = None
    raw_var_sort = payload.get("sortByVariance")
    if isinstance(raw_var_sort, Mapping) and raw_var_sort.get("columnId"):
        sort_by_var = (
            str(raw_var_sort["columnId"]),
            str(raw_var_sort.get("metric") or "delta"),
            bool(raw_var_sort.get("desc", True)),
        )

    return ViewportWindow(
        start_row=max(0, start_row),
        end_row=max(max(0, start_row), end_row),
        col_start=int(col_start) if col_start is not None else None,
        col_end=int(col_end) if col_end is not None else None,
        expanded_paths=expanded_paths,
        sort_specs=sorting,
        sort_mode=sort_mode,
        changed_only=changed_only,
        sort_by_variance=sort_by_var,
    )


class RuntimeSubscriptionService:
    """Host-neutral orchestrator for subscription & baseline requests."""

    def __init__(
        self,
        adapter_getter: Callable[[], Any],
        *,
        authorizer: Optional[Callable[[Any, str], Any]] = None,
    ) -> None:
        self._adapter_getter = adapter_getter
        self._authorizer = authorizer
        self._lock = threading.RLock()
        self._bound_controller_id: Optional[int] = None
        self._source_adapter: Optional[VersionedSourceAdapter] = None
        self._snapshot_provider: Optional[SnapshotProvider] = None
        self._comparison_service: Optional[BaselineComparisonService] = None
        self._coordinator: Optional[ReactiveQueryCoordinator] = None
        self._broker: Optional[SubscriptionBroker] = None

    def _ensure_stack(self) -> Tuple[
        VersionedSourceAdapter,
        SnapshotProvider,
        BaselineComparisonService,
        ReactiveQueryCoordinator,
        SubscriptionBroker,
    ]:
        adapter = self._adapter_getter()
        controller = getattr(adapter, "controller", None)
        if controller is None:
            raise RuntimeError("Adapter has no controller configured for streaming.")
        ctrl_id = id(controller)
        with self._lock:
            if self._broker is not None and self._bound_controller_id == ctrl_id:
                assert self._source_adapter is not None
                assert self._snapshot_provider is not None
                assert self._comparison_service is not None
                assert self._coordinator is not None
                return (
                    self._source_adapter,
                    self._snapshot_provider,
                    self._comparison_service,
                    self._coordinator,
                    self._broker,
                )

            # Reuse adapter-owned streaming stack if already initialized on the adapter.
            existing_stack = getattr(adapter, "_streaming_stack", None)
            if existing_stack is not None:
                self._source_adapter = existing_stack["source_adapter"]
                self._snapshot_provider = existing_stack["snapshot_provider"]
                self._comparison_service = existing_stack["comparison_service"]
                self._coordinator = existing_stack["coordinator"]
                self._broker = SubscriptionBroker(
                    source_adapter=self._source_adapter,
                    snapshot_provider=self._snapshot_provider,
                    coordinator=self._coordinator,
                    authorizer=self._authorizer,
                )
                self._bound_controller_id = ctrl_id
                return (
                    self._source_adapter,
                    self._snapshot_provider,
                    self._comparison_service,
                    self._coordinator,
                    self._broker,
                )

            source_adapter = VersionedSourceAdapter(controller)
            snapshot_provider = SnapshotProvider(controller, source_adapter)
            comparison_service = BaselineComparisonService(controller, snapshot_provider)
            coordinator = ReactiveQueryCoordinator(
                controller, source_adapter, snapshot_provider, comparison_service
            )
            broker = SubscriptionBroker(
                source_adapter=source_adapter,
                snapshot_provider=snapshot_provider,
                coordinator=coordinator,
                authorizer=self._authorizer,
            )
            setattr(
                adapter,
                "_streaming_stack",
                {
                    "source_adapter": source_adapter,
                    "snapshot_provider": snapshot_provider,
                    "comparison_service": comparison_service,
                    "coordinator": coordinator,
                    "broker": broker,
                },
            )
            self._source_adapter = source_adapter
            self._snapshot_provider = snapshot_provider
            self._comparison_service = comparison_service
            self._coordinator = coordinator
            self._broker = broker
            self._bound_controller_id = ctrl_id
            return source_adapter, snapshot_provider, comparison_service, coordinator, broker

    @property
    def broker(self) -> SubscriptionBroker:
        return self._ensure_stack()[4]

    @property
    def source_adapter(self) -> VersionedSourceAdapter:
        return self._ensure_stack()[0]

    @property
    def snapshot_provider(self) -> SnapshotProvider:
        return self._ensure_stack()[1]

    # Actions that address an existing subscription by id (REQ-LIVE-SEC-001).
    _SUBSCRIPTION_ID_ACTIONS = frozenset({
        "poll", "sync", "stream_sync", "ack", "modify_viewport", "set_baseline",
        "update_options", "pause", "resume", "unfreeze", "set_pause_mode",
        "freeze", "unsubscribe",
    })

    @staticmethod
    def _principal_key(principal: Any) -> str:
        for attr in ("id", "subject", "username", "sub"):
            value = getattr(principal, attr, None)
            if value is None and isinstance(principal, Mapping):
                value = principal.get(attr)
            if value not in (None, ""):
                return str(value)
        return ""

    def _require_owner(self, broker: Any, sub_id: str, principal: Any, row_scope: Any) -> None:
        """Only the principal + grant that created a subscription may drive it."""
        sub = broker.get_subscription(sub_id) if sub_id else None
        if sub is None:
            return  # the broker reports unknown/expired ids itself
        same_principal = self._principal_key(sub.principal) == self._principal_key(principal)
        same_scope = (getattr(sub.row_scope, "scope_fingerprint", None) or "global") == (
            getattr(row_scope, "scope_fingerprint", None) or "global"
        )
        if not (same_principal and same_scope):
            raise RuntimeDatasetAuthorizationError("Subscription is not owned by this principal.")

    def handle_command(
        self,
        *,
        table: str,
        action: str,
        payload: Mapping[str, Any],
        state: Mapping[str, Any],
        principal: Any = None,
        row_scope: Any = None,
    ) -> Dict[str, Any]:
        """Dispatch a command with ownership checks and the grant bound to dataset access."""
        from ..core.authority.scope import connections_of, is_protecting, scoped_execution

        _, snapshot_provider, _, _, broker = self._ensure_stack()
        act = str(action or "").strip().lower()
        if act in self._SUBSCRIPTION_ID_ACTIONS:
            self._require_owner(broker, str(payload.get("subscriptionId") or ""), principal, row_scope)
        if act in {"delete_baseline", "clear_baseline"}:
            snap_id = str(payload.get("baselineId") or payload.get("snapshotId") or "")
            if snap_id:
                # Raises SnapshotAuthorizationError for a foreign scope.
                snapshot_provider.get_descriptor(
                    snap_id, principal=principal, row_scope=row_scope, require_unexpired=False
                )
        connections = connections_of(self._adapter_getter()) if is_protecting(row_scope) else ()
        with scoped_execution(row_scope, connections=connections):
            return self._dispatch_command(
                table=table, action=act, payload=payload, state=state,
                principal=principal, row_scope=row_scope,
            )

    def _dispatch_command(
        self,
        *,
        table: str,
        action: str,
        payload: Mapping[str, Any],
        state: Mapping[str, Any],
        principal: Any = None,
        row_scope: Any = None,
    ) -> Dict[str, Any]:
        """Dispatch a subscription or baseline command and return a JSON-safe payload."""
        source_adapter, snapshot_provider, _, _, broker = self._ensure_stack()
        act = str(action or "").strip().lower()

        if act == "subscribe":
            spec = build_pivot_spec_from_subscription_state(table, state)
            viewport = build_viewport_window_from_payload(payload, state)
            baseline_id = payload.get("baselineId") or payload.get("snapshotId")
            known_rev = payload.get("knownRevision")
            _, snapshot_frame = broker.subscribe(
                spec=spec,
                viewport=viewport,
                principal=principal,
                row_scope=row_scope,
                baseline_id=str(baseline_id) if baseline_id else None,
                known_revision=int(known_rev) if known_rev is not None else None,
            )
            return snapshot_frame

        if act in {"poll", "sync", "stream_sync"}:
            sub_id = str(payload.get("subscriptionId") or "")
            ack_seq = payload.get("ackSeq", payload.get("lastAckSeq"))
            client_epoch = payload.get("serverEpochId")
            return broker.poll_updates(
                sub_id,
                ack_seq=int(ack_seq) if ack_seq is not None else None,
                client_server_epoch_id=str(client_epoch) if client_epoch else None,
            )

        if act == "ack":
            sub_id = str(payload.get("subscriptionId") or "")
            ack_seq = int(payload.get("ackSeq", payload.get("seq", 0)) or 0)
            ok = broker.acknowledge(sub_id, ack_seq)
            return {"type": "ACK", "protocolVersion": PROTOCOL_VERSION_V2, "subscriptionId": sub_id, "ackSeq": ack_seq, "ok": ok}

        if act in {"modify_viewport", "set_baseline", "update_options"}:
            sub_id = str(payload.get("subscriptionId") or "")
            viewport = build_viewport_window_from_payload(payload, state)
            update_baseline = "baselineId" in payload or "snapshotId" in payload or act == "set_baseline"
            baseline_id = payload.get("baselineId", payload.get("snapshotId"))
            return broker.modify_viewport(
                sub_id,
                viewport,
                baseline_id=str(baseline_id) if baseline_id else None,
                update_baseline=update_baseline,
            )

        if act in {"pause", "resume", "unfreeze", "set_pause_mode", "freeze"}:
            sub_id = str(payload.get("subscriptionId") or "")
            if act in {"resume", "unfreeze"}:
                mode = "active"
            elif act == "freeze":
                mode = "freeze_snapshot"
            elif act == "pause":
                mode = str(payload.get("pauseMode") or "pause_live")
            else:
                mode = str(payload.get("pauseMode") or payload.get("mode") or "active")
            ttl = payload.get("ttlSeconds")
            return broker.set_pause_mode(
                sub_id,
                mode,
                ttl_seconds=float(ttl) if ttl is not None else None,
            )

        if act == "unsubscribe":
            sub_id = str(payload.get("subscriptionId") or "")
            closed = broker.unsubscribe(sub_id)
            return {"type": "UNSUBSCRIBED", "protocolVersion": PROTOCOL_VERSION_V2, "subscriptionId": sub_id, "closed": closed}

        if act in {"capture_baseline", "capture_snapshot"}:
            kind = "freeze_snapshot" if payload.get("kind") == "freeze_snapshot" else "baseline"
            ttl = payload.get("ttlSeconds")
            desc = snapshot_provider.capture_snapshot(
                table,
                principal=principal,
                row_scope=row_scope,
                label=payload.get("label"),
                kind=kind,
                ttl_seconds=float(ttl) if ttl is not None else None,
                external_table=payload.get("externalTable"),
                time_travel_token=payload.get("timeTravelToken"),
            )
            return {"type": "BASELINE_CAPTURED", "protocolVersion": PROTOCOL_VERSION_V2, "snapshot": desc.to_wire_dict()}

        if act in {"delete_baseline", "clear_baseline"}:
            snap_id = str(payload.get("baselineId") or payload.get("snapshotId") or "")
            deleted = snapshot_provider.delete_snapshot(snap_id) if snap_id else False
            return {"type": "BASELINE_DELETED", "protocolVersion": PROTOCOL_VERSION_V2, "snapshotId": snap_id, "deleted": deleted}

        if act == "list_baselines":
            snaps = snapshot_provider.list_snapshots(table=table, row_scope=row_scope)
            return {
                "type": "BASELINE_LIST",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "table": table,
                "snapshots": [s.to_wire_dict() for s in snaps],
            }

        if act == "push_changes":
            rows = payload.get("rows") or []
            mode = str(payload.get("mode") or "upsert")
            key_fields = payload.get("keyFields") or payload.get("key_fields")
            batch = source_adapter.apply_mutation_batch(
                table,
                rows,
                mode=mode,
                key_fields=key_fields,
                source=str(payload.get("source") or "runtime_push"),
            )
            return {
                "type": "CHANGES_COMMITTED",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "table": table,
                "sourceRevision": batch.source_revision,
                "schemaVersion": batch.schema_version,
                "serverEpochId": batch.server_epoch_id,
                "committedAt": batch.committed_at,
                "changedRows": len(batch.new_rows) + len(batch.old_rows),
            }

        if act == "capabilities":
            caps = source_adapter.get_source_capabilities(table)
            rev, schema_ver = source_adapter.read_coherent_revision(table)
            return {
                "type": "CAPABILITIES",
                "protocolVersion": PROTOCOL_VERSION_V2,
                "serverEpochId": source_adapter.server_epoch_id,
                "sourceRevision": rev,
                "schemaVersion": schema_ver,
                "capabilities": caps.to_wire_dict(),
            }

        raise ValueError(f"Unsupported subscription action: '{action}'.")

    def close(self) -> None:
        with self._lock:
            if self._broker is not None:
                self._broker.close()
            if self._coordinator is not None:
                self._coordinator.close()
            if self._snapshot_provider is not None:
                self._snapshot_provider.close()
            if self._source_adapter is not None:
                self._source_adapter.close()
