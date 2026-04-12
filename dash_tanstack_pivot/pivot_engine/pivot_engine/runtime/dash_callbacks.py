"""Reusable Dash transport callbacks for pivot runtime service."""

from __future__ import annotations

import asyncio
import inspect
import json
import traceback
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Set

try:
    import dash
    from dash import Input, Output, State, no_update
except ImportError:
    dash = None
    Input = Output = State = no_update = None

from .models import PivotRequestContext, PivotViewState
from .async_bridge import run_awaitable_sync
from .service import PivotRuntimeService


def _normalize_id(value: Any) -> str:
    """Create a stable string key for string/dict Dash ids."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return str(value)
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    except TypeError:  # pragma: no cover - defensive fallback
        return repr(value)


def _get_callback_registry(app: Any) -> Set[str]:
    """Get (or initialize) a registry of callbacks already registered on app."""
    registry = getattr(app, "_pivot_engine_callback_registry", None)
    if registry is None:
        registry = set()
        setattr(app, "_pivot_engine_callback_registry", registry)
    return registry


def _debug_print(enabled: bool, *parts: Any) -> None:
    if enabled:
        try:
            print(*parts, flush=True)
        except OSError:
            try:
                print(*parts)
            except OSError:
                pass


async def _await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _is_bootstrap_without_viewport(triggered_prop: Optional[str], viewport: Any) -> bool:
    """
    Detect an initial Dash callback call that has no concrete viewport/session
    metadata from the frontend yet.

    Dash commonly fires the first callback because static props like rowFields
    are present in layout, before the React component has emitted its first
    runtimeRequest. In that case we still want a real bootstrap data response,
    not a stale no-op keyed to anonymous/default window_seq=0 metadata.
    """
    if isinstance(triggered_prop, str) and triggered_prop.endswith(".viewport") and not isinstance(viewport, dict):
        return False
    if not isinstance(viewport, dict):
        return True
    return not (
        viewport.get("start") is not None
        and viewport.get("end") is not None
        and viewport.get("session_id")
        and viewport.get("client_instance")
    )


def _get_triggered_props(ctx: Any) -> list[str]:
    triggered = getattr(ctx, "triggered", None) or []
    props: list[str] = []
    for item in triggered:
        prop_id = item.get("prop_id")
        if prop_id:
            props.append(str(prop_id))
    return props


def _extract_request_id(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    request_id = (
        payload.get("requestId")
        or payload.get("request_id")
        or payload.get("request_signature")
        or payload.get("window_seq")
        or payload.get("version")
    )
    return str(request_id) if request_id is not None else None


def _coerce_runtime_request_payload(runtime_request: Dict[str, Any]) -> Dict[str, Any]:
    payload = runtime_request.get("payload")
    if isinstance(payload, dict):
        return payload
    return {
        key: value
        for key, value in runtime_request.items()
        if key
        not in {
            "kind",
            "requestId",
            "table",
            "session_id",
            "client_instance",
            "state_epoch",
            "window_seq",
            "abort_generation",
        }
    }


def _merge_profiles(*profiles: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    merged: Dict[str, Any] = {}
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        for key, value in profile.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = {**merged[key], **value}
            else:
                merged[key] = value
    return merged or None


def _normalize_transport_request(
    *,
    pivot_id: Any,
    runtime_request: Any,
) -> Dict[str, Any]:
    if isinstance(runtime_request, dict):
        kind = str(runtime_request.get("kind") or "data").strip().lower() or "data"
        kind = {
            "batch_update": "update",
            "cell_updates": "update",
        }.get(kind, kind)
        payload = _coerce_runtime_request_payload(runtime_request)
        request_id = _extract_request_id(runtime_request) or _extract_request_id(payload)
        return {
            "kind": kind,
            "request_id": request_id,
            "payload": payload,
            "trigger_prop": {
                "filter_options": f"{_normalize_id(pivot_id)}.filterRequest",
                "chart": f"{_normalize_id(pivot_id)}.chartRequest",
                "drill": f"{_normalize_id(pivot_id)}.drillThrough",
                "detail": f"{_normalize_id(pivot_id)}.detailRequest",
                "update": f"{_normalize_id(pivot_id)}.cellUpdates",
                "transaction": f"{_normalize_id(pivot_id)}.runtimeRequest",
            }.get(kind, f"{_normalize_id(pivot_id)}.viewport"),
        }
    return {
        "kind": "data",
        "request_id": None,
        "payload": {},
        "trigger_prop": f"{_normalize_id(pivot_id)}.viewport",
    }


def _build_runtime_response(
    *,
    kind: str,
    request_id: Optional[str],
    status: str,
    payload: Dict[str, Any],
    table: Optional[str] = None,
    session_id: Optional[str] = None,
    client_instance: Optional[str] = None,
    state_epoch: Optional[int] = None,
    window_seq: Optional[int] = None,
    message: Optional[str] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    response: Dict[str, Any] = {
        "kind": kind,
        "status": status,
        "payload": payload,
    }
    if request_id is not None:
        response["requestId"] = request_id
    if table is not None:
        response["table"] = table
    if session_id is not None:
        response["session_id"] = session_id
    if client_instance is not None:
        response["client_instance"] = client_instance
    if state_epoch is not None:
        response["state_epoch"] = state_epoch
    if window_seq is not None:
        response["window_seq"] = window_seq
    if message:
        response["message"] = message
    if isinstance(profile, dict) and profile:
        response["profile"] = profile
    return response


def _format_transport_callback_output(
    include_drill_store: bool,
    runtime_response_payload: Any,
    drill_payload: Any = no_update,
):
    if not include_drill_store:
        return runtime_response_payload
    return (runtime_response_payload, drill_payload)


@dataclass
class DashPivotInstanceConfig:
    """IDs that define one pivot instance wiring in a Dash app."""

    pivot_id: Any
    drill_store_id: Optional[Any] = None
    drill_modal_id: Optional[Any] = None
    drill_table_id: Optional[Any] = None
    close_drill_id: Optional[Any] = None


def register_dash_pivot_transport_callback(
    app: Any,
    runtime_service_getter: Callable[[], Any],
    *,
    pivot_id: Any,
    drill_store_id: Optional[Any] = None,
    debug: bool = False,
    sort_options_default: Optional[Dict] = None,
) -> bool:
    """
    Register the main Dash -> runtime transport callback for one pivot component.

    Returns True when the callback is newly registered, False when it already existed.
    """
    registry = _get_callback_registry(app)
    callback_key = f"pivot::{_normalize_id(pivot_id)}::{_normalize_id(drill_store_id)}"
    if callback_key in registry:
        return False

    include_drill_store = drill_store_id is not None

    outputs = [Output(pivot_id, "runtimeResponse")]
    if include_drill_store:
        outputs.append(Output(drill_store_id, "data"))

    def _response_tuple(runtime_response_payload, drill_payload=no_update):
        return _format_transport_callback_output(
            include_drill_store,
            runtime_response_payload,
            drill_payload,
        )

    callback_args = (
        *outputs,
        Input(pivot_id, "rowFields"),
        Input(pivot_id, "colFields"),
        Input(pivot_id, "valConfigs"),
        Input(pivot_id, "filters"),
        Input(pivot_id, "sorting"),
        Input(pivot_id, "expanded"),
        Input(pivot_id, "immersiveMode"),
        Input(pivot_id, "showRowTotals"),
        Input(pivot_id, "showColTotals"),
        Input(pivot_id, "cellUpdate"),
        Input(pivot_id, "cellUpdates"),
        Input(pivot_id, "runtimeRequest"),
        Input(pivot_id, "viewMode"),
        Input(pivot_id, "detailMode"),
        Input(pivot_id, "treeConfig"),
        Input(pivot_id, "detailConfig"),
        Input(pivot_id, "reportDef"),
        State(pivot_id, "table"),
        State(pivot_id, "sortOptions"),
    )

    async def _update_pivot_table_async(
        row_fields,
        col_fields,
        val_configs,
        filters,
        sorting,
        expanded,
        immersive_mode,
        show_row_totals,
        show_col_totals,
        cell_update,
        cell_updates,
        runtime_request,
        view_mode,
        detail_mode,
        tree_config,
        detail_config,
        report_def,
        table_name,
        sort_options,
    ):
        """Main transport adapter callback for one pivot instance."""
        callback_started_at = time.perf_counter()
        ctx = dash.callback_context
        triggered_props = _get_triggered_props(ctx)
        triggered_prop = triggered_props[0] if triggered_props else None
        transport_request = _normalize_transport_request(
            pivot_id=pivot_id,
            runtime_request=runtime_request,
        )
        request_kind = transport_request["kind"]
        request_id = transport_request.get("request_id")
        request_payload = transport_request.get("payload") if isinstance(transport_request.get("payload"), dict) else {}
        synthetic_trigger_prop = transport_request.get("trigger_prop")
        effective_trigger_prop = synthetic_trigger_prop or triggered_prop
        if isinstance(triggered_prop, str) and not triggered_prop.endswith(".runtimeRequest"):
            effective_trigger_prop = triggered_prop
            if triggered_prop.endswith(".cellUpdate") or triggered_prop.endswith(".cellUpdates"):
                request_kind = "update"
        profiling_enabled = bool(request_payload.get("profile") or request_payload.get("profiling"))
        resolved_show_row_totals = True if show_row_totals is None else bool(show_row_totals)
        resolved_show_col_totals = True if show_col_totals is None else bool(show_col_totals)

        if request_kind == "data" and _is_bootstrap_without_viewport(triggered_prop, request_payload):
            _debug_print(
                debug,
                "Bootstrap request without viewport/session context detected; using fallback bootstrap viewport.",
            )
            bootstrap_request_id = f"bootstrap:{_normalize_id(pivot_id)}:1"
            request_payload = {
                "requestId": bootstrap_request_id,
                "start": 0,
                "end": 99,
                "count": 100,
                "window_seq": 1,
                "state_epoch": 0,
                "abort_generation": 0,
                "session_id": f"bootstrap::{_normalize_id(pivot_id)}",
                "client_instance": "bootstrap",
                "intent": "structural",
                "needs_col_schema": True,
                # Keep first-load row semantics aligned with frontend totals mode.
                "include_grand_total": resolved_show_col_totals,
            }
            request_id = request_id or bootstrap_request_id
            if triggered_prop is None:
                triggered_prop = f"{_normalize_id(pivot_id)}.bootstrap"

        if request_kind == "filter_options":
            column_id = request_payload.get("columnId") or request_payload.get("column_id")
            resolved_table = table_name or request_payload.get("table")
            filter_started_at = time.perf_counter()
            if not column_id or not resolved_table:
                callback_finished_at = time.perf_counter()
                return _response_tuple(
                    _build_runtime_response(
                        kind="filter_options",
                        request_id=request_id,
                        status="error",
                        payload={"columnId": column_id, "options": []},
                        table=resolved_table,
                        message="Missing filter request column or table.",
                        profile={
                            "callback": {
                                "totalMs": round((callback_finished_at - callback_started_at) * 1000, 3),
                                "executeMs": round((callback_finished_at - filter_started_at) * 1000, 3),
                            }
                        } if profiling_enabled else None,
                    ),
                )

            _debug_print(debug, "[filter-request]", {"columnId": column_id, "table": resolved_table})

            from ..tanstack_adapter import TanStackOperation, TanStackRequest

            service = runtime_service_getter()
            adapter = service._adapter_getter()
            request_columns = PivotRuntimeService._build_request_columns(
                row_fields or [],
                col_fields or [],
                val_configs or [],
            )
            if isinstance(tree_config, dict):
                for field_name in (
                    tree_config.get("labelField"),
                    tree_config.get("idField"),
                    tree_config.get("parentIdField"),
                    tree_config.get("pathField"),
                ):
                    if isinstance(field_name, str) and field_name and not any(col.get("id") == field_name for col in request_columns):
                        request_columns.append({"id": field_name})
                for field_name in (tree_config.get("valueFields") or []):
                    if isinstance(field_name, str) and field_name and not any(col.get("id") == field_name for col in request_columns):
                        request_columns.append({"id": field_name})
            request = TanStackRequest(
                operation=TanStackOperation.GET_UNIQUE_VALUES,
                table=resolved_table,
                columns=request_columns,
                filters=filters or {},
                sorting=[],
                grouping=row_fields or [],
                aggregations=[],
                pagination={"pageIndex": 0, "pageSize": 1000},
                global_filter=column_id,
            )
            response = await _await_if_needed(adapter.handle_request(request))
            next_filter_options = [d.get("value") for d in (response.data or [])]
            _debug_print(debug, "[filter-response]", {"columnId": column_id, "count": len(next_filter_options)})
            callback_finished_at = time.perf_counter()
            return _response_tuple(
                _build_runtime_response(
                    kind="filter_options",
                    request_id=request_id,
                    status="ok",
                    payload={
                        "columnId": column_id,
                        "options": next_filter_options,
                    },
                    table=resolved_table,
                    profile={
                        "request": {
                            "requestId": request_id,
                            "kind": "filter_options",
                            "table": resolved_table,
                            "columnId": column_id,
                        },
                        "callback": {
                            "totalMs": round((callback_finished_at - callback_started_at) * 1000, 3),
                            "executeMs": round((callback_finished_at - filter_started_at) * 1000, 3),
                        },
                    } if profiling_enabled else None,
                ),
            )

        active_request_meta = request_payload
        viewport_table = request_payload.get("table") if isinstance(request_payload, dict) else None
        request_table = active_request_meta.get("table") if isinstance(active_request_meta, dict) else None
        resolved_table = table_name or request_table or viewport_table
        if not resolved_table:
            _debug_print(debug, "Missing table in request context; skipping update.")
            return _response_tuple(
                _build_runtime_response(
                    kind=request_kind,
                    request_id=request_id,
                    status="error",
                    payload={},
                    message="Missing table in request context.",
                ),
            )

        context = PivotRequestContext.from_frontend(
            table=resolved_table,
            trigger_prop=effective_trigger_prop,
            viewport=active_request_meta,
        )
        chart_state_override = (
            request_payload.get("state_override")
            if request_kind == "chart" and isinstance(request_payload, dict)
            else None
        )
        effective_row_fields = chart_state_override.get("rowFields") if isinstance(chart_state_override, dict) and isinstance(chart_state_override.get("rowFields"), list) else (row_fields or [])
        effective_col_fields = chart_state_override.get("colFields") if isinstance(chart_state_override, dict) and isinstance(chart_state_override.get("colFields"), list) else (col_fields or [])
        effective_val_configs = chart_state_override.get("valConfigs") if isinstance(chart_state_override, dict) and isinstance(chart_state_override.get("valConfigs"), list) else (val_configs or [])
        effective_filters = chart_state_override.get("filters") if isinstance(chart_state_override, dict) and isinstance(chart_state_override.get("filters"), dict) else (filters or {})
        effective_sorting = chart_state_override.get("sorting") if isinstance(chart_state_override, dict) and isinstance(chart_state_override.get("sorting"), list) else (sorting or [])
        effective_sort_options = chart_state_override.get("sortOptions") if isinstance(chart_state_override, dict) and isinstance(chart_state_override.get("sortOptions"), dict) else (sort_options or sort_options_default or {})
        effective_expanded = chart_state_override.get("expanded") if isinstance(chart_state_override, dict) else expanded
        effective_show_row_totals = chart_state_override.get("showRowTotals") if isinstance(chart_state_override, dict) and chart_state_override.get("showRowTotals") is not None else resolved_show_row_totals
        effective_show_col_totals = chart_state_override.get("showColTotals") if isinstance(chart_state_override, dict) and chart_state_override.get("showColTotals") is not None else resolved_show_col_totals
        runtime_single_update = None
        runtime_batch_updates = []
        runtime_transaction_request = None
        if request_kind in {"update", "transaction"} and isinstance(request_payload, dict):
            if isinstance(request_payload.get("update"), dict):
                runtime_single_update = request_payload.get("update")
            elif request_payload.get("rowId") is not None and request_payload.get("colId") is not None:
                runtime_single_update = request_payload
            batch_source = request_payload.get("updates")
            if batch_source is None:
                batch_source = request_payload.get("cellUpdates")
            if isinstance(batch_source, list):
                runtime_batch_updates = [
                    update_payload
                    for update_payload in batch_source
                    if isinstance(update_payload, dict)
                ]
            transaction_markers = (
                request_kind == "transaction"
                or any(
                    marker in request_payload
                    for marker in ("add", "remove", "upsert", "operations", "refreshMode", "refresh_mode", "keyFields", "key_fields")
                )
            )
            if transaction_markers:
                runtime_transaction_request = request_payload
        state = PivotViewState(
            row_fields=effective_row_fields,
            col_fields=effective_col_fields,
            val_configs=effective_val_configs,
            filters=effective_filters,
            sorting=effective_sorting,
            sort_options=effective_sort_options,
            expanded=effective_expanded,
            immersive_mode=bool(immersive_mode),
            show_row_totals=effective_show_row_totals,
            show_col_totals=effective_show_col_totals,
            cell_update=runtime_single_update or (cell_update if isinstance(cell_update, dict) else None),
            cell_updates=(
                runtime_batch_updates
                if runtime_batch_updates
                else [update_payload for update_payload in (cell_updates or []) if isinstance(update_payload, dict)]
            ),
            transaction_request=runtime_transaction_request,
            drill_through=request_payload if request_kind == "drill" and isinstance(request_payload, dict) else None,
            detail_request=request_payload if request_kind == "detail" and isinstance(request_payload, dict) else None,
            viewport=request_payload if isinstance(request_payload, dict) else {},
            chart_request=request_payload if request_kind == "chart" and isinstance(request_payload, dict) else {},
            view_mode=view_mode if isinstance(view_mode, str) else "pivot",
            detail_mode=detail_mode if isinstance(detail_mode, str) else "none",
            tree_config=tree_config if isinstance(tree_config, dict) else None,
            detail_config=detail_config if isinstance(detail_config, dict) else None,
            report_def=report_def if isinstance(report_def, dict) else None,
        )
        state_built_at = time.perf_counter()

        _debug_print(
            debug,
            "[pivot-request]",
            {
                "trigger": triggered_prop,
                "request_kind": request_kind,
                "view_mode": state.view_mode,
                "table": context.table,
                "session_id": context.session_id,
                "client_instance": context.client_instance,
                "state_epoch": context.state_epoch,
                "window_seq": context.window_seq,
                "abort_generation": context.abort_generation,
                "intent": context.intent,
                "viewport_active": context.viewport_active,
                "start_row": context.start_row,
                "end_row": context.end_row,
                "col_start": context.col_start,
                "col_end": context.col_end,
                "needs_col_schema": context.needs_col_schema,
                "include_grand_total": context.include_grand_total,
                "chart_request": request_payload if request_kind == "chart" and isinstance(request_payload, dict) else None,
                "detail_request": request_payload if request_kind == "detail" and isinstance(request_payload, dict) else None,
                "row_fields": state.row_fields,
                "sorting": state.sorting,
                "sort_options": state.sort_options,
                "sort_options_default_used": bool(
                    sort_options_default and not sort_options
                ),
            },
        )

        try:
            service = runtime_service_getter()
            process_async = getattr(service, "process_async", None)
            if callable(process_async):
                result = await _await_if_needed(process_async(state, context))
            else:
                result = await asyncio.to_thread(service.process, state, context)
            service_finished_at = time.perf_counter()
        except Exception as exc:  # pragma: no cover - defensive
            service_finished_at = time.perf_counter()
            _debug_print(debug, f"Error in pivot runtime service: {exc}")
            if debug:
                _debug_print(debug, traceback.format_exc())
            return _response_tuple(
                _build_runtime_response(
                    kind=request_kind,
                    request_id=request_id,
                    status="error",
                    payload={},
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    message=str(exc),
                    profile={
                        "callback": {
                            "totalMs": round((service_finished_at - callback_started_at) * 1000, 3),
                            "normalizeMs": round((state_built_at - callback_started_at) * 1000, 3),
                            "serviceMs": round((service_finished_at - state_built_at) * 1000, 3),
                        }
                    } if profiling_enabled else None,
                ),
            )

        callback_finished_at = time.perf_counter()
        merged_profile = _merge_profiles(
            result.profile,
            {
                "callback": {
                    "totalMs": round((callback_finished_at - callback_started_at) * 1000, 3),
                    "normalizeMs": round((state_built_at - callback_started_at) * 1000, 3),
                    "serviceMs": round((service_finished_at - state_built_at) * 1000, 3),
                    "responseBuildMs": round((callback_finished_at - service_finished_at) * 1000, 3),
                }
            } if profiling_enabled else None,
        )

        if debug:
            data_rows = result.data or []
            first_row = data_rows[0] if data_rows and isinstance(data_rows[0], dict) else {}
            first_row_keys = list(first_row.keys())[:12] if first_row else []
            columns_sample = []
            col_schema_total = None
            col_schema_sample = []
            if isinstance(result.columns, list):
                for col in result.columns[:8]:
                    if isinstance(col, dict):
                        columns_sample.append(col.get("id"))
                    else:  # pragma: no cover - defensive
                        columns_sample.append(str(col))
            if isinstance(result.col_schema, dict):
                col_schema_total = result.col_schema.get("total_center_cols")
                for item in (result.col_schema.get("columns") or [])[:8]:
                    if isinstance(item, dict):
                        col_schema_sample.append(item.get("id"))
            _debug_print(
                debug,
                "[pivot-response]",
                {
                    "status": result.status,
                    "session_id": context.session_id,
                    "client_instance": context.client_instance,
                    "state_epoch": context.state_epoch,
                    "window_seq": context.window_seq,
                    "intent": context.intent,
                    "original_intent": context.original_intent,
                    "start_row": context.start_row,
                    "end_row": context.end_row,
                    "col_start": context.col_start,
                    "col_end": context.col_end,
                    "needs_col_schema": context.needs_col_schema,
                    "include_grand_total": context.include_grand_total,
                    "rows": len(data_rows),
                    "total_rows": result.total_rows,
                    "data_offset": result.data_offset,
                    "data_version": result.data_version,
                    "columns_emitted": result.columns is not None,
                    "columns_count": len(result.columns or []),
                    "columns_sample": columns_sample,
                    "col_schema_total": col_schema_total,
                    "col_schema_sample": col_schema_sample,
                    "first_row_path": first_row.get("_path") if first_row else None,
                    "first_row_keys": first_row_keys,
                    "message": result.message,
                },
            )

        if result.status == "stale":
            return _response_tuple(
                _build_runtime_response(
                    kind=request_kind,
                    request_id=request_id,
                    status="stale",
                    payload={},
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    profile=merged_profile,
                ),
            )

        if result.status == "drillthrough":
            return _response_tuple(
                _build_runtime_response(
                    kind="drill",
                    request_id=request_id,
                    status="drillthrough",
                    payload=result.drill_payload or {"rows": result.drill_records or []},
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    profile=merged_profile,
                ),
                (result.drill_records or []),
            )

        if result.status == "detail_data":
            return _response_tuple(
                _build_runtime_response(
                    kind="detail",
                    request_id=request_id,
                    status="detail_data",
                    payload=result.detail_payload or {},
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    profile=merged_profile,
                ),
            )

        if result.status == "chart_data":
            return _response_tuple(
                _build_runtime_response(
                    kind="chart",
                    request_id=request_id,
                    status="chart_data",
                    payload=result.chart_data or {},
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    profile=merged_profile,
                ),
            )

        if result.status == "data":
            columns_out = result.columns if result.columns is not None else no_update
            data_out = list(result.data or [])
            if result.color_scale_stats is not None:
                # Prepend a sentinel row the frontend strips before rendering
                data_out = [{"_path": "__color_scale_stats__", "_colorScaleStats": result.color_scale_stats}] + data_out
            return _response_tuple(
                _build_runtime_response(
                    kind="transaction" if request_kind == "transaction" and result.transaction_result else "data",
                    request_id=request_id,
                    status="data",
                    payload={
                        "data": data_out,
                        "rowCount": result.total_rows if result.total_rows is not None else 0,
                        "columns": columns_out if columns_out is not no_update else None,
                        "colSchema": result.col_schema,
                        "dataOffset": result.data_offset,
                        "dataVersion": result.data_version,
                        "transaction": result.transaction_result,
                        "editOverlay": result.edit_overlay,
                    },
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    profile=merged_profile,
                ),
            )

        if result.status == "transaction_applied":
            return _response_tuple(
                _build_runtime_response(
                    kind="transaction",
                    request_id=request_id,
                    status="transaction_applied",
                    payload={
                        "transaction": result.transaction_result or {},
                    },
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    profile=merged_profile,
                ),
            )

        if result.status == "patched":
            return _response_tuple(
                _build_runtime_response(
                    kind="transaction",
                    request_id=request_id,
                    status="patched",
                    payload={
                        "patch": result.patch_payload or {},
                        "dataOffset": result.data_offset,
                        "dataVersion": result.data_version,
                        "transaction": result.transaction_result or {},
                        "editOverlay": result.edit_overlay,
                    },
                    table=resolved_table,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                    state_epoch=context.state_epoch,
                    window_seq=context.window_seq,
                    profile=merged_profile,
                ),
            )

        if result.message:
            _debug_print(debug, f"Error in pivot engine: {result.message}")
        return _response_tuple(
            _build_runtime_response(
                kind=request_kind,
                request_id=request_id,
                status="error",
                payload={},
                table=resolved_table,
                session_id=context.session_id,
                client_instance=context.client_instance,
                state_epoch=context.state_epoch,
                window_seq=context.window_seq,
                message=result.message,
                profile=merged_profile,
            ),
        )

    if getattr(app, "_use_async", False):
        @app.callback(*callback_args)
        async def _update_pivot_table(*callback_values):
            return await _update_pivot_table_async(*callback_values)
    else:
        @app.callback(*callback_args)
        def _update_pivot_table(*callback_values):
            return run_awaitable_sync(_update_pivot_table_async(*callback_values))

    registry.add(callback_key)
    return True


def register_dash_drill_modal_callback(
    app: Any,
    *,
    drill_store_id: Any,
    close_drill_id: Any,
    drill_modal_id: Any,
    drill_table_id: Any,
) -> bool:
    """
    Register drill modal callback for one pivot instance.

    Returns True when newly registered, False when already registered.
    """
    registry = _get_callback_registry(app)
    callback_key = (
        f"drill::{_normalize_id(drill_store_id)}::{_normalize_id(close_drill_id)}::"
        f"{_normalize_id(drill_modal_id)}::{_normalize_id(drill_table_id)}"
    )
    if callback_key in registry:
        return False

    @app.callback(
        Output(drill_modal_id, "style"),
        Output(drill_table_id, "data"),
        Output(drill_table_id, "columns"),
        Input(drill_store_id, "data"),
        Input(close_drill_id, "n_clicks"),
        prevent_initial_call=True,
    )
    def _toggle_drill_modal(drill_data, n_clicks):
        ctx = dash.callback_context
        if not ctx.triggered:
            return no_update

        trigger = ctx.triggered[0]["prop_id"]

        if trigger.endswith(".n_clicks"):
            return {"display": "none"}, [], []

        if drill_data:
            columns = [{"name": key, "id": key} for key in drill_data[0].keys()] if drill_data else []
            return {
                "display": "block",
                "position": "fixed",
                "zIndex": 10002,
                "left": 0,
                "top": 0,
                "width": "100%",
                "height": "100%",
                "backgroundColor": "rgba(0,0,0,0.5)",
            }, drill_data, columns

        return {"display": "none"}, [], []

    registry.add(callback_key)
    return True


def register_dash_callbacks_for_instances(
    app: Any,
    runtime_service_getter: Callable[[], Any],
    instances: Iterable[DashPivotInstanceConfig],
    *,
    debug: bool = False,
) -> Dict[str, bool]:
    """
    Register callbacks for multiple instances.

    Returns a dict of normalized pivot id -> registration status for transport callback.
    """
    status: Dict[str, bool] = {}
    for instance in instances:
        registered = register_dash_pivot_transport_callback(
            app,
            runtime_service_getter,
            pivot_id=instance.pivot_id,
            drill_store_id=instance.drill_store_id,
            debug=debug,
        )
        status[_normalize_id(instance.pivot_id)] = registered

        if (
            instance.drill_modal_id is not None
            and instance.drill_table_id is not None
            and instance.close_drill_id is not None
        ):
            register_dash_drill_modal_callback(
                app,
                drill_store_id=instance.drill_store_id,
                close_drill_id=instance.close_drill_id,
                drill_modal_id=instance.drill_modal_id,
                drill_table_id=instance.drill_table_id,
            )
    return status
