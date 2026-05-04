"""Reusable Dash transport callbacks for pivot runtime service."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
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
from .payload_store import RuntimePayloadStore
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


def _extract_request_state_override(request_payload: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(request_payload, dict):
        return None
    override = request_payload.get("state_override")
    if override is None:
        override = request_payload.get("stateOverride")
    return override if isinstance(override, dict) else None


def _state_override_value(
    state_override: Optional[Dict[str, Any]],
    key: str,
    fallback: Any,
    expected_type: Optional[Any] = None,
    *,
    allow_none: bool = False,
) -> Any:
    if not isinstance(state_override, dict) or key not in state_override:
        return fallback
    value = state_override.get(key)
    if value is None:
        return None if allow_none else fallback
    if expected_type is not None and not isinstance(value, expected_type):
        return fallback
    return value


@dataclass
class _RuntimeExportContext:
    request: Any
    expanded: Any
    row_fields: Any
    val_configs: Any


def _build_runtime_export_context(
    *,
    request_payload: Dict[str, Any],
    table_name: Optional[str],
    row_fields: Any,
    col_fields: Any,
    val_configs: Any,
    filters: Any,
    custom_dimensions: Any,
    sorting: Any,
    sort_options: Any,
    sort_options_default: Optional[Dict],
    expanded: Any,
    show_row_totals: bool,
    show_col_totals: bool,
    show_subtotals: bool = True,
) -> _RuntimeExportContext:
    """Build the export request from the same effective state as the visible pivot."""
    from ..tanstack_adapter import TanStackOperation, TanStackRequest

    request_state_override = _extract_request_state_override(request_payload)
    effective_row_fields = _state_override_value(request_state_override, "rowFields", row_fields or [], list)
    effective_col_fields = _state_override_value(request_state_override, "colFields", col_fields or [], list)
    effective_val_configs = _state_override_value(request_state_override, "valConfigs", val_configs or [], list)
    effective_filters = _state_override_value(request_state_override, "filters", filters or {}, dict)
    effective_custom_dimensions = _state_override_value(request_state_override, "customDimensions", custom_dimensions or [], list)
    effective_sorting = _state_override_value(request_state_override, "sorting", sorting or [], list)
    effective_sort_options = _state_override_value(
        request_state_override,
        "sortOptions",
        sort_options or sort_options_default or {},
        dict,
    )
    effective_expanded = _state_override_value(request_state_override, "expanded", expanded, (dict, bool))
    effective_show_row_totals = _state_override_value(
        request_state_override,
        "showRowTotals",
        show_row_totals,
        bool,
    )
    effective_show_col_totals = _state_override_value(
        request_state_override,
        "showColTotals",
        show_col_totals,
        bool,
    )
    effective_show_subtotals = _state_override_value(
        request_state_override,
        "showSubtotals",
        show_subtotals,
        bool,
    )

    export_state = PivotViewState(
        row_fields=effective_row_fields,
        col_fields=effective_col_fields,
        val_configs=effective_val_configs,
        filters=effective_filters,
        custom_dimensions=effective_custom_dimensions,
        sorting=effective_sorting,
        sort_options=effective_sort_options,
        expanded=effective_expanded,
        show_row_totals=effective_show_row_totals,
        show_col_totals=effective_show_col_totals,
        show_subtotals=effective_show_subtotals,
    )
    tanstack_sorting, column_sort_options = PivotRuntimeService._build_tanstack_sorting(export_state)
    request_columns = PivotRuntimeService._build_request_columns(
        effective_row_fields or [],
        effective_col_fields or [],
        effective_val_configs or [],
    )
    resolved_table = table_name or request_payload.get("table")
    effective_filters = effective_filters or {}

    return _RuntimeExportContext(
        request=TanStackRequest(
            operation=TanStackOperation.GET_DATA,
            table=resolved_table,
            columns=request_columns,
            filters=effective_filters,
            custom_dimensions=effective_custom_dimensions or [],
            sorting=tanstack_sorting,
            grouping=effective_row_fields or [],
            aggregations=[],
            pagination={"pageIndex": 0, "pageSize": 10_000_000},
            global_filter=effective_filters.get("global") if isinstance(effective_filters, dict) else None,
            totals=effective_show_col_totals,
            row_totals=effective_show_row_totals,
            include_subtotals=True,  # hierarchical always; flat path gated by layout_mode in adapter
            layout_mode=request_layout_mode,
            show_subtotal_footers=effective_show_subtotals,
            column_sort_options=column_sort_options or None,
        ),
        expanded=effective_expanded,
        row_fields=effective_row_fields,
        val_configs=effective_val_configs,
    )


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
                "export": f"{_normalize_id(pivot_id)}.runtimeRequest",
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


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _runtime_payload_refs_enabled() -> bool:
    return os.environ.get("PIVOT_RUNTIME_PAYLOAD_REFS", "1").lower() not in {"0", "false", "no"}


def _contains_nested_sequence(value: Any, *, depth: int = 0, max_depth: int = 4) -> bool:
    if depth > max_depth:
        return False
    if isinstance(value, (str, bytes, bytearray)):
        return False
    if isinstance(value, (list, tuple)):
        return True
    if isinstance(value, dict):
        return any(_contains_nested_sequence(item, depth=depth + 1, max_depth=max_depth) for item in value.values())
    return False


def _payload_rows_have_nested_values(rows: Any, *, sample_size: int = 50) -> bool:
    if not isinstance(rows, list):
        return False
    for row in rows[:max(0, sample_size)]:
        if isinstance(row, dict) and any(_contains_nested_sequence(value) for value in row.values()):
            return True
    return False


def _get_runtime_payload_store(app: Any) -> RuntimePayloadStore:
    store = getattr(app, "_pivot_runtime_payload_store", None)
    if store is None:
        store = RuntimePayloadStore(
            default_ttl_seconds=_env_int("PIVOT_RUNTIME_PAYLOAD_TTL_SECONDS", 120),
            max_entries=_env_int("PIVOT_RUNTIME_PAYLOAD_MAX_ENTRIES", 256),
            max_bytes=_env_int("PIVOT_RUNTIME_PAYLOAD_MAX_BYTES", 128 * 1024 * 1024),
        )
        setattr(app, "_pivot_runtime_payload_store", store)
    return store


def _register_runtime_payload_endpoint(app: Any) -> None:
    if "_dash_tanstack_pivot_runtime_payload" in getattr(app.server, "view_functions", {}):
        return

    @app.server.route("/_dash_tanstack_pivot/payload/<token>", methods=["GET"], endpoint="_dash_tanstack_pivot_runtime_payload")
    def _dash_tanstack_pivot_runtime_payload(token: str):
        from flask import Response

        store = _get_runtime_payload_store(app)
        item = store.get(token)
        if item is None:
            return Response("payload expired or not found", status=404, content_type="text/plain")

        if item.file_path:
            try:
                file_handle = open(item.file_path, "rb")
            except OSError:
                return Response("payload expired or not found", status=404, content_type="text/plain")

            def _stream_file():
                try:
                    while True:
                        chunk = file_handle.read(1024 * 1024)
                        if not chunk:
                            break
                        yield chunk
                finally:
                    file_handle.close()

            response = Response(_stream_file(), status=200, content_type=item.content_type)
        else:
            response = Response(item.body, status=200, content_type=item.content_type)
        response.headers["Cache-Control"] = "no-store"
        filename = item.metadata.get("filename") if isinstance(item.metadata, dict) else None
        if filename:
            safe_filename = str(filename).replace("\\", "_").replace("/", "_").replace('"', "")
            response.headers["Content-Disposition"] = f'attachment; filename="{safe_filename}"'
        response.headers["Content-Length"] = str(int(item.size or len(item.body or b"")))
        return response


def _externalize_data_payload_if_needed(
    app: Any,
    payload: Dict[str, Any],
    *,
    request_id: Optional[str],
    context: PivotRequestContext,
) -> Dict[str, Any]:
    if not _runtime_payload_refs_enabled() or not isinstance(payload, dict):
        return payload
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        return payload

    min_rows = max(1, _env_int("PIVOT_RUNTIME_PAYLOAD_REF_MIN_ROWS", 500))
    nested_values = _payload_rows_have_nested_values(
        rows,
        sample_size=max(1, _env_int("PIVOT_RUNTIME_PAYLOAD_REF_NESTED_SAMPLE", 50)),
    )
    if len(rows) < min_rows and not nested_values:
        return payload

    store = _get_runtime_payload_store(app)
    payload_ref = store.put_json(
        {"data": rows},
        metadata={
            "requestId": request_id,
            "stateEpoch": context.state_epoch,
            "windowSeq": context.window_seq,
            "clientInstance": context.client_instance,
            "dataVersion": payload.get("dataVersion"),
            "rowCount": payload.get("rowCount"),
            "dataOffset": payload.get("dataOffset"),
        },
    )
    return {
        **payload,
        "data": [],
        "payloadRef": payload_ref,
        "payloadInline": False,
    }


def _externalize_export_payload(
    app: Any,
    payload: Dict[str, Any],
    *,
    request_id: Optional[str],
    context: PivotRequestContext,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    content_path = payload.get("contentPath") or payload.get("content_path")
    if isinstance(content_path, str) and content_path:
        store = _get_runtime_payload_store(app)
        payload_ref = store.put_file(
            content_path,
            content_type=str(payload.get("contentType") or "application/octet-stream"),
            metadata={
                "requestId": request_id,
                "stateEpoch": context.state_epoch,
                "windowSeq": context.window_seq,
                "clientInstance": context.client_instance,
                "filename": payload.get("filename"),
                "exportFormat": payload.get("format"),
                "rowCount": payload.get("rows"),
                "partId": payload.get("partId"),
            },
        )
        return {
            "payloadRef": payload_ref,
            "payloadInline": False,
            "format": payload.get("format"),
            "filename": payload.get("filename"),
            "rows": payload.get("rows"),
            "columns": payload.get("columns"),
            "partId": payload.get("partId"),
        }
    content = payload.get("content")
    if not isinstance(content, (bytes, bytearray)):
        return {
            key: value
            for key, value in payload.items()
            if key != "content"
        }
    store = _get_runtime_payload_store(app)
    payload_ref = store.put_bytes(
        bytes(content),
        content_type=str(payload.get("contentType") or "application/octet-stream"),
        metadata={
            "requestId": request_id,
            "stateEpoch": context.state_epoch,
            "windowSeq": context.window_seq,
            "clientInstance": context.client_instance,
            "filename": payload.get("filename"),
            "exportFormat": payload.get("format"),
            "rowCount": payload.get("rows"),
            "partId": payload.get("partId"),
        },
    )
    return {
        "payloadRef": payload_ref,
        "payloadInline": False,
        "format": payload.get("format"),
        "filename": payload.get("filename"),
        "rows": payload.get("rows"),
        "columns": payload.get("columns"),
        "partId": payload.get("partId"),
    }


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
    _register_runtime_payload_endpoint(app)

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
        Input(pivot_id, "customDimensions"),
        Input(pivot_id, "sorting"),
        Input(pivot_id, "expanded"),
        Input(pivot_id, "immersiveMode"),
        Input(pivot_id, "showRowTotals"),
        Input(pivot_id, "showColTotals"),
        Input(pivot_id, "showSubtotals"),
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
        custom_dimensions,
        sorting,
        expanded,
        immersive_mode,
        show_row_totals,
        show_col_totals,
        show_subtotals,
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
            # When a Dash prop (not runtimeRequest) triggers the callback, the request_id
            # comes from the last runtimeRequest and was already marked as handled by the
            # frontend's deduplication guard. Generate a fresh id so the response is accepted.
            request_id = f"prop-refresh:{triggered_prop}:{int(time.time() * 1000)}"
            if triggered_prop.endswith(".cellUpdate") or triggered_prop.endswith(".cellUpdates"):
                request_kind = "update"
        profiling_enabled = bool(request_payload.get("profile") or request_payload.get("profiling"))
        resolved_show_row_totals = True if show_row_totals is None else bool(show_row_totals)
        resolved_show_col_totals = True if show_col_totals is None else bool(show_col_totals)
        resolved_show_subtotals = True if show_subtotals is None else bool(show_subtotals)

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
            request_state_override = _extract_request_state_override(request_payload)
            effective_row_fields = _state_override_value(request_state_override, "rowFields", row_fields or [], list)
            effective_col_fields = _state_override_value(request_state_override, "colFields", col_fields or [], list)
            effective_val_configs = _state_override_value(request_state_override, "valConfigs", val_configs or [], list)
            effective_filters = _state_override_value(request_state_override, "filters", filters or {}, dict)
            effective_custom_dimensions = _state_override_value(request_state_override, "customDimensions", custom_dimensions or [], list)
            effective_tree_config = _state_override_value(
                request_state_override,
                "treeConfig",
                tree_config if isinstance(tree_config, dict) else None,
                dict,
                allow_none=True,
            )
            search = str(request_payload.get("search") or "").strip()
            try:
                limit = max(1, min(int(request_payload.get("limit", 250)), 500))
            except (TypeError, ValueError):
                limit = 250
            try:
                offset = max(0, int(request_payload.get("offset", 0)))
            except (TypeError, ValueError):
                offset = 0
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

            _debug_print(debug, "[filter-request]", {"columnId": column_id, "table": resolved_table, "search": search, "offset": offset, "limit": limit})

            from ..tanstack_adapter import TanStackOperation, TanStackRequest

            service = runtime_service_getter()
            adapter = service._adapter_getter()
            request_columns = PivotRuntimeService._build_request_columns(
                effective_row_fields or [],
                effective_col_fields or [],
                effective_val_configs or [],
            )
            if isinstance(effective_tree_config, dict):
                for field_name in (
                    effective_tree_config.get("labelField"),
                    effective_tree_config.get("idField"),
                    effective_tree_config.get("parentIdField"),
                    effective_tree_config.get("pathField"),
                ):
                    if isinstance(field_name, str) and field_name and not any(col.get("id") == field_name for col in request_columns):
                        request_columns.append({"id": field_name})
                for field_name in (effective_tree_config.get("valueFields") or []):
                    if isinstance(field_name, str) and field_name and not any(col.get("id") == field_name for col in request_columns):
                        request_columns.append({"id": field_name})
            request = TanStackRequest(
                operation=TanStackOperation.GET_UNIQUE_VALUES,
                table=resolved_table,
                columns=request_columns,
                filters=effective_filters or {},
                custom_dimensions=effective_custom_dimensions or [],
                sorting=[],
                grouping=effective_row_fields or [],
                aggregations=[],
                pagination={"pageIndex": offset // limit if limit else 0, "pageSize": limit, "offset": offset, "search": search},
                global_filter=column_id,
            )
            response = await _await_if_needed(adapter.handle_request(request))
            next_filter_options = [d.get("value") for d in (response.data or [])]
            pagination = response.pagination if isinstance(response.pagination, dict) else {}
            total_options = pagination.get("totalRows", len(next_filter_options))
            has_more = bool(pagination.get("hasMore", False))
            _debug_print(debug, "[filter-response]", {"columnId": column_id, "count": len(next_filter_options), "total": total_options, "hasMore": has_more})
            callback_finished_at = time.perf_counter()
            return _response_tuple(
                _build_runtime_response(
                    kind="filter_options",
                    request_id=request_id,
                    status="ok",
                    payload={
                        "columnId": column_id,
                        "options": next_filter_options,
                        "search": search,
                        "offset": offset,
                        "limit": limit,
                        "total": total_options,
                        "hasMore": has_more,
                    },
                    table=resolved_table,
                    profile={
                        "request": {
                            "requestId": request_id,
                            "kind": "filter_options",
                            "table": resolved_table,
                            "columnId": column_id,
                            "search": search,
                            "offset": offset,
                            "limit": limit,
                        },
                        "callback": {
                            "totalMs": round((callback_finished_at - callback_started_at) * 1000, 3),
                            "executeMs": round((callback_finished_at - filter_started_at) * 1000, 3),
                        },
                    } if profiling_enabled else None,
                ),
            )

        active_request_meta = request_payload
        # When a Dash prop (reportDef, viewMode, etc.) triggers the callback, active_request_meta
        # carries the previous runtimeRequest with intent="viewport" and a stale window_seq.
        # The session gate rejects viewport requests whose window_seq isn't strictly increasing,
        # so prop-triggered refreshes would be silently dropped. Force "structural" so the gate
        # always accepts them.
        if not (isinstance(effective_trigger_prop, str) and effective_trigger_prop.endswith(".runtimeRequest")):
            active_request_meta = {**(active_request_meta if isinstance(active_request_meta, dict) else {}), "intent": "structural"}
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
        # Runtime state_override is a snapshot attached to a specific runtimeRequest.
        # When a normal Dash prop (viewMode, reportDef, rowFields, etc.) triggers this
        # callback, reusing the previous runtimeRequest override can force the backend
        # to process stale mode/fields and leave the UI/data out of sync.
        request_state_override = (
            _extract_request_state_override(request_payload)
            if isinstance(effective_trigger_prop, str) and effective_trigger_prop.endswith(".runtimeRequest")
            else None
        )
        effective_row_fields = _state_override_value(request_state_override, "rowFields", row_fields or [], list)
        effective_col_fields = _state_override_value(request_state_override, "colFields", col_fields or [], list)
        effective_val_configs = _state_override_value(request_state_override, "valConfigs", val_configs or [], list)
        effective_filters = _state_override_value(request_state_override, "filters", filters or {}, dict)
        effective_custom_dimensions = _state_override_value(request_state_override, "customDimensions", custom_dimensions or [], list)
        effective_sorting = _state_override_value(request_state_override, "sorting", sorting or [], list)
        effective_sort_options = _state_override_value(
            request_state_override,
            "sortOptions",
            sort_options or sort_options_default or {},
            dict,
        )
        effective_expanded = _state_override_value(request_state_override, "expanded", expanded, (dict, bool))
        effective_immersive_mode = _state_override_value(request_state_override, "immersiveMode", bool(immersive_mode), bool)
        effective_show_row_totals = _state_override_value(
            request_state_override,
            "showRowTotals",
            resolved_show_row_totals,
            bool,
        )
        effective_show_col_totals = _state_override_value(
            request_state_override,
            "showColTotals",
            resolved_show_col_totals,
            bool,
        )
        effective_show_subtotals = _state_override_value(
            request_state_override,
            "showSubtotals",
            resolved_show_subtotals,
            bool,
        )
        # Read layout_mode from payload — it is local frontend state (not a Dash prop),
        # so it can only arrive via the request payload.
        request_layout_mode = "hierarchy"
        if isinstance(request_payload, dict) and request_payload.get("layout_mode"):
            request_layout_mode = str(request_payload["layout_mode"])
        effective_view_mode = _state_override_value(
            request_state_override,
            "viewMode",
            view_mode if isinstance(view_mode, str) else "pivot",
            str,
        )
        effective_detail_mode = _state_override_value(
            request_state_override,
            "detailMode",
            detail_mode if isinstance(detail_mode, str) else "none",
            str,
        )
        effective_tree_config = _state_override_value(
            request_state_override,
            "treeConfig",
            tree_config if isinstance(tree_config, dict) else None,
            dict,
            allow_none=True,
        )
        effective_detail_config = _state_override_value(
            request_state_override,
            "detailConfig",
            detail_config if isinstance(detail_config, dict) else None,
            dict,
            allow_none=True,
        )
        effective_report_def = _state_override_value(
            request_state_override,
            "reportDef",
            report_def if isinstance(report_def, dict) else None,
            dict,
            allow_none=True,
        )
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
            custom_dimensions=effective_custom_dimensions,
            sorting=effective_sorting,
            sort_options=effective_sort_options,
            expanded=effective_expanded,
            immersive_mode=bool(effective_immersive_mode),
            show_row_totals=effective_show_row_totals,
            show_col_totals=effective_show_col_totals,
            show_subtotals=effective_show_subtotals,
            cell_update=runtime_single_update or (cell_update if isinstance(cell_update, dict) else None),
            cell_updates=(
                runtime_batch_updates
                if runtime_batch_updates
                else [update_payload for update_payload in (cell_updates or []) if isinstance(update_payload, dict)]
            ),
            transaction_request=runtime_transaction_request,
            drill_through=request_payload if request_kind == "drill" and isinstance(request_payload, dict) else None,
            detail_request=request_payload if request_kind == "detail" and isinstance(request_payload, dict) else None,
            export_request=request_payload if request_kind == "export" and isinstance(request_payload, dict) else {},
            viewport=request_payload if isinstance(request_payload, dict) else {},
            chart_request=request_payload if request_kind == "chart" and isinstance(request_payload, dict) else {},
            view_mode=effective_view_mode,
            detail_mode=effective_detail_mode,
            tree_config=effective_tree_config,
            detail_config=effective_detail_config,
            report_def=effective_report_def,
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
                "custom_dimensions": len(state.custom_dimensions or []),
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

        if result.status == "export":
            export_payload = _externalize_export_payload(
                app,
                result.export_payload or {},
                request_id=request_id,
                context=context,
            )
            return _response_tuple(
                _build_runtime_response(
                    kind="export",
                    request_id=request_id,
                    status="ok",
                    payload=export_payload,
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
            data_payload = {
                "data": data_out,
                "rowCount": result.total_rows if result.total_rows is not None else 0,
                "columns": columns_out if columns_out is not no_update else None,
                "colSchema": result.col_schema,
                "dataOffset": result.data_offset,
                "dataVersion": result.data_version,
                "transaction": result.transaction_result,
                "editOverlay": result.edit_overlay,
                "formulaErrors": result.formula_errors or None,
            }
            data_payload = _externalize_data_payload_if_needed(
                app,
                data_payload,
                request_id=request_id,
                context=context,
            )
            return _response_tuple(
                _build_runtime_response(
                    kind="transaction" if request_kind == "transaction" and result.transaction_result else "data",
                    request_id=request_id,
                    status="data",
                    payload=data_payload,
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
