"""Reusable Dash transport callbacks for pivot runtime service."""

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Set

try:
    import dash
    from dash import Input, Output, State, no_update
except ImportError:
    dash = None
    Input = Output = State = no_update = None

from .models import PivotRequestContext, PivotViewState


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


def _is_bootstrap_without_viewport(triggered_prop: Optional[str], viewport: Any) -> bool:
    """
    Detect the initial Dash bootstrap callback call that has no triggering prop
    and no concrete viewport metadata from the frontend.
    """
    if triggered_prop is not None:
        return False
    if not isinstance(viewport, dict):
        return True
    return not (
        viewport.get("start") is not None
        and viewport.get("end") is not None
        and viewport.get("session_id")
        and viewport.get("client_instance")
    )


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

    outputs = [
        Output(pivot_id, "data"),
        Output(pivot_id, "rowCount"),
        Output(pivot_id, "columns"),
        Output(pivot_id, "filterOptions"),
        Output(pivot_id, "chartData"),
    ]
    if include_drill_store:
        outputs.append(Output(drill_store_id, "data"))
    outputs.extend(
        [
            Output(pivot_id, "dataOffset"),
            Output(pivot_id, "dataVersion"),
        ]
    )

    def _response_tuple(
        data_payload,
        row_count_payload,
        columns_payload,
        filter_options_payload,
        chart_data_payload,
        drill_payload,
        data_offset_payload,
        data_version_payload,
    ):
        response = [
            data_payload,
            row_count_payload,
            columns_payload,
            filter_options_payload,
            chart_data_payload,
        ]
        if include_drill_store:
            response.append(drill_payload)
        response.extend([data_offset_payload, data_version_payload])
        return tuple(response)

    @app.callback(
        *outputs,
        Input(pivot_id, "rowFields"),
        Input(pivot_id, "colFields"),
        Input(pivot_id, "valConfigs"),
        Input(pivot_id, "filters"),
        Input(pivot_id, "sorting"),
        Input(pivot_id, "expanded"),
        Input(pivot_id, "showRowTotals"),
        Input(pivot_id, "showColTotals"),
        Input(pivot_id, "cellUpdate"),
        Input(pivot_id, "drillThrough"),
        Input(pivot_id, "chartRequest"),
        Input(pivot_id, "viewport"),
        State(pivot_id, "table"),
        State(pivot_id, "sortOptions"),
        State(pivot_id, "columns"),
        State(pivot_id, "filterOptions"),
    )
    def _update_pivot_table(
        row_fields,
        col_fields,
        val_configs,
        filters,
        sorting,
        expanded,
        show_row_totals,
        show_col_totals,
        cell_update,
        drill_through,
        chart_request,
        viewport,
        table_name,
        sort_options,
        current_columns,
        current_filter_options,
    ):
        """Main transport adapter callback for one pivot instance."""
        ctx = dash.callback_context
        triggered_prop = ctx.triggered[0]["prop_id"] if ctx.triggered else None
        # Component defaults are true for totals, but Dash can provide None until
        # the prop is explicitly sent from frontend. Treat None as default true.
        resolved_show_row_totals = True if show_row_totals is None else bool(show_row_totals)
        resolved_show_col_totals = True if show_col_totals is None else bool(show_col_totals)

        if _is_bootstrap_without_viewport(triggered_prop, viewport):
            _debug_print(
                debug,
                "Bootstrap request without viewport/session context detected; using fallback bootstrap viewport.",
            )
            viewport = {
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
            if triggered_prop is None:
                triggered_prop = f"{_normalize_id(pivot_id)}.bootstrap"

        active_request_meta = (
            chart_request
            if triggered_prop and triggered_prop.endswith(".chartRequest") and isinstance(chart_request, dict)
            else viewport
        )
        viewport_table = viewport.get("table") if isinstance(viewport, dict) else None
        request_table = active_request_meta.get("table") if isinstance(active_request_meta, dict) else None
        resolved_table = table_name or request_table or viewport_table
        if not resolved_table:
            _debug_print(debug, "Missing table in request context; skipping update.")
            return _response_tuple(
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
            )

        context = PivotRequestContext.from_frontend(
            table=resolved_table,
            trigger_prop=triggered_prop,
            viewport=active_request_meta,
        )
        chart_state_override = (
            chart_request.get("state_override")
            if triggered_prop and triggered_prop.endswith(".chartRequest") and isinstance(chart_request, dict)
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
        state = PivotViewState(
            row_fields=effective_row_fields,
            col_fields=effective_col_fields,
            val_configs=effective_val_configs,
            filters=effective_filters,
            sorting=effective_sorting,
            sort_options=effective_sort_options,
            expanded=effective_expanded,
            show_row_totals=effective_show_row_totals,
            show_col_totals=effective_show_col_totals,
            cell_update=cell_update,
            drill_through=drill_through,
            viewport=viewport if isinstance(viewport, dict) else {},
            chart_request=chart_request if isinstance(chart_request, dict) else {},
        )

        _debug_print(
            debug,
            "[pivot-request]",
            {
                "trigger": triggered_prop,
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
                "chart_request": chart_request if isinstance(chart_request, dict) else None,
                "row_fields": state.row_fields,
                "sorting": state.sorting,
                "sort_options": state.sort_options,
                "sort_options_default_used": bool(
                    sort_options_default and not sort_options
                ),
            },
        )

        try:
            result = runtime_service_getter().process(
                state,
                context,
                current_filter_options=current_filter_options or {},
            )
        except Exception as exc:  # pragma: no cover - defensive
            _debug_print(debug, f"Error in pivot runtime service: {exc}")
            if debug:
                _debug_print(debug, traceback.format_exc())
            return _response_tuple(
                [],
                0,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
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
                schema_entry = next(
                    (
                        col
                        for col in result.columns
                        if isinstance(col, dict) and col.get("id") == "__col_schema"
                    ),
                    None,
                )
                if isinstance(schema_entry, dict) and isinstance(schema_entry.get("col_schema"), dict):
                    schema_obj = schema_entry.get("col_schema") or {}
                    col_schema_total = schema_obj.get("total_center_cols")
                    for item in (schema_obj.get("columns") or [])[:8]:
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
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
            )

        if result.status == "drillthrough":
            return _response_tuple(
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                (result.drill_records or []),
                no_update,
                no_update,
            )

        if result.status == "unique_values":
            return _response_tuple(
                no_update,
                no_update,
                no_update,
                (result.filter_options or {}),
                no_update,
                no_update,
                no_update,
                no_update,
            )

        if result.status == "chart_data":
            return _response_tuple(
                no_update,
                no_update,
                no_update,
                no_update,
                (result.chart_data or {}),
                no_update,
                no_update,
                no_update,
            )

        if result.status == "data":
            columns_out = result.columns if result.columns is not None else no_update
            data_out = list(result.data or [])
            if result.color_scale_stats is not None:
                # Prepend a sentinel row the frontend strips before rendering
                data_out = [{"_path": "__color_scale_stats__", "_colorScaleStats": result.color_scale_stats}] + data_out
            return _response_tuple(
                data_out,
                result.total_rows if result.total_rows is not None else 0,
                columns_out,
                no_update,
                no_update,
                no_update,
                result.data_offset if result.data_offset is not None else no_update,
                result.data_version if result.data_version is not None else no_update,
            )

        if result.message:
            _debug_print(debug, f"Error in pivot engine: {result.message}")
        return _response_tuple(
            [],
            0,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )

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
