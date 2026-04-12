"""
dash_integration.py — One-call Dash wiring for a pivot app.

Usage::

    from pivot_engine import register_pivot_app

    app = Dash(__name__)
    register_pivot_app(app, adapter_getter=get_adapter, pivot_id="pivot-grid")
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .runtime.async_bridge import run_awaitable_in_worker_thread
from .runtime import PivotRuntimeService, SessionRequestGate, register_dash_pivot_transport_callback


def _component_props(component: Any) -> dict:
    if component is None or isinstance(component, (str, int, float, bool)):
        return {}
    to_plotly_json = getattr(component, "to_plotly_json", None)
    if callable(to_plotly_json):
        try:
            payload = to_plotly_json()
            props = payload.get("props") if isinstance(payload, dict) else None
            if isinstance(props, dict):
                return props
        except Exception:
            return {}
    props = {}
    for name in getattr(component, "_prop_names", []) or []:
        try:
            props[name] = getattr(component, name)
        except Exception:
            continue
    return props


def _find_sort_options_in_layout(layout: Any, target_id: Any, _seen: Optional[set[int]] = None) -> Optional[dict]:
    """
    Recursively walk the Dash component prop tree to find ``target_id`` and
    return its ``sortOptions`` prop (or None if not found / not set).
    """
    if layout is None or callable(layout):
        return None
    if _seen is None:
        _seen = set()
    layout_id = id(layout)
    if layout_id in _seen:
        return None
    _seen.add(layout_id)

    if isinstance(layout, (list, tuple, set)):
        for item in layout:
            result = _find_sort_options_in_layout(item, target_id, _seen)
            if result is not None:
                return result
        return None
    if isinstance(layout, dict):
        if layout.get("id") == target_id and isinstance(layout.get("sortOptions"), dict):
            return layout["sortOptions"]
        for value in layout.values():
            result = _find_sort_options_in_layout(value, target_id, _seen)
            if result is not None:
                return result
        return None

    props = _component_props(layout)
    component_id = props.get("id", getattr(layout, "id", None))
    if component_id == target_id:
        sort_options = props.get("sortOptions", getattr(layout, "sortOptions", None))
        return sort_options or None

    for value in props.values():
        result = _find_sort_options_in_layout(value, target_id, _seen)
        if result is not None:
            return result
    return None


@dataclass
class _RuntimeServiceHolder:
    service: Optional[PivotRuntimeService] = None


def register_pivot_app(
    app: Any,
    adapter_getter: Callable[[], Any],
    pivot_id: Any,
    *,
    debug: Optional[bool] = None,
    drill_store_id: Optional[Any] = None,
) -> None:
    """
    Wire a pivot component into a Dash app with minimal boilerplate.

    Internally manages the ``SessionRequestGate`` and ``PivotRuntimeService``
    singletons and registers the ``/api/drill-through`` Flask endpoint.

    Parameters
    ----------
    app:
        The ``dash.Dash`` application instance.
    adapter_getter:
        A zero-argument callable that returns (or lazily creates) the
        ``TanStackPivotAdapter`` for this app.
    pivot_id:
        The ``id`` prop of the ``DashTanstackPivot`` component.
    debug:
        Print request/response logs. Defaults to the ``PIVOT_DEBUG_OUTPUT``
        environment variable (``"1"`` / ``"true"`` / ``"yes"`` → ``True``).
    drill_store_id:
        Optional ``dcc.Store`` id for drill-through data (only needed when
        you also call ``register_dash_drill_modal_callback`` separately).
    """
    if debug is None:
        debug = os.environ.get("PIVOT_DEBUG_OUTPUT", "1").lower() in {"1", "true", "yes"}

    # --- singletons (one per register_pivot_app call) ---
    _session_gate = SessionRequestGate()
    _runtime_service = _RuntimeServiceHolder()

    def _get_runtime_service():
        if _runtime_service.service is None:
            _runtime_service.service = PivotRuntimeService(
                adapter_getter=adapter_getter,
                session_gate=_session_gate,
                debug=debug,
            )
        return _runtime_service.service

    # --- drill-through REST endpoint (guard against double-registration) ---
    if "_api_drill_through" not in app.server.view_functions:
        @app.server.route("/api/drill-through")
        def _api_drill_through():
            from flask import request as flask_request, jsonify
            from .types.pivot_spec import PivotSpec

            table = flask_request.args.get("table", "")
            row_path = flask_request.args.get("row_path", "")
            row_fields_raw = flask_request.args.get("row_fields", "")
            page = int(flask_request.args.get("page", 0))
            page_size = min(int(flask_request.args.get("page_size", 500)), 500)
            sort_col = flask_request.args.get("sort_col") or None
            sort_dir = flask_request.args.get("sort_dir", "asc")
            text_filter = flask_request.args.get("filter", "")

            if not table:
                # Fix L8: generic error in production, detailed only when debug=True
                error_msg = "Missing required parameter" if not debug else "table param required"
                return jsonify({"error": error_msg}), 400

            row_fields = [f for f in row_fields_raw.split(",") if f]
            path_parts = row_path.split("|||") if row_path else []

            drill_filters = []
            for i, field in enumerate(row_fields):
                if i < len(path_parts) and path_parts[i]:
                    drill_filters.append({"field": field, "op": "=", "value": path_parts[i]})

            spec = PivotSpec(table=table, rows=[], measures=[], filters=[])
            async def _load_drill_through_data():
                adapter = adapter_getter()
                return await adapter.controller.get_drill_through_data(
                    spec,
                    drill_filters,
                    limit=page_size,
                    offset=page * page_size,
                    sort_col=sort_col,
                    sort_dir=sort_dir,
                    text_filter=text_filter,
                )

            result = run_awaitable_in_worker_thread(_load_drill_through_data())
            return jsonify(
                {
                    "rows": result["rows"],
                    "page": page,
                    "page_size": page_size,
                    "total_rows": result["total_rows"],
                }
            )

    # Extract sortOptions from the layout at registration time so the callback
    # always has a reliable fallback when State(pivot_id, "sortOptions") is
    # None or empty (e.g. before the React component has mounted).
    _layout_sort_options: Optional[dict] = None
    try:
        if hasattr(app, "layout") and app.layout is not None:
            _layout_sort_options = _find_sort_options_in_layout(app.layout, pivot_id)
    except Exception:
        pass

    # --- Dash transport callback ---
    register_dash_pivot_transport_callback(
        app,
        _get_runtime_service,
        pivot_id=pivot_id,
        drill_store_id=drill_store_id,
        debug=debug,
        sort_options_default=_layout_sort_options,
    )

