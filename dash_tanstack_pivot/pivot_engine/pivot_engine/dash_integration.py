"""
dash_integration.py — One-call Dash wiring for a pivot app.

Usage::

    from pivot_engine import register_pivot_app

    app = Dash(__name__)
    register_pivot_app(app, adapter_getter=get_adapter, pivot_id="pivot-grid")
"""
from __future__ import annotations

import os
from typing import Any, Callable, Optional

from .runtime import PivotRuntimeService, SessionRequestGate, register_dash_pivot_transport_callback


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
    _runtime_service: list = [None]  # mutable cell so the closure can replace it

    def _get_runtime_service():
        if _runtime_service[0] is None:
            _runtime_service[0] = PivotRuntimeService(
                adapter_getter=adapter_getter,
                session_gate=_session_gate,
                debug=debug,
            )
        return _runtime_service[0]

    # --- drill-through REST endpoint (guard against double-registration) ---
    if "_api_drill_through" not in app.server.view_functions:
        @app.server.route("/api/drill-through")
        def _api_drill_through():
            import asyncio
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
                return jsonify({"error": "table param required"}), 400

            row_fields = [f for f in row_fields_raw.split(",") if f]
            path_parts = row_path.split("|||") if row_path else []

            drill_filters = []
            for i, field in enumerate(row_fields):
                if i < len(path_parts) and path_parts[i]:
                    drill_filters.append({"field": field, "op": "=", "value": path_parts[i]})

            spec = PivotSpec(table=table, rows=[], measures=[], filters=[])
            result = asyncio.run(
                adapter_getter().controller.get_drill_through_data(
                    spec,
                    drill_filters,
                    limit=page_size,
                    offset=page * page_size,
                    sort_col=sort_col,
                    sort_dir=sort_dir,
                    text_filter=text_filter,
                )
            )
            return jsonify(
                {
                    "rows": result["rows"],
                    "page": page,
                    "page_size": page_size,
                    "total_rows": result["total_rows"],
                }
            )

    # --- Dash transport callback ---
    register_dash_pivot_transport_callback(
        app,
        _get_runtime_service,
        pivot_id=pivot_id,
        drill_store_id=drill_store_id,
        debug=debug,
    )
