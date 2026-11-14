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
from typing import Any, Callable, Iterable, Optional

from .runtime.async_bridge import run_awaitable_in_worker_thread
from .production_security import (
    ProductionSecuritySettings,
    install_flask_security,
    resolve_runtime_environment,
)
from .security_events import log_authorization_denied
from .runtime import (
    PivotRuntimeService,
    RuntimeDatasetAuthorizer,
    SessionRequestGate,
    register_dash_pivot_transport_callback,
)


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
        except Exception:  # nosec B112
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


def _is_production_environment() -> bool:
    """Use the one environment decision shared by all public transports."""
    return resolve_runtime_environment() == "production"


def register_pivot_app(
    app: Any,
    adapter_getter: Callable[[], Any],
    pivot_id: Any,
    *,
    debug: Optional[bool] = None,
    drill_store_id: Optional[Any] = None,
    allowed_drill_tables: Optional[Iterable[str]] = None,
    drill_authorizer: Optional[Callable[[Any, str], bool]] = None,
    runtime_dataset_authorizer: Optional[RuntimeDatasetAuthorizer] = None,
    runtime_principal_getter: Optional[Callable[[Any], Any]] = None,
) -> None:
    """
    Wire a pivot component into a Dash app with minimal boilerplate.

    Internally manages one ``SessionRequestGate`` and ``PivotRuntimeService``
    for this registration and registers the ``/api/drill-through`` Flask endpoint.

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
    allowed_drill_tables:
        Optional allowlist for the ``/api/drill-through`` table parameter.
        Provide this for every production application so the endpoint cannot
        be used to discover unrelated tables on the shared connection.
    drill_authorizer:
        A synchronous ``(flask_request, table_name) -> bool`` authorization
        callback for drill-through. It is required in a production environment
        (``ENV``/``APP_ENV``/``FLASK_ENV``/``DASH_ENV`` set to ``production``
        or ``prod``). Dash runtime callbacks should still be protected by the
        host application's normal authentication middleware.
    runtime_dataset_authorizer:
        Server-owned per-table policy for Dash runtime data, exports, and
        filter values. In production it must be paired with
        ``runtime_principal_getter``; otherwise the callback fails closed.
    runtime_principal_getter:
        ``(flask_request) -> principal`` resolver used for every Dash runtime
        request. The principal is passed to the runtime, so row-level filters
        apply to data and filter-option queries as well as table permissions.
    """
    security_settings = ProductionSecuritySettings.from_env()
    production = security_settings.is_production
    if production:
        # Install before drill and Dash callback routes become public. The
        # installer is idempotent, so multiple pivot instances share one host
        # boundary rather than accumulating duplicate Flask hooks.
        install_flask_security(app.server, security_settings)
    if debug is None:
        debug = os.environ.get("PIVOT_DEBUG_OUTPUT", "0").lower() in {"1", "true", "yes"}
    allowed_table_names = frozenset(
        str(table).strip()
        for table in (allowed_drill_tables or ())
        if str(table).strip()
    )

    # --- per-registration runtime state ---
    _session_gate = SessionRequestGate()
    _runtime_service = _RuntimeServiceHolder()
    try:
        request_timeout_seconds = float(os.environ.get("PIVOT_REQUEST_TIMEOUT_SECONDS", "300") or 300)
    except (TypeError, ValueError):
        request_timeout_seconds = 300.0

    def _get_runtime_service():
        if _runtime_service.service is None:
            _runtime_service.service = PivotRuntimeService(
                adapter_getter=adapter_getter,
                session_gate=_session_gate,
                debug=debug,
                request_timeout_seconds=request_timeout_seconds,
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
            sort_col = flask_request.args.get("sort_col") or None
            sort_dir = flask_request.args.get("sort_dir", "asc")
            text_filter = flask_request.args.get("filter", "")

            if not table:
                # Fix L8: generic error in production, detailed only when debug=True
                error_msg = "Missing required parameter" if not debug else "table param required"
                return jsonify({"error": error_msg}), 400

            if allowed_table_names and table not in allowed_table_names:
                # Avoid revealing whether an unapproved table exists.
                return jsonify({"error": "Not found"}), 404
            if production and drill_authorizer is None:
                log_authorization_denied(
                    principal=None,
                    table=table,
                    operation="drill",
                    reason="drill_authorizer_not_configured",
                )
                return jsonify({"error": "Drill-through is not configured"}), 403
            if drill_authorizer is not None:
                try:
                    allowed = bool(drill_authorizer(flask_request, table))
                except Exception:
                    # Authorization errors must fail closed and must not leak
                    # an implementation detail to the browser.
                    allowed = False
                if not allowed:
                    log_authorization_denied(
                        principal=None,
                        table=table,
                        operation="drill",
                        reason="drill_authorizer_denied",
                    )
                    return jsonify({"error": "Forbidden"}), 403

            import re as _re
            _SAFE_FIELD_RE = _re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
            row_fields = [f for f in row_fields_raw.split(",") if f and _SAFE_FIELD_RE.match(f)]
            path_parts = row_path.split("|||") if row_path else []

            try:
                page = max(0, int(flask_request.args.get("page", 0)))
                page_size = min(max(1, int(flask_request.args.get("page_size", 500))), 500)
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid pagination"}), 400

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
    except Exception:  # nosec B110
        pass

    # --- Dash transport callback ---
    register_dash_pivot_transport_callback(
        app,
        _get_runtime_service,
        pivot_id=pivot_id,
        drill_store_id=drill_store_id,
        debug=debug,
        sort_options_default=_layout_sort_options,
        dataset_authorizer=runtime_dataset_authorizer,
        principal_getter=runtime_principal_getter,
        require_dataset_authorization=production,
    )
