"""Streamlit V2 host for the host-neutral Pivot Runtime Protocol V1.

The Streamlit component only mounts the shared React workspace.  All viewport
and command traffic goes directly from the browser to ``/v1/runtime``; it is
not serialized through Streamlit's rerun channel.  The optional ``view_state``
output is deliberately reserved for durable, user-meaningful state changes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence


_COMPONENT_NAME = "serverside-pivot.pivot_workspace"
_component: Any = None


def _component_script(build_dir: Optional[Path] = None) -> str:
    """Return the Vite entry declared by the package-local build manifest.

    A hashed entry filename is necessary for browser cache correctness, but
    filesystem timestamps cannot identify it reliably: an in-place Python
    upgrade may give every old and new asset the same timestamp. Vite's
    manifest records the exact entry instead. The fallback only supports
    distributions built before the manifest was introduced.
    """
    build_dir = build_dir or Path(__file__).parent / "frontend" / "build"
    manifest_path = build_dir / "pivot-workspace-manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "The Streamlit workspace build manifest is invalid. "
                "Install a built serverside-pivot distribution."
            ) from error
        entry = manifest.get("src/index.tsx") if isinstance(manifest, dict) else None
        entry_file = entry.get("file") if isinstance(entry, dict) else None
        candidate = build_dir / entry_file if isinstance(entry_file, str) else None
        if candidate is not None and candidate.is_file():
            return candidate.relative_to(build_dir).as_posix()
        raise RuntimeError(
            "The Streamlit workspace entry declared by its build manifest is missing. "
            "Install a built serverside-pivot distribution."
        )

    candidates = list(build_dir.glob("pivot-workspace-*.js"))
    if not candidates:
        raise RuntimeError(
            "The Streamlit workspace assets are missing. "
            "Install a built serverside-pivot distribution."
        )
    return max(candidates, key=lambda path: path.stat().st_mtime_ns).name


def _get_component() -> Any:
    """Register lazily so importing this package does not require Streamlit."""
    global _component
    if _component is None:
        try:
            import streamlit as st
        except ImportError as error:  # pragma: no cover - exercised by users
            raise ImportError(
                "Streamlit support is optional. Install serverside-pivot[streamlit]."
            ) from error
        component_api = getattr(getattr(st, "components", None), "v2", None)
        if component_api is None or not hasattr(component_api, "component"):
            raise RuntimeError(
                "The Streamlit host requires Streamlit >=1.51. "
                "Install serverside-pivot[streamlit]."
            )
        _component = component_api.component(
            _COMPONENT_NAME,
            js=_component_script(),
            # V2 requires a non-empty HTML asset declaration even though the
            # React renderer owns the contents of the host element.
            html=" ",
            isolate_styles=True,
        )
    return _component


def pivot_workspace(
    table: str,
    *,
    runtime_endpoint: str = "/v1/runtime",
    key: Optional[str] = None,
    server_side: bool = True,
    row_fields: Optional[Sequence[str]] = None,
    col_fields: Optional[Sequence[str]] = None,
    val_configs: Optional[Sequence[Mapping[str, Any]]] = None,
    view_state: Optional[Mapping[str, Any]] = None,
    transport_headers: Optional[Mapping[str, str]] = None,
    on_view_state_change: Optional[Callable[[], None]] = None,
    **configuration: Any,
) -> Any:
    """Render the pivot in a Streamlit application.

    ``runtime_endpoint`` must route to
    :func:`pivot_engine.register_runtime_http_transport` (normally the
    ``/v1/runtime`` endpoint registered by :class:`CompletePivotAPI`).  It is
    intentionally a browser endpoint, not a Streamlit callback.

    ``transport_headers`` are sent by browser requests. Prefer same-origin
    cookie authentication and never put a secret intended only for the Python
    process in this argument.
    """
    normalized_table = str(table or "").strip()
    if not normalized_table:
        raise ValueError("pivot_workspace requires a non-empty table name.")

    # ``configuration`` deliberately accepts the shared React workspace
    # shape as well as this Python wrapper's snake_case arguments.  This is
    # important for host-neutral presets: a caller may pass one
    # ``PivotWorkspaceProps`` mapping to Dash, React, and Streamlit without
    # having its layout silently reset here.  Explicit snake_case arguments
    # continue to take precedence, including an intentional empty list.
    shared_row_fields = configuration.pop("rowFields", None)
    shared_col_fields = configuration.pop("colFields", None)
    shared_val_configs = configuration.pop("valConfigs", None)
    resolved_row_fields = row_fields if row_fields is not None else shared_row_fields
    resolved_col_fields = col_fields if col_fields is not None else shared_col_fields
    resolved_val_configs = val_configs if val_configs is not None else shared_val_configs

    component_data = {
        "runtimeEndpoint": str(runtime_endpoint or "/v1/runtime"),
        "transportHeaders": dict(transport_headers or {}),
        # ``setStateValue`` requests a Streamlit script rerun. Only enable
        # that output channel for callers which explicitly asked for it;
        # viewport and resize work must remain entirely in the browser.
        "viewStateEnabled": on_view_state_change is not None,
        "configuration": {
            **dict(configuration),
            "table": normalized_table,
            "serverSide": bool(server_side),
            "rowFields": list(resolved_row_fields or ()),
            "colFields": list(resolved_col_fields or ()),
            "valConfigs": [dict(item) for item in (resolved_val_configs or ())],
            **dict(view_state or {}),
        },
    }
    mount_options: dict[str, Any] = {
        "key": key,
        "data": component_data,
    }
    # Streamlit V2 permits default state only for state keys which have a
    # matching change callback.  The workspace configuration itself already
    # carries the initial view state, so omit the callback state entirely when
    # an app chooses the one-way embedding API.
    if on_view_state_change is not None:
        mount_options["default"] = {"view_state": dict(view_state or {})}
        mount_options["on_view_state_change"] = on_view_state_change
    return _get_component()(**mount_options)


__all__ = ["pivot_workspace"]
