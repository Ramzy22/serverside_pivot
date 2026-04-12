"""Transport-agnostic runtime models for pivot requests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def safe_int(value: Any, default: int = 0) -> int:
    """Convert value to int with a safe fallback."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def first_present(mapping: Any, *keys: str, default: Any = None) -> Any:
    """Return the first non-None value found in mapping for the given keys."""
    if not isinstance(mapping, dict):
        return default
    for key in keys:
        if key in mapping and mapping.get(key) is not None:
            return mapping.get(key)
    return default


@dataclass
class PivotViewState:
    """Frontend-independent pivot state for a single request."""

    row_fields: List[str] = field(default_factory=list)
    col_fields: List[str] = field(default_factory=list)
    val_configs: List[Dict[str, Any]] = field(default_factory=list)
    filters: Dict[str, Any] = field(default_factory=dict)
    sorting: List[Dict[str, Any]] = field(default_factory=list)
    sort_options: Dict[str, Any] = field(default_factory=dict)
    expanded: Any = field(default_factory=dict)
    immersive_mode: bool = False
    show_row_totals: bool = False
    show_col_totals: bool = True
    cell_update: Optional[Dict[str, Any]] = None
    cell_updates: List[Dict[str, Any]] = field(default_factory=list)
    transaction_request: Optional[Dict[str, Any]] = None
    drill_through: Optional[Dict[str, Any]] = None
    detail_request: Optional[Dict[str, Any]] = None
    viewport: Dict[str, Any] = field(default_factory=dict)
    chart_request: Dict[str, Any] = field(default_factory=dict)
    view_mode: str = "pivot"
    detail_mode: str = "none"
    tree_config: Optional[Dict[str, Any]] = None
    detail_config: Optional[Dict[str, Any]] = None
    report_def: Optional[Dict[str, Any]] = None


@dataclass
class PivotRequestContext:
    """Execution context used for request ordering and multi-instance isolation."""

    table: str
    trigger_prop: Optional[str] = None
    session_id: str = "anonymous"
    client_instance: str = "default"
    state_epoch: int = 0
    window_seq: int = 0
    abort_generation: int = 0
    original_intent: Optional[str] = None
    intent: str = "structural"
    col_start: int = 0
    col_end: Optional[int] = None
    needs_col_schema: bool = False
    include_grand_total: bool = False
    viewport_active: bool = False
    start_row: int = 0
    end_row: Optional[int] = None
    request_id: Optional[str] = None
    profiling: bool = False

    @classmethod
    def from_frontend(
        cls,
        *,
        table: str,
        trigger_prop: Optional[str],
        viewport: Any,
    ) -> "PivotRequestContext":
        """Build context from generic frontend metadata."""
        viewport_meta = viewport if isinstance(viewport, dict) else {}
        original_intent = viewport_meta.get("intent")

        if original_intent == "expansion":
            intent = "structural"
        elif original_intent == "chart":
            intent = "chart"
        elif original_intent not in {"viewport", "structural", "chart"}:
            intent = "viewport" if trigger_prop and trigger_prop.endswith(".viewport") else "structural"
        else:
            intent = original_intent

        viewport_active = (
            isinstance(viewport_meta, dict)
            and first_present(viewport_meta, "start") is not None
            and first_present(viewport_meta, "end") is not None
        )

        start_row = safe_int(first_present(viewport_meta, "start"), 0) if viewport_active else 0
        end_row = safe_int(first_present(viewport_meta, "end"), 1000) if viewport_active else None
        col_end_raw = first_present(viewport_meta, "col_end", "colEnd")
        col_end = safe_int(col_end_raw, 0) if col_end_raw is not None else None

        return cls(
            table=table,
            trigger_prop=trigger_prop,
            session_id=str(first_present(viewport_meta, "session_id", "sessionId", default="anonymous") or "anonymous"),
            client_instance=str(first_present(viewport_meta, "client_instance", "clientInstance", default="default") or "default"),
            state_epoch=safe_int(first_present(viewport_meta, "state_epoch", "stateEpoch"), 0),
            window_seq=safe_int(first_present(viewport_meta, "window_seq", "windowSeq", "version"), 0),
            abort_generation=safe_int(first_present(viewport_meta, "abort_generation", "abortGeneration"), 0),
            original_intent=original_intent,
            intent=intent,
            col_start=safe_int(first_present(viewport_meta, "col_start", "colStart"), 0),
            col_end=col_end,
            needs_col_schema=bool(first_present(viewport_meta, "needs_col_schema", "needsColSchema", default=False)),
            include_grand_total=bool(first_present(viewport_meta, "include_grand_total", "includeGrandTotal", default=False)),
            viewport_active=viewport_active,
            start_row=start_row,
            end_row=end_row,
            request_id=str(first_present(viewport_meta, "requestId", "request_id", default="") or "") or None,
            profiling=bool(first_present(viewport_meta, "profile", "profiling", default=False)),
        )

    @property
    def trigger_kind(self) -> Optional[str]:
        """Categorize trigger for transport adapters."""
        if not self.trigger_prop:
            return None
        if self.trigger_prop.endswith(".detailRequest"):
            return "detail"
        if self.trigger_prop.endswith(".drillThrough"):
            return "drill"
        if self.trigger_prop.endswith(".cellUpdate") or self.trigger_prop.endswith(".cellUpdates"):
            return "update"
        if self.trigger_prop.endswith(".chartRequest"):
            return "chart"
        if self.trigger_prop.endswith(".viewport"):
            return "viewport"
        return "structural"


@dataclass
class PivotServiceResponse:
    """Generic response contract for transport adapters (Dash, REST, etc.)."""

    status: str
    data: Optional[List[Dict[str, Any]]] = None
    total_rows: Optional[int] = None
    columns: Optional[List[Dict[str, Any]]] = None
    filter_options: Optional[Dict[str, Any]] = None
    drill_records: Optional[List[Dict[str, Any]]] = None
    drill_payload: Optional[Dict[str, Any]] = None
    detail_payload: Optional[Dict[str, Any]] = None
    data_offset: Optional[int] = None
    data_version: Optional[int] = None
    message: Optional[str] = None
    color_scale_stats: Optional[Dict[str, Any]] = None
    col_schema: Optional[Dict[str, Any]] = None
    chart_data: Optional[Dict[str, Any]] = None
    transaction_result: Optional[Dict[str, Any]] = None
    patch_payload: Optional[Dict[str, Any]] = None
    edit_overlay: Optional[Dict[str, Any]] = None
    profile: Optional[Dict[str, Any]] = None
