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
    cinema_mode: bool = False
    show_row_totals: bool = False
    show_col_totals: bool = True
    cell_update: Optional[Dict[str, Any]] = None
    drill_through: Optional[Dict[str, Any]] = None
    viewport: Dict[str, Any] = field(default_factory=dict)
    chart_request: Dict[str, Any] = field(default_factory=dict)


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
            and viewport_meta.get("start") is not None
            and viewport_meta.get("end") is not None
        )

        start_row = safe_int(viewport_meta.get("start"), 0) if viewport_active else 0
        end_row = safe_int(viewport_meta.get("end"), 1000) if viewport_active else None
        col_end_raw = viewport_meta.get("col_end")
        col_end = safe_int(col_end_raw, 0) if col_end_raw is not None else None

        return cls(
            table=table,
            trigger_prop=trigger_prop,
            session_id=str(viewport_meta.get("session_id") or "anonymous"),
            client_instance=str(viewport_meta.get("client_instance") or "default"),
            state_epoch=safe_int(viewport_meta.get("state_epoch"), 0),
            window_seq=safe_int(viewport_meta.get("window_seq", viewport_meta.get("version", 0)), 0),
            abort_generation=safe_int(viewport_meta.get("abort_generation"), 0),
            original_intent=original_intent,
            intent=intent,
            col_start=safe_int(viewport_meta.get("col_start"), 0),
            col_end=col_end,
            needs_col_schema=bool(viewport_meta.get("needs_col_schema", False)),
            include_grand_total=bool(viewport_meta.get("include_grand_total", False)),
            viewport_active=viewport_active,
            start_row=start_row,
            end_row=end_row,
        )

    @property
    def trigger_kind(self) -> Optional[str]:
        """Categorize trigger for transport adapters."""
        if not self.trigger_prop:
            return None
        if self.trigger_prop.endswith(".drillThrough"):
            return "drill"
        if self.trigger_prop.endswith(".cellUpdate"):
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
    data_offset: Optional[int] = None
    data_version: Optional[int] = None
    message: Optional[str] = None
    color_scale_stats: Optional[Dict[str, Any]] = None
    chart_data: Optional[Dict[str, Any]] = None
