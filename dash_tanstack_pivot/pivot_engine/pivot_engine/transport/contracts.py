"""Stable transport models shared by runtime and the TanStack adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class TanStackOperation(str, Enum):
    """TanStack operation types."""

    GET_DATA = "get_data"
    GET_ROWS = "get_rows"
    GET_COLUMNS = "get_columns"
    GET_PAGE_COUNT = "get_page_count"
    FILTER = "filter"
    SORT = "sort"
    GROUP = "group"
    GET_UNIQUE_VALUES = "get_unique_values"


@dataclass
class TanStackRequest:
    """Transport request accepted by :class:`TanStackPivotAdapter`."""

    operation: TanStackOperation
    table: str
    columns: List[Dict[str, Any]]
    filters: Dict[str, Any]
    sorting: List[Dict[str, Any]]
    grouping: List[str]
    aggregations: List[Dict[str, Any]]
    pagination: Optional[Dict[str, Any]] = None
    global_filter: Optional[str] = None
    totals: Optional[bool] = True
    row_totals: Optional[bool] = False
    include_subtotals: Optional[bool] = True
    version: Optional[int] = None
    column_sort_options: Optional[Dict[str, Any]] = None
    custom_dimensions: List[Dict[str, Any]] = field(default_factory=list)
    measure_axis: Optional[Dict[str, Any]] = None
    layout_mode: Optional[str] = None
    show_subtotal_footers: Optional[bool] = False
    # Full exports must bypass the interactive viewport cache so their result
    # cannot be truncated to a previously requested window.
    full_export: bool = False
    # Server-owned pivot ordering. A manifest is sent only on change/miss;
    # ordinary viewport windows carry the resolved layout revision.
    layout_revision: Optional[str] = None
    layout_manifest: Optional[tuple[str, ...]] = None
    pinned_column_ids: tuple[str, ...] = ()


@dataclass
class TanStackResponse:
    """Transport response emitted by :class:`TanStackPivotAdapter`."""

    data: List[Dict[str, Any]]
    columns: List[Dict[str, Any]]
    pagination: Optional[Dict[str, Any]] = None
    total_rows: Optional[int] = None
    grouping: Optional[List[Dict[str, Any]]] = None
    version: Optional[int] = None
    col_schema: Optional[Dict[str, Any]] = None
    color_scale_stats: Optional[Dict[str, Any]] = None
    profile: Optional[Dict[str, Any]] = None
    formula_errors: Optional[Dict[str, str]] = None
    layout_revision: Optional[str] = None
