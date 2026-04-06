"""
Types and validation for PivotSpec.
Simple pydantic-like dataclass replacement to avoid heavy dependencies.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union
import copy

class NullHandling:  # Replicating from planner for consistency
    IGNORE = "ignore"
    AS_ZERO = "as_zero"
    AS_EMPTY = "as_empty"
    EXCLUDE_ROWS = "exclude_rows"

@dataclass
class Measure:
    field: Optional[str]
    agg: str
    alias: str
    weighted_field: Optional[str] = None
    expression: Optional[str] = None
    percentile: Optional[float] = None
    separator: Optional[str] = None
    null_handling: Optional[str] = None  # "ignore", "as_zero", "as_empty", "exclude_rows"
    filter_condition: Optional[str] = None
    
    # NEW: Ratio metrics support
    ratio_numerator: Optional[str] = None  # Reference to another measure alias
    ratio_denominator: Optional[str] = None
    ratio_format: str = "decimal"  # "decimal" or "percentage"
    ratio_null_value: Optional[float] = None  # Value to use when denominator is 0

    # NEW: Window Function Support
    window_func: Optional[str] = None  # "cumulative", "percent_of_total", "rank", "dense_rank", "running_avg", "moving_avg"
    window_group_by: Optional[List[str]] = None  # Dimensions to partition by. If None, uses current pivot row groups.
    window_order_by: Optional[List[str]] = None  # Dimensions to order by for the window.
    window_frame_start: Optional[int] = None  # For moving windows (e.g. -3)
    window_frame_end: Optional[int] = None    # For moving windows (e.g. 0)

    def __post_init__(self):
        if not self.alias and self.field:
            self.alias = f"{self.agg}_{self.field.replace('.', '_')}" if self.field else self.agg

@dataclass
class GroupingConfig:
    """Configuration for GROUPING SETS operations"""
    mode: str = "standard"
    grouping_sets: Optional[List[List[str]]] = None
    include_grand_total: bool = False
    include_subtotals: bool = False
    subtotal_dimensions: Optional[List[str]] = None

@dataclass
class PivotConfig:
    """Configuration for pivot operations"""
    enabled: bool = False
    top_n: Optional[int] = None
    order_by_measure: Optional[str] = None
    include_totals_column: bool = False
    null_column_label: str = "(null)"
    column_cursor: Optional[str] = None  # Keyset cursor for horizontal scrolling
    materialized_column_values: Optional[List[str]] = None

@dataclass
class DrillPath:
    """Represents a drill-down path in hierarchical data"""
    dimensions: List[str]
    values: List[Any]
    level: int


@dataclass
class PivotSpec:
    table: str
    rows: List[str] = field(default_factory=list)
    full_rows: List[str] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    measures: List[Measure] = field(default_factory=list)
    filters: List[Dict[str, Any]] = field(default_factory=list)
    sort: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = field(default_factory=list)
    limit: int = 1000
    offset: int = 0
    cursor: Optional[Dict[str, Any]] = None
    totals: bool = False
    having: Optional[List[Dict[str, Any]]] = None
    subtotals: Optional[List[str]] = None
    grouping_config: Optional[GroupingConfig] = None
    pivot_config: Optional[PivotConfig] = None
    drill_paths: Optional[List[DrillPath]] = None
    column_sort_options: Optional[Dict[str, Any]] = None

    def copy(self):
        """Deep copy the PivotSpec"""
        return copy.deepcopy(self)

    @staticmethod
    def from_dict(d: dict):
        measures = [Measure(**m) if isinstance(m, dict) else m for m in d.get("measures", [])]
        grouping_config = GroupingConfig(**d.get("grouping_config")) if d.get("grouping_config") else None
        pivot_config = PivotConfig(**d.get("pivot_config")) if d.get("pivot_config") else None
        drill_paths = [DrillPath(**p) for p in d.get("drill_paths", [])] if d.get("drill_paths") else None

        return PivotSpec(
            table=d.get("table"),
            rows=d.get("rows", []),
            full_rows=d.get("full_rows", []),
            columns=d.get("columns", []),
            measures=measures,
            filters=d.get("filters", []),
            sort=d.get("sort", []),
            limit=d.get("limit", 1000),
            offset=d.get("offset", 0),
            cursor=d.get("cursor"),
            totals=d.get("totals", False),
            having=d.get("having"),
            subtotals=d.get("subtotals"),
            grouping_config=grouping_config,
            pivot_config=pivot_config,
            drill_paths=drill_paths,
            column_sort_options=d.get("column_sort_options"),
        )

    def to_dict(self):
        # Helper to convert spec to a dictionary for hashing/diffing
        return {
            "table": self.table,
            "rows": self.rows,
            "full_rows": self.full_rows,
            "columns": self.columns,
            "measures": [m.__dict__ for m in self.measures],
            "filters": self.filters,
            "sort": self.sort,
            "limit": self.limit,
            "offset": self.offset,
            "cursor": self.cursor,
            "totals": self.totals,
            "having": self.having,
            "subtotals": self.subtotals,
            "grouping_config": self.grouping_config.__dict__ if self.grouping_config else None,
            "pivot_config": self.pivot_config.__dict__ if self.pivot_config else None,
            "drill_paths": [p.__dict__ for p in self.drill_paths] if self.drill_paths else None,
            "column_sort_options": self.column_sort_options,
        }
