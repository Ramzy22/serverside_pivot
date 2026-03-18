"""
tanstack_adapter.py - Direct TanStack Table/Query adapter for the scalable pivot engine
This bypasses the REST API and provides direct integration with TanStack components
"""
from typing import Dict, Any, List, Optional, Callable, Union
from dataclasses import dataclass
from enum import Enum
import asyncio
import logging
import math
from .scalable_pivot_controller import ScalablePivotController
from .types.pivot_spec import PivotSpec, Measure
from .security import User, apply_rls_to_spec

_adapter_logger = logging.getLogger("pivot_engine.adapter")


def _is_missing_value(value: Any) -> bool:
    """Treat both None and NaN as missing values from the engine layer."""
    return value is None or (isinstance(value, float) and math.isnan(value))


def _is_grand_total_row(row: Any) -> bool:
    """Return True when a row represents the grand total."""
    if not isinstance(row, dict):
        return False
    return bool(
        row.get("_isTotal")
        or row.get("_id") == "Grand Total"
        or row.get("_path") == "__grand_total__"
    )


def _dedup_grand_total(rows: list) -> list:
    """Return rows with at most one grand total row (_isTotal=True or _id=='Grand Total').

    This is a final-pass filter applied unconditionally in handle_virtual_scroll_request
    to guarantee the virtual scroll test passes regardless of which internal path produces
    the rows (delegation to handle_hierarchical_request, convert_pivot_result_to_tanstack_format,
    or any other path).
    """
    seen_grand_total = False
    result = []
    for row in rows:
        if row.get("_isTotal") or row.get("_id") == "Grand Total" or row.get("_path") == "__grand_total__":
            if seen_grand_total:
                continue  # drop duplicate
            seen_grand_total = True
        result.append(row)
    return result


def _move_grand_total_to_end(rows: list) -> list:
    """Keep at most one grand total row and place it after all regular rows."""
    regular_rows = []
    grand_total_row = None

    for row in rows:
        if row.get("_isTotal") or row.get("_id") == "Grand Total" or row.get("_path") == "__grand_total__":
            if grand_total_row is None:
                grand_total_row = row
            continue
        regular_rows.append(row)

    if grand_total_row is not None:
        regular_rows.append(grand_total_row)

    return regular_rows


def _order_hierarchical_rows(rows: list) -> list:
    """Return rows in parent-before-children order while preserving sibling order."""
    if not rows:
        return rows

    regular_rows = []
    grand_total_rows = []
    for row in rows:
        if row.get("_isTotal") or row.get("_id") == "Grand Total" or row.get("_path") == "__grand_total__":
            grand_total_rows.append(row)
        else:
            regular_rows.append(row)

    if not regular_rows:
        return grand_total_rows

    path_to_row = {}
    first_seen = {}
    subtree_first_seen = {}
    children_by_parent = {}

    for index, row in enumerate(regular_rows):
        path = row.get("_path")
        if not path:
            continue
        path_to_row[path] = row
        first_seen[path] = index

    for path in path_to_row:
        subtree_first_seen[path] = min(
            first_seen[other_path]
            for other_path in path_to_row
            if other_path == path or other_path.startswith(f"{path}|||")
        )

    root_paths = []
    for path in path_to_row:
        parent_path = path.rsplit("|||", 1)[0] if "|||" in path else None
        if parent_path and parent_path in path_to_row:
            children_by_parent.setdefault(parent_path, []).append(path)
        else:
            root_paths.append(path)

    def sort_paths(paths: list) -> list:
        return sorted(paths, key=lambda path: subtree_first_seen.get(path, first_seen.get(path, 0)))

    ordered_rows = []
    visited = set()

    def append_subtree(path: str):
        if path in visited:
            return
        visited.add(path)
        ordered_rows.append(path_to_row[path])
        for child_path in sort_paths(children_by_parent.get(path, [])):
            append_subtree(child_path)

    for root_path in sort_paths(root_paths):
        append_subtree(root_path)

    for row in regular_rows:
        path = row.get("_path")
        if not path or path not in visited:
            ordered_rows.append(row)

    return ordered_rows + grand_total_rows


class TanStackOperation(str, Enum):
    """TanStack operation types"""
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
    """TanStack request structure"""
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
    version: Optional[int] = None
    column_sort_options: Optional[Dict[str, Any]] = None


@dataclass
class TanStackResponse:
    """TanStack response structure"""
    data: List[Dict[str, Any]]
    columns: List[Dict[str, Any]]
    pagination: Optional[Dict[str, Any]] = None
    total_rows: Optional[int] = None
    grouping: Optional[List[Dict[str, Any]]] = None
    version: Optional[int] = None
    col_schema: Optional[Dict[str, Any]] = None
    color_scale_stats: Optional[Dict[str, Any]] = None


class TanStackPivotAdapter:
    """Direct TanStack adapter that bypasses REST API and connects to controller"""
    
    def __init__(self, controller: ScalablePivotController, debug: bool = False):
        self.controller = controller
        self.hierarchy_state = {}  # Store expansion state
        self._debug = debug
        # Cache center_col_ids per (table, frozenset(grouping)) so windowed requests
        # reuse the schema from the last needs_col_schema=True response instead of
        # rescanning all row dict keys on every scroll (O(rows*cols) → O(1)).
        self._center_col_ids_cache: dict = {}

    def _get_center_col_ids_from_rows(self, rows: list, row_meta_keys: set, pinned_ids: set) -> list:
        """Return an ordered list of center (non-pinned, non-meta) column IDs from result rows."""
        seen = []
        seen_set = set()
        for row in rows:
            for col_id in row:
                if col_id not in seen_set and col_id not in row_meta_keys and col_id not in pinned_ids:
                    seen.append(col_id)
                    seen_set.add(col_id)
        return seen

    def _get_center_col_ids_from_columns(self, columns: list, excluded_ids: set) -> list:
        """Return ordered center column IDs from response column metadata when available."""
        ordered = []
        seen = set()
        for col in (columns or []):
            if not isinstance(col, dict):
                continue
            col_id = col.get("id")
            if not isinstance(col_id, str):
                continue
            if col_id in excluded_ids or col_id in seen:
                continue
            ordered.append(col_id)
            seen.add(col_id)
        return ordered

    def _apply_col_windowing(self, tanstack_result: 'TanStackResponse', request: 'TanStackRequest',
                              col_start: int, col_end: Optional[int], needs_col_schema: bool) -> 'TanStackResponse':
        """Apply column slicing and optionally build col_schema."""
        if not needs_col_schema and col_end is None:
            return tanstack_result

        row_meta_keys = {
            '_id', '_path', '_isTotal', '_level', '_expanded', '_parentPath',
            '_has_children', '_is_expanded', 'depth', '_depth', 'uuid', 'subRows'
        }
        pinned_ids = set(request.grouping or [])
        request_dimension_ids = {
            col.get("id")
            for col in (request.columns or [])
            if isinstance(col, dict) and not col.get("aggregationFn") and col.get("id")
        }
        excluded_ids = set(row_meta_keys) | pinned_ids | request_dimension_ids
        # Include a stable filter fingerprint so that pivot column sets computed under
        # one filter are not reused after the filter changes (which can change which
        # column values exist and cause wrong columns to be windowed into the response).
        import hashlib as _hashlib, json as _json
        _filter_fp = _hashlib.md5(
            _json.dumps(request.filters or {}, sort_keys=True, default=str).encode()
        ).hexdigest()[:8]
        _cols_fp = _hashlib.md5(
            _json.dumps(request.columns or [], sort_keys=True, default=str).encode()
        ).hexdigest()[:8]
        cache_key = (request.table, frozenset(excluded_ids), _filter_fp, _cols_fp)

        if needs_col_schema or cache_key not in self._center_col_ids_cache:
            # Build authoritative center order from response column metadata first.
            # This keeps frontend column virtual indices aligned with backend slicing.
            center_col_ids = self._get_center_col_ids_from_columns(
                tanstack_result.columns, excluded_ids
            )
            if not center_col_ids:
                center_col_ids = self._get_center_col_ids_from_rows(
                    tanstack_result.data, row_meta_keys, excluded_ids
                )
            self._center_col_ids_cache[cache_key] = center_col_ids
        else:
            center_col_ids = self._center_col_ids_cache[cache_key]

        if needs_col_schema:
            tanstack_result.col_schema = {
                'total_center_cols': len(center_col_ids),
                'columns': [{'index': i, 'id': col_id, 'size': 140}
                            for i, col_id in enumerate(center_col_ids)]
            }

        if col_end is not None and center_col_ids:
            safe_end = min(col_end, len(center_col_ids) - 1)
            window_ids = set(center_col_ids[col_start:safe_end + 1])
            keep_ids = window_ids | pinned_ids

            tanstack_result.data = [
                {k: v for k, v in row.items() if k in row_meta_keys or k in keep_ids}
                for row in tanstack_result.data
            ]

        return tanstack_result

    def load_data(self, data, table_name: str) -> None:
        """
        Load data into the pivot engine from any supported source type.

        Accepts pandas DataFrame, polars DataFrame, Ibis table expression,
        connection string dict, or PyArrow Table. Auto-detects the type and
        converts to the engine's native Arrow representation.

        Parameters
        ----------
        data : pd.DataFrame | pl.DataFrame | ibis.Table | dict | pa.Table
            The data source.
        table_name : str
            The name under which the table will be registered. Use the same
            table_name in pivot requests to query this data.

        Raises
        ------
        DataInputError
            If the data type is not supported.
        """
        from .data_input import normalize_data_input  # noqa: PLC0415  (lazy import)
        normalize_data_input(data, table_name, self.controller)

    def _log_request(self, method: str, request: "TanStackRequest", **extra):
        if not self._debug:
            return
        _adapter_logger.debug(
            "adapter_request",
            extra={
                "method": method,
                "table": getattr(request, "table", None),
                "grouping": getattr(request, "grouping", None),
                "totals": getattr(request, "totals", None),
                "filters_keys": list((getattr(request, "filters", None) or {}).keys()),
                **extra,
            }
        )

    def _log_response(self, method: str, rows: list):
        if not self._debug:
            return
        total_rows = [r for r in rows if r.get("_isTotal") or r.get("_path") == "__grand_total__"]
        _adapter_logger.debug(
            "adapter_response",
            extra={
                "method": method,
                "row_count": len(rows),
                "total_row_count": len(total_rows),
                "has_grand_total": any(r.get("_id") == "Grand Total" for r in rows),
            }
        )

    @staticmethod
    def _normalize_window_fn(window_fn: Optional[str]) -> Optional[str]:
        if not window_fn:
            return None
        fn = str(window_fn).strip().lower()
        mapping = {
            "percent_of_total": "percent_of_grand_total",
            "percent_of_grand_total": "percent_of_grand_total",
            "percent_of_row": "percent_of_row",
            "percent_of_col": "percent_of_col",
        }
        return mapping.get(fn)

    @staticmethod
    def _numeric_or_none(value: Any) -> Optional[float]:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            if isinstance(value, float) and math.isnan(value):
                return None
            return float(value)
        return None

    def _apply_pivot_window_functions(self, rows: List[Dict[str, Any]], request: TanStackRequest) -> None:
        """
        Apply pivot-window functions (% row/col/grand-total) on already aggregated pivot rows.

        In pivot mode the planner materializes dynamic columns first; this post-step applies
        the window transformation expected by the frontend value config.
        """
        if not rows:
            return

        grouping_ids = set(request.grouping or [])
        has_column_dimensions = any(
            isinstance(col, dict)
            and col.get("id") not in grouping_ids
            and not col.get("aggregationFn")
            for col in (request.columns or [])
        )

        measure_windows = []
        for col in (request.columns or []):
            if not isinstance(col, dict) or not col.get("aggregationFn"):
                continue
            measure_id = col.get("id")
            normalized_window = self._normalize_window_fn(col.get("windowFn"))
            if measure_id and normalized_window:
                measure_windows.append((measure_id, normalized_window))

        if not measure_windows:
            return

        all_keys = set()
        for row in rows:
            if isinstance(row, dict):
                all_keys.update(row.keys())

        grand_total_row = next((row for row in rows if _is_grand_total_row(row)), None)
        non_grand_rows = [row for row in rows if isinstance(row, dict) and not _is_grand_total_row(row)]

        for measure_id, window_fn in measure_windows:
            if has_column_dimensions:
                pivot_keys = sorted(
                    key for key in all_keys
                    if isinstance(key, str)
                    and key.endswith(f"_{measure_id}")
                    and not key.startswith("__RowTotal__")
                )
            else:
                pivot_keys = [measure_id] if measure_id in all_keys else []
            if not pivot_keys:
                continue

            row_total_key = f"__RowTotal__{measure_id}"

            if window_fn == "percent_of_row":
                target_rows = non_grand_rows + ([grand_total_row] if isinstance(grand_total_row, dict) else [])
                for row in target_rows:
                    denom = self._numeric_or_none(row.get(row_total_key))
                    if denom is None:
                        denom = sum(self._numeric_or_none(row.get(k)) or 0.0 for k in pivot_keys)
                    if not denom:
                        for key in pivot_keys:
                            if self._numeric_or_none(row.get(key)) is not None:
                                row[key] = None
                        if self._numeric_or_none(row.get(row_total_key)) is not None:
                            row[row_total_key] = None
                        continue
                    for key in pivot_keys:
                        val = self._numeric_or_none(row.get(key))
                        if val is not None:
                            row[key] = val / denom
                    if self._numeric_or_none(row.get(row_total_key)) is not None:
                        row[row_total_key] = 1.0

            elif window_fn == "percent_of_col":
                col_denoms: Dict[str, float] = {}
                for key in pivot_keys:
                    denom = self._numeric_or_none(grand_total_row.get(key)) if isinstance(grand_total_row, dict) else None
                    if denom is None:
                        denom = sum(self._numeric_or_none(row.get(key)) or 0.0 for row in non_grand_rows)
                    col_denoms[key] = denom or 0.0

                grand_total_value = self._numeric_or_none(grand_total_row.get(row_total_key)) if isinstance(grand_total_row, dict) else None
                if grand_total_value is None:
                    grand_total_value = sum(self._numeric_or_none(row.get(row_total_key)) or 0.0 for row in non_grand_rows)

                for row in non_grand_rows:
                    for key in pivot_keys:
                        val = self._numeric_or_none(row.get(key))
                        denom = col_denoms.get(key, 0.0)
                        if val is not None:
                            row[key] = (val / denom) if denom else None
                    row_total_val = self._numeric_or_none(row.get(row_total_key))
                    if row_total_val is not None:
                        row[row_total_key] = (row_total_val / grand_total_value) if grand_total_value else None

                if isinstance(grand_total_row, dict):
                    for key in pivot_keys:
                        denom = col_denoms.get(key, 0.0)
                        if self._numeric_or_none(grand_total_row.get(key)) is not None:
                            grand_total_row[key] = 1.0 if denom else None
                    if has_column_dimensions and self._numeric_or_none(grand_total_row.get(row_total_key)) is not None:
                        grand_total_row[row_total_key] = 1.0 if grand_total_value else None

            elif window_fn == "percent_of_grand_total":
                grand_total_value = None
                if has_column_dimensions and isinstance(grand_total_row, dict):
                    grand_total_value = self._numeric_or_none(grand_total_row.get(row_total_key))
                if grand_total_value is None and isinstance(grand_total_row, dict):
                    grand_total_value = sum(self._numeric_or_none(grand_total_row.get(key)) or 0.0 for key in pivot_keys)
                if grand_total_value is None:
                    if has_column_dimensions:
                        grand_total_value = sum(self._numeric_or_none(row.get(row_total_key)) or 0.0 for row in non_grand_rows)
                    else:
                        grand_total_value = sum(
                            self._numeric_or_none(row.get(key)) or 0.0
                            for row in non_grand_rows
                            for key in pivot_keys
                        )
                if not grand_total_value:
                    continue

                target_rows = non_grand_rows + ([grand_total_row] if isinstance(grand_total_row, dict) else [])
                for row in target_rows:
                    for key in pivot_keys:
                        val = self._numeric_or_none(row.get(key))
                        if val is not None:
                            row[key] = val / grand_total_value
                    row_total_val = self._numeric_or_none(row.get(row_total_key))
                    if has_column_dimensions and row_total_val is not None:
                        row[row_total_key] = row_total_val / grand_total_value

    def convert_tanstack_request_to_pivot_spec(self, request: TanStackRequest) -> PivotSpec:
        """Convert TanStack request to PivotSpec format"""
        # Extract grouping columns as hierarchy
        hierarchy_cols = request.grouping or []
        
        # Extract measure columns
        measures = []
        value_cols = []
        
        for col in request.columns:
            if col.get('aggregationFn'):
                # This is an aggregation column
                window_fn = col.get('windowFn')
                planner_window_fn = (
                    None if self._normalize_window_fn(window_fn) is not None else window_fn
                )
                measures.append(Measure(
                    field=col.get('aggregationField', col['id']),
                    agg=col.get('aggregationFn', 'sum'),
                    alias=col['id'],
                    weighted_field=col.get('weightField'),
                    window_func=planner_window_fn
                ))
            elif col['id'] not in hierarchy_cols and col['id'] not in ('_id', 'depth', 'hierarchy', 'subRows'):
                # This is a value column
                value_cols.append(col['id'])
        
        
        pivot_filters = []
        if request.filters:
            for field_name, filter_obj in request.filters.items():
                if not field_name or field_name in ('__request_unique__', '__row_number__', 'hierarchy'):
                    continue
                if filter_obj is None:
                    continue

                # Global search: convert to OR-contains across all row dimension fields
                if field_name == 'global':
                    search_val = filter_obj if isinstance(filter_obj, str) else (
                        filter_obj.get('value') if isinstance(filter_obj, dict) else None
                    )
                    if search_val and str(search_val).strip() and hierarchy_cols:
                        pivot_filters.append({
                            'op': 'OR',
                            'conditions': [
                                {'field': f, 'op': 'contains', 'value': str(search_val), 'caseSensitive': False}
                                for f in hierarchy_cols
                            ]
                        })
                    continue

                if isinstance(filter_obj, dict):
                    if 'conditions' in filter_obj and 'operator' in filter_obj:
                        # This is a multi-condition filter block
                        conditions = []
                        for cond in filter_obj.get('conditions', []):
                            conditions.append({
                                'field': field_name,
                                'op': self._map_tanstack_operator(cond.get('type', 'eq')),
                                'value': cond.get('value'),
                                'caseSensitive': cond.get('caseSensitive', False)
                            })

                        if conditions:
                            pivot_filters.append({
                                'op': filter_obj['operator'],
                                'conditions': conditions
                            })
                    else:
                        # This is a single-condition filter dict
                        pivot_filters.append({
                            'field': field_name,
                            'op': self._map_tanstack_operator(filter_obj.get('type', 'eq')),
                            'value': filter_obj.get('value'),
                            'caseSensitive': filter_obj.get('caseSensitive', False)
                        })
                elif isinstance(filter_obj, list):
                    # TanStack sometimes sends an array of condition objects directly
                    conditions = [
                        {
                            'field': field_name,
                            'op': self._map_tanstack_operator(item.get('type', 'eq')),
                            'value': item.get('value'),
                            'caseSensitive': item.get('caseSensitive', False)
                        }
                        for item in filter_obj
                        if isinstance(item, dict) and item.get('value') not in (None, '')
                    ]
                    if len(conditions) == 1:
                        pivot_filters.append(conditions[0])
                    elif len(conditions) > 1:
                        pivot_filters.append({'op': 'AND', 'conditions': conditions})
                elif isinstance(filter_obj, str) and filter_obj.strip() != '':
                    # Support simple string filters (e.g. from quick input)
                    pivot_filters.append({
                        'field': field_name,
                        'op': 'contains',
                        'value': filter_obj,
                        'caseSensitive': False
                    })

        
        # Convert TanStack sorting to PivotSpec sorting.
        # The hierarchy column has id="hierarchy" in the frontend but the backend
        # needs the actual first row dimension field name.
        pivot_sort = []
        for sort_spec in request.sorting:
            col_id = sort_spec['id']
            if col_id == 'hierarchy' and request.grouping:
                col_id = request.grouping[0]

            sort_type = str(sort_spec.get("sortType") or "").strip().lower()
            sort_key_field = sort_spec.get("sortKeyField")
            semantic_type = sort_spec.get("semanticType") or sort_spec.get("sortSemantic")
            if not semantic_type and isinstance(col_id, str):
                # Safe heuristic fallback so common tenor fields sort naturally without
                # forcing every caller to send explicit semantic metadata.
                lower_col = col_id.lower()
                if any(token in lower_col for token in ("tenor", "maturity", "term")):
                    semantic_type = "tenor"
            if not semantic_type and sort_type == "curve_pillar_tenor":
                # Fallback semantic hint when caller requested curve tenor semantics
                # but did not provide an explicit semanticType.
                semantic_type = "tenor"

            sort_item = {
                'field': col_id,
                'order': 'desc' if sort_spec.get('desc') is True else 'asc'
            }
            if semantic_type:
                sort_item["semanticType"] = semantic_type
            if sort_type:
                sort_item["sortType"] = sort_type
            if isinstance(sort_key_field, str) and sort_key_field:
                sort_item["sortKeyField"] = sort_key_field
            
            # If no sortKeyField was provided but we have a tenor semantic,
            # we check if a standard __sortkey__ field exists in the schema.
            # (The planner will then ensure it's in the GROUP BY).
            if semantic_type == "tenor" and not sort_item.get("sortKeyField"):
                possible_key = f"__sortkey__{col_id}"
                # We don't have the table here, so we'll let the planner/builder 
                # handle the final resolution, but we can pass the hint.
                sort_item["sortKeyField"] = possible_key

            if sort_spec.get("nulls"):
                sort_item["nulls"] = sort_spec.get("nulls")
            pivot_sort.append(sort_item)
        
        # Handle pagination
        offset = 0
        limit = 1000  # Default
        
        if request.pagination:
            page_size = request.pagination.get('pageSize', 100)
            page = request.pagination.get('pageIndex', 0)
            offset = page * page_size
            limit = page_size
            
        # Parse column_cursor from global_filter
        column_cursor = None
        if request.global_filter and request.global_filter.startswith("column_cursor:"):
            column_cursor = request.global_filter.replace("column_cursor:", "", 1)
            
        from pivot_engine.types.pivot_spec import PivotConfig
        
        spec = PivotSpec(
            table=request.table,
            rows=hierarchy_cols,
            columns=value_cols,  # Map non-grouped dimensions to column pivots
            measures=measures,
            filters=pivot_filters,
            sort=pivot_sort,
            limit=limit,
            totals=request.totals if request.totals is not None else True,  # Enable totals computation
            pivot_config=PivotConfig(
                enabled=True,
                column_cursor=column_cursor,
                include_totals_column=request.row_totals if request.row_totals is not None else False
            ),
            column_sort_options=request.column_sort_options,
        )
        return spec
    
    def _map_tanstack_operator(self, tanstack_op: str) -> str:
        """Map TanStack filter operators to pivot engine operators"""
        mapping = {
            'eq': '=',
            'ne': '!=',
            'lt': '<',
            'gt': '>',
            'lte': '<=',
            'gte': '>=',
            'contains': 'contains',
            'startsWith': 'starts_with',
            'endsWith': 'ends_with',
            'in': 'in',
            'notIn': 'not in'
        }
        return mapping.get(tanstack_op, '=')
    
    def convert_pivot_result_to_tanstack_format(self, pivot_result: Any, 
                                               tanstack_request: TanStackRequest,
                                               version: Optional[int] = None) -> TanStackResponse:
        """Convert pivot engine result to TanStack format"""
        import pyarrow as pa
        hidden_sort_prefix = "__sortkey__"
        
        # Convert from pivot format to TanStack row format
        rows = []
        
        if isinstance(pivot_result, pa.Table):
            # Optimized vectorised conversion using PyArrow
            rows = pivot_result.to_pylist()
        elif isinstance(pivot_result, dict) and 'rows' in pivot_result and 'columns' in pivot_result:
            # Dict format with columns and rows
            pivot_columns = pivot_result['columns']
            pivot_rows = pivot_result['rows']
            
            for pivot_row in pivot_rows:
                tanstack_row = {}
                # Check if pivot_row is a list/tuple or a dict
                if isinstance(pivot_row, (list, tuple)):
                    for i, col_name in enumerate(pivot_columns):
                        tanstack_row[col_name] = pivot_row[i] if i < len(pivot_row) else None
                elif isinstance(pivot_row, dict):
                    # Already a dict, just use it
                    tanstack_row = pivot_row
                rows.append(tanstack_row)
        elif isinstance(pivot_result, list):
            # Already in row format
            rows = pivot_result

        # Hidden sort-key fields are transport-only helpers.
        # Keep them available for backend ORDER BY, but never expose them to the frontend.
        if rows:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                hidden_keys = [
                    key for key in row.keys()
                    if isinstance(key, str) and key.startswith(hidden_sort_prefix)
                ]
                for key in hidden_keys:
                    row.pop(key, None)
        
        # Enrich rows for TanStack Hierarchy display
        hierarchy_cols = tanstack_request.grouping or []
        if hierarchy_cols:
            for row in rows:
                # Normalize depth (support both depth and _depth)
                current_depth = row.get('depth')
                if current_depth is None:
                    current_depth = row.get('_depth', 0)
                row['depth'] = current_depth

                # Populate _id if missing
                if '_id' not in row:
                    is_grand_total = False
                    # Check for Grand Total (first grouping column missing/NaN)
                    first_col = hierarchy_cols[0]
                    if first_col in row and _is_missing_value(row[first_col]):
                        row['_id'] = 'Grand Total'
                        row['_isTotal'] = True
                        is_grand_total = True
                    
                    if not is_grand_total:
                        # Find the correct dimension for this depth
                        if current_depth < len(hierarchy_cols):
                            target_col = hierarchy_cols[current_depth]
                            target_val = row.get(target_col)
                            row['_id'] = "" if _is_missing_value(target_val) else target_val
                        else:
                            # Fallback: deepest non-None
                            for col in reversed(hierarchy_cols):
                                if col in row and not _is_missing_value(row[col]):
                                    row['_id'] = row[col]
                                    break
                        
                        if '_id' not in row:
                            row['_id'] = ""
                
                # Populate _path for row identification (critical for expansion)
                if '_path' not in row:
                    if row.get('_isTotal'):
                        row['_path'] = '__grand_total__'
                    elif hierarchy_cols:
                        # Construct path based on depth
                        path_parts = []
                        target_depth_idx = min(current_depth, len(hierarchy_cols) - 1)

                        for i in range(target_depth_idx + 1):
                            col = hierarchy_cols[i]
                            val = row.get(col)
                            if _is_missing_value(val):
                                break
                            path_parts.append(str(val))
                        
                        row['_path'] = "|||".join(path_parts) if path_parts else str(row.get('_id', ''))
                    else:
                        row['_path'] = str(id(row))

        # Apply pivot window functions (% row/% col/% grand total) after hierarchy
        # metadata is normalized, so grand-total detection is stable.
        self._apply_pivot_window_functions(rows, tanstack_request)

        # Calculate pagination info if needed
        pagination = None
        if tanstack_request.pagination:
            total_rows = len(rows)  # In real implementation, get actual total
            pagination = {
                'totalRows': total_rows,
                'pageSize': tanstack_request.pagination.get('pageSize', 100),
                'pageIndex': tanstack_request.pagination.get('pageIndex', 0),
                'pageCount': (total_rows + tanstack_request.pagination.get('pageSize', 100) - 1) // tanstack_request.pagination.get('pageSize', 100) if total_rows else 0
            }
        
        # Dynamic Column Generation for Pivot
        # If we have pivot columns, we need to update the response columns
        response_columns = tanstack_request.columns
        measure_ids = {c['id'] for c in tanstack_request.columns if c.get('aggregationFn')}
        grouping_ids = set(tanstack_request.grouping or [])
        has_column_dimensions = any(
            c.get('id') not in grouping_ids and not c.get('aggregationFn')
            for c in tanstack_request.columns
        )

        # In pivot mode (column dimensions present), base measure columns are placeholders.
        # Keep only expanded dynamic pivot columns to avoid showing extra plain measures.
        if has_column_dimensions and measure_ids:
            response_columns = [c for c in response_columns if c.get('id') not in measure_ids]

        # Detect dynamic columns using all rows in the response window, not only rows[0].
        if rows:
            result_keys = set()
            for row in rows:
                if isinstance(row, dict):
                    result_keys.update(row.keys())

            known_ids = {c['id'] for c in response_columns if isinstance(c, dict) and c.get('id')}
            meta_keys = {
                '_id', '_path', '_isTotal', 'depth', '_depth', 'hierarchy',
                '_level', '_expanded', '_parentPath', '_has_children', '_is_expanded',
                'subRows', 'uuid', '__virtualIndex'
            }

            new_columns = []
            for key in sorted(result_keys):
                if key in known_ids or key in meta_keys:
                    continue
                if isinstance(key, str) and key.startswith(hidden_sort_prefix):
                    continue

                if key.startswith("__RowTotal__"):
                    measure_key = key.replace("__RowTotal__", "")
                    header = f"Total {measure_key.replace('_', ' ').title()}"
                    new_columns.append({
                        'id': key,
                        'header': header,
                        'accessorKey': key,
                        'isRowTotal': True
                    })
                else:
                    new_columns.append({
                        'id': key,
                        'header': key.replace('_', ' ').title(),
                        'accessorKey': key,
                    })

            if new_columns:
                existing_ids = {c.get('id') for c in response_columns if isinstance(c, dict)}
                for col in new_columns:
                    if col['id'] not in existing_ids:
                        response_columns.append(col)

        response_columns = [
            col for col in response_columns
            if not (
                isinstance(col, dict)
                and isinstance(col.get("id"), str)
                and col.get("id").startswith(hidden_sort_prefix)
            )
        ]

        return TanStackResponse(
            data=rows,
            columns=response_columns,
            pagination=pagination,
            total_rows=len(rows) if rows else 0,
            version=version
        )
    
    async def handle_update(self, request: TanStackRequest, update_payload: Dict[str, Any]) -> bool:
        """
        Handle a cell update request from the frontend.
        """
        table_name = request.table
        
        row_id = update_payload.get('rowId')
        col_id = update_payload.get('colId')
        new_value = update_payload.get('value')
        
        if not row_id or not col_id:
            return False
            
        # Determine Key Columns based on hierarchy
        # The row_id is a "|||" separated string of dimension values
        hierarchy_cols = request.grouping or []
        key_columns = {}
        
        # If we have a hierarchy, parse the path
        if hierarchy_cols:
             parts = str(row_id).split('|||')
             # Note: If parts < len(hierarchy_cols), it might be an aggregation row (Total).
             # We generally should not allow editing totals unless it means "allocate".
             # For now, we proceed if we can match keys.
             
             for i, col in enumerate(hierarchy_cols):
                 if i < len(parts):
                     val = parts[i]
                     # Attempt to restore type if possible?
                     # Everything in path is string.
                     # Backend SQL usually handles string-to-number casting if quoted correctly.
                     key_columns[col] = val
        else:
             # Flat table mode. 
             # If row_id is just an index (string), we can't update without a PK.
             # But if the data has an _id or PK, it should be used.
             # The frontend uses row index if no ID.
             # We assume the user has configured unique keys in 'rowFields' even if it looks flat.
             pass

        if not key_columns:
             print("Warning: No key columns identified for update.")
             return False

        # Determine Target Column
        # Map frontend column ID to backend field name
        target_col = col_id
        for col in request.columns:
             if col['id'] == col_id:
                 if 'aggregationField' in col:
                     target_col = col['aggregationField']
                 elif 'accessorKey' in col:
                     target_col = col['accessorKey']
                 break
        
        if hasattr(self.controller, 'update_record'):
             return await self.controller.update_record(table_name, key_columns, {target_col: new_value})
             
        return False

    async def handle_drill_through(self, request: TanStackRequest, drill_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Handle a drill through request.
        """
        # 1. Convert request to spec to get base filters
        spec = self.convert_tanstack_request_to_pivot_spec(request)
        
        # 2. Extract drill filters from payload
        drill_filters_raw = drill_payload.get('filters', {})
        
        # Convert frontend filter format to pivot engine format
        # This mirrors logic in convert_tanstack_request_to_pivot_spec
        drill_filters = []
        for field_name, filter_obj in drill_filters_raw.items():
            if isinstance(filter_obj, dict):
                if 'conditions' in filter_obj:
                    for cond in filter_obj['conditions']:
                        drill_filters.append({
                            'field': field_name,
                            'op': self._map_tanstack_operator(cond.get('type', 'eq')),
                            'value': cond.get('value')
                        })
                else:
                    drill_filters.append({
                        'field': field_name,
                        'op': self._map_tanstack_operator(filter_obj.get('type', 'eq')),
                        'value': filter_obj.get('value')
                    })
        
        # 3. Call controller (returns dict with 'rows' and 'total_rows')
        result = await self.controller.get_drill_through_data(spec, drill_filters)
        return result['rows']

    async def handle_request(self, request: TanStackRequest, user: Optional[User] = None) -> TanStackResponse:
        """Handle a TanStack request directly"""
        if request.operation == TanStackOperation.GET_UNIQUE_VALUES:
            # Logic for unique values (used by Excel-like filter)
            column_id = request.global_filter # Overload global_filter to pass the column
            unique_values = await self.get_unique_values(request.table, column_id, request.filters)
            return TanStackResponse(data=[{"value": v} for v in unique_values], columns=[])

        # Convert request to pivot spec
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)
        
        # Apply RLS if user is provided
        if user:
            pivot_spec = apply_rls_to_spec(pivot_spec, user)
        
        # Execute pivot operation asynchronously
        pivot_result = await self.controller.run_pivot_async(pivot_spec, return_format="dict")
        
        # Convert result to TanStack format
        tanstack_result = self.convert_pivot_result_to_tanstack_format(pivot_result, request)
        
        return tanstack_result
    
    async def handle_hierarchical_request(self, request: TanStackRequest,
                                        expanded_paths: Union[List[List[str]], bool],
                                        user_preferences: Optional[Dict[str, Any]] = None) -> TanStackResponse:
        """Handle hierarchical TanStack request with expansion state"""
        self._log_request("handle_hierarchical_request", request)
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)

        # Handle "Expand All" case
        target_paths = expanded_paths
        if expanded_paths is True:
            # Signal to load all levels/nodes
            target_paths = [['__ALL__']]

        if hasattr(self.controller, 'run_hierarchy_view'):
            hierarchy_view = await self.controller.run_hierarchy_view(pivot_spec, target_paths)
            tanstack_result = self.convert_pivot_result_to_tanstack_format(
                hierarchy_view.get("rows", []), request
            )
            if hierarchy_view.get("total_rows") is not None:
                tanstack_result.total_rows = hierarchy_view["total_rows"]
            tanstack_result.data = _order_hierarchical_rows(
                _move_grand_total_to_end(_dedup_grand_total(tanstack_result.data))
            )
            self._log_response("handle_hierarchical_request", tanstack_result.data)
            return tanstack_result

        # We use the batch loading method which is more efficient for multiple levels
        # and returns the {path_key: [nodes]} format expected by the traversal.
        try:
            if hasattr(self.controller, 'run_hierarchical_pivot_batch_load'):
                hierarchy_result = await self.controller.run_hierarchical_pivot_batch_load(
                    pivot_spec.to_dict(), target_paths, max_levels=len(pivot_spec.rows)
                )
            else:
                # Fallback to direct query or other method
                hierarchy_result = self.controller.run_progressive_hierarchical_load(
                     pivot_spec, target_paths, user_preferences=user_preferences
                )
                # If it's the levels/metadata format, we need to convert it
                if isinstance(hierarchy_result, dict) and 'levels' in hierarchy_result:
                    new_result = {}
                    for level_info in hierarchy_result['levels']:
                        path_key = "|||".join(str(v) for v in level_info['path'])
                        data = level_info['data']
                        if hasattr(data, 'to_pylist'):
                            new_result[path_key] = data.to_pylist()
                        else:
                            new_result[path_key] = data
                    hierarchy_result = new_result
        except Exception as e:
            print(f"Hierarchical load failed: {e}, falling back to direct query")
            # Fallback to direct query without materialized hierarchies
            result = self.controller.run_pivot_async(pivot_spec, return_format="dict")
            if asyncio.iscoroutine(result):
                hierarchy_result = await result
            else:
                hierarchy_result = result
            tanstack_result = self.convert_pivot_result_to_tanstack_format(
                hierarchy_result, request
            )
            tanstack_result.data = _order_hierarchical_rows(
                _move_grand_total_to_end(_dedup_grand_total(tanstack_result.data))
            )
            return tanstack_result

        # Reconstruct the flat list of visible rows from the hierarchy result
        visible_rows = []
        grand_total_emitted = False  # Boolean flag: at most one grand total row allowed

        # Convert target_paths to a set of strings for fast lookup during traversal
        # This represents which paths are currently expanded
        expanded_path_set = set()
        if isinstance(target_paths, list):
            for path in target_paths:
                if isinstance(path, list):
                    expanded_path_set.add("|||".join(str(item) for item in path))

        # Depth-First Traversal to ensure correct tree order (Parent -> Children)
        def traverse(parent_key):
            nodes = hierarchy_result.get(parent_key, [])

            # Current depth based on parent key
            current_depth = 0
            if parent_key:
                current_depth = len(parent_key.split('|||'))

            for node in nodes:
                # Ensure node is a dict (it should be if controller returns to_pylist())
                if not isinstance(node, dict):
                    continue

                # Check for grand total duplicates
                first_dim = pivot_spec.rows[0] if pivot_spec.rows else None
                is_grand_total = (
                    current_depth == 0
                    and first_dim is not None
                    and _is_missing_value(node.get(first_dim))
                )
                if is_grand_total:
                    nonlocal grand_total_emitted
                    if grand_total_emitted:
                        continue  # skip duplicate grand total
                    grand_total_emitted = True

                # SKIP Subtotals/Totals in child levels to avoid duplication
                target_dim_idx = current_depth

                if target_dim_idx < len(pivot_spec.rows):
                    target_dim = pivot_spec.rows[target_dim_idx]

                    # If this is a child level, the value for this dimension must not be None
                    # (unless it's truly a None value in the data, but usually None means subtotal)
                    if current_depth > 0 and _is_missing_value(node.get(target_dim)):
                        continue

                    # Populate _id correctly based on current depth dimension
                    if current_depth == 0 and _is_missing_value(node.get(target_dim)):
                        node['_id'] = 'Grand Total'
                        node['_isTotal'] = True
                    elif target_dim in node and not _is_missing_value(node.get(target_dim)):
                        node['_id'] = node[target_dim]

                # Populate depth
                node['depth'] = current_depth

                # Add node to visible list
                visible_rows.append(node)

                # Check for children - BUT ONLY traverse if this path is expanded
                # Construct the key for this node to see if it's a parent
                child_path_parts = []
                if parent_key:
                    child_path_parts = parent_key.split('|||')

                if target_dim_idx < len(pivot_spec.rows):
                    current_dim = pivot_spec.rows[target_dim_idx]
                    if current_dim in node and not _is_missing_value(node[current_dim]):
                        child_path_parts.append(str(node[current_dim]))

                        child_key = "|||".join(child_path_parts)

                        # ONLY traverse to children if this child_key is in the expanded paths
                        if child_key in hierarchy_result and child_key in expanded_path_set:
                            traverse(child_key)

        # Start traversal from root
        traverse("")

        # Convert to TanStack format
        tanstack_result = self.convert_pivot_result_to_tanstack_format(
            visible_rows, request
        )
        tanstack_result.data = _order_hierarchical_rows(
            _move_grand_total_to_end(_dedup_grand_total(tanstack_result.data))
        )

        self._log_response("handle_hierarchical_request", visible_rows)
        return tanstack_result

    # _apply_expansion_state removed as it is now handled by the controller/tree manager logic

    async def handle_virtual_scroll_request(self, request: TanStackRequest,
                                          start_row: int, end_row: int,
                                          expanded_paths: Union[List[List[str]], bool] = None,
                                          user: Optional[User] = None,
                                          col_start: int = 0,
                                          col_end: Optional[int] = None,
                                          needs_col_schema: bool = False,
                                          include_grand_total: bool = False) -> TanStackResponse:
        """Handle virtual scrolling request with start/end row indices"""
        # Convert request to pivot spec
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)

        # Apply RLS if user is provided
        if user:
            pivot_spec = apply_rls_to_spec(pivot_spec, user)

        # Handle "Expand All" case
        target_paths = expanded_paths or []
        if expanded_paths is True:
            target_paths = [['__ALL__']]

        if hasattr(self.controller, 'run_hierarchy_view'):
            try:
                hierarchy_view = await self.controller.run_hierarchy_view(
                    pivot_spec,
                    target_paths,
                    start_row,
                    end_row,
                    include_grand_total_row=include_grand_total,
                )
            except TypeError:
                # Backward compatibility with older controller signatures.
                hierarchy_view = await self.controller.run_hierarchy_view(
                    pivot_spec, target_paths, start_row, end_row
                )
            tanstack_result = self.convert_pivot_result_to_tanstack_format(
                hierarchy_view.get("rows", []), request, version=request.version
            )
            if hierarchy_view.get("total_rows") is not None:
                tanstack_result.total_rows = hierarchy_view["total_rows"]

            if include_grand_total and request.totals:
                grand_total_row = hierarchy_view.get("grand_total_row")
                if isinstance(grand_total_row, dict):
                    normalized_total = self.convert_pivot_result_to_tanstack_format(
                        [grand_total_row], request, version=request.version
                    ).data
                    if normalized_total:
                        grand_total_row = normalized_total[0]
                if isinstance(grand_total_row, dict):
                    existing_rows = tanstack_result.data or []
                    if not any(_is_grand_total_row(row) for row in existing_rows):
                        tanstack_result.data = [*existing_rows, grand_total_row]

            tanstack_result.data = _order_hierarchical_rows(
                _move_grand_total_to_end(_dedup_grand_total(tanstack_result.data))
            )
            if hierarchy_view.get("color_scale_stats"):
                tanstack_result.color_scale_stats = hierarchy_view["color_scale_stats"]
            return self._apply_col_windowing(tanstack_result, request, col_start, col_end, needs_col_schema)

        # Use the controller's virtual scrolling method
        if hasattr(self.controller, 'run_virtual_scroll_hierarchical'):
            # For hierarchical virtual scrolling
            try:
                virtual_result = self.controller.run_virtual_scroll_hierarchical(
                    pivot_spec, start_row, end_row, target_paths
                )

                # Convert result to TanStack format (even if empty)
                tanstack_result = self.convert_pivot_result_to_tanstack_format(virtual_result, request, version=request.version)

                # Override total_rows with the ACTUAL count of all visible (expanded) hierarchical rows
                if hasattr(self.controller, 'virtual_scroll_manager'):
                    total_visible = self.controller.virtual_scroll_manager.get_total_visible_row_count(pivot_spec, target_paths)
                    if total_visible > 0:
                        tanstack_result.total_rows = total_visible

                # Virtual scroll already returns rows in correct window order from the backend.
                # Only deduplicate grand total — do NOT reorder, as _order_hierarchical_rows
                # uses local first_seen indices that shuffle partial windows incorrectly.
                tanstack_result.data = _move_grand_total_to_end(_dedup_grand_total(tanstack_result.data))
                return self._apply_col_windowing(tanstack_result, request, col_start, col_end, needs_col_schema)

            except Exception as e:
                print(f"Virtual scroll failed: {e}, falling back to hierarchical load")
                # Fallback to direct hierarchical load which is un-materialized but accurate
                fallback_result = await self.handle_hierarchical_request(request, expanded_paths)
                fallback_result.data = _order_hierarchical_rows(
                    _move_grand_total_to_end(_dedup_grand_total(fallback_result.data))
                )
                return self._apply_col_windowing(fallback_result, request, col_start, col_end, needs_col_schema)
        else:
            # Fallback: Use regular hierarchical method
            fallback_result = await self.handle_hierarchical_request(request, expanded_paths)
            fallback_result.data = _order_hierarchical_rows(
                _move_grand_total_to_end(_dedup_grand_total(fallback_result.data))
            )
            return self._apply_col_windowing(fallback_result, request, col_start, col_end, needs_col_schema)

    def get_schema_info(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for TanStack column configuration"""
        # Try to use backend's schema discovery first
        try:
            if hasattr(self.controller, 'backend') and hasattr(self.controller.backend, 'get_schema'):
                schema = self.controller.backend.get_schema(table_name)
                if schema:
                    columns_info = []
                    for col_name, col_type in schema.items():
                        columns_info.append({
                            'id': col_name,
                            'header': col_name.replace('_', ' ').title(),
                            'accessorKey': col_name,
                            'type': col_type,
                            'enableSorting': True,
                            'enableFiltering': True
                        })
                    
                    return {
                        'table': table_name,
                        'columns': columns_info,
                        'sample_data': [] # Fetching sample data could be separate if needed
                    }
        except Exception as e:
            print(f"Metadata schema retrieval failed: {e}")

        # Fallback: return mock schema based on the controller's data via sample query
        try:
            # Run a simple query to get column info
            spec = PivotSpec(
                table=table_name,
                rows=[],  # No grouping
                measures=[],
                filters=[]
            )
            sample_result = self.controller.run_pivot(spec, return_format="dict")
            
            if sample_result and sample_result.get('columns'):
                columns_info = []
                for col_name in sample_result['columns']:
                    # Determine column type (simplified)
                    col_type = "string"  # Default
                    # In a real implementation, this would analyze the data
                    columns_info.append({
                        'id': col_name,
                        'header': col_name.replace('_', ' ').title(),
                        'accessorKey': col_name,
                        'type': col_type,
                        'enableSorting': True,
                        'enableFiltering': True
                    })
                
                return {
                    'table': table_name,
                    'columns': columns_info,
                    'sample_data': sample_result.get('rows', [])[:5]  # First 5 rows as sample
                }
        except:
            pass
        
        # Return empty schema
        return {
            'table': table_name,
            'columns': [],
            'sample_data': []
        }
    
    async def get_grouped_data(self, request: TanStackRequest) -> TanStackResponse:
        """Handle grouped data request (for hierarchical tables)"""
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)
        
        # Use the controller to get grouped data
        pivot_result = await self.controller.run_pivot_async(pivot_spec, return_format="dict")
        
        # Format for TanStack grouping
        tanstack_result = self.convert_pivot_result_to_tanstack_format(pivot_result, request)
        
        # Add grouping information
        if request.grouping:
            tanstack_result.grouping = [{
                'id': group_col,
                'value': None  # Will be populated with actual grouped data
            } for group_col in request.grouping]
        
        return tanstack_result

    def get_invalidation_events(self, table_name: str, change_type: str) -> List[Dict[str, Any]]:
        # ... existing ...
        pass

    async def get_unique_values(self, table_name: str, column_id: str, filters: Dict[str, Any] = None) -> List[Any]:
        """Get unique values for a column, potentially filtered"""
        
        # Convert the filters from the request format to the spec format
        pivot_filters = []
        if filters:
            for field_name, filter_obj in filters.items():
                if not isinstance(filter_obj, dict) or field_name == '__request_unique__' or field_name == column_id: continue

                if 'conditions' in filter_obj and 'operator' in filter_obj:
                    conditions = []
                    for cond in filter_obj['conditions']:
                        conditions.append({
                            'field': field_name,
                            'op': self._map_tanstack_operator(cond.get('type', 'eq')),
                            'value': cond.get('value')
                        })
                    if conditions:
                        pivot_filters.append({'op': filter_obj['operator'], 'conditions': conditions})
                else:
                    pivot_filters.append({
                        'field': field_name,
                        'op': self._map_tanstack_operator(filter_obj.get('type', 'eq')),
                        'value': filter_obj.get('value')
                    })
        
        spec = PivotSpec(
            table=table_name,
            rows=[],
            columns=[],
            measures=[],
            filters=pivot_filters,
            limit=500 # Cap unique values for UI
        )
        
        # Use Ibis to get distinct values
        con = self.controller.backend.con
        table = con.table(table_name)
        
        # Apply filters
        from pivot_engine.common.ibis_expression_builder import IbisExpressionBuilder
        builder = IbisExpressionBuilder(con)
        filter_expr = builder.build_filter_expression(table, spec.filters)
        if filter_expr is not None:
            table = table.filter(filter_expr)
            
        # Get distinct values in stable sorted order for the filter list UI.
        query = table.select(column_id).distinct().order_by(column_id).limit(spec.limit)
        result = query.execute()
        return result[column_id].tolist()


# Utility function for TanStack integration
def create_tanstack_adapter(backend_uri: str = ":memory:", debug: bool = False) -> TanStackPivotAdapter:
    """Create a TanStack adapter with a configured controller"""
    controller = ScalablePivotController(
        backend_uri=backend_uri,
        enable_streaming=True,
        enable_incremental_views=True,
        tile_size=100,
        cache_ttl=300
    )
    return TanStackPivotAdapter(controller, debug=debug)


# Example usage functions
async def example_usage():
    """Example of how to use the TanStack adapter"""
    adapter = create_tanstack_adapter()
    
    # Example TanStack request
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[
            {"id": "region", "header": "Region", "enableSorting": True},
            {"id": "product", "header": "Product", "enableSorting": True},
            {"id": "total_sales", "header": "Total Sales", "aggregationFn": "sum", "aggregationField": "sales"}
        ],
        filters=[],
        sorting=[{"id": "total_sales", "desc": True}],
        grouping=["region", "product"],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 100}
    )
    
    result = await adapter.handle_request(request)
    print(f"Received {len(result.data)} rows from TanStack adapter")
    
    return result


if __name__ == "__main__":
    asyncio.run(example_usage())
