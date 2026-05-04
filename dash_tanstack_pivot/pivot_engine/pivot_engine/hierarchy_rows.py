"""Shared hierarchy row construction and normalization policy."""

from __future__ import annotations

import decimal
import math
from typing import Any, Dict, List, Optional, Tuple


GRAND_TOTAL_ID = "Grand Total"
GRAND_TOTAL_PATH = "__grand_total__"
EXPAND_ALL_TOKEN = "__ALL__"


def is_missing_hierarchy_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, decimal.Decimal) and value.is_nan():
        return True
    return False


def is_hierarchy_grand_total(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    return bool(
        row.get("_isTotal")
        or row.get("_path") == GRAND_TOTAL_PATH
        or row.get("_id") == GRAND_TOTAL_ID
    )


def normalize_expanded_paths(expanded_paths: Any) -> Tuple[Tuple[str, ...], ...]:
    if expanded_paths is True:
        return ((EXPAND_ALL_TOKEN,),)
    if expanded_paths == [[EXPAND_ALL_TOKEN]] or any(path == [EXPAND_ALL_TOKEN] for path in (expanded_paths or [])):
        return ((EXPAND_ALL_TOKEN,),)

    normalized = []
    for path in expanded_paths or []:
        if not isinstance(path, list) or not path:
            continue
        normalized.append(tuple("" if value is None else str(value) for value in path))
    return tuple(sorted(set(normalized)))


def expanded_path_state(expanded_paths: Any) -> Tuple[bool, set[str]]:
    normalized = normalize_expanded_paths(expanded_paths)
    expand_all = normalized == ((EXPAND_ALL_TOKEN,),)
    expanded_set = {
        "|||".join(path)
        for path in normalized
        if path != (EXPAND_ALL_TOKEN,)
    }
    return expand_all, expanded_set


def clone_hierarchy_rows(rows: Any) -> List[Dict[str, Any]]:
    return [dict(row) for row in (rows or []) if isinstance(row, dict)]


def _row_fields(spec: Any) -> List[str]:
    return list(getattr(spec, "rows", None) or [])


def build_visible_hierarchy_rows(
    spec: Any,
    hierarchy_result: Dict[str, List[Dict[str, Any]]],
    expanded_paths: Any,
    *,
    show_subtotal_footers: bool = False,
    tabular_subtotals: bool = False,
) -> List[Dict[str, Any]]:
    return list(iter_visible_hierarchy_rows(
        spec, hierarchy_result, expanded_paths,
        show_subtotal_footers=show_subtotal_footers,
        tabular_subtotals=tabular_subtotals,
    ))


def iter_visible_hierarchy_rows(
    spec: Any,
    hierarchy_result: Dict[str, List[Dict[str, Any]]],
    expanded_paths: Any,
    *,
    show_subtotal_footers: bool = False,
    tabular_subtotals: bool = False,
):
    """Build visible rows from batch-loaded hierarchy levels.

    This is the single policy for hierarchy row construction: normalize expanded
    paths, skip subtotal placeholders below root, emit at most one grand total,
    assign display id/depth metadata, and traverse parent before children.

    show_subtotal_footers: hierarchy/outline — after each expanded group's
        children emit a copy of the group row marked _isSubtotalFooter=True.
    tabular_subtotals: tabular layout — post-order traversal (children first,
        then parent as _isTabularSubtotal=True); all groups are force-expanded.
    """
    rows = _row_fields(spec)
    grand_total_emitted = False
    expand_all, expanded_path_set = expanded_path_state(expanded_paths)

    if tabular_subtotals:
        expand_all = True  # always show all levels in tabular subtotals mode

    def traverse(parent_key: str):
        nonlocal grand_total_emitted
        nodes = hierarchy_result.get(parent_key, [])
        current_depth = len(parent_key.split("|||")) if parent_key else 0

        for node in nodes:
            if not isinstance(node, dict):
                continue

            row = dict(node)
            first_dim = rows[0] if rows else None
            is_grand_total = (
                current_depth == 0
                and first_dim is not None
                and is_missing_hierarchy_value(row.get(first_dim))
            )
            if is_grand_total:
                if grand_total_emitted:
                    continue
                grand_total_emitted = True
                row["_id"] = GRAND_TOTAL_ID
                row["_isTotal"] = True
                row["_path"] = GRAND_TOTAL_PATH
                row["depth"] = current_depth
                yield row
                continue

            target_dim_idx = current_depth
            child_key = None
            if target_dim_idx < len(rows):
                target_dim = rows[target_dim_idx]
                if current_depth > 0 and is_missing_hierarchy_value(row.get(target_dim)):
                    continue
                if target_dim in row and not is_missing_hierarchy_value(row.get(target_dim)):
                    row["_id"] = row[target_dim]
                    # Build the full path for this row from parent + current dim value.
                    # Do this before yielding so downstream code (JS tabular mode) always
                    # gets a correct _path even when parent field values are absent from
                    # child-level rows returned by the data engine.
                    child_path_parts = parent_key.split("|||") if parent_key else []
                    child_path_parts.append(str(row[target_dim]))
                    child_key = "|||".join(child_path_parts)
                    if "_path" not in row:
                        row["_path"] = child_key
                    if "_pathFields" not in row:
                        row["_pathFields"] = list(rows[: current_depth + 1])

            row["depth"] = current_depth
            is_expanded = (
                child_key is not None
                and child_key in hierarchy_result
                and (expand_all or child_key in expanded_path_set)
            )

            if tabular_subtotals:
                # Post-order: children first, then this group row as a subtotal marker.
                # Leaf nodes (not expanded) are emitted as regular data rows.
                if is_expanded:
                    yield from traverse(child_key)
                    subtotal_row = dict(row)
                    subtotal_row["_isTabularSubtotal"] = True
                    yield subtotal_row
                else:
                    yield row
            else:
                # Pre-order: group row first, then children, then optional footer.
                yield row
                if is_expanded:
                    yield from traverse(child_key)
                    if show_subtotal_footers:
                        footer_row = dict(row)
                        footer_row["_isSubtotalFooter"] = True
                        yield footer_row

    yield from traverse("")


def _split_grand_total(rows: Any) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    regular_rows: List[Dict[str, Any]] = []
    grand_total_row: Optional[Dict[str, Any]] = None
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if is_hierarchy_grand_total(row):
            if grand_total_row is None:
                grand_total_row = dict(row)
            continue
        regular_rows.append(dict(row))
    return regular_rows, grand_total_row


def order_hierarchy_rows(rows: Any) -> List[Dict[str, Any]]:
    """Return parent-before-child order while preserving sibling discovery order."""
    regular_rows, grand_total_row = _split_grand_total(rows)
    if not regular_rows:
        return [grand_total_row] if grand_total_row is not None else []

    path_to_row: Dict[str, Dict[str, Any]] = {}
    first_seen: Dict[str, int] = {}
    subtree_first_seen: Dict[str, int] = {}
    children_by_parent: Dict[str, List[str]] = {}

    for index, row in enumerate(regular_rows):
        path = row.get("_path")
        if not path:
            continue
        path_to_row[str(path)] = row
        first_seen[str(path)] = index

    for path in path_to_row:
        subtree_first_seen[path] = min(
            first_seen[other_path]
            for other_path in path_to_row
            if other_path == path or other_path.startswith(f"{path}|||")
        )

    root_paths: List[str] = []
    for path in path_to_row:
        parent_path = path.rsplit("|||", 1)[0] if "|||" in path else None
        if parent_path and parent_path in path_to_row:
            children_by_parent.setdefault(parent_path, []).append(path)
        else:
            root_paths.append(path)

    def sort_paths(paths: List[str]) -> List[str]:
        return sorted(paths, key=lambda value: subtree_first_seen.get(value, first_seen.get(value, 0)))

    ordered_rows: List[Dict[str, Any]] = []
    visited: set[str] = set()

    def append_subtree(path: str) -> None:
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
        if not path or str(path) not in visited:
            ordered_rows.append(row)

    if grand_total_row is not None:
        ordered_rows.append(grand_total_row)
    return ordered_rows


def finalize_hierarchy_rows(rows: Any, *, preserve_window_order: bool = False) -> List[Dict[str, Any]]:
    """Deduplicate totals and place grand total consistently.

    Full materializations can be tree-ordered. Already-windowed result sets must
    keep backend order because reordering a partial viewport can move children
    before/after unrelated rows.
    """
    if preserve_window_order:
        regular_rows, grand_total_row = _split_grand_total(rows)
        return [*regular_rows, grand_total_row] if grand_total_row is not None else regular_rows
    return order_hierarchy_rows(rows)


def find_hierarchy_grand_total_row(rows: Any) -> Optional[Dict[str, Any]]:
    return next((dict(row) for row in (rows or []) if is_hierarchy_grand_total(row)), None)


def compute_hierarchy_color_scale_stats(rows: Any, row_fields: Any, col_fields: Any) -> Dict[str, Any]:
    meta_keys = _hierarchy_color_meta_keys(row_fields, col_fields)
    by_col: Dict[str, Dict[str, float]] = {}
    table_min = float("inf")
    table_max = float("-inf")

    for row in rows or []:
        table_min, table_max = _accumulate_hierarchy_color_stats(row, meta_keys, by_col, table_min, table_max)

    return _hierarchy_color_stats_result(by_col, table_min, table_max)


def _hierarchy_color_meta_keys(row_fields: Any, col_fields: Any) -> set:
    meta_keys = {
        "_id", "_path", "_isTotal", "depth", "_depth", "_level", "_expanded",
        "_parentPath", "_has_children", "_is_expanded", "subRows", "uuid",
        "__virtualIndex",
    }
    for field in row_fields or []:
        meta_keys.add(field)
    for field in col_fields or []:
        meta_keys.add(field)
    return meta_keys


def _accumulate_hierarchy_color_stats(
    row: Any,
    meta_keys: set,
    by_col: Dict[str, Dict[str, float]],
    table_min: float,
    table_max: float,
) -> Tuple[float, float]:
    if not isinstance(row, dict) or is_hierarchy_grand_total(row):
        return table_min, table_max
    for key, value in row.items():
        if key in meta_keys or not isinstance(value, (int, float)):
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        existing = by_col.get(key)
        if existing is None:
            by_col[key] = {"min": value, "max": value}
        else:
            if value < existing["min"]:
                existing["min"] = value
            if value > existing["max"]:
                existing["max"] = value
        if value < table_min:
            table_min = value
        if value > table_max:
            table_max = value
    return table_min, table_max


def _hierarchy_color_stats_result(
    by_col: Dict[str, Dict[str, float]],
    table_min: float,
    table_max: float,
) -> Dict[str, Any]:
    table_stats = None
    if table_min != float("inf") and table_max != float("-inf"):
        table_stats = {"min": table_min, "max": table_max}
    return {"byCol": by_col, "table": table_stats}


def build_hierarchy_row_window(
    spec: Any,
    hierarchy_result: Dict[str, List[Dict[str, Any]]],
    expanded_paths: Any,
    *,
    start_row: Optional[int] = None,
    end_row: Optional[int] = None,
    collect_formula_source_rows: bool = False,
    show_subtotal_footers: bool = False,
    tabular_subtotals: bool = False,
) -> Dict[str, Any]:
    has_window = start_row is not None and end_row is not None
    if not has_window:
        rows = build_visible_hierarchy_rows(
            spec, hierarchy_result, expanded_paths,
            show_subtotal_footers=show_subtotal_footers,
            tabular_subtotals=tabular_subtotals,
        )
        finalized_rows = finalize_hierarchy_rows(rows, preserve_window_order=False)
        grand_total_row = find_hierarchy_grand_total_row(finalized_rows)
        grand_total_formula_source_rows = [
            dict(row)
            for row in finalized_rows
            if isinstance(row, dict) and not is_hierarchy_grand_total(row) and row.get("depth") == 0
        ] if collect_formula_source_rows else []
        return {
            "rows": clone_hierarchy_rows(finalized_rows),
            "total_rows": len(finalized_rows),
            "grand_total_row": dict(grand_total_row) if isinstance(grand_total_row, dict) else grand_total_row,
            "grand_total_formula_source_rows": clone_hierarchy_rows(grand_total_formula_source_rows),
            "color_scale_stats": compute_hierarchy_color_scale_stats(
                finalized_rows,
                _row_fields(spec),
                getattr(spec, "columns", []),
            ),
            "full_rows": clone_hierarchy_rows(finalized_rows),
        }

    safe_start = max(int(start_row or 0), 0)
    safe_end = max(int(end_row), safe_start)
    window_rows: List[Dict[str, Any]] = []
    grand_total_row = None
    regular_count = 0
    grand_total_formula_source_rows: List[Dict[str, Any]] = []
    meta_keys = _hierarchy_color_meta_keys(_row_fields(spec), getattr(spec, "columns", []))
    by_col: Dict[str, Dict[str, float]] = {}
    table_min = float("inf")
    table_max = float("-inf")

    for row in iter_visible_hierarchy_rows(
        spec, hierarchy_result, expanded_paths,
        show_subtotal_footers=show_subtotal_footers,
        tabular_subtotals=tabular_subtotals,
    ):
        if is_hierarchy_grand_total(row):
            if grand_total_row is None:
                grand_total_row = dict(row)
            continue
        if collect_formula_source_rows and row.get("depth") == 0:
            grand_total_formula_source_rows.append(dict(row))
        table_min, table_max = _accumulate_hierarchy_color_stats(row, meta_keys, by_col, table_min, table_max)
        row_index = regular_count
        regular_count += 1
        if safe_start <= row_index <= safe_end:
            window_rows.append(dict(row))

    total_rows = regular_count + (1 if grand_total_row is not None else 0)
    if grand_total_row is not None:
        grand_total_index = regular_count
        if safe_start <= grand_total_index <= safe_end:
            window_rows.append(dict(grand_total_row))

    return {
        "rows": clone_hierarchy_rows(window_rows),
        "total_rows": total_rows,
        "grand_total_row": dict(grand_total_row) if isinstance(grand_total_row, dict) else grand_total_row,
        "grand_total_formula_source_rows": clone_hierarchy_rows(grand_total_formula_source_rows),
        "color_scale_stats": _hierarchy_color_stats_result(by_col, table_min, table_max),
        "full_rows": None,
    }
