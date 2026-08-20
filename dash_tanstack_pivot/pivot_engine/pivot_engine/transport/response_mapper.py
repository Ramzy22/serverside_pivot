"""Mapping from pivot-engine results to TanStack transport responses."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from ..formula_mixin import FormulaEngineMixin
from ..types.pivot_spec import MeasureAxisConfig
from .contracts import TanStackRequest, TanStackResponse


def _is_missing_value(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


class TanStackResponseMapper(FormulaEngineMixin):
    """Own domain-result conversion and formula post-processing."""

    @staticmethod
    def _implicit_formula_ref_request_ids(request: "TanStackRequest") -> set[str]:
        """Return aggregate request ids used only as hidden formula inputs."""
        return {
            str(col.get("id"))
            for col in (request.columns or [])
            if isinstance(col, dict)
            and col.get("_isImplicitFormulaRef")
            and col.get("id")
        }

    @staticmethod
    def _matches_materialized_request_id(column_id: Any, request_id: Any) -> bool:
        if (
            not isinstance(column_id, str)
            or not isinstance(request_id, str)
            or not request_id
        ):
            return False
        return (
            column_id == request_id
            or column_id.endswith(f"_{request_id}")
            or column_id == f"__RowTotal__{request_id}"
        )

    def _is_implicit_formula_ref_center_id(
        self, column_id: Any, request: "TanStackRequest"
    ) -> bool:
        return any(
            self._matches_materialized_request_id(column_id, request_id)
            for request_id in self._implicit_formula_ref_request_ids(request)
        )

    def _filter_implicit_formula_ref_columns(
        self,
        columns: Optional[List[Dict[str, Any]]],
        request: "TanStackRequest",
    ) -> List[Dict[str, Any]]:
        if not columns:
            return []
        return [
            col
            for col in columns
            if not (
                isinstance(col, dict)
                and (
                    col.get("_isImplicitFormulaRef")
                    or self._is_implicit_formula_ref_center_id(col.get("id"), request)
                )
            )
        ]

    def _reorder_materialized_dynamic_ids(
        self,
        observed_ids: list,
        request: "TanStackRequest",
    ) -> list[str]:
        """Normalize materialized value/formula column order for flat and pivot payloads.

        Planner output already determines the pivot-prefix sequence for regular
        measures, but post-aggregation formulas are materialized later from row
        dicts. Without a canonical reorder pass, those late formula keys drift to
        the end of the schema in arbitrary set/dict insertion order, which breaks
        the horizontal window contract expected by the client.
        """
        requested_dynamic_ids = self._dynamic_request_ids(request)
        column_formula_ids = [
            str(col.get("id"))
            for col in (request.columns or [])
            if self._is_column_formula_column(col) and col.get("id")
        ]
        if not requested_dynamic_ids:
            observed_list = [
                str(col_id)
                for col_id in (observed_ids or [])
                if isinstance(col_id, str)
            ]
            if not column_formula_ids:
                return observed_list
            observed_set = set(observed_list)
            ordered: list[str] = []
            seen: set[str] = set()
            for formula_id in column_formula_ids:
                if formula_id in observed_set and formula_id not in seen:
                    ordered.append(formula_id)
                    seen.add(formula_id)
            for col_id in observed_list:
                if col_id not in seen:
                    ordered.append(col_id)
                    seen.add(col_id)
            return ordered

        observed_list = [
            str(col_id) for col_id in (observed_ids or []) if isinstance(col_id, str)
        ]
        observed_set = set(observed_list)
        ordered: list[str] = []
        seen: set[str] = set()

        pivot_prefixes: list[str] = []
        seen_prefixes: set[str] = set()
        has_pivoted_dynamic = False

        for col_id in observed_list:
            for dynamic_id in requested_dynamic_ids:
                suffix = f"_{dynamic_id}"
                if not col_id.endswith(suffix):
                    continue
                has_pivoted_dynamic = True
                prefix = col_id[: -len(suffix)]
                if prefix and prefix not in seen_prefixes:
                    pivot_prefixes.append(prefix)
                    seen_prefixes.add(prefix)
                break

        if has_pivoted_dynamic:
            for prefix in pivot_prefixes:
                for dynamic_id in requested_dynamic_ids:
                    candidate = f"{prefix}_{dynamic_id}"
                    if candidate in observed_set and candidate not in seen:
                        ordered.append(candidate)
                        seen.add(candidate)
        else:
            for dynamic_id in requested_dynamic_ids:
                if dynamic_id in observed_set and dynamic_id not in seen:
                    ordered.append(dynamic_id)
                    seen.add(dynamic_id)

        for formula_id in column_formula_ids:
            if formula_id in observed_set and formula_id not in seen:
                ordered.append(formula_id)
                seen.add(formula_id)

        for col_id in observed_list:
            if col_id not in seen:
                ordered.append(col_id)
                seen.add(col_id)

        return ordered

    @staticmethod
    def _dynamic_request_ids(request: "TanStackRequest") -> list[str]:
        measure_axis = MeasureAxisConfig.from_dict(
            getattr(request, "measure_axis", None)
        )
        if measure_axis and measure_axis.placement in {"rows", "columns"}:
            return [measure_axis.value_field or "__measure_value__"]
        return [
            str(col.get("id"))
            for col in (request.columns or [])
            if isinstance(col, dict)
            and col.get("id")
            and not col.get("_isImplicitFormulaRef")
            and (
                col.get("aggregationFn")
                or FormulaEngineMixin._is_measure_formula_column(col)
            )
        ]

    def convert_pivot_result_to_tanstack_format(
        self,
        pivot_result: Any,
        tanstack_request: TanStackRequest,
        version: Optional[int] = None,
    ) -> TanStackResponse:
        """Convert pivot engine result to TanStack format"""
        import pyarrow as pa

        hidden_sort_prefix = "__sortkey__"

        # Convert from pivot format to TanStack row format
        rows = []

        if isinstance(pivot_result, pa.Table):
            # Optimized vectorised conversion using PyArrow
            rows = pivot_result.to_pylist()
        elif (
            isinstance(pivot_result, dict)
            and "rows" in pivot_result
            and "columns" in pivot_result
        ):
            # Dict format with columns and rows
            pivot_columns = pivot_result["columns"]
            pivot_rows = pivot_result["rows"]

            for pivot_row in pivot_rows:
                tanstack_row = {}
                # Check if pivot_row is a list/tuple or a dict
                if isinstance(pivot_row, (list, tuple)):
                    for i, col_name in enumerate(pivot_columns):
                        tanstack_row[col_name] = (
                            pivot_row[i] if i < len(pivot_row) else None
                        )
                elif isinstance(pivot_row, dict):
                    # Already a dict, just use it
                    tanstack_row = pivot_row
                rows.append(tanstack_row)
        elif isinstance(pivot_result, list):
            # Already in row format
            rows = pivot_result

        # Hidden sort-key fields are transport-only helpers.
        # Keep them available for backend ORDER BY, but never expose them to the frontend.
        # Also sanitize NaN floats ??? None so JSON serialization produces valid null.
        import math as _math

        if rows:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                hidden_keys = [
                    key
                    for key in row.keys()
                    if isinstance(key, str) and key.startswith(hidden_sort_prefix)
                ]
                for key in hidden_keys:
                    row.pop(key, None)
                for key, val in row.items():
                    if isinstance(val, float) and _math.isnan(val):
                        row[key] = None

        # Enrich rows for TanStack Hierarchy display
        hierarchy_cols = tanstack_request.grouping or []
        if hierarchy_cols:
            max_depth = max(len(hierarchy_cols) - 1, 0)
            for row in rows:
                # Normalize depth (support both depth and _depth).
                # When depth is absent, infer it from how many consecutive
                # grouping fields are non-null ??? this is the only reliable
                # source when controllers return flat rows without depth metadata.
                current_depth = row.get("depth")
                if current_depth is None:
                    current_depth = row.get("_depth")
                if current_depth is None:
                    inferred_parts = []
                    for col in hierarchy_cols:
                        if _is_missing_value(row.get(col)):
                            break
                        inferred_parts.append(str(row[col]))
                    current_depth = min(max(len(inferred_parts) - 1, 0), max_depth)
                row["depth"] = current_depth

                # Populate _id if missing
                if "_id" not in row:
                    is_grand_total = False
                    # Check for Grand Total (first grouping column missing/NaN)
                    first_col = hierarchy_cols[0]
                    if first_col in row and _is_missing_value(row[first_col]):
                        row["_id"] = "Grand Total"
                        row["_isTotal"] = True
                        is_grand_total = True

                    if not is_grand_total:
                        # Find the correct dimension for this depth
                        if current_depth < len(hierarchy_cols):
                            target_col = hierarchy_cols[current_depth]
                            target_val = row.get(target_col)
                            row["_id"] = (
                                "" if _is_missing_value(target_val) else target_val
                            )
                        else:
                            # Fallback: deepest non-None
                            for col in reversed(hierarchy_cols):
                                if col in row and not _is_missing_value(row[col]):
                                    row["_id"] = row[col]
                                    break

                        if "_id" not in row:
                            row["_id"] = ""

                # Populate _path for row identification (critical for expansion).
                # Build the full path from ALL non-null grouping fields up to the
                # inferred depth so tabular layout mode can retrieve parent labels
                # (e.g. "USA") for child rows (e.g. depth-1 city rows) even when
                # the parent field value is absent from the row dict.
                if "_path" not in row:
                    if row.get("_isTotal"):
                        row["_path"] = "__grand_total__"
                    elif hierarchy_cols:
                        path_parts = []
                        for col in hierarchy_cols[: current_depth + 1]:
                            val = row.get(col)
                            if _is_missing_value(val):
                                break
                            path_parts.append(str(val))
                        row["_path"] = (
                            "|||".join(path_parts)
                            if path_parts
                            else str(row.get("_id", ""))
                        )
                    else:
                        row["_path"] = str(id(row))

        # Apply pivot window functions (% row/% col/% grand total) after hierarchy
        # metadata is normalized, so grand-total detection is stable.
        self._apply_pivot_window_functions(rows, tanstack_request)

        # Apply formula columns (post-aggregation calculated fields) after window functions.
        measure_axis_for_response = MeasureAxisConfig.from_dict(
            getattr(tanstack_request, "measure_axis", None)
        )
        formulas_materialized_by_measure_axis = bool(
            measure_axis_for_response
            and measure_axis_for_response.placement in {"rows", "columns"}
            and any(
                isinstance(column, dict) and column.get("isFormula")
                for column in (tanstack_request.columns or [])
            )
        )
        _formula_errors_main = {}
        if not formulas_materialized_by_measure_axis:
            _formula_errors_main = self._apply_formula_columns(rows, tanstack_request)
            rows = self._sort_rows_for_formula_sort(rows, tanstack_request)

        # Calculate pagination info if needed
        pagination = None
        if tanstack_request.pagination:
            total_rows = len(rows)  # In real implementation, get actual total
            pagination = {
                "totalRows": total_rows,
                "pageSize": tanstack_request.pagination.get("pageSize", 100),
                "pageIndex": tanstack_request.pagination.get("pageIndex", 0),
                "pageCount": (
                    (total_rows + tanstack_request.pagination.get("pageSize", 100) - 1)
                    // tanstack_request.pagination.get("pageSize", 100)
                    if total_rows
                    else 0
                ),
            }

        # Dynamic Column Generation for Pivot
        # If we have pivot columns, we need to update the response columns
        response_columns = list(tanstack_request.columns or [])
        measure_ids = {
            c["id"] for c in tanstack_request.columns if c.get("aggregationFn")
        }
        formula_ids = {c["id"] for c in tanstack_request.columns if c.get("isFormula")}
        formula_labels = {
            c["id"]: self._formula_label(c)
            for c in tanstack_request.columns
            if c.get("isFormula") and c.get("id")
        }
        grouping_ids = set(tanstack_request.grouping or [])
        has_column_dimensions = any(
            c.get("id") not in grouping_ids
            and not c.get("aggregationFn")
            and not c.get("isFormula")
            for c in tanstack_request.columns
        )

        # In pivot mode (column dimensions present), base measure columns are placeholders.
        # Keep only expanded dynamic pivot columns to avoid showing extra plain measures.
        if has_column_dimensions and (measure_ids or formula_ids):
            placeholder_ids = measure_ids | formula_ids
            response_columns = [
                c for c in response_columns if c.get("id") not in placeholder_ids
            ]

        # Detect dynamic columns using backend schema order first so pivot column
        # sorting survives transport and horizontal windowing.
        if rows:
            ordered_result_keys = []
            seen_result_keys = set()
            if isinstance(pivot_result, pa.Table):
                for key in pivot_result.column_names or []:
                    if key not in seen_result_keys:
                        ordered_result_keys.append(key)
                        seen_result_keys.add(key)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                for key in row.keys():
                    if key not in seen_result_keys:
                        ordered_result_keys.append(key)
                        seen_result_keys.add(key)

            known_ids = {
                c["id"] for c in response_columns if isinstance(c, dict) and c.get("id")
            }
            meta_keys = {
                "_id",
                "_path",
                "_isTotal",
                "depth",
                "_depth",
                "hierarchy",
                "_level",
                "_expanded",
                "_parentPath",
                "_has_children",
                "_is_expanded",
                "subRows",
                "uuid",
                "__virtualIndex",
            }

            materialized_ids = []
            for key in ordered_result_keys:
                if key in known_ids or key in meta_keys:
                    continue
                if isinstance(key, str) and key.startswith(hidden_sort_prefix):
                    continue
                if self._is_implicit_formula_ref_center_id(key, tanstack_request):
                    continue
                materialized_ids.append(key)

            materialized_ids = self._reorder_materialized_dynamic_ids(
                materialized_ids,
                tanstack_request,
            )

            new_columns = []
            for key in materialized_ids:
                if key.startswith("__RowTotal__"):
                    measure_key = key.replace("__RowTotal__", "")
                    header = f"Total {measure_key.replace('_', ' ').title()}"
                    new_columns.append(
                        {
                            "id": key,
                            "header": header,
                            "accessorKey": key,
                            "isRowTotal": True,
                        }
                    )
                else:
                    formula_label = None
                    for formula_id, label in formula_labels.items():
                        if self._matches_formula_column_id(key, formula_id):
                            formula_label = label
                            break

                    new_column = {
                        "id": key,
                        "header": formula_label or key.replace("_", " ").title(),
                        "accessorKey": key,
                    }
                    if formula_label:
                        new_column["formulaLabel"] = formula_label
                    new_columns.append(new_column)

            if new_columns:
                existing_ids = {
                    c.get("id") for c in response_columns if isinstance(c, dict)
                }
                for col in new_columns:
                    if col["id"] not in existing_ids:
                        response_columns.append(col)

        response_columns = [
            col
            for col in response_columns
            if not (
                isinstance(col, dict)
                and isinstance(col.get("id"), str)
                and col.get("id").startswith(hidden_sort_prefix)
            )
        ]
        response_columns = self._filter_implicit_formula_ref_columns(
            response_columns, tanstack_request
        )

        # Apply formula columns here so that all call paths (handle_request,
        # handle_hierarchical_request, handle_virtual_scroll_request) evaluate
        # formula columns. handle_request also calls _apply_formula_columns after
        # window functions, but that second pass is idempotent so it is safe.
        _formula_errors = {}
        if not formulas_materialized_by_measure_axis:
            _formula_errors = self._apply_formula_columns(rows, tanstack_request)
            rows = self._sort_rows_for_formula_sort(rows, tanstack_request)

        return TanStackResponse(
            data=rows,
            columns=response_columns,
            pagination=pagination,
            total_rows=len(rows) if rows else 0,
            version=version,
            formula_errors=_formula_errors or None,
        )
