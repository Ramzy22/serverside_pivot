"""
Formula and window function methods extracted from TanStackPivotAdapter.

Provides FormulaEngineMixin: formula column evaluation, formula sorting,
and pivot window functions (percent_of_grand_total, percent_of_row, etc.).
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from functools import cmp_to_key
import math
import re

if TYPE_CHECKING:
    from .tanstack_adapter import TanStackRequest

_FORMULA_IDENTIFIER_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
_COLUMN_FORMULA_REF_RE = re.compile(r"\[([^\]]+)\]")
MISSING_FORMULA_VALUE = object()


def _normalize_formula_reference_key(value: Any, fallback: Any = "formula") -> str:
    base = str(value or "").strip().lower()
    base = re.sub(r"\s+", "", base)
    base = re.sub(r"[^a-z0-9_]", "", base)
    fallback_base = re.sub(r"[^a-z0-9_]", "", str(fallback or "formula").strip().lower()) or "formula"
    normalized = base or fallback_base
    return normalized if re.match(r"^[a-z_]", normalized) else f"f_{normalized}"


def _is_missing_value(value: Any) -> bool:
    return value is None or value is MISSING_FORMULA_VALUE or (isinstance(value, float) and math.isnan(value))


def _formula_namespace_value(value: Optional[float]) -> Any:
    return value if value is not None else MISSING_FORMULA_VALUE


def _is_grand_total_row(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    return bool(
        row.get("_isTotal")
        or row.get("_id") == "Grand Total"
        or row.get("_path") == "__grand_total__"
    )


class FormulaEngineMixin:
    """Mixin providing formula and window-function logic for TanStackPivotAdapter."""

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

    @staticmethod
    def _formula_label(column: Dict[str, Any]) -> str:
        label = (
            column.get("formulaLabel")
            or column.get("header")
            or column.get("accessorKey")
            or column.get("id")
        )
        return str(label) if label is not None else ""

    @staticmethod
    def _formula_reference_key(column: Dict[str, Any]) -> str:
        ref = (
            column.get("formulaRef")
            or _normalize_formula_reference_key(
                column.get("formulaLabel") or column.get("header") or column.get("id"),
                column.get("id"),
            )
            or column.get("id")
        )
        return str(ref) if ref is not None else ""

    @staticmethod
    def _matches_formula_column_id(column_id: Any, formula_id: Any) -> bool:
        if not isinstance(column_id, str) or not isinstance(formula_id, str):
            return False
        return column_id == formula_id or column_id.endswith(f"_{formula_id}")

    @staticmethod
    def _formula_scope(column: Dict[str, Any]) -> str:
        scope = str(
            column.get("formulaScope")
            or column.get("formula_scope")
            or column.get("scope")
            or "measures"
        ).strip().lower()
        if scope in {"columns", "display", "displayed", "displayed_columns", "rendered", "rendered_columns"}:
            return "columns"
        return "measures"

    @staticmethod
    def _is_column_formula_column(column: Any) -> bool:
        return isinstance(column, dict) and column.get("isFormula") and FormulaEngineMixin._formula_scope(column) == "columns"

    @staticmethod
    def _is_measure_formula_column(column: Any) -> bool:
        return isinstance(column, dict) and column.get("isFormula") and FormulaEngineMixin._formula_scope(column) != "columns"

    @staticmethod
    def _extract_column_formula_references(expression: Any) -> List[str]:
        if not isinstance(expression, str):
            return []
        references = []
        seen = set()
        for match in _COLUMN_FORMULA_REF_RE.finditer(expression):
            reference = str(match.group(1) or "").strip()
            if reference and reference not in seen:
                references.append(reference)
                seen.add(reference)
        return references

    def _canonicalize_column_formula_expression(
        self,
        expression: Any,
        row: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        if not isinstance(expression, str):
            return "", {}

        namespace: Dict[str, Any] = {}
        reference_tokens: Dict[str, str] = {}

        def replace_reference(match: Any) -> str:
            reference = str(match.group(1) or "").strip()
            if not reference:
                return "missing_column"
            token = reference_tokens.get(reference)
            if token is None:
                token = f"col_{len(reference_tokens) + 1}"
                reference_tokens[reference] = token
                namespace[token] = _formula_namespace_value(self._numeric_or_none(row.get(reference)))
            return token

        normalized = _COLUMN_FORMULA_REF_RE.sub(replace_reference, expression)
        return normalized, namespace

    @staticmethod
    def _extract_formula_identifiers(expression: Any) -> List[str]:
        if not isinstance(expression, str):
            return []
        return list(dict.fromkeys(_FORMULA_IDENTIFIER_RE.findall(expression)))

    def _canonicalize_formula_expression(self, expression: Any, alias_map: Dict[str, str]) -> str:
        if not isinstance(expression, str) or not alias_map:
            return str(expression or "")
        normalized = str(expression)
        for alias, canonical in sorted(alias_map.items(), key=lambda item: len(item[0]), reverse=True):
            if not alias:
                continue
            normalized = re.sub(rf"\b{re.escape(alias)}\b", canonical, normalized, flags=re.IGNORECASE)
        return normalized

    def _build_formula_evaluation_plan(self, formula_cols: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], set[str]]:
        formula_by_id = {
            str(col.get("id")): col
            for col in formula_cols
            if isinstance(col, dict) and col.get("id")
        }
        if not formula_by_id:
            return [], set()

        formula_alias_to_id: Dict[str, str] = {}
        for formula_id, col in formula_by_id.items():
            formula_alias_to_id[formula_id.lower()] = formula_id
            formula_ref = self._formula_reference_key(col)
            if formula_ref:
                formula_alias_to_id[formula_ref.lower()] = formula_id

        dependencies: Dict[str, set[str]] = {}
        self_referencing: set[str] = set()
        ordered_formula_ids = [str(col.get("id")) for col in formula_cols if isinstance(col, dict) and col.get("id")]

        for formula_id, col in formula_by_id.items():
            identifiers = set(self._extract_formula_identifiers(col.get("formulaExpr", "")))
            canonical_dependencies = {
                formula_alias_to_id[identifier.lower()]
                for identifier in identifiers
                if identifier.lower() in formula_alias_to_id
            }
            if formula_id in canonical_dependencies:
                self_referencing.add(formula_id)
            dependencies[formula_id] = {identifier for identifier in canonical_dependencies if identifier != formula_id}

        resolved: set[str] = set()
        plan: List[Dict[str, Any]] = []

        while True:
            progressed = False
            for formula_id in ordered_formula_ids:
                if formula_id in resolved or formula_id in self_referencing:
                    continue
                if dependencies.get(formula_id, set()).issubset(resolved):
                    plan.append(formula_by_id[formula_id])
                    resolved.add(formula_id)
                    progressed = True
            if not progressed:
                break

        unresolved = (set(formula_by_id.keys()) - resolved) | self_referencing
        return plan, unresolved

    def _evaluate_formula_expression(self, parser: Any, expression: str, namespace: Dict[str, Any]) -> Optional[float]:
        try:
            result = parser.evaluate(expression, namespace)
        except Exception:
            return None
        numeric_result = self._numeric_or_none(result)
        if numeric_result is None or not math.isfinite(numeric_result):
            return None
        return numeric_result

    def _resolved_sort_field(self, sort_spec: Dict[str, Any], request: TanStackRequest) -> Optional[str]:
        if not isinstance(sort_spec, dict):
            return None
        sort_field = sort_spec.get("id")
        if sort_field == "hierarchy" and request.grouping:
            sort_field = request.grouping[0]
        return str(sort_field) if isinstance(sort_field, str) and sort_field else None

    @staticmethod
    def _formula_ids_from_request(request: TanStackRequest) -> set[str]:
        return {
            str(col.get("id"))
            for col in (request.columns or [])
            if isinstance(col, dict) and col.get("isFormula") and col.get("id")
        }

    @staticmethod
    def _measure_formula_ids_from_request(request: TanStackRequest) -> set[str]:
        return {
            str(col.get("id"))
            for col in (request.columns or [])
            if FormulaEngineMixin._is_measure_formula_column(col) and col.get("id")
        }

    def _build_formula_rollup_values(self, rows: List[Dict[str, Any]], formula_ids: set[str]) -> Dict[str, Optional[float]]:
        if not rows or not formula_ids:
            return {}

        regular_rows = [row for row in rows if isinstance(row, dict) and not _is_grand_total_row(row)]
        if not regular_rows:
            return {}

        total_source_rows = [
            row for row in regular_rows
            if row.get("depth") == 0
        ] or regular_rows

        materialized_formula_keys = set()
        for row in total_source_rows:
            for key in row.keys():
                if not isinstance(key, str):
                    continue
                if key in formula_ids or key.startswith("__RowTotal__"):
                    if key in formula_ids or any(
                        key == f"__RowTotal__{formula_id}" or self._matches_formula_column_id(key, formula_id)
                        for formula_id in formula_ids
                    ):
                        materialized_formula_keys.add(key)
                        continue
                if any(self._matches_formula_column_id(key, formula_id) for formula_id in formula_ids):
                    materialized_formula_keys.add(key)

        rollups: Dict[str, Optional[float]] = {}
        for key in materialized_formula_keys:
            values = [
                numeric_value
                for numeric_value in (
                    self._numeric_or_none(row.get(key))
                    for row in total_source_rows
                )
                if numeric_value is not None
            ]
            rollups[key] = sum(values) if values else None

        return rollups

    def _has_formula_sort(self, request: TanStackRequest) -> bool:
        formula_ids = self._formula_ids_from_request(request)
        if not formula_ids:
            return False
        for sort_spec in (request.sorting or []):
            sort_field = self._resolved_sort_field(sort_spec, request)
            if not sort_field:
                continue
            if any(self._matches_formula_column_id(sort_field, formula_id) for formula_id in formula_ids):
                return True
        return False

    def _compare_row_values(
        self,
        left_value: Any,
        right_value: Any,
        desc: bool = False,
        absolute_sort: bool = False,
    ) -> int:
        left_missing = _is_missing_value(left_value)
        right_missing = _is_missing_value(right_value)
        if left_missing or right_missing:
            if left_missing and right_missing:
                return 0
            return 1 if left_missing else -1

        left_numeric = self._numeric_or_none(left_value)
        right_numeric = self._numeric_or_none(right_value)
        if left_numeric is not None and right_numeric is not None:
            comparable_left = abs(left_numeric) if absolute_sort else left_numeric
            comparable_right = abs(right_numeric) if absolute_sort else right_numeric
            if comparable_left < comparable_right:
                result = -1
            elif comparable_left > comparable_right:
                result = 1
            elif absolute_sort and left_numeric < right_numeric:
                result = -1
            elif absolute_sort and left_numeric > right_numeric:
                result = 1
            else:
                result = 0
        else:
            left_text = str(left_value).casefold()
            right_text = str(right_value).casefold()
            if left_text < right_text:
                result = -1
            elif left_text > right_text:
                result = 1
            else:
                result = 0

        return -result if desc else result

    def _compare_rows_for_requested_sort(
        self,
        left_row: Dict[str, Any],
        right_row: Dict[str, Any],
        request: TanStackRequest,
        original_order: Dict[str, int],
        left_key: str,
        right_key: str,
    ) -> int:
        for sort_spec in (request.sorting or []):
            sort_field = self._resolved_sort_field(sort_spec, request)
            if not sort_field:
                continue
            comparison = self._compare_row_values(
                left_row.get(sort_field),
                right_row.get(sort_field),
                desc=bool(sort_spec.get("desc")),
                absolute_sort=(
                    bool(sort_spec.get("absoluteSort"))
                    or str(sort_spec.get("sortType") or "").strip().lower()
                    in {"absolute", "abs", "absolute_value", "absolute-value"}
                ),
            )
            if comparison:
                return comparison
        return (original_order.get(left_key, 0) > original_order.get(right_key, 0)) - (
            original_order.get(left_key, 0) < original_order.get(right_key, 0)
        )

    def _sort_rows_for_formula_sort(self, rows: List[Dict[str, Any]], request: TanStackRequest) -> List[Dict[str, Any]]:
        if not rows or not self._has_formula_sort(request):
            return rows

        grand_total_rows = [row for row in rows if _is_grand_total_row(row)]
        regular_rows = [row for row in rows if not _is_grand_total_row(row)]
        if not regular_rows:
            return rows

        can_preserve_tree = request.grouping and all(
            isinstance(row, dict) and isinstance(row.get("_path"), str) and row.get("_path")
            for row in regular_rows
        )

        if not can_preserve_tree:
            keyed_rows = [
                (f"__row_{index}", row)
                for index, row in enumerate(regular_rows)
                if isinstance(row, dict)
            ]
            original_order = {key: index for index, (key, _) in enumerate(keyed_rows)}
            sorted_rows = [
                row
                for _, row in sorted(
                    keyed_rows,
                    key=cmp_to_key(
                        lambda left, right: self._compare_rows_for_requested_sort(
                            left[1],
                            right[1],
                            request,
                            original_order,
                            left[0],
                            right[0],
                        )
                    ),
                )
            ]
            return sorted_rows + grand_total_rows

        path_to_row = {}
        original_order = {}
        children_by_parent: Dict[Optional[str], List[str]] = {}
        root_paths: List[str] = []

        for index, row in enumerate(regular_rows):
            path = row.get("_path")
            if not isinstance(path, str) or not path:
                continue
            path_to_row[path] = row
            original_order[path] = index

        for path in path_to_row:
            parent_path = path.rsplit("|||", 1)[0] if "|||" in path else None
            if parent_path and parent_path in path_to_row:
                children_by_parent.setdefault(parent_path, []).append(path)
            else:
                root_paths.append(path)

        def sort_paths(paths: List[str]) -> List[str]:
            return sorted(
                paths,
                key=cmp_to_key(
                    lambda left_path, right_path: self._compare_rows_for_requested_sort(
                        path_to_row[left_path],
                        path_to_row[right_path],
                        request,
                        original_order,
                        left_path,
                        right_path,
                    )
                ),
            )

        sorted_rows: List[Dict[str, Any]] = []

        def append_subtree(path: str) -> None:
            row = path_to_row.get(path)
            if row is None:
                return
            sorted_rows.append(row)
            for child_path in sort_paths(children_by_parent.get(path, [])):
                append_subtree(child_path)

        for root_path in sort_paths(root_paths):
            append_subtree(root_path)

        return sorted_rows + grand_total_rows

    def _apply_column_formula_columns(
        self,
        rows: List[Dict[str, Any]],
        formula_cols: List[Dict[str, Any]],
        parser: Any,
    ) -> None:
        if not rows or not formula_cols:
            return

        formula_plan, unresolved_formula_ids = self._build_formula_evaluation_plan(formula_cols)
        formula_alias_map: Dict[str, str] = {}
        for col in formula_cols:
            if not isinstance(col, dict) or not col.get("id"):
                continue
            formula_id = str(col.get("id"))
            formula_alias_map[formula_id.lower()] = formula_id
            formula_ref = self._formula_reference_key(col)
            if formula_ref:
                formula_alias_map[formula_ref.lower()] = formula_id

        for row in rows:
            if not isinstance(row, dict):
                continue
            formula_namespace: Dict[str, Any] = {}
            for fcol in formula_plan:
                formula_id = str(fcol.get("id") or "")
                formula_expr = self._canonicalize_formula_expression(
                    fcol.get("formulaExpr", ""),
                    formula_alias_map,
                )
                if not formula_id or not formula_expr:
                    continue
                prepared_expr, column_namespace = self._canonicalize_column_formula_expression(
                    formula_expr,
                    row,
                )
                namespace = {
                    **column_namespace,
                    **formula_namespace,
                }
                result = self._evaluate_formula_expression(parser, prepared_expr, namespace)
                row[formula_id] = result
                formula_namespace[formula_id] = _formula_namespace_value(result)
                formula_ref = self._formula_reference_key(fcol)
                if formula_ref:
                    formula_namespace[formula_ref] = _formula_namespace_value(result)

            for formula_id in unresolved_formula_ids:
                row[formula_id] = None
                formula_namespace[formula_id] = MISSING_FORMULA_VALUE
                unresolved_col = next((col for col in formula_cols if col.get("id") == formula_id), None)
                formula_ref = self._formula_reference_key(unresolved_col or {})
                if formula_ref:
                    formula_namespace[formula_ref] = MISSING_FORMULA_VALUE

    def _apply_formula_columns(self, rows: List[Dict[str, Any]], request: TanStackRequest) -> None:
        """
        Apply formula columns (post-aggregation calculated fields) to each row.

        Formula configs arrive in request.columns as entries with isFormula=True and a formulaExpr
        string like "revenue - cost".  References in the expression are field names (without agg
        suffix).  At runtime we look for keys of the form <dim_prefix>_<field>_<agg> in each row
        and evaluate the formula for every matching prefix, writing result back as
        <dim_prefix>_<formula_id> (or just <formula_id> in flat mode).
        """
        if not rows:
            return

        formula_cols = [
            col for col in (request.columns or [])
            if isinstance(col, dict) and col.get("isFormula")
        ]
        if not formula_cols:
            return
        measure_formula_cols = [
            col for col in formula_cols
            if self._is_measure_formula_column(col)
        ]
        column_formula_cols = [
            col for col in formula_cols
            if self._is_column_formula_column(col)
        ]
        formula_ids = {
            str(col.get("id"))
            for col in measure_formula_cols
            if isinstance(col, dict) and col.get("id")
        }
        formula_plan, unresolved_formula_ids = self._build_formula_evaluation_plan(measure_formula_cols)
        formula_alias_map = {}
        for col in measure_formula_cols:
            if not isinstance(col, dict) or not col.get("id"):
                continue
            formula_id = str(col.get("id"))
            formula_alias_map[formula_id.lower()] = formula_id
            formula_ref = self._formula_reference_key(col)
            if formula_ref:
                formula_alias_map[formula_ref.lower()] = formula_id

        from .planner.expression_parser import SafeExpressionParser

        parser = SafeExpressionParser()

        if not measure_formula_cols:
            self._apply_column_formula_columns(rows, column_formula_cols, parser)
            return

        # Gather all row keys once to detect pivot prefixes.
        all_keys: set = set()
        for row in rows:
            if isinstance(row, dict):
                all_keys.update(row.keys())

        # Build a map: measure_field -> list of agg suffixes present in data (e.g. "sum", "avg")
        # so we can resolve formula references like "revenue" -> row["revenue_sum"]
        agg_cols = [
            col for col in (request.columns or [])
            if isinstance(col, dict) and col.get("aggregationFn")
        ]
        # field -> agg suffix  (pick first match; formula references just the field name)
        field_agg_map: Dict[str, str] = {}
        field_measure_id_map: Dict[str, str] = {}
        for col in agg_cols:
            field = col.get("aggregationField") or col.get("id", "")
            measure_id = col.get("id") or ""
            agg = col.get("aggregationFn", "sum")
            if field and field not in field_agg_map:
                field_agg_map[field] = agg
            if field and field not in field_measure_id_map and measure_id:
                field_measure_id_map[field] = str(measure_id)

        grouping_ids = set(request.grouping or [])
        has_column_dimensions = any(
            isinstance(col, dict)
            and col.get("id") not in grouping_ids
            and not col.get("aggregationFn")
            and not col.get("isFormula")
            for col in (request.columns or [])
        )

        if has_column_dimensions:
            # Collect unique dim prefixes across all measure columns.
            # A pivot key looks like: <dim_prefix>_<field>_<agg>
            dim_prefixes: set = set()
            for field, agg in field_agg_map.items():
                suffix = f"_{field}_{agg}"
                for key in all_keys:
                    if isinstance(key, str) and key.endswith(suffix) and not key.startswith("__RowTotal__"):
                        prefix = key[: len(key) - len(suffix)]
                        dim_prefixes.add(prefix)
            row_total_measure_keys = {
                field: f"__RowTotal__{measure_id}"
                for field, measure_id in field_measure_id_map.items()
            }
            has_row_total_measure_values = bool(row_total_measure_keys) and any(
                isinstance(row, dict) and any(total_key in row for total_key in row_total_measure_keys.values())
                for row in rows
            )

            for row in rows:
                if not isinstance(row, dict):
                    continue
                for prefix in dim_prefixes:
                    namespace: Dict[str, Any] = {}
                    for field, agg in field_agg_map.items():
                        key = f"{prefix}_{field}_{agg}"
                        val = self._numeric_or_none(row.get(key))
                        namespace[field] = _formula_namespace_value(val)

                    for fcol in formula_plan:
                        formula_id = fcol.get("id", "")
                        formula_expr = self._canonicalize_formula_expression(fcol.get("formulaExpr", ""), formula_alias_map)
                        if not formula_id or not formula_expr:
                            continue
                        result_key = f"{prefix}_{formula_id}"
                        result = self._evaluate_formula_expression(parser, formula_expr, namespace)
                        row[result_key] = result
                        namespace[formula_id] = _formula_namespace_value(result)
                        formula_ref = self._formula_reference_key(fcol)
                        if formula_ref:
                            namespace[formula_ref] = _formula_namespace_value(result)

                    for formula_id in unresolved_formula_ids:
                        row[f"{prefix}_{formula_id}"] = None
                        namespace[formula_id] = MISSING_FORMULA_VALUE
                        unresolved_col = next((col for col in measure_formula_cols if col.get("id") == formula_id), None)
                        formula_ref = self._formula_reference_key(unresolved_col or {})
                        if formula_ref:
                            namespace[formula_ref] = MISSING_FORMULA_VALUE

                if has_row_total_measure_values:
                    materialized_formula_values: Dict[str, List[float]] = {
                        str(fcol.get("id")): []
                        for fcol in formula_plan
                        if isinstance(fcol, dict) and fcol.get("id")
                    }
                    for prefix in dim_prefixes:
                        for formula_id in materialized_formula_values:
                            result_value = self._numeric_or_none(row.get(f"{prefix}_{formula_id}"))
                            if result_value is not None:
                                materialized_formula_values[formula_id].append(result_value)

                    for fcol in formula_plan:
                        formula_id = fcol.get("id", "")
                        if not formula_id:
                            continue
                        values = materialized_formula_values.get(str(formula_id), [])
                        row[f"__RowTotal__{formula_id}"] = sum(values) if values else None

                    for formula_id in unresolved_formula_ids:
                        row[f"__RowTotal__{formula_id}"] = None
        else:
            # Flat mode: formula key is just the formula_id
            for row in rows:
                if not isinstance(row, dict):
                    continue
                namespace = {}
                for field, agg in field_agg_map.items():
                    key = f"{field}_{agg}"
                    val = self._numeric_or_none(row.get(key))
                    namespace[field] = _formula_namespace_value(val)

                for fcol in formula_plan:
                    formula_id = fcol.get("id", "")
                    formula_expr = self._canonicalize_formula_expression(fcol.get("formulaExpr", ""), formula_alias_map)
                    if not formula_id or not formula_expr:
                        continue
                    result = self._evaluate_formula_expression(parser, formula_expr, namespace)
                    row[formula_id] = result
                    namespace[formula_id] = _formula_namespace_value(result)
                    formula_ref = self._formula_reference_key(fcol)
                    if formula_ref:
                        namespace[formula_ref] = _formula_namespace_value(result)

                for formula_id in unresolved_formula_ids:
                    row[formula_id] = None
                    namespace[formula_id] = MISSING_FORMULA_VALUE
                    unresolved_col = next((col for col in measure_formula_cols if col.get("id") == formula_id), None)
                    formula_ref = self._formula_reference_key(unresolved_col or {})
                    if formula_ref:
                        namespace[formula_ref] = MISSING_FORMULA_VALUE

        self._apply_column_formula_columns(rows, column_formula_cols, parser)

        grand_total_rows = [row for row in rows if isinstance(row, dict) and _is_grand_total_row(row)]
        regular_rows = [row for row in rows if isinstance(row, dict) and not _is_grand_total_row(row)]
        if grand_total_rows and regular_rows and formula_ids:
            rollup_values = self._build_formula_rollup_values(regular_rows, formula_ids)
            for grand_total_row in grand_total_rows:
                grand_total_row.update(rollup_values)

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
            and not col.get("isFormula")
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
