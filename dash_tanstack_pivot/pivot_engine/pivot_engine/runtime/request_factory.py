"""Pure construction of transport requests from runtime view state."""

from __future__ import annotations

from dataclasses import replace
import re
from typing import Any, Dict, List, Optional, Tuple

from ..export_limits import EXPORT_ROW_LIMIT_CODE, ExportLimitError
from ..tanstack_adapter import TanStackOperation, TanStackRequest
from .models import PivotRequestContext, PivotViewState, first_present, safe_int


class RuntimeRequestFactory:
    """Build TanStack requests without owning execution or lifecycle state."""

    @staticmethod
    def build_sorting(
        state: PivotViewState,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        tanstack_sorting: List[Dict[str, Any]] = []
        sort_options = (
            state.sort_options if isinstance(state.sort_options, dict) else {}
        )
        column_sort_options = (
            sort_options.get("columnOptions")
            if isinstance(sort_options.get("columnOptions"), dict)
            else {}
        )
        effective_sorting = list(state.sorting or [])
        if not effective_sorting and state.row_fields and column_sort_options:
            for row_field in state.row_fields:
                options = column_sort_options.get(row_field)
                if isinstance(options, dict) and options.get("sortKeyField"):
                    effective_sorting.append({"id": row_field, "desc": False})
                    break

        metadata_keys = (
            "semanticType",
            "sortSemantic",
            "nulls",
            "sortType",
            "sortKeyField",
            "absoluteSort",
        )
        for sort_item in effective_sorting:
            if not isinstance(sort_item, dict) or sort_item.get("id") is None:
                continue
            sort_id = sort_item.get("id")
            normalized: Dict[str, Any] = {
                "id": sort_id,
                "desc": bool(sort_item.get("desc", False)),
            }
            lookup_id = sort_id
            if lookup_id == "hierarchy" and state.row_fields and column_sort_options:
                for row_field in state.row_fields:
                    if row_field in column_sort_options:
                        lookup_id = row_field
                        break
            static_options = column_sort_options.get(lookup_id)
            if isinstance(static_options, dict):
                for key in metadata_keys:
                    if key in static_options and static_options.get(key) is not None:
                        normalized[key] = static_options.get(key)
            for key in metadata_keys:
                if key in sort_item:
                    normalized[key] = sort_item.get(key)
            tanstack_sorting.append(normalized)
        return tanstack_sorting, column_sort_options

    @staticmethod
    def normalize_formula_reference_key(value: Any, fallback: Any = "formula") -> str:
        base = re.sub(r"\s+", "", str(value or "").strip().lower())
        base = re.sub(r"[^a-z0-9_]", "", base)
        fallback_base = (
            re.sub(r"[^a-z0-9_]", "", str(fallback or "formula").strip().lower())
            or "formula"
        )
        normalized = base or fallback_base
        return normalized if re.match(r"^[a-z_]", normalized) else f"f_{normalized}"

    @classmethod
    def build_columns(
        cls,
        row_fields: Optional[List[str]],
        col_fields: Optional[List[str]],
        val_configs: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        columns: List[Dict[str, Any]] = []
        columns.extend({"id": field} for field in (row_fields or []))
        columns.extend({"id": field} for field in (col_fields or []))
        for measure in val_configs or []:
            if not isinstance(measure, dict):
                continue
            field = measure.get("field")
            agg = measure.get("agg")
            if not field or not agg:
                continue
            if agg == "formula":
                label = measure.get("label") or field
                formula_ref = (
                    measure.get("formulaRef")
                    or measure.get("referenceKey")
                    or cls.normalize_formula_reference_key(label, field)
                )
                column: Dict[str, Any] = {
                    "id": field,
                    "header": label,
                    "accessorKey": field,
                    "formulaExpr": measure.get("formula", ""),
                    "formulaRef": formula_ref,
                    "formulaLabel": label,
                    "isFormula": True,
                }
                raw_scope = (
                    measure.get("formulaScope")
                    or measure.get("formula_scope")
                    or measure.get("scope")
                )
                if raw_scope is not None:
                    normalized_scope = str(raw_scope).strip().lower()
                    column["formulaScope"] = (
                        "columns"
                        if normalized_scope
                        in {
                            "columns",
                            "display",
                            "displayed",
                            "displayed_columns",
                            "rendered",
                            "rendered_columns",
                        }
                        else "measures"
                    )
                columns.append(column)
                continue
            columns.append(
                {
                    "id": measure.get("alias") or f"{field}_{agg}",
                    "header": measure.get("label") or field,
                    "aggregationField": field,
                    "aggregationFn": agg,
                    "format": measure.get("format"),
                    "windowFn": measure.get("windowFn"),
                    "weightField": measure.get("weightField"),
                }
            )

        existing_agg_fields = {
            measure.get("field")
            for measure in (val_configs or [])
            if isinstance(measure, dict)
            and measure.get("field")
            and measure.get("agg") != "formula"
        }
        formula_output_fields = {
            measure.get("field")
            for measure in (val_configs or [])
            if isinstance(measure, dict) and measure.get("agg") == "formula"
        }
        formula_ref_fields = {
            measure.get("formulaRef") or ""
            for measure in (val_configs or [])
            if isinstance(measure, dict) and measure.get("agg") == "formula"
        }
        identifier_re = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
        bracket_ref_re = re.compile(r"\[([^\]]+)\]")
        reserved = {
            "True",
            "False",
            "None",
            "and",
            "or",
            "not",
            "if",
            "else",
            "in",
            "is",
        }

        def formula_references(expression: Any) -> List[str]:
            references: List[str] = []
            seen = set()

            def add(value: Any) -> None:
                reference = str(value or "").strip()
                if reference and reference not in seen:
                    seen.add(reference)
                    references.append(reference)

            text = str(expression or "")
            for match in bracket_ref_re.finditer(text):
                add(match.group(1))
            for identifier in identifier_re.findall(bracket_ref_re.sub(" ", text)):
                add(identifier)
            return references

        implicit_fields = set()
        column_scope_aliases = {
            "columns",
            "display",
            "displayed",
            "displayed_columns",
            "rendered",
            "rendered_columns",
        }
        for measure in val_configs or []:
            if not isinstance(measure, dict) or measure.get("agg") != "formula":
                continue
            raw_scope = (
                measure.get("formulaScope")
                or measure.get("formula_scope")
                or measure.get("scope")
            )
            if str(raw_scope or "").strip().lower() in column_scope_aliases:
                continue
            for identifier in formula_references(measure.get("formula") or ""):
                if (
                    identifier in reserved
                    or identifier in existing_agg_fields
                    or identifier in formula_output_fields
                    or identifier in formula_ref_fields
                    or identifier in implicit_fields
                ):
                    continue
                columns.append(
                    {
                        "id": f"{identifier}_sum",
                        "aggregationField": identifier,
                        "aggregationFn": "sum",
                        "windowFn": None,
                        "weightField": None,
                        "_isImplicitFormulaRef": True,
                    }
                )
                implicit_fields.add(identifier)
        return columns

    @staticmethod
    def build_pagination(context: PivotRequestContext) -> Dict[str, Any]:
        if context.viewport_active and context.end_row is not None:
            count = max(context.end_row - context.start_row + 1, 1)
            return {
                "pageIndex": context.start_row // 1000,
                "pageSize": min(count, 1000),
                "startRow": context.start_row,
                "endRow": context.end_row,
            }
        return {"pageIndex": 0, "pageSize": 1000}

    @staticmethod
    def build_export_pagination(
        export_request: Optional[Dict[str, Any]],
        *,
        max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload = export_request if isinstance(export_request, dict) else {}
        row_start = safe_int(first_present(payload, "rowStart", "row_start"), 0)
        row_end_raw = first_present(payload, "rowEnd", "row_end")
        if row_end_raw is not None:
            row_end = max(row_start + 1, safe_int(row_end_raw, row_start + 1))
            page_size = max(1, row_end - row_start)
        else:
            requested_page_size = first_present(payload, "pageSize", "page_size")
            page_size = max(
                1,
                safe_int(requested_page_size, max_rows or 10_000_000),
            )
            row_end = row_start + page_size
        if max_rows is not None:
            safe_max_rows = max(1, int(max_rows))
            if page_size > safe_max_rows:
                raise ExportLimitError(
                    f"Export requests are limited to {safe_max_rows:,} rows. "
                    "Narrow the selected range or raise PIVOT_EXPORT_MAX_ROWS.",
                    limit_code=EXPORT_ROW_LIMIT_CODE,
                )
        return {
            "pageIndex": row_start // page_size if page_size else 0,
            "pageSize": page_size,
            "startRow": row_start,
            "endRow": row_end - 1,
        }

    @classmethod
    def build_export_context(
        cls,
        context: PivotRequestContext,
        export_request: Optional[Dict[str, Any]],
    ) -> PivotRequestContext:
        pagination = cls.build_export_pagination(export_request)
        payload = export_request if isinstance(export_request, dict) else {}
        return replace(
            context,
            original_intent="export",
            intent="export",
            viewport_active=True,
            start_row=safe_int(pagination.get("startRow"), 0),
            end_row=safe_int(pagination.get("endRow"), 0),
            needs_col_schema=True,
            include_grand_total=bool(
                first_present(
                    payload,
                    "include_grand_total",
                    "includeGrandTotal",
                    default=context.include_grand_total,
                )
            ),
        )

    @classmethod
    def build_request(
        cls,
        state: PivotViewState,
        context: PivotRequestContext,
        *,
        export: bool = False,
    ) -> TanStackRequest:
        sorting, column_sort_options = cls.build_sorting(state)
        pagination = (
            cls.build_export_pagination(state.export_request)
            if export
            else cls.build_pagination(context)
        )
        return TanStackRequest(
            operation=TanStackOperation.GET_DATA,
            table=context.table,
            columns=cls.build_columns(
                state.row_fields, state.col_fields, state.val_configs
            ),
            filters=state.filters or {},
            custom_dimensions=state.custom_dimensions or [],
            measure_axis=state.measure_axis or None,
            sorting=sorting,
            grouping=state.row_fields or [],
            aggregations=[],
            pagination=pagination,
            global_filter=(
                (state.filters or {}).get("global")
                if isinstance(state.filters, dict)
                else None
            ),
            totals=state.show_col_totals,
            row_totals=state.show_row_totals,
            include_subtotals=state.show_subtotals,
            layout_mode=state.layout_mode,
            show_subtotal_footers=state.show_subtotals if state.layout_mode else False,
            version=context.window_seq,
            column_sort_options=column_sort_options or None,
            layout_revision=context.layout_revision,
            layout_manifest=context.layout_manifest,
            pinned_column_ids=context.pinned_column_ids,
        )

    @staticmethod
    def parse_expanded_paths(expanded: Any) -> List[List[str]]:
        if expanded is None or (isinstance(expanded, dict) and not expanded):
            return []
        if isinstance(expanded, dict):
            if expanded.get("__ALL__") is True:
                return [
                    ["__ALL__"],
                    *[
                        ["__COLLAPSED__", *key.split("|||")]
                        for key, value in expanded.items()
                        if key != "__ALL__" and value is False
                    ],
                ]
            return [
                key.split("|||") for key, value in expanded.items() if value is True
            ]
        if expanded is True:
            return [["__ALL__"]]
        return []
