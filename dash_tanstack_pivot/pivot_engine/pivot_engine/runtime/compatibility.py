"""Explicit forwarding shims retained for zero-break runtime compatibility."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from ..tanstack_adapter import TanStackRequest, TanStackResponse
from .drill_service import RuntimeDrillService
from .export_service import RuntimeExportService
from .lifecycle import RuntimeRequestLifecycle
from .models import PivotRequestContext, PivotViewState
from .report_service import RuntimeReportService
from .request_factory import RuntimeRequestFactory


class RuntimeCompatibilityMixin:
    """Forward historical helper methods to their capability owners.

    This class intentionally contains no mutable state and no algorithms. It
    can be removed in a future major version after downstream call sites stop
    relying on private helpers.
    """

    @staticmethod
    def _build_tanstack_sorting(
        state: PivotViewState,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        return RuntimeRequestFactory.build_sorting(state)

    @staticmethod
    def _backend_circuit_key(
        context: PivotRequestContext, trigger_kind: Optional[str]
    ) -> Tuple[str, str]:
        return RuntimeRequestLifecycle.circuit_key(context, trigger_kind)

    async def _run_backend_operation(
        self,
        context: PivotRequestContext,
        trigger_kind: Optional[str],
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        return await self._lifecycle.run_backend_operation(
            context,
            trigger_kind,
            operation,
        )

    async def _execute_tanstack_data_request(
        self,
        adapter: Any,
        request: TanStackRequest,
        state: PivotViewState,
        context: PivotRequestContext,
        expanded_paths: List[List[str]],
        *,
        trigger_kind: str,
        effective_needs_col_schema: bool,
        profiling_enabled: bool,
    ) -> TanStackResponse:
        return await self._router._execute_tanstack_data_request(
            adapter,
            request,
            state,
            context,
            expanded_paths,
            trigger_kind=trigger_kind,
            effective_needs_col_schema=effective_needs_col_schema,
            profiling_enabled=profiling_enabled,
        )

    @staticmethod
    def _build_export_pagination(
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return RuntimeRequestFactory.build_export_pagination(export_request)

    @staticmethod
    def _build_export_request_context(
        context: PivotRequestContext,
        export_request: Optional[Dict[str, Any]],
    ) -> PivotRequestContext:
        return RuntimeRequestFactory.build_export_context(context, export_request)

    @staticmethod
    def _build_export_payload(
        response: TanStackResponse,
        state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return RuntimeExportService._build_export_payload(
            response, state, export_request
        )

    @staticmethod
    def _write_xlsx_export(
        content_path: str,
        *,
        rows: List[Dict[str, Any]],
        field_order: List[str],
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
        include_headers: bool,
    ) -> None:
        return RuntimeExportService._write_xlsx_export(
            content_path,
            rows=rows,
            field_order=field_order,
            header_map=header_map,
            state=state,
            style_profile=style_profile,
            include_headers=include_headers,
        )

    @staticmethod
    def _export_key_aliases(key: str) -> List[str]:
        return RuntimeExportService._export_key_aliases(key)

    @staticmethod
    def _export_header_for_key(
        key: str,
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
    ) -> str:
        return RuntimeExportService._export_header_for_key(
            key, header_map, state, style_profile
        )

    @staticmethod
    def _export_column_width(key: str, style_profile: Dict[str, Any]) -> Optional[int]:
        return RuntimeExportService._export_column_width(key, style_profile)

    @staticmethod
    def _normalize_export_header_style(raw_style: Any) -> Dict[str, Any]:
        return RuntimeExportService._normalize_export_header_style(raw_style)

    @staticmethod
    def _export_header_formatting_style(
        key: str, header_label: str, style_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        return RuntimeExportService._export_header_formatting_style(
            key, header_label, style_profile
        )

    @staticmethod
    def _css_property_name(name: str) -> str:
        return RuntimeExportService._css_property_name(name)

    @staticmethod
    def _style_to_css(style: Dict[str, Any]) -> str:
        return RuntimeExportService._style_to_css(style)

    @staticmethod
    def _format_export_number(
        value: float, fmt: Any, decimal_places: Any, group_separator: Any
    ) -> str:
        return RuntimeExportService._format_export_number(
            value, fmt, decimal_places, group_separator
        )

    @staticmethod
    def _matching_value_config_for_export(
        key: str, state: PivotViewState, style_profile: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        return RuntimeExportService._matching_value_config_for_export(
            key, state, style_profile
        )

    @staticmethod
    def _formatted_export_value(
        key: str,
        value: Any,
        row: Dict[str, Any],
        state: PivotViewState,
        style_profile: Dict[str, Any],
    ) -> str:
        return RuntimeExportService._formatted_export_value(
            key, value, row, state, style_profile
        )

    @staticmethod
    def _export_condition_matches(
        condition: Any, left_value: float, right_value: Any
    ) -> bool:
        return RuntimeExportService._export_condition_matches(
            condition, left_value, right_value
        )

    @staticmethod
    def _conditional_export_style(
        key: str, value: Any, row: Dict[str, Any], style_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        return RuntimeExportService._conditional_export_style(
            key, value, row, style_profile
        )

    @staticmethod
    def _export_cell_style(
        key: str,
        value: Any,
        row: Dict[str, Any],
        header_label: str,
        style_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        return RuntimeExportService._export_cell_style(
            key, value, row, header_label, style_profile
        )

    @staticmethod
    def _export_header_style(
        key: str,
        header_label: str,
        style_profile: Dict[str, Any],
        state: Optional[PivotViewState] = None,
    ) -> Dict[str, Any]:
        return RuntimeExportService._export_header_style(
            key, header_label, style_profile, state
        )

    @staticmethod
    def _write_styled_html_export(
        buf: Any,
        *,
        rows: List[Dict[str, Any]],
        field_order: List[str],
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
        include_headers: bool,
    ) -> None:
        return RuntimeExportService._write_styled_html_export(
            buf,
            rows=rows,
            field_order=field_order,
            header_map=header_map,
            state=state,
            style_profile=style_profile,
            include_headers=include_headers,
        )

    @staticmethod
    def _resolve_export_field_order(
        rows: List[Dict[str, Any]],
        columns: List[Dict[str, Any]],
        col_ids: Any,
        state: Optional[PivotViewState] = None,
    ) -> List[str]:
        return RuntimeExportService._resolve_export_field_order(
            rows, columns, col_ids, state
        )

    @staticmethod
    def _build_export_header_map(
        columns: List[Dict[str, Any]],
        state: PivotViewState,
    ) -> Dict[str, str]:
        return RuntimeExportService._build_export_header_map(columns, state)

    @staticmethod
    def _display_export_header(key: str, state: PivotViewState) -> str:
        return RuntimeExportService._display_export_header(key, state)

    @staticmethod
    def _normalize_drill_request_payload(
        drill_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        return RuntimeDrillService.normalize_request(drill_payload)

    @staticmethod
    def _normalize_drill_response_payload(
        drill_result: Dict[str, Any],
        drill_payload: Dict[str, Any],
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return RuntimeDrillService.normalize_response(
            drill_result, drill_payload, records
        )

    @staticmethod
    def _normalize_formula_reference_key(value: Any, fallback: Any = "formula") -> str:
        return RuntimeRequestFactory.normalize_formula_reference_key(value, fallback)

    @staticmethod
    def _build_request_columns(
        row_fields: Optional[List[str]],
        col_fields: Optional[List[str]],
        val_configs: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        return RuntimeRequestFactory.build_columns(row_fields, col_fields, val_configs)

    @staticmethod
    def _build_pagination(context: PivotRequestContext) -> Dict[str, Any]:
        return RuntimeRequestFactory.build_pagination(context)

    @staticmethod
    def _has_branching_report_root(report_def: Optional[Dict[str, Any]]) -> bool:
        return RuntimeReportService._has_branching_report_root(report_def)

    @staticmethod
    def _normalize_report_condition(condition: Any) -> Dict[str, Any]:
        return RuntimeReportService._normalize_report_condition(condition)

    @staticmethod
    def _normalize_report_format(format_value: Any) -> Dict[str, Any]:
        return RuntimeReportService._normalize_report_format(format_value)

    @staticmethod
    def _report_condition_summary(condition: Optional[Dict[str, Any]]) -> str:
        return RuntimeReportService._report_condition_summary(condition)

    @staticmethod
    def _report_display_label(
        base_label: Any, report_format: Optional[Dict[str, Any]]
    ) -> str:
        return RuntimeReportService._report_display_label(base_label, report_format)

    @staticmethod
    def _decorate_report_row_metadata(
        row_out: Dict[str, Any],
        *,
        node: Dict[str, Any],
        depth: int,
        row_path: str,
        row_path_values: List[str],
        row_path_fields: List[str],
        base_label: Any,
        branch_match: Optional[Dict[str, Any]] = None,
        children_source: str = "Default children",
    ) -> Dict[str, Any]:
        return RuntimeReportService._decorate_report_row_metadata(
            row_out,
            node=node,
            depth=depth,
            row_path=row_path,
            row_path_values=row_path_values,
            row_path_fields=row_path_fields,
            base_label=base_label,
            branch_match=branch_match,
            children_source=children_source,
        )

    @staticmethod
    def _evaluate_report_clause(clause: Dict[str, Any], row: Dict[str, Any]) -> bool:
        return RuntimeReportService._evaluate_report_clause(clause, row)

    @staticmethod
    def _evaluate_report_condition(
        condition: Dict[str, Any], row: Dict[str, Any]
    ) -> bool:
        return RuntimeReportService._evaluate_report_condition(condition, row)

    @staticmethod
    def _normalize_branching_report_node(
        node: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        return RuntimeReportService._normalize_branching_report_node(node)

    @staticmethod
    def _get_branching_report_branch(
        node: Dict[str, Any], row: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        return RuntimeReportService._get_branching_report_branch(node, row)

    @staticmethod
    def _get_branching_report_child(
        node: Dict[str, Any], raw_value: Any, row: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        return RuntimeReportService._get_branching_report_child(node, raw_value, row)

    @staticmethod
    def _merge_exact_report_filter(
        filters: Dict[str, Any], field: str, value: Any
    ) -> Dict[str, Any]:
        return RuntimeReportService._merge_exact_report_filter(filters, field, value)

    @staticmethod
    def _merge_in_report_filter(
        filters: Dict[str, Any], field: str, values: List[Any]
    ) -> Dict[str, Any]:
        return RuntimeReportService._merge_in_report_filter(filters, field, values)

    @staticmethod
    def _is_report_numeric_value(value: Any) -> bool:
        return RuntimeReportService._is_report_numeric_value(value)

    @staticmethod
    def _build_report_other_row_from_rows(
        rows: List[Dict[str, Any]],
        node_field: str,
    ) -> Dict[str, Any]:
        return RuntimeReportService._build_report_other_row_from_rows(rows, node_field)

    @staticmethod
    def _decorate_report_other_row(
        base_row: Optional[Dict[str, Any]],
        *,
        node_field: str,
        path_values: List[str],
        path_fields: List[str],
        depth: int,
        level_label: str,
        source_count: int,
    ) -> Dict[str, Any]:
        return RuntimeReportService._decorate_report_other_row(
            base_row,
            node_field=node_field,
            path_values=path_values,
            path_fields=path_fields,
            depth=depth,
            level_label=level_label,
            source_count=source_count,
        )

    async def _handle_branching_report_request(
        self,
        adapter: Any,
        base_request: TanStackRequest,
        state: PivotViewState,
        context: PivotRequestContext,
        expanded_paths: List[List[str]],
    ) -> TanStackResponse:
        return await self._report_service._handle_branching_report_request(
            adapter, base_request, state, context, expanded_paths
        )

    @staticmethod
    def _resolve_report_level(
        report_def: Dict[str, Any],
        row_fields: List[str],
        depth: int,
        parent_values: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        return RuntimeReportService._resolve_report_level(
            report_def, row_fields, depth, parent_values
        )

    @staticmethod
    def _report_row_depth(row: Dict[str, Any]) -> int:
        return RuntimeReportService._report_row_depth(row)

    @staticmethod
    def _report_row_path_parts(row: Dict[str, Any]) -> List[str]:
        return RuntimeReportService._report_row_path_parts(row)

    @staticmethod
    def _is_report_summary_row(row: Dict[str, Any]) -> bool:
        return RuntimeReportService._is_report_summary_row(row)

    @staticmethod
    def _count_report_tree_rows(node: Dict[str, Any]) -> int:
        return RuntimeReportService._count_report_tree_rows(node)

    @staticmethod
    def _report_sort_key(value: Any, abs_sort: bool = False) -> Any:
        return RuntimeReportService._report_sort_key(value, abs_sort)

    @staticmethod
    def _apply_report_annotations(
        data: Optional[List[Dict[str, Any]]],
        total_rows: Optional[int],
        report_def: Dict[str, Any],
        row_fields: List[str],
    ) -> tuple:
        return RuntimeReportService._apply_report_annotations(
            data, total_rows, report_def, row_fields
        )

    @staticmethod
    def _parse_expanded_paths(expanded: Any) -> List[List[str]]:
        return RuntimeRequestFactory.parse_expanded_paths(expanded)
