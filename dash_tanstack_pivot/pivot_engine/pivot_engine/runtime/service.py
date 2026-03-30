"""Transport-agnostic runtime service for pivot requests."""

from __future__ import annotations

import asyncio
import re
from typing import Any, Callable, Dict, List, Optional

from ..tanstack_adapter import TanStackOperation, TanStackRequest

from .models import PivotRequestContext, PivotServiceResponse, PivotViewState
from .session_gate import SessionRequestGate


class PivotRuntimeService:
    """Executes pivot operations using generic state/context contracts."""

    def __init__(
        self,
        adapter_getter: Callable[[], Any],
        session_gate: Optional[SessionRequestGate] = None,
        debug: bool = False,
    ):
        self._adapter_getter = adapter_getter
        self._session_gate = session_gate or SessionRequestGate()
        self._debug = debug

    async def process_async(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
        current_filter_options: Optional[Dict[str, Any]] = None,
    ) -> PivotServiceResponse:
        """Process one pivot request and return a transport-neutral response."""
        adapter = self._adapter_getter()
        current_filter_options = current_filter_options or {}

        if not self._session_gate.register_request(
            session_id=context.session_id,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            abort_generation=context.abort_generation,
            intent=context.intent,
            client_instance=context.client_instance,
        ):
            return PivotServiceResponse(status="stale")

        target_unique_col = None
        if state.filters and "__request_unique__" in state.filters:
            target_unique_col = state.filters["__request_unique__"]

        tanstack_sorting = []
        sort_options = state.sort_options if isinstance(state.sort_options, dict) else {}
        column_sort_options = (
            sort_options.get("columnOptions")
            if isinstance(sort_options.get("columnOptions"), dict)
            else {}
        )
        # Auto-inject a default ascending sort for the first row field that has
        # a sortKeyField in columnOptions when the frontend sends no explicit sort.
        effective_sorting = list(state.sorting or [])
        if not effective_sorting and state.row_fields and column_sort_options:
            for rf in (state.row_fields or []):
                col_opts = column_sort_options.get(rf)
                if isinstance(col_opts, dict) and col_opts.get("sortKeyField"):
                    effective_sorting.append({"id": rf, "desc": False})
                    break

        for s in effective_sorting:
            if not isinstance(s, dict) or s.get("id") is None:
                continue

            sort_id = s.get("id")
            sort_item = {"id": sort_id, "desc": bool(s.get("desc", False))}

            # Static per-column sort metadata (from sortOptions) is merged first.
            # Dynamic sorting payload keys override these defaults.
            # The frontend sends id="hierarchy" for the row-group column;
            # resolve to the actual row field for sortOptions lookup by
            # checking all row fields for a matching sortOptions entry.
            lookup_id = sort_id
            if lookup_id == "hierarchy" and state.row_fields and column_sort_options:
                for rf in state.row_fields:
                    if rf in column_sort_options:
                        lookup_id = rf
                        break
            static_column_sort = column_sort_options.get(lookup_id)
            if isinstance(static_column_sort, dict):
                for key in ("semanticType", "sortSemantic", "nulls", "sortType", "sortKeyField"):
                    if key in static_column_sort and static_column_sort.get(key) is not None:
                        sort_item[key] = static_column_sort.get(key)

            # Preserve optional semantic hints for backend ordering (e.g. tenor sort)
            # and hidden-key directives for deterministic curve-pillar ordering.
            for key in ("semanticType", "sortSemantic", "nulls", "sortType", "sortKeyField"):
                if key in s:
                    sort_item[key] = s.get(key)
            tanstack_sorting.append(sort_item)

        request_columns = self._build_request_columns(state.row_fields, state.col_fields, state.val_configs)
        pagination_info = self._build_pagination(context)

        request = TanStackRequest(
            operation=TanStackOperation.GET_UNIQUE_VALUES if target_unique_col else TanStackOperation.GET_DATA,
            table=context.table,
            columns=request_columns,
            filters=state.filters or {},
            sorting=tanstack_sorting,
            grouping=state.row_fields or [],
            aggregations=[],
            pagination=pagination_info,
            global_filter=(state.filters or {}).get("global")
            if isinstance(state.filters, dict)
            else None,
            totals=state.show_col_totals,
            row_totals=state.show_row_totals,
            version=context.window_seq,
            column_sort_options=column_sort_options or None,
        )

        trigger_kind = context.trigger_kind
        if trigger_kind == "drill" and state.drill_through:
            try:
                records = await adapter.handle_drill_through(request, state.drill_through)
            except Exception as exc:  # pragma: no cover - defensive
                if self._debug:
                    print(f"Drill through failed: {exc}")
                records = []
            return PivotServiceResponse(status="drillthrough", drill_records=records)

        if trigger_kind == "update" and state.cell_update:
            try:
                await adapter.handle_update(request, state.cell_update)
            except Exception as exc:  # pragma: no cover - defensive
                if self._debug:
                    print(f"Cell update failed: {exc}")

        if target_unique_col:
            request.global_filter = target_unique_col
            response = await adapter.handle_request(request)
            new_options = {
                **current_filter_options,
                target_unique_col: [d.get("value") for d in (response.data or [])],
            }
            return PivotServiceResponse(status="unique_values", filter_options=new_options)

        expanded_paths = self._parse_expanded_paths(state.expanded)

        try:
            if trigger_kind == "chart":
                requested_series_ids = (
                    [
                        value for value in (state.chart_request or {}).get("series_column_ids", [])
                        if isinstance(value, str) and value
                    ]
                    if isinstance((state.chart_request or {}).get("series_column_ids"), list)
                    else None
                )
                response = await adapter.handle_virtual_scroll_request(
                    request,
                    context.start_row,
                    context.end_row if context.end_row is not None else context.start_row,
                    expanded_paths,
                    col_start=context.col_start,
                    col_end=context.col_end,
                    needs_col_schema=bool((state.chart_request or {}).get("needs_col_schema", False)),
                    include_grand_total=context.include_grand_total,
                    requested_center_ids=requested_series_ids,
                )
            elif context.viewport_active and context.end_row is not None:
                response = await adapter.handle_virtual_scroll_request(
                    request,
                    context.start_row,
                    context.end_row,
                    expanded_paths,
                    col_start=context.col_start,
                    col_end=context.col_end,
                    needs_col_schema=context.needs_col_schema,
                    include_grand_total=context.include_grand_total,
                )
            else:
                if state.row_fields:
                    initial_end_row = min(request.pagination.get("pageSize", 1000), 100) - 1
                    response = await adapter.handle_virtual_scroll_request(
                        request,
                        0,
                        initial_end_row,
                        expanded_paths,
                        needs_col_schema=True,
                    )
                else:
                    response = await adapter.handle_request(request)
        except Exception as exc:
            if self._debug:
                print(f"Pivot execution failed: {exc}")
            return PivotServiceResponse(status="error", message=str(exc), data=[], total_rows=0)

        response_version = context.window_seq if context.window_seq is not None else response.version
        if not self._session_gate.response_is_current(
            session_id=context.session_id,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            abort_generation=context.abort_generation,
            intent=context.intent,
            client_instance=context.client_instance,
        ):
            return PivotServiceResponse(status="stale")

        if trigger_kind == "chart":
            return PivotServiceResponse(
                status="chart_data",
                chart_data={
                    "rows": list(response.data or []),
                    "columns": list(response.columns or []),
                    "colSchema": response.col_schema,
                    "rowStart": context.start_row,
                    "rowEnd": context.end_row,
                    "colStart": context.col_start,
                    "colEnd": context.col_end,
                    "totalRows": response.total_rows,
                    "dataVersion": response_version,
                    "stateEpoch": context.state_epoch,
                    "abortGeneration": context.abort_generation,
                    "windowSeq": context.window_seq,
                    "paneId": (state.chart_request or {}).get("pane_id"),
                    "requestSignature": (state.chart_request or {}).get("request_signature"),
                },
                data_version=response_version,
            )

        cols_payload: List[Dict[str, Any]] = list(response.columns or [])
        should_emit_columns = (
            context.needs_col_schema
            or not context.viewport_active
            or (context.intent == "structural" and context.original_intent != "expansion")
        )
        should_attach_col_schema = bool(response.col_schema) and (
            context.needs_col_schema or should_emit_columns
        )
        if should_attach_col_schema:
            cols_payload = cols_payload + [{"id": "__col_schema", "col_schema": response.col_schema}]

        return PivotServiceResponse(
            status="data",
            data=response.data,
            total_rows=response.total_rows,
            columns=cols_payload if should_emit_columns else None,
            data_offset=context.start_row,
            data_version=response_version,
            color_scale_stats=response.color_scale_stats,
        )

    def process(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
        current_filter_options: Optional[Dict[str, Any]] = None,
    ) -> PivotServiceResponse:
        """Sync wrapper for sync transports."""
        return asyncio.run(self.process_async(state, context, current_filter_options=current_filter_options))

    @staticmethod
    def _normalize_formula_reference_key(value: Any, fallback: Any = "formula") -> str:
        base = str(value or "").strip().lower()
        base = re.sub(r"\s+", "", base)
        base = re.sub(r"[^a-z0-9_]", "", base)
        fallback_base = re.sub(r"[^a-z0-9_]", "", str(fallback or "formula").strip().lower()) or "formula"
        normalized = base or fallback_base
        return normalized if re.match(r"^[a-z_]", normalized) else f"f_{normalized}"

    @staticmethod
    def _build_request_columns(
        row_fields: Optional[List[str]],
        col_fields: Optional[List[str]],
        val_configs: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        columns: List[Dict[str, Any]] = []
        for field in (row_fields or []):
            columns.append({"id": field})
        for field in (col_fields or []):
            columns.append({"id": field})
        for measure in (val_configs or []):
            if not isinstance(measure, dict):
                continue
            field = measure.get("field")
            agg = measure.get("agg")
            if not field or not agg:
                continue
            if agg == "formula":
                # Formula columns are computed post-aggregation; pass through as metadata only
                formula_label = measure.get("label") or field
                formula_ref = measure.get("formulaRef") or measure.get("referenceKey") or PivotRuntimeService._normalize_formula_reference_key(formula_label, field)
                columns.append(
                    {
                        "id": field,
                        "header": formula_label,
                        "accessorKey": field,
                        "formulaExpr": measure.get("formula", ""),
                        "formulaRef": formula_ref,
                        "formulaLabel": formula_label,
                        "isFormula": True,
                    }
                )
                continue
            columns.append(
                {
                    "id": f"{field}_{agg}",
                    "aggregationField": field,
                    "aggregationFn": agg,
                    "windowFn": measure.get("windowFn"),
                    "weightField": measure.get("weightField"),
                }
            )
        return columns

    @staticmethod
    def _build_pagination(context: PivotRequestContext) -> Dict[str, Any]:
        pagination = {"pageIndex": 0, "pageSize": 1000}
        if context.viewport_active and context.end_row is not None:
            count = max(context.end_row - context.start_row + 1, 1)
            pagination = {
                "pageIndex": context.start_row // 1000,
                "pageSize": min(count, 1000),
                "startRow": context.start_row,
                "endRow": context.end_row,
            }
        return pagination

    @staticmethod
    def _parse_expanded_paths(expanded: Any) -> List[List[str]]:
        if expanded is None or (isinstance(expanded, dict) and len(expanded) == 0):
            return []
        if isinstance(expanded, dict):
            return [k.split("|||") for k, v in expanded.items() if v is True]
        if expanded is True:
            return [["__ALL__"]]
        return []
