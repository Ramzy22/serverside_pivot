"""Transport-agnostic runtime service for pivot requests."""

from __future__ import annotations

import asyncio
import re
from typing import Any, Callable, Dict, List, Optional

from ..tanstack_adapter import TanStackOperation, TanStackRequest, TanStackResponse

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
    ) -> PivotServiceResponse:
        """Process one pivot request and return a transport-neutral response."""
        adapter = self._adapter_getter()

        if not self._session_gate.register_request(
            session_id=context.session_id,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            abort_generation=context.abort_generation,
            intent=context.intent,
            client_instance=context.client_instance,
        ):
            return PivotServiceResponse(status="stale")

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
            operation=TanStackOperation.GET_DATA,
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

        expanded_paths = self._parse_expanded_paths(state.expanded)

        try:
            if (
                trigger_kind != "chart"
                and state.pivot_mode == "report"
                and self._has_branching_report_root(state.report_def)
            ):
                response = await self._handle_branching_report_request(
                    adapter,
                    request,
                    state,
                    context,
                    expanded_paths,
                )
            elif trigger_kind == "chart":
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

        # --- Report Mode: annotate rows with level metadata ---
        response_data = response.data
        response_total_rows = response.total_rows
        if (
            state.pivot_mode == "report"
            and state.report_def
            and isinstance(state.report_def, dict)
            and not self._has_branching_report_root(state.report_def)
        ):
            response_data, response_total_rows = self._apply_report_annotations(
                response_data, response_total_rows, state.report_def, state.row_fields or []
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
            data=response_data,
            total_rows=response_total_rows,
            columns=cols_payload if should_emit_columns else None,
            data_offset=context.start_row,
            data_version=response_version,
            color_scale_stats=response.color_scale_stats,
        )

    def process(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        """Sync wrapper for sync transports."""
        return asyncio.run(self.process_async(state, context))

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
    def _has_branching_report_root(report_def: Optional[Dict[str, Any]]) -> bool:
        return bool(
            isinstance(report_def, dict)
            and isinstance(report_def.get("root"), dict)
            and report_def.get("root", {}).get("field")
        )

    @staticmethod
    def _report_sort_value(value: Any) -> Any:
        if value is None:
            return (2, "", 0)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value != value:
                return (2, "", 0)
            return (0, "", float(value))
        return (1, str(value).lower(), 0)

    @staticmethod
    def _normalize_branching_report_node(node: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        field = node.get("field")
        if not isinstance(field, str) or not field:
            return None

        normalized = {
            "field": field,
            "label": node.get("label") if isinstance(node.get("label"), str) else "",
            "topN": int(node.get("topN")) if isinstance(node.get("topN"), (int, float)) and node.get("topN", 0) > 0 else None,
            "sortBy": node.get("sortBy") if isinstance(node.get("sortBy"), str) and node.get("sortBy") else None,
            "sortDir": "asc" if node.get("sortDir") == "asc" else "desc",
        }

        default_child = PivotRuntimeService._normalize_branching_report_node(node.get("defaultChild"))
        if default_child:
            normalized["defaultChild"] = default_child

        children_source = node.get("childrenByValue")
        if isinstance(children_source, dict):
            children_by_value = {}
            for key, child_node in children_source.items():
                normalized_child = PivotRuntimeService._normalize_branching_report_node(child_node)
                if normalized_child:
                    children_by_value[str(key)] = normalized_child
            if children_by_value:
                normalized["childrenByValue"] = children_by_value

        return normalized

    @staticmethod
    def _get_branching_report_child(node: Dict[str, Any], raw_value: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        children_by_value = node.get("childrenByValue")
        if isinstance(children_by_value, dict):
            exact_child = children_by_value.get(str(raw_value))
            if isinstance(exact_child, dict) and exact_child.get("field"):
                return exact_child
        default_child = node.get("defaultChild")
        if isinstance(default_child, dict) and default_child.get("field"):
            return default_child
        return None

    @staticmethod
    def _merge_exact_report_filter(filters: Dict[str, Any], field: str, value: Any) -> Dict[str, Any]:
        next_filters = dict(filters or {})
        next_filters[field] = {"type": "eq", "value": value}
        return next_filters

    async def _handle_branching_report_request(
        self,
        adapter: Any,
        base_request: TanStackRequest,
        state: PivotViewState,
        context: PivotRequestContext,
        expanded_paths: List[List[str]],
    ) -> TanStackResponse:
        report_root = self._normalize_branching_report_node((state.report_def or {}).get("root"))
        if not report_root:
            return TanStackResponse(data=[], columns=[], total_rows=0)

        expanded_all = expanded_paths == [["__ALL__"]] or state.expanded is True
        expanded_set = {
            "|||".join(path)
            for path in expanded_paths
            if isinstance(path, list) and path and path != ["__ALL__"]
        }

        emitted_columns: Optional[List[Dict[str, Any]]] = None
        emitted_schema: Optional[Dict[str, Any]] = None
        emitted_stats: Optional[Dict[str, Any]] = None
        base_filters = dict(base_request.filters or {})

        async def fetch_node_rows(
            node: Dict[str, Any],
            active_filters: Dict[str, Any],
            path_values: List[str],
            path_fields: List[str],
            depth: int,
        ) -> List[Dict[str, Any]]:
            nonlocal emitted_columns, emitted_schema, emitted_stats

            node_field = node.get("field")
            if not node_field:
                return []

            top_n = node.get("topN")
            sort_by = node.get("sortBy")
            sort_dir = node.get("sortDir") or "desc"
            branch_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=self._build_request_columns([node_field], [], state.val_configs),
                filters=active_filters,
                sorting=[{"id": sort_by, "desc": sort_dir != "asc"}] if sort_by else [],
                grouping=[node_field],
                aggregations=[],
                pagination={
                    "pageIndex": 0,
                    "pageSize": 100000,
                },
                global_filter=base_request.global_filter,
                totals=False,
                row_totals=False,
                version=base_request.version,
                column_sort_options=base_request.column_sort_options,
            )
            branch_response = await adapter.handle_request(branch_request)
            if emitted_columns is None:
                emitted_columns = list(branch_response.columns or [])
                emitted_schema = branch_response.col_schema
                emitted_stats = branch_response.color_scale_stats

            visible_rows = [
                row for row in (branch_response.data or [])
                if isinstance(row, dict) and not self._is_report_summary_row(row)
            ]
            if sort_by:
                visible_rows = sorted(
                    visible_rows,
                    key=lambda row: self._report_sort_value(row.get(sort_by)),
                    reverse=(sort_dir != "asc"),
                )

            total_count = branch_response.total_rows if isinstance(branch_response.total_rows, int) else len(visible_rows)
            if top_n and len(visible_rows) > top_n:
                visible_rows = visible_rows[:top_n]

            output_rows: List[Dict[str, Any]] = []
            for row in visible_rows:
                raw_value = row.get(node_field, row.get("_id"))
                path_token = "" if raw_value is None else str(raw_value)
                row_path_values = path_values + [path_token]
                row_path_fields = path_fields + [node_field]
                row_path = "|||".join(row_path_values)
                child_node = self._get_branching_report_child(node, raw_value)
                has_children = bool(child_node and child_node.get("field"))
                is_expanded = expanded_all or row_path in expanded_set

                row_out = dict(row)
                row_out["_id"] = path_token or str(row.get("_id") or "")
                row_out["_path"] = row_path
                row_out["depth"] = depth
                row_out["_levelLabel"] = node.get("label") or node_field
                row_out["_levelField"] = node_field
                row_out["_pathFields"] = list(row_path_fields)
                row_out["_has_children"] = has_children
                row_out["_is_expanded"] = bool(is_expanded and has_children)
                if top_n:
                    row_out["_levelTopN"] = int(top_n)
                    if total_count > len(visible_rows):
                        row_out["_groupTotalCount"] = total_count
                output_rows.append(row_out)

                if has_children and is_expanded:
                    child_filters = self._merge_exact_report_filter(active_filters, node_field, raw_value)
                    child_rows = await fetch_node_rows(
                        child_node,
                        child_filters,
                        row_path_values,
                        row_path_fields,
                        depth + 1,
                    )
                    output_rows.extend(child_rows)

            return output_rows

        visible_report_rows = await fetch_node_rows(report_root, base_filters, [], [], 0)

        if context.include_grand_total and state.show_col_totals:
            grand_total_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=self._build_request_columns([], [], state.val_configs),
                filters=base_filters,
                sorting=[],
                grouping=[],
                aggregations=[],
                pagination={"pageIndex": 0, "pageSize": 1},
                global_filter=base_request.global_filter,
                totals=False,
                row_totals=False,
                version=base_request.version,
                column_sort_options=base_request.column_sort_options,
            )
            grand_total_response = await adapter.handle_request(grand_total_request)
            if emitted_columns is None:
                emitted_columns = list(grand_total_response.columns or [])
                emitted_schema = grand_total_response.col_schema
                emitted_stats = grand_total_response.color_scale_stats
            grand_total_row = next(
                (row for row in (grand_total_response.data or []) if isinstance(row, dict)),
                None,
            )
            if grand_total_row:
                total_row = dict(grand_total_row)
                total_row["_id"] = "Grand Total"
                total_row["_path"] = "__grand_total__"
                total_row["_isTotal"] = True
                total_row["depth"] = 0
                total_row["_has_children"] = False
                total_row["_is_expanded"] = False
                visible_report_rows.append(total_row)

        total_rows = len(visible_report_rows)
        if context.viewport_active and context.end_row is not None:
            window_start = max(context.start_row, 0)
            window_end = max(context.end_row + 1, window_start)
            windowed_rows = visible_report_rows[window_start:window_end]
        else:
            windowed_rows = visible_report_rows

        return TanStackResponse(
            data=windowed_rows,
            columns=emitted_columns or [],
            total_rows=total_rows,
            version=base_request.version,
            col_schema=emitted_schema,
            color_scale_stats=emitted_stats,
        )

    @staticmethod
    def _resolve_report_level(
        report_def: Dict[str, Any],
        row_fields: List[str],
        depth: int,
        parent_values: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve the LevelRule for a given depth, with conditional children support.

        Walks the levels list from depth 0. At each depth, if the previous level
        has conditionalChildren and the parent value matches a key in that map,
        follow the override; otherwise follow the wildcard "*" or fall through
        to the flat levels list.
        """
        levels = report_def.get("levels") or []
        if not levels:
            return None

        current_rule = levels[0] if levels else None
        for d in range(1, depth + 1):
            if current_rule is None:
                break
            conditional = current_rule.get("conditionalChildren")
            parent_val = parent_values[d - 1] if parent_values and d - 1 < len(parent_values) else None

            if isinstance(conditional, dict) and conditional:
                # Try exact match first, then wildcard
                if parent_val is not None and str(parent_val) in conditional:
                    current_rule = conditional[str(parent_val)]
                elif "*" in conditional:
                    current_rule = conditional["*"]
                elif d < len(levels):
                    current_rule = levels[d]
                else:
                    current_rule = None
            elif d < len(levels):
                current_rule = levels[d]
            else:
                current_rule = None

        return current_rule

    @staticmethod
    def _report_row_depth(row: Dict[str, Any]) -> int:
        depth = row.get("depth", 0)
        if isinstance(depth, str):
            try:
                return int(depth)
            except (ValueError, TypeError):
                return 0
        return int(depth) if isinstance(depth, (int, float)) else 0

    @staticmethod
    def _report_row_path_parts(row: Dict[str, Any]) -> List[str]:
        path_str = row.get("_path", "")
        return path_str.split("|||") if isinstance(path_str, str) and path_str else []

    @staticmethod
    def _is_report_summary_row(row: Dict[str, Any]) -> bool:
        return bool(
            row.get("_isTotal")
            or row.get("_id") == "Grand Total"
            or row.get("_path") == "__grand_total__"
        )

    @staticmethod
    def _count_report_tree_rows(node: Dict[str, Any]) -> int:
        total = 1 + len(node.get("after_rows") or [])
        for child in node.get("children") or []:
            total += PivotRuntimeService._count_report_tree_rows(child)
        return total

    @staticmethod
    def _report_sort_key(value: Any) -> Any:
        if value is None:
            return (2, "", 0)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value != value:
                return (2, "", 0)
            return (0, "", float(value))
        return (1, str(value).lower(), 0)

    @staticmethod
    def _apply_report_annotations(
        data: Optional[List[Dict[str, Any]]],
        total_rows: Optional[int],
        report_def: Dict[str, Any],
        row_fields: List[str],
    ) -> tuple:
        """Annotate rows with report level metadata AND enforce topN.

        Two-pass approach:
        1. Annotate every row with _levelLabel, _levelField, _levelTopN
        2. Enforce topN by grouping children by their parent path and keeping
           only the first N at each depth where a topN rule applies.
        """
        if not data:
            return data, total_rows

        levels = report_def.get("levels") or []
        if not levels:
            return data, total_rows

        # --- Pass 1: annotate ---
        annotated = []
        for row in data:
            if not isinstance(row, dict):
                annotated.append(row)
                continue

            if PivotRuntimeService._is_report_summary_row(row):
                annotated.append(row)
                continue

            depth = PivotRuntimeService._report_row_depth(row)
            parent_values = PivotRuntimeService._report_row_path_parts(row)

            rule = PivotRuntimeService._resolve_report_level(
                report_def, row_fields, depth, parent_values
            )

            if rule and isinstance(rule, dict):
                row = {**row}
                row["_levelLabel"] = rule.get("label") or rule.get("field") or ""
                row["_levelField"] = rule.get("field") or ""
                top_n = rule.get("topN")
                if top_n and isinstance(top_n, (int, float)) and top_n > 0:
                    row["_levelTopN"] = int(top_n)
                sort_by = rule.get("sortBy")
                if isinstance(sort_by, str) and sort_by:
                    row["__reportSortBy"] = sort_by
                row["__reportSortDir"] = "asc" if rule.get("sortDir") == "asc" else "desc"

            annotated.append(row)

        root_prefix_rows: List[Any] = []
        root_suffix_rows: List[Any] = []
        roots: List[Dict[str, Any]] = []
        stack: List[Dict[str, Any]] = []
        saw_regular_row = False

        for row in annotated:
            if not isinstance(row, dict):
                target = root_suffix_rows if saw_regular_row else root_prefix_rows
                target.append(row)
                continue

            if PivotRuntimeService._is_report_summary_row(row):
                target_node = None
                if row.get("_path") != "__grand_total__":
                    row_depth = PivotRuntimeService._report_row_depth(row)
                    for candidate in reversed(stack):
                        if candidate["depth"] <= row_depth:
                            target_node = candidate
                            break
                    if target_node is None and stack:
                        target_node = stack[-1]
                if target_node is None:
                    target = root_suffix_rows if saw_regular_row else root_prefix_rows
                    target.append(row)
                else:
                    target_node["after_rows"].append(row)
                continue

            depth = PivotRuntimeService._report_row_depth(row)
            while stack and stack[-1]["depth"] >= depth:
                stack.pop()

            node = {
                "row": row,
                "depth": depth,
                "children": [],
                "after_rows": [],
            }
            if stack:
                stack[-1]["children"].append(node)
            else:
                roots.append(node)
            stack.append(node)
            saw_regular_row = True

        def _flatten_report_nodes(nodes: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
            if not nodes:
                return [], 0

            working_nodes = list(nodes)
            sort_by = working_nodes[0]["row"].get("__reportSortBy")
            sort_dir = working_nodes[0]["row"].get("__reportSortDir") or "desc"
            if isinstance(sort_by, str) and sort_by:
                sortable_nodes = []
                missing_nodes = []
                for node in working_nodes:
                    value = node["row"].get(sort_by)
                    if value is None or value != value:
                        missing_nodes.append(node)
                    else:
                        sortable_nodes.append(node)
                sortable_nodes = sorted(
                    sortable_nodes,
                    key=lambda node: PivotRuntimeService._report_sort_key(node["row"].get(sort_by)),
                    reverse=(sort_dir != "asc"),
                )
                working_nodes = sortable_nodes + missing_nodes

            top_n = working_nodes[0]["row"].get("_levelTopN")
            total_count = len(working_nodes)
            removed = 0
            if top_n and isinstance(top_n, int) and top_n > 0 and total_count > top_n:
                removed = sum(
                    PivotRuntimeService._count_report_tree_rows(node)
                    for node in working_nodes[top_n:]
                )
                working_nodes = working_nodes[:top_n]

            trimmed_count = len(working_nodes)
            flattened: List[Dict[str, Any]] = []
            for node in working_nodes:
                row = node["row"]
                row_out = {k: v for k, v in row.items() if k not in {"__reportSortBy", "__reportSortDir"}}
                if top_n and total_count > trimmed_count:
                    row_out["_groupTotalCount"] = total_count
                child_rows, child_removed = _flatten_report_nodes(node["children"])
                removed += child_removed
                flattened.append(row_out)
                flattened.extend(child_rows)
                flattened.extend(node["after_rows"])

            return flattened, removed

        filtered_rows, removed_count = _flatten_report_nodes(roots)
        filtered = root_prefix_rows + filtered_rows + root_suffix_rows

        adjusted_total = (total_rows - removed_count) if total_rows is not None and removed_count > 0 else total_rows
        return filtered, adjusted_total

    @staticmethod
    def _parse_expanded_paths(expanded: Any) -> List[List[str]]:
        if expanded is None or (isinstance(expanded, dict) and len(expanded) == 0):
            return []
        if isinstance(expanded, dict):
            return [k.split("|||") for k, v in expanded.items() if v is True]
        if expanded is True:
            return [["__ALL__"]]
        return []
