"""Report hierarchy normalization, execution, and row annotation."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

from ..tanstack_adapter import TanStackOperation, TanStackRequest, TanStackResponse
from .models import PivotRequestContext, PivotViewState
from .request_factory import RuntimeRequestFactory


class RuntimeReportService:
    """Own report tree semantics independently from runtime routing."""

    @staticmethod
    def _has_branching_report_root(report_def: Optional[Dict[str, Any]]) -> bool:
        return bool(
            isinstance(report_def, dict)
            and isinstance(report_def.get("root"), dict)
            and report_def.get("root", {}).get("field")
        )

    @staticmethod
    def _normalize_report_condition(condition: Any) -> Dict[str, Any]:
        if not isinstance(condition, dict):
            return {"op": "AND", "clauses": []}
        op = "OR" if condition.get("op") == "OR" else "AND"
        clauses = []
        for clause in condition.get("clauses") or []:
            if (
                not isinstance(clause, dict)
                or not isinstance(clause.get("field"), str)
                or not clause["field"]
            ):
                continue
            clauses.append(
                {
                    "field": clause["field"],
                    "operator": str(clause.get("operator") or "eq"),
                    "value": str(clause.get("value", "")),
                    "values": [
                        str(v) for v in (clause.get("values") or []) if v is not None
                    ],
                }
            )
        return {"op": op, "clauses": clauses}

    @staticmethod
    def _normalize_report_format(format_value: Any) -> Dict[str, Any]:
        source = format_value if isinstance(format_value, dict) else {}

        def _str(key: str) -> str:
            value = source.get(key)
            return value if isinstance(value, str) else ""

        indent_raw = source.get("indent")
        try:
            indent = int(indent_raw)
            if indent < 0:
                indent = None
        except (TypeError, ValueError):
            indent = None

        border_width_raw = source.get("borderWidth")
        try:
            border_width = int(border_width_raw)
            if border_width <= 0:
                border_width = None
        except (TypeError, ValueError):
            border_width = None

        border_style = _str("borderStyle")
        if border_style not in {"", "solid", "dashed", "dotted", "double"}:
            border_style = ""

        normalized: Dict[str, Any] = {
            "bold": source.get("bold") is True,
            "showSubtotal": False if source.get("showSubtotal") is False else True,
            "rowColor": _str("rowColor"),
            "labelPrefix": _str("labelPrefix"),
            "labelSuffix": _str("labelSuffix"),
            "numberFormat": _str("numberFormat"),
            "borderStyle": border_style,
            "borderColor": _str("borderColor"),
        }
        if indent is not None:
            normalized["indent"] = indent
        if border_width is not None:
            normalized["borderWidth"] = border_width
        return normalized

    @staticmethod
    def _report_condition_summary(condition: Optional[Dict[str, Any]]) -> str:
        if not isinstance(condition, dict) or not condition.get("clauses"):
            return ""
        parts = []
        for clause in condition.get("clauses") or []:
            if not isinstance(clause, dict):
                continue
            field = str(clause.get("field") or "")
            operator = str(clause.get("operator") or "eq")
            if operator in {"in", "not_in"}:
                values = clause.get("values") or []
                parts.append(
                    f"{field} {operator} [{', '.join(str(v) for v in values[:3])}]"
                )
            else:
                parts.append(f"{field} {operator} {clause.get('value', '')}")
        return f" {condition.get('op') or 'AND'} ".join(parts)

    @staticmethod
    def _report_display_label(
        base_label: Any, report_format: Optional[Dict[str, Any]]
    ) -> str:
        text = "" if base_label is None else str(base_label)
        fmt = report_format if isinstance(report_format, dict) else {}
        return f"{fmt.get('labelPrefix') or ''}{text}{fmt.get('labelSuffix') or ''}"

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
        node_format = RuntimeReportService._normalize_report_format(node.get("format"))
        report_format = dict(node_format)
        row_out["_reportFormat"] = report_format
        row_out["_reportDisplayLabel"] = RuntimeReportService._report_display_label(
            base_label, report_format
        )
        row_out["_reportDebug"] = {
            "matchedLevel": node.get("label") or node.get("field") or "",
            "levelLabel": node.get("label") or node.get("field") or "",
            "levelField": node.get("field") or "",
            "depth": depth,
            "rowPath": row_path,
            "pathValues": list(row_path_values or []),
            "pathFields": list(row_path_fields or []),
            "childrenSource": children_source,
            "overrideApplied": isinstance(branch_match, dict),
            "matchedCondition": RuntimeReportService._report_condition_summary(
                branch_match.get("condition")
                if isinstance(branch_match, dict)
                else None
            ),
        }
        return row_out

    @staticmethod
    def _evaluate_report_clause(clause: Dict[str, Any], row: Dict[str, Any]) -> bool:
        field = clause.get("field", "")
        operator = clause.get("operator", "eq")
        raw_value = row.get(field)
        str_value = "" if raw_value is None else str(raw_value)
        clause_value = str(clause.get("value", ""))
        clause_values = clause.get("values") or []
        if operator == "eq":
            return str_value == clause_value
        if operator == "not_eq":
            return str_value != clause_value
        if operator == "in":
            return str_value in clause_values
        if operator == "not_in":
            return str_value not in clause_values
        if operator == "contains":
            return clause_value.lower() in str_value.lower()
        if operator == "not_contains":
            return clause_value.lower() not in str_value.lower()
        try:
            num_value = float(str_value)
            num_clause = float(clause_value)
        except (ValueError, TypeError):
            return False
        if operator == "gt":
            return num_value > num_clause
        if operator == "gte":
            return num_value >= num_clause
        if operator == "lt":
            return num_value < num_clause
        if operator == "lte":
            return num_value <= num_clause
        return False

    @staticmethod
    def _evaluate_report_condition(
        condition: Dict[str, Any], row: Dict[str, Any]
    ) -> bool:
        clauses = condition.get("clauses") or []
        if not clauses:
            return True
        if condition.get("op") == "OR":
            return any(
                RuntimeReportService._evaluate_report_clause(c, row) for c in clauses
            )
        return all(
            RuntimeReportService._evaluate_report_clause(c, row) for c in clauses
        )

    @staticmethod
    def _normalize_branching_report_node(
        node: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        field = node.get("field")
        if not isinstance(field, str) or not field:
            return None

        normalized = {
            "field": field,
            "label": node.get("label") if isinstance(node.get("label"), str) else "",
            "topN": (
                int(node.get("topN"))
                if isinstance(node.get("topN"), (int, float))
                and node.get("topN", 0) > 0
                else None
            ),
            "sortBy": (
                node.get("sortBy")
                if isinstance(node.get("sortBy"), str) and node.get("sortBy")
                else None
            ),
            "sortDir": "asc" if node.get("sortDir") == "asc" else "desc",
            "sortAbs": node.get("sortAbs") is True,
            "format": RuntimeReportService._normalize_report_format(node.get("format")),
        }

        node_filters_raw = node.get("filters")
        if (
            isinstance(node_filters_raw, dict)
            and isinstance(node_filters_raw.get("clauses"), list)
            and node_filters_raw["clauses"]
        ):
            normalized["filters"] = RuntimeReportService._normalize_report_condition(
                node_filters_raw
            )

        default_child = RuntimeReportService._normalize_branching_report_node(
            node.get("defaultChild")
        )
        if default_child:
            normalized["defaultChild"] = default_child

        branches_source = node.get("branches")
        if isinstance(branches_source, list) and branches_source:
            normalized_branches = []
            for branch in branches_source:
                if not isinstance(branch, dict):
                    continue
                norm_child = RuntimeReportService._normalize_branching_report_node(
                    branch.get("child")
                )
                normalized_branches.append(
                    {
                        "label": (
                            branch.get("label", "")
                            if isinstance(branch.get("label"), str)
                            else ""
                        ),
                        "condition": RuntimeReportService._normalize_report_condition(
                            branch.get("condition")
                        ),
                        "sourceRowPath": (
                            [str(v) for v in (branch.get("sourceRowPath") or [])]
                            if isinstance(branch.get("sourceRowPath"), list)
                            else []
                        ),
                        "sourcePathFields": (
                            [str(v) for v in (branch.get("sourcePathFields") or [])]
                            if isinstance(branch.get("sourcePathFields"), list)
                            else []
                        ),
                        "child": norm_child,
                    }
                )
            if normalized_branches:
                normalized["branches"] = normalized_branches
        else:
            children_source = node.get("childrenByValue")
            if isinstance(children_source, dict):
                children_by_value = {}
                for key, child_node in children_source.items():
                    normalized_child = (
                        RuntimeReportService._normalize_branching_report_node(
                            child_node
                        )
                    )
                    if normalized_child:
                        children_by_value[str(key)] = normalized_child
                if children_by_value:
                    normalized["childrenByValue"] = children_by_value

        return normalized

    @staticmethod
    def _get_branching_report_branch(
        node: Dict[str, Any], row: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict) or row is None:
            return None
        branches = node.get("branches")
        if not isinstance(branches, list) or not branches:
            return None
        for branch in branches:
            if not isinstance(branch, dict):
                continue
            condition = branch.get("condition")
            if isinstance(
                condition, dict
            ) and RuntimeReportService._evaluate_report_condition(condition, row):
                return branch
        return None

    @staticmethod
    def _get_branching_report_child(
        node: Dict[str, Any], raw_value: Any, row: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        branch = RuntimeReportService._get_branching_report_branch(node, row)
        if isinstance(branch, dict):
            child = branch.get("child")
            if isinstance(child, dict) and child.get("field"):
                return child
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
    def _merge_exact_report_filter(
        filters: Dict[str, Any], field: str, value: Any
    ) -> Dict[str, Any]:
        next_filters = dict(filters or {})
        next_filters[field] = {"type": "eq", "value": value}
        return next_filters

    @staticmethod
    def _merge_in_report_filter(
        filters: Dict[str, Any], field: str, values: List[Any]
    ) -> Dict[str, Any]:
        next_filters = dict(filters or {})
        next_filters[field] = {"type": "in", "value": list(values or [])}
        return next_filters

    @staticmethod
    def _is_report_numeric_value(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float, Decimal)):
            try:
                return value == value
            except Exception:
                return False
        return False

    @staticmethod
    def _build_report_other_row_from_rows(
        rows: List[Dict[str, Any]],
        node_field: str,
    ) -> Dict[str, Any]:
        """Build a best-effort aggregate from already-aggregated sibling rows."""
        aggregate: Dict[str, Any] = {}
        ordered_keys: List[str] = []
        seen = set()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    ordered_keys.append(key)

        ignored = {
            "depth",
            "_id",
            "_label",
            "_path",
            "_rowKey",
            "_levelLabel",
            "_levelField",
            "_pathFields",
            "_has_children",
            "_is_expanded",
            "_isTotal",
            "_isOther",
            "_levelTopN",
            "_groupTotalCount",
            "_otherCount",
            "__reportSortBy",
            "__reportSortDir",
        }
        for key in ordered_keys:
            if key == node_field or key in ignored or str(key).startswith("_"):
                continue
            values = [
                row.get(key)
                for row in rows or []
                if isinstance(row, dict) and row.get(key) is not None
            ]
            numeric_values = [
                value
                for value in values
                if RuntimeReportService._is_report_numeric_value(value)
            ]
            if values and len(values) == len(numeric_values):
                total = numeric_values[0]
                for value in numeric_values[1:]:
                    total += value
                aggregate[key] = total

        return aggregate

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
        row_out = dict(base_row or {})
        # Internal hierarchy sentinel, not a credential.
        other_token = "__other__"  # nosec B105
        row_path_values = list(path_values or []) + [other_token]
        row_path = "|||".join(row_path_values)
        row_path_fields = list(path_fields or []) + ([node_field] if node_field else [])

        if node_field:
            row_out[node_field] = "Other"
        row_out["_id"] = "Other"
        row_out["_label"] = "Other"
        row_out["_path"] = row_path
        row_out["_rowKey"] = row_path
        row_out["depth"] = depth
        row_out["_levelLabel"] = level_label or node_field or ""
        row_out["_levelField"] = node_field or ""
        row_out["_pathFields"] = row_path_fields
        row_out["_has_children"] = False
        row_out["_is_expanded"] = False
        row_out["_isOther"] = True
        row_out["_otherCount"] = max(int(source_count or 0), 0)
        row_out.pop("_isTotal", None)
        return row_out

    async def _handle_branching_report_request(
        self,
        adapter: Any,
        base_request: TanStackRequest,
        state: PivotViewState,
        context: PivotRequestContext,
        expanded_paths: List[List[str]],
    ) -> TanStackResponse:
        report_root = self._normalize_branching_report_node(
            (state.report_def or {}).get("root")
        )
        if not report_root:
            return TanStackResponse(data=[], columns=[], total_rows=0)

        expanded_all = (
            any(path == ["__ALL__"] for path in expanded_paths)
            or state.expanded is True
        )
        expanded_set = {
            "|||".join(path)
            for path in expanded_paths
            if isinstance(path, list)
            and path
            and path != ["__ALL__"]
            and path[0] != "__COLLAPSED__"
        }
        collapsed_set = {
            "|||".join(path[1:])
            for path in expanded_paths
            if isinstance(path, list) and len(path) > 1 and path[0] == "__COLLAPSED__"
        }

        emitted_columns: Optional[List[Dict[str, Any]]] = None
        emitted_schema: Optional[Dict[str, Any]] = None
        emitted_stats: Optional[Dict[str, Any]] = None
        base_filters = dict(base_request.filters or {})

        async def execute_report_query(request: TanStackRequest) -> TanStackResponse:
            # Branching reports retain the same pivot axes as the public view.
            # The existing adapter owns the column catalog and aggregate-first
            # execution; reports only annotate and arrange the resulting rows.
            if state.col_fields and request.grouping:
                return await adapter.handle_virtual_scroll_request(
                    request, 0, 100000, needs_col_schema=True,
                    include_grand_total=False, user=context.principal,
                    row_scope=context.row_scope,
                )
            return await adapter.handle_request(
                request, user=context.principal, row_scope=context.row_scope,
            )

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
            sort_by_raw = node.get("sortBy")
            sort_by = node_field if sort_by_raw == "__field__" else sort_by_raw
            sort_dir = node.get("sortDir") or "desc"
            sort_abs = node.get("sortAbs") is True
            branch_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=RuntimeRequestFactory.build_columns(
                    [node_field], state.col_fields, state.val_configs
                ),
                filters=active_filters,
                custom_dimensions=base_request.custom_dimensions or [],
                measure_axis=base_request.measure_axis or None,
                sorting=(
                    [{"id": sort_by, "desc": sort_dir != "asc"}]
                    if sort_by and sort_by_raw != "__field__"
                    else []
                ),
                grouping=[node_field],
                aggregations=[],
                pagination={
                    "pageIndex": 0,
                    "pageSize": 100000,
                },
                global_filter=base_request.global_filter,
                totals=False,
                row_totals=bool(state.col_fields and state.show_row_totals),
                version=base_request.version,
                column_sort_options=base_request.column_sort_options,
            )
            branch_response = await execute_report_query(branch_request)
            if emitted_columns is None:
                emitted_columns = [
                    c
                    for c in (branch_response.columns or [])
                    if not (isinstance(c, dict) and c.get("_isImplicitFormulaRef"))
                ]
                emitted_schema = branch_response.col_schema
                emitted_stats = branch_response.color_scale_stats

            visible_rows = [
                row
                for row in (branch_response.data or [])
                if isinstance(row, dict) and not self._is_report_summary_row(row)
            ]
            if sort_by:
                visible_rows = sorted(
                    visible_rows,
                    key=lambda row: self._report_sort_key(row.get(sort_by), sort_abs),
                    reverse=(sort_dir != "asc"),
                )

            # Apply level filter; rows that don't match go into Others to preserve totals
            filter_excluded: List[Dict[str, Any]] = []
            node_level_filters = node.get("filters")
            if isinstance(node_level_filters, dict) and (
                node_level_filters.get("clauses") or []
            ):
                pass_rows: List[Dict[str, Any]] = []
                for _row in visible_rows:
                    if RuntimeReportService._evaluate_report_condition(
                        node_level_filters, _row
                    ):
                        pass_rows.append(_row)
                    else:
                        filter_excluded.append(_row)
                visible_rows = pass_rows

            total_count = (
                branch_response.total_rows
                if isinstance(branch_response.total_rows, int)
                else len(visible_rows)
            )
            other_rows: List[Dict[str, Any]] = list(filter_excluded)
            if top_n and len(visible_rows) > top_n:
                other_rows.extend(visible_rows[top_n:])
                visible_rows = visible_rows[:top_n]

            output_rows: List[Dict[str, Any]] = []
            for row in visible_rows:
                raw_value = row.get(node_field, row.get("_id"))
                path_token = "" if raw_value is None else str(raw_value)
                row_path_values = path_values + [path_token]
                row_path_fields = path_fields + [node_field]
                row_path = "|||".join(row_path_values)
                branch_match = self._get_branching_report_branch(node, row)
                child_node = self._get_branching_report_child(node, raw_value, row)
                has_children = bool(child_node and child_node.get("field"))
                is_expanded = (
                    expanded_all and row_path not in collapsed_set
                ) or row_path in expanded_set

                row_out = dict(row)
                if state.col_fields:
                    for column in (emitted_schema or {}).get("columns", []):
                        row_out.setdefault(column["id"], None)
                row_out["_id"] = path_token or str(row.get("_id") or "")
                if (
                    isinstance(branch_match, dict)
                    and isinstance(branch_match.get("label"), str)
                    and branch_match.get("label")
                ):
                    row_out["_label"] = branch_match["label"]
                row_out["_path"] = row_path
                row_out["depth"] = depth
                row_out["_levelLabel"] = node.get("label") or node_field
                row_out["_levelField"] = node_field
                row_out["_pathFields"] = list(row_path_fields)
                row_out["_has_children"] = has_children
                row_out["_is_expanded"] = bool(is_expanded and has_children)
                base_label = row_out.get("_label", row_out.get("_id", path_token))
                self._decorate_report_row_metadata(
                    row_out,
                    node=node,
                    depth=depth,
                    row_path=row_path,
                    row_path_values=row_path_values,
                    row_path_fields=row_path_fields,
                    base_label=base_label,
                    branch_match=branch_match,
                    children_source=(
                        "Custom children"
                        if isinstance(branch_match, dict)
                        and isinstance(branch_match.get("child"), dict)
                        else "Default children"
                    ),
                )
                if top_n:
                    row_out["_levelTopN"] = int(top_n)
                    if total_count > len(visible_rows):
                        row_out["_groupTotalCount"] = total_count
                output_rows.append(row_out)

                if has_children and is_expanded:
                    child_filters = self._merge_exact_report_filter(
                        active_filters, node_field, raw_value
                    )
                    child_rows = await fetch_node_rows(
                        child_node,
                        child_filters,
                        row_path_values,
                        row_path_fields,
                        depth + 1,
                    )
                    output_rows.extend(child_rows)

            report_format = (
                node.get("format") if isinstance(node.get("format"), dict) else {}
            )
            if other_rows and report_format.get("showSubtotal") is not False:
                other_values = [
                    row.get(node_field, row.get("_id"))
                    for row in other_rows
                    if isinstance(row, dict)
                ]
                other_values = [value for value in other_values if value is not None]
                other_aggregate_row: Optional[Dict[str, Any]] = None
                if other_values:
                    other_request = TanStackRequest(
                        operation=TanStackOperation.GET_DATA,
                        table=base_request.table,
                        columns=RuntimeRequestFactory.build_columns(
                            [], state.col_fields, state.val_configs
                        ),
                        filters=self._merge_in_report_filter(
                            active_filters, node_field, other_values
                        ),
                        custom_dimensions=base_request.custom_dimensions or [],
                        measure_axis=base_request.measure_axis or None,
                        sorting=[],
                        grouping=[],
                        aggregations=[],
                        pagination={
                            "pageIndex": 0,
                            "pageSize": 1,
                        },
                        global_filter=base_request.global_filter,
                        totals=False,
                        row_totals=bool(state.col_fields and state.show_row_totals),
                        version=base_request.version,
                        column_sort_options=base_request.column_sort_options,
                    )
                    other_response = await execute_report_query(other_request)
                    other_aggregate_row = next(
                        (
                            candidate
                            for candidate in (other_response.data or [])
                            if isinstance(candidate, dict)
                        ),
                        None,
                    )

                if other_aggregate_row is None:
                    other_aggregate_row = self._build_report_other_row_from_rows(
                        other_rows, node_field
                    )

                other_out = self._decorate_report_other_row(
                    other_aggregate_row,
                    node_field=node_field,
                    path_values=path_values,
                    path_fields=path_fields,
                    depth=depth,
                    level_label=node.get("label") or node_field,
                    source_count=len(other_rows),
                )
                if state.col_fields:
                    for column in (emitted_schema or {}).get("columns", []):
                        other_out.setdefault(column["id"], None)
                self._decorate_report_row_metadata(
                    other_out,
                    node=node,
                    depth=depth,
                    row_path=other_out.get("_path") or "",
                    row_path_values=path_values + ["__other__"],
                    row_path_fields=path_fields + [node_field],
                    base_label="Other",
                    children_source="Subtotal/other row",
                )
                output_rows.append(other_out)

            return output_rows

        visible_report_rows = await fetch_node_rows(
            report_root, base_filters, [], [], 0
        )

        if context.include_grand_total and state.show_col_totals:
            grand_total_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=RuntimeRequestFactory.build_columns([], state.col_fields, state.val_configs),
                filters=base_filters,
                custom_dimensions=base_request.custom_dimensions or [],
                measure_axis=base_request.measure_axis or None,
                sorting=[],
                grouping=[],
                aggregations=[],
                pagination={"pageIndex": 0, "pageSize": 1},
                global_filter=base_request.global_filter,
                totals=False,
                row_totals=bool(state.col_fields and state.show_row_totals),
                version=base_request.version,
                column_sort_options=base_request.column_sort_options,
            )
            grand_total_response = await execute_report_query(grand_total_request)
            if emitted_columns is None:
                emitted_columns = list(grand_total_response.columns or [])
                emitted_schema = grand_total_response.col_schema
                emitted_stats = grand_total_response.color_scale_stats
            grand_total_row = next(
                (
                    row
                    for row in (grand_total_response.data or [])
                    if isinstance(row, dict)
                ),
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
            layout_revision=(emitted_schema or {}).get("layoutRevision"),
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
            parent_val = (
                parent_values[d - 1]
                if parent_values and d - 1 < len(parent_values)
                else None
            )

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
            total += RuntimeReportService._count_report_tree_rows(child)
        return total

    @staticmethod
    def _report_sort_key(value: Any, abs_sort: bool = False) -> Any:
        if value is None:
            return (2, "", 0)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value != value:
                return (2, "", 0)
            numeric = abs(float(value)) if abs_sort else float(value)
            return (0, "", numeric)
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

            if RuntimeReportService._is_report_summary_row(row):
                annotated.append(row)
                continue

            depth = RuntimeReportService._report_row_depth(row)
            parent_values = RuntimeReportService._report_row_path_parts(row)

            rule = RuntimeReportService._resolve_report_level(
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
                if sort_by == "__field__":
                    sort_by = rule.get("field") or None
                if isinstance(sort_by, str) and sort_by:
                    row["__reportSortBy"] = sort_by
                row["__reportSortDir"] = (
                    "asc" if rule.get("sortDir") == "asc" else "desc"
                )
                if rule.get("sortAbs") is True:
                    row["__reportSortAbs"] = True

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

            if RuntimeReportService._is_report_summary_row(row):
                target_node = None
                if row.get("_path") != "__grand_total__":
                    row_depth = RuntimeReportService._report_row_depth(row)
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

            depth = RuntimeReportService._report_row_depth(row)
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

        def _flatten_report_nodes(
            nodes: List[Dict[str, Any]],
        ) -> tuple[List[Dict[str, Any]], int]:
            if not nodes:
                return [], 0

            working_nodes = list(nodes)
            sort_by = working_nodes[0]["row"].get("__reportSortBy")
            sort_dir = working_nodes[0]["row"].get("__reportSortDir") or "desc"
            sort_abs = working_nodes[0]["row"].get("__reportSortAbs") is True
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
                    key=lambda node: RuntimeReportService._report_sort_key(
                        node["row"].get(sort_by), sort_abs
                    ),
                    reverse=(sort_dir != "asc"),
                )
                working_nodes = sortable_nodes + missing_nodes

            top_n = working_nodes[0]["row"].get("_levelTopN")
            total_count = len(working_nodes)
            removed = 0
            other_nodes: List[Dict[str, Any]] = []
            if top_n and isinstance(top_n, int) and top_n > 0 and total_count > top_n:
                other_nodes = working_nodes[top_n:]
                omitted_count = sum(
                    RuntimeReportService._count_report_tree_rows(node)
                    for node in other_nodes
                )
                removed = max(omitted_count - 1, 0)
                working_nodes = working_nodes[:top_n]

            trimmed_count = len(working_nodes)
            flattened: List[Dict[str, Any]] = []
            for node in working_nodes:
                row = node["row"]
                row_out = {
                    k: v
                    for k, v in row.items()
                    if k not in {"__reportSortBy", "__reportSortDir", "__reportSortAbs"}
                }
                if top_n and total_count > trimmed_count:
                    row_out["_groupTotalCount"] = total_count
                child_rows, child_removed = _flatten_report_nodes(node["children"])
                removed += child_removed
                flattened.append(row_out)
                flattened.extend(child_rows)
                flattened.extend(node["after_rows"])

            if other_nodes:
                first_other_row = (
                    other_nodes[0].get("row")
                    if isinstance(other_nodes[0], dict)
                    else {}
                )
                first_other_row = (
                    first_other_row if isinstance(first_other_row, dict) else {}
                )
                node_field = first_other_row.get("_levelField") or ""
                row_path_parts = RuntimeReportService._report_row_path_parts(
                    first_other_row
                )
                parent_path_values = row_path_parts[:-1] if row_path_parts else []
                path_fields = first_other_row.get("_pathFields")
                parent_path_fields = (
                    list(path_fields[:-1])
                    if isinstance(path_fields, list) and path_fields
                    else []
                )
                aggregate_row = RuntimeReportService._build_report_other_row_from_rows(
                    [node.get("row") for node in other_nodes if isinstance(node, dict)],
                    node_field,
                )
                flattened.append(
                    RuntimeReportService._decorate_report_other_row(
                        aggregate_row,
                        node_field=node_field,
                        path_values=parent_path_values,
                        path_fields=parent_path_fields,
                        depth=RuntimeReportService._report_row_depth(first_other_row),
                        level_label=first_other_row.get("_levelLabel") or node_field,
                        source_count=len(other_nodes),
                    )
                )

            return flattened, removed

        filtered_rows, removed_count = _flatten_report_nodes(roots)
        filtered = root_prefix_rows + filtered_rows + root_suffix_rows

        adjusted_total = (
            (total_rows - removed_count)
            if total_rows is not None and removed_count > 0
            else total_rows
        )
        return filtered, adjusted_total
