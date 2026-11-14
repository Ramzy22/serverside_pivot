"""Mapping from TanStack transport requests to immutable pivot specifications."""

from __future__ import annotations

import re
from typing import Any, Dict, List

from ..formula_mixin import FormulaEngineMixin
from ..security import User, apply_rls_to_spec
from ..types.pivot_spec import Measure, MeasureAxisConfig, PivotSpec
from .contracts import TanStackRequest


_FORMULA_IDENTIFIER_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")


def _normalize_formula_reference_key(value: Any, fallback: Any = "formula") -> str:
    base = re.sub(r"\s+", "", str(value or "").strip().lower())
    base = re.sub(r"[^a-z0-9_]", "", base)
    fallback_base = (
        re.sub(r"[^a-z0-9_]", "", str(fallback or "formula").strip().lower())
        or "formula"
    )
    normalized = base or fallback_base
    return normalized if re.match(r"^[a-z_]", normalized) else f"f_{normalized}"


class TanStackRequestMapper(FormulaEngineMixin):
    """Own transport-to-domain request conversion without execution state."""

    @staticmethod
    def _derive_measure_axis_members(
        measure_axis: Optional[MeasureAxisConfig],
        measures: List[Measure],
        request_columns: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not measure_axis or measure_axis.placement == "none":
            return []

        configured_members = [
            dict(member)
            for member in (measure_axis.members or [])
            if isinstance(member, dict)
        ]
        if configured_members:
            if not measures:
                return configured_members
            measures_by_alias = {
                str(getattr(measure, "alias", "")): measure
                for measure in (measures or [])
                if getattr(measure, "alias", None)
            }

            def resolve_configured_member(member: Dict[str, Any]) -> Optional[Measure]:
                candidates = [
                    member.get("measureAlias"),
                    member.get("measure_alias"),
                    member.get("alias"),
                    member.get("id"),
                ]
                for candidate in candidates:
                    key = str(candidate or "").strip()
                    if key and key in measures_by_alias:
                        return measures_by_alias[key]

                source_field = str(
                    member.get("sourceField")
                    or member.get("source_field")
                    or member.get("field")
                    or member.get("aggregationField")
                    or member.get("aggregation_field")
                    or ""
                ).strip()
                agg = (
                    str(member.get("agg") or member.get("aggregation") or "sum")
                    .strip()
                    .lower()
                )
                for measure in measures or []:
                    measure_field = str(getattr(measure, "field", None) or "").strip()
                    measure_agg = (
                        str(getattr(measure, "agg", None) or "sum").strip().lower()
                    )
                    if (
                        source_field
                        and measure_field == source_field
                        and measure_agg == agg
                    ):
                        return measure
                for measure in measures or []:
                    measure_field = str(getattr(measure, "field", None) or "").strip()
                    if source_field and measure_field == source_field:
                        return measure
                return None

            valid_configured_members: List[Dict[str, Any]] = []
            seen_aliases: set = set()
            for member in configured_members:
                measure = resolve_configured_member(member)
                if measure is None:
                    continue
                measure_alias = str(getattr(measure, "alias", "") or "").strip()
                if not measure_alias or measure_alias in seen_aliases:
                    continue
                seen_aliases.add(measure_alias)
                valid_configured_members.append(
                    {
                        **member,
                        "id": measure_alias,
                        "sourceField": getattr(measure, "field", None),
                        "measureAlias": measure_alias,
                        "agg": getattr(measure, "agg", None)
                        or member.get("agg")
                        or "sum",
                        "order": len(valid_configured_members),
                    }
                )
            return valid_configured_members

        columns_by_id = {
            str(column.get("id")): column
            for column in (request_columns or [])
            if isinstance(column, dict) and column.get("id") is not None
        }
        derived_members: List[Dict[str, Any]] = []
        for index, measure in enumerate(measures or []):
            column = columns_by_id.get(str(measure.alias), {})
            label = (
                column.get("header")
                or column.get("label")
                or column.get("aggregationLabel")
                or getattr(measure, "field", None)
                or getattr(measure, "alias", None)
            )
            derived_members.append(
                {
                    "id": getattr(measure, "alias", None)
                    or getattr(measure, "field", None)
                    or f"measure_{index + 1}",
                    "sourceField": getattr(measure, "field", None),
                    "measureAlias": getattr(measure, "alias", None),
                    "agg": getattr(measure, "agg", None) or "sum",
                    "label": str(
                        label
                        or getattr(measure, "alias", None)
                        or f"Measure {index + 1}"
                    ),
                    "order": index,
                    "format": column.get("format"),
                    "editable": bool(column.get("editable", False)),
                }
            )
        return derived_members

    @staticmethod
    def _formula_alias_lookup_from_request_columns(
        request_columns: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        lookup: Dict[str, str] = {}

        def add_alias(alias: Any, target: str) -> None:
            key = str(alias or "").strip()
            if not key:
                return
            lookup.setdefault(key, target)
            lookup.setdefault(key.lower(), target)

        for column in request_columns or []:
            if not isinstance(column, dict) or not column.get("id"):
                continue
            column_id = str(column.get("id"))
            add_alias(column_id, column_id)
            if column.get("aggregationFn"):
                add_alias(column.get("aggregationField"), column_id)
                add_alias(column.get("header"), column_id)
                add_alias(column.get("label"), column_id)
                add_alias(column.get("aggregationLabel"), column_id)
            elif column.get("isFormula"):
                add_alias(column.get("formulaRef"), column_id)
                add_alias(column.get("formulaLabel"), column_id)
                add_alias(column.get("header"), column_id)
        return lookup

    @staticmethod
    def _canonicalize_measure_formula_expression(
        expression: Any, alias_lookup: Dict[str, str]
    ) -> str:
        text = str(expression or "")
        if not text:
            return ""

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
            "abs",
            "round",
            "ceil",
            "floor",
            "max",
            "min",
            "int",
            "float",
        }

        def resolve_alias(value: str) -> str:
            return alias_lookup.get(value, alias_lookup.get(value.lower(), value))

        def replace_bracket(match: re.Match) -> str:
            resolved = resolve_alias(match.group(1))
            return (
                f"[{resolved}]"
                if not _FORMULA_IDENTIFIER_RE.fullmatch(resolved)
                else resolved
            )

        bracketed = re.sub(r"\[([^\]]+)\]", replace_bracket, text)

        protected: Dict[str, str] = {}

        def protect_bracket(match: re.Match) -> str:
            token = f"__formula_bracket_{len(protected)}__"
            protected[token] = match.group(0)
            return token

        protected_text = re.sub(r"\[[^\]]+\]", protect_bracket, bracketed)

        def replace_identifier(match: re.Match) -> str:
            token = match.group(0)
            if token in protected or token in reserved:
                return token
            return resolve_alias(token)

        normalized = _FORMULA_IDENTIFIER_RE.sub(replace_identifier, protected_text)
        for token, value in protected.items():
            normalized = normalized.replace(token, value)
        return normalized

    def convert_tanstack_request_to_pivot_spec(
        self, request: TanStackRequest
    ) -> PivotSpec:
        """Convert TanStack request to PivotSpec format"""
        # Extract grouping columns as hierarchy
        hierarchy_cols = request.grouping or []

        # Extract measure columns
        measures = []
        value_cols = []
        measure_axis = MeasureAxisConfig.from_dict(request.measure_axis)
        measure_axis_active = bool(measure_axis and measure_axis.placement != "none")
        measure_axis_member_aliases = {
            str(
                member.get("measureAlias")
                or member.get("measure_alias")
                or member.get("alias")
                or member.get("id")
                or ""
            ).strip()
            for member in ((measure_axis.members if measure_axis else []) or [])
            if isinstance(member, dict)
        }
        measure_axis_member_aliases.discard("")
        formula_alias_lookup = self._formula_alias_lookup_from_request_columns(
            request.columns or []
        )

        for col in request.columns:
            if col.get("isFormula"):
                if (
                    measure_axis_active
                    and not self._is_column_formula_column(col)
                    and col.get("id")
                    and (
                        not measure_axis_member_aliases
                        or str(col.get("id")) in measure_axis_member_aliases
                    )
                ):
                    measures.append(
                        Measure(
                            field=None,
                            agg="formula",
                            alias=str(col.get("id")),
                            expression=self._canonicalize_measure_formula_expression(
                                col.get("formulaExpr", ""),
                                formula_alias_lookup,
                            ),
                        )
                    )
                continue
            if col.get("aggregationFn"):
                # This is an aggregation column
                window_fn = col.get("windowFn")
                planner_window_fn = (
                    None
                    if self._normalize_window_fn(window_fn) is not None
                    else window_fn
                )
                measures.append(
                    Measure(
                        field=col.get("aggregationField", col["id"]),
                        agg=col.get("aggregationFn", "sum"),
                        alias=col["id"],
                        weighted_field=col.get("weightField"),
                        window_func=planner_window_fn,
                    )
                )
            elif col["id"] not in hierarchy_cols and col["id"] not in (
                "_id",
                "depth",
                "hierarchy",
                "subRows",
            ):
                # This is a value column
                value_cols.append(col["id"])

        if measure_axis and measure_axis.placement != "none":
            measure_axis.members = self._derive_measure_axis_members(
                measure_axis, measures, request.columns or []
            )
            label_field = measure_axis.label_field
            if measure_axis.placement == "rows":
                if label_field not in hierarchy_cols:
                    hierarchy_cols = list(hierarchy_cols) + [label_field]
                value_cols = [field for field in value_cols if field != label_field]
            elif measure_axis.placement == "columns":
                if label_field not in value_cols:
                    value_cols = list(value_cols) + [label_field]
                hierarchy_cols = [
                    field for field in hierarchy_cols if field != label_field
                ]

            sort_key_field = f"__sortkey__{label_field}"
            has_measure_axis_sort = any(
                isinstance(item, dict)
                and (
                    item.get("field") == label_field
                    or item.get("sortKeyField") == sort_key_field
                )
                for item in (request.sorting or [])
            )
        else:
            has_measure_axis_sort = False

        pivot_filters = []
        if request.filters:
            for field_name, filter_obj in request.filters.items():
                if not field_name or field_name in ("__row_number__", "hierarchy"):
                    continue
                if filter_obj is None:
                    continue

                # Global search: convert to OR-contains across all row dimension fields
                if field_name == "global":
                    search_val = (
                        filter_obj
                        if isinstance(filter_obj, str)
                        else (
                            filter_obj.get("value")
                            if isinstance(filter_obj, dict)
                            else None
                        )
                    )
                    if search_val and str(search_val).strip() and hierarchy_cols:
                        pivot_filters.append(
                            {
                                "op": "OR",
                                "conditions": [
                                    {
                                        "field": f,
                                        "op": "contains",
                                        "value": str(search_val),
                                        "caseSensitive": False,
                                    }
                                    for f in hierarchy_cols
                                ],
                            }
                        )
                    continue

                if isinstance(filter_obj, dict):
                    if "conditions" in filter_obj and "operator" in filter_obj:
                        # This is a multi-condition filter block
                        conditions = []
                        for cond in filter_obj.get("conditions", []):
                            conditions.append(
                                {
                                    "field": field_name,
                                    "op": self._map_tanstack_operator(
                                        cond.get("type", "eq")
                                    ),
                                    "value": cond.get("value"),
                                    "caseSensitive": cond.get("caseSensitive", False),
                                }
                            )

                        if conditions:
                            pivot_filters.append(
                                {"op": filter_obj["operator"], "conditions": conditions}
                            )
                    else:
                        # This is a single-condition filter dict
                        pivot_filters.append(
                            {
                                "field": field_name,
                                "op": self._map_tanstack_operator(
                                    filter_obj.get("type", "eq")
                                ),
                                "value": filter_obj.get("value"),
                                "caseSensitive": filter_obj.get("caseSensitive", False),
                            }
                        )
                elif isinstance(filter_obj, list):
                    # TanStack sometimes sends an array of condition objects directly
                    conditions = [
                        {
                            "field": field_name,
                            "op": self._map_tanstack_operator(item.get("type", "eq")),
                            "value": item.get("value"),
                            "caseSensitive": item.get("caseSensitive", False),
                        }
                        for item in filter_obj
                        if isinstance(item, dict)
                        and item.get("value") not in (None, "")
                    ]
                    if len(conditions) == 1:
                        pivot_filters.append(conditions[0])
                    elif len(conditions) > 1:
                        pivot_filters.append({"op": "AND", "conditions": conditions})
                elif isinstance(filter_obj, str) and filter_obj.strip() != "":
                    # Support simple string filters (e.g. from quick input)
                    pivot_filters.append(
                        {
                            "field": field_name,
                            "op": "contains",
                            "value": filter_obj,
                            "caseSensitive": False,
                        }
                    )

        # Convert TanStack sorting to PivotSpec sorting.
        # The hierarchy column has id="hierarchy" in the frontend but the backend
        # needs the actual first row dimension field name.
        pivot_sort = []
        for sort_spec in request.sorting:
            col_id = sort_spec["id"]
            if not col_id or col_id == "__row_number__":
                continue
            if col_id == "hierarchy" and request.grouping:
                col_id = request.grouping[0]

            sort_type = str(sort_spec.get("sortType") or "").strip().lower()
            sort_key_field = sort_spec.get("sortKeyField")
            semantic_type = sort_spec.get("semanticType") or sort_spec.get(
                "sortSemantic"
            )
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
                "field": col_id,
                "order": "desc" if sort_spec.get("desc") is True else "asc",
            }
            if semantic_type:
                sort_item["semanticType"] = semantic_type
            if sort_type:
                sort_item["sortType"] = sort_type
            if sort_spec.get("absoluteSort") is True:
                sort_item["absoluteSort"] = True
                if not sort_item.get("sortType"):
                    sort_item["sortType"] = "absolute"
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

        # Inject default sort specs for custom dimensions with explicit order settings.
        # Only injected when: user hasn't already sorted by that field AND the
        # dimension is actually present in the current grouping (avoids carrying
        # unnecessary hidden sort-key columns through unrelated queries).
        explicit_sort_fields = {s.get("field") for s in pivot_sort}
        active_dims = set(request.grouping or [])
        for dim in request.custom_dimensions or []:
            if not isinstance(dim, dict):
                continue
            dim_field = dim.get("field") or ""
            if (
                not dim_field
                or dim_field in explicit_sort_fields
                or dim_field not in active_dims
            ):
                continue
            dim_order = str(dim.get("order") or "creation").strip().lower()
            if dim_order == "alpha":
                continue
            sort_key_col = f"__sortkey__{dim_field}"
            if dim_order == "alpha_desc":
                pivot_sort.append({"field": dim_field, "order": "desc"})
            elif dim_order in ("numeric", "creation"):
                pivot_sort.append(
                    {"field": dim_field, "order": "asc", "sortKeyField": sort_key_col}
                )
            elif dim_order == "numeric_desc":
                pivot_sort.append(
                    {"field": dim_field, "order": "desc", "sortKeyField": sort_key_col}
                )

        if (
            measure_axis
            and measure_axis.placement != "none"
            and not has_measure_axis_sort
        ):
            label_field = measure_axis.label_field
            if label_field in set(hierarchy_cols or []) or label_field in set(
                value_cols or []
            ):
                pivot_sort.append(
                    {
                        "field": label_field,
                        "order": "asc",
                        "sortKeyField": f"__sortkey__{label_field}",
                    }
                )

        # Handle pagination
        offset = 0
        limit = 1000  # Default

        if request.pagination:
            page_size = request.pagination.get("pageSize", 100)
            page = request.pagination.get("pageIndex", 0)
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
            custom_dimensions=request.custom_dimensions or [],
            measure_axis=measure_axis,
            sort=pivot_sort,
            limit=limit,
            totals=(
                request.totals if request.totals is not None else True
            ),  # Enable totals computation
            pivot_config=PivotConfig(
                enabled=True,
                column_cursor=column_cursor,
                include_totals_column=(
                    request.row_totals if request.row_totals is not None else False
                ),
            ),
            column_sort_options=request.column_sort_options,
            group_allow_unbalanced=bool(getattr(request, "group_allow_unbalanced", False)),
            unbalanced_values=getattr(request, "unbalanced_values", None),
        )
        return spec

    def _map_tanstack_operator(self, tanstack_op: str) -> str:
        """Map TanStack filter operators to pivot engine operators"""
        mapping = {
            "eq": "=",
            "ne": "!=",
            "lt": "<",
            "gt": ">",
            "lte": "<=",
            "gte": ">=",
            "contains": "contains",
            "startsWith": "starts_with",
            "endsWith": "ends_with",
            "in": "in",
            "notIn": "not in",
        }
        return mapping.get(tanstack_op, "=")
