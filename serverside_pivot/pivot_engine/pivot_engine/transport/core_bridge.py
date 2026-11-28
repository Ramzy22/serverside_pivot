"""Edge between the legacy request contract and the semantic core.

* :func:`spec_to_query` maps a legacy ``PivotSpec`` + expansion to a core
  :class:`~pivot_engine.core.ir.PivotQuery`, or raises :class:`CoreUnsupported`
  listing every feature the core does not express yet (never a silent
  approximation).
* :func:`core_hierarchy_view` answers the ``controller.run_hierarchy_view``
  contract from the core, so every adapter-based edge (Dash, HTTP gateway,
  Streamlit) is served through one execution boundary.

Row identity: ``_path`` / ``_id`` / ``_pathFields`` / ``depth`` keep the legacy
display contract; ``_key`` carries the canonical typed identity
(REQ-ID-001), which the frontend adopts in M3.
"""

from __future__ import annotations

import ast
import math
import re
import time
from dataclasses import is_dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from ..core.authority.verifier import verify_scoped
from ..core.exec import (
    column_members, detail, distinct_values, execute, materialize, measure_expr, record_predicate,
    records,
)
from ..core.identity import GRAND_TOTAL_DISPLAY_PATH, CanonicalPathCodec
from ..core.ir import (
    FORMULA_AGG, MEMBER_AGG, Category, ColumnOrder, Expr, CategoryRule, DerivedMeasure, DetailQuery, DistinctQuery, MeasureFilter, to_wire, MeasureRef, PivotQuery, Predicate, PredicateGroup, Ragged, SortKey, Unpivot, UnpivotMember, Window,
)
from ..core.registry import type_family

__all__ = ["CoreUnsupported", "core_drill_through", "core_hierarchy_view", "core_unique_values",
           "legacy_column_key", "spec_to_query"]

GRAND_TOTAL_ID = "Grand Total"
ALL_EXPANDED = "__ALL__"

_AGG = {
    "sum": "sum", "avg": "avg", "mean": "avg", "min": "min", "max": "max", "count": "count",
    "count_distinct": "count_distinct", "distinct_count": "count_distinct", "median": "median",
    "stddev": "stddev", "variance": "variance", "percentile": "percentile",
    "weighted_avg": "weighted_avg", "wavg": "weighted_avg", "weighted_mean": "weighted_avg",
    "first": "first", "last": "last",  # REQ-AGG-008: need an order field (the IR rejects one without)
    "array_agg": "array_agg",  # REQ-AGG-009: a series ordered by its order field, else by value
}
# Sort types that order by the plain (signed) value; "value" is the UI's name for a measure's signed sort.
_PLAIN_SORT_TYPES = frozenset({"alphanumeric", "basic", "default", "natural", "value"})
_OPS = {
    "=": "=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">=", "in": "in", "not in": "not_in",
    "not_in": "not_in", "between": "between", "contains": "contains", "not_contains": "not_contains",
    "starts_with": "starts_with", "ends_with": "ends_with", "is_null": "is_null", "is_not_null": "is_not_null",
    "is_empty": "is_empty", "is_not_empty": "is_not_empty",
    # The edge vocabulary (TanStack / spec API spellings) of the same operators.
    "==": "=", "eq": "=", "equals": "=", "ne": "!=", "neq": "!=", "not_equals": "!=", "<>": "!=",
    "lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "startswith": "starts_with", "endswith": "ends_with",
    "notcontains": "not_contains", "notin": "not_in",
}
_MEASURE_FILTER_OPS = {"=", "!=", "<", "<=", ">", ">="}


def _op(f: Mapping[str, Any]) -> Optional[str]:
    """The core operator of a spec filter (case-insensitive), None if unknown."""
    raw = str(f.get("op") or f.get("operator") or "=").strip()
    return _OPS.get(raw) or _OPS.get(raw.lower())


class CoreUnsupported(Exception):
    """The request uses features the semantic core does not express: a typed client error
    (``unsupported_query``), never a fallback to another engine."""

    code = "unsupported_query"

    def __init__(self, reasons: Sequence[str]) -> None:
        self.reasons: Tuple[str, ...] = tuple(sorted(set(reasons)))
        super().__init__("core does not support: " + ", ".join(self.reasons))


# --------------------------------------------------------------------- values


def _coerce(value: Any, family: str) -> Any:
    """Legacy UI values arrive as strings; coerce them exactly to the column family."""
    if value is None or not isinstance(value, str):
        return value
    text = value.strip()
    try:
        if family == "integer":
            return int(text)
        if family == "floating":
            return float(text)
        if family == "decimal":
            return Decimal(text)
    except (ValueError, InvalidOperation):
        raise CoreUnsupported([f"value {value!r} is not a {family}"]) from None
    if family == "boolean" and text.lower() in {"true", "false"}:
        return text.lower() == "true"
    return value


# --------------------------------------------------------------------- filters


def _flatten_fields(f: Mapping[str, Any]) -> List[str]:
    if "conditions" in f:
        return [x for c in f.get("conditions") or [] for x in _flatten_fields(c)]
    return [str(f.get("field"))]


def _dimension_filter(f: Mapping[str, Any], families: Mapping[str, str], reasons: List[str]) -> Any:
    if not isinstance(f, Mapping):  # e.g. a SQL-text condition: never parsed, a typed refusal
        reasons.append(f"filter of type {type(f).__name__}")
        return None
    if "conditions" in f:
        op = str(f.get("op") or f.get("operator") or "AND").lower()
        if op not in {"and", "or"}:
            reasons.append(f"filter group operator {op!r}")
            return None
        items = [_dimension_filter(c, families, reasons) for c in f.get("conditions") or []]
        items = [i for i in items if i is not None]
        return PredicateGroup(op, tuple(items)) if items else None
    field = f.get("field")
    if field not in families:
        reasons.append(f"filter on unknown field {field!r}")
        return None
    op = _op(f)
    if op is None:
        reasons.append(f"filter operator {f.get('op') or f.get('operator')!r}")
        return None
    family = families[field]
    value = f.get("value")
    if op in {"in", "not_in"}:
        value = tuple(_coerce(v, family) for v in (value if isinstance(value, (list, tuple, set)) else [value]))
    elif op == "between":
        if not (isinstance(value, (list, tuple)) and len(value) == 2):
            reasons.append("between without [low, high]")
            return None
        value = (_coerce(value[0], family), _coerce(value[1], family))
    elif op not in {"is_null", "is_not_null", "is_empty", "is_not_empty", "contains", "not_contains", "starts_with",
                    "ends_with"}:
        value = _coerce(value, family)
    return Predicate(str(field), op, value, bool(f.get("caseSensitive", False)))


def _filters(spec: Any, aliases: FrozenSet[str], families: Mapping[str, str], depth: int,
             reasons: List[str]) -> Tuple[Tuple[Any, ...], Tuple[MeasureFilter, ...]]:
    rows: List[Any] = []
    measure_filters: List[MeasureFilter] = []
    having = [h for h in getattr(spec, "having", None) or [] if isinstance(h, Mapping)]
    for h in having:  # REQ-TOT-007: a having entry is a level-less measure filter, never a record filter
        if not set(_flatten_fields(h)) <= aliases:
            reasons.append("having on a non-measure field")
    for f in [*(spec.filters or []), *having]:
        fields = set(_flatten_fields(f))
        if fields & aliases:
            # REQ-TOT-007: a level-less measure filter applies at the deepest row level.  A conjunction
            # (a column filter menu's AND group) is one filter per condition: a dimension condition is a
            # row filter (E0), a measure condition a measure filter, so the group means WHERE ... HAVING ...
            terms = [f]
            if "conditions" in f:
                group_op = str(f.get("op") or f.get("operator") or "AND").upper()
                terms = list(f.get("conditions") or [])
                if (group_op != "AND" and len(terms) > 1) or any(
                        not isinstance(t, dict) or "conditions" in t for t in terms):
                    reasons.append("compound measure filter")
                    continue
            elif not fields <= aliases:
                reasons.append("compound measure filter")
                continue
            dimension_terms = [t for t in terms if str(t.get("field")) not in aliases]
            terms = [t for t in terms if str(t.get("field")) in aliases]
            for term in dimension_terms:
                mapped = _dimension_filter(term, families, reasons)
                if mapped is not None:
                    rows.append(mapped)
            for term in terms:
                op = _op(term) or str(term.get("op"))
                if op not in _MEASURE_FILTER_OPS or depth == 0:
                    reasons.append(f"measure filter {term.get('op')!r}" if depth else "measure filter without row levels")
                    continue
                measure_filters.append(MeasureFilter(depth, str(term["field"]), op,
                                                     _coerce(term.get("value"), "floating")))
            continue
        mapped = _dimension_filter(f, families, reasons)
        if mapped is not None:
            rows.append(mapped)
    return tuple(rows), tuple(measure_filters)


# --------------------------------------------------------------------- custom dimensions (REQ-DIM-001)

CATEGORY_PREFIX = "__custom_category__"
_CATEGORY_OPS = {
    "=": "=", "eq": "=", "==": "=", "!=": "!=", "<>": "!=", "ne": "!=", "not_eq": "!=", "not eq": "!=",
    "<": "<", "lt": "<", "<=": "<=", "lte": "<=", ">": ">", "gt": ">", ">=": ">=", "gte": ">=",
    "in": "in", "not in": "not_in", "not_in": "not_in", "notin": "not_in", "notIn": "not_in",
    "contains": "contains", "not_contains": "not_contains", "between": "between",
    "starts_with": "starts_with", "startsWith": "starts_with", "startswith": "starts_with",
    "ends_with": "ends_with", "endsWith": "ends_with", "endswith": "ends_with",
    # The category UI's "empty" operators (REQ-NULL-004): NULL or a text of spaces only.
    "is null": "is_empty", "is_null": "is_empty", "isNull": "is_empty", "isnull": "is_empty",
    "is not null": "is_not_empty", "is_not_null": "is_not_empty", "isNotNull": "is_not_empty",
    "isnotnull": "is_not_empty",
}


def category_field_id(dimension: Mapping[str, Any]) -> str:
    """The legacy column name of a custom dimension (``__custom_category__<slug>``)."""
    raw_field = dimension.get("field")
    if isinstance(raw_field, str) and raw_field.startswith(CATEGORY_PREFIX):
        return raw_field
    raw_id = str(dimension.get("id") or dimension.get("name") or dimension.get("label") or "category")
    slug = re.sub(r"[^a-z0-9_]+", "_", raw_id.strip().lower()).strip("_") or "category"
    if not re.match(r"^[a-z_]", slug):
        slug = f"c_{slug}"
    return f"{CATEGORY_PREFIX}{slug}"


def _category_clause(clause: Any, fallback_field: str) -> Optional[Dict[str, Any]]:
    if not isinstance(clause, Mapping):
        return None
    field_name = clause.get("field")
    if not isinstance(field_name, str) or not field_name.strip():
        field_name = fallback_field
    if not isinstance(field_name, str) or not field_name.strip():
        return None
    raw_op = str(clause.get("operator", clause.get("op", clause.get("type", "eq"))) or "eq").strip()
    op = _CATEGORY_OPS.get(raw_op) or _CATEGORY_OPS.get(raw_op.replace(" ", "_").lower(), raw_op)
    if op in {"in", "not_in"}:
        raw = clause.get("values") if clause.get("values") is not None else clause.get("value")
        if isinstance(raw, str):
            value: Any = [item.strip() for item in raw.split(",") if item.strip()]
        elif isinstance(raw, (list, tuple, set)):
            value = list(raw)
        else:
            value = [] if raw is None else [raw]
    else:
        value = clause.get("value")
    return {"field": field_name.strip(), "op": op, "value": value, "caseSensitive": bool(clause.get("caseSensitive", False))}


def _category_condition(condition: Any, fallback_field: str) -> Optional[Dict[str, Any]]:
    if not isinstance(condition, Mapping):
        return None
    op = "or" if str(condition.get("op") or condition.get("operator") or "AND").upper() == "OR" else "and"
    source = condition.get("clauses")
    if source is None:
        source = condition.get("conditions")
    if isinstance(source, list):
        clauses = [c for c in (_category_clause(item, fallback_field) for item in source) if c is not None]
    else:
        single = _category_clause(condition, fallback_field)
        clauses = [single] if single else []
    return {"op": op, "conditions": clauses} if clauses else None


def _categories(spec: Any, families: Dict[str, str], reasons: List[str]) -> Tuple[Category, ...]:
    """Legacy custom dimensions as IR categories; ``families`` gains each category column.

    Same labels, fallback and hidden order keys as legacy
    (``__sortkey__<field>``: rule index for "creation", the numeric label for
    "numeric"), with conditions mapped to exact core filters.
    """
    out: List[Category] = []
    for dimension in getattr(spec, "custom_dimensions", None) or []:
        if not isinstance(dimension, Mapping):
            continue
        name = category_field_id(dimension)
        fallback = str(dimension.get("fallbackLabel") or dimension.get("defaultLabel") or dimension.get("fallback")
                       or "Other")
        rules = dimension.get("rules") if isinstance(dimension.get("rules"), list) else []
        labelled: List[CategoryRule] = []
        creation: List[CategoryRule] = []
        numeric: List[CategoryRule] = []
        for i, rule in enumerate(rules):
            if not isinstance(rule, Mapping):
                continue
            raw = _category_condition(rule.get("condition") or rule, str(rule.get("field") or ""))
            if raw is None:
                continue
            when = _dimension_filter(raw, families, reasons)
            if when is None:
                continue
            label = str(rule.get("label") or "").strip()
            if label:
                labelled.append(CategoryRule(when, label))
            creation.append(CategoryRule(when, i))
            try:
                numeric.append(CategoryRule(when, float(label)))
            except (TypeError, ValueError):
                pass
        if name in families:
            reasons.append(f"custom dimension {name!r} collides with a column")
            continue
        out.append(Category(name, tuple(labelled), fallback))
        families[name] = "string"
        order = str(dimension.get("order") or "creation").strip().lower()
        key = f"__sortkey__{name}"
        if order == "creation":
            out.append(Category(key, tuple(creation), len(rules)))
            families[key] = "integer"
        elif order in {"numeric", "numeric_desc"}:
            out.append(Category(key, tuple(numeric), 1e18))
            families[key] = "floating"
    return tuple(out)


# --------------------------------------------------------------------- measure axis (REQ-DIM-002)

_AXIS_AGGS = frozenset({"sum", "avg", "count", "min", "max"})  # legacy pre-aggregates only these


def _axis_member_measure(member: Mapping[str, Any], source: str, agg: str, measures: Sequence[Any]) -> Any:
    """The spec measure a legacy measure-axis member names (legacy resolution order)."""
    by_alias = {str(m.alias): m for m in measures if getattr(m, "alias", None)}
    for candidate in (member.get("measureAlias"), member.get("measure_alias"), member.get("alias"),
                      member.get("id"), source):
        key = str(candidate or "").strip()
        if key and key in by_alias:
            return by_alias[key]
    same_field = [m for m in measures if str(getattr(m, "field", None) or "").strip() == source and source]
    for m in same_field:
        if str(getattr(m, "agg", None) or "sum").strip().lower() == agg:
            return m
    return same_field[0] if same_field else None


def _measure_axis(spec: Any, families: Dict[str, str], reasons: List[str]) -> Optional[Dict[str, Any]]:
    """A legacy measure axis as an IR :class:`Unpivot`, or None when the spec has none.

    One common aggregation of sum/avg/count/min/max over plain measures is
    an unpivot aggregated once (REQ-DIM-002).  Otherwise every member keeps
    its own measure (REQ-DIM-003): exact at and below the label, NULL above
    it.  Zero suppression and the axis on columns are named reasons.
    ``families`` gains the label, value and key columns.
    """
    config = getattr(spec, "measure_axis", None)
    if config is None or getattr(config, "placement", "none") == "none":
        return None
    label = str(config.label_field or "__measure_name__")
    value = str(config.value_field or "__measure_value__")
    key = f"__sortkey__{label}"
    measures = list(spec.measures or [])
    raw_members = [dict(m) for m in config.members or [] if isinstance(m, Mapping)] or [
        {"id": m.alias or m.field, "sourceField": m.field, "measureAlias": m.alias, "agg": m.agg or "sum",
         "label": m.field or m.alias, "order": i}
        for i, m in enumerate(measures) if m and getattr(m, "alias", None)]
    members: List[Tuple[str, Optional[str], int, str, Any]] = []  # (label, source, order, agg, measure)
    seen: set = set()
    for i, member in enumerate(raw_members):
        source = str(member.get("sourceField") or member.get("source_field") or member.get("field") or "").strip()
        raw_agg = member.get("agg") or member.get("aggregation")
        agg = str(raw_agg or "sum").strip().lower()
        measure = _axis_member_measure(member, source, agg, measures)
        if measure is None and measures:
            continue
        if measure is not None and raw_agg is None:
            agg = str(getattr(measure, "agg", None) or "sum").strip().lower()
        alias = str(getattr(measure, "alias", None) or member.get("measureAlias") or member.get("id") or source or "").strip()
        if not alias or alias in seen:
            continue
        seen.add(alias)
        if not source and measure is not None:
            source = str(getattr(measure, "field", None) or alias)
        text = str(member.get("label") or member.get("name") or member.get("alias") or source or f"Measure {i + 1}")
        raw_order = member.get("order", member.get("sort", i))
        try:
            order = int(float(raw_order))
        except (TypeError, ValueError):
            order = i
        members.append((text, source, order, agg, measure))
    if not members:
        reasons.append("measure axis without members")
        return None
    aggs = {agg for _l, _s, _o, agg, _m in members}
    plain = all(m is not None and not (getattr(m, "expression", None) or getattr(m, "ratio_numerator", None)
                                       or getattr(m, "window_func", None))
                and agg == str(getattr(m, "agg", None) or "sum").strip().lower()
                and source == str(getattr(m, "field", None) or "").strip()
                for _l, source, _o, agg, m in members)
    uniform = len(aggs) == 1 and aggs <= _AXIS_AGGS and plain
    own: List[Optional[MeasureRef]] = [None] * len(members)
    hidden: List[MeasureRef] = []  # REQ-DIM-007: the plain measures formula members reference
    if not uniform:  # REQ-DIM-003: each member keeps its own measure (legacy aggregates first, then unpivots)
        for i, (text, source, _o, agg, m) in enumerate(members):
            if m is not None and (getattr(m, "ratio_numerator", None) or getattr(m, "window_func", None)):
                reasons.append("measure axis member is a derived measure")
                continue
            if m is not None and str(getattr(m, "agg", "") or "").strip().lower() == "formula":
                expr = _axis_formula(m, measures, hidden, reasons)
                if expr is not None:
                    own[i] = MeasureRef(text, FORMULA_AGG, None, expr=expr)
                    members[i] = (text, None, members[i][2], "formula", m)
                continue
            ref = _measure(m, reasons) if m is not None else (
                MeasureRef(text, _AGG[agg], source) if agg in _AGG else None)
            if ref is None:
                if m is None:
                    reasons.append(f"aggregation {agg!r}")
                continue
            if ref.field is None:
                reasons.append("measure axis member counts records (no field to unpivot)")
                continue
            own[i] = ref
            members[i] = (text, ref.field, members[i][2], agg, m)
    on_columns = config.placement == "columns"  # REQ-DIM-006: the label as a column dimension
    if config.placement not in {"rows", "columns"}:
        reasons.append(f"measure axis on {config.placement}")
    if any(n in families for n in (label, value, key)):
        reasons.append("measure axis columns collide with a column")
    missing = [source for _l, source, _o, _a, _m in members if source is not None and source not in families]
    if missing:
        reasons.append(f"measure axis unknown fields {missing}")
    if len({text for text, *_rest in members}) != len(members):
        reasons.append("measure axis repeated member label")
    if reasons:
        return None
    unpivot = Unpivot(label, value, key, tuple(
        UnpivotMember(t, s, o) if ref is None else UnpivotMember(t, s, o, ref.agg, ref.weight, ref.q, ref.order_by,
                                                                  ref.expr)
        for (t, s, o, _a, _m), ref in zip(members, own, strict=True)), drop_nulls=bool(config.suppress_empty_members))
    families.update(unpivot.extend(families))
    rows = [str(r) for r in spec.rows or [] if not (on_columns and str(r) == label)]
    columns = [str(c) for c in getattr(spec, "columns", None) or [] if on_columns or str(c) != label]
    if on_columns and label not in columns:
        columns.append(label)  # the label is the innermost column dimension unless placed
    if not on_columns and label not in rows and not getattr(spec, "_measure_axis_exact_layout", False):
        rows.append(label)  # legacy appends a missing label level
    sort = list(spec.sort if isinstance(spec.sort, list) else ([spec.sort] if spec.sort else []))
    if label in rows and not any(isinstance(s, Mapping) and (s.get("field") == label or s.get("sortKeyField") == key)
                                 for s in sort):
        sort.append({"field": label, "order": "asc", "sortKeyField": key})  # members in their order
    agg = _AGG[next(iter(aggs))] if uniform else MEMBER_AGG
    measure_filters: List[MeasureFilter] = []
    if config.suppress_zero_members:
        # REQ-DIM-006: a member whose value is zero (or NULL) is suppressed: on rows a label node, on
        # columns a cell; records of a suppressed member leave E, so a node left without any is not shown.
        if on_columns and rows:
            measure_filters.append(MeasureFilter(len(rows), value, "!=", 0, cells=True))
        elif on_columns:
            reasons.append("measure axis zero suppression without row levels")
            return None
        else:
            measure_filters.append(MeasureFilter(rows.index(label) + 1, value, "!=", 0))
    return {"unpivot": unpivot, "rows": rows, "sort": sort, "columns": columns,
            "measures": [MeasureRef(value, agg, value), *hidden], "measure_filters": measure_filters}


def _axis_formula(m: Any, measures: Sequence[Any], hidden: List[MeasureRef], reasons: List[str]) -> Optional[Expr]:
    """A formula axis member's expression over hidden copies of the plain measures it names (REQ-DIM-007).

    A reference to another formula measure is that formula's expression, inlined; a cycle is NULL.
    """
    by_alias = {str(x.alias): x for x in measures if getattr(x, "alias", None)}
    lowered = {k.lower(): k for k in by_alias}
    inlining: List[str] = []

    def resolve(name: str) -> Optional[str]:
        alias = name if name in by_alias else lowered.get(name.lower())
        if alias is None:
            return None
        target = by_alias[alias]
        if str(getattr(target, "agg", "") or "").strip().lower() == "formula":
            return f"__formula__{alias}"
        ref = f"{HIDDEN}ax_{alias}"
        if ref not in {h.alias for h in hidden}:
            base = _measure(target, reasons, alias=ref)
            if base is None:
                raise _FormulaDecline("formula member over an unsupported measure")
            hidden.append(base)
        return ref

    def expand(expr: Expr) -> Expr:
        if expr.op == "ref" and str(expr.args[0]).startswith("__formula__"):
            alias = str(expr.args[0])[len("__formula__"):]
            if alias in inlining:
                return Expr("null")
            inlining.append(alias)
            try:
                return expand(_formula_expr(str(getattr(by_alias[alias], "expression", "") or ""), resolve))
            finally:
                inlining.pop()
        return Expr(expr.op, tuple(expand(a) if isinstance(a, Expr) else a for a in expr.args))

    try:
        inlining.append(str(m.alias))
        return expand(_formula_expr(str(getattr(m, "expression", "") or ""), resolve))
    except _FormulaDecline as exc:
        reasons.append(str(exc))
        return None


# --------------------------------------------------------------------- ragged hierarchy (REQ-DIM-005)


def _ragged(spec: Any, rows: Sequence[str], families: Dict[str, str], subtotals: str, axis: Any,
            reasons: List[str]) -> Optional[Ragged]:
    """``group_allow_unbalanced`` as an IR :class:`Ragged` over the row fields; ``families`` gains its columns."""
    if axis:
        reasons.append("ragged hierarchy with a measure axis")
    if subtotals == "none":
        reasons.append("ragged hierarchy in a flat layout")  # its leaves end at different depths
    unknown = [r for r in rows if r not in families]
    if not rows or unknown or len({families[r] for r in rows}) != 1:
        reasons.append("ragged hierarchy over fields of different types")
    if reasons:
        return None
    raw = getattr(spec, "unbalanced_values", None)
    family = families[rows[0]]
    missing = None if raw is None else tuple(_coerce(v, family) for v in raw)
    ragged = Ragged(tuple(rows), missing)
    families.update(ragged.extend(families))
    return ragged


# --------------------------------------------------------------------- spec -> IR


HIDDEN = "__base_"
_WINDOWS = {"rank": "rank", "dense_rank": "dense_rank", "cumulative": "cumulative", "running_avg": "moving_avg",
            "moving_avg": "moving_avg", "percent_of_total": "percent_of_total",
            "percent_of_grand_total": "percent_of_total"}


def _measures(spec: Any, reasons: List[str]) -> Tuple[List[MeasureRef], List[DerivedMeasure]]:
    """Base measures plus derived ones (ratios, window measures over a hidden base measure)."""
    base: List[MeasureRef] = []
    derived: List[DerivedMeasure] = []
    ratios = []
    for m in spec.measures or []:
        if getattr(m, "ratio_numerator", None):
            ratios.append(m)
            continue
        func = getattr(m, "window_func", None)
        if func:
            name = _WINDOWS.get(str(func).strip().lower())
            if name is None:
                reasons.append(f"window function {func!r}")
                continue
            if getattr(m, "window_group_by", None) is not None or getattr(m, "window_order_by", None):
                reasons.append("window partition or order override")
                continue
            ref = _measure(m, reasons, alias=HIDDEN + str(m.alias))
            if ref is None:
                continue
            base.append(ref)
            frame = None
            if str(func).strip().lower() == "moving_avg" and (
                    m.window_frame_start is not None or m.window_frame_end is not None):
                frame = (m.window_frame_start, 0 if m.window_frame_end is None else m.window_frame_end)
            derived.append(DerivedMeasure(str(m.alias), name, ref.alias, frame=frame))
            continue
        ref = _measure(m, reasons)
        if ref is not None:
            base.append(ref)
    plain = {b.alias for b in base}
    for m in ratios:
        num, den = str(m.ratio_numerator), str(m.ratio_denominator or "")
        if num not in plain or den not in plain:
            reasons.append("ratio of a derived or unknown measure")
            continue
        derived.append(DerivedMeasure(str(m.alias), "ratio", num, den,
                                      null_value=None if m.ratio_null_value is None else float(m.ratio_null_value)))
    return base, derived


def _measure(m: Any, reasons: List[str], *, alias: Optional[str] = None) -> Optional[MeasureRef]:
    agg = _AGG.get(str(m.agg or "sum").strip().lower())
    if agg is None:
        reasons.append(f"aggregation {m.agg!r}")
        return None
    for attr, label in (("expression", "measure expression"), ("separator", "string aggregation"),
                        ("filter_condition", "conditional measure")):
        if getattr(m, attr, None):
            reasons.append(label)
            return None
    if getattr(m, "null_handling", None) not in (None, "ignore"):
        reasons.append(f"null handling {m.null_handling!r}")
        return None
    field = None if m.field in (None, "*") else str(m.field)
    weight = getattr(m, "weighted_field", None) if agg == "weighted_avg" else None
    q = getattr(m, "percentile", None) if agg == "percentile" else None
    order_by = getattr(m, "order_by", None) if agg in {"first", "last"} else None
    return MeasureRef(alias or str(m.alias), agg, field, weight, None if q is None else float(q),
                      None if order_by is None else str(order_by))


# --------------------------------------------------------------------- formulas (REQ-AGG-010)

_FORMULA_UNARY = {"abs": "abs", "ceil": "ceil", "floor": "floor", "int": "trunc"}
_FORMULA_BINARY = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/"}
_FORMULA_MAX_LENGTH = 2048


class _FormulaDecline(Exception):
    pass


def _formula_expr(text: str, resolve: Callable[[str], Optional[str]]) -> Expr:
    """A formula's text (the request mapper's canonical form) as a core expression.

    The grammar is the adapter's: numbers, measure references (``name`` or
    ``[name]``), ``+ - * / **``, unary minus, ``abs round ceil floor min max int
    float``.  An unknown reference or an expression that cannot be evaluated
    is NULL (the adapter's result for it); a computed exponent is declined.
    """
    null = Expr("null")
    brackets: Dict[str, str] = {}

    def bracket(match: Any) -> str:
        name = f"__formula_bracket_{len(brackets)}"
        brackets[name] = match.group(1).strip()
        return name

    source = re.sub(r"\[([^\]]+)\]", bracket, text)
    if not source.strip() or len(source) > _FORMULA_MAX_LENGTH:
        return null
    try:
        tree = ast.parse(source, mode="eval").body
    except (SyntaxError, ValueError):
        return null

    def number(node: Any) -> Optional[float]:
        if isinstance(node, ast.Constant) and not isinstance(node.value, bool) and isinstance(node.value, (int, float)):
            return float(node.value) if math.isfinite(node.value) else None
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            inner = number(node.operand)
            return None if inner is None else (-inner if isinstance(node.op, ast.USub) else inner)
        return None

    def convert(node: Any, depth: int) -> Expr:
        if depth > 30:
            return null
        constant = number(node)
        if constant is not None:
            return Expr("num", (int(constant) if constant.is_integer() and abs(constant) < 2 ** 53 else constant,))
        if isinstance(node, ast.Name):
            alias = resolve(brackets.get(node.id, node.id))
            return null if alias is None else Expr("ref", (alias,))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return Expr("neg", (convert(node.operand, depth + 1),))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.UAdd):
            return convert(node.operand, depth + 1)
        if isinstance(node, ast.BinOp) and type(node.op) in _FORMULA_BINARY:
            return Expr(_FORMULA_BINARY[type(node.op)], (convert(node.left, depth + 1), convert(node.right, depth + 1)))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Pow):
            exponent = number(node.right)
            if exponent is None:
                raise _FormulaDecline("formula power with a computed exponent")
            if abs(exponent) > 64:
                return null
            return Expr("pow", (convert(node.left, depth + 1), int(exponent) if exponent.is_integer() else exponent))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and not node.keywords:
            name, args = node.func.id, node.args
            if name in _FORMULA_UNARY and len(args) == 1:
                return Expr(_FORMULA_UNARY[name], (convert(args[0], depth + 1),))
            if name == "float" and len(args) == 1:
                return convert(args[0], depth + 1)
            if name == "round" and len(args) in (1, 2):
                digits = 0.0 if len(args) == 1 else number(args[1])
                if digits is None or not digits.is_integer() or not 0 <= digits <= 12:
                    raise _FormulaDecline("formula round with computed or out-of-range digits")
                return Expr("round", (convert(args[0], depth + 1), int(digits)))
            if name in {"min", "max"} and 2 <= len(args) <= 8:
                return Expr(name, tuple(convert(a, depth + 1) for a in args))
        return null

    return convert(tree, 0)


def _formulas(spec: Any, measures: Sequence[MeasureRef], derived: Sequence[DerivedMeasure],
              reasons: List[str]) -> List[MeasureRef]:
    """The spec's measure formulas as core measures; a formula in a reference cycle is NULL (the adapter's)."""
    raw = [f for f in getattr(spec, "formulas", None) or [] if isinstance(f, Mapping) and f.get("id")]
    if not raw:
        return []
    plain = {m.alias for m in measures if not m.alias.startswith(HIDDEN) and m.agg != "array_agg"}
    series = {m.alias for m in measures if m.agg == "array_agg"}
    derived_aliases = {d.alias for d in derived}
    ids = [str(f["id"]) for f in raw]
    if len(set(ids)) != len(ids) or set(ids) & (plain | series | derived_aliases):
        reasons.append("formula id repeated or naming a measure")
        return []
    lookup: Dict[str, str] = {}
    for alias in [*plain, *ids]:
        lookup.setdefault(alias, alias)
        lookup.setdefault(alias.lower(), alias)

    def resolve(name: str) -> Optional[str]:
        if name in derived_aliases or name.lower() in {d.lower() for d in derived_aliases}:
            raise _FormulaDecline("formula over a derived measure")
        return lookup.get(name, lookup.get(name.lower()))

    exprs: Dict[str, Expr] = {}
    try:
        for f in raw:
            exprs[str(f["id"])] = _formula_expr(str(f.get("expr") or ""), resolve)
    except _FormulaDecline as exc:
        reasons.append(str(exc))
        return []
    resolved: set = set()
    progressed = True
    while progressed:  # a formula resolves once every formula it references has (the adapter's evaluation plan)
        progressed = False
        for fid in ids:
            deps = {r for r in exprs[fid].refs() if r in exprs}
            if fid not in resolved and fid not in deps and deps <= resolved:
                resolved.add(fid)
                progressed = True
    return [MeasureRef(fid, FORMULA_AGG, None, expr=exprs[fid] if fid in resolved else Expr("null")) for fid in ids]


def _expanded(target_paths: Any, rows: Sequence[str], families: Mapping[str, str]) -> Optional[FrozenSet[Tuple[Any, ...]]]:
    if target_paths is True or target_paths is None:
        return None
    paths = [list(p) for p in (target_paths or []) if isinstance(p, (list, tuple))]
    if any(p == [ALL_EXPANDED] for p in paths):
        return None
    out = set()
    for p in paths:
        if not 1 <= len(p) < len(rows):
            continue  # a leaf or over-long path expands nothing
        if any(v is None or v == "" for v in p):
            # A display path cannot tell the NULL member from "" (F-EXPAND-EMPTY): not addressable.
            raise CoreUnsupported(["expanded path with an empty or NULL member (send rowKey)"])
        try:
            out.add(tuple(_coerce(v, families[rows[i]]) for i, v in enumerate(p)))
        except CoreUnsupported:
            continue  # a stale path (the field's type changed) names no node: it expands nothing
    return frozenset(out)


# Every PivotSpec / Measure / PivotConfig field the bridge interprets, or that
# has no effect on the hierarchy view's values (display-only).  Any other field
# with a non-default value is rejected by name, so the mapping is complete by
# construction rather than silently partial.
_SPEC_FIELDS = {"table", "rows", "columns", "measures", "filters", "custom_dimensions", "sort", "offset", "cursor",
                "totals", "having", "subtotals", "grouping_config", "pivot_config", "measure_axis", "drill_paths",
                "group_allow_unbalanced", "unbalanced_values",  # REQ-DIM-005: a ragged hierarchy
                "full_rows",  # checked below: the rows extended with known fields (a no-op) are served
                "limit",  # the hierarchy view is windowed by start/end rows
                "column_sort_options",  # orders the column catalog only; cells are keyed by member
                "formulas"}  # REQ-AGG-010
_MEASURE_FIELDS = {"field", "agg", "alias", "weighted_field", "expression", "percentile", "separator",
                   "null_handling", "filter_condition", "ratio_numerator", "ratio_denominator", "ratio_null_value",
                   "window_func", "window_group_by", "window_order_by", "window_frame_start", "window_frame_end",
                   "ratio_format",  # display formatting only
                   "order_by"}
_PIVOT_CONFIG_FIELDS = {"enabled", "top_n", "order_by_measure", "include_totals_column", "column_cursor",
                        "materialized_column_values",
                        "null_column_label"}  # display only; cell keys use the legacy key
_SORT_KEYS = {"field", "order", "sortType", "absoluteSort", "nulls", "semanticType", "semantic", "sortSemantic",
              "sortKeyField"}
_FILTER_KEYS = {"field", "op", "value", "caseSensitive", "conditions"}


def _uninterpreted(obj: Any, known: FrozenSet[str], label: str, reasons: List[str]) -> None:
    import dataclasses

    if obj is None or not dataclasses.is_dataclass(obj):
        return
    for f in dataclasses.fields(obj):
        if f.name in known:
            continue
        if f.default is not dataclasses.MISSING:
            default: Any = f.default
        elif f.default_factory is not dataclasses.MISSING:
            default = f.default_factory()
        else:
            default = object()
        if getattr(obj, f.name) != default:
            reasons.append(f"{label} field {f.name!r}")


def _unknown_keys(f: Any, reasons: List[str]) -> None:
    if not isinstance(f, Mapping):
        reasons.append(f"filter {f!r}")
        return
    extra = set(f) - _FILTER_KEYS - ({"operator"} if "conditions" in f else set())  # a group's connective alias
    if "op" in f and "operator" in f and str(f["op"]).lower() != str(f["operator"]).lower():
        reasons.append("filter group with conflicting op and operator")
    if extra:
        reasons.append(f"filter keys {sorted(extra)}")
    for c in f.get("conditions") or []:
        _unknown_keys(c, reasons)


def spec_to_query(spec: Any, target_paths: Any, schema: Mapping[str, Any], *, start_row: int = 0,
                  end_row: Optional[int] = None, subtotals: str = "before",
                  cell_sorts: Optional[List[Tuple[int, str, str, bool, bool, bool]]] = None) -> PivotQuery:
    """Map a legacy spec to the IR; raise :class:`CoreUnsupported` with every reason.

    The query's window addresses the legacy flattened list (nodes in tree
    order; the core orders the grand total first, hence the +1).  Column
    members are left empty: :func:`core_hierarchy_view` resolves them.

    With a column pivot (REQ-SORT-004), ``__RowTotal__<alias>`` sorts by the
    row total and ``<column key>_<alias>`` by that pivot cell; a cell sort
    needs the typed member, so it is appended to ``cell_sorts`` as
    (precedence, key, alias, desc, absolute) for :func:`_resolve_members` (without
    ``cell_sorts`` it is unsupported).  A plain measure alias names no column
    of a pivoted view and is ignored, as legacy does.
    """
    reasons: List[str] = []
    families = {name: type_family(dtype) for name, dtype in schema.items()}
    _uninterpreted(spec, frozenset(_SPEC_FIELDS), "spec", reasons)
    _uninterpreted(getattr(spec, "pivot_config", None), frozenset(_PIVOT_CONFIG_FIELDS), "pivot_config", reasons)
    for m in spec.measures or []:
        _uninterpreted(m, frozenset(_MEASURE_FIELDS), "measure", reasons)
    for f in spec.filters or []:
        _unknown_keys(f, reasons)
    for s in (spec.sort if isinstance(spec.sort, list) else ([spec.sort] if spec.sort else [])):
        extra = set(s) - _SORT_KEYS if isinstance(s, Mapping) else {"<not a mapping>"}
        if extra:
            reasons.append(f"sort keys {sorted(extra)}")
    categories = _categories(spec, families, reasons)
    for attr, label in (("subtotals", "explicit subtotals"),
                        ("drill_paths", "drill paths"), ("cursor", "keyset cursor"),
                        ("grouping_config", "grouping sets")):
        if getattr(spec, attr, None):
            reasons.append(label)
    full_rows = [str(r) for r in getattr(spec, "full_rows", None) or []]
    shown_rows = [str(r) for r in spec.rows or []]
    if full_rows and (full_rows[: len(shown_rows)] != shown_rows or any(f not in families for f in full_rows)):
        # A hierarchy deeper than the shown rows changes no node value (REQ-TOT-001: a node is agg(E∩n)),
        # so only a full_rows that extends the rows with known fields is a no-op.
        reasons.append("spec field 'full_rows'")
    axis = _measure_axis(spec, families, reasons)  # REQ-DIM-002
    if axis is None and reasons and getattr(getattr(spec, "measure_axis", None), "placement", "none") != "none":
        raise CoreUnsupported(reasons)  # the rest of the spec names the axis columns: only the axis reasons count
    columns = axis["columns"] if axis else [str(c) for c in getattr(spec, "columns", None) or []]
    cfg = getattr(spec, "pivot_config", None)
    if columns:
        if cfg is None or not cfg.enabled:
            reasons.append("column pivot without pivot_config")
        elif cfg.materialized_column_values is None and (cfg.top_n or cfg.order_by_measure or cfg.column_cursor):
            reasons.append("column pivot options")
        unknown_columns = [c for c in columns if c not in families]
        if unknown_columns:
            reasons.append(f"unknown column fields {unknown_columns}")
    if getattr(spec, "offset", 0):
        reasons.append("spec offset")

    rows = axis["rows"] if axis else [str(r) for r in spec.rows or []]
    measures, derived = (axis["measures"], []) if axis else _measures(spec, reasons)
    if axis and getattr(spec, "formulas", None):
        reasons.append("measure formula with a measure axis")
    elif not axis:
        measures = [*measures, *_formulas(spec, measures, derived, reasons)]
    ragged = None
    raw_sort = axis["sort"] if axis else (spec.sort if isinstance(spec.sort, list) else ([spec.sort] if spec.sort else []))
    if getattr(spec, "group_allow_unbalanced", False):  # REQ-DIM-005
        ragged = _ragged(spec, rows, families, subtotals, axis, reasons)
        if ragged is not None:
            positions = dict(zip(rows, ragged.names()))
            raw_sort = [{**s, "field": positions[s["field"]]} if isinstance(s, Mapping) and s.get("field") in positions
                        else s for s in raw_sort]
            rows = list(ragged.names())
            measures = [*measures, MeasureRef(f"{HIDDEN}ragn", "max", "__ragn"),
                        *(MeasureRef(f"{HIDDEN}ragf{i}", "min", f"__ragf{i}") for i in range(1, len(rows) + 1))]
    aliases = frozenset(m.alias for m in measures if not m.alias.startswith(HIDDEN))
    if derived and columns:
        reasons.append("derived measure with column pivot")
    unknown = [r for r in rows if r not in families]
    if unknown:
        reasons.append(f"unknown row fields {unknown}")

    sort: List[SortKey] = []
    for s in raw_sort:
        sort_type = str(s.get("sortType") or "").strip().lower()
        absolute = bool(s.get("absoluteSort")) or sort_type in {"absolute", "abs", "absolute_value", "absolute-value"}
        semantic = s.get("semanticType") or s.get("semantic") or s.get("sortSemantic")
        key_field = s.get("sortKeyField")
        axis_label = axis["unpivot"].label if axis else None
        if (s.get("field") not in rows and key_field == f"__sortkey__{s.get('field')}"
                and (s.get("field") == axis_label or (axis is None and s.get("field") not in families))):
            continue  # the measure-axis level is not shown (axis off or label not a row): nothing to order
        semantic_kind = str(semantic or ("tenor" if sort_type == "curve_pillar_tenor" else "")).strip().lower()
        nulls = str(s.get("nulls") or "last").strip().lower()
        nulls_first = nulls == "first"  # REQ-SORT-006
        if key_field and key_field in families and s.get("field") in rows:
            # REQ-SORT-003: members ordered by MIN(key) (legacy: a hidden sort key per member).  A supplied
            # key is the order: it wins over a semantic type or a pillar sort type, as in legacy.
            # The key is the order, whatever the sort type (an absolute key orders as the key: keys are ranks).
            sort.append(SortKey(str(s["field"]), str(s.get("order", "asc")).lower() == "desc", False, str(key_field),
                                nulls_first=nulls_first))
        elif key_field and key_field in families and s.get("field") not in columns:
            continue  # a member order for a dimension that is not a row level: nothing to order
        elif key_field and key_field in families:
            reasons.append("custom member order on a column dimension")
        elif semantic_kind == "tenor" and s.get("field") in rows and families.get(str(s.get("field"))) == "string":
            if absolute:
                reasons.append("tenor sort with absolute")
            else:  # REQ-SORT-005
                sort.append(SortKey(str(s["field"]), str(s.get("order", "asc")).lower() == "desc", semantic="tenor",
                                    nulls_first=nulls_first))
        elif semantic:
            reasons.append(f"semantic sort {semantic!r}")
        elif nulls not in {"first", "last"}:
            reasons.append(f"nulls {s.get('nulls')!r}")
        elif sort_type and not absolute and sort_type not in _PLAIN_SORT_TYPES:
            reasons.append(f"sort type {sort_type!r}")
        elif s.get("field") in rows or (s.get("field") in aliases and not columns):
            sort.append(SortKey(str(s["field"]), str(s.get("order", "asc")).lower() == "desc", absolute,
                                nulls_first=nulls_first))
        elif s.get("field") in aliases:
            continue  # a pivoted view shows no column for the plain measure: ignored, as legacy does
        elif columns and str(s.get("field") or "").startswith("__RowTotal__")                 and str(s["field"])[len("__RowTotal__"):] in aliases:
            sort.append(SortKey(str(s["field"])[len("__RowTotal__"):], str(s.get("order", "asc")).lower() == "desc",
                                absolute, nulls_first=nulls_first))
        elif columns and cell_sorts is not None and any(
                str(s.get("field") or "").endswith(f"_{a}") for a in aliases):
            field_text = str(s["field"])
            alias = max((a for a in aliases if field_text.endswith(f"_{a}")), key=len)
            cell_sorts.append((len(sort), field_text[: -len(alias) - 1], alias,
                               str(s.get("order", "asc")).lower() == "desc", absolute, nulls_first))
        elif s.get("field") not in columns and (
                s.get("field") in families
                or s.get("field") in {f"{m.field}_{m.agg}" for m in spec.measures or [] if m.field}):
            continue  # a source field or a measure's field_agg name is no column of the view: ignored (legacy)
        else:
            reasons.append(f"sort on {s.get('field')!r}")

    filters, measure_filters = _filters(spec, aliases, families, len(rows), reasons)
    if axis:
        measure_filters = (*measure_filters, *axis["measure_filters"])
    if reasons:
        raise CoreUnsupported(reasons)

    # Pre-order puts the grand total first (+1 skips it); post-order puts it last (REQ-LAYOUT-001).
    offset = max(0, int(start_row)) + (1 if subtotals == "before" else 0)
    limit = None if end_row is None else max(0, int(end_row) - int(start_row) + 1)
    query = PivotQuery(
        dataset=str(spec.table),
        rows=tuple(rows),
        measures=tuple(measures),
        filters=filters,
        measure_filters=measure_filters,
        sort=tuple(sort),
        subtotals=subtotals,
        expanded=_expanded(target_paths, rows, families) if rows else None,
        window=Window(offset, limit),
        columns=tuple(columns),
        derived=tuple(derived),
        categories=categories,
        unpivot=axis["unpivot"] if axis else None,
        ragged=ragged,
    )
    # Typed errors before any core expression is built (REQ-AGG-001, REQ-AGG-008).
    return query.validate({name: type_family(dtype) for name, dtype in schema.items()})


# --------------------------------------------------------------------- columns


def _column_orders(spec: Any, query: PivotQuery) -> List[ColumnOrder]:
    """REQ-PIV-002 from the request's ``column_sort_options`` (the order the column catalog shows).

    Per column field: ``pivotSortMode`` "total" orders by the first measure's
    total, "label" by the member; with neither, the order is "label" (ascending
    unless a direction is given). "total" defaults to descending.
    A custom dimension's "creation"/"numeric" order is the field's default
    member key (``__sortkey__<field>``).
    """
    options = getattr(spec, "column_sort_options", None)
    options = options if isinstance(options, Mapping) else {}
    value_aliases = [m.alias for m in query.measures if not m.alias.startswith(HIDDEN)] + [d.alias for d in query.derived]
    first = str(getattr((spec.measures or [None])[0], "alias", None) or "")
    total = first if first in value_aliases else (value_aliases[0] if query.unpivot is not None and value_aliases
                                                  else None)
    creation = {category_field_id(d): str(d.get("order") or "creation").strip().lower()
                for d in getattr(spec, "custom_dimensions", None) or [] if isinstance(d, Mapping)}
    orders: List[ColumnOrder] = []
    for field in query.columns:
        default: Dict[str, Any] = {}
        dim_order = creation.get(field)
        if dim_order == "alpha_desc":
            default = {"pivotSortMode": "label", "pivotDirection": "desc"}
        elif dim_order in {"creation", "numeric"}:
            default = {"pivotSortMode": "label", "pivotDirection": "asc", "sortKeyField": f"__sortkey__{field}"}
        elif dim_order == "numeric_desc":
            default = {"pivotSortMode": "label", "pivotDirection": "desc", "sortKeyField": f"__sortkey__{field}"}
        explicit = options.get(field) if isinstance(options.get(field), Mapping) else {}
        raw = {**default, **explicit}
        mode = str(raw.get("pivotSortMode") or "").strip().lower()
        direction = str(raw.get("pivotDirection") or "").strip().lower()
        if mode not in {"label", "total"} or (mode == "total" and total is None):
            mode = "label"
        if direction not in {"asc", "desc"}:
            direction = "desc" if mode == "total" and total is not None else "asc"
        sort_type = str(raw.get("sortType") or "").strip().lower()
        semantic = str(raw.get("semanticType") or raw.get("sortSemantic")
                       or ("tenor" if sort_type == "curve_pillar_tenor" else "")).strip().lower()
        absolute = bool(raw.get("absoluteSort")) or sort_type in {"absolute", "abs", "absolute_value", "absolute-value"}
        nulls_first = str(raw.get("nulls") or "last").strip().lower() == "first"
        if mode == "total" and total is not None:
            orders.append(ColumnOrder(field, direction == "desc", total=total, absolute=absolute,
                                      nulls_first=nulls_first))
        else:
            key = raw.get("sortKeyField")
            orders.append(ColumnOrder(field, direction == "desc", by=str(key) if key else None,
                                      semantic=semantic if semantic == "tenor" else None,
                                      absolute=absolute and not key, nulls_first=nulls_first))
    return orders


def core_column_catalog(resolve: Callable[[str], Any], spec: Any) -> List[Dict[str, Any]]:
    """The ordered column catalog of a pivot spec (REQ-PIV-001, REQ-PIV-002).

    One entry per column member, in catalog order: each column field's value
    and the member's display key ``_col_key``.  A ``column_cursor`` keeps the
    members whose key sorts after it.  The column window the view then shows is
    chosen from this catalog (``pivot_config.materialized_column_values``).
    """
    return core_column_catalog_members(resolve, spec)[0]


def core_column_catalog_members(resolve: Callable[[str], Any],
                                spec: Any) -> Tuple[List[Dict[str, Any]], Tuple[Tuple[Any, ...], ...]]:
    """The catalog entries and every typed column member (catalog order, before the cursor) from one read.

    The members are what :func:`core_hierarchy_view` binds the column window
    to (``column_members=``), so a request reads the members once.
    """
    import copy

    relation = resolve(spec.table)
    plain = copy.copy(spec)
    plain.offset = 0
    cfg = copy.copy(getattr(spec, "pivot_config", None))
    cursor = getattr(cfg, "column_cursor", None) if cfg is not None else None
    if cfg is not None:
        cfg.top_n = None
        cfg.order_by_measure = None
        cfg.column_cursor = None
        cfg.materialized_column_values = None
        plain.pivot_config = cfg
    query = spec_to_query(plain, [["__ALL__"]], relation.schema(), subtotals="none", cell_sorts=[])
    if not query.columns:
        return [], ()
    resolver = lambda _name: relation  # noqa: E731 - one resolution per request (scope checked once)
    members = column_members(query, resolver, order=_column_orders(spec, query))
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for member in members:
        key = legacy_column_key(member)
        if key in seen:
            raise CoreUnsupported([f"ambiguous pivot column key {key!r}"])  # F-PIVOT-DELIM
        seen.add(key)
        if cursor is None or key > str(cursor):
            out.append({**dict(zip(query.columns, member)), "_col_key": key})
    return out, members


def legacy_column_key(member: Sequence[Any]) -> str:
    """The legacy display key of a column member (``str`` of each value joined by ``|``)."""
    return str(member[0]) if len(member) == 1 else "|".join(str(v) for v in member)


def _resolve_members(query: PivotQuery, spec: Any, resolver: Callable[[str], Any],
                     cell_sorts: Sequence[Tuple[int, str, str, bool, bool, bool]] = (),
                     catalog: Optional[Sequence[Tuple[Any, ...]]] = None) -> Tuple[PivotQuery, List[str]]:
    """Bind the column window: every member, or the legacy-selected keys, in their order.

    ``catalog``: the request's typed members from :func:`core_column_catalog_members`
    (same E0, so the same member set). A selected window binds to it without
    reading the members again; with no selection the members are read here,
    in the default member order.

    Deferred cell sorts are typed here; one naming no member is ignored, as legacy does.
    """
    if not query.columns:
        return query, []
    wanted = getattr(spec.pivot_config, "materialized_column_values", None)
    members = catalog if catalog is not None and wanted is not None else column_members(query, resolver)
    by_key: Dict[str, Tuple[Any, ...]] = {}
    for member in members:
        key = legacy_column_key(member)
        if key in by_key:
            raise CoreUnsupported([f"ambiguous pivot column key {key!r}"])  # F-PIVOT-DELIM
        by_key[key] = member
    keys = list(by_key) if wanted is None else [str(k) for k in wanted if str(k) in by_key]
    sort = list(query.sort)
    inserted = 0
    for position, key, alias, desc, absolute, nulls_first in cell_sorts:
        if key in by_key:
            sort.insert(position + inserted, SortKey(alias, desc, absolute, member=by_key[key], nulls_first=nulls_first))
            inserted += 1
    return replace(query, column_members=tuple(by_key[k] for k in keys), sort=tuple(sort)), keys


# --------------------------------------------------------------------- frame -> legacy rows


def _value_projection(query: PivotQuery, keys: Sequence[str], totals_column: bool) -> List[Tuple[str, str]]:
    """(legacy column, frame value) pairs of a view, computed once per request."""
    if not query.columns:
        return [(n, n) for n in query.value_columns() if not n.startswith(HIDDEN)]
    out: List[Tuple[str, str]] = []
    for j, _member, m, name in query.cells():
        if not m.alias.startswith(HIDDEN):  # helper measures (ragged paths, axis bases) stay internal
            out.append((f"{keys[j]}_{m.alias}", name))
    if totals_column:
        out += [(f"__RowTotal__{m.alias}", m.alias) for m in query.measures if not m.alias.startswith(HIDDEN)]
    return out


def _ragged_row(record: Mapping[str, Any], query: PivotQuery, values: Dict[str, Any]) -> Dict[str, Any]:
    """REQ-DIM-005: a ragged node in the legacy shape: each path member under the field it came from."""
    if query.ragged is None:
        raise RuntimeError("invariant violated: query.ragged is not None")
    fields = query.ragged.fields
    raw, path = record["values"], record["path"]
    if not path:
        out: Dict[str, Any] = {fields[0]: None, **values}
        out.update({"_id": GRAND_TOTAL_ID, "_isTotal": True, "_path": GRAND_TOTAL_DISPLAY_PATH, "depth": 0,
                    "_rowKey": "[]"})
        return out
    path_fields = [fields[int(raw[f"{HIDDEN}ragf{i}"])] for i in range(1, len(path) + 1)]
    out = {**dict(zip(path_fields, path)), **values}
    out.update({
        "_id": str(path[-1]),
        "_path": CanonicalPathCodec.display_path(list(path)),
        "_pathFields": path_fields,
        "depth": len(path) - 1,
        "_has_children": int(raw[f"{HIDDEN}ragn"] or 0) > len(path),
    })
    out["_rowKey"] = record["key"]
    return out


def _legacy_row(record: Mapping[str, Any], query: PivotQuery, projection: Sequence[Tuple[str, str]]) -> Dict[str, Any]:
    rows = query.rows
    path = record["path"]
    raw = record["values"]
    values = {out: raw[src] for out, src in projection}
    if query.ragged is not None:
        return _ragged_row(record, query, values)
    if not path:
        out: Dict[str, Any] = {f: None for f in rows[:1]}
        out.update(values)
        out.update({"_id": GRAND_TOTAL_ID, "_isTotal": True, "_path": GRAND_TOTAL_DISPLAY_PATH, "depth": 0,
                    "_rowKey": "[]"})
        return out
    out = dict(record["dims"])
    out.update(values)
    last = path[-1]
    out.update({
        "_id": "" if last is None else str(last),
        "_path": CanonicalPathCodec.display_path(list(path)),
        "_pathFields": list(rows[: len(path)]),
        "depth": len(path) - 1,
        "_rowKey": record["key"],
    })
    return out


def _legacy_rows_and_color_stats(
    data: Dict[str, List[Any]],
    query: PivotQuery,
    projection: Sequence[Tuple[str, str]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], bool]:
    """Single-pass columnar build of legacy rows and color-scale stats directly from `frame._data`."""
    import math

    from ..core.frame import DEPTH, _token
    from ..hierarchy_rows import _hierarchy_color_meta_keys, _hierarchy_color_stats_result

    depths = data[DEPTH]
    n = len(depths)
    if n == 0:
        return [], {"byCol": {}, "table": None}, False

    row_fields = query.rows
    dims = [data[k] for k in row_fields]
    proj_cols = [(out, data[src]) for out, src in projection]
    tokens: List[Dict[Any, str]] = [{} for _ in row_fields]
    tok = _token
    r0 = row_fields[0] if row_fields else ""
    d0 = dims[0] if dims else None
    t0 = tokens[0] if tokens else None
    r1 = row_fields[1] if len(row_fields) > 1 else ""
    d1 = dims[1] if len(dims) > 1 else None
    t1 = tokens[1] if len(tokens) > 1 else None
    pf_templates = [list(row_fields[:d]) for d in range(len(row_fields) + 1)]
    gt_path = GRAND_TOTAL_DISPLAY_PATH
    gt_id = GRAND_TOTAL_ID

    rows: List[Dict[str, Any]] = []
    append = rows.append
    skip_color: Optional[set[int]] = None
    has_nan = False

    for i, raw_depth in enumerate(depths):
        depth = int(raw_depth)
        if depth == 1 and d0 is not None and t0 is not None:
            v0 = d0[i]
            if type(v0) is float and v0 != v0:
                has_nan = True
            v0_str = "" if v0 is None else (v0 if type(v0) is str else str(v0))
            out: Dict[str, Any] = {r0: v0}
            for col_out, col_vals in proj_cols:
                out[col_out] = col_vals[i]
            out["_id"] = v0_str
            out["_path"] = v0_str
            out["_pathFields"] = [r0]
            out["depth"] = 0
            out["_rowKey"] = "[" + tok(t0, v0) + "]"
            append(out)
            if v0_str == gt_id or v0_str == gt_path:
                if skip_color is None:
                    skip_color = {i}
                else:
                    skip_color.add(i)
        elif depth == 2 and d0 is not None and d1 is not None and t0 is not None and t1 is not None:
            v0 = d0[i]
            v1 = d1[i]
            if (type(v0) is float and v0 != v0) or (type(v1) is float and v1 != v1):
                has_nan = True
            v0_str = "" if v0 is None else (v0 if type(v0) is str else str(v0))
            v1_str = "" if v1 is None else (v1 if type(v1) is str else str(v1))
            path_str = v0_str + "|||" + v1_str
            out = {r0: v0, r1: v1}
            for col_out, col_vals in proj_cols:
                out[col_out] = col_vals[i]
            out["_id"] = v1_str
            out["_path"] = path_str
            out["_pathFields"] = [r0, r1]
            out["depth"] = 1
            out["_rowKey"] = "[" + tok(t0, v0) + "," + tok(t1, v1) + "]"
            append(out)
            if v1_str == gt_id or path_str == gt_path:
                if skip_color is None:
                    skip_color = {i}
                else:
                    skip_color.add(i)
        elif depth == 0:
            out = {f: None for f in row_fields[:1]}
            for col_out, col_vals in proj_cols:
                out[col_out] = col_vals[i]
            out["_id"] = gt_id
            out["_isTotal"] = True
            out["_path"] = gt_path
            out["depth"] = 0
            out["_rowKey"] = "[]"
            append(out)
            if skip_color is None:
                skip_color = {i}
            else:
                skip_color.add(i)
        else:
            out = {}
            parts: List[str] = []
            key_parts: List[str] = []
            for j in range(depth):
                vj = dims[j][i]
                if type(vj) is float and vj != vj:
                    has_nan = True
                out[row_fields[j]] = vj
                parts.append("" if vj is None else (vj if type(vj) is str else str(vj)))
                key_parts.append(tok(tokens[j], vj))
            for col_out, col_vals in proj_cols:
                out[col_out] = col_vals[i]
            last_str = parts[-1]
            path_str = "|||".join(parts)
            out["_id"] = last_str
            out["_path"] = path_str
            out["_pathFields"] = pf_templates[depth].copy()
            out["depth"] = depth - 1
            out["_rowKey"] = "[" + ",".join(key_parts) + "]"
            append(out)
            if last_str == gt_id or path_str == gt_path:
                if skip_color is None:
                    skip_color = {i}
                else:
                    skip_color.add(i)

    # Columnar color-scale stats over projected measure columns (exact match with compute_hierarchy_color_scale_stats).
    meta_keys = _hierarchy_color_meta_keys(list(row_fields), list(query.columns))
    effective_proj: Dict[str, List[Any]] = {}
    for col_out, col_vals in proj_cols:
        effective_proj[col_out] = col_vals
    by_col: Dict[str, Dict[str, float]] = {}
    table_min = float("inf")
    table_max = float("-inf")
    for col_out, col_vals in effective_proj.items():
        in_meta = col_out in meta_keys
        cmin: Any = None
        cmax: Any = None
        if skip_color is None:
            for v in col_vals:
                vt = type(v)
                if vt is int:
                    if in_meta:
                        continue
                elif vt is float:
                    if v != v:
                        has_nan = True
                        continue
                    if in_meta:
                        continue
                elif v is None:
                    continue
                elif isinstance(v, (int, float)):
                    if isinstance(v, float) and math.isnan(v):
                        has_nan = True
                        continue
                    if in_meta:
                        continue
                else:
                    continue
                if cmin is None:
                    cmin = v
                    cmax = v
                else:
                    if v < cmin:
                        cmin = v
                    if v > cmax:
                        cmax = v
        else:
            for i, v in enumerate(col_vals):
                vt = type(v)
                if vt is float and v != v:
                    has_nan = True
                    continue
                if i in skip_color or in_meta or v is None:
                    continue
                if vt is int or vt is float:
                    pass
                elif isinstance(v, (int, float)):
                    if isinstance(v, float) and math.isnan(v):
                        has_nan = True
                        continue
                else:
                    continue
                if cmin is None:
                    cmin = v
                    cmax = v
                else:
                    if v < cmin:
                        cmin = v
                    if v > cmax:
                        cmax = v
        if cmin is not None:
            by_col[col_out] = {"min": cmin, "max": cmax}
            if cmin < table_min:
                table_min = cmin
            if cmax > table_max:
                table_max = cmax

    return rows, _hierarchy_color_stats_result(by_col, table_min, table_max), has_nan


def core_hierarchy_view(
    resolve: Callable[[str], Any],
    spec: Any,
    target_paths: Any,
    start_row: int,
    end_row: Optional[int],
    *,
    include_grand_total_row: bool = False,
    capture: Optional[Dict[str, Any]] = None,
    tabular_subtotals: bool = False,
    flat: bool = False,
    column_members: Optional[Sequence[Tuple[Any, ...]]] = None,
) -> Dict[str, Any]:
    """The ``run_hierarchy_view`` contract, answered by the core.

    ``column_members``: the request's catalog members (:func:`core_column_catalog_members`),
    so the column window is bound without a second member read.

    ``tabular_subtotals``: the tabular subtotal layout (REQ-LAYOUT-001), the
    post-order of the fully expanded tree; group rows carry
    ``_isTabularSubtotal`` and the grand total comes last.

    ``capture`` (shadow referee, opt-in) receives the core query and, for
    datasets of at most ``CAPTURE_MAX_ROWS`` records, the scoped data.

    The flattened list is the visible nodes in tree order followed, when
    ``spec.totals``, by the grand total.  ``rows`` is its inclusive
    ``[start_row, end_row]`` window and ``total_rows`` its length;
    ``grand_total_row`` is returned only when requested and enabled.
    """
    started = time.perf_counter()
    relation = resolve(spec.table)
    if tabular_subtotals or flat:
        target_paths = [["__ALL__"]]  # every group is open: collapse state does not apply to a flat layout
    cell_sorts: List[Tuple[int, str, str, bool, bool, bool]] = []
    query = spec_to_query(spec, target_paths, relation.schema(), start_row=start_row, end_row=end_row,
                          subtotals="none" if flat else ("after" if tabular_subtotals else "before"),
                          cell_sorts=cell_sorts)
    resolver = lambda _name: relation  # noqa: E731 - one resolution per request (scope checked once)
    query, keys = _resolve_members(query, spec, resolver, cell_sorts, column_members)
    if capture is not None:
        _capture(capture, query, relation)
    totals = bool(getattr(spec, "totals", False))
    totals_column = bool(getattr(getattr(spec, "pivot_config", None), "include_totals_column", False))

    projection = _value_projection(query, keys, totals_column)

    def legacy(record: Mapping[str, Any]) -> Dict[str, Any]:
        return _legacy_row(record, query, projection)

    # One statement: the window, the tree's row count and the grand total.
    frame = execute(query, resolver, grand_total=totals, prune_total=not totals, _need_arrow=False)
    nodes = int(frame.total or 0) - 1  # the tree's rows, less its grand total
    if end_row is None:  # the whole flattened list
        end_row = nodes
    total_record = frame.grand_total_record()
    grand_total = legacy(total_record) if totals and total_record is not None else None
    if tabular_subtotals or flat:
        return _post_order_view(query, frame, legacy, nodes, int(start_row), int(end_row), totals, grand_total,
                                include_grand_total_row, started, flat=flat)
    if query.ragged is None and frame._data is not None:
        rows, color_scale_stats, has_nan = _legacy_rows_and_color_stats(frame._data, query, projection)
        gt_has_nan = (
            any(type(v) is float and v != v for v in grand_total.values())
            if grand_total is not None
            else False
        )
        append_gt = grand_total is not None and int(start_row) <= nodes <= int(end_row)
        if append_gt and grand_total is not None:
            rows.append(dict(grand_total))
        return {
            "rows": rows,
            "total_rows": nodes + (1 if totals else 0),
            "grand_total_row": grand_total if include_grand_total_row else None,
            "color_scale_stats": color_scale_stats,
            "_core_fast_meta": {
                "normalized": not has_nan and (not append_gt or not gt_has_nan),
                "gt_normalized": not gt_has_nan,
            },
            # Same profile layer as the legacy controller, so profile consumers keep working.
            "profile": {"controller": {"operation": "hierarchy_view", "engine": "core",
                                       "totalMs": round((time.perf_counter() - started) * 1000.0, 3)}},
        }
    rows = [legacy(r) for r in frame.records()]  # pre-order: the window starts after the grand total
    if grand_total is not None and int(start_row) <= nodes <= int(end_row):
        rows.append(dict(grand_total))
    from ..hierarchy_rows import compute_hierarchy_color_scale_stats

    return {
        "rows": rows,
        "total_rows": nodes + (1 if totals else 0),
        "grand_total_row": grand_total if include_grand_total_row else None,
        "color_scale_stats": compute_hierarchy_color_scale_stats(rows, list(query.rows), list(query.columns)),
        # Same profile layer as the legacy controller, so profile consumers keep working.
        "profile": {"controller": {"operation": "hierarchy_view", "engine": "core",
                                   "totalMs": round((time.perf_counter() - started) * 1000.0, 3)}},
    }


async def run_core_read(controller: Any, table: Any, read: Callable[[Callable[[str], Any]], Any]) -> Any:
    """Run one core read for ``controller``: ``read(resolve)`` with its scoped table resolver.

    The read queues on the table's execution lock and runs through the
    backend's ``run_statements`` (request-level cancellation and the query
    deadline on every statement) when the backend offers it.  A controller
    without a scoped resolver cannot serve the core: ``CoreUnsupported``.
    """
    import asyncio

    from ..core.authority.scope import run_in_executor_scoped

    resolve = getattr(controller, "resolve_table", None)
    if resolve is None:
        raise CoreUnsupported(["controller without resolve_table"])

    def work() -> Any:
        return core_locked_call(controller, table, lambda: read(resolve))

    run_statements = getattr(getattr(controller, "backend", None), "run_statements", None)
    if callable(run_statements):
        return await run_statements(work)
    return await run_in_executor_scoped(asyncio.get_running_loop(), None, work)


def core_locked_call(controller: Any, table: Any, fn: Callable[[], Any]) -> Any:
    """Run a core read under the controller's execution lock for ``table``.

    The core shares the controller's backend connection, which is not safe for
    concurrent use: core reads queue on the same per-table lock as every other
    read of that table.
    """
    lock_for = getattr(controller, "_execution_lock_for_table", None)
    if lock_for is None:
        return fn()
    with lock_for(table):
        return fn()


def core_pivot_table(resolve: Callable[[str], Any], spec: Any) -> Any:
    """The ``run_pivot`` contract (one row per leaf group), answered by the core as an Arrow table.

    Columns: the row fields, then the measures, or with a column pivot the
    cells ``<column key>_<alias>`` (and ``__RowTotal__<alias>`` with the totals
    column).  Rows are the leaves of the flat layout (REQ-LAYOUT-002) in its
    order, windowed by ``spec.offset``/``spec.limit``; with no row fields the
    single row is the grand total.  With ``spec.totals`` the grand total
    follows the leaves (its row fields NULL), at every depth, when the window
    reaches it: the consumer contract the hierarchy view reads.
    """
    import copy

    import pyarrow.compute as pc

    from ..core.frame import DEPTH

    relation = resolve(spec.table)
    offset = max(0, int(getattr(spec, "offset", 0) or 0))
    cursor = getattr(spec, "cursor", None)
    if cursor:  # the continuation this read returns as ``next_cursor``: an offset into the same order
        if set(cursor) != {"offset"} or not isinstance(cursor["offset"], int) or cursor["offset"] < 0:
            raise CoreUnsupported(["keyset cursor"])
        offset += cursor["offset"]
    limit = getattr(spec, "limit", None) or None  # the run_pivot contract: limit 0 (or None) = no limit
    plain = copy.copy(spec)
    plain.offset = 0
    plain.cursor = None
    cell_sorts: List[Tuple[int, str, str, bool, bool, bool]] = []
    query = spec_to_query(plain, [["__ALL__"]], relation.schema(), subtotals="none", cell_sorts=cell_sorts)
    resolver = lambda _name: relation  # noqa: E731 - one resolution per request (scope checked once)
    query, keys = _resolve_members(query, spec, resolver, cell_sorts)
    totals_column = bool(getattr(getattr(spec, "pivot_config", None), "include_totals_column", False))
    end = None if limit is None else offset + max(0, int(limit))
    if query.rows:  # the flat layout: leaves first, the grand total last
        frame = execute(replace(query, window=Window(offset, None if end is None else end - offset)), resolver,
                        prune_total=not bool(getattr(spec, "totals", False)))
        table = frame.table if getattr(spec, "totals", False) else frame.table.filter(pc.greater(frame.table[DEPTH], 0))
    else:
        frame = execute(replace(query, window=Window()), resolver)
        table = frame.table.slice(offset, None if end is None else max(0, end - offset))
    columns, names = list(query.rows), list(query.rows)
    if not query.columns:
        values = [m.alias for m in query.measures if not m.alias.startswith(HIDDEN)] + [d.alias for d in query.derived]
        columns += values
        names += values
    else:
        for j, _member, m, name in query.cells():
            if not m.alias.startswith(HIDDEN):
                columns.append(name)
                names.append(f"{keys[j]}_{m.alias}")
        if totals_column:
            shown = [m.alias for m in query.measures if not m.alias.startswith(HIDDEN)]
            columns += shown
            names += [f"__RowTotal__{alias}" for alias in shown]
    return table.select(columns).rename_columns(names)


def _post_order_view(query: PivotQuery, frame: Any, legacy: Callable[[Mapping[str, Any]], Dict[str, Any]],
                     nodes: int, start_row: int, end_row: int, totals: bool, grand_total: Optional[Dict[str, Any]],
                     include_grand_total_row: bool, started: float, *, flat: bool = False) -> Dict[str, Any]:
    """Grand total last, at index ``nodes``: tabular subtotals (post-order) or the flat layout (leaves)."""
    depth = len(query.rows)
    rows: List[Dict[str, Any]] = []
    for record in frame.records():
        if not record["path"]:
            continue  # the grand total: appended last, below, when the window reaches it
        row = legacy(record)
        if flat:  # leaves of a flat table: nothing to expand
            row.update({"_has_children": False, "_is_expanded": False, "_depth": row["depth"]})
        elif len(record["path"]) < depth:
            row["_isTabularSubtotal"] = True
        rows.append(row)
    if totals and grand_total is not None and start_row <= nodes <= end_row:
        rows.append(dict(grand_total))
    from ..hierarchy_rows import compute_hierarchy_color_scale_stats

    return {
        "rows": rows,
        "total_rows": nodes + (1 if totals else 0),
        "grand_total_row": grand_total if include_grand_total_row else None,
        "color_scale_stats": compute_hierarchy_color_scale_stats(rows, list(query.rows), list(query.columns)),
        "profile": {"controller": {"operation": "hierarchy_view", "engine": "core",
                                   "layout": "flat" if flat else "tabular_subtotals",
                                   "totalMs": round((time.perf_counter() - started) * 1000.0, 3)}},
    }


CAPTURE_MAX_ROWS = 2000


def _capture(capture: Dict[str, Any], query: PivotQuery, relation: Any) -> None:
    import base64

    import pyarrow as pa

    capture["query"] = to_wire(query)
    counted = relation.count()
    verify_scoped(counted)
    if int(materialize(counted).as_py()) > CAPTURE_MAX_ROWS:
        return
    verify_scoped(relation)
    table = materialize(relation)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    capture["data"] = base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")


# --------------------------------------------------------------------- drill-through


def _node_path(payload: Mapping[str, Any], fields: Sequence[str], families: Mapping[str, str]) -> Tuple[Any, ...]:
    """The typed node path: the canonical ``rowKey`` when sent, else the display path."""
    key = payload.get("rowKey") or payload.get("row_key")
    if key:
        path = CanonicalPathCodec.decode(str(key))
        if len(path) != len(fields):
            raise CoreUnsupported(["row key does not match the row fields"])
        return tuple(path)
    display = payload.get("row_path") or payload.get("rowPath") or ""
    parts = str(display).split("|||") if display else []
    if len(parts) != len(fields):
        raise CoreUnsupported(["display path does not match the row fields"])
    if any(part == "" for part in parts):
        raise CoreUnsupported(["display path with an empty or NULL member (send rowKey)"])
    return tuple(_coerce(part, families[f]) for part, f in zip(parts, fields, strict=True))


def core_drill_through(resolve: Callable[[str], Any], spec: Any, extra_filters: Sequence[Mapping[str, Any]],
                       payload: Mapping[str, Any], *, limit: int, offset: int, sort_col: Optional[str],
                       sort_dir: str, text_filter: str) -> Dict[str, Any]:
    """The ``get_drill_through_data`` contract, answered by the core (REQ-DET-001)."""
    relation = resolve(spec.table)
    families = {name: type_family(dtype) for name, dtype in relation.schema().items()}
    reasons: List[str] = []
    if getattr(spec, "group_allow_unbalanced", False):
        reasons.append("drill-through in a ragged hierarchy")  # its paths are positional, not per field
    categories = _categories(spec, families, reasons)  # REQ-DIM-001
    fields = payload.get("row_fields") or payload.get("pathFields") or payload.get("rowFields") or []
    fields = [str(f) for f in fields] if isinstance(fields, list) else []
    unknown = [f for f in fields if f not in families]
    if unknown:
        reasons.append(f"unknown row fields {unknown}")
    aliases = frozenset(str(m.alias) for m in spec.measures or [])
    filters: List[Any] = []
    for f in [*(spec.filters or []), *extra_filters]:
        if set(_flatten_fields(f)) & aliases:
            reasons.append("measure filter in drill-through")
            continue
        mapped = _dimension_filter(f, families, reasons)
        if mapped is not None:
            filters.append(mapped)
    if sort_col and sort_col not in families:
        reasons.append(f"sort on {sort_col!r}")
    if reasons:
        raise CoreUnsupported(reasons)
    path = _node_path(payload, fields, families)
    query = DetailQuery(
        dataset=str(spec.table),
        filters=tuple(filters),
        path_fields=tuple(fields),
        path=path,
        search=text_filter or None,
        sort=(SortKey(sort_col, sort_dir == "desc"),) if sort_col else (),
        window=Window(max(0, int(offset)), max(0, int(limit))),
        categories=categories,
    )
    table, total = detail(query, lambda _name: relation)
    return {"rows": table.to_pylist(), "total_rows": total}


# --------------------------------------------------------------------- record-level reads (live aggregation)


def core_records(relation: Any, spec: Any, filters: Sequence[Mapping[str, Any]]) -> Any:
    """The E0 records of ``spec`` (its categories, then ``filters``) as an unmaterialized relation.

    Filters on measure aliases are post-aggregation and do not select records.  Every other filter must
    map exactly onto the core: an unmappable one (a row-scope filter, for instance) is an error, never a
    silently wider record set (REQ-SEC-001).
    """
    families = {name: type_family(dtype) for name, dtype in relation.schema().items()}
    reasons: List[str] = []
    categories = _categories(spec, families, reasons)
    aliases = frozenset(str(m.alias) for m in spec.measures or [])
    mapped: List[Any] = []
    for f in filters:
        if set(_flatten_fields(f)) <= aliases:
            continue
        item = _dimension_filter(f, families, reasons)
        if item is not None:
            mapped.append(item)
    if reasons:
        raise CoreUnsupported(reasons)
    query = DetailQuery(dataset=str(spec.table), filters=tuple(mapped), categories=categories)
    return records(query, lambda _name: relation)


def core_record_filter(relation: Any, f: Mapping[str, Any]) -> Any:
    """The boolean expression of one spec filter over ``relation``; an unmappable filter is an error."""
    families = {name: type_family(dtype) for name, dtype in relation.schema().items()}
    reasons: List[str] = []
    item = _dimension_filter(f, families, reasons)
    if reasons or item is None:
        raise CoreUnsupported(reasons or ["empty filter"])
    return record_predicate(relation, item)


def core_measure_expr(relation: Any, m: Any) -> Any:
    """One spec measure as a registry aggregation over ``relation``, named by its alias (conditional
    measures apply their condition exactly); an unsupported measure is an error."""
    reasons: List[str] = []
    condition = None
    if getattr(m, "filter_condition", None):
        condition = core_record_filter(relation, m.filter_condition)
        m = replace(m, filter_condition=None) if is_dataclass(m) else m
    ref = _measure(m, reasons)
    if ref is None or reasons:
        raise CoreUnsupported(reasons or [f"measure {getattr(m, 'alias', None)!r}"])
    return measure_expr(relation, ref, condition)


# --------------------------------------------------------------------- filter options


def core_unique_values(resolve: Callable[[str], Any], spec: Any, column_id: str, *, search: str, limit: int,
                       offset: int) -> Tuple[List[Any], int]:
    """The ``get_unique_values`` contract, answered by the core (REQ-DET-002)."""
    relation = resolve(spec.table)
    families = {name: type_family(dtype) for name, dtype in relation.schema().items()}
    reasons: List[str] = []
    categories = _categories(spec, families, reasons)  # REQ-DIM-001
    axis = getattr(spec, "measure_axis", None)
    if axis is not None and getattr(axis, "placement", "none") != "none":
        reasons.append("measure axis")
    if column_id not in families:
        reasons.append(f"unknown column {column_id!r}")
    aliases = frozenset(str(m.alias) for m in spec.measures or [])
    filters: List[Any] = []
    for f in spec.filters or []:
        _unknown_keys(f, reasons)
        if set(_flatten_fields(f)) & aliases:
            reasons.append("measure filter in filter options")
            continue
        mapped = _dimension_filter(f, families, reasons)
        if mapped is not None:
            filters.append(mapped)
    if reasons:
        raise CoreUnsupported(reasons)
    query = DistinctQuery(str(spec.table), column_id, tuple(filters), str(search or "").strip() or None,
                          Window(max(0, int(offset)), max(0, int(limit))), categories)
    return distinct_values(query, lambda _name: relation)
