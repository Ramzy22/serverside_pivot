"""Leaf-partials strategy: every node and cell merged from one scan's sufficient statistics.

One ``GROUP BY`` of the prepared, row-filtered E0 at the partial grain (the
visible levels, the measure-filter depth, the member-sort ancestors and the
column dimensions) computes each measure's mergeable statistics.
Eligibility, level totals, pivot cells and sort lookups are algebraic merges
of those partials; E/E0 semantics match the reference.  Holistic aggregations (median, exact distinct) and DECIMAL
averages cannot be merged exactly this way, so :func:`supports` rejects them
and the planner uses ``union_all``.  Result-equivalence with ``union_all`` is
enforced by tests/core/test_strategy_equivalence.py.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

import ibis

from ..logical import LogicalPlan
from .union_all import (
    _CMP,
    AggregateProvider,
    Restriction,
    _member_condition,
    _semi_join_on_keys,
    build,
    prepared,
    restrict,
    tree,
)

__all__ = ["grain", "lower", "partials", "supports", "tree_of"]

_MERGEABLE = {"sum", "count", "min", "max", "avg"}


def supports(plan: LogicalPlan, schema: Any) -> bool:
    if plan.query.ragged is not None:
        return False  # ragged levels end at different depths; the reference strategy serves them
    for m in plan.query.measures:
        if m.agg not in _MERGEABLE:
            return False
        if m.agg == "avg" and m.field is not None and not (
            schema[m.field].is_integer() or schema[m.field].is_floating()
        ):
            return False
    return bool(plan.rows)


def grain(plan: LogicalPlan) -> List[str]:
    """The partial grain: the visible levels, filter depth and member-sort ancestors, then the columns."""
    depth = max((level.depth for level in plan.levels), default=0)
    depth = max([depth, *(plan.rows.index(dim) + 1 for dim, _ in plan.member_keys),
                 max(plan.filters_by_level, default=0)])
    return list(dict.fromkeys([*plan.rows[:depth], *plan.query.columns]))


def _minimum(schema: Any, field: str) -> str:
    """The partial-MIN column of a member-key sort column (REQ-SORT-003), by schema position."""
    return f"__min_{list(schema.names).index(field)}"


def partials(source: Any, plan: LogicalPlan) -> Any:
    """The partials of an already prepared, row-filtered E0 relation (never E), at :func:`grain`.

    Besides the measures' statistics it carries the partial MIN of each
    member-key sort column, no other.
    """
    if not supports(plan, source.schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    metrics = _partials(source, plan)
    schema = source.schema()
    metrics.update({_minimum(schema, by): source[by].min() for by in sorted({by for _dim, by in plan.member_keys})})
    keys = grain(plan)
    return source.group_by(keys).aggregate(**metrics) if keys else source.aggregate(**metrics)


def _partials(source: Any, plan: LogicalPlan) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i, m in enumerate(plan.query.measures):
        col: Any = None if m.field is None else source[m.field]
        if m.agg in {"sum", "avg"}:
            out[f"__s{i}"] = col.sum() if col is not None else source.count()
        if m.agg in {"count", "avg"}:
            out[f"__c{i}"] = source.count() if col is None else col.count()
        if m.agg == "min":
            out[f"__m{i}"] = col.min()
        if m.agg == "max":
            out[f"__m{i}"] = col.max()
    return out


def _merged(leaf: Any, i: int, agg: str, where: Optional[Any]) -> Any:
    """Measure ``i`` merged from the partials ``where`` selects (all of the group's, when None)."""
    if agg == "sum":
        return leaf[f"__s{i}"].sum(where=where)
    if agg == "count":
        return leaf[f"__c{i}"].sum(where=where).coalesce(0)  # count over no rows is 0
    if agg == "min":
        return leaf[f"__m{i}"].min(where=where)
    if agg == "max":
        return leaf[f"__m{i}"].max(where=where)
    # avg = sum / count, NULL when no non-null inputs
    total, count = leaf[f"__s{i}"].sum(where=where), leaf[f"__c{i}"].sum(where=where)
    return ibis.ifelse(count.coalesce(0) == 0, ibis.null(), total.cast("float64") / count)


def _merge(leaf: Any, plan: LogicalPlan, schema: Any) -> Dict[str, Any]:
    """Row totals, then one merge per pivot cell over its column member's partials."""
    q = plan.query
    index = {m.alias: i for i, m in enumerate(q.measures)}
    out = {m.alias: _merged(leaf, i, m.agg, None) for i, m in enumerate(q.measures)}
    for _j, member, m, name in q.cells():
        out[name] = _merged(leaf, index[m.alias], m.agg, _member_condition(leaf, schema, q.columns, tuple(member)))
    return out


def _provider(raw: Any, plan: LogicalPlan) -> AggregateProvider:
    schema = raw.schema()
    leaf = partials(raw, plan)  # the one scan; every lookup below merges it
    # Every passing-group lookup is independently evaluated against E0,
    # never against the result of a preceding measure filter.
    passing = []
    for depth, filters in sorted(plan.filters_by_level.items()):
        for cells in sorted({f.cells for f in filters}):
            selected = [f for f in filters if f.cells == cells]
            keys = list(plan.rows[:depth]) + (list(plan.query.columns) if cells else [])
            values = {f.measure: _merged(leaf, next(i for i, m in enumerate(plan.query.measures)
                                                  if m.alias == f.measure),
                                        next(m.agg for m in plan.query.measures if m.alias == f.measure), None)
                      for f in selected}
            grouped = leaf.group_by(keys).aggregate(**values)
            terms = [_CMP[f.op](grouped[f.measure], f.value) for f in selected]
            condition = terms[0]
            for term in terms[1:]:
                condition = condition & term
            passing.append((keys, grouped.filter(condition)))

    def eligible(cube: Any) -> Any:
        for keys, groups in passing:
            cube = _semi_join_on_keys(cube, groups, keys)
        return cube

    def records(keys: Sequence[str], restriction: Restriction) -> Any:
        cube = restrict(leaf, restriction, schema)
        return eligible(cube) if plan.values_from == "E" else cube

    def aggregate(keys: Sequence[str], restriction: Restriction) -> Any:
        cube = records(keys, restriction)
        metrics = _merge(cube, plan, schema)
        return cube.group_by(list(keys)).aggregate(**metrics) if keys else cube.aggregate(**metrics)

    def member_min(keys: Sequence[str], restriction: Restriction, by: str, name: str) -> Any:
        cube = records(keys, restriction)
        return cube.group_by(list(keys)).aggregate(**{name: cube[_minimum(schema, by)].min()})

    def cell_values(keys: Sequence[str], restriction: Restriction, entries: Sequence[Any]) -> Any:
        cube = records(keys, restriction)
        indices = {m.alias: i for i, m in enumerate(plan.query.measures)}
        aggs = {m.alias: m.agg for m in plan.query.measures}
        metrics = {f"__cell{len(keys)}_{n}": _merged(
            cube, indices[s.field], aggs[s.field], _member_condition(cube, schema, plan.query.columns, tuple(member)))
            for n, s, member in entries}
        return cube.group_by(list(keys)).aggregate(**metrics)

    def eligible_records(keys: Sequence[str], restriction: Restriction) -> Any:
        return eligible(restrict(leaf, restriction, schema))

    return AggregateProvider(raw, schema, eligible_records, aggregate, member_min, cell_values)


def lower(plan: LogicalPlan, relation: Any) -> Any:
    if not supports(plan, prepared(relation, plan.query).schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    return build(plan, relation, None, _provider)


def tree_of(plan: LogicalPlan, relation: Any) -> Tuple[Any, List[Any]]:
    if not supports(plan, prepared(relation, plan.query).schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    return tree(plan, relation, None, _provider)
