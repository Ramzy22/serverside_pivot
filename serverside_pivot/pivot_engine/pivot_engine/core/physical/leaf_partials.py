"""Leaf-partials strategy: every node and cell merged from reusable sufficient statistics.

Cubes aggregate prepared, row-filtered E0 at a requested row grain plus
column dimensions and any deeper measure-filter keys. Eligibility, totals,
pivot cells and sort lookups are algebraic merges of their sufficient
statistics. Materialization belongs to the caller; a shared Ibis expression
does not guarantee one physical scan. E/E0 semantics match the reference.
Holistic aggregations (median, exact distinct) and DECIMAL
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
    ordered_tree,
    prepared,
    restrict,
    tree,
)

__all__ = ["grain", "minima_fields", "scalar_minima", "partials", "from_partials", "lower", "supports", "tree_of"]

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


def grain(plan: LogicalPlan, keys: Optional[Sequence[str]] = None) -> List[str]:
    """Requested grain plus filter depth, member-sort prefixes and column dimensions.

    Explicit keys allow shallow, restricted per-level cache entries. With no
    keys, include every visible level and every member-sort ancestor.
    """
    if keys is None:
        depth = max((level.depth for level in plan.levels), default=0)
        depth = max([depth, *(plan.rows.index(dim) + 1 for dim, _ in plan.member_keys)])
        keys = plan.rows[:depth]
    depth = max(plan.filters_by_level, default=0)
    return list(dict.fromkeys([*keys, *plan.rows[:depth], *plan.query.columns]))


def minima_fields(plan: LogicalPlan) -> Tuple[str, ...]:
    """The source columns whose partial MIN the cube carries: the member-key sort columns (REQ-SORT-003)."""
    return tuple(sorted({by for _dim, by in plan.member_keys}))


def scalar_minima(schema: Any, fields: Optional[Sequence[str]] = None) -> Dict[str, str]:
    """Stable source-column to partial-MIN aliases (by schema position), for ``fields`` (default: every
    orderable column)."""
    wanted = None if fields is None else set(fields)
    return {name: f"__scalar_min_{i}" for i, (name, dtype) in enumerate(schema.items())
            if (wanted is None or name in wanted)
            and (dtype.is_numeric() or dtype.is_string() or dtype.is_boolean() or dtype.is_temporal())}


def partials(source: Any, plan: LogicalPlan, keys: Optional[Sequence[str]] = None) -> Any:
    """Aggregate an already prepared, row-filtered E0 relation (never E).

    ``keys`` requests a row grain; filter-level keys and all pivot dimensions
    are added automatically. Restrict E0 before calling for a child cache
    entry. The cube carries the partial MIN of each member-key sort column
    (:func:`minima_fields`, part of the cube's identity), no other. This
    expression does not promise materialization or a physical scan count.
    """
    if not supports(plan, source.schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    metrics = _partials(source, plan)
    minima = scalar_minima(source.schema(), minima_fields(plan))
    metrics.update({alias: source[name].min() for name, alias in minima.items()})
    keys = grain(plan, keys)
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


def _provider(raw: Any, plan: LogicalPlan, cube_for: Any) -> AggregateProvider:
    schema = raw.schema()
    # Every passing-group lookup is independently evaluated against E0,
    # never against the result of a preceding measure filter.
    passing = []
    for depth, filters in sorted(plan.filters_by_level.items()):
        for cells in sorted({f.cells for f in filters}):
            selected = [f for f in filters if f.cells == cells]
            keys = list(plan.rows[:depth]) + (list(plan.query.columns) if cells else [])
            cube = cube_for(keys, None)
            values = {f.measure: _merged(cube, next(i for i, m in enumerate(plan.query.measures)
                                                  if m.alias == f.measure),
                                        next(m.agg for m in plan.query.measures if m.alias == f.measure), None)
                      for f in selected}
            grouped = cube.group_by(keys).aggregate(**values)
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
        cube = restrict(cube_for(keys, restriction), restriction, schema)
        return eligible(cube) if plan.values_from == "E" else cube

    def aggregate(keys: Sequence[str], restriction: Restriction) -> Any:
        cube = records(keys, restriction)
        metrics = _merge(cube, plan, schema)
        return cube.group_by(list(keys)).aggregate(**metrics) if keys else cube.aggregate(**metrics)

    def member_min(keys: Sequence[str], restriction: Restriction, by: str, name: str) -> Any:
        cube = records(keys, restriction)
        return cube.group_by(list(keys)).aggregate(**{name: cube[scalar_minima(schema)[by]].min()})

    def cell_values(keys: Sequence[str], restriction: Restriction, entries: Sequence[Any]) -> Any:
        cube = records(keys, restriction)
        indices = {m.alias: i for i, m in enumerate(plan.query.measures)}
        aggs = {m.alias: m.agg for m in plan.query.measures}
        metrics = {f"__cell{len(keys)}_{n}": _merged(
            cube, indices[s.field], aggs[s.field], _member_condition(cube, schema, plan.query.columns, tuple(member)))
            for n, s, member in entries}
        return cube.group_by(list(keys)).aggregate(**metrics)

    def eligible_records(keys: Sequence[str], restriction: Restriction) -> Any:
        return eligible(restrict(cube_for(keys, restriction), restriction, schema))

    return AggregateProvider(raw, schema, eligible_records, aggregate, member_min, cell_values)


def _prepare(partial_provider: Any = None) -> Any:
    def prepare(e0: Any, plan: LogicalPlan) -> AggregateProvider:
        cubes = {}
        shared = partials(e0, plan) if partial_provider is None else None

        def cube_for(keys: Sequence[str], restriction: Restriction) -> Any:
            if shared is not None:
                return shared
            requested = tuple(grain(plan, keys))
            token = (requested, restriction)
            if token not in cubes:
                cubes[token] = partial_provider(plan, e0, requested, restriction)
            return cubes[token]

        return _provider(e0, plan, cube_for)
    return prepare


def from_partials(plan: LogicalPlan, raw_relation: Any, cube_relation: Any) -> Any:
    """Ordered whole tree from materialized E0 partials, without applying the window.

    ``cube_relation`` is either one sufficient cube or
    ``cube_for(keys, restriction) -> relation``. A callable must return E0
    partials at a grain containing :func:`grain(plan, keys)`; unrestricted
    requests must cover all E0, including measure-filter lookups. Raw data
    supplies schema/backend only; aggregate and sort work uses the cubes.
    """
    raw = prepared(raw_relation, plan.query)
    if not supports(plan, raw.schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    cube_for = cube_relation if callable(cube_relation) else lambda keys, restriction: cube_relation
    union, order = tree(plan, raw_relation, None, lambda e0, p: _provider(e0, p, cube_for))
    return ordered_tree(plan, raw_relation, union, order)


def lower(plan: LogicalPlan, relation: Any, partial_provider: Any = None) -> Any:
    """Lower with optional ``provider(plan, e0, grain, restriction)`` cache hook."""
    if not supports(plan, prepared(relation, plan.query).schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    return build(plan, relation, None, _prepare(partial_provider))


def tree_of(plan: LogicalPlan, relation: Any, partial_provider: Any = None) -> Tuple[Any, List[Any]]:
    if not supports(plan, prepared(relation, plan.query).schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    return tree(plan, relation, None, _prepare(partial_provider))
