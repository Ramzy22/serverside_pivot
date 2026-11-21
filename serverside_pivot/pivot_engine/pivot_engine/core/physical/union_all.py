"""Reference physical strategy: one aggregate per level, typed UNION ALL.

Portable to every Ibis backend.  Other strategies (native GROUPING SETS, leaf
partials) must be result-equivalent to this one; tests/core enforce that.

Output columns: the row dimensions (NULL where a level is rolled up), one
column per measure alias, and ``__depth`` (0 = grand total).  Presence is
``i <= __depth``, so a NULL member and an absent level are distinguishable
(REQ-ID-002).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Sequence, Tuple

import ibis
import pyarrow as pa

from ..ir import Predicate
from ..logical import Level, LogicalPlan
from ..registry import REGISTRY

__all__ = ["DEPTH", "LevelAggregate", "build", "lower"]

# (keys) -> relation with ``keys`` + one column per measure alias.
LevelAggregate = Callable[[Sequence[str]], Any]

DEPTH = "__depth"
_CMP = {
    "=": lambda c, v: c == v,
    "!=": lambda c, v: c != v,
    ">": lambda c, v: c > v,
    ">=": lambda c, v: c >= v,
    "<": lambda c, v: c < v,
    "<=": lambda c, v: c <= v,
}


def _predicate(table: Any, p: Predicate) -> Any:
    col = table[p.field]
    if p.op in _CMP:
        if p.value is None:  # SQL: comparison with NULL is never true
            return ibis.literal(False)
        return _CMP[p.op](col, p.value)
    if p.op == "in":
        values = [v for v in p.value if v is not None]
        term = col.isin(values) if values else ibis.literal(False)
        return (term | col.isnull()) if any(v is None for v in p.value) else term
    if p.op == "not_in":
        values = [v for v in p.value if v is not None]
        term = col.notin(values) if values else col.notnull()
        return term & col.notnull() if any(v is None for v in p.value) else term
    if p.op == "is_null":
        return col.isnull()
    if p.op == "is_not_null":
        return col.notnull()
    raise ValueError(f"unsupported operator {p.op!r}")


def _measures(table: Any, plan: LogicalPlan, aliases: Sequence[str] = ()) -> Dict[str, Any]:
    wanted = set(aliases) if aliases else None
    out = {}
    for m in plan.query.measures:
        if wanted is None or m.alias in wanted:
            entry = REGISTRY.get(m.agg)
            out[m.alias] = entry.lower(None if m.field is None else table[m.field], table)
    return out


def _semi_join_on_keys(left: Any, right: Any, keys: Sequence[str]) -> Any:
    """``left ⋉ right`` on null-safe equality of ``keys`` (IS NOT DISTINCT FROM)."""
    if not keys:
        return left
    renamed = right.select(**{f"__k_{i}": right[k] for i, k in enumerate(keys)}).distinct()
    preds = [left[k].identical_to(renamed[f"__k_{i}"]) for i, k in enumerate(keys)]
    return left.semi_join(renamed, preds)


def _paths_table(paths: Sequence[Tuple[Any, ...]], keys: Sequence[str], schema: Any) -> Any:
    arrays = {
        k: pa.array([p[i] for p in paths], type=schema[k].to_pyarrow()) for i, k in enumerate(keys)
    }
    return ibis.memtable(pa.table(arrays) if paths else pa.table(arrays, schema=pa.schema(
        [(k, schema[k].to_pyarrow()) for k in keys])))


def _eligible(e0: Any, plan: LogicalPlan) -> Any:
    """E: records of E0 whose group passes at every filtered level (REQ-TOT-001)."""
    e = e0
    for level_no, filters in sorted(plan.filters_by_level.items()):
        keys = list(plan.rows[:level_no])
        grouped = e0.group_by(keys).aggregate(**_measures(e0, plan, [f.measure for f in filters]))
        cond = None
        for f in filters:
            term = _CMP[f.op](grouped[f.measure], f.value)
            cond = term if cond is None else (cond & term)
        e = _semi_join_on_keys(e, grouped.filter(cond), keys)
    return e


def _direct(source: Any, plan: LogicalPlan) -> LevelAggregate:
    def aggregate(keys: Sequence[str]) -> Any:
        if keys:
            return source.group_by(list(keys)).aggregate(**_measures(source, plan))
        return source.aggregate(**_measures(source, plan))

    return aggregate


def _level_rows(aggregate: LevelAggregate, eligible: Any, level: Level, plan: LogicalPlan, schema: Any) -> Any:
    keys = list(level.keys)
    rel = aggregate(keys)
    if plan.values_from == "E0" and keys:
        rel = _semi_join_on_keys(rel, eligible.select(keys), keys)  # shown iff E ∩ n ≠ ∅
    if level.visible_parents is not None:
        parent_keys = keys[:-1]
        parents = _paths_table(sorted(level.visible_parents, key=repr), parent_keys, schema)
        rel = _semi_join_on_keys(rel, parents, parent_keys)
    cols: Dict[str, Any] = {}
    for k in plan.rows:
        cols[k] = rel[k] if k in keys else ibis.null().cast(schema[k])
    for m in plan.query.measures:
        cols[m.alias] = rel[m.alias]
    cols[DEPTH] = ibis.literal(level.depth, type="int16")
    return rel.select(**cols)


def lower(plan: LogicalPlan, relation: Any) -> Any:
    """Return the Ibis expression for the flattened, ordered, windowed tree."""
    return build(plan, relation, _direct)


def build(plan: LogicalPlan, relation: Any, level_aggregate: Callable[[Any, LogicalPlan], LevelAggregate]) -> Any:
    """Shared skeleton: E0/E, one aggregate per level, typed UNION ALL, order, window."""
    schema = relation.schema()
    q = plan.query
    e0 = relation.filter([_predicate(relation, p) for p in q.filters]) if q.filters else relation
    e = _eligible(e0, plan)
    source = e if plan.values_from == "E" else e0
    aggregate = level_aggregate(source, plan)
    parts: List[Any] = [_level_rows(aggregate, e, level, plan, schema) for level in plan.levels]
    # Align measure types across levels (a global aggregate and a grouped one
    # may differ in nullability/width on some backends).
    target = parts[-1].schema()
    parts = [p.cast({m.alias: target[m.alias] for m in q.measures}) for p in parts]
    union = parts[0]
    for part in parts[1:]:
        union = union.union(part, distinct=False)
    order = []
    for i, k in enumerate(q.rows, start=1):
        present = (union[DEPTH] >= i).cast("int8")
        is_null = union[k].isnull().cast("int8")
        order += [present, is_null, union[k].desc() if plan.descending.get(k) else union[k].asc()]
    ordered = union.order_by(order) if order else union
    if q.window.limit is not None or q.window.offset:
        ordered = ordered.limit(q.window.limit, offset=q.window.offset)
    return ordered
