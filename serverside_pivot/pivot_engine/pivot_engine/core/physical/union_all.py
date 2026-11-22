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

from ..ir import CASE_FOLDING_OPS, STRING_OPS, Predicate, PredicateGroup
from ..logical import Level, LogicalPlan
from .lowering import lower as lower_measure

__all__ = ["DEPTH", "LevelAggregate", "build", "filtered", "lower", "output_columns", "tree", "tree_of"]

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


def _predicate(table: Any, p: Any) -> Any:
    if isinstance(p, PredicateGroup):
        terms = [_predicate(table, item) for item in p.items]
        out = terms[0]
        for term in terms[1:]:
            out = (out & term) if p.op == "and" else (out | term)
        return out
    return _leaf_predicate(table, p)


def _leaf_predicate(table: Any, p: Predicate) -> Any:
    col = table[p.field]
    if p.op in STRING_OPS:
        text, needle = (col, p.value) if p.case_sensitive else (col.lower(), p.value.lower())
        if p.op == "contains":
            hit = text.contains(needle)
        elif p.op == "not_contains":
            hit = ~text.contains(needle)
        elif p.op == "starts_with":
            hit = text.startswith(needle)
        else:
            hit = text.endswith(needle)
        return col.notnull() & hit.fill_null(False)  # never matches NULL
    if p.op == "between":
        low, high = p.value
        if low is None or high is None:
            return ibis.literal(False)
        return col.notnull() & (col >= low) & (col <= high)
    if p.op in _CMP:
        if p.value is None:  # SQL: comparison with NULL is never true
            return ibis.literal(False)
        if (not p.case_sensitive and p.op in CASE_FOLDING_OPS and isinstance(p.value, str)
                and col.type().is_string()):
            return col.notnull() & _CMP[p.op](col.lower(), p.value.lower())
        return col.notnull() & _CMP[p.op](col, p.value)
    if p.op == "in":
        values = [v for v in p.value if v is not None]
        term = col.isin(values) if values else ibis.literal(False)
        return (term | col.isnull()) if any(v is None for v in p.value) else term
    if p.op == "not_in":  # set semantics (REQ-NULL-003): NULL stays unless listed
        values = [v for v in p.value if v is not None]
        null_listed = any(v is None for v in p.value)
        if not values:
            return col.notnull() if null_listed else ibis.literal(True)
        outside = col.notin(values).fill_null(False)
        return (outside & col.notnull()) if null_listed else (outside | col.isnull())
    if p.op == "is_null":
        return col.isnull()
    if p.op == "is_not_null":
        return col.notnull()
    raise ValueError(f"unsupported operator {p.op!r}")


def _measures(table: Any, plan: LogicalPlan, aliases: Sequence[str] = ()) -> Dict[str, Any]:
    """Row-total measures, then one conditional aggregate per pivot cell (REQ-PIV-001)."""
    wanted = set(aliases) if aliases else None
    out = {}
    for m in plan.query.measures:
        if wanted is None or m.alias in wanted:
            out[m.alias] = lower_measure(m, table)
    if wanted is None:
        schema = table.schema()
        for _j, member, m, name in plan.query.cells():
            out[name] = lower_measure(m, table, _member_condition(table, schema, plan.query.columns, member))
    return out


def _member_condition(table: Any, schema: Any, columns: Sequence[str], member: Tuple[Any, ...]) -> Any:
    """Null-safe typed equality to one column member (a NULL member is the NULL group)."""
    cond = None
    for k, v in zip(columns, member, strict=True):
        term = table[k].isnull() if v is None else table[k].identical_to(ibis.literal(v, type=schema[k]))
        cond = term if cond is None else (cond & term)
    return cond


def filtered(relation: Any, query: Any) -> Any:
    """E0 before measure filters: the scoped relation with the row filters applied."""
    return relation.filter([_predicate(relation, p) for p in query.filters]) if query.filters else relation


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
    for name in plan.query.aggregate_columns():
        cols[name] = rel[name]
    cols[DEPTH] = ibis.literal(level.depth, type="int16")
    return rel.select(**cols)


def lower(plan: LogicalPlan, relation: Any) -> Any:
    """Return the Ibis expression for the flattened, ordered, windowed tree."""
    return build(plan, relation, _direct)


def tree_of(plan: LogicalPlan, relation: Any) -> Tuple[Any, List[Any]]:
    """The unordered flattened tree and its total order key (for materialized paging)."""
    return tree(plan, relation, _direct)


def tree(
    plan: LogicalPlan, relation: Any, level_aggregate: Callable[[Any, LogicalPlan], LevelAggregate]
) -> Tuple[Any, List[Any]]:
    """Shared skeleton: E0/E, one aggregate per level, typed UNION ALL, and the order key."""
    schema = relation.schema()
    q = plan.query
    e0 = filtered(relation, q)
    e = _eligible(e0, plan)
    source = e if plan.values_from == "E" else e0
    aggregate = level_aggregate(source, plan)
    parts: List[Any] = [_level_rows(aggregate, e, level, plan, schema) for level in plan.levels]
    # Align measure types across levels (a global aggregate and a grouped one
    # may differ in nullability/width on some backends).
    target = parts[-1].schema()
    parts = [p.cast({name: target[name] for name in q.aggregate_columns()}) for p in parts]
    union = parts[0]
    for part in parts[1:]:
        union = union.union(part, distinct=False)
    union, ancestor = _with_ancestor_measures(union, parts, plan)
    order: List[Any] = []
    for i, k in enumerate(q.rows, start=1):
        order.append((union[DEPTH] >= i).cast("int8"))  # parent before its children
        dimension_sorted = False
        for field, desc, is_measure, absolute in plan.sort:
            if is_measure:
                value = union[ancestor[(i, field)]]
            elif field == k:
                value, dimension_sorted = union[k], True
            else:
                continue  # a dimension entry orders only its own level
            if absolute:
                value = value.abs()
            order += [value.isnull().cast("int8"), value.desc() if desc else value.asc()]
        if not dimension_sorted:  # deterministic tie-break: the member value, ascending
            order += [union[k].isnull().cast("int8"), union[k].asc()]
    if q.derived:
        return _with_derived(union, order, plan)
    return union, order


ORD = "__ord"


def _with_derived(union: Any, order: List[Any], plan: LogicalPlan) -> Tuple[Any, List[Any]]:
    """Derived measures over the visible tree; siblings share a parent partition (REQ-AGG-005/007)."""
    q = plan.query
    union = union.mutate(**{ORD: ibis.row_number().over(order_by=order)})  # display order, total
    parents = {f"__p{j}": ibis.ifelse(union[DEPTH] > j + 1, union[k], ibis.null().cast(union[k].type()))
               for j, k in enumerate(q.rows)}
    union = union.mutate(**parents)
    grand = sorted({d.of for d in q.derived if d.func == "percent_of_total"})
    if grand:
        totals = union.filter(union[DEPTH] == 0).select(**{f"__gt_{a}": union[a] for a in grand})
        union = union.cross_join(totals)
    partition = [union[DEPTH], *(union[name] for name in parents)]  # after the join: one relation
    exprs: Dict[str, Any] = {}
    for d in q.derived:
        value = union[d.of]
        if d.func == "ratio":
            den = union[d.of2]
            fallback = ibis.literal(None if d.null_value is None else float(d.null_value), type="float64")
            exprs[d.alias] = ibis.ifelse(den.isnull() | (den == 0), fallback,
                                         value.cast("float64") / den.cast("float64"))
        elif d.func == "percent_of_total":
            total = union[f"__gt_{d.of}"]
            exprs[d.alias] = ibis.ifelse(total.isnull() | (total == 0), ibis.null().cast("float64"),
                                         value.cast("float64") / total.cast("float64"))
        elif d.func in {"rank", "dense_rank"}:
            w = ibis.window(group_by=partition,
                            order_by=[value.isnull().cast("int8"), value.desc() if d.desc else value.asc()])
            ranked = (ibis.rank() if d.func == "rank" else ibis.dense_rank()).over(w) + 1
            exprs[d.alias] = ibis.ifelse(value.isnull(), ibis.null().cast("int64"), ranked.cast("int64"))
        else:
            frame = d.frame if d.frame is not None else (None, 0)
            w = ibis.window(group_by=partition, order_by=[union[ORD]], rows=frame)
            agg = value.sum() if d.func == "cumulative" else value.cast("float64").mean()
            exprs[d.alias] = agg.over(w)
    union = union.mutate(**exprs)
    keep = [c for c in union.columns if not c.startswith("__p") and not c.startswith("__gt_")]
    union = union.select(*keep)
    return union, [union[ORD]]


def output_columns(plan: LogicalPlan) -> List[str]:
    return [*plan.rows, *plan.query.value_columns(), DEPTH]


def _with_ancestor_measures(union: Any, parts: List[Any], plan: LogicalPlan) -> Tuple[Any, Dict[Tuple[int, str], str]]:
    """Attach, for each level i, the sorted measures of the row's level-i ancestor (REQ-SORT-001)."""
    measures = [field for field, _desc, is_measure, _abs in plan.sort if is_measure]
    names: Dict[Tuple[int, str], str] = {}
    if not measures:
        return union, names
    by_depth = {level.depth: part for level, part in zip(plan.levels, parts, strict=True)}
    for i in range(1, len(plan.rows) + 1):
        part = by_depth.get(i)
        keys = list(plan.rows[:i])
        if part is None:  # the level shows no node: nothing to order there
            for alias in measures:
                names[(i, alias)] = f"__sort{i}_{alias}"
                union = union.mutate(**{names[(i, alias)]: ibis.null().cast(part_type(parts, alias))})
            continue
        renamed = {f"__anc{i}_{j}": part[k] for j, k in enumerate(keys)}
        for alias in dict.fromkeys(measures):
            names[(i, alias)] = f"__sort{i}_{alias}"
            renamed[names[(i, alias)]] = part[alias]
        ancestors = part.select(**renamed)
        predicates = [union[k].identical_to(ancestors[f"__anc{i}_{j}"]) for j, k in enumerate(keys)]
        joined = union.left_join(ancestors, predicates)
        union = joined.select(*[joined[c] for c in union.columns],
                              *[joined[names[(i, a)]] for a in dict.fromkeys(measures)])
    return union, names


def part_type(parts: List[Any], alias: str) -> Any:
    return parts[-1].schema()[alias]


def build(plan: LogicalPlan, relation: Any, level_aggregate: Callable[[Any, LogicalPlan], LevelAggregate]) -> Any:
    """Ordered and windowed tree."""
    union, order = tree(plan, relation, level_aggregate)
    q = plan.query
    ordered = (union.order_by(order) if order else union).select(*output_columns(plan))
    if q.window.limit is not None or q.window.offset:
        ordered = ordered.limit(q.window.limit, offset=q.window.offset)
    return ordered
