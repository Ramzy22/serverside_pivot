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

from ..ir import CASE_FOLDING_OPS, MEMBER_AGG, STRING_OPS, Predicate, PredicateGroup
from ..logical import Level, LogicalPlan
from ..registry import type_family
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
    if p.op in {"is_empty", "is_not_empty"}:  # REQ-NULL-004
        empty = col.isnull() | (col.strip() == "") if col.type().is_string() else col.isnull()
        return empty if p.op == "is_empty" else ~empty
    if p.op == "is_null":
        return col.isnull()
    if p.op == "is_not_null":
        return col.notnull()
    raise ValueError(f"unsupported operator {p.op!r}")


def _lower(m: Any, table: Any, plan: LogicalPlan, keys: Sequence[str], condition: Any = None) -> Any:
    """One measure at a level grouped by ``keys``; a per-member axis value is level-aware (REQ-DIM-003)."""
    if m.agg != MEMBER_AGG:
        return lower_measure(m, table, condition)
    axis = plan.query.unpivot
    assert axis is not None  # the IR admits MEMBER_AGG only with a per-member axis
    dtype = table[axis.value].type()
    if axis.members[0].agg == "array_agg":  # a series axis (REQ-AGG-009): one list type
        dtype = ibis.expr.datatypes.Array(dtype)
    if axis.label not in keys:  # above the label: the members' aggregations differ, no value
        # A reduction (not a literal), so a global aggregate still yields its one row over no records.
        first = lower_measure(axis.members[0].measure(), table, condition)
        return ibis.ifelse(ibis.literal(False), first.cast(dtype), ibis.null().cast(dtype))
    value = ibis.null().cast(dtype)
    for member in reversed(axis.members):
        is_member = table[axis.label] == ibis.literal(member.label, type="string")
        cond = is_member if condition is None else (is_member & condition)
        own = lower_measure(member.measure(), table, cond).cast(dtype)
        value = ibis.ifelse(is_member.any(), own, value)  # the group holds one member
    return value


def _measures(table: Any, plan: LogicalPlan, aliases: Sequence[str] = (), keys: Sequence[str] = ()) -> Dict[str, Any]:
    """Row-total measures, then one conditional aggregate per pivot cell (REQ-PIV-001)."""
    wanted = set(aliases) if aliases else None
    out = {}
    for m in plan.query.measures:
        if wanted is None or m.alias in wanted:
            out[m.alias] = _lower(m, table, plan, keys)
    if wanted is None:
        schema = table.schema()
        for _j, member, m, name in plan.query.cells():
            out[name] = _lower(m, table, plan, keys, _member_condition(table, schema, plan.query.columns, member))
    return out


def _member_condition(table: Any, schema: Any, columns: Sequence[str], member: Tuple[Any, ...]) -> Any:
    """Null-safe typed equality to one column member (a NULL member is the NULL group)."""
    cond = None
    for k, v in zip(columns, member, strict=True):
        term = table[k].isnull() if v is None else table[k].identical_to(ibis.literal(v, type=schema[k]))
        cond = term if cond is None else (cond & term)
    return cond


def with_categories(relation: Any, categories: Sequence[Any]) -> Any:
    """Attach the category columns (REQ-DIM-001), in order: the first matching rule's label, else the fallback.

    A rule condition has row-filter semantics (``_predicate``), so it never
    matches NULL unless it says so.  Idempotent: validation forbids a
    category named like an existing column.
    """
    for c in categories:
        if c.name in relation.columns:
            continue
        family = c.family()
        dtype = {"string": "string", "integer": "int64", "floating": "float64"}[family]
        expr = ibis.literal(c.fallback, type=dtype)
        for rule in reversed(c.rules):
            expr = ibis.ifelse(_predicate(relation, rule.when), ibis.literal(rule.label, type=dtype), expr)
        relation = relation.mutate(**{c.name: expr})
    return relation


def with_unpivot(relation: Any, unpivot: Any) -> Any:
    """The measure axis (REQ-DIM-002): one record per member; idempotent (its columns are new)."""
    if unpivot is None or unpivot.label in relation.columns:
        return relation
    schema = relation.schema()
    family = unpivot.value_family({name: type_family(dtype) for name, dtype in schema.items()})
    cast = "float64" if family == "floating" else ("string" if family == "string" else None)
    branches = []
    for m in unpivot.members:
        value = relation[m.field] if cast is None else relation[m.field].cast(cast)
        branch = relation.select(*[relation[c] for c in relation.columns],
                                 ibis.literal(m.label, type="string").name(unpivot.label),
                                 value.name(unpivot.value),
                                 ibis.literal(m.order, type="int64").name(unpivot.key))
        if unpivot.drop_nulls:
            branch = branch.filter(branch[unpivot.value].notnull())
        branches.append(branch)
    out = branches[0]
    for branch in branches[1:]:
        out = out.union(branch, distinct=False)
    return out


def prepared(relation: Any, query: Any) -> Any:
    """The relation the query sees: categories (REQ-DIM-001), then the measure axis (REQ-DIM-002)."""
    relation = with_categories(relation, getattr(query, "categories", ()))
    return with_unpivot(relation, getattr(query, "unpivot", None))


def filtered(relation: Any, query: Any) -> Any:
    """E0 before measure filters: the prepared relation with the row filters applied."""
    relation = prepared(relation, query)
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
        grouped = e0.group_by(keys).aggregate(**_measures(e0, plan, [f.measure for f in filters], keys))
        cond = None
        for f in filters:
            term = _CMP[f.op](grouped[f.measure], f.value)
            cond = term if cond is None else (cond & term)
        e = _semi_join_on_keys(e, grouped.filter(cond), keys)
    return e


def _direct(source: Any, plan: LogicalPlan) -> LevelAggregate:
    def aggregate(keys: Sequence[str]) -> Any:
        if keys:
            return source.group_by(list(keys)).aggregate(**_measures(source, plan, (), keys))
        return source.aggregate(**_measures(source, plan, (), keys))

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
    q = plan.query
    relation = prepared(relation, q)
    schema = relation.schema()
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
    union, member_key = _with_member_keys(union, source, plan)
    union, cell_sort = _with_cell_sorts(union, source, plan)
    if q.subtotals == "none":
        flat = _flat_order(union, plan, member_key, cell_sort)
        return _with_derived(union, flat, plan) if q.derived else (union, flat)
    order: List[Any] = []
    for i, k in enumerate(q.rows, start=1):
        below = (union[DEPTH] >= i).cast("int8")  # 1 for the descendants of a level-i group
        # REQ-LAYOUT-001: the group row first (pre-order) or after its descendants (post-order).
        order.append(below if q.subtotals == "before" else below.desc())
        dimension_sorted = False
        for n, (field, desc, is_measure, absolute) in enumerate(plan.sort):
            if is_measure and (i, n) in cell_sort:  # REQ-SORT-004: the ancestor's value in one column member
                value = union[cell_sort[(i, n)]]
            elif is_measure:
                value = union[ancestor[(i, field)]]
            elif field == k and k in member_key:  # REQ-SORT-003: the member's key; ties by member value
                value = union[member_key[k]]
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


def _flat_order(union: Any, plan: LogicalPlan, member_key: Dict[str, str],
                cell_sort: Dict[Tuple[int, int], str]) -> List[Any]:
    """REQ-LAYOUT-002: leaves ordered by the sort entries in precedence, then every row dimension; grand total last."""
    leaf = len(plan.rows)
    order: List[Any] = [(union[DEPTH] == 0).cast("int8")]
    for n, (field, desc, is_measure, absolute) in enumerate(plan.sort):
        if is_measure and (leaf, n) in cell_sort:
            value = union[cell_sort[(leaf, n)]]
        elif field in member_key:
            value = union[member_key[field]]
        else:
            value = union[field]
        if absolute:
            value = value.abs()
        order += [value.isnull().cast("int8"), value.desc() if desc else value.asc()]
    ordered = {field for field, _d, is_measure, absolute in plan.sort
               if not is_measure and not absolute and field not in member_key}
    for k in plan.rows:
        if k not in ordered:  # an entry already orders by this value: a tie-break adds nothing
            order += [union[k].isnull().cast("int8"), union[k].asc()]
    return order


def _with_member_keys(union: Any, source: Any, plan: LogicalPlan) -> Tuple[Any, Dict[str, str]]:
    """Attach MIN(key) over each node's level-i ancestor records, for dimensions ordered by a key (REQ-SORT-003)."""
    names: Dict[str, str] = {}
    for dim, by in plan.member_keys:
        i = plan.rows.index(dim) + 1
        keys = list(plan.rows[:i])
        name = names[dim] = f"__key{i}"
        lookup = source.group_by(keys).aggregate(**{name: source[by].min()})
        lookup = lookup.select(**{f"__kanc{i}_{j}": lookup[k] for j, k in enumerate(keys)}, **{name: lookup[name]})
        predicates = [union[k].identical_to(lookup[f"__kanc{i}_{j}"]) for j, k in enumerate(keys)]
        joined = union.left_join(lookup, predicates)
        union = joined.select(*[joined[c] for c in union.columns], joined[name])
    for dim, _kind in plan.semantic_keys:  # REQ-SORT-005: a key of the member value itself
        name = names[dim] = f"__key{plan.rows.index(dim) + 1}"
        union = union.mutate(**{name: tenor_days(union[dim])})
    return union, names


_WS = "[\t\n\f\r ]*"
TENOR_PATTERN = f"^{_WS}([0-9]+(?:\\.[0-9]+)?){_WS}([A-Za-z]){_WS}$"
ISO_PATTERN = f"^{_WS}([0-9]{{4}})-([0-9]{{2}})-([0-9]{{2}}){_WS}$"
MONTH_YEAR_PATTERN = f"^{_WS}([A-Za-z]+)[\t\n\f\r ]+([0-9]{{4}}){_WS}$"
_UNITS = {"d": 1.0, "w": 7.0, "m": 30.4375, "y": 365.25}
_MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")


def tenor_days(text: Any) -> Any:
    """REQ-SORT-005: days of a tenor (1D, 2W, 6M, 1Y), else of an ISO date, else of a month-year; else NULL."""
    def group(pattern: str, i: int) -> Any:
        return text.re_extract(pattern, i).nullif("")

    number = group(TENOR_PATTERN, 1).cast("float64")
    unit = group(TENOR_PATTERN, 2).lower()
    factor = ibis.null().cast("float64")
    for letter, days in reversed(list(_UNITS.items())):
        factor = ibis.ifelse(unit == letter, ibis.literal(days), factor)
    year, month, day = (group(ISO_PATTERN, i).cast("float64") for i in (1, 2, 3))
    iso = (year - 1970.0) * 365.25 + (month - 1.0) * 30.4375 + day
    my_year = group(MONTH_YEAR_PATTERN, 2).cast("float64")
    abbr = group(MONTH_YEAR_PATTERN, 1).lower().substr(0, 3)
    my_month = ibis.null().cast("float64")
    for n, name in reversed(list(enumerate(_MONTHS, start=1))):
        my_month = ibis.ifelse(abbr == name, ibis.literal(float(n)), my_month)
    month_year = (my_year - 1970.0) * 365.25 + (my_month - 1.0) * 30.4375
    return ibis.ifelse(number.notnull() & factor.notnull(), number * factor,
                       ibis.ifelse(year.notnull(), iso, ibis.ifelse(month_year.notnull(), month_year,
                                                                    ibis.null().cast("float64"))))


def _with_cell_sorts(union: Any, source: Any, plan: LogicalPlan) -> Tuple[Any, Dict[Tuple[int, int], str]]:
    """Attach, per level i, a measure's value within one column member for each cell sort entry (REQ-SORT-004).

    A conditional aggregate over the node's records, so a member outside the
    returned column window orders exactly like one inside it.
    """
    names: Dict[Tuple[int, int], str] = {}
    q = plan.query
    entries = [(n, s, s.member) for n, s in enumerate(q.sort) if s.member is not None]
    if not entries:
        return union, names
    schema = source.schema()
    measures = {m.alias: m for m in q.measures}
    for i in range(1, len(plan.rows) + 1):
        keys = list(plan.rows[:i])
        aggregates = {}
        for n, s, member in entries:
            names[(i, n)] = f"__cell{i}_{n}"
            condition = _member_condition(source, schema, q.columns, tuple(member))
            aggregates[names[(i, n)]] = _lower(measures[s.field], source, plan, keys, condition)
        lookup = source.group_by(keys).aggregate(**aggregates)
        lookup = lookup.select(**{f"__canc{i}_{j}": lookup[k] for j, k in enumerate(keys)},
                               **{name: lookup[name] for name in aggregates})
        predicates = [union[k].identical_to(lookup[f"__canc{i}_{j}"]) for j, k in enumerate(keys)]
        joined = union.left_join(lookup, predicates)
        union = joined.select(*[joined[c] for c in union.columns], *[joined[name] for name in aggregates])
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
