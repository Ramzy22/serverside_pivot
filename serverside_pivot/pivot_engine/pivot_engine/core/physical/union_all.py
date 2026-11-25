"""Reference physical strategy: one aggregate per level, typed UNION ALL.

Portable to every Ibis backend.  Other strategies (native GROUPING SETS, leaf
partials) must be result-equivalent to this one; tests/core enforce that.

Output columns: the row dimensions (NULL where a level is rolled up), one
column per measure alias, and ``__depth`` (0 = grand total).  Presence is
``i <= __depth``, so a NULL member and an absent level are distinguishable
(REQ-ID-002).
"""

from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import ibis
import pyarrow as pa

from ..ir import CASE_FOLDING_OPS, FORMULA_AGG, MEMBER_AGG, STRING_OPS, Predicate, PredicateGroup
from ..logical import Level, LogicalPlan
from ..registry import type_family
from .capabilities import native_null_safe_semi_join
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


@ibis.udf.scalar.builtin(name="round")  # type: ignore[untyped-decorator]  # Ibis decorators are untyped
def _round(x: float, digits: int) -> float:  # type: ignore[empty-body]
    """SQL ROUND on a double: half away from zero, a double result (Ibis would cast round(x, 0) to BIGINT)."""


@ibis.udf.scalar.builtin(name="ceil")  # type: ignore[untyped-decorator]  # Ibis decorators are untyped
def _ceil(x: float) -> float:  # type: ignore[empty-body]
    """SQL CEIL on a double, a double result."""


@ibis.udf.scalar.builtin(name="floor")  # type: ignore[untyped-decorator]  # Ibis decorators are untyped
def _floor(x: float) -> float:  # type: ignore[empty-body]
    """SQL FLOOR on a double, a double result."""


@ibis.udf.scalar.builtin(name="trunc")  # type: ignore[untyped-decorator]  # Ibis decorators are untyped
def _trunc(x: float) -> float:  # type: ignore[empty-body]
    """SQL TRUNC on a double (toward zero), a double result."""


_NOT_CONSTANT = object()


def _constant(e: Any) -> Any:
    """A formula subtree without measure references, evaluated once (``None`` is NULL), else ``_NOT_CONSTANT``.

    Only operators with exact IEEE-double semantics are folded; ``round`` stays with the backend's
    own ROUND. Folding also sidesteps a Polars engine panic on a NULL-guarded min/max of constants
    inside an aggregation (REQ-BACK-004).
    """
    if e.op == "num":
        return float(e.args[0])
    if e.op == "null":
        return None
    if e.op not in {"+", "-", "*", "/", "neg", "abs", "min", "max", "ceil", "floor", "trunc", "pow"}:
        return _NOT_CONSTANT
    xs = [_constant(a) for a in (e.args[:1] if e.op == "pow" else e.args)]
    if any(x is _NOT_CONSTANT for x in xs):
        return _NOT_CONSTANT
    if any(x is None for x in xs):
        return None
    x = xs[0]
    if e.op == "pow":
        exponent = float(e.args[1])
        if (exponent != int(exponent) and x < 0) or (exponent < 0 and x == 0):
            return None
        try:
            return math.pow(x, exponent)
        except OverflowError:
            return math.copysign(math.inf, x) if exponent == int(exponent) and int(exponent) % 2 else math.inf
    if e.op in {"ceil", "floor", "trunc"}:
        if not math.isfinite(x):
            return x
        if e.op == "ceil":
            return float(math.ceil(x))
        return float(math.floor(x) if e.op == "floor" else math.trunc(x))
    if e.op == "+":
        return x + xs[1]
    if e.op == "-":
        return x - xs[1]
    if e.op == "*":
        return x * xs[1]
    if e.op == "/":
        return None if xs[1] == 0 else x / xs[1]
    if e.op == "neg":
        return -x
    if e.op == "abs":
        return abs(x)
    return min(xs) if e.op == "min" else max(xs)


def _formula(e: Any, table: Any, plan: LogicalPlan, keys: Sequence[str], condition: Any,
             cell: Optional[Tuple[Any, ...]] = None) -> Any:
    """REQ-AGG-010: a formula over the node's own aggregates; a NULL input or an undefined result is NULL."""
    null = ibis.null().cast("float64")
    constant = _constant(e)
    if constant is not _NOT_CONSTANT:
        return null if constant is None else ibis.literal(constant, type="float64")
    args = e.args
    if e.op == "null":
        return null
    if e.op == "num":
        return ibis.literal(float(args[0]), type="float64")
    if e.op == "ref":
        m = next(x for x in plan.query.measures if x.alias == args[0])
        return _lower(m, table, plan, keys, condition, cell).cast("float64")
    if e.op in {"round", "pow"}:
        x = _formula(args[0], table, plan, keys, condition, cell)
        if e.op == "round":
            return _round(x, ibis.literal(int(args[1]), type="int32"))
        exponent = float(args[1])
        if exponent != int(exponent):  # a negative base has no real power
            x = ibis.ifelse(x < 0, null, x)
        if exponent < 0:
            x = ibis.ifelse(x == 0, null, x)
        return x.pow(ibis.literal(exponent, type="float64"))
    xs = [_formula(a, table, plan, keys, condition, cell) for a in args]
    if e.op == "+":
        return xs[0] + xs[1]
    if e.op == "-":
        return xs[0] - xs[1]
    if e.op == "*":
        return xs[0] * xs[1]
    if e.op == "/":
        return ibis.ifelse(xs[1] == 0, null, xs[0] / xs[1])
    if e.op == "neg":
        return -xs[0]
    if e.op == "abs":
        return xs[0].abs()
    if e.op == "ceil":
        return _ceil(xs[0])
    if e.op == "floor":
        return _floor(xs[0])
    if e.op == "trunc":
        return _trunc(xs[0])
    any_null = xs[0].isnull()
    for x in xs[1:]:
        any_null = any_null | x.isnull()
    pick = ibis.least(*xs) if e.op == "min" else ibis.greatest(*xs)
    return ibis.ifelse(any_null, null, pick)


def _lower(m: Any, table: Any, plan: LogicalPlan, keys: Sequence[str], condition: Any = None,
           cell: Optional[Tuple[Any, ...]] = None) -> Any:
    """``cell``: the column member of a pivot cell (``condition`` selects it); it fixes a column label's member."""
    """One measure at a level grouped by ``keys``; a per-member axis value is level-aware (REQ-DIM-003)."""
    if m.agg == FORMULA_AGG:
        value = _formula(m.expr, table, plan, keys, condition, cell)
        return ibis.ifelse(value.isnan() | value.isinf(), ibis.null().cast("float64"), value)
    if m.agg != MEMBER_AGG:
        return lower_measure(m, table, condition)
    axis = plan.query.unpivot
    if axis is None:
        raise RuntimeError("the IR admits MEMBER_AGG only with a per-member axis")
    dtype = table[axis.value].type()
    if axis.members[0].agg == "array_agg":  # a series axis (REQ-AGG-009): one list type
        dtype = ibis.expr.datatypes.Array(dtype)
    fixed = None  # REQ-DIM-006: a cell fixes the member when the label is a column dimension
    if cell is not None and axis.label in plan.query.columns:
        fixed = cell[list(plan.query.columns).index(axis.label)]
        keys = [*keys, axis.label]
    if axis.label not in keys:  # above the label: the members' aggregations differ, no value
        # A reduction (not a literal), so a global aggregate still yields its one row over no records.
        first = lower_measure(axis.members[0].measure(), table, condition)
        return ibis.ifelse(ibis.literal(False), first.cast(dtype), ibis.null().cast(dtype))
    value = ibis.null().cast(dtype)
    for member in reversed(axis.members):
        if fixed is not None and member.label != fixed:
            continue
        is_member = table[axis.label] == ibis.literal(member.label, type="string")
        cond = is_member if condition is None else (is_member & condition)
        if member.agg == FORMULA_AGG:  # REQ-DIM-007: the formula on the member's records
            f = _formula(member.expr, table, plan, keys, cond, cell)
            own = ibis.ifelse(f.isnan() | f.isinf(), ibis.null().cast("float64"), f).cast(dtype)
        else:
            own = lower_measure(member.measure(), table, cond).cast(dtype)
        if fixed is not None:  # an empty cell is the member's own aggregation over no records
            return own
        value = ibis.ifelse(is_member.any(), own, value)  # the node holds one member
    if fixed is not None:  # a label naming no member: no records, no value
        none = ibis.null().cast(dtype)
        return ibis.ifelse(table[axis.key].count() < 0, none, none)  # a reduction: a global aggregate keeps its row
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
        for _j, member, m, name in plan.query.cells():  # a cell fixes its column members (REQ-DIM-006)
            out[name] = _lower(m, table, plan, keys,
                               _member_condition(table, schema, plan.query.columns, member), tuple(member))
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
        if m.field is None:  # REQ-DIM-007: a formula member carries the records, no source value
            value = ibis.null().cast(cast or "float64")
        else:
            value = relation[m.field] if cast is None else relation[m.field].cast(cast)
        branch = relation.select(*[relation[c] for c in relation.columns],
                                 ibis.literal(m.label, type="string").name(unpivot.label),
                                 value.name(unpivot.value),
                                 ibis.literal(m.order, type="int64").name(unpivot.key))
        if unpivot.drop_nulls and m.field is not None:
            branch = branch.filter(branch[unpivot.value].notnull())
        branches.append(branch)
    out = branches[0]
    for branch in branches[1:]:
        out = out.union(branch, distinct=False)
    return out


def with_ragged(relation: Any, ragged: Any) -> Any:
    """REQ-DIM-005: per position i, the i-th present value, the index of its field, and the path length."""
    if ragged is None or "__ragn" in relation.columns:
        return relation
    schema = relation.schema()
    dtypes = {schema[f] for f in ragged.fields}
    first = schema[ragged.fields[0]]
    widened = "int64" if first.is_integer() else "float64" if first.is_floating() else (
        "string" if first.is_string() else None)
    dtype = first if len(dtypes) == 1 else widened
    if dtype is None:
        raise ValueError(f"ragged hierarchy fields of different types: {sorted(map(str, dtypes))}")
    present = []
    for f in ragged.fields:
        col = relation[f]
        ok = col.notnull()
        if schema[f].is_floating():
            ok = ok & ~col.isnan()
        if ragged.missing is None:
            if schema[f].is_string():
                ok = ok & (col != "")
        else:
            ok = ok & ~col.isin(list(ragged.missing))
        present.append(ok.fill_null(False))
    counts, running = [], ibis.literal(0, type="int64")
    for ok in present:
        running = running + ok.cast("int64")
        counts.append(running)
    cols: Dict[str, Any] = {}
    for i in range(1, len(ragged.fields) + 1):
        value = ibis.null().cast(dtype)
        index = ibis.null().cast("int64")
        for j in reversed(range(len(ragged.fields))):
            hit = present[j] & (counts[j] == i)
            value = ibis.ifelse(hit, relation[ragged.fields[j]].cast(dtype), value)
            index = ibis.ifelse(hit, ibis.literal(j, type="int64"), index)
        cols[f"__rag{i}"] = value
        cols[f"__ragf{i}"] = index
    cols["__ragn"] = counts[-1]
    return relation.mutate(**cols)


def prepared(relation: Any, query: Any) -> Any:
    """The relation the query sees: categories (REQ-DIM-001), the measure axis (REQ-DIM-002), then a ragged
    hierarchy (REQ-DIM-005)."""
    relation = with_categories(relation, getattr(query, "categories", ()))
    relation = with_unpivot(relation, getattr(query, "unpivot", None))
    return with_ragged(relation, getattr(query, "ragged", None))


def filtered(relation: Any, query: Any) -> Any:
    """E0 before measure filters: the prepared relation with the row filters applied."""
    relation = prepared(relation, query)
    return relation.filter([_predicate(relation, p) for p in query.filters]) if query.filters else relation


def _semi_join_on_keys(left: Any, right: Any, keys: Sequence[str]) -> Any:
    """``left ⋉ right`` on null-safe equality of ``keys`` (IS NOT DISTINCT FROM)."""
    if not keys:
        return left
    renamed = right.select(**{f"__k_{i}": right[k] for i, k in enumerate(keys)}).distinct()
    if native_null_safe_semi_join(left):
        preds = [left[k].identical_to(renamed[f"__k_{i}"]) for i, k in enumerate(keys)]
        return left.semi_join(renamed, preds)
    # Portable exact form: null-safe equality as two plain equalities, (k IS NULL, COALESCE(k, zero)),
    # joined against the distinct keys, so each left row matches at most once (a semi join).
    pairs = renamed.select(
        **{f"__n_{i}": renamed[f"__k_{i}"].isnull() for i in range(len(keys))},
        **{f"__v_{i}": renamed[f"__k_{i}"].coalesce(_zero(renamed[f"__k_{i}"].type())) for i in range(len(keys))},
    )
    preds = []
    for i, k in enumerate(keys):
        preds += [left[k].isnull() == pairs[f"__n_{i}"], left[k].coalesce(_zero(left[k].type())) == pairs[f"__v_{i}"]]
    return left.inner_join(pairs, preds).select(*left.columns)


def _zero(dtype: Any) -> Any:
    """A non-NULL literal of ``dtype`` (its value is irrelevant: the NULL flag disambiguates it)."""
    if dtype.is_string():
        return ibis.literal("")
    if dtype.is_boolean():
        return ibis.literal(False)
    if dtype.is_timestamp():
        return ibis.literal("1970-01-01T00:00:00").cast(dtype)
    if dtype.is_date():
        return ibis.literal("1970-01-01").cast(dtype)
    return ibis.literal(0).cast(dtype)


def _paths_table(paths: Sequence[Tuple[Any, ...]], keys: Sequence[str], schema: Any) -> Any:
    arrays = {
        k: pa.array([p[i] for p in paths], type=schema[k].to_pyarrow()) for i, k in enumerate(keys)
    }
    return ibis.memtable(pa.table(arrays) if paths else pa.table(arrays, schema=pa.schema(
        [(k, schema[k].to_pyarrow()) for k in keys])))


def _eligible(e0: Any, plan: LogicalPlan) -> Any:
    """E: records of E0 whose group passes at every filtered level (REQ-TOT-001)."""
    e = e0
    ragged = plan.query.ragged is not None
    for level_no, level_filters in sorted(plan.filters_by_level.items()):
        for cells in sorted({f.cells for f in level_filters}):
            filters = [f for f in level_filters if f.cells == cells]
            row_keys = list(plan.rows[:level_no])
            keys = row_keys + list(plan.query.columns) if cells else row_keys  # REQ-DIM-006: a cell's grain
            # REQ-DIM-005: only records with a node at this level are filtered by it
            groups = e0.filter(e0[row_keys[-1]].notnull()) if ragged else e0
            grouped = groups.group_by(keys).aggregate(**_measures(groups, plan, [f.measure for f in filters], keys))
            cond = None
            for f in filters:
                term = _CMP[f.op](grouped[f.measure], f.value)
                cond = term if cond is None else (cond & term)
            passing = _semi_join_on_keys(e, grouped.filter(cond), keys)
            e = passing.union(e.filter(e[row_keys[-1]].isnull()), distinct=False) if ragged else passing
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
    if plan.query.ragged is not None and keys:  # REQ-DIM-005: a path that has ended is no node
        rel = rel.filter(rel[keys[-1]].notnull())
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
        for n, (field, desc, is_measure, absolute, nulls_first) in enumerate(plan.sort):
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
            order += [_null_rank(value, nulls_first), value.desc() if desc else value.asc()]
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
    measures = [field for field, _desc, is_measure, _abs, _nf in plan.sort if is_measure]
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


def _null_rank(value: Any, nulls_first: bool) -> Any:
    """The NULL position key of a sort entry: NULLS LAST (REQ-BACK-001), or first (REQ-SORT-006)."""
    return (value.notnull() if nulls_first else value.isnull()).cast("int8")


def _flat_order(union: Any, plan: LogicalPlan, member_key: Dict[str, str],
                cell_sort: Dict[Tuple[int, int], str]) -> List[Any]:
    """REQ-LAYOUT-002: leaves ordered by the sort entries in precedence, then every row dimension; grand total last."""
    leaf = len(plan.rows)
    order: List[Any] = [(union[DEPTH] == 0).cast("int8")]
    for n, (field, desc, is_measure, absolute, nulls_first) in enumerate(plan.sort):
        if is_measure and (leaf, n) in cell_sort:
            value = union[cell_sort[(leaf, n)]]
        elif field in member_key:
            value = union[member_key[field]]
        else:
            value = union[field]
        if absolute:
            value = value.abs()
        order += [_null_rank(value, nulls_first), value.desc() if desc else value.asc()]
    ordered = {field for field, _d, is_measure, absolute, nulls_first in plan.sort
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

    # Simple CASE (``x WHEN a ...``) and a flat searched CASE keep the SQL shallow: nested IFs repeat
    # each regex extraction per branch and overflow parsers with a fixed stack (SQLite).
    null = ibis.null().cast("float64")
    number = group(TENOR_PATTERN, 1).cast("float64")
    factor = group(TENOR_PATTERN, 2).lower().cases(*((u, ibis.literal(d)) for u, d in _UNITS.items()), else_=null)
    year, month, day = (group(ISO_PATTERN, i).cast("float64") for i in (1, 2, 3))
    iso = (year - 1970.0) * 365.25 + (month - 1.0) * 30.4375 + day
    my_year = group(MONTH_YEAR_PATTERN, 2).cast("float64")
    my_month = group(MONTH_YEAR_PATTERN, 1).lower().substr(0, 3).cases(
        *((name, ibis.literal(float(n))) for n, name in enumerate(_MONTHS, start=1)), else_=null)
    month_year = (my_year - 1970.0) * 365.25 + (my_month - 1.0) * 30.4375
    tenor = number * factor
    return ibis.cases((tenor.notnull(), tenor), (year.notnull(), iso), (month_year.notnull(), month_year), else_=null)


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
            aggregates[names[(i, n)]] = _lower(measures[s.field], source, plan, keys, condition, tuple(member))
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
