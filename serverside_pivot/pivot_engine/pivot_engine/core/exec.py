"""Single execution boundary of the core.

resolve (Authority-scoped relation) → validate IR → logical plan → physical
strategy → verify scope on the final expression → execute → ResultFrame.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from .authority.scope import current_scope, run_in_executor_scoped
from .authority.verifier import verify_scoped
from .cache import CacheKey, CacheManager, key_for
from .cube_cache import current_cube_access
from .frame import ResultFrame, split_window
from .identity import CanonicalPathCodec, CanonicalScalarCodec
from .ir import ColumnOrder, DetailQuery, DistinctQuery, MeasureRef, PivotQuery, PredicateGroup, Window, expr_to_wire
from .logical import plan as logical_plan
from .physical import STRATEGIES, check_capabilities, choose
from .physical.capabilities import refusing_untranslatable
from .registry import type_family

__all__ = ["canonical", "clear_compile_cache", "compile_query", "compiled_sql",
           "execute", "execute_async", "fingerprint", "materialize",
           "measure_expr", "record_predicate", "records", "scope_fingerprint", "statement_runner"]

Resolver = Callable[[str], Any]
StatementRunner = Callable[[Any], Any]

_RUNNER: ContextVar[Optional[StatementRunner]] = ContextVar("pivot_core_statement_runner", default=None)


def materialize(expr: Any) -> Any:
    """Run one verified core statement: the only place the core touches the database.

    A request binds a runner (``statement_runner``) that owns cancellation, the
    query deadline and the physical lease; without one the statement runs on
    the expression's own backend.  Returns what ``expr.to_pyarrow()`` returns
    (an Arrow table, or a scalar for a scalar expression).  A statement the
    backend cannot compile is a typed ``BackendCapabilityError`` before it runs.
    """
    check_capabilities(expr)
    runner = _RUNNER.get()
    with refusing_untranslatable(expr):
        if runner is not None:
            return runner(expr)
        from .physical.fetch import to_pyarrow
        access = current_cube_access()
        return to_pyarrow(expr, access.backend if access is not None else None)


@contextmanager
def statement_runner(runner: Optional[StatementRunner]) -> Iterator[None]:
    """Bind the runner every ``materialize`` in this context uses."""
    token = _RUNNER.set(runner)
    try:
        yield
    finally:
        _RUNNER.reset(token)


def canonical(query: PivotQuery) -> str:
    """Deterministic encoding of a query's meaning (stable across processes)."""

    def scalar(value: Any) -> Any:
        if isinstance(value, (list, tuple, set, frozenset)):
            items = [CanonicalScalarCodec.encode(v) for v in value]
            return sorted(items) if isinstance(value, (set, frozenset)) else items
        return CanonicalScalarCodec.encode(value)

    body = {
        "dataset": query.dataset,
        "rows": list(query.rows),
        "measures": [[m.alias, m.agg, m.field, m.weight, m.q, *([m.order_by] if m.order_by is not None else []),
                      *([expr_to_wire(m.expr)] if m.expr is not None else [])]
                     for m in query.measures],
        "filters": [_canonical_filter(f, scalar) for f in query.filters],
        "measureFilters": [[f.level, f.measure, f.op, scalar(f.value), *([True] if f.cells else [])]
                           for f in query.measure_filters],
        "sort": [[k.field, k.desc, k.absolute, *([k.by] if k.by else []),
                  *([CanonicalPathCodec.encode(list(k.member))] if k.member is not None else []),
                  *([{"semantic": k.semantic}] if k.semantic else []),
                  *([{"nullsFirst": True}] if k.nulls_first else [])]
                 for k in query.sort],
        "totals": query.totals,
        "expanded": None if query.expanded is None
        else sorted(CanonicalPathCodec.encode(list(p)) for p in query.expanded),
        "window": [query.window.offset, query.window.limit],
        "columns": list(query.columns),
        "derived": [[d.alias, d.func, d.of, d.of2, d.desc, None if d.frame is None else list(d.frame), d.null_value]
                    for d in query.derived],
        "columnMembers": [CanonicalPathCodec.encode(list(m)) for m in query.column_members],
    }
    if query.subtotals != "before":  # default omitted: existing plan identities are unchanged
        body["subtotals"] = query.subtotals
    if query.categories:  # REQ-DIM-001: the rules are part of the query's meaning
        body["categories"] = [[c.name, [[_canonical_filter(r.when, scalar), CanonicalScalarCodec.encode(r.label)]
                                        for r in c.rules], CanonicalScalarCodec.encode(c.fallback)]
                              for c in query.categories]
    if query.ragged is not None:  # REQ-DIM-005
        body["ragged"] = [list(query.ragged.fields),
                          None if query.ragged.missing is None else [scalar(v) for v in query.ragged.missing]]
    if query.unpivot is not None:  # REQ-DIM-002
        u = query.unpivot
        body["unpivot"] = [u.label, u.value, u.key, u.drop_nulls,
                           [[m.label, m.field, m.order, *([m.agg, m.weight, m.q, m.order_by] if m.agg else []),
                             *([expr_to_wire(m.expr)] if m.expr is not None else [])]
                            for m in u.members]]
    return json.dumps(body, sort_keys=True, separators=(",", ":"))


def _canonical_filter(f: Any, scalar: Callable[[Any], Any]) -> Any:
    if isinstance(f, PredicateGroup):
        return [f.op, [_canonical_filter(item, scalar) for item in f.items]]
    return [f.field, f.op, scalar(f.value), f.case_sensitive]


def scope_fingerprint(dataset: str) -> str:
    grant = current_scope().grant_for(dataset)
    return str(getattr(grant, "scope_fingerprint", None) or "global")


def fingerprint(query: PivotQuery) -> str:
    """Plan identity: query meaning + the grant it runs under (REQ-SEC-004)."""
    return hashlib.sha256(f"{canonical(query)}|{scope_fingerprint(query.dataset)}".encode()).hexdigest()[:32]


# The plan tier of the core cache (REQ-SEC-004): the key holds the query's meaning, the scope
# fingerprint and the policy version of the active grant; the entry holds the operation it was built
# from (the resolved relation, or the final expression) and its backend, and a hit requires that
# operation to be structurally equal to the current one (same table, schema and scope filter) on the
# very same backend object: Ibis compares connections by their parameters, so two in-memory DuckDB
# connections holding different data compare equal.  The entry keeps its backend alive, so an
# identity match can never be a reused address.  Building the
# same input twice gives the same expression, so a hit is exact.  Counted in entries.
_COMPILED = CacheManager(budgets={"plan": 512}, sizeof=lambda _entry: 1)


def clear_compile_cache() -> None:
    _COMPILED.invalidate(tiers=("plan",))


def _backends(op: Any) -> Tuple[int, ...]:
    return tuple(sorted({id(t.source) for t in op.find(_database_table())}))


def _cached(key: CacheKey, source: Any) -> Any:
    entry = _COMPILED.get(key)
    if entry is None or entry[0] != source or entry[1] != _backends(source):
        return None
    return entry[3]


def _remember(key: CacheKey, source: Any, value: Any) -> None:
    sources = tuple({id(t.source): t.source for t in source.find(_database_table())}.values())
    _COMPILED.put(key, (source, _backends(source), sources, value))


def _database_table() -> Any:
    from ibis.expr.operations import DatabaseTable

    return DatabaseTable


def compiled_sql(backend: Any, expr: Any) -> str:
    """``backend.compile(expr)``, memoized on the expression's structure."""
    from ibis.expr.operations import InMemoryTable

    op = expr.op()
    if op.find(InMemoryTable):
        # Request-owned Arrow buffers must never escape into the entry-counted compile tier.
        return str(backend.compile(expr))
    key = key_for("plan", "", f"sql:{backend.name}:{hash(op)}")
    sql = _cached(key, op)
    if sql is None:
        sql = str(backend.compile(expr))
        _remember(key, op, sql)
    return str(sql)


def compile_query(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None,
                  grand_total: bool = False) -> Any:
    """The read's one statement: the window, the tree's row count and (``grand_total``) the grand total.

    ``strategy=None`` lets the physical planner choose.  An unwindowed read
    without the grand total is the ordered tree (with its ``__ord`` column).
    """
    from .physical.union_all import windowed

    tree = _tree(query, resolve, strategy)
    expr = windowed(tree, query.window, grand_total=grand_total)
    if expr is not tree:
        verify_scoped(expr)
    return expr


def _tree(query: PivotQuery, resolve: Resolver, strategy: Optional[str]) -> Any:
    """The whole ordered tree of ``query`` (any window), from the plan tier of the compile cache."""
    from .physical.union_all import prepared

    relation = resolve(query.dataset)
    query.validate({name: type_family(dtype) for name, dtype in relation.schema().items()})
    whole = replace(query, window=Window())
    from .physical import leaf_partials

    plan = logical_plan(whole)
    selected = strategy or choose(plan, prepared(relation, query).schema())
    access = current_cube_access()
    if selected == "leaf_partials" and access is not None:
        tree = access.program(whole)
        if tree is not None:
            return tree
        tree = leaf_partials.lower(plan, relation, partial_provider=_partial_provider)
        verify_scoped(tree)
        access.store_program(whole, tree)
        return tree
    source = relation.op()
    key = key_for("plan", query.dataset, hashlib.sha256(f"{canonical(whole)}|{strategy}".encode()).hexdigest())
    tree = _cached(key, source)
    if tree is None:
        relation = prepared(relation, query)  # REQ-DIM-001/002: strategies see the category and measure-axis columns
        plan = logical_plan(whole)
        tree = STRATEGIES[strategy or choose(plan, relation.schema())](plan, relation)
        verify_scoped(tree)
        _remember(key, source, tree)
    return tree


def _partial_provider(plan: Any, e0: Any, grain: Sequence[str], restriction: Any) -> Any:
    """Materialize verified E0 partials, or read matching versioned partials under this request's grant."""
    import ibis

    from .physical import leaf_partials
    from .physical.arrow_relation import release
    from .physical.union_all import restrict

    access = current_cube_access()
    if access is None:
        raise RuntimeError("partial provider requires a bound cube access")
    # The grand total and roots share one complete root cube. Deeper levels are disjoint parent extensions.
    requested = tuple(dict.fromkeys([*plan.rows[:1], *grain]))

    def build(missing: Any) -> Any:
        expr = leaf_partials.partials(restrict(e0, missing, e0.schema()), plan, requested)
        verify_scoped(expr)
        return materialize(expr)

    table = access.get(plan.query, requested, restriction, build)
    token = id(table)
    relation = access.relations.get(token)
    if relation is None:
        relation = ibis.memtable(table)
        access.relations[token] = relation
        access.on_exit(lambda: release(access.backend, relation))
    return relation


def execute(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None,
            grand_total: bool = False) -> ResultFrame:
    """The window's rows, with ``total`` and (``grand_total``) the grand total: one statement on a backend
    with window functions, else the window, the count and the grand total read from the same tree."""
    from .physical.union_all import windowed

    tree = _tree(query, resolve, strategy)
    statements = _statements(tree, windowed(tree, query.window, grand_total=grand_total), query, grand_total)
    return _frame([materialize(s) for s in statements], query, grand_total)


async def execute_async(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None,
                        grand_total: bool = False) -> ResultFrame:
    from .physical.union_all import windowed

    tree = _tree(query, resolve, strategy)  # resolve + verify on the caller's context
    statements = _statements(tree, windowed(tree, query.window, grand_total=grand_total), query, grand_total)
    loop = asyncio.get_running_loop()
    return _frame([await run_in_executor_scoped(loop, None, materialize, s) for s in statements], query, grand_total)


def _statements(tree: Any, page: Any, query: PivotQuery, grand_total: bool) -> List[Any]:
    """The verified statements of one read: the page, then (a backend without window functions, windowed)
    the tree's row count and its grand total."""
    from .frame import DEPTH, ORD

    statements = [page]
    windowed = query.window.limit is not None or bool(query.window.offset)
    if ORD not in tree.columns and windowed:
        statements.append(tree.count())
        if grand_total:
            statements.append(tree.filter(tree[DEPTH] == 0))
    for statement in statements:
        if statement is not tree:
            verify_scoped(statement)
    return statements


def _frame(results: List[Any], query: PivotQuery, grand_total: bool) -> ResultFrame:
    window = (query.window.offset, query.window.limit)
    body, total, grand = split_window(results[0], window)
    if len(results) > 1:  # the count and grand total statements of a backend without window functions
        body, total = results[0], int(results[1].as_py())
        grand = results[2] if len(results) > 2 else grand
    return ResultFrame(
        table=body,
        rows=tuple(query.rows),
        measures=tuple(query.value_columns()),
        fingerprint=fingerprint(query),
        window=window,
        total=total,
        grand_total=grand if grand_total else None,
    )


def column_members(query: PivotQuery, resolve: Resolver, *, limit: Optional[int] = None,
                   offset: int = 0, order: Sequence[ColumnOrder] = ()) -> Tuple[Tuple[Any, ...], ...]:
    """The ordered column catalog (REQ-PIV-001, REQ-PIV-002): distinct members over E0.

    Members are ordered column by column: by the field's ``ColumnOrder``
    when ``order`` names it, else by value NULLS LAST; a measure-axis label by
    its member order (REQ-DIM-006).  ``offset``/``limit`` select a column
    window.  The result feeds ``PivotQuery.column_members``.
    """
    from .physical.union_all import DEPTH, _null_rank, filtered, tenor_days

    relation = resolve(query.dataset)
    schema = relation.schema()
    query.validate({name: type_family(dtype) for name, dtype in schema.items()})
    if not query.columns:
        return ()
    axis = query.unpivot
    keyed = axis is not None and axis.label in query.columns  # REQ-DIM-006: the label orders by member order
    extra = [axis.key] if keyed and axis is not None else []
    e0 = filtered(relation, query)
    access = current_cube_access()
    if access is not None:
        from .physical import leaf_partials

        plan = logical_plan(query)
        keyed_order = any(o.by is not None and o.field in query.columns for o in order)  # needs record minima
        if (not keyed_order and leaf_partials.supports(plan, e0.schema())
                and choose(plan, e0.schema()) == "leaf_partials"):
            e0 = _partial_provider(plan, e0, leaf_partials.grain(plan, query.rows[:1]), None)
    source = e0.select(*query.columns, *extra).distinct()
    by_field: Dict[str, ColumnOrder] = {o.field: o for o in order if o.field in query.columns}
    aliases = {m.alias for m in query.measures} | {d.alias for d in query.derived}

    def attach(source: Any, lookup: Any, field: str, name: str) -> Any:
        lookup = lookup.select(**{f"{name}_k": lookup[field], name: lookup[name]})
        joined = source.left_join(lookup, source[field].identical_to(lookup[f"{name}_k"]))
        return joined.select(*[joined[c] for c in source.columns], joined[name])

    keys: List[Tuple[str, Optional[str], ColumnOrder]] = []  # (field, key column or None, its order)
    for i, c in enumerate(query.columns):
        o = by_field.get(c)
        if o is None or (keyed and axis is not None and c == axis.label):
            continue
        name: Optional[str] = None
        if o.total is not None and o.total in aliases:
            # The member's own value: a one-level tree over E0 (no measure filters, no column pivot).
            one_level = replace(query, rows=(c,), columns=(), column_members=(), measure_filters=(), sort=(),
                                expanded=None, window=Window(), subtotals="before", ragged=None)
            tree = compile_query(one_level, resolve)
            name = f"__corder{i}"
            source = attach(source, tree.filter(tree[DEPTH] == 1).select(c, **{name: tree[o.total]}), c, name)
        elif o.by is not None and o.by in e0.columns:  # REQ-SORT-003; an unknown key column is ignored
            name = f"__corder{i}"
            source = attach(source, e0.group_by(c).aggregate(**{name: e0[o.by].min()}), c, name)
        elif o.semantic == "tenor":  # REQ-SORT-005
            name = f"__corder{i}"
            source = source.mutate(**{name: tenor_days(source[c])})
        keys.append((c, name, o))

    by_key = {c: (name, o) for c, name, o in keys}
    expr_order: List[Any] = []
    for c in query.columns:
        if keyed and axis is not None and c == axis.label:
            expr_order += [source[axis.key].isnull().ifelse(1, 0), source[axis.key].asc(), source[c].asc()]
            continue
        if c not in by_key:
            expr_order += [source[c].isnull().ifelse(1, 0), source[c].asc()]
            continue
        name, o = by_key[c]
        value = source[name] if name is not None else source[c]
        if o.absolute:
            value = value.abs()
        expr_order += [_null_rank(value, o.nulls_first), value.desc() if o.desc else value.asc()]
        if name is not None:  # ties: the member value ascending, NULLS LAST
            expr_order += [source[c].isnull().ifelse(1, 0), source[c].asc()]
    expr = source.order_by(expr_order)
    if limit is not None or offset:
        expr = expr.limit(limit, offset=offset)
    verify_scoped(expr)
    return tuple(tuple(r[c] for c in query.columns) for r in materialize(expr).to_pylist())


def detail(query: DetailQuery, resolve: Resolver) -> Tuple[Any, int]:
    """(Arrow page, total matching records) for a drill-through / detail query."""
    from .physical.detail import lower_detail

    relation = resolve(query.dataset)
    schema = relation.schema()
    query.validate({name: type_family(dtype) for name, dtype in schema.items()})
    page, count = lower_detail(query, relation)
    verify_scoped(page)
    verify_scoped(count)
    return materialize(page), int(materialize(count).as_py())


def records(query: DetailQuery, resolve: Resolver) -> Any:
    """The (unmaterialized) relation of the records a detail query selects, for record-level consumers
    (live aggregation): the same E0 the reads see, never a looser filter."""
    from .physical.detail import lower_records

    relation = resolve(query.dataset)
    query.validate({name: type_family(dtype) for name, dtype in relation.schema().items()})
    out = lower_records(query, relation)
    verify_scoped(out)
    return out


def record_predicate(relation: Any, f: Any) -> Any:
    """The boolean expression of one IR filter over ``relation`` (exact core semantics: NULLs, case, sets)."""
    from .physical.union_all import _predicate

    return _predicate(relation, f)


def measure_expr(relation: Any, m: MeasureRef, condition: Any = None) -> Any:
    """One registry aggregation over ``relation`` (records where ``condition`` holds), named by its alias."""
    from .physical.lowering import lower

    return lower(m, relation, condition).name(m.alias)


def distinct_values(query: DistinctQuery, resolve: Resolver) -> Tuple[List[Any], int]:
    """(page of distinct values, total distinct values) for filter options."""
    from .physical.detail import lower_distinct

    relation = resolve(query.dataset)
    schema = relation.schema()
    query.validate({name: type_family(dtype) for name, dtype in schema.items()})
    page, count = lower_distinct(query, relation)
    verify_scoped(page)
    verify_scoped(count)
    return materialize(page).column(query.column).to_pylist(), int(materialize(count).as_py())


def dataset_fingerprint(relation: Any) -> Tuple[Any, ...]:
    """(row count, per-column hash sums) of a relation: changes when its committed contents change.

    Used to stamp a comparison of two reads (shadow mode) so a write landing
    between them is detected instead of being mistaken for a semantic diff.
    """
    columns = list(relation.columns)
    expr = relation.aggregate(
        __n=relation.count(), **{f"__h{i}": (relation[c].hash() % 1000000007).sum() for i, c in enumerate(columns)}
    )
    verify_scoped(expr)
    row = materialize(expr).to_pylist()[0]
    return tuple(row[k] for k in ["__n", *(f"__h{i}" for i in range(len(columns)))])
