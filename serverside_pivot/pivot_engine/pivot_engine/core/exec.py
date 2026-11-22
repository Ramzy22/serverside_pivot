"""Single execution boundary of the core.

resolve (Authority-scoped relation) → validate IR → logical plan → physical
strategy → verify scope on the final expression → execute → ResultFrame.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Callable, List, Optional, Tuple

from .authority.scope import current_scope, run_in_executor_scoped
from .authority.verifier import verify_scoped
from .frame import ResultFrame
from .identity import CanonicalPathCodec, CanonicalScalarCodec
from .ir import DetailQuery, DistinctQuery, PivotQuery, PredicateGroup, expr_to_wire
from .logical import plan as logical_plan
from .physical import STRATEGIES, choose
from .registry import type_family

__all__ = ["canonical", "compile_query", "execute", "execute_async", "fingerprint", "scope_fingerprint"]

Resolver = Callable[[str], Any]


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
        "measureFilters": [[f.level, f.measure, f.op, scalar(f.value)] for f in query.measure_filters],
        "sort": [[k.field, k.desc, k.absolute, *([k.by] if k.by else []),
                  *([CanonicalPathCodec.encode(list(k.member))] if k.member is not None else []),
                  *([{"semantic": k.semantic}] if k.semantic else [])]
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
                           [[m.label, m.field, m.order, *([m.agg, m.weight, m.q, m.order_by] if m.agg else [])]
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


def compile_query(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None) -> Any:
    """``strategy=None`` lets the physical planner choose."""
    from .physical.union_all import prepared

    relation = resolve(query.dataset)
    query.validate({name: type_family(dtype) for name, dtype in relation.schema().items()})
    relation = prepared(relation, query)  # REQ-DIM-001/002: strategies see the category and measure-axis columns
    schema = relation.schema()
    plan = logical_plan(query)
    expr = STRATEGIES[strategy or choose(plan, schema)](plan, relation)
    verify_scoped(expr)
    return expr


def execute(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None) -> ResultFrame:
    expr = compile_query(query, resolve, strategy=strategy)
    return ResultFrame(
        table=expr.to_pyarrow(),
        rows=tuple(query.rows),
        measures=tuple(query.value_columns()),
        fingerprint=fingerprint(query),
        window=(query.window.offset, query.window.limit),
    )


async def execute_async(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None) -> ResultFrame:
    expr = compile_query(query, resolve, strategy=strategy)  # resolve + verify on the caller's context
    table = await run_in_executor_scoped(asyncio.get_running_loop(), None, expr.to_pyarrow)
    return ResultFrame(
        table=table,
        rows=tuple(query.rows),
        measures=tuple(query.value_columns()),
        fingerprint=fingerprint(query),
        window=(query.window.offset, query.window.limit),
    )


def column_members(query: PivotQuery, resolve: Resolver, *, limit: Optional[int] = None,
                   offset: int = 0) -> Tuple[Tuple[Any, ...], ...]:
    """The ordered column catalog (REQ-PIV-001): distinct members over E0.

    Members are ordered by value per column, NULLS LAST; ``offset``/``limit``
    select a column window.  The result feeds ``PivotQuery.column_members``.
    """
    from .physical.union_all import filtered

    relation = resolve(query.dataset)
    schema = relation.schema()
    query.validate({name: type_family(dtype) for name, dtype in schema.items()})
    if not query.columns:
        return ()
    source = filtered(relation, query).select(*query.columns).distinct()
    order = [k for c in query.columns for k in (source[c].isnull().cast("int8"), source[c].asc())]
    expr = source.order_by(order)
    if limit is not None or offset:
        expr = expr.limit(limit, offset=offset)
    verify_scoped(expr)
    return tuple(tuple(r[c] for c in query.columns) for r in expr.to_pyarrow().to_pylist())


def detail(query: DetailQuery, resolve: Resolver) -> Tuple[Any, int]:
    """(Arrow page, total matching records) for a drill-through / detail query."""
    from .physical.detail import lower_detail

    relation = resolve(query.dataset)
    schema = relation.schema()
    query.validate({name: type_family(dtype) for name, dtype in schema.items()})
    page, count = lower_detail(query, relation)
    verify_scoped(page)
    verify_scoped(count)
    return page.to_pyarrow(), int(count.to_pyarrow().as_py())


def distinct_values(query: DistinctQuery, resolve: Resolver) -> Tuple[List[Any], int]:
    """(page of distinct values, total distinct values) for filter options."""
    from .physical.detail import lower_distinct

    relation = resolve(query.dataset)
    schema = relation.schema()
    query.validate({name: type_family(dtype) for name, dtype in schema.items()})
    page, count = lower_distinct(query, relation)
    verify_scoped(page)
    verify_scoped(count)
    return page.to_pyarrow().column(query.column).to_pylist(), int(count.to_pyarrow().as_py())


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
    row = expr.to_pyarrow().to_pylist()[0]
    return tuple(row[k] for k in ["__n", *(f"__h{i}" for i in range(len(columns)))])
