"""Single execution boundary of the core.

resolve (Authority-scoped relation) → validate IR → logical plan → physical
strategy → verify scope on the final expression → execute → ResultFrame.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Callable, Optional

from .authority.scope import current_scope, run_in_executor_scoped
from .authority.verifier import verify_scoped
from .frame import ResultFrame
from .identity import CanonicalPathCodec, CanonicalScalarCodec
from .ir import PivotQuery
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
        "measures": [[m.alias, m.agg, m.field] for m in query.measures],
        "filters": [[p.field, p.op, scalar(p.value)] for p in query.filters],
        "measureFilters": [[f.level, f.measure, f.op, scalar(f.value)] for f in query.measure_filters],
        "sort": [[k.field, k.desc] for k in query.sort],
        "totals": query.totals,
        "expanded": None if query.expanded is None
        else sorted(CanonicalPathCodec.encode(list(p)) for p in query.expanded),
        "window": [query.window.offset, query.window.limit],
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":"))


def scope_fingerprint(dataset: str) -> str:
    grant = current_scope().grant_for(dataset)
    return str(getattr(grant, "scope_fingerprint", None) or "global")


def fingerprint(query: PivotQuery) -> str:
    """Plan identity: query meaning + the grant it runs under (REQ-SEC-004)."""
    return hashlib.sha256(f"{canonical(query)}|{scope_fingerprint(query.dataset)}".encode()).hexdigest()[:32]


def compile_query(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None) -> Any:
    """``strategy=None`` lets the physical planner choose."""
    relation = resolve(query.dataset)
    schema = relation.schema()
    query.validate({name: type_family(dtype) for name, dtype in schema.items()})
    plan = logical_plan(query)
    expr = STRATEGIES[strategy or choose(plan, schema)](plan, relation)
    verify_scoped(expr)
    return expr


def execute(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None) -> ResultFrame:
    expr = compile_query(query, resolve, strategy=strategy)
    return ResultFrame(
        table=expr.to_pyarrow(),
        rows=tuple(query.rows),
        measures=tuple(m.alias for m in query.measures),
        fingerprint=fingerprint(query),
        window=(query.window.offset, query.window.limit),
    )


async def execute_async(query: PivotQuery, resolve: Resolver, *, strategy: Optional[str] = None) -> ResultFrame:
    expr = compile_query(query, resolve, strategy=strategy)  # resolve + verify on the caller's context
    table = await run_in_executor_scoped(asyncio.get_running_loop(), None, expr.to_pyarrow)
    return ResultFrame(
        table=table,
        rows=tuple(query.rows),
        measures=tuple(m.alias for m in query.measures),
        fingerprint=fingerprint(query),
        window=(query.window.offset, query.window.limit),
    )
