"""Backend capability check: a plan the backend cannot compile is a typed error.

Runs on the final expression before execution, so an aggregation a backend
lacks (e.g. ``median`` on SQLite) is answered with a stable, typed
:class:`BackendCapabilityError` naming the operations, never a raw driver or
compiler exception halfway through a request.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Tuple

import ibis.expr.operations as ops
from ibis.backends.sql import SQLBackend
from ibis.common.exceptions import OperationNotDefinedError, UnsupportedOperationError
from ibis.common.graph import Graph
from ibis.expr.operations.udf import AggUDF, ScalarUDF

__all__ = ["BackendCapabilityError", "backend_name", "check_capabilities", "native_null_safe_semi_join",
           "refusing_untranslatable", "window_functions"]

# Value nodes that shape a relation instead of computing a value: backends
# translate them as part of their parent and do not list them as operations.
# Builtin UDFs (round, ceil, ...) are compiled by name, so backends do not list them either; a
# non-SQL engine that cannot translate one is caught by the translation check below.
_STRUCTURAL = (ops.SortKey, ScalarUDF, AggUDF)

# Backends verified to run ``semi_join`` on ``IS NOT DISTINCT FROM`` predicates
# (tests/core/test_cross_backend.py). Every other backend gets the portable
# equality-only form, which is exact everywhere but ~35% slower on DuckDB.
_NATIVE_NULL_SAFE_SEMI_JOIN = frozenset({"duckdb"})


def _filtered_ordered_aggregate(node: Any) -> bool:
    return isinstance(node, (ops.First, ops.Last, ops.ArrayCollect)) and bool(node.order_by) and node.where is not None


# Gaps a backend reports as supported but fails on at execution time, each reproduced in
# tests/core/test_cross_backend.py.
# Polars (1.3 via Ibis 12): an ordered aggregate with a
# filter raises "expressions in 'sort_by' produced a different number of groups".
# SQLite (Ibis 12): sample variance/stddev are a Python aggregate whose finalize raises on a group
# with fewer than two values: a data-dependent failure, so the plan is refused for every input.
def _sample_dispersion(node: Any) -> bool:
    return isinstance(node, (ops.Variance, ops.StandardDev))


# Polars (1.3 via Ibis 12): a percentile pivot cell (a quantile of a member-conditional value) over the
# measure axis (a union of the member columns) raises "cannot evaluate two Series of different lengths".
def _conditional_quantile_over_union(node: Any) -> bool:
    if not isinstance(node, ops.Quantile):
        return False
    kinds = {type(n) for n in Graph.from_bfs(node.arg).keys()}
    return ops.IfElse in kinds and ops.Union in kinds


_KNOWN_GAPS: Dict[str, Tuple[Tuple[Callable[[Any], bool], str], ...]] = {
    "polars": ((_filtered_ordered_aggregate, "ordered aggregate with a filter (first/last/array_agg)"),
               (_conditional_quantile_over_union, "percentile pivot cell over the measure axis")),
    "sqlite": ((_sample_dispersion, "sample variance/stddev"),),
}


def backend_name(expr: Any) -> str:
    try:
        return str(expr._find_backend().name)
    except Exception:  # noqa: BLE001 - an unbound expression (memtables only)
        return ""


def native_null_safe_semi_join(expr: Any) -> bool:
    return backend_name(expr) in _NATIVE_NULL_SAFE_SEMI_JOIN


def window_functions(expr: Any) -> bool:
    """The backend runs ``ROW_NUMBER``/``COUNT`` over windows (an unbound expression: assumed, as DuckDB)."""
    try:
        backend = expr._find_backend()
    except Exception:  # noqa: BLE001 - an unbound expression (memtables only)
        return True
    has = getattr(backend, "has_operation", None)
    return has is None or all(has(op) for op in (ops.WindowFunction, ops.RowNumber, ops.CountStar))


class BackendCapabilityError(ValueError):
    """The plan uses operations the target backend does not implement."""

    code = "unsupported_query"

    def __init__(self, backend: str, operations: Tuple[str, ...]) -> None:
        self.backend = backend
        self.operations = operations
        super().__init__(f"backend {backend!r} does not support: {', '.join(operations)}")


def check_capabilities(expr: Any) -> None:
    try:
        backend = expr._find_backend()
    except Exception:  # noqa: BLE001 - an unbound expression has no backend to check against
        return
    has = getattr(backend, "has_operation", None)
    if has is None:
        return
    missing = sorted({type(node).__name__ for node in Graph.from_bfs(expr.op()).keys()
                      if isinstance(node, ops.Value) and not isinstance(node, _STRUCTURAL) and not has(type(node))})
    name = str(getattr(backend, "name", type(backend).__name__))
    gaps = _KNOWN_GAPS.get(name, ())
    if gaps:
        nodes = list(Graph.from_bfs(expr.op()).keys())
        missing = sorted(set(missing) | {label for test, label in gaps if any(test(n) for n in nodes)})
    if missing:
        raise BackendCapabilityError(name, tuple(missing))
    if not isinstance(backend, SQLBackend):
        # Non-SQL engines (Polars) reject some plans only while translating them; translating is cheap
        # and side-effect free, so a plan they cannot run is refused here instead of failing mid-request.
        try:
            backend.compile(expr)
        except Exception as exc:  # noqa: BLE001 - any translation failure is a capability gap
            raise BackendCapabilityError(name, (f"{type(exc).__name__}: {exc}"[:200],)) from exc


@contextmanager
def refusing_untranslatable(expr: Any) -> Iterator[None]:
    """Turn a SQL compiler's refusal (raised while translating, before anything runs) into the typed error.

    Some gaps are only known to the compiler, e.g. ordered aggregations on SQLite; these Ibis errors are
    raised during translation, never by the database.
    """
    try:
        yield
    except (OperationNotDefinedError, UnsupportedOperationError) as exc:
        raise BackendCapabilityError(backend_name(expr) or "unknown", (f"{type(exc).__name__}: {exc}"[:200],)) from exc
