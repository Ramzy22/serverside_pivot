"""Row-scope enforcement attached to dataset access (REQ-SEC-001..004).

Design
------
A request entry point (runtime router, adapter public methods, REST, Flight,
subscriptions) opens :func:`scoped_execution` with the server-issued
``RowScopeGrant`` and the Ibis connections it may touch.  This:

1. installs a *scope guard* on each connection's ``table()``/``sql()``/
   ``raw_sql()`` (idempotent).  Every relation read for a protected table is
   returned as ``base.filter(<exact scope predicate>)`` — callers can no
   longer obtain the unscoped base relation;
2. marks the table as protected **on that connection**.  From then on, any
   resolution of it with no grant in the current context (for example a worker
   thread that lost the ``contextvars`` context) raises
   :class:`UnscopedAccessError` — fail closed, never fail open;
3. refuses raw SQL that mentions a protected table while a scope is active
   (raw SQL cannot be proven scoped).

The scope predicate is compiled here, independently of the legacy filter
builder: it compares exactly (case-sensitive), converts values to the column
type without loss, and treats an unknown scope column as an error.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import re
import threading
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Callable, Dict, FrozenSet, Iterable, Iterator, Mapping, Optional, Tuple

__all__ = [
    "bind_request_scope",
    "PathNotProvenError",
    "ScopeColumnError",
    "ScopeContext",
    "ScopeValueError",
    "UnscopedAccessError",
    "compile_scope_predicate",
    "current_scope",
    "install_scope_guard",
    "is_protecting",
    "unguarded_table",
    "require_proven_path",
    "row_scoped",
    "scoped_execution",
    "submit_with_scope",
    "wrap_with_scope",
]


class ScopeError(PermissionError):
    """Base class: every scope failure is an authorization failure."""


class UnscopedAccessError(ScopeError):
    """A protected dataset was reached without a provable row grant."""


class ScopeColumnError(ScopeError):
    """A grant references a column the dataset does not have."""


class ScopeValueError(ScopeError):
    """A grant value cannot be represented exactly in the column type."""


class PathNotProvenError(ScopeError):
    """An execution path has not been proven scoped for protected datasets."""


def _bare(name: Any) -> str:
    text = str(name or "").strip().strip('"')
    return text.rsplit(".", 1)[-1].strip('"').lower()


def _grant_fields(grant: Any) -> Mapping[str, Tuple[Any, ...]]:
    values = getattr(grant, "field_values", None)
    return values if isinstance(values, Mapping) else {}


def _is_protecting(grant: Any) -> bool:
    return bool(_grant_fields(grant))


is_protecting = _is_protecting


@dataclass(frozen=True)
class ScopeContext:
    """Grants active for the current logical request."""

    grants: Mapping[str, Any] = field(default_factory=dict)  # bare table name -> grant
    proven_paths: FrozenSet[str] = frozenset()

    def grant_for(self, table: Any) -> Any:
        return self.grants.get(_bare(table))

    @property
    def active(self) -> bool:
        return any(_is_protecting(g) for g in self.grants.values())


_EMPTY_SCOPE = ScopeContext(grants=MappingProxyType({}))
# The default is immutable (frozen dataclass over a read-only mapping).
_CURRENT: contextvars.ContextVar[ScopeContext] = contextvars.ContextVar(  # noqa: B039
    "pivot_scope_context", default=_EMPTY_SCOPE
)


def current_scope() -> ScopeContext:
    return _CURRENT.get()


# --------------------------------------------------------------------- predicate


def _coerce_exact(value: Any, dtype: Any, field_name: str) -> Any:
    if value is None:
        return None
    try:
        if dtype.is_string():
            return value if isinstance(value, str) else str(value)
        if dtype.is_integer():
            if isinstance(value, bool):
                raise ScopeValueError(f"Scope value for {field_name!r} is not an integer.")
            if isinstance(value, int):
                return value
            if isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
                return int(value.strip())
            if isinstance(value, Decimal) and value == value.to_integral_value():
                return int(value)
            raise ScopeValueError(f"Scope value for {field_name!r} is not an exact integer.")
        if dtype.is_decimal():
            return Decimal(str(value))
        if dtype.is_floating():
            return float(Decimal(str(value)))
        if dtype.is_boolean():
            if isinstance(value, bool):
                return value
            raise ScopeValueError(f"Scope value for {field_name!r} is not a boolean.")
    except (InvalidOperation, ValueError) as exc:
        raise ScopeValueError(f"Scope value for {field_name!r} is invalid.") from exc
    return value


def compile_scope_predicate(relation: Any, grant: Any) -> Any:
    """Return the exact Ibis predicate for ``grant`` on ``relation`` (or None)."""
    fields = _grant_fields(grant)
    if not fields:
        return None
    columns = set(relation.columns)
    schema = relation.schema()
    predicate = None
    for field_name in sorted(fields):
        if field_name not in columns:
            raise ScopeColumnError(
                f"Row policy column {field_name!r} is not present in the dataset; access denied."
            )
        column = relation[field_name]
        values = tuple(fields[field_name])
        if not values:
            raise UnscopedAccessError("Row grant is empty; access denied.")
        dtype = schema[field_name]
        non_null = [_coerce_exact(v, dtype, field_name) for v in values if v is not None]
        terms = []
        if non_null:
            terms.append(column == non_null[0] if len(non_null) == 1 else column.isin(non_null))
        if any(v is None for v in values):
            terms.append(column.isnull())
        term = terms[0] if len(terms) == 1 else (terms[0] | terms[1])
        predicate = term if predicate is None else (predicate & term)
    return predicate


def apply_scope(table_name: Any, relation: Any, *, protected: Iterable[str] = ()) -> Any:
    """Scope ``relation`` (a resolved base relation of ``table_name``)."""
    bare = _bare(table_name)
    grant = current_scope().grant_for(bare)
    if grant is not None and _is_protecting(grant):
        return relation.filter(compile_scope_predicate(relation, grant))
    if bare in set(protected):
        raise UnscopedAccessError(
            f"Dataset {bare!r} is protected and no row grant is active for this execution."
        )
    return relation


# --------------------------------------------------------------------- guard

_GUARD_ATTR = "_pivot_scope_guard"
_GUARD_LOCK = threading.Lock()


class _ConnectionGuard:
    def __init__(self, con: Any) -> None:
        self.protected: set[str] = set()
        self.raw_table = con.table
        self._raw_sql = getattr(con, "sql", None)
        self._raw_raw_sql = getattr(con, "raw_sql", None)

    def table(self, name: Any, *args: Any, **kwargs: Any) -> Any:
        return apply_scope(name, self.raw_table(name, *args, **kwargs), protected=self.protected)

    def _check_sql(self, query: Any) -> None:
        if not self.protected:
            return
        text = str(query or "").lower()
        hits = [t for t in self.protected if re.search(rf"(?<![a-z0-9_]){re.escape(t)}(?![a-z0-9_])", text)]
        if hits:
            raise UnscopedAccessError(
                "Raw SQL over a protected dataset cannot be proven scoped; access denied."
            )

    def sql(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        self._check_sql(query)
        return self._raw_sql(query, *args, **kwargs)

    def raw_sql(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        self._check_sql(query)
        return self._raw_raw_sql(query, *args, **kwargs)


def install_scope_guard(con: Any) -> Optional[_ConnectionGuard]:
    """Install (idempotently) the scope guard on an Ibis backend connection."""
    if con is None or not hasattr(con, "table"):
        return None
    with _GUARD_LOCK:
        guard = getattr(con, _GUARD_ATTR, None)
        if guard is not None:
            return guard
        guard = _ConnectionGuard(con)
        object.__setattr__(con, _GUARD_ATTR, guard)
        object.__setattr__(con, "table", guard.table)
        if guard._raw_sql is not None:
            object.__setattr__(con, "sql", guard.sql)
        if guard._raw_raw_sql is not None:
            object.__setattr__(con, "raw_sql", guard.raw_sql)
        return guard


def connection_guard(con: Any) -> Optional[_ConnectionGuard]:
    return getattr(con, _GUARD_ATTR, None) if con is not None else None


def protected_tables(con: Any) -> FrozenSet[str]:
    guard = connection_guard(con)
    return frozenset(guard.protected) if guard is not None else frozenset()


def connections_of(owner: Any) -> list:
    """Every Ibis connection reachable from an adapter/controller/planner."""
    seen: list = []

    def add(con: Any) -> None:
        if con is not None and hasattr(con, "table") and all(con is not s for s in seen):
            seen.append(con)

    controller = getattr(owner, "controller", owner)
    for holder in (controller, getattr(controller, "planner", None), getattr(controller, "backend", None)):
        if holder is None:
            continue
        add(getattr(holder, "con", None))
        builder = getattr(holder, "builder", None)
        catalog = getattr(builder, "catalog", None)
        for backend in list(getattr(catalog, "_registered_backends", {}).values()):
            add(getattr(backend, "con", backend))
    return seen


# --------------------------------------------------------------------- context


def _prepare_scope(grants: Any, connections: Iterable[Any], proven_paths: Iterable[str]) -> ScopeContext:
    if grants is None:
        mapping: Dict[str, Any] = {}
    elif isinstance(grants, Mapping) and not hasattr(grants, "field_values"):
        mapping = {_bare(k): v for k, v in grants.items() if v is not None}
    else:
        mapping = {_bare(getattr(grants, "table", "")): grants}
    outer = current_scope()
    for table, grant in mapping.items():
        existing = outer.grants.get(table)
        if existing is not None and _is_protecting(existing) and existing is not grant and (
            getattr(existing, "scope_fingerprint", None) != getattr(grant, "scope_fingerprint", None)
        ):
            raise UnscopedAccessError("Nested execution attempted to replace an active row grant.")
    for con in connections:
        guard = install_scope_guard(con)
        if guard is not None:
            guard.protected.update(t for t, g in mapping.items() if _is_protecting(g))
    return ScopeContext(
        grants=MappingProxyType({**outer.grants, **mapping}),
        proven_paths=outer.proven_paths | frozenset(proven_paths),
    )


@contextlib.contextmanager
def scoped_execution(
    grants: Any,
    *,
    connections: Iterable[Any] = (),
    proven_paths: Iterable[str] = (),
) -> Iterator[ScopeContext]:
    """Activate ``grants`` (one grant, a mapping table->grant, or None)."""
    ctx = _prepare_scope(grants, connections, proven_paths)
    token = _CURRENT.set(ctx)
    try:
        yield ctx
    finally:
        _CURRENT.reset(token)


def wrap_with_scope(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Bind ``fn`` to a snapshot of the current context (for thread hops)."""
    ctx = contextvars.copy_context()

    @functools.wraps(fn)
    def bound(*args: Any, **kwargs: Any) -> Any:
        return ctx.run(fn, *args, **kwargs)

    return bound


def submit_with_scope(executor: Any, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """``executor.submit`` that carries the active scope into the worker."""
    return executor.submit(wrap_with_scope(fn), *args, **kwargs)


# --------------------------------------------------------------------- path gate

# Paths proven by tests/security/test_tenant_isolation_matrix.py.  Anything
# else is denied for protected datasets until its isolation case passes.
PROVEN_PATHS: FrozenSet[str] = frozenset()


def require_proven_path(path_id: str) -> None:
    """Deny ``path_id`` while a protecting grant is active unless proven."""
    ctx = current_scope()
    if ctx.active and path_id not in (PROVEN_PATHS | ctx.proven_paths):
        raise PathNotProvenError(
            f"Execution path {path_id!r} is not proven tenant-safe; denied for protected datasets."
        )


def row_scoped(method: Callable[..., Any]) -> Callable[..., Any]:
    """Bind a method's ``row_scope=`` keyword to dataset access for its duration.

    For adapter-level entry points reached without the runtime router
    (Dash filter options, REST, Flight).  Works for sync and async methods.
    """
    import inspect

    if inspect.iscoroutinefunction(method):

        @functools.wraps(method)
        async def async_bound(self: Any, *args: Any, **kwargs: Any) -> Any:
            with scoped_execution(kwargs.get("row_scope"), connections=connections_of(self)):
                return await method(self, *args, **kwargs)

        return async_bound

    @functools.wraps(method)
    def bound(self: Any, *args: Any, **kwargs: Any) -> Any:
        with scoped_execution(kwargs.get("row_scope"), connections=connections_of(self)):
            return method(self, *args, **kwargs)

    return bound


def bind_request_scope(grant: Any, *, connections: Iterable[Any] = ()) -> ScopeContext:
    """Bind ``grant`` for the remainder of the current request task.

    For framework handlers (FastAPI routes, Flight RPCs) where the grant is
    obtained mid-handler and a ``with`` block would not cover the handler's
    remaining awaits.  Each ASGI request / RPC runs in its own context copy,
    so the binding cannot outlive the request.  Rebinding a *different*
    protecting grant in the same context is refused.
    """
    ctx = _prepare_scope(grant, connections, ())
    _CURRENT.set(ctx)
    return ctx



def run_in_executor_scoped(loop: Any, executor: Any, fn: Callable[..., Any], *args: Any) -> Any:
    """``loop.run_in_executor`` that carries the active scope (REQ-SEC-001).

    ``run_in_executor`` does not copy ``contextvars``; without this a worker
    thread would resolve protected datasets with no grant and fail closed.
    """
    return loop.run_in_executor(executor, wrap_with_scope(fn), *args)


def unguarded_table(con: Any, name: Any, *args: Any, **kwargs: Any) -> Any:
    """Test/inspection hook: the base relation, bypassing the scope guard.

    Never call from request-serving code; ``tests/security`` asserts that no
    module under ``pivot_engine`` (other than this one) references it.
    """
    guard = connection_guard(con)
    return (guard.raw_table if guard is not None else con.table)(name, *args, **kwargs)


def scope_where_sql(grant: Any, quote: Callable[[str], str]) -> Tuple[str, list]:
    """Exact parameterized ``WHERE`` for raw-SQL components (snapshot capture).

    Same semantics as :func:`compile_scope_predicate`: exact equality, NULL
    grant value matches only NULL, empty value list denies.
    """
    fields = _grant_fields(grant)
    if not fields:
        return "", []
    clauses: list = []
    params: list = []
    for name in sorted(fields):
        values = tuple(fields[name])
        if not values:
            raise UnscopedAccessError("Row grant is empty; access denied.")
        col = quote(str(name))
        non_null = [v for v in values if v is not None]
        parts = []
        if non_null:
            parts.append(f"{col} IN ({', '.join('?' for _ in non_null)})")
            params.extend(non_null)
        if len(non_null) != len(values):
            parts.append(f"{col} IS NULL")
        clauses.append("(" + " OR ".join(parts) + ")")
    return " WHERE " + " AND ".join(clauses), params
