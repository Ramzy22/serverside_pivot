"""Request access to versioned Arrow partials; publication belongs to the data owner.

The owner supplies an atomic epoch check/publication and optional memory reservation.
The executor supplies verified builders. Neither a cache hit nor an Arrow relation
replaces dataset authorization: the executor resolves the source before using this tier.
"""

from __future__ import annotations

import hashlib
import time
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from functools import partial
from typing import Any, Callable, List, Optional, Tuple

import pyarrow as pa

from .authority.scope import current_scope
from .cache import CacheKey, CacheManager, key_for
from .identity import CanonicalScalarCodec
from .ir import PivotQuery, Window
from .physical.union_all import Restriction


def cube_identity(query: PivotQuery, grain: Tuple[str, ...]) -> str:
    """Input semantics and grain; independent of presentation, displayed cells and expansion."""
    from .exec import canonical

    whole = replace(query, sort=(), expanded=None, window=Window(), column_members=(), derived=(), subtotals="before")
    minima = tuple(sorted({s.by for s in query.sort if s.by is not None}))  # the member-key MINs the cube carries
    grant = current_scope().grant_for(query.dataset)
    predicates = tuple((field, tuple(sorted(CanonicalScalarCodec.encode(v) for v in values)))
                       for field, values in sorted(getattr(grant, "field_values", {}).items()))
    return hashlib.sha256((canonical(whole) + repr((grain, minima, predicates))).encode()).hexdigest()


@dataclass
class CubeEntry:
    table: pa.Table
    coverage: Restriction
    created: float = field(default_factory=time.monotonic)
    lease: Any = None

    @property
    def nbytes(self) -> int:
        return int(self.table.nbytes) + 512 + len(repr(self.coverage).encode())

    def __del__(self) -> None:
        # A request holds this object while its Arrow relation is in use, even after LRU eviction.
        if self.lease is not None:
            self.lease.release()


@dataclass
class CubeProgram:
    """A compiled tree pinned to its exact partial buffers, owned by the same governed cache manager."""

    tree: Any
    entries: Tuple[CubeEntry, ...]
    relations: Tuple[Any, ...]
    created: float
    nbytes: int
    lease: Any = None

    def __del__(self) -> None:
        if self.lease is not None:
            self.lease.release()


_ACTIVE: ContextVar[Optional[CubeAccess]] = ContextVar("pivot_core_cube_access", default=None)


def current_cube_access() -> Optional[CubeAccess]:
    return _ACTIVE.get()


class CubeAccess:
    """A read's cache view. An epoch change prevents lookup and publication, including during extensions."""

    def __init__(self, cache: CacheManager, revision: str,
                 publish: Callable[[CacheKey, Any], None], current: Callable[[], bool],
                 ttl: float = 300.0, backend: Any = None, refresh: bool = False):
        self.cache, self.revision, self.publish, self.current, self.ttl = cache, revision, publish, current, ttl
        self.backend = backend
        self.refresh = refresh  # a forced refresh reads the facts; what it computes is still stored
        self.relations: dict[int, Any] = {}
        self._held: List[CubeEntry] = []
        self._cleanup: List[Callable[[], None]] = []
        self._programs: List[CubeProgram] = []

    def __enter__(self) -> CubeAccess:
        self._token = _ACTIVE.set(self)
        return self

    def __exit__(self, *args: Any) -> None:
        try:
            for cleanup in reversed(self._cleanup):
                cleanup()
        finally:
            self._cleanup.clear()
            self.relations.clear()
            self._held.clear()
            self._programs.clear()
            _ACTIVE.reset(self._token)

    def on_exit(self, cleanup: Callable[[], None]) -> None:
        self._cleanup.append(cleanup)

    def _program_key(self, query: PivotQuery) -> CacheKey:
        from .exec import canonical

        whole = replace(query, window=Window())
        identity = hashlib.sha256((canonical(whole) + cube_identity(query, ())).encode()).hexdigest()
        return key_for("plan", query.dataset, "cube-program:" + identity, self.revision)

    def program(self, query: PivotQuery) -> Any:
        from .physical.arrow_relation import release

        key = self._program_key(query)
        program = self.cache.get(key) if self.current() and not self.refresh else None
        if program is None:
            return None
        if time.monotonic() - program.created >= self.ttl:
            self.cache.discard(key)
            return None
        self._programs.append(program)
        self._held.extend(program.entries)
        for relation in program.relations:
            self.relations[id(relation.op().data)] = relation
            self.on_exit(partial(release, self.backend, relation))
        return program.tree

    def store_program(self, query: PivotQuery, tree: Any) -> None:
        from ibis.common.graph import Graph

        entries = tuple({id(entry): entry for entry in self._held}.values())
        program = CubeProgram(tree, entries, tuple(self.relations.values()),
                              min((e.created for e in entries), default=time.monotonic()),
                              len(Graph.from_bfs(tree.op())) * 256 + 1024)
        self._programs.append(program)
        if self.current():
            self.publish(self._program_key(query), program)

    def get(self, query: PivotQuery, grain: Tuple[str, ...], restriction: Restriction,
            build: Callable[[Restriction], pa.Table]) -> pa.Table:
        key = key_for("cube", query.dataset, cube_identity(query, grain), self.revision)
        entry = self.cache.get(key) if self.current() and not self.refresh else None
        if entry is not None and time.monotonic() - entry.created >= self.ttl:
            self.cache.discard(key)
            entry = None
        missing = restriction
        if entry is not None:
            self._held.append(entry)
            if entry.coverage is None:
                return entry.table
            if restriction is not None and entry.coverage[0] == restriction[0]:
                missing = (restriction[0], restriction[1] - entry.coverage[1])
                if not missing[1]:
                    return entry.table
        table = build(missing)
        coverage = restriction
        if entry is not None and missing is not None and entry.coverage is not None:
            table = pa.concat_tables([entry.table, table])
            coverage = (missing[0], missing[1] | entry.coverage[1])
        fresh = CubeEntry(table, coverage)
        self._held.append(fresh)
        if self.current():
            self.publish(key, fresh)
        return table
