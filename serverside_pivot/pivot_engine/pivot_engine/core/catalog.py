"""Revision-scoped column catalogs and exact members for equality pushdown."""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Optional

from .authority.level_binding import binding
from .cache import CacheManager, key_for
from .ir import PivotQuery, PredicateGroup, Window


@dataclass
class CatalogEntry:
    source: Any
    data: Any
    nbytes: int


def store_for(backend: Any) -> CatalogStore:
    store = getattr(backend, "_pivot_catalog_store", None)
    if store is None:
        store = CatalogStore(backend)
        object.__setattr__(backend, "_pivot_catalog_store", store)
    return store


class CatalogStore:
    def __init__(self, backend: Any, budget: int = 32 * 1024 * 1024) -> None:
        self.backend = backend
        self.lock = threading.RLock()
        self.cache = CacheManager(budgets={"catalog": budget})
        self.revisions: Dict[str, int] = {}

    def invalidate(self, dataset: Optional[str] = None) -> None:
        with self.lock:
            if dataset is not None:
                self.revisions[dataset] = self.revisions.get(dataset, 0) + 1
            self.cache.invalidate(dataset=dataset, tiers=("catalog",))

    def get(self, query: PivotQuery, relation: Any, identity: str, compute: Callable[[], Any]) -> Any:
        token = hashlib.sha256(f"{identity}|{hash(relation.op())}|{binding(query.dataset)}".encode()).hexdigest()
        key = key_for("catalog", query.dataset, token, str(self.revisions.get(query.dataset, 0)))
        with self.lock:
            entry = self.cache.get(key)
            if entry is None or entry.source != relation.op():
                data = compute()
                # Account for retained Python members as well as Arrow buffers.
                entry = CatalogEntry(relation.op(), data, max(1024, len(repr(data).encode()) * 4))
                self.cache.put(key, entry)
            return entry.data

    def lower_filters(self, query: PivotQuery, relation: Any, *, allow_scan: bool = True) -> PivotQuery:
        """Resolve case-folded equality using backend-folded catalog values.

        Both case variants stay distinct members. Missing values retain the
        original LOWER predicate, preserving backend string semantics.
        When ``allow_scan=False``, only already-cached catalog entries or
        in-memory level-1 tables are consulted (never triggering an extra scan
        of the raw facts).
        """
        from .authority.verifier import verify_scoped
        from .exec import materialize
        from .physical.union_all import prepared

        if not query.filters:
            return query
        source = prepared(relation, query)
        backend = source._find_backend()

        def cached_members(field: str) -> Optional[Any]:
            token = hashlib.sha256(
                f"equality:{field}|{hash(source.op())}|{binding(query.dataset)}".encode()
            ).hexdigest()
            key = key_for("catalog", query.dataset, token, str(self.revisions.get(query.dataset, 0)))
            with self.lock:
                entry = self.cache.get(key)
                if entry is not None and entry.source == source.op():
                    return entry.data
            # Check if an already-populated unfiltered level-1 table holds all distinct values of `field`.
            level_store = getattr(backend, "_pivot_level_store", None)
            if level_store is not None:
                found = level_store.distinct_values_for_field(query.dataset, source.op(), field)
                if found is not None:
                    return found
            return None

        def lower(p: Any) -> Any:
            if isinstance(p, PredicateGroup):
                return replace(p, items=tuple(lower(i) for i in p.items))
            if (p.op != "=" or p.case_sensitive or not isinstance(p.value, str)
                    or not source[p.field].type().is_string()):
                return p
            if not allow_scan:
                rows = cached_members(p.field)
                if rows is None:
                    return p
            else:
                def members() -> Any:
                    expr = source.select(source[p.field], __fold=source[p.field].lower()).distinct()
                    verify_scoped(expr)
                    return materialize(expr).to_pylist()
                rows = self.get(query, source, f"equality:{p.field}", members)
            matches = tuple(r[p.field] for r in rows if r['__fold'] == p.value.lower())
            return replace(p, op="in", value=matches, case_sensitive=True) if matches else p
        return replace(query, filters=tuple(lower(p) for p in query.filters))


def catalog_meaning(query: PivotQuery) -> PivotQuery:
    return replace(query, rows=(), expanded=None, window=Window(), column_members=(), sort=(),
                   measure_filters=(), subtotals="before", ragged=None)
