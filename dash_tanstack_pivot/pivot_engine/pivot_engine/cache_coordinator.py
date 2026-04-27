"""Shared cache namespace registry, invalidation events, and metrics."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, Iterable, Mapping, Optional, Sequence


class CacheNamespaces:
    """Stable cache namespace identifiers used across runtime components."""

    RUNTIME_PAYLOAD = "runtime.payload"
    CONTROLLER_QUERY = "controller.query"
    CONTROLLER_HIERARCHY_VIEW = "controller.hierarchy_view"
    CONTROLLER_HIERARCHY_ROOT_COUNT = "controller.hierarchy_root_count"
    CONTROLLER_HIERARCHY_ROOT_PAGE = "controller.hierarchy_root_page"
    CONTROLLER_HIERARCHY_GRAND_TOTAL = "controller.hierarchy_grand_total"
    ADAPTER_CENTER_COLUMNS = "adapter.center_columns"
    ADAPTER_PIVOT_CATALOG = "adapter.pivot_catalog"
    ADAPTER_RESPONSE_WINDOW = "adapter.response_window"
    ADAPTER_ROW_BLOCK = "adapter.row_block"
    ADAPTER_GRAND_TOTAL = "adapter.grand_total"
    ADAPTER_PREFETCH_PENDING = "adapter.prefetch_pending"


@dataclass(frozen=True)
class CacheNamespaceSpec:
    name: str
    owner: str
    key_dimensions: tuple[str, ...] = ()
    max_entries: Optional[int] = None
    max_bytes: Optional[int] = None
    ttl_seconds: Optional[float] = None


@dataclass(frozen=True)
class CacheInvalidationEvent:
    sequence: int
    event: str
    namespaces: tuple[str, ...]
    reason: str = ""
    table: Optional[str] = None
    structural: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass
class CacheNamespaceMetrics:
    generation: int = 0
    hits: int = 0
    misses: int = 0
    stores: int = 0
    evictions: int = 0
    expirations: int = 0
    invalidations: int = 0
    deletes: int = 0
    entries: int = 0
    bytes: int = 0
    last_event: Optional[str] = None
    last_reason: Optional[str] = None
    last_updated_at: Optional[float] = None


SizeGetter = Callable[[], Mapping[str, int]]
Invalidator = Callable[[CacheInvalidationEvent], None]


class CacheCoordinator:
    """Coordinates cache namespace metadata, metrics, and invalidation events.

    The coordinator intentionally does not replace the backing cache structures.
    It gives each cache an explicit namespace contract and a single invalidation
    event bus so correctness-sensitive clears are observable and testable.
    """

    def __init__(self, *, max_events: int = 100) -> None:
        self._lock = threading.RLock()
        self._namespaces: Dict[str, CacheNamespaceSpec] = {}
        self._metrics: Dict[str, CacheNamespaceMetrics] = {}
        self._size_getters: Dict[str, SizeGetter] = {}
        self._invalidators: Dict[str, Invalidator] = {}
        self._events: Deque[CacheInvalidationEvent] = deque(maxlen=max(1, int(max_events)))
        self._sequence = 0

    def register_namespace(
        self,
        name: str,
        *,
        owner: str,
        key_dimensions: Sequence[str] = (),
        max_entries: Optional[int] = None,
        max_bytes: Optional[int] = None,
        ttl_seconds: Optional[float] = None,
        size_getter: Optional[SizeGetter] = None,
        invalidator: Optional[Invalidator] = None,
    ) -> None:
        namespace = str(name)
        with self._lock:
            self._namespaces[namespace] = CacheNamespaceSpec(
                name=namespace,
                owner=str(owner),
                key_dimensions=tuple(str(item) for item in (key_dimensions or ())),
                max_entries=max_entries,
                max_bytes=max_bytes,
                ttl_seconds=ttl_seconds,
            )
            self._metrics.setdefault(namespace, CacheNamespaceMetrics())
            if size_getter is not None:
                self._size_getters[namespace] = size_getter
            if invalidator is not None:
                self._invalidators[namespace] = invalidator

    def namespaces(self, *, owner: Optional[str] = None) -> tuple[str, ...]:
        with self._lock:
            if owner is None:
                return tuple(sorted(self._namespaces))
            owner_prefix = str(owner)
            return tuple(
                sorted(
                    name
                    for name, spec in self._namespaces.items()
                    if spec.owner == owner_prefix or spec.owner.startswith(f"{owner_prefix}.")
                )
            )

    def record_lookup(self, namespace: str, *, hit: bool, expired: bool = False) -> None:
        with self._lock:
            metrics = self._metrics.setdefault(str(namespace), CacheNamespaceMetrics())
            if hit:
                metrics.hits += 1
            else:
                metrics.misses += 1
            if expired:
                metrics.expirations += 1
            metrics.last_event = "lookup_hit" if hit else ("lookup_expired" if expired else "lookup_miss")
            metrics.last_updated_at = time.time()

    def record_store(
        self,
        namespace: str,
        *,
        entries: Optional[int] = None,
        bytes_used: Optional[int] = None,
    ) -> None:
        with self._lock:
            metrics = self._metrics.setdefault(str(namespace), CacheNamespaceMetrics())
            metrics.stores += 1
            if entries is not None:
                metrics.entries = max(0, int(entries))
            if bytes_used is not None:
                metrics.bytes = max(0, int(bytes_used))
            metrics.last_event = "store"
            metrics.last_updated_at = time.time()

    def record_eviction(
        self,
        namespace: str,
        *,
        count: int = 1,
        bytes_removed: int = 0,
        reason: str = "lru",
        entries: Optional[int] = None,
        bytes_used: Optional[int] = None,
    ) -> None:
        with self._lock:
            metrics = self._metrics.setdefault(str(namespace), CacheNamespaceMetrics())
            metrics.evictions += max(0, int(count))
            if entries is not None:
                metrics.entries = max(0, int(entries))
            if bytes_used is not None:
                metrics.bytes = max(0, int(bytes_used))
            metrics.last_event = "eviction"
            metrics.last_reason = str(reason or "")
            metrics.last_updated_at = time.time()

    def record_delete(
        self,
        namespace: str,
        *,
        count: int = 1,
        bytes_removed: int = 0,
        reason: str = "delete",
        entries: Optional[int] = None,
        bytes_used: Optional[int] = None,
    ) -> None:
        with self._lock:
            metrics = self._metrics.setdefault(str(namespace), CacheNamespaceMetrics())
            metrics.deletes += max(0, int(count))
            if entries is not None:
                metrics.entries = max(0, int(entries))
            if bytes_used is not None:
                metrics.bytes = max(0, int(bytes_used))
            metrics.last_event = "delete"
            metrics.last_reason = str(reason or "")
            metrics.last_updated_at = time.time()

    def record_expiration(
        self,
        namespace: str,
        *,
        count: int = 1,
        bytes_removed: int = 0,
        entries: Optional[int] = None,
        bytes_used: Optional[int] = None,
    ) -> None:
        with self._lock:
            metrics = self._metrics.setdefault(str(namespace), CacheNamespaceMetrics())
            metrics.expirations += max(0, int(count))
            if entries is not None:
                metrics.entries = max(0, int(entries))
            if bytes_used is not None:
                metrics.bytes = max(0, int(bytes_used))
            metrics.last_event = "expiration"
            metrics.last_updated_at = time.time()

    def invalidate(
        self,
        namespaces: Optional[Iterable[str]] = None,
        *,
        event: str,
        reason: str = "",
        table: Optional[str] = None,
        structural: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheInvalidationEvent:
        with self._lock:
            resolved = tuple(
                dict.fromkeys(
                    str(name)
                    for name in (namespaces if namespaces is not None else self._namespaces.keys())
                    if str(name) in self._namespaces
                )
            )
            self._sequence += 1
            invalidation = CacheInvalidationEvent(
                sequence=self._sequence,
                event=str(event),
                namespaces=resolved,
                reason=str(reason or ""),
                table=str(table) if table is not None else None,
                structural=bool(structural),
                metadata=dict(metadata or {}),
            )
            self._events.append(invalidation)
            invalidators = [self._invalidators[name] for name in resolved if name in self._invalidators]
            now = time.time()
            for namespace in resolved:
                metrics = self._metrics.setdefault(namespace, CacheNamespaceMetrics())
                metrics.invalidations += 1
                metrics.generation += 1
                metrics.last_event = str(event)
                metrics.last_reason = str(reason or "")
                metrics.last_updated_at = now

        for invalidator in invalidators:
            invalidator(invalidation)
        self.refresh_sizes(resolved)
        return invalidation

    def refresh_sizes(self, namespaces: Optional[Iterable[str]] = None) -> None:
        with self._lock:
            resolved = tuple(
                str(name)
                for name in (namespaces if namespaces is not None else self._namespaces.keys())
                if str(name) in self._size_getters
            )
            getters = {name: self._size_getters[name] for name in resolved}

        for namespace, getter in getters.items():
            try:
                size = getter() or {}
            except Exception:
                continue
            with self._lock:
                metrics = self._metrics.setdefault(namespace, CacheNamespaceMetrics())
                if "entries" in size:
                    metrics.entries = max(0, int(size.get("entries") or 0))
                if "bytes" in size:
                    metrics.bytes = max(0, int(size.get("bytes") or 0))

    def snapshot(
        self,
        namespaces: Optional[Iterable[str]] = None,
        *,
        include_events: bool = False,
    ) -> Dict[str, Any]:
        self.refresh_sizes(namespaces)
        with self._lock:
            resolved = tuple(
                str(name)
                for name in (namespaces if namespaces is not None else self._namespaces.keys())
                if str(name) in self._namespaces
            )
            payload = {
                "namespaces": {
                    name: {
                        "spec": {
                            "owner": self._namespaces[name].owner,
                            "keyDimensions": list(self._namespaces[name].key_dimensions),
                            "maxEntries": self._namespaces[name].max_entries,
                            "maxBytes": self._namespaces[name].max_bytes,
                            "ttlSeconds": self._namespaces[name].ttl_seconds,
                        },
                        "metrics": {
                            "generation": self._metrics.get(name, CacheNamespaceMetrics()).generation,
                            "hits": self._metrics.get(name, CacheNamespaceMetrics()).hits,
                            "misses": self._metrics.get(name, CacheNamespaceMetrics()).misses,
                            "stores": self._metrics.get(name, CacheNamespaceMetrics()).stores,
                            "evictions": self._metrics.get(name, CacheNamespaceMetrics()).evictions,
                            "expirations": self._metrics.get(name, CacheNamespaceMetrics()).expirations,
                            "invalidations": self._metrics.get(name, CacheNamespaceMetrics()).invalidations,
                            "deletes": self._metrics.get(name, CacheNamespaceMetrics()).deletes,
                            "entries": self._metrics.get(name, CacheNamespaceMetrics()).entries,
                            "bytes": self._metrics.get(name, CacheNamespaceMetrics()).bytes,
                            "lastEvent": self._metrics.get(name, CacheNamespaceMetrics()).last_event,
                            "lastReason": self._metrics.get(name, CacheNamespaceMetrics()).last_reason,
                        },
                    }
                    for name in resolved
                }
            }
            if include_events:
                payload["events"] = [
                    {
                        "sequence": event.sequence,
                        "event": event.event,
                        "namespaces": list(event.namespaces),
                        "reason": event.reason,
                        "table": event.table,
                        "structural": event.structural,
                        "metadata": dict(event.metadata or {}),
                        "createdAt": event.created_at,
                    }
                    for event in self._events
                ]
            return payload
