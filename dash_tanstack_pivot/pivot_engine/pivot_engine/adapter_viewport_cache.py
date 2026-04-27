"""Thread-safe local cache state for TanStack viewport responses."""

from __future__ import annotations

from collections import OrderedDict
import threading
import time
from typing import Any, Optional

from .cache_coordinator import CacheCoordinator, CacheInvalidationEvent, CacheNamespaces


class AdapterViewportCache:
    """Single owner for adapter-local viewport cache state.

    The adapter coordinates multiple cache namespaces for virtual scrolling:
    full window responses, fixed row blocks, grand totals, pivot-column
    catalogs, center column ids, and pending background prefetches. Keeping the
    mutable state here avoids spreading lock/generation/invalidation rules
    through the request execution path.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float,
        pivot_catalog_size: int,
        response_window_size: int,
        row_block_cache_size: int,
        cache_coordinator: Optional[CacheCoordinator] = None,
    ) -> None:
        self.lock = threading.RLock()
        self.ttl_seconds = ttl_seconds
        self.pivot_catalog_size = pivot_catalog_size
        self.response_window_size = response_window_size
        self.row_block_cache_size = row_block_cache_size
        self.cache_coordinator = cache_coordinator or CacheCoordinator()
        self.center_col_ids_cache: dict[Any, list[str]] = {}
        self.pivot_column_catalog_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.response_window_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.row_block_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.grand_total_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.prefetch_request_keys: set[str] = set()
        self._generation = 0
        self._last_coordinator_invalidation_sequence = 0
        self._cache_namespace_by_id = {
            id(self.pivot_column_catalog_cache): CacheNamespaces.ADAPTER_PIVOT_CATALOG,
            id(self.response_window_cache): CacheNamespaces.ADAPTER_RESPONSE_WINDOW,
            id(self.row_block_cache): CacheNamespaces.ADAPTER_ROW_BLOCK,
            id(self.grand_total_cache): CacheNamespaces.ADAPTER_GRAND_TOTAL,
        }
        self._adapter_namespaces = (
            CacheNamespaces.ADAPTER_CENTER_COLUMNS,
            CacheNamespaces.ADAPTER_PIVOT_CATALOG,
            CacheNamespaces.ADAPTER_RESPONSE_WINDOW,
            CacheNamespaces.ADAPTER_ROW_BLOCK,
            CacheNamespaces.ADAPTER_GRAND_TOTAL,
            CacheNamespaces.ADAPTER_PREFETCH_PENDING,
        )
        self._register_cache_namespaces()

    def _register_cache_namespaces(self) -> None:
        self.cache_coordinator.register_namespace(
            CacheNamespaces.ADAPTER_CENTER_COLUMNS,
            owner="adapter",
            key_dimensions=("table", "request_structure", "column_window", "cache_generation"),
            ttl_seconds=self.ttl_seconds,
            size_getter=lambda: self._entries_snapshot(self.center_col_ids_cache),
            invalidator=self._invalidate_from_coordinator,
        )
        self.cache_coordinator.register_namespace(
            CacheNamespaces.ADAPTER_PIVOT_CATALOG,
            owner="adapter",
            key_dimensions=("table", "columns", "filters", "custom_dimensions", "column_sort_options", "cache_generation"),
            max_entries=self.pivot_catalog_size,
            ttl_seconds=self.ttl_seconds,
            size_getter=lambda: self._entries_snapshot(self.pivot_column_catalog_cache),
            invalidator=self._invalidate_from_coordinator,
        )
        self.cache_coordinator.register_namespace(
            CacheNamespaces.ADAPTER_RESPONSE_WINDOW,
            owner="adapter",
            key_dimensions=("request_structure", "expanded", "row_window", "column_window", "grand_total", "cache_generation"),
            max_entries=self.response_window_size,
            ttl_seconds=self.ttl_seconds,
            size_getter=lambda: self._entries_snapshot(self.response_window_cache),
            invalidator=self._invalidate_from_coordinator,
        )
        self.cache_coordinator.register_namespace(
            CacheNamespaces.ADAPTER_ROW_BLOCK,
            owner="adapter",
            key_dimensions=("request_structure", "expanded", "block_index", "column_window", "cache_generation"),
            max_entries=self.row_block_cache_size,
            ttl_seconds=self.ttl_seconds,
            size_getter=lambda: self._entries_snapshot(self.row_block_cache),
            invalidator=self._invalidate_from_coordinator,
        )
        self.cache_coordinator.register_namespace(
            CacheNamespaces.ADAPTER_GRAND_TOTAL,
            owner="adapter",
            key_dimensions=("request_structure", "expanded", "column_window", "cache_generation"),
            max_entries=self.row_block_cache_size,
            ttl_seconds=self.ttl_seconds,
            size_getter=lambda: self._entries_snapshot(self.grand_total_cache),
            invalidator=self._invalidate_from_coordinator,
        )
        self.cache_coordinator.register_namespace(
            CacheNamespaces.ADAPTER_PREFETCH_PENDING,
            owner="adapter",
            key_dimensions=("response_window_key",),
            max_entries=self.response_window_size,
            ttl_seconds=self.ttl_seconds,
            size_getter=lambda: self._entries_snapshot(self.prefetch_request_keys),
            invalidator=self._invalidate_from_coordinator,
        )

    def _entries_snapshot(self, cache: Any) -> dict[str, int]:
        with self.lock:
            return {"entries": len(cache)}

    def generation_value(self) -> int:
        with self.lock:
            return self._generation

    def metrics_snapshot(self) -> dict[str, Any]:
        return self.cache_coordinator.snapshot(self._adapter_namespaces)

    @staticmethod
    def lookup_entry(cache: OrderedDict, key: str) -> Optional[Any]:
        cached_entry = cache.get(key)
        if not cached_entry:
            return None
        value, expires_at = cached_entry
        if time.time() > expires_at:
            cache.pop(key, None)
            return None
        cache.move_to_end(key)
        return value

    @staticmethod
    def store_entry(
        cache: OrderedDict,
        key: str,
        value: Any,
        *,
        max_size: int,
        ttl_seconds: float,
    ) -> int:
        cache[key] = (value, time.time() + ttl_seconds)
        cache.move_to_end(key)
        evicted = 0
        while len(cache) > max_size:
            cache.popitem(last=False)
            evicted += 1
        return evicted

    def lookup(self, cache: OrderedDict, key: str) -> Optional[Any]:
        with self.lock:
            namespace = self._cache_namespace_by_id.get(id(cache))
            had_entry = key in cache
            value = self.lookup_entry(cache, key)
            if namespace is not None:
                self.cache_coordinator.record_lookup(
                    namespace,
                    hit=value is not None,
                    expired=had_entry and value is None,
                )
            return value

    def store(
        self,
        cache: OrderedDict,
        key: str,
        value: Any,
        *,
        max_size: int,
        ttl_seconds: float,
    ) -> None:
        with self.lock:
            namespace = self._cache_namespace_by_id.get(id(cache))
            evicted = self.store_entry(
                cache,
                key,
                value,
                max_size=max_size,
                ttl_seconds=ttl_seconds,
            )
            if namespace is not None:
                self.cache_coordinator.record_store(namespace, entries=len(cache))
                if evicted:
                    self.cache_coordinator.record_eviction(namespace, count=evicted, entries=len(cache))

    def center_col_ids_get(self, key: Any) -> Optional[list[str]]:
        with self.lock:
            if key not in self.center_col_ids_cache:
                self.cache_coordinator.record_lookup(CacheNamespaces.ADAPTER_CENTER_COLUMNS, hit=False)
                return None
            self.cache_coordinator.record_lookup(CacheNamespaces.ADAPTER_CENTER_COLUMNS, hit=True)
            return list(self.center_col_ids_cache.get(key) or [])

    def center_col_ids_set(self, key: Any, value: list[str]) -> None:
        with self.lock:
            self.center_col_ids_cache[key] = list(value or [])
            self.cache_coordinator.record_store(
                CacheNamespaces.ADAPTER_CENTER_COLUMNS,
                entries=len(self.center_col_ids_cache),
            )

    def mark_prefetch_request_pending(self, cache_key: str) -> bool:
        with self.lock:
            if cache_key in self.prefetch_request_keys:
                self.cache_coordinator.record_lookup(CacheNamespaces.ADAPTER_PREFETCH_PENDING, hit=True)
                return False
            had_response_entry = cache_key in self.response_window_cache
            if self.lookup_entry(self.response_window_cache, cache_key) is not None:
                self.cache_coordinator.record_lookup(CacheNamespaces.ADAPTER_RESPONSE_WINDOW, hit=True)
                self.cache_coordinator.record_lookup(CacheNamespaces.ADAPTER_PREFETCH_PENDING, hit=True)
                return False
            if had_response_entry:
                self.cache_coordinator.record_lookup(
                    CacheNamespaces.ADAPTER_RESPONSE_WINDOW,
                    hit=False,
                    expired=True,
                )
            self.prefetch_request_keys.add(cache_key)
            self.cache_coordinator.record_store(
                CacheNamespaces.ADAPTER_PREFETCH_PENDING,
                entries=len(self.prefetch_request_keys),
            )
            return True

    def clear_prefetch_request_pending(self, cache_key: str) -> None:
        with self.lock:
            if cache_key in self.prefetch_request_keys:
                self.prefetch_request_keys.discard(cache_key)
                self.cache_coordinator.record_delete(
                    CacheNamespaces.ADAPTER_PREFETCH_PENDING,
                    entries=len(self.prefetch_request_keys),
                    reason="prefetch_finished",
                )

    def invalidate_all(self) -> None:
        self.cache_coordinator.invalidate(
            self._adapter_namespaces,
            event="adapter.invalidate_all",
            reason="adapter_local_cache_reset",
        )

    def _invalidate_from_coordinator(self, event: CacheInvalidationEvent) -> None:
        with self.lock:
            if event.sequence == self._last_coordinator_invalidation_sequence:
                return
            self._last_coordinator_invalidation_sequence = event.sequence
            self._generation += 1
            self.center_col_ids_cache.clear()
            self.pivot_column_catalog_cache.clear()
            self.response_window_cache.clear()
            self.row_block_cache.clear()
            self.grand_total_cache.clear()
            self.prefetch_request_keys.clear()
