"""Thread-safe local cache state for TanStack viewport responses."""

from __future__ import annotations

from collections import OrderedDict
import threading
import time
from typing import Any, Optional


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
    ) -> None:
        self.lock = threading.RLock()
        self.ttl_seconds = ttl_seconds
        self.pivot_catalog_size = pivot_catalog_size
        self.response_window_size = response_window_size
        self.row_block_cache_size = row_block_cache_size
        self.center_col_ids_cache: dict[Any, list[str]] = {}
        self.pivot_column_catalog_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.response_window_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.row_block_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.grand_total_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.prefetch_request_keys: set[str] = set()
        self._generation = 0

    def generation_value(self) -> int:
        with self.lock:
            return self._generation

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
    ) -> None:
        cache[key] = (value, time.time() + ttl_seconds)
        cache.move_to_end(key)
        while len(cache) > max_size:
            cache.popitem(last=False)

    def lookup(self, cache: OrderedDict, key: str) -> Optional[Any]:
        with self.lock:
            return self.lookup_entry(cache, key)

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
            self.store_entry(
                cache,
                key,
                value,
                max_size=max_size,
                ttl_seconds=ttl_seconds,
            )

    def center_col_ids_get(self, key: Any) -> Optional[list[str]]:
        with self.lock:
            if key not in self.center_col_ids_cache:
                return None
            return list(self.center_col_ids_cache.get(key) or [])

    def center_col_ids_set(self, key: Any, value: list[str]) -> None:
        with self.lock:
            self.center_col_ids_cache[key] = list(value or [])

    def mark_prefetch_request_pending(self, cache_key: str) -> bool:
        with self.lock:
            if cache_key in self.prefetch_request_keys:
                return False
            if self.lookup_entry(self.response_window_cache, cache_key) is not None:
                return False
            self.prefetch_request_keys.add(cache_key)
            return True

    def clear_prefetch_request_pending(self, cache_key: str) -> None:
        with self.lock:
            self.prefetch_request_keys.discard(cache_key)

    def invalidate_all(self) -> None:
        with self.lock:
            self._generation += 1
            self.center_col_ids_cache.clear()
            self.pivot_column_catalog_cache.clear()
            self.response_window_cache.clear()
            self.row_block_cache.clear()
            self.grand_total_cache.clear()
            self.prefetch_request_keys.clear()
