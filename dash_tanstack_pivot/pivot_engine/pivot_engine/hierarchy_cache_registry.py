"""Ownership of controller-local hierarchy cache namespaces and storage."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import sys
import time
from typing import Any

from .cache.memory_cache import MemoryCache
from .cache_coordinator import CacheCoordinator, CacheInvalidationEvent, CacheNamespaces
from .resource_governor import ResourceGovernor, ResourceLimits, estimate_value_bytes


def estimate_container_bytes(value: Any, seen: set[int] | None = None) -> int | None:
    """Compose the governor's value sizing for known hierarchy/metadata containers.

    Counts retained value data, not Python allocator or native heap overhead.
    Unknown leaves are rejected without serializing arbitrary user objects.
    """
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    measured = estimate_value_bytes(value)
    if measured is not None:
        return measured
    if value is None:
        return 0
    if isinstance(value, (bool, int, float, Decimal, date, datetime)):
        return sys.getsizeof(value)
    if isinstance(value, dict):
        children = (part for pair in value.items() for part in pair)
    elif isinstance(value, (list, tuple, set, frozenset)):
        children = iter(value)
    else:
        return None
    total = 0
    for child in children:
        charge = estimate_container_bytes(child, seen)
        if charge is None:
            return None
        total += charge
    return total


@dataclass
class _SizedValue:
    value: Any
    nbytes: int


class BoundedHierarchyCache(OrderedDict):
    """OrderedDict compatibility view whose mutations all enter MemoryCache.

    Mapping factories return detached, shallow OrderedDicts, as the original
    registry aliases did. These are caller-owned snapshots, not cache storage;
    writing them back through the view still requires bounded admission.
    """

    def __init__(self, *, ttl, max_size, max_bytes, governor):
        super().__init__()
        self.storage = MemoryCache(ttl, max_size, max_bytes=max_bytes, governor=governor)

    def _sync(self):
        OrderedDict.clear(self)
        for key, (value, expiry) in self.storage._cache.items():
            OrderedDict.__setitem__(self, key, (value.value, expiry))

    def set_entry(self, key, entry) -> bool:
        value, expiry = entry
        charge = estimate_container_bytes(value)
        if charge is None:
            return False
        with self.storage._lock:
            accepted = self.storage.set(key, _SizedValue(value, charge), ttl=expiry - time.time())
            self._sync()
            return accepted

    def __setitem__(self, key, entry):
        self.set_entry(key, entry)

    def __getitem__(self, key):
        with self.storage._lock:
            value = self.storage.get(key)
            self._sync()
            if value is None:
                raise KeyError(key)
            return OrderedDict.__getitem__(self, key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __delitem__(self, key):
        with self.storage._lock:
            if key not in self.storage._cache:
                raise KeyError(key)
            self.storage.delete(key)
            self._sync()

    def pop(self, key, *default):
        with self.storage._lock:
            if key not in self.storage._cache:
                if default:
                    return default[0]
                raise KeyError(key)
            value = OrderedDict.__getitem__(self, key)
            self.__delitem__(key)
            return value

    def popitem(self, last=True):
        with self.storage._lock:
            if not self.storage._cache:
                raise KeyError("dictionary is empty")
            key = next(reversed(self.storage._cache) if last else iter(self.storage._cache))
            return key, self.pop(key)

    def clear(self):
        with self.storage._lock:
            self.storage.clear()
            self._sync()

    def move_to_end(self, key, last=True):
        with self.storage._lock:
            self.storage._cache.move_to_end(key, last=last)
            self._sync()

    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).items():
            self[key] = value

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self.get(key)

    def copy(self):
        with self.storage._lock:
            # Construct from items so reads do not change this cache's LRU order.
            return OrderedDict(self.items())

    @classmethod
    def fromkeys(cls, iterable, value=None):
        return OrderedDict.fromkeys(iterable, value)

    def __or__(self, other):
        if not isinstance(other, dict):
            return NotImplemented
        result = self.copy()
        result.update(other.copy() if isinstance(other, BoundedHierarchyCache) else other)
        return result

    def __ror__(self, other):
        if not isinstance(other, dict):
            return NotImplemented
        result = OrderedDict(other.items())
        result.update(self.copy())
        return result

    def __ior__(self, other):
        self.update(other)
        return self

    def size_snapshot(self):
        with self.storage._lock:
            self.storage.get_all_keys()
            self._sync()
            return {"entries": len(self), "bytes": self.storage.bytes_used}


class HierarchyCacheRegistry:
    """Create and invalidate controller-local hierarchy cache namespaces."""

    def __init__(self, coordinator: CacheCoordinator, *, ttl_seconds: float,
                 max_bytes: int = ResourceLimits().cache_bytes,
                 governor: ResourceGovernor | None = None) -> None:
        self.coordinator = coordinator
        self.max_bytes = max_bytes
        self.view_ttl = ttl_seconds
        self.root_count_ttl = ttl_seconds
        self.root_page_ttl = ttl_seconds
        self.root_result_ttl = ttl_seconds
        self.grand_total_ttl = ttl_seconds
        self.view_size = 32
        self.root_count_size = 64
        self.root_page_size = 128
        self.root_result_size = 8
        self.grand_total_size = 64
        for name in ("view", "root_count", "root_page", "root_result", "grand_total"):
            setattr(self, name, BoundedHierarchyCache(
                ttl=ttl_seconds, max_size=getattr(self, name + "_size"),
                max_bytes=max_bytes, governor=governor,
            ))
        self._register_namespaces()

    def store(self, name: str, key: str, value: Any, *, ttl: float, max_size: int) -> bool:
        cache = getattr(self, name)
        namespace = getattr(CacheNamespaces, "CONTROLLER_HIERARCHY_" + name.upper())
        with cache.storage._lock:
            cache.storage.max_size = max_size
            before = cache.size_snapshot()
            replacing = key in cache
            accepted = cache.set_entry(key, (value, time.time() + ttl))
            after = cache.size_snapshot()
            if accepted:
                self.coordinator.record_store(namespace, entries=after["entries"], bytes_used=after["bytes"])
            evicted = before["entries"] + int(accepted and not replacing) - after["entries"]
            if evicted > 0:
                self.coordinator.record_eviction(namespace, count=evicted,
                                                entries=after["entries"], bytes_used=after["bytes"])
            return accepted

    def _register_namespaces(self) -> None:
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_VIEW,
            owner="controller.hierarchy",
            key_dimensions=("table", "spec_fingerprint", "expanded_paths", "row_window"),
            max_entries=self.view_size,
            ttl_seconds=self.view_ttl,
            max_bytes=self.max_bytes,
            size_getter=self.view.size_snapshot,
            invalidator=self._invalidate_view,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_COUNT,
            owner="controller.hierarchy",
            key_dimensions=("table", "rows", "filters"),
            max_entries=self.root_count_size,
            ttl_seconds=self.root_count_ttl,
            max_bytes=self.max_bytes,
            size_getter=self.root_count.size_snapshot,
            invalidator=self._invalidate_root_count,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_PAGE,
            owner="controller.hierarchy",
            key_dimensions=("table", "root_count_key", "sort", "row_window"),
            max_entries=self.root_page_size,
            ttl_seconds=self.root_page_ttl,
            max_bytes=self.max_bytes,
            size_getter=self.root_page.size_snapshot,
            invalidator=self._invalidate_root_page,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_RESULT,
            owner="controller.hierarchy",
            key_dimensions=("table", "root_spec", "sort"),
            max_entries=self.root_result_size,
            ttl_seconds=self.root_result_ttl,
            size_getter=self._root_result_size_snapshot,
            max_bytes=self.max_bytes,
            invalidator=self._invalidate_root_result,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_GRAND_TOTAL,
            owner="controller.hierarchy",
            key_dimensions=("table", "spec_fingerprint", "filters", "measures", "custom_dimensions"),
            max_entries=self.grand_total_size,
            ttl_seconds=self.grand_total_ttl,
            max_bytes=self.max_bytes,
            size_getter=self.grand_total.size_snapshot,
            invalidator=self._invalidate_grand_total,
        )

    def _invalidate_view(self, _event: CacheInvalidationEvent) -> None:
        self.view.clear()

    def _invalidate_root_count(self, _event: CacheInvalidationEvent) -> None:
        self.root_count.clear()

    def _invalidate_root_page(self, _event: CacheInvalidationEvent) -> None:
        self.root_page.clear()

    def _root_result_size_snapshot(self) -> dict[str, int]:
        return self.root_result.size_snapshot()

    def _invalidate_root_result(self, _event: CacheInvalidationEvent) -> None:
        self.root_result.clear()

    def _invalidate_grand_total(self, _event: CacheInvalidationEvent) -> None:
        self.grand_total.clear()
