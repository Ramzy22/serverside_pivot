"""Byte-bounded, process-local LRU cache with TTL and optional admission leases."""

import threading
import time
from collections import OrderedDict
from typing import Any, Optional, Protocol

from ..resource_governor import (
    ResourceExhaustedError, ResourceGovernor, ResourceKind, ResourceLease,
    ResourceLimits, estimate_value_bytes,
)


class CacheProtocol(Protocol):
    """Minimal cache protocol shared by local and guarded cache callers."""

    def get(self, key: str) -> Optional[Any]: ...
    def set(self, key: str, value: Any, ttl: Optional[int] = None): ...
    def clear(self): ...
    def delete(self, key: str): ...
    def get_all_keys(self): ...


class MemoryCache:
    """Bound known value bytes; this is not a measure of native allocations."""

    def __init__(self, ttl: int = 300, max_size: int = 1000, *,
                 max_bytes: int = ResourceLimits().cache_bytes,
                 governor: Optional[ResourceGovernor] = None):
        self._cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._charges: dict[str, tuple[int, Optional[ResourceLease]]] = {}
        self.default_ttl = ttl
        self.max_size = max(0, int(max_size))
        self.max_bytes = max(0, int(max_bytes))
        self.governor = governor
        self._bytes = 0
        self._lock = threading.RLock()

    @property
    def bytes_used(self) -> int:
        with self._lock:
            return self._bytes

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.time() >= expiry:
                self._remove(key)
                return None
            self._cache.move_to_end(key)
            return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        charge = estimate_value_bytes(value)
        # Reject before touching the existing LRU, including replacements.
        if charge is None or charge > self.max_bytes or self.max_size == 0:
            return False
        expiry = time.time() + (self.default_ttl if ttl is None else ttl)
        with self._lock:
            self.get_all_keys()
            self._remove(key)
            while self._cache and (
                len(self._cache) >= self.max_size or self._bytes + charge > self.max_bytes
            ):
                self._remove(next(iter(self._cache)))
            lease = None
            if self.governor is not None and charge:
                try:
                    lease = self.governor.reserve(ResourceKind.CACHE, charge)
                except ResourceExhaustedError:
                    return False
            self._cache[key] = (value, expiry)
            self._charges[key] = (charge, lease)
            self._bytes += charge
            return True

    def _remove(self, key: str) -> None:
        if key not in self._cache:
            return
        del self._cache[key]
        charge, lease = self._charges.pop(key)
        self._bytes -= charge
        if lease is not None:
            lease.release()

    def clear(self):
        with self._lock:
            for key in list(self._cache):
                self._remove(key)

    def delete(self, key: str):
        with self._lock:
            self._remove(key)

    def get_all_keys(self):
        with self._lock:
            now = time.time()
            for key, (_, expiry) in list(self._cache.items()):
                if now >= expiry:
                    self._remove(key)
            return list(self._cache)

