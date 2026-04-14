
"""
Simple in-memory cache with TTL support for various data types including PyArrow Tables.
"""
import time
import threading
from typing import Optional, Any, Dict, Union
from collections import OrderedDict
import pyarrow as pa

class MemoryCache:
    """
    An in-memory cache for various data types including PyArrow Tables with a time-to-live (TTL) and LRU eviction.
    """
    def __init__(self, ttl: int = 300, max_size: int = 1000):
        """
        Initialize the cache.

        Args:
            ttl: Default time-to-live for cache entries in seconds.
            max_size: Maximum number of items to hold in cache (LRU eviction).
        """
        # OrderedDict for LRU: 
        # Popitem(last=False) removes from beginning (Least Recently Used)
        # Move to end on access (Most Recently Used)
        self._cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.default_ttl = ttl
        self.max_size = max_size
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve an item from the cache.

        Args:
            key: The key of the item to retrieve.

        Returns:
            The cached item, or None if the item is not found or expired.
        """
        with self._lock:
            if key not in self._cache:
                return None

            value, expiry = self._cache[key]

            if time.time() > expiry:
                # Entry has expired
                del self._cache[key]
                return None

            # LRU: Move to end (mark as recently used)
            self._cache.move_to_end(key)
            return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Add an item to the cache.

        Args:
            key: The key of the item to add.
            value: The item to add to the cache (any type).
            ttl: Time-to-live for this specific entry. If None, use default.
        """
        ttl_to_use = ttl if ttl is not None else self.default_ttl
        expiry = time.time() + ttl_to_use
        
        with self._lock:
            if key in self._cache:
                # Update existing
                self._cache.move_to_end(key)
            self._cache[key] = (value, expiry)

            # Evict if over size
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

    def clear(self):
        """Clear all items from the cache."""
        with self._lock:
            self._cache.clear()

    def delete(self, key: str):
        """Delete a specific key from the cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def get_all_keys(self):
        """Get all keys in the cache (for cache invalidation purposes)."""
        # Remove expired entries first and return valid keys
        with self._lock:
            current_time = time.time()
            valid_keys = []
            # Create a copy of items to iterate safely while deleting
            for key, (value, expiry) in list(self._cache.items()):
                if current_time <= expiry:
                    valid_keys.append(key)
                else:
                    # Remove expired entries
                    del self._cache[key]
            return valid_keys

