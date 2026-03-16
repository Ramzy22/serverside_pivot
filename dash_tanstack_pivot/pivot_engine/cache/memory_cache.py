
"""
Simple in-memory cache with TTL support for various data types including PyArrow Tables.
"""
import time
from typing import Optional, Any, Dict, Union
import pyarrow as pa

class MemoryCache:
    """
    An in-memory cache for various data types including PyArrow Tables with a time-to-live (TTL).
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        # Singleton pattern to ensure one cache instance per process
        if not cls._instance:
            cls._instance = super(MemoryCache, cls).__new__(cls)
        return cls._instance

    def __init__(self, ttl: int = 300):
        """
        Initialize the cache.

        Args:
            ttl: Default time-to-live for cache entries in seconds.
        """
        self._cache: Dict[str, tuple[Any, float]] = {}
        self.default_ttl = ttl

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve an item from the cache.

        Args:
            key: The key of the item to retrieve.

        Returns:
            The cached item, or None if the item is not found or expired.
        """
        if key not in self._cache:
            return None

        value, expiry = self._cache[key]

        if time.time() > expiry:
            # Entry has expired
            del self._cache[key]
            return None

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
        self._cache[key] = (value, expiry)

    def clear(self):
        """Clear all items from the cache."""
        self._cache.clear()

    def delete(self, key: str):
        """Delete a specific key from the cache."""
        if key in self._cache:
            del self._cache[key]

    def get_all_keys(self):
        """Get all keys in the cache (for cache invalidation purposes)."""
        # Remove expired entries first and return valid keys
        current_time = time.time()
        valid_keys = []
        for key, (value, expiry) in list(self._cache.items()):
            if current_time <= expiry:
                valid_keys.append(key)
            else:
                # Remove expired entries
                del self._cache[key]
        return valid_keys

