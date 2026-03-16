"""
Redis-based cache for pivot query results, supporting various data types including Arrow IPC format.
"""
try:
    import redis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False
    redis = None

import pyarrow as pa
import pyarrow.ipc as ipc
import json
import pickle
from typing import Optional, Any, Dict

class RedisCache:
    """
    A cache implementation that uses Redis as the backend.
    It stores various data types with automatic serialization.
    """

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, ttl: int = 300):
        """
        Initialize the Redis cache.

        Args:
            host: Redis server host.
            port: Redis server port.
            db: Redis database number.
            ttl: Default time-to-live for cache entries in seconds.
        """
        try:
            # Note: decode_responses=False is important as we are storing binary data
            self.client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=False)
            self.client.ping()
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError(f"Could not connect to Redis at {host}:{port}. Please ensure Redis is running.") from e

        self.default_ttl = ttl

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve an item from the cache.

        Args:
            key: The key of the item to retrieve.

        Returns:
            The cached item, or None if the item is not found.
        """
        cached_value = self.client.get(key)
        if cached_value:
            try:
                # Try to deserialize as Arrow table first (for backward compatibility)
                buffer = pa.py_buffer(cached_value)
                return ipc.read_table(buffer)
            except pa.lib.ArrowInvalid:
                # Try to deserialize as JSON
                try:
                    return json.loads(cached_value.decode('utf-8'))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Try to deserialize as pickle
                    try:
                        return pickle.loads(cached_value)
                    except (pickle.PickleError, EOFError):
                        # If all attempts fail, return None
                        return None
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Add an item to the cache.

        Args:
            key: The key of the item to add.
            value: The item to add to the cache (any type, will be serialized).
            ttl: Time-to-live for the cache entry in seconds.
                 If not provided, the default TTL is used.
        """
        ttl_to_use = ttl if ttl is not None else self.default_ttl

        # Serialize based on type
        if isinstance(value, pa.Table):
            # For Arrow tables, use IPC format (optimal)
            buffer = ipc.serialize_table(value).to_pybytes()
        elif isinstance(value, (dict, list, str, int, float, bool, type(None))):
            # For JSON-serializable types, use JSON
            buffer = json.dumps(value).encode('utf-8')
        else:
            # For other types, use pickle
            buffer = pickle.dumps(value)

        self.client.set(key, buffer, ex=ttl_to_use)

    def clear(self):
        """Clear the entire cache."""
        self.client.flushdb()

    def delete(self, key: str):
        """Delete a specific key from the cache."""
        self.client.delete(key)

    def get_all_keys(self):
        """Get all keys in the Redis cache (for cache invalidation purposes)."""
        # Use SCAN to get all keys without blocking the server
        all_keys = []
        for key in self.client.scan_iter("*"):
            all_keys.append(key.decode('utf-8'))
        return all_keys

    def publish_invalidation(self, channel: str, message: str):
        """Publish a cache invalidation message."""
        self.client.publish(channel, message)

    def subscribe_invalidation(self, channel: str):
        """Subscribe to cache invalidation messages."""
        pubsub = self.client.pubsub()
        pubsub.subscribe(channel)
        return pubsub
