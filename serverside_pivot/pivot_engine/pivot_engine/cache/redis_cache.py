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
from typing import Optional, Any, Dict


_ARROW_PREFIX = b"pivot:arrow-ipc:v1\x00"
_JSON_PREFIX = b"pivot:json:v1\x00"


def _is_json_value(value: Any) -> bool:
    """Accept the JSON data model, not Python's broader pickle model."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, list):
        return all(_is_json_value(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) and _is_json_value(item) for key, item in value.items())
    return False

class RedisCache:
    """
    A cache implementation that uses Redis as the backend.
    It stores various data types with automatic serialization.
    """

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        ttl: int = 300,
        *,
        url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl_cert_reqs: str = "required",
    ):
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
            if url:
                self.client = redis.Redis.from_url(
                    url,
                    decode_responses=False,
                    ssl_cert_reqs=ssl_cert_reqs,
                )
            else:
                self.client = redis.StrictRedis(
                    host=host,
                    port=port,
                    db=db,
                    username=username,
                    password=password,
                    decode_responses=False,
                    ssl_cert_reqs=ssl_cert_reqs,
                )
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
        if isinstance(cached_value, str):
            cached_value = cached_value.encode("utf-8")
        if isinstance(cached_value, bytes):
            if cached_value.startswith(_ARROW_PREFIX):
                try:
                    buffer = pa.py_buffer(cached_value[len(_ARROW_PREFIX):])
                    return ipc.open_stream(buffer).read_all()
                except (pa.ArrowInvalid, OSError, ValueError):
                    return None
            if cached_value.startswith(_JSON_PREFIX):
                try:
                    return json.loads(cached_value[len(_JSON_PREFIX):].decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    return None
            # Never attempt to deserialize legacy/unmarked bytes. In
            # particular, a Redis writer must not be able to trigger pickle
            # execution in the application process.
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
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, value.schema) as writer:
                writer.write_table(value)
            buffer = _ARROW_PREFIX + sink.getvalue().to_pybytes()
        elif _is_json_value(value):
            # Persist only an explicit, versioned JSON representation. This
            # format is data-only and does not execute code when read.
            try:
                buffer = _JSON_PREFIX + json.dumps(
                    value,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise TypeError("RedisCache values must be JSON-compatible or pyarrow.Table.") from exc
        else:
            raise TypeError("RedisCache values must be JSON-compatible or pyarrow.Table.")

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
