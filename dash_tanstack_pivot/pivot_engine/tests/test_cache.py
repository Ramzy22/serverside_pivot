"""
Tests for cache implementations.
"""
import pytest
import time
import pyarrow as pa
from pivot_engine.cache.memory_cache import MemoryCache
from pivot_engine.cache.redis_cache import RedisCache

# Conditional import for fakeredis
try:
    import fakeredis
    FAKEREDIS_AVAILABLE = True
except ImportError:
    FAKEREDIS_AVAILABLE = False

@pytest.fixture(params=["memory", "redis"])
def cache(request):
    """Fixture to test both memory and Redis cache."""
    if request.param == "memory":
        yield MemoryCache(ttl=10)
    elif request.param == "redis":
        if not FAKEREDIS_AVAILABLE:
            pytest.skip("fakeredis is not installed, skipping Redis cache tests.")
        
        # Use fakeredis for testing
        fake_redis_client = fakeredis.FakeStrictRedis(decode_responses=True)
        
        # Monkeypatch the redis.StrictRedis client in the RedisCache
        class MockRedisCache(RedisCache):
            def __init__(self, ttl=10):
                self.client = fake_redis_client
                self.default_ttl = ttl

        cache_instance = MockRedisCache()
        yield cache_instance
        cache_instance.clear()


def test_cache_set_get(cache):
    """Test setting and getting a simple value."""
    sample_table = pa.table({"a": [1, 2, 3]})
    cache.set("key1", sample_table)
    retrieved_table = cache.get("key1")
    assert retrieved_table is not None
    assert retrieved_table.equals(sample_table)

def test_cache_get_nonexistent(cache):
    """Test getting a nonexistent key."""
    assert cache.get("nonexistent") is None

def test_cache_set_overwrite(cache):
    """Test that setting a key twice overwrites the value."""
    table1 = pa.table({"a": [1, 2, 3]})
    table2 = pa.table({"b": [4, 5, 6]})
    cache.set("key1", table1)
    cache.set("key1", table2)
    retrieved_table = cache.get("key1")
    assert retrieved_table is not None
    assert retrieved_table.equals(table2)
    assert not retrieved_table.equals(table1)

def test_cache_clear(cache):
    """Test clearing the cache."""
    table1 = pa.table({"a": [1]})
    table2 = pa.table({"b": [2]})
    cache.set("key1", table1)
    cache.set("key2", table2)
    cache.clear()
    assert cache.get("key1") is None
    assert cache.get("key2") is None

def test_cache_ttl(cache):
    """Test that the cache entry expires after the TTL."""
    sample_table = pa.table({"ttl_test": [1]})
    cache.set("key_ttl", sample_table, ttl=1)
    
    assert cache.get("key_ttl").equals(sample_table)
    
    time.sleep(1.1)
    
    assert cache.get("key_ttl") is None

def test_cache_complex_value(cache):
    """Test caching a more complex table."""
    complex_table = pa.table({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
        "col3": [True, False, True]
    })
    cache.set("complex", complex_table)
    retrieved = cache.get("complex")
    assert retrieved.equals(complex_table)
