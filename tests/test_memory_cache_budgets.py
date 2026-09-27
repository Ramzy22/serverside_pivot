import pyarrow as pa
import pytest

from pivot_engine.cache.memory_cache import MemoryCache
from pivot_engine.resource_governor import ResourceGovernor, ResourceLimits


def governor():
    return ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)


def test_memory_cache_evicts_lru_by_total_bytes_and_rejects_one_oversize_value():
    cache = MemoryCache(max_size=10, max_bytes=10)
    assert cache.set("old", b"123456") is True
    assert cache.set("new", b"abcdef") is True
    assert cache.get("old") is None
    assert cache.set("oversize", b"01234567890") is False
    assert cache.get("new") == b"abcdef"
    assert cache.set("unknown", object()) is False
    assert cache.get("new") == b"abcdef"


def test_cache_replacement_delete_clear_and_expiry_release_exact_charge(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("pivot_engine.cache.memory_cache.time.time", lambda: clock[0])
    budget = governor()
    cache = MemoryCache(ttl=5, max_bytes=20, governor=budget)
    assert cache.set("a", b"1234")
    assert cache.set("a", b"12")
    assert budget.snapshot()["reserved_bytes"] == 2
    assert cache.set("b", b"345")
    cache.delete("a")
    cache.delete("a")
    assert budget.snapshot()["reserved_bytes"] == 3
    clock[0] = 106.0
    assert cache.get_all_keys() == []
    assert budget.snapshot()["reserved_bytes"] == 0
    assert cache.set("c", b"12")
    clock[0] = 112.0
    assert cache.get("c") is None
    assert budget.snapshot()["reserved_bytes"] == 0
    assert cache.set("d", b"123")
    cache.clear()
    cache.clear()
    assert cache.bytes_used == 0
    assert budget.snapshot()["reserved_bytes"] == 0


def test_cache_arrow_sizing_and_eviction_release():
    budget = governor()
    cache = MemoryCache(max_bytes=16, governor=budget)
    assert cache.set("arrow", pa.table({"a": pa.array([1, 2], type=pa.int64())}))
    assert cache.bytes_used == 16
    assert cache.set("bytes", b"a")
    assert cache.get("arrow") is None
    assert budget.snapshot()["reserved_bytes"] == 1


def test_cache_admission_failure_and_invalid_replacement_preserve_existing():
    budget = governor()
    cache = MemoryCache(max_bytes=10, governor=budget)
    assert cache.set("a", b"123")
    assert not cache.set("a", object())
    assert not cache.set("a", b"12345678901")
    assert cache.get("a") == b"123"
    budget._rss_bytes_getter = lambda: 10000
    assert not cache.set("b", b"x")
    assert cache.get("a") == b"123"
    assert budget.snapshot()["reserved_bytes"] == 3


def test_cache_zero_byte_values_have_no_phantom_charge():
    budget = governor()
    cache = MemoryCache(max_bytes=1, governor=budget)
    assert cache.set("empty", b"")
    assert cache.get("empty") == b""
    assert cache.bytes_used == budget.snapshot()["reserved_bytes"] == 0
