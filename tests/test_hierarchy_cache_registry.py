from collections import OrderedDict
import time
from types import SimpleNamespace

import pytest

from pivot_engine.cache_coordinator import CacheCoordinator, CacheNamespaces
from pivot_engine.hierarchy_cache_registry import BoundedHierarchyCache, HierarchyCacheRegistry
from pivot_engine.resource_governor import ResourceGovernor, ResourceLimits


@pytest.fixture
def advancing_clock(monkeypatch):
    clock = SimpleNamespace(now=1000.0)

    def tick():
        now = clock.now
        clock.now += 0.125
        return now

    clock.time = tick
    monkeypatch.setattr("pivot_engine.hierarchy_cache_registry.time", clock)
    monkeypatch.setattr("pivot_engine.cache.memory_cache.time", clock)
    return clock


def test_hierarchy_alias_writes_are_bounded_and_invalidation_releases():
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    coordinator = CacheCoordinator()
    registry = HierarchyCacheRegistry(coordinator, ttl_seconds=60, max_bytes=10, governor=governor)
    assert isinstance(registry.view, OrderedDict)
    registry.view["one"] = ({"x": b"123456"}, time.time() + 60)
    registry.view["two"] = ({"x": b"abcdef"}, time.time() + 60)
    assert "one" not in registry.view
    registry.view["huge"] = ({"x": b"12345678901"}, time.time() + 60)
    assert "huge" not in registry.view
    assert "two" in registry.view
    assert governor.snapshot()["reserved_bytes"] == 7
    metrics = coordinator.snapshot()["namespaces"][CacheNamespaces.CONTROLLER_HIERARCHY_VIEW]
    assert metrics["metrics"]["bytes"] == 7
    coordinator.invalidate([CacheNamespaces.CONTROLLER_HIERARCHY_VIEW], event="test")
    assert governor.snapshot()["reserved_bytes"] == 0


def test_hierarchy_alias_mutations_do_not_bypass_charges():
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    registry = HierarchyCacheRegistry(CacheCoordinator(), ttl_seconds=60, max_bytes=30, governor=governor)
    registry.root_page.update({"a": ([b"abc"], time.time() + 60)})
    registry.root_page.setdefault("b", ([b"de"], time.time() + 60))
    assert governor.snapshot()["reserved_bytes"] == 5
    del registry.root_page["a"]
    assert governor.snapshot()["reserved_bytes"] == 2
    registry.root_page.popitem()
    assert governor.snapshot()["reserved_bytes"] == 0


def test_hierarchy_copy_is_an_ordered_detached_snapshot():
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    registry = HierarchyCacheRegistry(CacheCoordinator(), ttl_seconds=60, max_bytes=5, governor=governor)
    expiry = time.time() + 60
    registry.view.update({"a": (b"abc", expiry), "b": (b"de", expiry)})
    before = list(registry.view.items())

    copied = registry.view.copy()

    assert type(copied) is OrderedDict
    assert list(copied.items()) == before
    assert list(registry.view.items()) == before
    assert governor.snapshot()["reserved_bytes"] == 5
    copied["a"] = (b"too large", expiry)
    copied.pop("b")
    copied["c"] = (b"z", expiry)
    assert list(registry.view.items()) == before
    registry.view.update(copied)
    assert list(registry.view) == ["b", "c"]
    assert registry.view.size_snapshot() == {"entries": 2, "bytes": 3}
    assert governor.snapshot()["reserved_bytes"] == 3
    registry.view.clear()
    assert list(copied) == ["a", "c"]
    assert governor.snapshot()["reserved_bytes"] == 0


def test_hierarchy_fromkeys_preserves_ordered_dict_factory_semantics():
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    registry = HierarchyCacheRegistry(CacheCoordinator(), ttl_seconds=60, max_bytes=5, governor=governor)
    empty_values = BoundedHierarchyCache.fromkeys(["b", "a", "b"])
    assert type(empty_values) is OrderedDict
    assert list(empty_values.items()) == [("b", None), ("a", None)]

    entry = (b"abc", time.time() + 60)
    generated = registry.view.fromkeys(iter(["b", "a", "b"]), entry)
    assert type(generated) is OrderedDict
    assert list(generated.items()) == [("b", entry), ("a", entry)]
    assert generated["a"] is generated["b"]
    assert registry.view.size_snapshot() == {"entries": 0, "bytes": 0}
    assert governor.snapshot()["reserved_bytes"] == 0
    registry.view |= generated
    assert list(registry.view) == ["a"]
    assert governor.snapshot()["reserved_bytes"] == 3
    generated.clear()
    assert list(registry.view) == ["a"]
    registry.view.clear()
    assert governor.snapshot()["reserved_bytes"] == 0


@pytest.mark.parametrize("reverse", [False, True])
def test_hierarchy_union_is_detached_and_readmission_is_bounded(reverse, advancing_clock):
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    registry = HierarchyCacheRegistry(CacheCoordinator(), ttl_seconds=60, max_bytes=5, governor=governor)
    expiry = advancing_clock.time() + 60
    registry.view.update({"a": (b"abc", expiry), "b": (b"de", expiry)})
    before = list(registry.view.items())
    other = {"b": (b"z", expiry), "huge": (b"oversized", expiry)}

    merged = other | registry.view if reverse else registry.view | other

    assert type(merged) is OrderedDict
    expected = [("b", (b"de", expiry)), ("huge", (b"oversized", expiry)), ("a", (b"abc", expiry))] if reverse else [
        ("a", (b"abc", expiry)), ("b", (b"z", expiry)), ("huge", (b"oversized", expiry))
    ]
    assert list(merged.items()) == expected
    assert list(registry.view.items()) == before
    assert governor.snapshot()["reserved_bytes"] == 5
    merged["a"] = (b"oversized", expiry)
    alias = registry.view
    registry.view |= merged
    assert registry.view is alias
    assert "huge" not in alias
    assert alias["a"][0] == b"abc"
    assert alias.size_snapshot() == {"entries": 2, "bytes": 5 if reverse else 4}
    assert governor.snapshot()["reserved_bytes"] == (5 if reverse else 4)
    alias.clear()
    assert len(merged) == 3
    assert governor.snapshot()["reserved_bytes"] == 0


def test_hierarchy_readmission_preserves_absolute_deadline_and_atomic_charges(advancing_clock):
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    registry = HierarchyCacheRegistry(CacheCoordinator(), ttl_seconds=60, max_bytes=5, governor=governor)
    expiry = advancing_clock.time() + 60
    entry = (b"abc", expiry)
    assert registry.view.set_entry("a", entry)
    assert registry.view["a"] == entry
    assert registry.view.storage._cache["a"][1] == expiry
    assert governor.snapshot()["reserved_bytes"] == 3

    snapshot = registry.view.copy()
    registry.view |= snapshot
    assert registry.view["a"] == entry
    assert governor.snapshot()["reserved_bytes"] == 3
    assert not registry.view.set_entry("a", (b"oversized", expiry + 100))
    assert registry.view["a"] == entry
    assert governor.snapshot()["reserved_bytes"] == 3

    advancing_clock.now = expiry
    assert registry.view.get("a") is None
    assert registry.view.size_snapshot() == {"entries": 0, "bytes": 0}
    assert governor.snapshot()["reserved_bytes"] == 0
    assert snapshot == OrderedDict([("a", entry)])
