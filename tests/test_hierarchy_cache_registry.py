from collections import OrderedDict
import time

from pivot_engine.cache_coordinator import CacheCoordinator, CacheNamespaces
from pivot_engine.hierarchy_cache_registry import HierarchyCacheRegistry
from pivot_engine.resource_governor import ResourceGovernor, ResourceLimits


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
