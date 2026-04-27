import pyarrow as pa

from pivot_engine.cache_coordinator import CacheCoordinator, CacheNamespaces
from pivot_engine.runtime.payload_store import RuntimePayloadStore
from pivot_engine.scalable_pivot_controller import ScalablePivotController


def test_cache_coordinator_records_namespace_metrics_and_invalidation_events():
    coordinator = CacheCoordinator()
    invalidated = []

    coordinator.register_namespace(
        "unit.cache",
        owner="unit",
        key_dimensions=("table", "sort"),
        max_entries=2,
        ttl_seconds=10,
        size_getter=lambda: {"entries": 1},
        invalidator=invalidated.append,
    )

    coordinator.record_lookup("unit.cache", hit=True)
    coordinator.record_lookup("unit.cache", hit=False)
    coordinator.record_store("unit.cache", entries=1)
    coordinator.record_eviction("unit.cache", count=1, entries=1)
    event = coordinator.invalidate(
        ["unit.cache"],
        event="mutation",
        reason="test",
        table="sales_data",
        structural=True,
    )

    snapshot = coordinator.snapshot(include_events=True)
    namespace = snapshot["namespaces"]["unit.cache"]

    assert namespace["spec"]["keyDimensions"] == ["table", "sort"]
    assert namespace["spec"]["maxEntries"] == 2
    assert namespace["metrics"]["hits"] == 1
    assert namespace["metrics"]["misses"] == 1
    assert namespace["metrics"]["stores"] == 1
    assert namespace["metrics"]["evictions"] == 1
    assert namespace["metrics"]["invalidations"] == 1
    assert namespace["metrics"]["generation"] == 1
    assert snapshot["events"][-1]["sequence"] == event.sequence
    assert invalidated == [event]


def test_runtime_payload_store_exposes_ttl_lru_namespace_metrics():
    store = RuntimePayloadStore(default_ttl_seconds=60, max_entries=1, max_bytes=1024)

    first = store.put_bytes(b"first")
    assert store.get(first["id"]) is not None

    second = store.put_bytes(b"second")

    assert store.get(first["id"]) is None
    assert store.get(second["id"]) is not None

    namespace = store.metrics_snapshot()["namespaces"][CacheNamespaces.RUNTIME_PAYLOAD]
    assert namespace["spec"]["maxEntries"] == 1
    assert namespace["spec"]["maxBytes"] == 1024
    assert namespace["metrics"]["stores"] == 2
    assert namespace["metrics"]["hits"] == 2
    assert namespace["metrics"]["misses"] == 1
    assert namespace["metrics"]["evictions"] == 1
    assert namespace["metrics"]["entries"] == 1


def test_controller_mutation_invalidation_uses_cache_coordinator_namespaces():
    controller = ScalablePivotController(backend_uri=":memory:")
    table_name = "sales_data"
    table_key = controller._cache_table_key(table_name)

    controller.cache.set(f"pivot_ibis:query:{table_key}:abc", {"stale": True})
    controller._hierarchy_view_cache_set(
        "view",
        {
            "expanded_set": set(),
            "hierarchy_result": {},
            "visible_rows": [],
            "grand_total_row": None,
            "grand_total_formula_source_rows": [],
        },
    )
    controller._hierarchy_grand_total_cache_set("total", {"_id": "Grand Total"})
    controller._hierarchy_root_count_cache_set("count", 4)
    controller._hierarchy_root_page_cache_set("page", ["North", "South"])

    controller._clear_mutation_caches(table_name, structural=False)

    assert controller.cache.get(f"pivot_ibis:query:{table_key}:abc") is None
    assert controller._hierarchy_view_cache_get("view") is None
    assert controller._hierarchy_grand_total_cache_get("total") is None
    assert controller._hierarchy_root_count_cache_get("count") == 4
    assert controller._hierarchy_root_page_cache_get("page") == ["North", "South"]

    controller._clear_mutation_caches(table_name, structural=True)

    assert controller._hierarchy_root_count_cache_get("count") is None
    assert controller._hierarchy_root_page_cache_get("page") is None

    snapshot = controller.cache_coordinator.snapshot(include_events=True)
    assert snapshot["namespaces"][CacheNamespaces.CONTROLLER_QUERY]["metrics"]["invalidations"] >= 2
    assert snapshot["namespaces"][CacheNamespaces.CONTROLLER_HIERARCHY_VIEW]["metrics"]["invalidations"] >= 2
    assert snapshot["namespaces"][CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_COUNT]["metrics"]["invalidations"] == 1
    assert snapshot["events"][-1]["event"] == "mutation"
    assert snapshot["events"][-1]["structural"] is True


def test_data_load_invalidation_reaches_adapter_namespaces_after_registration():
    from pivot_engine.tanstack_adapter import create_tanstack_adapter

    adapter = create_tanstack_adapter(backend_uri=":memory:")
    generation = adapter._local_cache_generation

    adapter._locked_cache_store(
        adapter._response_window_cache,
        "window",
        {"value": 1},
        max_size=8,
        ttl_seconds=60,
    )
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict({"region": ["North"], "sales": [1]}),
    )

    assert adapter._local_cache_generation == generation + 1
    assert adapter._locked_cache_lookup(adapter._response_window_cache, "window") is None
    snapshot = adapter.cache_coordinator.snapshot()
    assert snapshot["namespaces"][CacheNamespaces.ADAPTER_RESPONSE_WINDOW]["metrics"]["invalidations"] >= 1
