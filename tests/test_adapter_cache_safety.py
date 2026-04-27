from types import SimpleNamespace

from pivot_engine.adapter_viewport_cache import AdapterViewportCache
from pivot_engine.cache_coordinator import CacheNamespaces
from pivot_engine.tanstack_adapter import TanStackOperation, TanStackPivotAdapter, TanStackRequest


def test_adapter_local_cache_abstraction_serializes_mutable_state():
    adapter = TanStackPivotAdapter(SimpleNamespace())

    assert adapter._viewport_cache is not None
    assert adapter._local_cache_lock is not None

    adapter._locked_cache_store(
        adapter._response_window_cache,
        "window",
        {"value": 1},
        max_size=8,
        ttl_seconds=60,
    )
    assert adapter._locked_cache_lookup(adapter._response_window_cache, "window") == {"value": 1}

    adapter._center_col_ids_cache_set(("center",), ["b", "a"])
    assert adapter._center_col_ids_cache_get(("center",)) == ["b", "a"]

    assert adapter._mark_prefetch_request_pending("prefetch") is True
    assert adapter._mark_prefetch_request_pending("prefetch") is False
    adapter._clear_prefetch_request_pending("prefetch")
    assert adapter._mark_prefetch_request_pending("prefetch") is True


def test_adapter_viewport_cache_invalidates_all_namespaces():
    cache = AdapterViewportCache(
        ttl_seconds=60,
        pivot_catalog_size=4,
        response_window_size=4,
        row_block_cache_size=4,
    )
    generation = cache.generation_value()

    cache.center_col_ids_set(("center",), ["a"])
    cache.store(cache.response_window_cache, "window", {"row": 1}, max_size=4, ttl_seconds=60)
    cache.store(cache.row_block_cache, "block", {"rows": []}, max_size=4, ttl_seconds=60)
    cache.store(cache.grand_total_cache, "total", {"_isTotal": True}, max_size=4, ttl_seconds=60)
    cache.store(cache.pivot_column_catalog_cache, "catalog", {"values": ["x"]}, max_size=4, ttl_seconds=60)
    assert cache.mark_prefetch_request_pending("prefetch") is True

    cache.invalidate_all()

    assert cache.generation_value() == generation + 1
    assert cache.center_col_ids_get(("center",)) is None
    assert cache.lookup(cache.response_window_cache, "window") is None
    assert cache.lookup(cache.row_block_cache, "block") is None
    assert cache.lookup(cache.grand_total_cache, "total") is None
    assert cache.lookup(cache.pivot_column_catalog_cache, "catalog") is None
    assert cache.mark_prefetch_request_pending("prefetch") is True

    snapshot = cache.metrics_snapshot()
    assert snapshot["namespaces"][CacheNamespaces.ADAPTER_RESPONSE_WINDOW]["metrics"]["invalidations"] == 1
    assert snapshot["namespaces"][CacheNamespaces.ADAPTER_ROW_BLOCK]["metrics"]["invalidations"] == 1


def _request(**overrides):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        totals=True,
    )
    for key, value in overrides.items():
        setattr(request, key, value)
    return request


def test_adapter_viewport_cache_key_separates_sort_filter_expand_value_and_custom_dimensions():
    adapter = TanStackPivotAdapter(SimpleNamespace())

    def key_for(request, expanded=None):
        return adapter._response_window_cache_key(
            request,
            0,
            50,
            [] if expanded is None else expanded,
            0,
            None,
            True,
            True,
            None,
        )

    base = key_for(_request())
    sorted_key = key_for(_request(sorting=[{"id": "sales_sum", "desc": True}]))
    filtered_key = key_for(_request(filters={"region": ["North"]}))
    expanded_key = key_for(_request(), expanded=[["North"]])
    value_config_key = key_for(
        _request(
            columns=[
                {"id": "region"},
                {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            ]
        )
    )
    custom_dimension_key = key_for(
        _request(
            custom_dimensions=[
                {
                    "id": "sales_band",
                    "field": "__custom_category__sales_band",
                    "rules": [{"label": "Large", "condition": {"field": "sales", "op": ">", "value": 100}}],
                }
            ]
        )
    )

    assert len({base, sorted_key, filtered_key, expanded_key, value_config_key, custom_dimension_key}) == 6


def test_adapter_edit_invalidation_clears_window_blocks_and_bumps_generation():
    adapter = TanStackPivotAdapter(SimpleNamespace())
    generation = adapter._local_cache_generation

    adapter._locked_cache_store(
        adapter._response_window_cache,
        "window",
        {"value": 1},
        max_size=8,
        ttl_seconds=60,
    )
    adapter._locked_cache_store(
        adapter._row_block_cache,
        "block",
        {"rows": []},
        max_size=8,
        ttl_seconds=60,
    )

    adapter._invalidate_local_caches()

    assert adapter._local_cache_generation == generation + 1
    assert adapter._locked_cache_lookup(adapter._response_window_cache, "window") is None
    assert adapter._locked_cache_lookup(adapter._row_block_cache, "block") is None
