from types import SimpleNamespace

from pivot_engine.adapter_viewport_cache import AdapterViewportCache
from pivot_engine.tanstack_adapter import TanStackPivotAdapter


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
