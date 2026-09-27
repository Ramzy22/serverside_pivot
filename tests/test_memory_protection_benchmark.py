import asyncio

import pytest

from pivot_engine.backends.duckdb_backend import DuckDBBackend
from pivot_engine.admin_api import get_system_health
from pivot_engine.resource_governor import ResourceGovernor, ResourceLimits
from scripts.benchmark_pivot_runtime import run_memory_benchmark


def test_duckdb_rejects_limit_outside_declared_worker_envelope():
    pytest.importorskip("duckdb")
    with pytest.raises(ValueError, match="worker memory"):
        DuckDBBackend(
            ":memory:",
            memory_limit="900MB",
            resource_limits=ResourceLimits.for_testing(
                worker_hard_limit_bytes=1024**3,
                total_bytes=512 * 1024**2,
                native_headroom_bytes=128 * 1024**2,
            ),
        )


def test_duckdb_resolves_literal_memory_limit_from_environment(monkeypatch):
    pytest.importorskip("duckdb")
    monkeypatch.setenv("PIVOT_DUCKDB_MEMORY_LIMIT", "64MB")
    backend = DuckDBBackend(":memory:")
    try:
        assert backend.memory_limit == "64MB"
    finally:
        backend.close()


def test_memory_benchmark_reports_rss_and_overload_rejections():
    report = run_memory_benchmark(rows=2_000, concurrent_requests=4)
    assert report["memory"]["peakRssBytes"] > 0
    assert report["resourceGovernor"]["rejections"] > 0


def test_authenticated_admin_health_exposes_aggregate_native_memory_metrics(monkeypatch):
    import pivot_engine.admin_api as admin_api

    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 123)
    monkeypatch.setattr(admin_api, "_resource_governor", governor)
    health = asyncio.run(get_system_health())
    assert health["resources"]["rss_bytes"] == 123
    assert health["resources"]["high_water_rss_bytes"] == 123
    assert health["resourceGovernor"]["rejections"] == 0
    assert "query" not in health["resourceGovernor"]["rejections_by_kind"]
