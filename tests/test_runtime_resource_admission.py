"""Worker resource admission and cleanup ordering at runtime boundaries."""

import asyncio
import threading

import pytest

from pivot_engine.resource_governor import ResourceExhaustedError, ResourceGovernor, ResourceKind, ResourceLimits
from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState
from pivot_engine.runtime.lifecycle import RuntimeRequestLifecycle
from pivot_engine.runtime.resilience import CircuitBreaker, PivotRequestTimeout
from pivot_engine.tanstack_adapter import TanStackResponse
from pivot_engine.backends.duckdb_backend import DuckDBBackend


def _governor(**limits):
    return ResourceGovernor(ResourceLimits.for_testing(**limits), rss_bytes_getter=lambda: 0)


@pytest.mark.asyncio
async def test_runtime_rejects_second_query_before_invoking_backend():
    governor = _governor(max_queries=1, query_bytes=64)
    lifecycle = RuntimeRequestLifecycle(resource_governor=governor)
    context = PivotRequestContext(table="sales")
    started, release = asyncio.Event(), asyncio.Event()

    async def blocking():
        started.set()
        await release.wait()
        return "done"

    def forbidden():
        pytest.fail("overloaded request reached backend")

    first = asyncio.create_task(lifecycle.run_backend_operation(
        context, "data", blocking, resource_kind=ResourceKind.QUERY, reservation_bytes=64,
    ))
    try:
        await asyncio.wait_for(started.wait(), 1)
        with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
            await lifecycle.run_backend_operation(
                context, "data", forbidden, resource_kind=ResourceKind.QUERY, reservation_bytes=1,
            )
    finally:
        release.set()
        assert await first == "done"
    assert governor.snapshot()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_rss_rejection_and_nested_exhaustion_do_not_trip_circuit():
    rss = [4000]
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: rss[0])
    breaker = CircuitBreaker(failure_threshold=1)
    lifecycle = RuntimeRequestLifecycle(resource_governor=governor, circuit_breaker=breaker)
    context = PivotRequestContext(table="sales")

    def forbidden():
        pytest.fail("RSS guard must run before backend")

    with pytest.raises(ResourceExhaustedError) as error:
        await lifecycle.run_backend_operation(context, "data", forbidden)
    assert error.value.reason == "rss_soft_limit"
    rss[0] = 0

    def nested_rejection():
        raise ResourceExhaustedError(ResourceKind.PAYLOAD, reason="private capacity detail")

    with pytest.raises(ResourceExhaustedError):
        await lifecycle.run_backend_operation(context, "data", nested_rejection)
    assert breaker.snapshot(lifecycle.circuit_key(context, "data")) == {"failures": 0, "open": False}
    assert await lifecycle.run_backend_operation(context, "data", lambda: "healthy") == "healthy"
    assert governor.snapshot()["reserved_bytes"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_capacity_is_held_until_cancelled_backend_cleanup_completes(timeout):
    governor = _governor(max_queries=1, query_bytes=64)
    lifecycle = RuntimeRequestLifecycle(
        resource_governor=governor, request_timeout_seconds=0.01 if timeout else 30,
    )
    context = PivotRequestContext(table="sales")
    started, cleanup_started, cleanup_release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def backend():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await cleanup_release.wait()

    task = asyncio.create_task(lifecycle.run_backend_operation(
        context, "data", backend, resource_kind=ResourceKind.QUERY, reservation_bytes=64,
    ))
    try:
        await asyncio.wait_for(started.wait(), 1)
        if not timeout:
            task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), 1)
        # A repeated cancellation must not propagate into the cleanup wait.
        if not timeout:
            task.cancel()
            await asyncio.sleep(0)
        assert governor.snapshot()["reserved_bytes_by_kind"]["query"] == 64
        with pytest.raises(ResourceExhaustedError):
            await lifecycle.run_backend_operation(context, "data", lambda: None)
    finally:
        cleanup_release.set()
        with pytest.raises(PivotRequestTimeout if timeout else asyncio.CancelledError):
            await task
    assert governor.snapshot()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_export_formatting_remains_inside_export_reservation():
    governor = _governor(max_exports=1, export_bytes=64)

    class Adapter:
        async def handle_request(self, *_args, **_kwargs):
            return TanStackResponse(data=[{"value": 1}], columns=[{"id": "value"}], total_rows=1)

        handle_virtual_scroll_request = handle_request

    service = PivotRuntimeService(lambda: Adapter(), resource_governor=governor)
    writer = service._export_service.build_export_payload

    def checked_writer(*args):
        assert governor.snapshot()["active_slots"]["export"] == 1
        return writer(*args)

    service._export_service.build_export_payload = checked_writer
    response = await service.process_async(
        PivotViewState(export_request={"format": "csv"}), PivotRequestContext(table="sales"),
    )
    try:
        assert response.status == "export"
        assert governor.snapshot()["reserved_bytes"] == 0
    finally:
        import os
        if response.export_payload and response.export_payload.get("contentPath"):
            os.remove(response.export_payload["contentPath"])


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["data", "detail", "drill", "chart", "export"])
async def test_service_routes_read_and_export_commands_to_shared_capacity(kind):
    governor = _governor(max_queries=1, max_exports=1, query_bytes=64, export_bytes=64)

    class Adapter:
        async def handle_request(self, *_args, **_kwargs):
            pytest.fail("capacity rejection must precede backend invocation")

    service = PivotRuntimeService(lambda: Adapter(), resource_governor=governor)
    state = PivotViewState(
        detail_request={"offset": 0} if kind == "detail" else None,
        drill_through={"rowPath": []} if kind == "drill" else None,
        export_request={"format": "csv"} if kind == "export" else {},
    )
    context = PivotRequestContext(table="sales", trigger_prop={
        "detail": "pivot.detailRequest", "drill": "pivot.drillThrough",
        "chart": "pivot.chartRequest", "data": "pivot.viewport", "export": "pivot.export",
    }[kind])
    resource_kind = ResourceKind.EXPORT if kind == "export" else ResourceKind.QUERY
    with governor.reserve(resource_kind, 64):
        with pytest.raises(ResourceExhaustedError) as error:
            await service.process_async(state, context)
        assert error.value.resource_kind == resource_kind
    assert governor.snapshot()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_independent_service_sessions_share_one_query_slot():
    governor = _governor(max_queries=1, query_bytes=64)
    started, release = asyncio.Event(), asyncio.Event()

    class Adapter:
        async def handle_request(self, *_args, **_kwargs):
            started.set()
            await release.wait()
            return TanStackResponse(data=[], columns=[], total_rows=0)

    service = PivotRuntimeService(lambda: Adapter(), resource_governor=governor)
    first = asyncio.create_task(service.process_async(PivotViewState(), PivotRequestContext(table="sales", session_id="one")))
    try:
        await asyncio.wait_for(started.wait(), 1)
        with pytest.raises(ResourceExhaustedError):
            await service.process_async(PivotViewState(), PivotRequestContext(table="sales", session_id="two"))
    finally:
        release.set()
        assert (await first).status == "data"
    assert governor.snapshot()["reserved_bytes"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("native_timeout", [False, True])
async def test_native_cursor_checkin_finishes_before_runtime_releases_capacity(monkeypatch, native_timeout):
    governor = _governor(max_queries=1, query_bytes=64)
    lifecycle = RuntimeRequestLifecycle(resource_governor=governor)
    backend = DuckDBBackend(":memory:", max_connections=1)
    checkin_started, finish_checkin = threading.Event(), threading.Event()
    original_checkin = backend.pool.checkin

    def delayed_checkin(*args, **kwargs):
        checkin_started.set()
        if not finish_checkin.wait(5):
            raise AssertionError("test did not release native cleanup")
        return original_checkin(*args, **kwargs)

    monkeypatch.setattr(backend.pool, "checkin", delayed_checkin)
    context = PivotRequestContext(table="sales")
    task = asyncio.create_task(lifecycle.run_backend_operation(
        context, "data",
        lambda: backend.execute_async(
            {"sql": "SELECT sum(i) FROM range(10000000000) AS t(i)", "params": []},
            timeout_seconds=0.01 if native_timeout else 30,
        ),
        resource_kind=ResourceKind.QUERY, reservation_bytes=64,
    ))
    try:
        for _ in range(200):
            if backend._running_queries:
                break
            await asyncio.sleep(0.005)
        assert backend._running_queries
        if not native_timeout:
            task.cancel()
        for _ in range(200):
            if checkin_started.is_set():
                break
            await asyncio.sleep(0.005)
        assert checkin_started.is_set()
        assert governor.snapshot()["active_slots"]["query"] == 1
        with pytest.raises(ResourceExhaustedError):
            await lifecycle.run_backend_operation(context, "data", lambda: None)
    finally:
        finish_checkin.set()
        try:
            with pytest.raises(PivotRequestTimeout if native_timeout else asyncio.CancelledError):
                await task
            assert backend.execution_metrics()["running"] == 0
            assert governor.snapshot()["reserved_bytes"] == 0
        finally:
            backend.close()
