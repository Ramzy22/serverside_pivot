"""Regression coverage for native database statement cancellation."""

from __future__ import annotations

import asyncio

import pytest

from pivot_engine.backends.execution import QueryCancelledError, QueryTimeoutError
from pivot_engine.backends.duckdb_backend import ConnectionPool
from pivot_engine.backends.duckdb_backend import DuckDBBackend


LONG_DUCKDB_QUERY = {
    "sql": "SELECT sum(i) FROM range(10000000000) AS t(i)",
    "params": [],
}


class _InterruptingCursor:
    def __init__(self) -> None:
        self.interrupt_calls = 0

    def interrupt(self) -> None:
        self.interrupt_calls += 1


class _HealthyCursor:
    def __init__(self) -> None:
        self.closed = False

    def execute(self, _sql: str):
        return self

    def fetchone(self):
        return (1,)

    def close(self) -> None:
        self.closed = True


def test_database_execution_interrupts_the_lease_attached_after_cancellation():
    """Removing native cancellation must leave a queued leased query uncancelled."""
    from pivot_engine.backends.execution import DatabaseExecution

    cursor = _InterruptingCursor()
    execution = DatabaseExecution("query-1")

    assert execution.request_cancel("client_cancelled") is True
    execution.attach_lease(cursor)

    assert cursor.interrupt_calls == 1
    assert execution.reason == "client_cancelled"


def test_connection_pool_checkin_discards_a_lease_the_execution_rejected():
    """Removing explicit discard would allow a cancelled cursor to re-enter the pool."""
    created: list[_HealthyCursor] = []

    def factory() -> _HealthyCursor:
        cursor = _HealthyCursor()
        created.append(cursor)
        return cursor

    pool = ConnectionPool(factory, max_size=1)
    lease = pool.checkout()

    pool.checkin(lease, discard=True)
    replacement = pool.checkout()

    assert lease.closed is True
    assert replacement is not lease
    assert len(created) == 2


async def _wait_for_running_query(backend: DuckDBBackend) -> str:
    for _ in range(200):
        query_ids = tuple(backend._running_queries)
        if query_ids:
            return query_ids[0]
        await asyncio.sleep(0.005)
    raise AssertionError("DuckDB statement did not enter the running-query registry.")


@pytest.mark.asyncio
async def test_duckdb_async_cancel_interrupts_the_running_cursor_and_keeps_pool_safe():
    """Removing native cursor interruption would leave this long query running."""
    backend = DuckDBBackend(":memory:", max_connections=1)
    task = asyncio.create_task(backend.execute_async(LONG_DUCKDB_QUERY))
    try:
        query_id = await _wait_for_running_query(backend)

        assert await backend.cancel_query(query_id) is True
        with pytest.raises(QueryCancelledError, match="query_cancelled"):
            await task

        metrics = backend.execution_metrics()
        assert metrics["running"] == 0
        assert metrics["healthy_checkins"] + metrics["discarded_leases"] == 1
        assert backend.execute({"sql": "SELECT 1 AS healthy"}).to_pylist() == [
            {"healthy": 1}
        ]
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        backend.close()


@pytest.mark.asyncio
async def test_duckdb_async_timeout_interrupts_database_without_leaking_capacity():
    """Removing the deadline interrupt would leave a worker and lease occupied."""
    backend = DuckDBBackend(":memory:", max_connections=1)
    try:
        with pytest.raises(QueryTimeoutError, match="query_timeout"):
            await backend.execute_async(LONG_DUCKDB_QUERY, timeout_seconds=0.01)

        metrics = backend.execution_metrics()
        assert metrics["running"] == 0
        assert metrics["timeouts"] == 1
        assert backend.pool.created_count <= backend.pool.max_size
        assert backend.execute({"sql": "SELECT 1 AS healthy"}).to_pylist() == [
            {"healthy": 1}
        ]
    finally:
        backend.close()


@pytest.mark.asyncio
async def test_duckdb_repeated_cancel_requests_interrupt_one_statement_once():
    """Removing idempotency could interrupt a later statement on a reused cursor."""
    backend = DuckDBBackend(":memory:", max_connections=1)
    task = asyncio.create_task(backend.execute_async(LONG_DUCKDB_QUERY))
    try:
        query_id = await _wait_for_running_query(backend)

        assert await backend.cancel_query(query_id) is True
        assert await backend.cancel_query(query_id) is False
        with pytest.raises(QueryCancelledError, match="query_cancelled"):
            await task

        metrics = backend.execution_metrics()
        assert metrics["cancellation_attempts"] == 1
        assert metrics["running"] == 0
        assert backend.pool.created_count <= backend.pool.max_size
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        backend.close()
