"""Regression coverage for native database statement cancellation."""

from __future__ import annotations

import asyncio

import pytest

import pivot_engine.backends.duckdb_backend as duckdb_backend_module
from pivot_engine.backends.execution import DatabaseExecutionError
from pivot_engine.backends.execution import QueryCancelledError, QueryTimeoutError
from pivot_engine.backends.duckdb_backend import ConnectionPool
from pivot_engine.backends.duckdb_backend import DuckDBBackend
from pivot_engine.backends.ibis_backend import IbisBackend
from pivot_engine.core.exec import materialize
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.transport.core_bridge import run_core_read


LONG_DUCKDB_QUERY = {
    "sql": "SELECT sum(i) FROM range(10000000000) AS t(i)",
    "params": [],
}
LONG_IBIS_DUCKDB_SQL = "SELECT sum(i) AS total FROM range(1000000000) AS t(i)"


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


def test_connection_pool_close_releases_idle_cursors_and_rejects_new_leases():
    created: list[_HealthyCursor] = []

    def factory() -> _HealthyCursor:
        cursor = _HealthyCursor()
        created.append(cursor)
        return cursor

    pool = ConnectionPool(factory, max_size=2)
    pool.close()

    assert len(created) == 2
    assert all(cursor.closed for cursor in created)
    with pytest.raises(RuntimeError, match="closed"):
        pool.checkout()


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
async def test_direct_duckdb_query_timeout_environment_interrupts_the_statement(monkeypatch):
    """Ignoring QUERY_TIMEOUT would make the direct backend's deadline advisory."""
    monkeypatch.setenv("QUERY_TIMEOUT", "0.01")
    backend = DuckDBBackend(":memory:", max_connections=1)
    try:
        with pytest.raises(QueryTimeoutError, match="query_timeout"):
            await backend.execute_async(LONG_DUCKDB_QUERY)

        assert backend.execution_metrics()["running"] == 0
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


@pytest.mark.asyncio
async def test_duckdb_cleanup_timeout_reports_failure_only_after_worker_exits(monkeypatch):
    """A cleanup deadline cannot acknowledge a still-running worker."""
    backend = DuckDBBackend(":memory:")
    worker = asyncio.get_running_loop().create_future()
    monkeypatch.setattr(
        duckdb_backend_module,
        "_CANCELLATION_CLEANUP_TIMEOUT_SECONDS",
        0.01,
        raising=False,
    )
    try:
        cleanup = asyncio.create_task(backend._await_execution_cleanup(worker))
        await asyncio.sleep(0.03)
        assert not cleanup.done()
        worker.set_result(None)
        with pytest.raises(DatabaseExecutionError, match="database_execution_failed"):
            await asyncio.wait_for(cleanup, timeout=0.5)
    finally:
        if not worker.done():
            worker.cancel()
        backend.close()


@pytest.mark.asyncio
async def test_ibis_duckdb_cancel_stops_compiled_expression_and_preserves_arrow_types():
    """Replacing the facade with direct Ibis execution would leave the statement alive."""
    pa = pytest.importorskip("pyarrow")
    backend = IbisBackend(connection_uri=":memory:")
    expression = backend.con.sql(LONG_IBIS_DUCKDB_SQL)
    task = asyncio.create_task(backend.execute_async({"ibis_expr": expression}))
    try:
        query_id = await _wait_for_running_query(backend)

        assert await backend.cancel_query(query_id) is True
        with pytest.raises(QueryCancelledError, match="query_cancelled"):
            await task

        typed_expression = backend.con.sql("SELECT CAST(1 AS BIGINT) AS value")
        expected_type = typed_expression.to_pyarrow().schema.field("value").type
        result = await backend.execute_async({"ibis_expr": typed_expression})
        assert result.schema.field("value").type == expected_type == pa.int64()
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        backend.close()


@pytest.mark.asyncio
async def test_ibis_duckdb_query_timeout_environment_interrupts_the_statement(monkeypatch):
    """Ignoring QUERY_TIMEOUT would leave the default Ibis database query running."""
    monkeypatch.setenv("QUERY_TIMEOUT", "0.01")
    backend = IbisBackend(connection_uri=":memory:")
    try:
        with pytest.raises(QueryTimeoutError, match="query_timeout"):
            await backend.execute_async({"ibis_expr": backend.con.sql(LONG_IBIS_DUCKDB_SQL)})

        assert backend.execution_metrics()["running"] == 0
        assert backend.execute({"ibis_expr": backend.con.sql("SELECT 1 AS value")}).num_rows == 1
    finally:
        backend.close()


def test_ibis_backend_rejects_hard_deadline_without_a_documented_capability():
    """Treating an unknown Ibis driver as cancellable would fake a hard deadline."""
    class UnsupportedConnection:
        name = "unsupported"

    from pivot_engine.backends.execution import CancellationUnavailableError

    with pytest.raises(CancellationUnavailableError, match="cancellation_unavailable"):
        IbisBackend(connection=UnsupportedConnection(), query_timeout_seconds=1)


def test_ibis_backend_reports_unavailable_cancellation_without_hard_deadline():
    """A backend without cancellation must advertise that fact instead of overclaiming."""
    class UnsupportedConnection:
        name = "unsupported"

    backend = IbisBackend(connection=UnsupportedConnection())

    assert backend.cancellation_status() == {
        "available": False,
        "backend": "unsupported",
        "code": "cancellation_unavailable",
    }

@pytest.mark.parametrize("backend_kind", ["duckdb", "ibis"])
def test_selected_cancel_drains_before_lease_becomes_borrowable(backend_kind, monkeypatch):
    import threading
    from pivot_engine.backends.execution import DatabaseExecution

    backend = DuckDBBackend(":memory:", max_connections=1) if backend_kind == "duckdb" else IbisBackend(connection_uri=":memory:")
    pool = backend.pool if backend_kind == "duckdb" else backend._duckdb_pool
    execution = DatabaseExecution("old")
    selected, release, checked_in = threading.Event(), threading.Event(), threading.Event()
    original_invoke = execution._invoke_native_cancel
    original_checkin = pool.checkin
    def delayed_cancel(*args):
        selected.set()
        assert release.wait(5)
        original_invoke(*args)
    def checkin(*args, **kwargs):
        result = original_checkin(*args, **kwargs)
        checked_in.set()
        return result
    monkeypatch.setattr(execution, "_invoke_native_cancel", delayed_cancel)
    monkeypatch.setattr(pool, "checkin", checkin)
    original_attach = execution.attach_lease
    def attach(lease):
        original_attach(lease)
        cancel.start()
        assert selected.wait(5)
    monkeypatch.setattr(execution, "attach_lease", attach)
    cancel = threading.Thread(target=lambda: execution.request_cancel("client_cancelled"))
    errors = []
    def run():
        try:
            if backend_kind == "duckdb":
                backend._execute_with_lease({"sql": "SELECT 1"}, execution)
            else:
                backend._execute_duckdb_ibis_with_lease({"sql": "SELECT 1"}, None, True, execution)
        except QueryCancelledError:
            pass
        except BaseException as exc:
            errors.append(exc)
    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert selected.wait(5)
        assert not checked_in.wait(0.3), "lease returned while selected cancellation can still interrupt it"
        assert not execution.wait_for_completion(0), "cleanup acknowledged before callback drained"
    finally:
        release.set()
        worker.join(5)
        cancel.join(5)
        backend.close()
    assert not worker.is_alive() and not cancel.is_alive()
    assert not errors


@pytest.mark.parametrize("backend_kind", ["duckdb", "ibis"])
def test_old_cancel_cannot_interrupt_new_borrower_after_checkin(backend_kind, monkeypatch):
    import threading
    from pivot_engine.backends.execution import DatabaseExecution

    backend = DuckDBBackend(":memory:", max_connections=1) if backend_kind == "duckdb" else IbisBackend(connection_uri=":memory:")
    pool = backend.pool if backend_kind == "duckdb" else backend._duckdb_pool
    execution = DatabaseExecution("old")
    returned, proceed = threading.Event(), threading.Event()
    original_checkin = pool.checkin
    returned_lease = []
    def paused_checkin(lease, **kwargs):
        retained = original_checkin(lease, **kwargs)
        returned_lease.append(lease)
        returned.set()
        assert proceed.wait(5)
        return retained
    monkeypatch.setattr(pool, "checkin", paused_checkin)
    errors = []
    def run():
        try:
            if backend_kind == "duckdb":
                backend._execute_with_lease({"sql": "SELECT 1"}, execution)
            else:
                backend._execute_duckdb_ibis_with_lease({"sql": "SELECT 1"}, None, True, execution)
        except BaseException as exc:
            errors.append(exc)
    worker = threading.Thread(target=run)
    worker.start()
    lease = None
    try:
        assert returned.wait(5)
        lease = pool.checkout()
        # Ibis's FIFO pool has other leases; drain it to obtain A's exact lease.
        borrowed = [lease]
        while lease is not returned_lease[0]:
            lease = pool.checkout()
            borrowed.append(lease)
        assert execution.request_cancel("client_cancelled") is False
        assert lease.execute("SELECT 42").fetchone() == (42,)
    finally:
        proceed.set()
        worker.join(5)
        for item in locals().get("borrowed", []):
            original_checkin(item)
        backend.close()
    assert not worker.is_alive()
    assert not errors

def test_late_cancel_does_not_interrupt_running_native_borrower(monkeypatch):
    import threading
    from pivot_engine.backends.execution import DatabaseExecution

    backend = DuckDBBackend(":memory:", max_connections=1)
    execution = DatabaseExecution("old")
    returned, finish_checkin = threading.Event(), threading.Event()
    entered, finish_query = threading.Event(), threading.Event()
    original_checkin = backend.pool.checkin
    def checkin(lease, **kwargs):
        retained = original_checkin(lease, **kwargs)
        returned.set()
        assert finish_checkin.wait(5)
        return retained
    monkeypatch.setattr(backend.pool, "checkin", checkin)
    errors, results = [], []
    def old_query():
        try:
            backend._execute_with_lease({"sql": "SELECT 1"}, execution)
        except BaseException as exc:
            errors.append(exc)
    worker = threading.Thread(target=old_query)
    worker.start()
    borrower = None
    def guarded_value():
        entered.set()
        assert finish_query.wait(5)
        return 42
    try:
        assert returned.wait(5)
        lease = backend.pool.checkout()
        lease.create_function("handoff_value", guarded_value, return_type="INTEGER")
        def query_b():
            try:
                results.append(lease.execute("SELECT handoff_value()").fetchone())
            except BaseException as exc:
                errors.append(exc)
        borrower = threading.Thread(target=query_b)
        borrower.start()
        assert entered.wait(5)
        requested = execution.request_cancel("client_cancelled")
        finish_query.set()
        borrower.join(5)
        assert results == [(42,)], f"borrower failed: {errors!r}"
        assert requested is False
        assert not errors
    finally:
        finish_query.set()
        finish_checkin.set()
        if borrower is not None:
            borrower.join(5)
        worker.join(5)
        if 'lease' in locals():
            original_checkin(lease)
        backend.close()
    assert not worker.is_alive() and not borrower.is_alive()


def _slow_core_read(backend):
    """A core read of two long statements: a request-level cancel must stop at the first."""
    slow = backend.con.sql(LONG_IBIS_DUCKDB_SQL)
    return lambda _resolve: (materialize(slow), materialize(slow))


@pytest.mark.asyncio
async def test_a_core_read_timeout_interrupts_its_statement_and_refuses_the_rest(monkeypatch):
    """Executing core statements with expression.to_pyarrow would ignore QUERY_TIMEOUT."""
    monkeypatch.setenv("QUERY_TIMEOUT", "0.2")
    controller = ScalablePivotController(backend_uri=":memory:")
    backend = controller.backend
    with pytest.raises(QueryTimeoutError, match="query_timeout"):
        await run_core_read(controller, "t", _slow_core_read(backend))
    metrics = backend.execution_metrics()
    assert metrics["running"] == 0
    assert metrics["timeouts"] == 1
    assert metrics["cancellation_attempts"] == metrics["cancellation_successes"] == 1  # the 2nd never ran
    controller.close()


@pytest.mark.asyncio
async def test_a_cancelled_core_read_interrupts_its_statement():
    """A client cancel of a core read must reach the database, not wait for every statement."""
    controller = ScalablePivotController(backend_uri=":memory:")
    backend = controller.backend
    task = asyncio.ensure_future(run_core_read(controller, "t", _slow_core_read(backend)))
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    metrics = backend.execution_metrics()
    assert metrics["running"] == 0
    assert metrics["cancellation_attempts"] == metrics["cancellation_successes"] == 1
    controller.close()


@pytest.mark.asyncio
async def test_every_core_statement_of_a_request_runs_on_a_cancellable_lease(monkeypatch):
    """A core read path executing outside the lease seam would be uncancellable."""
    pa = pytest.importorskip("pyarrow")
    from pivot_engine.types.pivot_spec import PivotSpec

    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow("s", pa.table({"r": ["N", "S", "N"], "c": ["a", "b", "a"], "v": [1, 2, 3]}))
    leased = []
    original = IbisBackend._execute_duckdb_ibis_with_lease

    def spy(self, query, params, return_arrow, execution):
        leased.append(query)
        return original(self, query, params, return_arrow, execution)

    monkeypatch.setattr(IbisBackend, "_execute_duckdb_ibis_with_lease", spy)
    spec = PivotSpec.from_dict({"table": "s", "rows": ["r"], "columns": ["c"], "pivot_config": {"enabled": True},
                                "measures": [{"field": "v", "agg": "sum", "alias": "v"}]})
    result = await controller.run_pivot_async(spec)
    assert sorted(result.column_names) == sorted(["r", "a_v", "b_v"])
    assert leased, "the core pivot did not run on a leased cursor"
    controller.close()


def test_a_lease_sees_the_in_memory_tables_of_its_expression():
    """Pooled cursors do not see the connection's in-memory tables unless registered on the lease."""
    pa = pytest.importorskip("pyarrow")
    import ibis
    from pivot_engine.backends.execution import DatabaseExecution

    backend = IbisBackend(connection_uri=":memory:")
    backend.con.create_table("t", pa.table({"a": [1, 2, 3], "b": ["x", "y", "x"]}))
    members = ibis.memtable(pa.table({"b": ["x"]}))
    expression = backend.con.table("t").join(members, "b").aggregate(s=lambda t: t.a.sum())
    result = backend._execute_duckdb_ibis_with_lease({"ibis_expr": expression}, None, True, DatabaseExecution("q"))
    assert result.to_pylist() == [{"s": 4}]
    assert backend.execution_metrics()["discarded_leases"] == 0
    backend.close()
