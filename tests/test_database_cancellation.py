"""Regression coverage for native database statement cancellation."""

from __future__ import annotations

from pivot_engine.backends.duckdb_backend import ConnectionPool


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
