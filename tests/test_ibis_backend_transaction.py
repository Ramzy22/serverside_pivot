import pytest

from pivot_engine.backends.ibis_backend import IbisBackend


def _count_rows(backend: IbisBackend, table_name: str) -> int:
    return backend.con.raw_sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def test_ibis_backend_transaction_commits_successful_work():
    pytest.importorskip("ibis")
    pytest.importorskip("duckdb")

    backend = IbisBackend(connection_uri=":memory:")
    try:
        backend.con.raw_sql("CREATE TABLE tx_commit_test (id INTEGER)")

        with backend.transaction():
            backend.con.raw_sql("INSERT INTO tx_commit_test VALUES (1)")

        assert _count_rows(backend, "tx_commit_test") == 1
    finally:
        backend.close()


def test_ibis_backend_transaction_rolls_back_failed_work():
    pytest.importorskip("ibis")
    pytest.importorskip("duckdb")

    backend = IbisBackend(connection_uri=":memory:")
    try:
        backend.con.raw_sql("CREATE TABLE tx_rollback_test (id INTEGER)")

        with pytest.raises(RuntimeError):
            with backend.transaction():
                backend.con.raw_sql("INSERT INTO tx_rollback_test VALUES (1)")
                raise RuntimeError("rollback")

        assert _count_rows(backend, "tx_rollback_test") == 0
    finally:
        backend.close()


def test_ibis_backend_transaction_fails_explicitly_without_raw_sql_support():
    backend = IbisBackend.__new__(IbisBackend)
    backend.con = object()

    with pytest.raises(NotImplementedError, match="Transactions are not supported"):
        with backend.transaction():
            pass


@pytest.mark.asyncio
async def test_ibis_backend_async_tracking_uses_stable_query_ids():
    backend = IbisBackend.__new__(IbisBackend)
    backend._query_count = 0
    backend._total_time = 0.0
    import itertools
    import threading

    backend._query_id_counter = itertools.count(1)
    backend._running_queries_lock = threading.Lock()
    backend._running_queries = {}

    def execute(_query, _params=None, _return_arrow=True):
        return "ok"

    backend.execute = execute

    assert await backend.execute_async("SELECT 1") == "ok"
    assert backend._running_queries == {}
