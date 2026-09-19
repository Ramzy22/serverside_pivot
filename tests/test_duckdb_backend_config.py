import asyncio

import pytest

from pivot_engine.backends.duckdb_backend import (
    DuckDBBackend,
    _validate_duckdb_memory_limit,
    _validate_duckdb_threads,
)


@pytest.mark.parametrize(
    ("raw_threads", "expected"),
    [
        (1, 1),
        (8, 8),
        ("16", 16),
        (" 2 ", 2),
    ],
)
def test_validate_duckdb_threads_accepts_positive_integers(raw_threads, expected):
    assert _validate_duckdb_threads(raw_threads) == expected


@pytest.mark.parametrize(
    "raw_threads",
    [
        0,
        -1,
        True,
        "1; DROP TABLE users",
        "1.5",
        2048,
    ],
)
def test_validate_duckdb_threads_rejects_unsafe_values(raw_threads):
    with pytest.raises(ValueError):
        _validate_duckdb_threads(raw_threads)


@pytest.mark.parametrize(
    ("raw_limit", "expected"),
    [
        ("512MB", "512MB"),
        ("4GB", "4GB"),
        ("1.5GiB", "1.5GiB"),
        (" 2 tb ", "2TB"),
    ],
)
def test_validate_duckdb_memory_limit_accepts_safe_size_literals(raw_limit, expected):
    assert _validate_duckdb_memory_limit(raw_limit) == expected


@pytest.mark.parametrize(
    "raw_limit",
    [
        "",
        "0GB",
        "-1GB",
        "4GB'; DROP TABLE users; --",
        "80%",
        "unlimited",
        1024,
    ],
)
def test_validate_duckdb_memory_limit_rejects_unsafe_values(raw_limit):
    with pytest.raises(ValueError):
        _validate_duckdb_memory_limit(raw_limit)


def test_duckdb_execute_arrow_uses_query_dict_params():
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    backend = DuckDBBackend(uri=":memory:")
    try:
        result = backend.execute_arrow({"sql": "SELECT ? AS value", "params": [42]})
        assert result.to_pylist() == [{"value": 42}]
    finally:
        backend.close()


def test_duckdb_execute_uses_legacy_fetch_arrow_table_for_parameterized_results():
    """DuckDB connections without ``to_arrow_table`` still return Arrow data."""
    arrow = pytest.importorskip("pyarrow")

    class LegacyCursor:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            self.params = params
            return self

        def fetchone(self):
            return (1,)

        def fetch_arrow_table(self):
            return arrow.table(
                {"value": arrow.array([self.params[0]], type=arrow.int64())}
            )

        def close(self):
            pass

    class LegacyConnection:
        def __init__(self):
            self.cursors = []
            self.closed = False

        def cursor(self):
            cursor = LegacyCursor()
            self.cursors.append(cursor)
            return cursor

        def close(self):
            self.closed = True

    connection = LegacyConnection()
    backend = DuckDBBackend(connection=connection)
    try:
        result = backend.execute({"sql": "SELECT ? AS value", "params": [42]})
        assert result.to_pylist() == [{"value": 42}]
        assert any(
            call == ("SELECT ? AS value", [42])
            for cursor in connection.cursors
            for call in cursor.calls
        )
    finally:
        backend.close()

    assert connection.closed


def test_duckdb_execute_batch_returns_arrow_tables_for_query_dicts():
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    backend = DuckDBBackend(uri=":memory:")
    try:
        results = backend.execute_batch(
            [
                {"sql": "SELECT ? AS value", "params": [1]},
                {"sql": "SELECT ? AS value", "params": [2]},
            ]
        )
        assert [table.to_pylist()[0]["value"] for table in results] == [1, 2]
    finally:
        backend.close()


def test_duckdb_execute_preserves_empty_arrow_schema():
    """An empty direct query retains its Arrow column names and physical types."""
    pytest.importorskip("duckdb")
    arrow = pytest.importorskip("pyarrow")
    backend = DuckDBBackend(uri=":memory:")
    try:
        result = backend.execute(
            {
                "sql": (
                    "SELECT CAST(NULL AS INTEGER) AS quantity, "
                    "CAST(NULL AS VARCHAR) AS label WHERE FALSE"
                ),
                "params": [],
            }
        )
        assert result.num_rows == 0
        assert result.schema.names == ["quantity", "label"]
        assert result.schema.field("quantity").type == arrow.int32()
        label_type = result.schema.field("label").type
        assert arrow.types.is_string(label_type) or arrow.types.is_large_string(label_type)
    finally:
        backend.close()


def test_duckdb_execute_async_preserves_parameterized_arrow_results():
    """The async facade delegates through the compatible Arrow execution path."""
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    backend = DuckDBBackend(uri=":memory:")
    try:
        result = asyncio.run(
            backend.execute_async({"sql": "SELECT ? AS value", "params": [7]})
        )
        assert result.to_pylist() == [{"value": 7}]
    finally:
        backend.close()


def test_duckdb_execute_streaming_uses_pool_connections(monkeypatch):
    pytest.importorskip("duckdb")
    backend = DuckDBBackend(uri=":memory:")
    borrowed = {"count": 0}
    original_get_connection = backend.pool.get_connection

    def tracking_get_connection():
        borrowed["count"] += 1
        return original_get_connection()

    monkeypatch.setattr(backend.pool, "get_connection", tracking_get_connection)
    try:
        rows = list(backend.execute_streaming({"sql": "SELECT 1 AS value"}, batch_size=1))
        assert rows == [[{"value": 1}]]
        assert borrowed["count"] == 1
    finally:
        backend.close()
