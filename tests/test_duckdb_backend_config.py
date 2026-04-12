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
