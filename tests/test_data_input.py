"""
TDD Red-phase tests for DataInputNormalizer (API-01 through API-06).
These tests FAIL until pivot_engine/pivot_engine/data_input.py is implemented.
"""
import inspect
import pytest
import pyarrow as pa
import pandas as pd

from pivot_engine.data_input import DataInputNormalizer, DataInputError, normalize_data_input


class MockController:
    """Minimal controller stub that records load_data_from_arrow calls."""
    def __init__(self):
        self.loaded = {}  # table_name -> arrow_table

    def load_data_from_arrow(self, table_name: str, arrow_table: pa.Table, register_checkpoint: bool = True):
        self.loaded[table_name] = arrow_table


# --- API-01: pandas DataFrame ---

def test_pandas_dataframe():
    controller = MockController()
    df = pd.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
    normalize_data_input(df, "tbl", controller)
    assert "tbl" in controller.loaded
    tbl = controller.loaded["tbl"]
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 2
    assert set(tbl.schema.names) == {"region", "sales"}


# --- API-02: polars DataFrame ---

def test_polars_dataframe():
    pl = pytest.importorskip("polars")
    controller = MockController()
    df = pl.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
    normalize_data_input(df, "tbl_pl", controller)
    assert "tbl_pl" in controller.loaded
    tbl = controller.loaded["tbl_pl"]
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 2


# --- API-03: Ibis table expression ---

def test_ibis_table():
    ibis = pytest.importorskip("ibis")
    controller = MockController()
    expr = ibis.memtable({"region": ["East", "West"], "revenue": [300, 400]})
    normalize_data_input(expr, "tbl_ibis", controller)
    assert "tbl_ibis" in controller.loaded
    tbl = controller.loaded["tbl_ibis"]
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 2


# --- API-04: connection string dict ---

def test_connection_string():
    """Load from an in-memory DuckDB via connection string dict."""
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE TABLE sales_data AS SELECT 'North' AS region, 100 AS sales")
    # Export to a temp file so connection_string can reference it
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    # Remove the empty placeholder file so DuckDB can create a fresh database at that path.
    # NamedTemporaryFile creates the file immediately; DuckDB refuses to open a non-DuckDB file.
    os.unlink(db_path)
    try:
        con2 = duckdb.connect(db_path)
        con2.execute("CREATE TABLE sales_data AS SELECT 'North' AS region, 100 AS sales")
        con2.close()
        controller = MockController()
        normalize_data_input(
            {"connection_string": f"duckdb://{db_path}", "table": "sales_data"},
            "tbl_conn",
            controller,
        )
        assert "tbl_conn" in controller.loaded
        tbl = controller.loaded["tbl_conn"]
        assert isinstance(tbl, pa.Table)
        assert tbl.num_rows == 1
    finally:
        os.unlink(db_path)


def test_connection_string_releases_duckdb_file_before_return(tmp_path):
    """One-shot loading must release its writer before another connection opens read-only."""
    import gc
    import duckdb

    db_path = tmp_path / "source.duckdb"
    writer = duckdb.connect(str(db_path))
    writer.execute("CREATE TABLE sales_data AS SELECT 100 AS sales")
    writer.close()

    was_enabled = gc.isenabled()
    gc.disable()
    try:
        controller = MockController()
        normalize_data_input(
            {"connection_string": f"duckdb://{db_path}", "table": "sales_data"},
            "loaded_sales",
            controller,
        )
        assert controller.loaded["loaded_sales"].column("sales").to_pylist() == [100]
        reader = duckdb.connect(str(db_path), read_only=True)
        try:
            assert reader.execute("SELECT sales FROM sales_data").fetchone() == (100,)
        finally:
            reader.close()
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()


# --- API-05: auto-detection via single function signature ---

def test_auto_detection():
    """normalize_data_input accepts all source types through a single 3-param interface."""
    sig = inspect.signature(normalize_data_input)
    params = list(sig.parameters.keys())
    assert params == ["data", "table_name", "controller"], (
        f"normalize_data_input must have exactly (data, table_name, controller) params; got {params}"
    )


# --- API-06: unsupported type raises DataInputError ---

@pytest.mark.parametrize("bad_input", [
    42,
    ["a", "list"],
    {"not_connection_string": "value"},
])
def test_unsupported_type_error(bad_input):
    controller = MockController()
    with pytest.raises(DataInputError) as exc_info:
        normalize_data_input(bad_input, "tbl_bad", controller)
    assert "Supported types" in str(exc_info.value), (
        f"Error message must contain 'Supported types'; got: {exc_info.value}"
    )
    assert "tbl_bad" not in controller.loaded
