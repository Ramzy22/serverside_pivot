"""
DataInputNormalizer — unified data ingestion for DashTanstackPivot.

Accepts pandas DataFrame, polars DataFrame, Ibis table expression,
connection string dict, or PyArrow Table and loads it into the pivot engine.

API:
    normalize_data_input(data, table_name, controller) -> None
"""
from __future__ import annotations

from typing import Any, Tuple
import pyarrow as pa


class DataInputError(TypeError):
    """Raised when the data= prop receives an unsupported or unloadable type."""
    pass


def _is_pandas_dataframe(data) -> bool:
    """Detect pandas DataFrame without requiring pandas at module level."""
    return type(data).__module__.startswith("pandas") and hasattr(data, "to_dict")


def _is_polars_dataframe(data) -> bool:
    """Detect polars DataFrame without requiring polars at module level."""
    return type(data).__module__.startswith("polars") and hasattr(data, "to_arrow")


def _is_ibis_table(data) -> bool:
    """Detect an Ibis table expression via lazy import."""
    try:
        import ibis.expr.types as ibis_types  # noqa: PLC0415
        return isinstance(data, ibis_types.Table)
    except ImportError:
        return False


def _is_connection_dict(data) -> bool:
    """Detect {"connection_string": ..., "table": ...} pattern."""
    return (
        isinstance(data, dict)
        and "connection_string" in data
        and "table" in data
    )


def _pandas_to_arrow(df) -> pa.Table:
    """Convert pandas DataFrame to Arrow. preserve_index=False prevents index leakage."""
    return pa.Table.from_pandas(df, preserve_index=False)


def _polars_to_arrow(df) -> pa.Table:
    """Convert polars DataFrame to Arrow. rechunk() is defensive for multi-chunk frames."""
    return df.rechunk().to_arrow()


def _ibis_to_arrow(expr) -> pa.Table:
    """Convert Ibis table expression to Arrow via to_pyarrow()."""
    return expr.to_pyarrow()


def _create_backend_for_connection_dict(data: dict) -> Tuple[Any, Any]:
    """Create an IbisBackend and resolve the target Ibis table expression from connection dict."""
    connection_string = data["connection_string"]
    table_name = data["table"]
    database = data.get("database")
    schema = data.get("schema")
    connect_kwargs = dict(data.get("connect_args") or data.get("kwargs") or {})
    try:
        from .backends.ibis_backend import IbisBackend  # noqa: PLC0415
        from .backends.table_catalog import IbisTableCatalog  # noqa: PLC0415
    except ImportError as exc:
        raise DataInputError(
            f"Could not import IbisBackend to handle connection_string input: {exc}. "
            f"Ensure ibis-framework is installed: pip install ibis-framework"
        ) from exc
    try:
        backend = IbisBackend(connection_uri=connection_string, **connect_kwargs)
    except ModuleNotFoundError as exc:
        # e.g. "No module named 'psycopg2'" when postgres driver missing
        scheme = connection_string.split("://")[0] if "://" in connection_string else "unknown"
        raise DataInputError(
            f"Missing database driver for connection scheme '{scheme}': {exc}.\n"
            f"Install the required ibis backend: pip install ibis-framework[{scheme}]"
        ) from exc

    catalog = IbisTableCatalog(backend.con)
    if database or schema:
        qualified = ".".join(part for part in (database, schema, table_name) if part)
        try:
            table_expr = catalog.resolve_table(qualified)
        except Exception:
            table_expr = backend.con.table(table_name)
    else:
        table_expr = catalog.resolve_table(table_name)
    return backend, table_expr


def _connection_dict_to_arrow(data: dict) -> pa.Table:
    """
    Load a table from an external database via connection string.

    data must have keys:
      - "connection_string": ibis-compatible URI (e.g. "duckdb://path.db", "postgres://...")
      - "table": table name in the target database
    """
    backend, table_expr = _create_backend_for_connection_dict(data)
    with backend:
        return table_expr.to_pyarrow()


def _should_register_connection_dict_lazily(data: dict, controller: Any) -> bool:
    """Return True when a connection dict should be registered lazily without materializing to Arrow."""
    if not hasattr(controller, "register_ibis_table"):
        return False
    if data.get("materialize") is True or data.get("lazy") is False:
        return False
    if data.get("lazy") is True:
        return True
    uri = str(data.get("connection_string") or "").strip().lower()
    # Local duckdb:// file paths are materialized by default so file locks are released immediately
    if uri.startswith("duckdb://") and not uri.startswith("duckdb://md:"):
        return False
    return True


def _unsupported_type_error(data) -> DataInputError:
    type_str = f"{type(data).__module__}.{type(data).__name__}"
    return DataInputError(
        f"Unsupported data type: {type_str}.\n"
        f"Supported types:\n"
        f"  - pandas.DataFrame            (pip install pandas)\n"
        f"  - polars.DataFrame            (pip install polars)\n"
        f"  - ibis.Table expression       (pip install ibis-framework)\n"
        f"  - dict with keys 'connection_string' and 'table'\n"
        f"  - pyarrow.Table\n"
    )


def normalize_data_input(data, table_name: str, controller) -> None:
    """
    Detect the type of *data*, convert it to a PyArrow Table (or register lazily
    for Ibis/cloud table sources when supported by *controller*), and load it into
    *controller* under *table_name*.

    Parameters
    ----------
    data : pd.DataFrame | pl.DataFrame | ibis.Table | dict | pa.Table
        The data source. Supported types are listed in the error message for
        unsupported inputs.
    table_name : str
        The name under which the table will be registered in the pivot engine.
    controller : ScalablePivotController (or any object with load_data_from_arrow)
        The pivot engine controller that owns the DuckDB/Ibis connection.

    Raises
    ------
    DataInputError
        If *data* is not one of the supported types.
    """
    if isinstance(data, pa.Table):
        arrow_table = data
    elif _is_pandas_dataframe(data):
        arrow_table = _pandas_to_arrow(data)
    elif _is_polars_dataframe(data):
        arrow_table = _polars_to_arrow(data)
    elif _is_ibis_table(data):
        if hasattr(controller, "register_ibis_table"):
            controller.register_ibis_table(table_name, data)
            return
        arrow_table = _ibis_to_arrow(data)
    elif _is_connection_dict(data):
        if _should_register_connection_dict_lazily(data, controller):
            backend, table_expr = _create_backend_for_connection_dict(data)
            controller.register_ibis_table(table_name, table_expr, backend=backend)
            return
        arrow_table = _connection_dict_to_arrow(data)
    else:
        raise _unsupported_type_error(data)

    controller.load_data_from_arrow(table_name, arrow_table)


class DataInputNormalizer:
    """
    Stateful wrapper around normalize_data_input that can be bound to a controller.

    Prefer the free function normalize_data_input() for simple one-shot use.
    This class is provided for adapters that need to bind a controller once.
    """

    def __init__(self, controller):
        self._controller = controller

    def load(self, data, table_name: str) -> None:
        """Normalize *data* and load it into the bound controller."""
        normalize_data_input(data, table_name, self._controller)
