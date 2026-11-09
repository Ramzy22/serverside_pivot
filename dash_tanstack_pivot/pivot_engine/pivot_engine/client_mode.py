"""
Client/Server Execution Mode Auto-Detection & Configuration Helper.

Provides Python-side mode resolution and prop generation that mirrors the
frontend's ``resolveExecutionMode`` and ``resolveClientEngineKind`` rules:
- ``execution_mode="auto"`` (default): automatically selects client-side
  execution (powered by DuckDB-Wasm / in-memory SQL planner) when in-memory
  records/DataFrame/Arrow Table are within ``client_row_threshold`` or when a
  browser ``data_url`` is supplied without a server ``table`` / ``adapter``.
- ``execution_mode="client"``: forces 100% browser-local execution with zero
  server callbacks.
- ``execution_mode="server"``: forces server-side execution via
  ``TanStackPivotAdapter`` / ``PivotRuntimeService``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple, Union

ExecutionMode = Literal["auto", "client", "server"]
ClientEngineKind = Literal["auto", "duckdb-wasm", "memory"]

DEFAULT_CLIENT_ROW_THRESHOLD = 50_000
DEFAULT_DUCKDB_WASM_THRESHOLD = 1_000


def _coerce_records_and_count(
    data: Any,
) -> Tuple[Optional[List[Dict[str, Any]]], int]:
    """Convert supported tabular inputs to JSON-serializable records and row count."""
    if data is None:
        return None, 0

    if isinstance(data, list):
        records = [dict(row) if isinstance(row, Mapping) else row for row in data]
        return records, len(records)

    if isinstance(data, tuple):
        records = [dict(row) if isinstance(row, Mapping) else row for row in data]
        return records, len(records)

    # PyArrow Table / RecordBatch
    if hasattr(data, "to_pylist") and callable(data.to_pylist):
        records = data.to_pylist()
        return records, len(records)

    # Pandas / Polars DataFrame
    if hasattr(data, "to_dicts") and callable(data.to_dicts):
        records = data.to_dicts()
        return records, len(records)

    if hasattr(data, "to_dict") and callable(data.to_dict):
        try:
            records = data.to_dict(orient="records")
            if isinstance(records, list):
                return records, len(records)
        except TypeError:
            pass

    return None, 0


def resolve_execution_mode(
    *,
    data: Any = None,
    data_url: Optional[str] = None,
    table: Optional[str] = None,
    adapter: Any = None,
    server_side: Optional[bool] = None,
    execution_mode: ExecutionMode = "auto",
    client_row_threshold: int = DEFAULT_CLIENT_ROW_THRESHOLD,
) -> Literal["client", "server"]:
    """
    Determine whether the pivot should execute in ``'client'`` or ``'server'`` mode.

    Precedence:
    1. Explicit ``execution_mode='client'`` or ``execution_mode='server'``.
    2. Explicit ``server_side=True`` or ``server_side=False``.
    3. ``auto`` heuristics:
       - If ``data_url`` is provided and neither ``table`` nor ``adapter`` is set -> ``'client'``.
       - If ``data`` is provided and ``adapter`` is None -> ``'client'``.
       - If ``data`` is provided alongside ``adapter`` / ``table`` and ``len(data) <= client_row_threshold`` -> ``'client'``.
       - Otherwise -> ``'server'``.
    """
    normalized_mode = str(execution_mode or "auto").strip().lower()
    if normalized_mode == "client":
        return "client"
    if normalized_mode == "server":
        return "server"

    if server_side is True:
        return "server"
    if server_side is False:
        return "client"

    _, row_count = _coerce_records_and_count(data)
    has_data = row_count > 0 or (isinstance(data, Sequence) and not isinstance(data, (str, bytes)))
    has_data_url = bool(data_url and str(data_url).strip())
    has_server_binding = bool((table and str(table).strip()) or adapter is not None)

    if has_data_url and not has_server_binding:
        return "client"

    if has_data:
        if not has_server_binding:
            return "client"
        if row_count <= max(1, int(client_row_threshold)):
            return "client"
        return "server"

    if has_server_binding:
        return "server"

    return "client"


def resolve_client_engine_kind(
    *,
    row_count: int = 0,
    data_url: Optional[str] = None,
    client_engine: ClientEngineKind = "auto",
    duckdb_wasm_threshold: int = DEFAULT_DUCKDB_WASM_THRESHOLD,
) -> Literal["duckdb-wasm", "memory"]:
    """Determine which client engine tier should execute queries."""
    normalized_engine = str(client_engine or "auto").strip().lower()
    if normalized_engine == "duckdb-wasm":
        return "duckdb-wasm"
    if normalized_engine == "memory":
        return "memory"

    if data_url and str(data_url).strip():
        return "duckdb-wasm"

    if int(row_count) >= max(0, int(duckdb_wasm_threshold)):
        return "duckdb-wasm"

    return "memory"


def configure_pivot_execution(
    *,
    data: Any = None,
    data_url: Optional[str] = None,
    table: Optional[str] = None,
    adapter: Any = None,
    server_side: Optional[bool] = None,
    execution_mode: ExecutionMode = "auto",
    client_engine: ClientEngineKind = "auto",
    client_row_threshold: int = DEFAULT_CLIENT_ROW_THRESHOLD,
    duckdb_wasm_threshold: int = DEFAULT_DUCKDB_WASM_THRESHOLD,
    performance_config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build normalized ``DashTanstackPivot`` props for automatic or forced
    client/server execution mode.

    Returns a dictionary containing:
    - ``serverSide`` (bool)
    - ``data`` (list of dicts when resolved to client mode or when data is provided)
    - ``table`` (table name)
    - ``performanceConfig`` (merged dict including ``executionMode``, ``clientEngine``,
      ``clientRowThreshold``, ``duckdbWasmThreshold``, and optional ``dataUrl``)
    - ``resolvedExecutionMode`` (``'client'`` or ``'server'`` metadata helper)
    - ``resolvedClientEngine`` (``'duckdb-wasm'`` or ``'memory'`` metadata helper)
    """
    records, row_count = _coerce_records_and_count(data)
    resolved_mode = resolve_execution_mode(
        data=records if records is not None else data,
        data_url=data_url,
        table=table,
        adapter=adapter,
        server_side=server_side,
        execution_mode=execution_mode,
        client_row_threshold=client_row_threshold,
    )
    resolved_engine = resolve_client_engine_kind(
        row_count=row_count,
        data_url=data_url,
        client_engine=client_engine,
        duckdb_wasm_threshold=duckdb_wasm_threshold,
    )

    merged_perf: Dict[str, Any] = dict(performance_config or {})
    merged_perf["executionMode"] = resolved_mode if execution_mode == "auto" else execution_mode
    merged_perf["clientEngine"] = client_engine
    merged_perf["clientRowThreshold"] = int(client_row_threshold)
    merged_perf["duckdbWasmThreshold"] = int(duckdb_wasm_threshold)
    if data_url:
        merged_perf["dataUrl"] = str(data_url)

    result: Dict[str, Any] = {
        "serverSide": resolved_mode == "server",
        "table": table or "client_data",
        "performanceConfig": merged_perf,
        "resolvedExecutionMode": resolved_mode,
        "resolvedClientEngine": resolved_engine,
        "rowCount": row_count,
    }
    if resolved_mode == "client":
        result["data"] = records if records is not None else []
    elif records is not None and not table and adapter is None:
        result["data"] = records

    return result
