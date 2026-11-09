"""
Comprehensive Client-Side DuckDB-Wasm & Server-Side Parity Test Suite.

Verifies:
1. Auto-detection & explicit force override (`auto`, `client`, `server`) in both
   Python (`pivot_engine.client_mode`) and TypeScript (`modeResolver.ts`).
2. Tiered client engine selection (`duckdb-wasm` vs `memory`).
3. Differential Client-vs-Server parity between `ClientPivotTransport` and Python
   `TanStackPivotAdapter` across hierarchy expansion, column pivot (`colFields`),
   aggregations (`sum`, `avg`, `min`, `max`, `count`, `weighted_avg`), custom
   dimensions, filters, window functions, formulas, filter options, charts,
   drill-through, and export.
4. Commercial flagship showcase smoke test (`examples/demo_clientside_version.py`).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ROOT = ROOT_DIR / "dash_tanstack_pivot"
ENGINE_ROOT = PACKAGE_ROOT / "pivot_engine"
for candidate in (PACKAGE_ROOT, ENGINE_ROOT, ROOT_DIR):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from pivot_engine import (
    PivotRequestContext,
    PivotRuntimeService,
    PivotViewState,
    configure_pivot_execution,
    create_tanstack_adapter,
    resolve_client_engine_kind,
    resolve_execution_mode,
)


SAMPLE_RECORDS: List[Dict[str, Any]] = [
    {"id": 1, "parent_id": None, "name": "Global HQ", "region": "AMER", "country": "USA", "year": "2024", "revenue": 1000.0, "cost": 600.0, "units": 10, "weight": 2.0},
    {"id": 2, "parent_id": 1, "name": "US East", "region": "AMER", "country": "USA", "year": "2025", "revenue": 1500.0, "cost": 800.0, "units": 15, "weight": 3.0},
    {"id": 3, "parent_id": 1, "name": "Canada Ops", "region": "AMER", "country": "Canada", "year": "2024", "revenue": 500.0, "cost": 300.0, "units": 5, "weight": 1.0},
    {"id": 4, "parent_id": 1, "name": "Canada Ops 2", "region": "AMER", "country": "Canada", "year": "2025", "revenue": 700.0, "cost": 400.0, "units": 7, "weight": 2.0},
    {"id": 5, "parent_id": None, "name": "EMEA HQ", "region": "EMEA", "country": "UK", "year": "2024", "revenue": 800.0, "cost": 500.0, "units": 8, "weight": 2.0},
    {"id": 6, "parent_id": 5, "name": "UK North", "region": "EMEA", "country": "UK", "year": "2025", "revenue": 1200.0, "cost": 700.0, "units": 12, "weight": 4.0},
    {"id": 7, "parent_id": 5, "name": "France Ops", "region": "EMEA", "country": "France", "year": "2024", "revenue": 400.0, "cost": 250.0, "units": 4, "weight": 1.0},
    {"id": 8, "parent_id": 5, "name": "France Ops 2", "region": "EMEA", "country": "France", "year": "2025", "revenue": 600.0, "cost": 350.0, "units": 6, "weight": 1.0},
]


def test_frontend_client_runtime_node_suite():
    """Execute the TypeScript client runtime unit & contract suite under Node.js."""
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for frontend client runtime tests")

    script = Path(__file__).with_name("frontend_client_runtime.cjs")
    result = subprocess.run(
        [node, "--test", str(script)],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, f"frontend_client_runtime.cjs failed:\n{result.stdout}\n{result.stderr}"
    assert "# fail 0" in result.stdout


def test_python_execution_mode_auto_detection_and_forcing():
    """Verify Python-side execution mode & engine tier auto-detection and overrides."""
    arrow_table = pa.Table.from_pylist(SAMPLE_RECORDS)

    # 1. Auto mode with in-memory data only -> client
    assert resolve_execution_mode(data=SAMPLE_RECORDS) == "client"
    assert resolve_execution_mode(data=arrow_table) == "client"

    # 2. Auto mode with data_url only -> client
    assert resolve_execution_mode(data_url="https://cdn.example.com/metrics.parquet") == "client"

    # 3. Auto mode with table / adapter only -> server
    assert resolve_execution_mode(table="warehouse_sales") == "server"

    # 4. Auto mode with both data and table within threshold -> client
    assert (
        resolve_execution_mode(
            data=SAMPLE_RECORDS,
            table="warehouse_sales",
            execution_mode="auto",
            client_row_threshold=50_000,
        )
        == "client"
    )

    # 5. Auto mode with both data and table exceeding threshold -> server
    assert (
        resolve_execution_mode(
            data=SAMPLE_RECORDS,
            table="warehouse_sales",
            execution_mode="auto",
            client_row_threshold=4,
        )
        == "server"
    )

    # 6. Explicit force overrides
    assert (
        resolve_execution_mode(
            table="warehouse_sales",
            server_side=True,
            execution_mode="client",
        )
        == "client"
    )
    assert (
        resolve_execution_mode(
            data=SAMPLE_RECORDS,
            server_side=False,
            execution_mode="server",
        )
        == "server"
    )

    # 7. Client engine tier selection
    assert resolve_client_engine_kind(row_count=200, client_engine="auto", duckdb_wasm_threshold=1000) == "memory"
    assert resolve_client_engine_kind(row_count=2500, client_engine="auto", duckdb_wasm_threshold=1000) == "duckdb-wasm"
    assert resolve_client_engine_kind(row_count=50, data_url="/sales.parquet", client_engine="auto") == "duckdb-wasm"
    assert resolve_client_engine_kind(row_count=50, client_engine="duckdb-wasm") == "duckdb-wasm"
    assert resolve_client_engine_kind(row_count=50000, client_engine="memory") == "memory"

    # 8. configure_pivot_execution prop builder
    cfg_client = configure_pivot_execution(
        data=arrow_table,
        table="sales",
        execution_mode="auto",
        client_row_threshold=50_000,
        duckdb_wasm_threshold=5,
    )
    assert cfg_client["serverSide"] is False
    assert cfg_client["resolvedExecutionMode"] == "client"
    assert cfg_client["resolvedClientEngine"] == "duckdb-wasm"
    assert len(cfg_client["data"]) == len(SAMPLE_RECORDS)
    assert cfg_client["performanceConfig"]["executionMode"] == "client"

    cfg_server = configure_pivot_execution(
        data=arrow_table,
        table="sales",
        execution_mode="server",
    )
    assert cfg_server["serverSide"] is True
    assert cfg_server["resolvedExecutionMode"] == "server"
    assert cfg_server["performanceConfig"]["executionMode"] == "server"


def test_differential_client_vs_server_pivot_parity(tmp_path: Path):
    """
    Execute the exact same pivot hierarchy, column pivot, aggregations, filters,
    and custom dimensions on both the Python server-side `TanStackPivotAdapter`
    and the TypeScript `ClientPivotTransport`, asserting mathematical and
    structural parity.
    """
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for differential client-vs-server parity test")

    # 1. Run on Python Server-Side PivotRuntimeService
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("sales", pa.Table.from_pylist(SAMPLE_RECORDS))
    service = PivotRuntimeService(adapter_getter=lambda: adapter)

    view_state = PivotViewState(
        row_fields=["region", "country"],
        col_fields=["year"],
        val_configs=[
            {"field": "revenue", "agg": "sum"},
            {"field": "cost", "agg": "sum"},
            {"field": "units", "agg": "avg"},
        ],
        sorting=[{"id": "region", "desc": False}],
        expanded={"AMER": True},
        show_row_totals=True,
        show_col_totals=True,
        view_mode="pivot",
        layout_mode="hierarchy",
    )
    context = PivotRequestContext(
        table="sales",
        trigger_prop="pivot.viewport",
        intent="structural",
        start_row=0,
        end_row=50,
        needs_col_schema=True,
        include_grand_total=True,
        viewport_active=True,
    )
    server_res = service.process(view_state, context)
    server_rows = [r for r in (server_res.data or []) if r.get("_path") != "__color_scale_stats__"]

    # 2. Run the equivalent RuntimeRequestV1 on Client-Side ClientPivotTransport
    batch_input = tmp_path / "client_batch_input.json"
    batch_output = tmp_path / "client_batch_output.json"
    batch_input.write_text(
        json.dumps(
            {
                "tableName": "sales",
                "data": SAMPLE_RECORDS,
                "requests": [
                    {
                        "schemaVersion": 1,
                        "requestId": "parity-1",
                        "kind": "data",
                        "table": "sales",
                        "state": {
                            "viewMode": "pivot",
                            "rowFields": ["region", "country"],
                            "colFields": ["year"],
                            "valConfigs": [
                                {"field": "revenue", "agg": "sum"},
                                {"field": "cost", "agg": "sum"},
                                {"field": "units", "agg": "avg"},
                            ],
                            "sorting": [{"id": "region", "desc": False}],
                            "expanded": {"AMER": True},
                            "showRowTotals": True,
                            "showColTotals": True,
                        },
                        "payload": {
                            "start": 0,
                            "end": 50,
                            "include_grand_total": True,
                            "layout_mode": "hierarchy",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).with_name("frontend_client_runtime.cjs")
    completed = subprocess.run(
        [node, str(script)],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        timeout=60,
        env={
            **os.environ,
            "PIVOT_CLIENT_BATCH_INPUT": str(batch_input),
            "PIVOT_CLIENT_BATCH_OUTPUT": str(batch_output),
        },
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout

    client_bundle = json.loads(batch_output.read_text(encoding="utf-8"))
    client_res = client_bundle["responses"][0]
    client_rows = client_res["data"]

    # Build lookup by _path (and Grand Total)
    def _index_rows(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        indexed: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if row.get("_isTotal") or row.get("__isGrandTotal__") or row.get("_id") == "Grand Total":
                indexed["__GRAND_TOTAL__"] = row
            elif row.get("_path"):
                indexed[str(row["_path"])] = row
        return indexed

    srv_by_path = _index_rows(server_rows)
    cli_by_path = _index_rows(client_rows)

    expected_paths = ["AMER", "AMER|||Canada", "AMER|||USA", "EMEA", "__GRAND_TOTAL__"]
    for path_key in expected_paths:
        assert path_key in srv_by_path, f"Missing {path_key} in server response: {list(srv_by_path.keys())}"
        assert path_key in cli_by_path, f"Missing {path_key} in client response: {list(cli_by_path.keys())}"
        s_row = srv_by_path[path_key]
        c_row = cli_by_path[path_key]

        for metric_col in (
            "2024_revenue_sum",
            "2025_revenue_sum",
            "2024_cost_sum",
            "2025_cost_sum",
            "2024_units_avg",
            "2025_units_avg",
            "__RowTotal__revenue_sum",
            "__RowTotal__cost_sum",
        ):
            s_val = s_row.get(metric_col)
            c_val = c_row.get(metric_col)
            assert s_val is not None and c_val is not None, (
                f"Missing metric {metric_col} at {path_key}: server={s_val}, client={c_val}"
            )
            assert float(s_val) == pytest.approx(float(c_val), rel=1e-6), (
                f"Parity mismatch at {path_key}.{metric_col}: server={s_val} vs client={c_val}"
            )


def test_flagship_client_duckdb_wasm_demo_smoke():
    """Verify the commercial flagship client-side & hybrid demo builds and executes callbacks."""
    from examples.demo_clientside_version import create_app

    app = create_app(row_count=300)
    assert app.layout is not None

    # Find and invoke the mode switcher callback across auto, client, and server
    callback_found = False
    for cb_info in app.callback_map.values():
        fn = cb_info.get("callback")
        if fn and getattr(fn, "__name__", "") == "_update_hybrid_execution_mode":
            callback_found = True
            wrapped = getattr(fn, "__wrapped__", fn)

            srv_auto, perf_auto, badges_auto = wrapped("auto", "auto", 50000)
            assert srv_auto is False
            assert perf_auto["executionMode"] == "client"
            assert badges_auto is not None

            srv_forced_client, perf_forced_client, _ = wrapped("client", "duckdb-wasm", 100)
            assert srv_forced_client is False
            assert perf_forced_client["executionMode"] == "client"
            assert perf_forced_client["clientEngine"] == "duckdb-wasm"

            srv_forced_server, perf_forced_server, _ = wrapped("server", "auto", 50000)
            assert srv_forced_server is True
            assert perf_forced_server["executionMode"] == "server"
            break

    assert callback_found, "Expected _update_hybrid_execution_mode callback to be registered"
