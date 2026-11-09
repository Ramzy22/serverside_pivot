"""
Flagship Demo 5 — Client-Side DuckDB-Wasm & Hybrid Auto-Detection Showcase
==========================================================================
Commercial-grade showcase demonstrating `DashTanstackPivot` with automatic
client/server execution mode resolution (`auto`, `client`, `server`) and
full parity between the browser-side `ClientPivotTransport` (`DuckDbWasmExecutor`
+ `ClientSqlPlanner` + `ClientResponsePostProcessor`) and the Python server-side
`TanStackPivotAdapter` / `IbisPlanner`.

Key Capabilities Demonstrated:
  - Automatic mode resolution (`executionMode="auto"` by default):
    * Browser-local execution (`client`) when in-memory data <= `clientRowThreshold`
      or `dataUrl` is supplied without a server table.
    * Server-side execution (`server`) when dataset exceeds `clientRowThreshold`
      or when backed by a remote warehouse table.
  - Explicit force override (`executionMode="client"` or `executionMode="server"`).
  - Tiered client execution (`clientEngine="auto" | "duckdb-wasm" | "memory"`).
  - Zero server roundtrips in Client Mode across hierarchy expansion, sorting,
    filtering, column pivot (`colFields`), window functions (`percent_of_total`),
    calculated formulas, custom dimensions, charts, drill-through, and export.

Run:
    python examples/demo_clientside_version.py
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List

import dash
from dash import Input, Output, State, dcc, html

ROOT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ROOT = ROOT_DIR / "dash_tanstack_pivot"
ENGINE_ROOT = PACKAGE_ROOT / "pivot_engine"
for candidate in (PACKAGE_ROOT, ENGINE_ROOT, ROOT_DIR):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import (
    TanStackPivotAdapter,
    configure_pivot_execution,
    create_tanstack_adapter,
    register_pivot_app,
    resolve_client_engine_kind,
    resolve_execution_mode,
)
from examples.demo_datasets import (
    build_saas_table,
    saas_workspace_config,
)

TABLE_NAME = "hybrid_saas_revenue"
PIVOT_ID = "hybrid-client-server-pivot"
DEFAULT_CLIENT_ROWS = 2_500

_ADAPTER_LOCK = threading.Lock()
_SHARED_ADAPTER: TanStackPivotAdapter | None = None
_SHARED_RECORDS: List[Dict[str, Any]] | None = None


def get_hybrid_demo_state(row_count: int = DEFAULT_CLIENT_ROWS) -> tuple[TanStackPivotAdapter, List[Dict[str, Any]]]:
    """Initialize both the server-side DuckDB/Ibis adapter and client-side records."""
    global _SHARED_ADAPTER, _SHARED_RECORDS
    with _ADAPTER_LOCK:
        if _SHARED_ADAPTER is not None and _SHARED_RECORDS is not None:
            return _SHARED_ADAPTER, _SHARED_RECORDS

        arrow_table = build_saas_table(row_count)
        records = arrow_table.to_pylist()
        adapter = create_tanstack_adapter(backend_uri=":memory:")
        adapter.controller.load_data_from_arrow(TABLE_NAME, arrow_table)

        _SHARED_ADAPTER = adapter
        _SHARED_RECORDS = records
        return adapter, records


def _badge(label: str, value: str, bg: str = "#eff6ff", fg: str = "#1d4ed8") -> html.Div:
    return html.Div(
        style={
            "display": "inline-flex",
            "alignItems": "center",
            "gap": "6px",
            "padding": "5px 11px",
            "borderRadius": "999px",
            "background": bg,
            "color": fg,
            "fontSize": "12px",
            "fontWeight": 600,
            "border": f"1px solid {fg}26",
        },
        children=[
            html.Span(f"{label}:", style={"opacity": 0.78, "fontWeight": 500}),
            html.Span(value, style={"fontFamily": "'JetBrains Mono', monospace", "fontWeight": 700}),
        ],
    )


def create_app(row_count: int = DEFAULT_CLIENT_ROWS) -> dash.Dash:
    adapter, records = get_hybrid_demo_state(row_count=row_count)
    ws = saas_workspace_config()

    initial_exec = configure_pivot_execution(
        data=records,
        table=TABLE_NAME,
        adapter=adapter,
        execution_mode="auto",
        client_engine="auto",
        client_row_threshold=50_000,
        duckdb_wasm_threshold=1_000,
    )

    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    app.title = "Client-Side DuckDB-Wasm & Hybrid Auto-Detection — serverside-pivot"

    register_pivot_app(
        app,
        adapter_getter=lambda: adapter,
        pivot_id=PIVOT_ID,
        debug=False,
    )

    app.layout = html.Div(
        style={
            "minHeight": "100vh",
            "display": "flex",
            "flexDirection": "column",
            "background": "#f8fafc",
            "color": "#0f172a",
            "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        },
        children=[
            # Top Header & Mode Switcher
            html.Div(
                style={
                    "padding": "14px 24px",
                    "background": "linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%)",
                    "color": "#f8fafc",
                    "borderBottom": "1px solid #312e81",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "flexWrap": "wrap",
                    "gap": "16px",
                },
                children=[
                    html.Div(
                        children=[
                            html.Div(
                                style={"display": "flex", "alignItems": "center", "gap": "10px"},
                                children=[
                                    html.Span(
                                        "DUCKDB-WASM + HYBRID RUNTIME",
                                        style={
                                            "fontSize": "10px",
                                            "fontWeight": 700,
                                            "letterSpacing": "0.08em",
                                            "padding": "3px 8px",
                                            "borderRadius": "4px",
                                            "background": "rgba(99, 102, 241, 0.25)",
                                            "color": "#a5b4fc",
                                            "border": "1px solid rgba(165, 180, 252, 0.35)",
                                        },
                                    ),
                                    html.H1(
                                        "Client-Side DuckDB-Wasm & Server-Side Hybrid Pivot",
                                        style={"margin": 0, "fontSize": "18px", "fontWeight": 700},
                                    ),
                                ],
                            ),
                            html.P(
                                "Auto-detects Client-Side DuckDB-Wasm vs Server-Side Python Ibis/DuckDB — or force either mode with 100% V1 contract & mathematical parity.",
                                style={"margin": "4px 0 0 0", "fontSize": "12px", "color": "#94a3b8"},
                            ),
                        ]
                    ),
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "12px", "flexWrap": "wrap"},
                        children=[
                            html.Div(
                                children=[
                                    html.Label(
                                        "Execution Mode",
                                        style={"fontSize": "11px", "color": "#cbd5e1", "display": "block", "marginBottom": "3px"},
                                    ),
                                    dcc.Dropdown(
                                        id="hybrid-execution-mode-select",
                                        clearable=False,
                                        value="auto",
                                        options=[
                                            {"label": "Auto-Detect (Smart Default)", "value": "auto"},
                                            {"label": "Force Client-Side (DuckDB-Wasm)", "value": "client"},
                                            {"label": "Force Server-Side (Python Ibis)", "value": "server"},
                                        ],
                                        style={"width": "255px", "color": "#0f172a", "fontSize": "13px"},
                                    ),
                                ]
                            ),
                            html.Div(
                                children=[
                                    html.Label(
                                        "Client Engine Tier",
                                        style={"fontSize": "11px", "color": "#cbd5e1", "display": "block", "marginBottom": "3px"},
                                    ),
                                    dcc.Dropdown(
                                        id="hybrid-client-engine-select",
                                        clearable=False,
                                        value="auto",
                                        options=[
                                            {"label": "Auto Tier (>=1k rows -> Wasm)", "value": "auto"},
                                            {"label": "Force DuckDB-Wasm", "value": "duckdb-wasm"},
                                            {"label": "Force In-Memory Planner", "value": "memory"},
                                        ],
                                        style={"width": "235px", "color": "#0f172a", "fontSize": "13px"},
                                    ),
                                ]
                            ),
                            html.Div(
                                children=[
                                    html.Label(
                                        "Auto Client Row Threshold",
                                        style={"fontSize": "11px", "color": "#cbd5e1", "display": "block", "marginBottom": "3px"},
                                    ),
                                    dcc.Dropdown(
                                        id="hybrid-row-threshold-select",
                                        clearable=False,
                                        value=50000,
                                        options=[
                                            {"label": "50,000 rows (Default -> Client)", "value": 50000},
                                            {"label": "1,000 rows (Demo >1k -> Server)", "value": 1000},
                                        ],
                                        style={"width": "240px", "color": "#0f172a", "fontSize": "13px"},
                                    ),
                                ]
                            ),
                        ],
                    ),
                ],
            ),
            # Telemetry & Parity Status Strip
            html.Div(
                id="hybrid-telemetry-strip",
                style={
                    "padding": "10px 24px",
                    "background": "#ffffff",
                    "borderBottom": "1px solid #e2e8f0",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "flexWrap": "wrap",
                    "gap": "10px",
                },
            ),
            # Main Pivot Workspace
            html.Div(
                style={"flex": "1", "padding": "16px 24px", "display": "flex", "flexDirection": "column"},
                children=[
                    DashTanstackPivot(
                        id=PIVOT_ID,
                        table=TABLE_NAME,
                        data=initial_exec["data"],
                        serverSide=initial_exec["serverSide"],
                        performanceConfig=initial_exec["performanceConfig"],
                        pivotTitle="SaaS Revenue Intelligence — Hybrid Client/Server Engine",
                        defaultTheme="flash",
                        rowFields=ws["rowFields"],
                        colFields=ws["colFields"],
                        valConfigs=ws["valConfigs"],
                        filters=ws.get("filters", {}),
                        sorting=ws.get("sorting", []),
                        customDimensions=ws.get("customDimensions", []),
                        showRowTotals=True,
                        showColTotals=True,
                        showSubtotals=True,
                        style={"height": "calc(100vh - 175px)", "minHeight": "580px"},
                    )
                ],
            ),
        ],
    )

    @app.callback(
        Output(PIVOT_ID, "serverSide"),
        Output(PIVOT_ID, "performanceConfig"),
        Output("hybrid-telemetry-strip", "children"),
        Input("hybrid-execution-mode-select", "value"),
        Input("hybrid-client-engine-select", "value"),
        Input("hybrid-row-threshold-select", "value"),
    )
    def _update_hybrid_execution_mode(
        execution_mode: str,
        client_engine: str,
        client_row_threshold: int,
    ):
        cfg = configure_pivot_execution(
            data=records,
            table=TABLE_NAME,
            adapter=adapter,
            execution_mode=execution_mode,  # type: ignore[arg-type]
            client_engine=client_engine,  # type: ignore[arg-type]
            client_row_threshold=int(client_row_threshold or 50_000),
            duckdb_wasm_threshold=1_000,
        )
        resolved_mode = cfg["resolvedExecutionMode"]
        resolved_engine = cfg["resolvedClientEngine"]

        is_client = resolved_mode == "client"
        mode_bg = "#dcfce7" if is_client else "#e0e7ff"
        mode_fg = "#15803d" if is_client else "#4338ca"

        badges = html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "8px", "flexWrap": "wrap"},
            children=[
                _badge("Configured Mode", str(execution_mode).upper(), "#f1f5f9", "#334155"),
                _badge(
                    "Resolved Execution",
                    "CLIENT (Browser Local)" if is_client else "SERVER (Python DuckDB/Ibis)",
                    mode_bg,
                    mode_fg,
                ),
                _badge(
                    "Active Engine",
                    f"ClientPivotTransport ({resolved_engine})" if is_client else "TanStackPivotAdapter (Ibis)",
                    "#fef3c7" if is_client else "#e0f2fe",
                    "#b45309" if is_client else "#0369a1",
                ),
                _badge("Dataset Rows", f"{len(records):,}", "#f3e8ff", "#6b21a8"),
                _badge(
                    "Network Roundtrips",
                    "0 (Zero Server Callbacks)" if is_client else "1 per Viewport/Action",
                    "#dcfce7" if is_client else "#f1f5f9",
                    "#15803d" if is_client else "#475569",
                ),
                _badge(
                    "Server-Side Parity",
                    "100% V1 Envelope + SQL + WindowFn + Formulas",
                    "#ecfdf5",
                    "#047857",
                ),
            ],
        )

        return cfg["serverSide"], cfg["performanceConfig"], badges

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Client-Side DuckDB-Wasm & Hybrid Pivot demo.")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8056")))
    parser.add_argument("--rows", type=int, default=DEFAULT_CLIENT_ROWS)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    demo_app = create_app(row_count=args.rows)
    print(f"Starting Client-Side DuckDB-Wasm & Hybrid Showcase on http://127.0.0.1:{args.port}")
    demo_app.run(host="127.0.0.1", port=args.port, debug=args.debug)
