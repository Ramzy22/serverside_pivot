"""
Flagship Demo 4 — Enterprise Cloud Data Warehouse & Multi-Backend Ibis Pushdown
===============================================================================
Commercial-grade showcase demonstrating `DashTanstackPivot` running over
zero-copy lazy `ibis.Table` expressions and cloud/SQL backends (`Snowflake`,
`BigQuery`, `PostgreSQL`, `MotherDuck`, `ClickHouse`, `Databricks`, `Trino`,
`SQLite`, and `DuckDB`) while preserving strict one-way capability boundaries.

Key Capabilities Demonstrated:
  - Zero-copy lazy `ibis.Table` registration via `normalize_data_input`
    (no full-table `to_pyarrow()` materialization).
  - Real-time `BackendCapabilities` inspection across cloud & RDBMS dialects
    (`is_cloud_or_remote`, `supports_ddl_materialization`, `supports_sql_mutations`,
    `supports_rowid`, `supports_ilike`, `param_style`, TTL schema cache).
  - Live Ibis SQL Pushdown Inspector showing the compiled SQL query generated
    by `IbisPlanner` for the active pivot hierarchy.
  - Governance & security controls (`RuntimeDatasetAuthorizer`, fail-closed
    read-only analytical pushdown vs opt-in remote SQL mutations).

Run:
    python examples/demo_cloud_warehouse.py
    # Optional environment overrides:
    #   BACKEND_URI="postgresql://user:pass@host:5432/warehouse"
    #   PIVOT_ALLOW_REMOTE_MUTATIONS="1"
    #   PIVOT_ALLOW_REMOTE_DDL="0"
    #   PIVOT_SCHEMA_CACHE_TTL_SECONDS="300"
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict

import dash
from dash import Input, Output, dcc, html
import ibis

ROOT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ROOT = ROOT_DIR / "dash_tanstack_pivot"
ENGINE_ROOT = PACKAGE_ROOT / "pivot_engine"
for candidate in (PACKAGE_ROOT, ENGINE_ROOT, ROOT_DIR):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import (
    RuntimeDatasetAuthorizer,
    TanStackPivotAdapter,
    create_tanstack_adapter,
    register_pivot_app,
)
from pivot_engine.backends.capabilities import (
    BackendCapabilities,
    resolve_backend_capabilities,
)
from pivot_engine.data_input import normalize_data_input
from pivot_engine.types.pivot_spec import Measure, PivotSpec
from examples.demo_datasets import (
    SAAS_DEFAULT_ROWS,
    build_saas_table,
    saas_workspace_config,
)

TABLE_NAME = "cloud_warehouse_metrics"
PIVOT_ID = "cloud-warehouse-pivot"

# Pre-configured cloud warehouse profiles for interactive capability & SQL inspection
CLOUD_WAREHOUSE_PROFILES: Dict[str, Dict[str, Any]] = {
    "snowflake": {
        "label": "Snowflake Data Cloud",
        "uri": "snowflake://analytics_svc:***@acme-us-east-1/FINOPS_PROD/PUBLIC?warehouse=COMPUTE_WH",
        "sqlglot_dialect": "snowflake",
        "badge_color": "#0284c7",
        "badge_bg": "#e0f2fe",
        "description": "Cloud data warehouse — zero-copy lazy Ibis pushdown, read-only analytical mode by default, native ILIKE, pyformat (%s) bindings.",
    },
    "bigquery": {
        "label": "Google BigQuery",
        "uri": "bigquery://acme-enterprise-analytics/finops_warehouse",
        "sqlglot_dialect": "bigquery",
        "badge_color": "#1d4ed8",
        "badge_bg": "#dbeafe",
        "description": "Serverless columnar warehouse — automatic LOWER(col) LIKE fallback (supports_ilike=False), rowid-free mutations, TTL schema cache.",
    },
    "postgres": {
        "label": "PostgreSQL / Supabase",
        "uri": "postgresql://pivot_ro:***@db.acme-cloud.internal:5432/enterprise_dw",
        "sqlglot_dialect": "postgres",
        "badge_color": "#4338ca",
        "badge_bg": "#e0e7ff",
        "description": "Transactional & analytical RDBMS — concurrent connection pooling, native ILIKE, primary-key / composite-key aggregate write-back.",
    },
    "motherduck": {
        "label": "MotherDuck Serverless",
        "uri": "md:finops_cloud_analytics",
        "sqlglot_dialect": "duckdb",
        "badge_color": "#b45309",
        "badge_bg": "#fef3c7",
        "description": "Hybrid serverless DuckDB in the cloud — Arrow RecordBatch streaming, shared connection lock safety, full OLAP grouping sets.",
    },
    "sqlite": {
        "label": "SQLite RDBMS (Ibis SQL)",
        "uri": "sqlite://:memory:",
        "sqlglot_dialect": "sqlite",
        "badge_color": "#047857",
        "badge_bg": "#d1fae5",
        "description": "Lightweight RDBMS backend via Ibis — proves dialect-portable SQL generation without DuckDB-specific extensions.",
    },
    "duckdb": {
        "label": "DuckDB Local OLAP",
        "uri": ":memory:",
        "sqlglot_dialect": "duckdb",
        "badge_color": "#334155",
        "badge_bg": "#f1f5f9",
        "description": "Embedded high-throughput OLAP engine — full local DDL rollup materialization and direct Arrow zero-copy integration.",
    },
}

_ADAPTER_LOCK = threading.Lock()
_ADAPTER_INSTANCE: TanStackPivotAdapter | None = None
_EXTERNAL_IBIS_CON: Any = None


def _build_lazy_warehouse_table():
    """
    Create an external Ibis connection and register the 40,000-row dataset as a
    remote/external table, returning a lazy `ibis.Table` expression.
    """
    global _EXTERNAL_IBIS_CON
    if _EXTERNAL_IBIS_CON is None:
        _EXTERNAL_IBIS_CON = ibis.duckdb.connect(":memory:")
        arrow_table = build_saas_table(SAAS_DEFAULT_ROWS)
        _EXTERNAL_IBIS_CON.create_table(TABLE_NAME, arrow_table, overwrite=True)
    return _EXTERNAL_IBIS_CON.table(TABLE_NAME)


def get_adapter() -> TanStackPivotAdapter:
    """Create or return the singleton TanStackPivotAdapter backed by a lazy Ibis table."""
    global _ADAPTER_INSTANCE
    if _ADAPTER_INSTANCE is None:
        with _ADAPTER_LOCK:
            if _ADAPTER_INSTANCE is None:
                backend_uri = os.environ.get("BACKEND_URI", ":memory:")
                print(f"[Cloud Warehouse Demo] Initializing TanStackPivotAdapter (backend_uri={backend_uri})...")
                adapter = create_tanstack_adapter(backend_uri=backend_uri)
                lazy_table = _build_lazy_warehouse_table()
                normalize_data_input(lazy_table, TABLE_NAME, adapter.controller)
                print(
                    f"[Cloud Warehouse Demo] Registered lazy Ibis table '{TABLE_NAME}' "
                    f"(capabilities={adapter.controller.backend_capabilities.backend_name})."
                )
                _ADAPTER_INSTANCE = adapter
    return _ADAPTER_INSTANCE


def compile_sample_pushdown_sql(profile_key: str = "snowflake") -> str:
    """
    Compile a representative hierarchical pivot query via IbisPlanner and render
    it in the selected cloud dialect when sqlglot/ibis supports transpilation.
    """
    adapter = get_adapter()
    planner = adapter.controller.planner
    spec = PivotSpec(
        table=TABLE_NAME,
        rows=["customer_segment", "subscription_tier"],
        measures=[
            Measure(field="arr", agg="sum", alias="arr_sum"),
            Measure(field="mrr", agg="sum", alias="mrr_sum"),
            Measure(field="net_revenue_retention", agg="avg", alias="nrr_avg"),
        ],
        filters=[{"field": "region", "op": "contains", "value": "North America", "caseSensitive": False}],
        limit=100,
        totals=False,
    )
    plan_result = planner.plan(spec)
    queries = plan_result.get("queries") or []
    if not queries:
        return "-- No query generated"
    ibis_expr = queries[0] if not isinstance(queries[0], dict) else queries[0].get("ibis_expr")
    if ibis_expr is None:
        return "-- No Ibis expression available"

    profile = CLOUD_WAREHOUSE_PROFILES.get(profile_key, CLOUD_WAREHOUSE_PROFILES["snowflake"])
    target_dialect = profile.get("sqlglot_dialect", "snowflake")
    try:
        raw_sql = str(ibis.to_sql(ibis_expr, dialect=target_dialect))
    except Exception:
        try:
            raw_sql = str(adapter.controller.con.compile(ibis_expr))
        except Exception as exc:
            raw_sql = f"-- SQL compilation preview unavailable: {exc}"
    return raw_sql


def _capability_badge(label: str, enabled: bool, detail: str = "") -> html.Span:
    bg = "#ecfdf5" if enabled else "#f8fafc"
    fg = "#047857" if enabled else "#64748b"
    border = "#a7f3d0" if enabled else "#e2e8f0"
    dot = "●" if enabled else "○"
    text = f"{dot} {label}" + (f": {detail}" if detail else "")
    return html.Span(
        text,
        style={
            "fontSize": "11px",
            "fontWeight": "600",
            "padding": "3px 9px",
            "borderRadius": "999px",
            "background": bg,
            "color": fg,
            "border": f"1px solid {border}",
            "whiteSpace": "nowrap",
        },
    )


def render_capability_bar(profile_key: str, allow_mutations: bool, allow_ddl: bool) -> html.Div:
    profile = CLOUD_WAREHOUSE_PROFILES.get(profile_key, CLOUD_WAREHOUSE_PROFILES["snowflake"])
    caps: BackendCapabilities = resolve_backend_capabilities(
        None,
        connection_uri=profile["uri"],
        allow_remote_mutations=allow_mutations,
        allow_remote_ddl=allow_ddl,
    )
    return html.Div(
        style={
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
            "flexWrap": "wrap",
            "gap": "8px",
            "padding": "8px 20px",
            "background": "#f8fafc",
            "borderBottom": "1px solid #e2e8f0",
        },
        children=[
            html.Div(
                style={"display": "flex", "alignItems": "center", "gap": "8px", "flexWrap": "wrap"},
                children=[
                    html.Span(
                        f"Engine: {caps.backend_name.upper()}",
                        style={
                            "fontSize": "11px",
                            "fontWeight": "700",
                            "padding": "3px 10px",
                            "borderRadius": "6px",
                            "background": profile["badge_bg"],
                            "color": profile["badge_color"],
                        },
                    ),
                    _capability_badge("Cloud / Remote", caps.is_cloud_or_remote),
                    _capability_badge("Zero-Copy Lazy Ibis", True),
                    _capability_badge("SQL Mutations", caps.supports_sql_mutations),
                    _capability_badge("DDL Rollups", caps.supports_ddl_materialization),
                    _capability_badge("Native ILIKE", caps.supports_ilike, "ILIKE" if caps.supports_ilike else "LOWER+LIKE"),
                    _capability_badge("RowID", caps.supports_rowid, "rowid" if caps.supports_rowid else "PK/Composite"),
                    html.Span(
                        f"Bind: {caps.param_style}",
                        style={
                            "fontSize": "11px",
                            "fontWeight": "600",
                            "padding": "3px 9px",
                            "borderRadius": "999px",
                            "background": "#eef2ff",
                            "color": "#4338ca",
                            "border": "1px solid #c7d2fe",
                        },
                    ),
                ],
            ),
            html.Div(
                profile["description"],
                style={"fontSize": "11px", "color": "#475569", "fontWeight": "500"},
            ),
        ],
    )


def _cloud_pivot_config() -> dict[str, Any]:
    cfg = dict(saas_workspace_config("cloud"))
    cfg["id"] = PIVOT_ID
    cfg["table"] = TABLE_NAME
    cfg["pivotTitle"] = "Cloud Data Warehouse Pushdown · Zero-Copy Lazy ibis.Table"
    cfg["style"] = {"height": "100%", "width": "100%"}
    return cfg


app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Cloud Data Warehouse & Ibis Pushdown — Enterprise Pivot"

app.layout = html.Div(
    style={
        "height": "100vh",
        "width": "100vw",
        "display": "flex",
        "flexDirection": "column",
        "background": "#f8fafc",
        "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
        "overflow": "hidden",
    },
    children=[
        # Top Enterprise Header
        html.Div(
            style={
                "height": "54px",
                "minHeight": "54px",
                "background": "#0f172a",
                "borderBottom": "1px solid #1e293b",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "space-between",
                "padding": "0 20px",
                "color": "#f8fafc",
                "gap": "16px",
            },
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "12px"},
                    children=[
                        html.Div(
                            "CLOUD IBIS ENGINE",
                            style={
                                "background": "linear-gradient(135deg, #0284c7 0%, #4f46e5 100%)",
                                "color": "#fff",
                                "fontWeight": "700",
                                "fontSize": "11px",
                                "padding": "4px 10px",
                                "borderRadius": "6px",
                                "letterSpacing": "0.06em",
                            },
                        ),
                        html.Span(
                            "Multi-Backend Cloud Data Warehouse Pushdown",
                            style={"fontWeight": "600", "fontSize": "15px", "color": "#f8fafc"},
                        ),
                        html.Span(
                            f"{SAAS_DEFAULT_ROWS:,} Workloads  ·  Zero-Copy Lazy ibis.Table  ·  Capability-Governed Architecture",
                            style={"fontSize": "12px", "color": "#94a3b8"},
                        ),
                    ],
                ),
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                    children=[
                        html.Span(
                            "Target Warehouse Profile:",
                            style={"fontSize": "12px", "color": "#cbd5e1", "fontWeight": "500"},
                        ),
                        dcc.Dropdown(
                            id="cloud-profile-selector",
                            options=[
                                {"label": cfg["label"], "value": key}
                                for key, cfg in CLOUD_WAREHOUSE_PROFILES.items()
                            ],
                            value="snowflake",
                            clearable=False,
                            style={"width": "220px", "fontSize": "12px", "color": "#0f172a"},
                        ),
                        dcc.Checklist(
                            id="cloud-governance-toggles",
                            options=[
                                {"label": " Allow Remote Mutations", "value": "mutations"},
                                {"label": " Allow Remote DDL", "value": "ddl"},
                            ],
                            value=[],
                            inline=True,
                            style={"fontSize": "11px", "color": "#e2e8f0", "display": "flex", "gap": "10px"},
                            inputStyle={"marginRight": "4px"},
                            labelStyle={"marginRight": "10px", "cursor": "pointer"},
                        ),
                        html.Button(
                            "Inspect Pushdown SQL",
                            id="toggle-sql-drawer-btn",
                            n_clicks=0,
                            style={
                                "fontSize": "12px",
                                "fontWeight": "600",
                                "padding": "6px 12px",
                                "borderRadius": "6px",
                                "background": "#1e293b",
                                "color": "#38bdf8",
                                "border": "1px solid #334155",
                                "cursor": "pointer",
                            },
                        ),
                    ],
                ),
            ],
        ),
        # Live BackendCapabilities Telemetry Bar
        html.Div(
            id="cloud-capability-bar",
            children=render_capability_bar("snowflake", allow_mutations=False, allow_ddl=False),
        ),
        # Collapsible Compiled SQL & Connection URI Inspector Drawer
        html.Div(
            id="sql-inspector-drawer",
            style={"display": "none"},
            children=[],
        ),
        # Main Pivot Grid Workspace
        html.Div(
            style={"flex": "1", "minHeight": "0", "width": "100%", "position": "relative"},
            children=[
                DashTanstackPivot(**_cloud_pivot_config()),
            ],
        ),
    ],
)


@app.callback(
    Output("cloud-capability-bar", "children"),
    Output("sql-inspector-drawer", "style"),
    Output("sql-inspector-drawer", "children"),
    Input("cloud-profile-selector", "value"),
    Input("cloud-governance-toggles", "value"),
    Input("toggle-sql-drawer-btn", "n_clicks"),
)
def _update_cloud_telemetry(profile_key: str, governance_values: list[str] | None, n_clicks: int):
    gov = set(governance_values or [])
    allow_mutations = "mutations" in gov
    allow_ddl = "ddl" in gov
    bar = render_capability_bar(profile_key, allow_mutations=allow_mutations, allow_ddl=allow_ddl)

    drawer_open = bool((n_clicks or 0) % 2 == 1)
    if not drawer_open:
        return bar, {"display": "none"}, []

    profile = CLOUD_WAREHOUSE_PROFILES.get(profile_key, CLOUD_WAREHOUSE_PROFILES["snowflake"])
    compiled_sql = compile_sample_pushdown_sql(profile_key)
    drawer_style = {
        "display": "grid",
        "gridTemplateColumns": "1fr 1.4fr",
        "gap": "16px",
        "padding": "12px 20px",
        "background": "#0f172a",
        "color": "#e2e8f0",
        "borderBottom": "1px solid #334155",
        "maxHeight": "210px",
        "overflowY": "auto",
    }
    drawer_children = [
        html.Div(
            children=[
                html.Div(
                    f"Connection & Governance Profile — {profile['label']}",
                    style={"fontSize": "12px", "fontWeight": "700", "color": "#38bdf8", "marginBottom": "6px"},
                ),
                html.Code(
                    f"BACKEND_URI=\"{profile['uri']}\"\n"
                    f"PIVOT_ALLOW_REMOTE_MUTATIONS=\"{'1' if allow_mutations else '0'}\"\n"
                    f"PIVOT_ALLOW_REMOTE_DDL=\"{'1' if allow_ddl else '0'}\"\n"
                    f"PIVOT_SCHEMA_CACHE_TTL_SECONDS=\"300\"",
                    style={
                        "display": "block",
                        "whiteSpace": "pre-wrap",
                        "fontSize": "11px",
                        "padding": "8px 10px",
                        "background": "#1e293b",
                        "borderRadius": "6px",
                        "color": "#a5f3fc",
                        "border": "1px solid #334155",
                    },
                ),
            ]
        ),
        html.Div(
            children=[
                html.Div(
                    f"Ibis Compiled Pushdown SQL ({profile['sqlglot_dialect'].upper()} Dialect)",
                    style={"fontSize": "12px", "fontWeight": "700", "color": "#38bdf8", "marginBottom": "6px"},
                ),
                html.Pre(
                    compiled_sql,
                    style={
                        "margin": 0,
                        "fontSize": "11px",
                        "padding": "8px 10px",
                        "background": "#1e293b",
                        "borderRadius": "6px",
                        "color": "#f8fafc",
                        "border": "1px solid #334155",
                        "overflowX": "auto",
                    },
                ),
            ]
        ),
    ]
    return bar, drawer_style, drawer_children


class _CloudPrincipal:
    id = "cloud-warehouse-demo"
    role = "cloud_architect"
    attributes = {}


register_pivot_app(
    app,
    adapter_getter=get_adapter,
    pivot_id=PIVOT_ID,
    debug=False,
    allowed_drill_tables=(TABLE_NAME,),
    drill_authorizer=lambda _request, table_name: table_name == TABLE_NAME,
    runtime_dataset_authorizer=RuntimeDatasetAuthorizer({
        TABLE_NAME: {
            "roles": {"cloud_architect"},
            "operations": {"read", "export"},
            "rowSecurity": {"mode": "global"},
        },
    }),
    runtime_principal_getter=lambda _request: _CloudPrincipal(),
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Cloud Data Warehouse & Ibis Pushdown Showcase")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8055")), help="Port to run on (default: 8055)")
    args = parser.parse_args()
    get_adapter()
    print(f"\n[Cloud Warehouse & Ibis Demo] Ready -> http://127.0.0.1:{args.port}\n")
    app.run(debug=False, use_reloader=False, port=args.port)
