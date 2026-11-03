"""Multi-Industry Showcase Hub for serverside-pivot.

Combines Trading, People Analytics, and B2B SaaS in a single interactive dashboard.

Run with:
    python examples/demo_showcase_hub.py
and open http://127.0.0.1:8054
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dash import Dash, Input, Output, dcc, html

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import RuntimeDatasetAuthorizer, register_pivot_app

from examples.demo_datasets import (
    HR_DEFAULT_ROWS,
    HR_TABLE_NAME,
    SAAS_DEFAULT_ROWS,
    SAAS_TABLE_NAME,
    TRADER_DEFAULT_ROWS,
    TRADER_TABLE_NAME,
    get_hr_adapter,
    get_saas_adapter,
    get_trader_adapter,
    hr_workspace_config,
    saas_workspace_config,
    trader_workspace_config,
)

app = Dash(
    __name__,
    title="Industry Showcases · serverside-pivot",
    suppress_callback_exceptions=True,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600;700&display=swap"
    ],
)

PAGE_STYLE = {
    "minHeight": "100vh",
    "background": "#0B111D",
    "color": "#E2E8F0",
    "fontFamily": "'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif",
    "paddingBottom": "48px",
}

CONTAINER_STYLE = {
    "width": "min(1600px, calc(100% - 48px))",
    "margin": "0 auto",
    "paddingTop": "24px",
}

TAB_STYLE = {
    "padding": "12px 24px",
    "fontWeight": 700,
    "fontSize": "13px",
    "letterSpacing": "0.02em",
    "borderRadius": "10px",
    "cursor": "pointer",
    "border": "1px solid transparent",
    "transition": "all 0.15s ease",
    "color": "#94A3B8",
    "background": "transparent",
}

SELECTED_TAB_STYLE = {
    "padding": "12px 24px",
    "fontWeight": 800,
    "fontSize": "13px",
    "letterSpacing": "0.02em",
    "borderRadius": "10px",
    "cursor": "pointer",
    "border": "1px solid #38BDF8",
    "color": "#FFFFFF",
    "background": "rgba(56, 189, 248, 0.15)",
    "boxShadow": "0 0 15px rgba(56, 189, 248, 0.2)",
}


app.layout = html.Div(
    style=PAGE_STYLE,
    children=[
        html.Main(
            style=CONTAINER_STYLE,
            children=[
                # Master Header
                html.Header(
                    style={
                        "display": "flex",
                        "justifyContent": "space-between",
                        "alignItems": "flex-start",
                        "flexWrap": "wrap",
                        "gap": "20px",
                        "paddingBottom": "20px",
                        "borderBottom": "1px solid #1E293B",
                    },
                    children=[
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.Span("⚡ SERVERSIDE-PIVOT V1.0", style={"fontSize": "11px", "fontWeight": 800, "padding": "4px 10px", "borderRadius": "999px", "background": "rgba(56, 189, 248, 0.15)", "color": "#38BDF8", "border": "1px solid rgba(56, 189, 248, 0.3)"}),
                                        html.Span("ENTERPRISE SHOWCASES", style={"fontSize": "11px", "fontWeight": 800, "padding": "4px 10px", "borderRadius": "999px", "background": "rgba(168, 85, 247, 0.15)", "color": "#C084FC", "border": "1px solid rgba(168, 85, 247, 0.3)"}),
                                    ],
                                    style={"display": "flex", "gap": "8px", "marginBottom": "12px"},
                                ),
                                html.H1(
                                    "Turnkey Server-Side Pivot Showcases",
                                    style={"fontSize": "clamp(28px, 3.5vw, 44px)", "fontWeight": 800, "letterSpacing": "-0.04em", "margin": "0 0 8px", "color": "#F8FAFC"},
                                ),
                                html.P(
                                    "Explore high-performance interactive pivot workspaces across Finance, HR, and SaaS. Powered by DuckDB & Ibis with O(D) batching, virtual scrolling, and zero client memory crashes.",
                                    style={"fontSize": "15px", "lineHeight": 1.6, "color": "#94A3B8", "margin": 0, "maxWidth": "860px"},
                                ),
                            ]
                        ),
                        html.Div(
                            style={"display": "flex", "gap": "10px", "alignItems": "center"},
                            children=[
                                html.A(
                                    "GitHub Repo",
                                    href="https://github.com/Ramzy22/serverside_pivot",
                                    target="_blank",
                                    style={"color": "#F8FAFC", "textDecoration": "none", "fontSize": "13px", "fontWeight": 600, "padding": "8px 16px", "borderRadius": "8px", "background": "#1E293B", "border": "1px solid #334155"},
                                ),
                            ],
                        ),
                    ],
                ),

                # Industry Selector Tabs
                html.Nav(
                    style={"margin": "24px 0 20px", "display": "flex", "gap": "10px", "background": "#0F172A", "padding": "6px", "borderRadius": "14px", "border": "1px solid #1E293B", "width": "fit-content"},
                    children=[
                        dcc.Tabs(
                            id="demo-tabs",
                            value="tab-trader",
                            parent_style={"display": "flex", "gap": "8px"},
                            content_style={"display": "none"},
                            children=[
                                dcc.Tab(label="📈 Trading & Risk (50k rows)", value="tab-trader", style=TAB_STYLE, selected_style=SELECTED_TAB_STYLE),
                                dcc.Tab(label="👥 People Analytics (35k rows)", value="tab-hr", style=TAB_STYLE, selected_style=SELECTED_TAB_STYLE),
                                dcc.Tab(label="🚀 B2B SaaS Growth (40k rows)", value="tab-saas", style=TAB_STYLE, selected_style=SELECTED_TAB_STYLE),
                            ],
                        ),
                    ],
                ),

                # Tab Content Container
                html.Div(id="demo-content"),
            ],
        )
    ],
)


@app.callback(
    Output("demo-content", "children"),
    Input("demo-tabs", "value"),
)
def render_demo_tab(tab_name: str):
    if tab_name == "tab-trader":
        return html.Div(
            children=[
                # KPI Cards
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(200px, 1fr))", "gap": "12px", "marginBottom": "20px"},
                    children=[
                        html.Div([html.Div("PORTFOLIO NOTIONAL", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("$214.8B", style={"fontSize": "22px", "color": "#38BDF8", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("TODAY'S NET PNL", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("+$48.62M", style={"fontSize": "22px", "color": "#34D399", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("PORTFOLIO VAR 95%", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("$14.28M", style={"fontSize": "22px", "color": "#F59E0B", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("AGGREGATE DV01", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("$482.4k", style={"fontSize": "22px", "color": "#A78BFA", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("ACTIVE POSITIONS", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div(f"{TRADER_DEFAULT_ROWS:,}", style={"fontSize": "22px", "color": "#E2E8F0", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                    ],
                ),
                html.Div(
                    style={"background": "#090F1B", "borderRadius": "16px", "border": "1px solid #1E293B", "overflow": "hidden"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-between", "padding": "12px 18px", "background": "rgba(15, 23, 42, 0.65)", "borderBottom": "1px solid #1E293B"},
                            children=[
                                html.Span("Institutional Trading · Asset Class > Desk > Ticker", style={"fontSize": "12px", "fontWeight": 700, "color": "#38BDF8"}),
                                html.Span("Stand-alone launcher: python examples/demo_trader.py --port 8051", style={"fontSize": "11px", "color": "#64748B"}),
                            ],
                        ),
                        DashTanstackPivot(**trader_workspace_config("hub")),
                    ],
                ),
            ]
        )
    elif tab_name == "tab-hr":
        return html.Div(
            children=[
                # KPI Cards
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(200px, 1fr))", "gap": "12px", "marginBottom": "20px"},
                    children=[
                        html.Div([html.Div("TOTAL WORKFORCE", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div(f"{HR_DEFAULT_ROWS:,}", style={"fontSize": "22px", "color": "#818CF8", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("AVG BASE SALARY", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("$178,450", style={"fontSize": "22px", "color": "#2DD4BF", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("ANNUAL BONUS POOL", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("$142.8M", style={"fontSize": "22px", "color": "#38BDF8", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("ENGAGEMENT INDEX", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("84.2 / 100", style={"fontSize": "22px", "color": "#34D399", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                        html.Div([html.Div("ATTRITION RISK", style={"fontSize": "10px", "color": "#94A3B8", "fontWeight": 700}), html.Div("5.8%", style={"fontSize": "22px", "color": "#F472B6", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "#0F172A", "padding": "14px", "borderRadius": "12px", "border": "1px solid #1E293B"}),
                    ],
                ),
                html.Div(
                    style={"background": "#090F1B", "borderRadius": "16px", "border": "1px solid #1E293B", "overflow": "hidden"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-between", "padding": "12px 18px", "background": "rgba(15, 23, 42, 0.65)", "borderBottom": "1px solid #1E293B"},
                            children=[
                                html.Span("People Analytics · Department > Sub-Team > Seniority Level", style={"fontSize": "12px", "fontWeight": 700, "color": "#818CF8"}),
                                html.Span("Stand-alone launcher: python examples/demo_hr.py --port 8052", style={"fontSize": "11px", "color": "#64748B"}),
                            ],
                        ),
                        DashTanstackPivot(**hr_workspace_config("hub")),
                    ],
                ),
            ]
        )
    else:
        return html.Div(
            children=[
                # KPI Cards
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(200px, 1fr))", "gap": "12px", "marginBottom": "20px"},
                    children=[
                        html.Div([html.Div("ANNUAL RUN-RATE (ARR)", style={"fontSize": "10px", "color": "#C4B5FD", "fontWeight": 700}), html.Div("$146.8M", style={"fontSize": "22px", "color": "#38BDF8", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "rgba(22, 16, 42, 0.8)", "padding": "14px", "borderRadius": "12px", "border": "1px solid rgba(139, 92, 246, 0.25)"}),
                        html.Div([html.Div("MONTHLY RECURRING (MRR)", style={"fontSize": "10px", "color": "#C4B5FD", "fontWeight": 700}), html.Div("$12.23M", style={"fontSize": "22px", "color": "#818CF8", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "rgba(22, 16, 42, 0.8)", "padding": "14px", "borderRadius": "12px", "border": "1px solid rgba(139, 92, 246, 0.25)"}),
                        html.Div([html.Div("NET REVENUE RETENTION (NRR)", style={"fontSize": "10px", "color": "#C4B5FD", "fontWeight": 700}), html.Div("124.6%", style={"fontSize": "22px", "color": "#34D399", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "rgba(22, 16, 42, 0.8)", "padding": "14px", "borderRadius": "12px", "border": "1px solid rgba(139, 92, 246, 0.25)"}),
                        html.Div([html.Div("LTV / CAC EFFICIENCY", style={"fontSize": "10px", "color": "#C4B5FD", "fontWeight": 700}), html.Div("8.4x", style={"fontSize": "22px", "color": "#F472B6", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "rgba(22, 16, 42, 0.8)", "padding": "14px", "borderRadius": "12px", "border": "1px solid rgba(139, 92, 246, 0.25)"}),
                        html.Div([html.Div("ACTIVE CUSTOMER ACCOUNTS", style={"fontSize": "10px", "color": "#C4B5FD", "fontWeight": 700}), html.Div(f"{SAAS_DEFAULT_ROWS:,}", style={"fontSize": "22px", "color": "#E2E8F0", "fontWeight": 800, "fontFamily": "'JetBrains Mono'"})], style={"background": "rgba(22, 16, 42, 0.8)", "padding": "14px", "borderRadius": "12px", "border": "1px solid rgba(139, 92, 246, 0.25)"}),
                    ],
                ),
                html.Div(
                    style={"background": "#0D0A1C", "borderRadius": "16px", "border": "1px solid rgba(139, 92, 246, 0.25)", "overflow": "hidden"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-between", "padding": "12px 18px", "background": "rgba(22, 16, 42, 0.8)", "borderBottom": "1px solid rgba(139, 92, 246, 0.2)"},
                            children=[
                                html.Span("B2B SaaS Growth · Segment > Subscription Tier > Acquisition Channel", style={"fontSize": "12px", "fontWeight": 700, "color": "#C4B5FD"}),
                                html.Span("Stand-alone launcher: python examples/demo_saas.py --port 8053", style={"fontSize": "11px", "color": "#8B5CF6"}),
                            ],
                        ),
                        DashTanstackPivot(**saas_workspace_config("hub")),
                    ],
                ),
            ]
        )


class _HubPrincipal:
    id = "hub-demo"
    role = "admin"
    attributes = {}


register_pivot_app(
    app,
    adapter_getter=get_trader_adapter,
    pivot_id="trader-hub-pivot",
    runtime_dataset_authorizer=RuntimeDatasetAuthorizer({
        TRADER_TABLE_NAME: {
            "roles": {"admin"},
            "operations": {"read", "export"},
            "rowSecurity": {"mode": "global"},
        },
    }),
    runtime_principal_getter=lambda _request: _HubPrincipal(),
)

register_pivot_app(
    app,
    adapter_getter=get_hr_adapter,
    pivot_id="hr-hub-pivot",
    runtime_dataset_authorizer=RuntimeDatasetAuthorizer({
        HR_TABLE_NAME: {
            "roles": {"admin"},
            "operations": {"read", "export"},
            "rowSecurity": {"mode": "global"},
        },
    }),
    runtime_principal_getter=lambda _request: _HubPrincipal(),
)

register_pivot_app(
    app,
    adapter_getter=get_saas_adapter,
    pivot_id="saas-hub-pivot",
    runtime_dataset_authorizer=RuntimeDatasetAuthorizer({
        SAAS_TABLE_NAME: {
            "roles": {"admin"},
            "operations": {"read", "export"},
            "rowSecurity": {"mode": "global"},
        },
    }),
    runtime_principal_getter=lambda _request: _HubPrincipal(),
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Unified Multi-Industry Pivot Showcase")
    parser.add_argument("--port", type=int, default=8054, help="Port to run on (default: 8054)")
    args = parser.parse_args()
    print(f"\n⚡ Unified Showcase Hub running at: http://127.0.0.1:{args.port}\n")
    app.run(debug=False, use_reloader=False, port=args.port)
