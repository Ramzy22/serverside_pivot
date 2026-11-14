"""Trading Desk & Portfolio Risk Showcase for serverside-pivot.

Run with:
    python examples/demo_trader.py
and open http://127.0.0.1:8051
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dash import Dash, html

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from serverside_pivot import DashTanstackPivot
from pivot_engine import RuntimeDatasetAuthorizer, register_pivot_app

from examples.demo_datasets import (
    TRADER_DEFAULT_ROWS,
    TRADER_TABLE_NAME,
    get_trader_adapter,
    trader_workspace_config,
)

app = Dash(
    __name__,
    title="Trading & Portfolio Risk · serverside-pivot",
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&family=Inter:wght@400;500;600;700;800&display=swap"
    ],
)

PAGE_STYLE = {
    "minHeight": "100vh",
    "background": "linear-gradient(180deg, #050B14 0%, #0A1220 100%)",
    "color": "#E2E8F0",
    "fontFamily": "Inter, -apple-system, BlinkMacSystemFont, sans-serif",
    "paddingBottom": "40px",
}

CONTAINER_STYLE = {
    "width": "min(1580px, calc(100% - 48px))",
    "margin": "0 auto",
    "paddingTop": "24px",
}

BADGE_STYLE = {
    "display": "inline-flex",
    "alignItems": "center",
    "gap": "6px",
    "padding": "4px 12px",
    "borderRadius": "9999px",
    "fontSize": "11px",
    "fontWeight": 700,
    "letterSpacing": "0.08em",
    "textTransform": "uppercase",
    "border": "1px solid rgba(16, 185, 129, 0.35)",
    "background": "rgba(16, 185, 129, 0.12)",
    "color": "#34D399",
}

def metric_card(label: str, value: str, subtext: str, delta_color: str = "#10B981"):
    return html.Div(
        style={
            "background": "rgba(13, 23, 38, 0.75)",
            "border": "1px solid #1E293B",
            "borderRadius": "14px",
            "padding": "16px 20px",
            "boxShadow": "0 4px 20px rgba(0, 0, 0, 0.25)",
            "backdropFilter": "blur(12px)",
        },
        children=[
            html.Div(label, style={"fontSize": "11px", "fontWeight": 600, "textTransform": "uppercase", "letterSpacing": "0.06em", "color": "#94A3B8"}),
            html.Div(value, style={"fontSize": "26px", "fontWeight": 800, "color": "#F8FAFC", "margin": "6px 0 2px", "fontFamily": "'JetBrains Mono', monospace"}),
            html.Div(subtext, style={"fontSize": "12px", "fontWeight": 600, "color": delta_color}),
        ],
    )

app.layout = html.Div(
    style=PAGE_STYLE,
    children=[
        html.Main(
            style=CONTAINER_STYLE,
            children=[
                # Top App Bar
                html.Header(
                    style={
                        "display": "flex",
                        "justifyContent": "space-between",
                        "alignItems": "flex-start",
                        "flexWrap": "wrap",
                        "gap": "20px",
                        "paddingBottom": "24px",
                        "borderBottom": "1px solid #1E293B",
                    },
                    children=[
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.Span("● LIVE EXECUTION", style=BADGE_STYLE),
                                        html.Span("HIGH-FREQUENCY RISK ENGINE", style={**BADGE_STYLE, "border": "1px solid rgba(56, 189, 248, 0.35)", "background": "rgba(56, 189, 248, 0.12)", "color": "#38BDF8"}),
                                    ],
                                    style={"display": "flex", "gap": "10px", "marginBottom": "12px"},
                                ),
                                html.H1(
                                    "Institutional Trading & Portfolio Risk",
                                    style={"fontSize": "clamp(26px, 3.2vw, 42px)", "fontWeight": 800, "letterSpacing": "-0.04em", "margin": "0 0 8px", "color": "#F8FAFC"},
                                ),
                                html.P(
                                    f"Instantaneous multi-dimensional aggregation across {TRADER_DEFAULT_ROWS:,} institutional positions. Slicing Equities, Rates, FX, Commodities, and Crypto with sub-second server-side DuckDB execution.",
                                    style={"fontSize": "14px", "lineHeight": 1.6, "color": "#94A3B8", "margin": 0, "maxWidth": "860px"},
                                ),
                            ]
                        ),
                        html.Div(
                            style={"display": "flex", "gap": "12px", "alignItems": "center"},
                            children=[
                                html.A(
                                    "← All Demos",
                                    href="/",
                                    style={"color": "#94A3B8", "textDecoration": "none", "fontSize": "13px", "fontWeight": 600, "padding": "8px 14px", "borderRadius": "8px", "border": "1px solid #334155"},
                                ),
                                html.Div(
                                    children=[
                                        html.Div("ENGINE LATENCY", style={"fontSize": "10px", "color": "#64748B", "fontWeight": 700}),
                                        html.Div("12ms avg", style={"fontSize": "15px", "color": "#34D399", "fontWeight": 700, "fontFamily": "'JetBrains Mono', monospace"}),
                                    ],
                                    style={"background": "#0F172A", "padding": "8px 16px", "borderRadius": "10px", "border": "1px solid #1E293B"},
                                ),
                            ],
                        ),
                    ],
                ),

                # KPI Ribbon
                html.Section(
                    style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "14px", "padding": "24px 0"},
                    children=[
                        metric_card("Total Book Notional", "$214.8B", "Across 5 Desks & 12 Books", "#38BDF8"),
                        metric_card("Today's Net PnL", "+$48.62M", "+3.42% vs prior session", "#10B981"),
                        metric_card("Portfolio VaR (95%)", "$14.28M", "Normal parametric 1-day", "#F59E0B"),
                        metric_card("Aggregate DV01", "$482.4k", "Per 1bp parallel curve shift", "#A78BFA"),
                        metric_card("Active Positions", f"{TRADER_DEFAULT_ROWS:,}", "Zero-copy Arrow memory", "#34D399"),
                    ],
                ),

                # Main Pivot Workspace Card
                html.Section(
                    style={
                        "background": "#090F1B",
                        "border": "1px solid #1E293B",
                        "borderRadius": "18px",
                        "overflow": "hidden",
                        "boxShadow": "0 24px 60px rgba(0, 0, 0, 0.45)",
                    },
                    children=[
                        html.Div(
                            style={
                                "display": "flex",
                                "justifyContent": "space-between",
                                "alignItems": "center",
                                "padding": "14px 20px",
                                "background": "rgba(15, 23, 42, 0.65)",
                                "borderBottom": "1px solid #1E293B",
                            },
                            children=[
                                html.Div(
                                    children=[
                                        html.Span("PORTFOLIO EXPLORER", style={"fontSize": "11px", "fontWeight": 700, "letterSpacing": "0.08em", "color": "#38BDF8", "fontFamily": "'JetBrains Mono', monospace"}),
                                        html.Span("Drag dimensions into rows or columns, toggle sub-totals, or click cells to drill down.", style={"marginLeft": "12px", "fontSize": "12px", "color": "#64748B"}),
                                    ]
                                ),
                                html.Div(
                                    children=[
                                        html.Span("O(D) Hierarchical Query", style={"fontSize": "11px", "fontWeight": 600, "color": "#94A3B8", "background": "#1E293B", "padding": "4px 10px", "borderRadius": "6px"}),
                                    ]
                                ),
                            ],
                        ),
                        DashTanstackPivot(**trader_workspace_config("dash")),
                    ],
                ),
            ],
        )
    ],
)


class _DemoPrincipal:
    id = "trader-demo"
    role = "trader"
    attributes = {}


register_pivot_app(
    app,
    adapter_getter=get_trader_adapter,
    pivot_id="trader-dash-pivot",
    runtime_dataset_authorizer=RuntimeDatasetAuthorizer({
        TRADER_TABLE_NAME: {
            "roles": {"trader"},
            "operations": {"read", "export"},
            "rowSecurity": {"mode": "global"},
        },
    }),
    runtime_principal_getter=lambda _request: _DemoPrincipal(),
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Trading Desk Pivot Showcase")
    parser.add_argument("--port", type=int, default=8051, help="Port to run on (default: 8051)")
    args = parser.parse_args()
    print(f"\nTrading Desk Demo running at: http://127.0.0.1:{args.port}\n")
    app.run(debug=False, use_reloader=False, port=args.port)
