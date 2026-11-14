"""B2B SaaS Revenue Intelligence Showcase for serverside-pivot.

Run with:
    python examples/demo_saas.py
and open http://127.0.0.1:8053
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
    SAAS_DEFAULT_ROWS,
    SAAS_TABLE_NAME,
    get_saas_adapter,
    saas_workspace_config,
)

app = Dash(
    __name__,
    title="B2B SaaS Intelligence · serverside-pivot",
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600;700&display=swap"
    ],
)

PAGE_STYLE = {
    "minHeight": "100vh",
    "background": "linear-gradient(180deg, #090614 0%, #100C22 100%)",
    "color": "#EDE9FE",
    "fontFamily": "'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif",
    "paddingBottom": "44px",
}

CONTAINER_STYLE = {
    "width": "min(1580px, calc(100% - 48px))",
    "margin": "0 auto",
    "paddingTop": "26px",
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
    "border": "1px solid rgba(139, 92, 246, 0.4)",
    "background": "rgba(139, 92, 246, 0.15)",
    "color": "#C4B5FD",
}

def metric_card(label: str, value: str, subtext: str, delta_color: str = "#A78BFA"):
    return html.Div(
        style={
            "background": "rgba(22, 16, 42, 0.75)",
            "border": "1px solid rgba(139, 92, 246, 0.22)",
            "borderRadius": "16px",
            "padding": "16px 20px",
            "boxShadow": "0 8px 24px rgba(0, 0, 0, 0.35)",
            "backdropFilter": "blur(14px)",
        },
        children=[
            html.Div(label, style={"fontSize": "11px", "fontWeight": 600, "textTransform": "uppercase", "letterSpacing": "0.06em", "color": "#A78BFA"}),
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
                        "borderBottom": "1px solid rgba(139, 92, 246, 0.2)",
                    },
                    children=[
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.Span("● REVENUE INTELLIGENCE", style=BADGE_STYLE),
                                        html.Span("B2B SAAS COHORTS", style={**BADGE_STYLE, "border": "1px solid rgba(236, 72, 153, 0.4)", "background": "rgba(236, 72, 153, 0.15)", "color": "#F472B6"}),
                                    ],
                                    style={"display": "flex", "gap": "10px", "marginBottom": "12px"},
                                ),
                                html.H1(
                                    "B2B SaaS Growth & Cohort Intelligence",
                                    style={"fontSize": "clamp(26px, 3.2vw, 42px)", "fontWeight": 800, "letterSpacing": "-0.04em", "margin": "0 0 8px", "color": "#FFFFFF"},
                                ),
                                html.P(
                                    f"Sub-second ARR, MRR, Net Revenue Retention (NRR), and CAC/LTV unit economics across {SAAS_DEFAULT_ROWS:,} enterprise and SMB accounts. Real-time drill-down through acquisition channels and customer tiers.",
                                    style={"fontSize": "14px", "lineHeight": 1.6, "color": "#C4B5FD", "margin": 0, "maxWidth": "860px"},
                                ),
                            ]
                        ),
                        html.Div(
                            style={"display": "flex", "gap": "12px", "alignItems": "center"},
                            children=[
                                html.A(
                                    "← All Demos",
                                    href="/",
                                    style={"color": "#C4B5FD", "textDecoration": "none", "fontSize": "13px", "fontWeight": 600, "padding": "8px 14px", "borderRadius": "8px", "border": "1px solid rgba(139, 92, 246, 0.3)"},
                                ),
                                html.Div(
                                    children=[
                                        html.Div("EXPANSION VELOCITY", style={"fontSize": "10px", "color": "#A78BFA", "fontWeight": 700}),
                                        html.Div("+122.4% Net NRR", style={"fontSize": "14px", "color": "#38BDF8", "fontWeight": 700, "fontFamily": "'JetBrains Mono', monospace"}),
                                    ],
                                    style={"background": "rgba(139, 92, 246, 0.15)", "padding": "8px 16px", "borderRadius": "10px", "border": "1px solid rgba(139, 92, 246, 0.3)"},
                                ),
                            ],
                        ),
                    ],
                ),

                # KPI Ribbon
                html.Section(
                    style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "14px", "padding": "24px 0"},
                    children=[
                        metric_card("Annual Run-Rate (ARR)", "$146.8M", "+38.4% YoY ARR acceleration", "#38BDF8"),
                        metric_card("Monthly Recurring (MRR)", "$12.23M", "Current recognized baseline", "#818CF8"),
                        metric_card("Net Revenue Retention", "124.6%", "Top-quartile SaaS benchmark", "#34D399"),
                        metric_card("Average LTV / CAC", "8.4x", "Payback period ~7.2 months", "#F472B6"),
                        metric_card("Active Customer Seats", "482,900", "Across 40k subscriber accounts", "#A78BFA"),
                    ],
                ),

                # Main Pivot Workspace Card
                html.Section(
                    style={
                        "background": "#0D0A1C",
                        "border": "1px solid rgba(139, 92, 246, 0.25)",
                        "borderRadius": "18px",
                        "overflow": "hidden",
                        "boxShadow": "0 24px 60px rgba(0, 0, 0, 0.55)",
                    },
                    children=[
                        html.Div(
                            style={
                                "display": "flex",
                                "justifyContent": "space-between",
                                "alignItems": "center",
                                "padding": "14px 20px",
                                "background": "rgba(22, 16, 42, 0.8)",
                                "borderBottom": "1px solid rgba(139, 92, 246, 0.2)",
                            },
                            children=[
                                html.Div(
                                    children=[
                                        html.Span("COHORT INTELLIGENCE ENGINE", style={"fontSize": "11px", "fontWeight": 700, "letterSpacing": "0.08em", "color": "#C4B5FD", "fontFamily": "'JetBrains Mono', monospace"}),
                                        html.Span("Analyze quarterly cohorts, customer segments, and churn vs expansion metrics in real-time.", style={"marginLeft": "12px", "fontSize": "12px", "color": "#8B5CF6"}),
                                    ]
                                ),
                                html.Div(
                                    children=[
                                        html.Span("DuckDB In-Memory OLAP", style={"fontSize": "11px", "fontWeight": 600, "color": "#E9D5FF", "background": "rgba(139, 92, 246, 0.25)", "padding": "4px 10px", "borderRadius": "6px"}),
                                    ]
                                ),
                            ],
                        ),
                        DashTanstackPivot(**saas_workspace_config("dash")),
                    ],
                ),
            ],
        )
    ],
)


class _DemoPrincipal:
    id = "saas-demo"
    role = "growth_ops"
    attributes = {}


register_pivot_app(
    app,
    adapter_getter=get_saas_adapter,
    pivot_id="saas-dash-pivot",
    runtime_dataset_authorizer=RuntimeDatasetAuthorizer({
        SAAS_TABLE_NAME: {
            "roles": {"growth_ops"},
            "operations": {"read", "export"},
            "rowSecurity": {"mode": "global"},
        },
    }),
    runtime_principal_getter=lambda _request: _DemoPrincipal(),
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run B2B SaaS Metrics Pivot Showcase")
    parser.add_argument("--port", type=int, default=8053, help="Port to run on (default: 8053)")
    args = parser.parse_args()
    print(f"\nB2B SaaS Demo running at: http://127.0.0.1:{args.port}\n")
    app.run(debug=False, use_reloader=False, port=args.port)
