"""People Analytics & Workforce Planning Showcase for serverside-pivot.

Run with:
    python examples/demo_hr.py
and open http://127.0.0.1:8052
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
    HR_DEFAULT_ROWS,
    HR_TABLE_NAME,
    get_hr_adapter,
    hr_workspace_config,
)

app = Dash(
    __name__,
    title="People Analytics · serverside-pivot",
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap"
    ],
)

PAGE_STYLE = {
    "minHeight": "100vh",
    "background": "#F8FAFC",
    "color": "#0F172A",
    "fontFamily": "'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif",
    "paddingBottom": "48px",
}

CONTAINER_STYLE = {
    "width": "min(1580px, calc(100% - 48px))",
    "margin": "0 auto",
    "paddingTop": "28px",
}

BADGE_STYLE = {
    "display": "inline-flex",
    "alignItems": "center",
    "gap": "6px",
    "padding": "5px 12px",
    "borderRadius": "9999px",
    "fontSize": "11px",
    "fontWeight": 700,
    "letterSpacing": "0.06em",
    "textTransform": "uppercase",
    "border": "1px solid rgba(79, 70, 229, 0.25)",
    "background": "rgba(79, 70, 229, 0.08)",
    "color": "#4338CA",
}

def metric_card(label: str, value: str, subtext: str, delta_color: str = "#4F46E5"):
    return html.Div(
        style={
            "background": "#FFFFFF",
            "border": "1px solid #E2E8F0",
            "borderRadius": "16px",
            "padding": "18px 22px",
            "boxShadow": "0 2px 10px rgba(15, 23, 42, 0.04)",
        },
        children=[
            html.Div(label, style={"fontSize": "12px", "fontWeight": 600, "textTransform": "uppercase", "letterSpacing": "0.05em", "color": "#64748B"}),
            html.Div(value, style={"fontSize": "28px", "fontWeight": 800, "color": "#0F172A", "margin": "6px 0 2px", "fontFamily": "'Plus Jakarta Sans', sans-serif"}),
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
                        "borderBottom": "1px solid #E2E8F0",
                    },
                    children=[
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.Span("● PEOPLE ANALYTICS", style=BADGE_STYLE),
                                        html.Span("WORKFORCE INTELLIGENCE", style={**BADGE_STYLE, "border": "1px solid rgba(13, 148, 136, 0.25)", "background": "rgba(13, 148, 136, 0.08)", "color": "#0F766E"}),
                                    ],
                                    style={"display": "flex", "gap": "10px", "marginBottom": "12px"},
                                ),
                                html.H1(
                                    "Global Workforce & Compensation Planning",
                                    style={"fontSize": "clamp(26px, 3.2vw, 42px)", "fontWeight": 800, "letterSpacing": "-0.04em", "margin": "0 0 8px", "color": "#0F172A"},
                                ),
                                html.P(
                                    f"Instant headcount modeling, salary distribution, bonus pooling, and retention forecasting across {HR_DEFAULT_ROWS:,} employees worldwide. Zero client-side slowdown with server-side virtual scrolling.",
                                    style={"fontSize": "15px", "lineHeight": 1.6, "color": "#475569", "margin": 0, "maxWidth": "880px"},
                                ),
                            ]
                        ),
                        html.Div(
                            style={"display": "flex", "gap": "12px", "alignItems": "center"},
                            children=[
                                html.A(
                                    "← All Demos",
                                    href="/",
                                    style={"color": "#475569", "textDecoration": "none", "fontSize": "13px", "fontWeight": 600, "padding": "8px 14px", "borderRadius": "8px", "border": "1px solid #CBD5E1", "background": "#FFFFFF"},
                                ),
                                html.Div(
                                    children=[
                                        html.Div("GLOBAL COVERAGE", style={"fontSize": "10px", "color": "#64748B", "fontWeight": 700}),
                                        html.Div("8 Hubs · 100% Remote", style={"fontSize": "14px", "color": "#4338CA", "fontWeight": 700}),
                                    ],
                                    style={"background": "#EEF2FF", "padding": "8px 16px", "borderRadius": "10px", "border": "1px solid #C7D2FE"},
                                ),
                            ],
                        ),
                    ],
                ),

                # KPI Ribbon
                html.Section(
                    style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "14px", "padding": "24px 0"},
                    children=[
                        metric_card("Total Workforce", f"{HR_DEFAULT_ROWS:,}", "+14.2% YoY talent expansion", "#4F46E5"),
                        metric_card("Average Base Salary", "$178,450", "Across all roles & seniority levels", "#0D9488"),
                        metric_card("Annual Bonus Pool", "$142.8M", "102% target achievement pool", "#0284C7"),
                        metric_card("Company Engagement", "84.2 / 100", "+3.8 pts vs previous survey", "#16A34A"),
                        metric_card("Predicted Attrition Risk", "5.8%", "Low risk corridor (< 8.0%)", "#6366F1"),
                    ],
                ),

                # Main Pivot Workspace Card
                html.Section(
                    style={
                        "background": "#FFFFFF",
                        "border": "1px solid #E2E8F0",
                        "borderRadius": "18px",
                        "overflow": "hidden",
                        "boxShadow": "0 12px 36px rgba(15, 23, 42, 0.08)",
                    },
                    children=[
                        html.Div(
                            style={
                                "display": "flex",
                                "justifyContent": "space-between",
                                "alignItems": "center",
                                "padding": "16px 22px",
                                "background": "#F1F5F9",
                                "borderBottom": "1px solid #E2E8F0",
                            },
                            children=[
                                html.Div(
                                    children=[
                                        html.Span("ORGANIZATIONAL WORKSPACE", style={"fontSize": "11px", "fontWeight": 700, "letterSpacing": "0.08em", "color": "#4F46E5"}),
                                        html.Span("Slice by Department, Team, Office Location, or Quarter. Full Excel XLSX export and interactive chart visualization available.", style={"marginLeft": "12px", "fontSize": "12px", "color": "#64748B"}),
                                    ]
                                ),
                                html.Div(
                                    children=[
                                        html.Span("Real-Time Aggregations", style={"fontSize": "11px", "fontWeight": 600, "color": "#334155", "background": "#E2E8F0", "padding": "4px 10px", "borderRadius": "6px"}),
                                    ]
                                ),
                            ],
                        ),
                        DashTanstackPivot(**hr_workspace_config("dash")),
                    ],
                ),
            ],
        )
    ],
)


class _DemoPrincipal:
    id = "hr-demo"
    role = "people_analytics"
    attributes = {}


register_pivot_app(
    app,
    adapter_getter=get_hr_adapter,
    pivot_id="hr-dash-pivot",
    runtime_dataset_authorizer=RuntimeDatasetAuthorizer({
        HR_TABLE_NAME: {
            "roles": {"people_analytics"},
            "operations": {"read", "export"},
            "rowSecurity": {"mode": "global"},
        },
    }),
    runtime_principal_getter=lambda _request: _DemoPrincipal(),
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run HR & People Analytics Pivot Showcase")
    parser.add_argument("--port", type=int, default=8052, help="Port to run on (default: 8052)")
    args = parser.parse_args()
    print(f"\nHR & People Analytics Demo running at: http://127.0.0.1:{args.port}\n")
    app.run(debug=False, use_reloader=False, port=args.port)
