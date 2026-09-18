"""Flagship Dash showcase for serverside-pivot.

Run with ``python examples/flagship_dash.py`` and open http://127.0.0.1:8050.
"""

from __future__ import annotations

import sys
from pathlib import Path

from dash import Dash, html

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import register_pivot_app

from examples.flagship_demo_data import DEMO_ROW_COUNT, get_demo_adapter, workspace_config


app = Dash(
    __name__,
    title="serverside-pivot · Dash showcase",
    external_stylesheets=["https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Manrope:wght@400;500;600;700;800&display=swap"],
)

PAGE = {"minHeight": "100vh", "background": "#07111F", "color": "#E5EEF9", "fontFamily": "Manrope, ui-sans-serif, system-ui"}
SHELL = {"width": "min(1540px, calc(100% - 40px))", "margin": "0 auto", "padding": "30px 0 48px"}
PILL = {"display": "inline-flex", "alignItems": "center", "gap": "7px", "border": "1px solid rgba(125, 211, 252, .26)", "background": "rgba(14, 116, 144, .18)", "color": "#A5F3FC", "borderRadius": "999px", "padding": "6px 10px", "fontSize": "11px", "fontWeight": 800, "letterSpacing": ".07em", "textTransform": "uppercase"}


def capability(title: str, copy: str, symbol: str):
    return html.Div(
        style={"border": "1px solid #1E3852", "background": "linear-gradient(145deg, #0D1C2F, #091523)", "borderRadius": "16px", "padding": "18px", "minHeight": "142px"},
        children=[
            html.Div(symbol, style={"color": "#5EEAD4", "fontFamily": "DM Mono, monospace", "fontSize": "13px", "fontWeight": 700, "marginBottom": "13px"}),
            html.Div(title, style={"fontSize": "14px", "fontWeight": 800, "color": "#F8FAFC", "marginBottom": "7px"}),
            html.Div(copy, style={"fontSize": "12px", "lineHeight": 1.55, "color": "#9DB0C6"}),
        ],
    )


app.layout = html.Div(
    style=PAGE,
    children=[
        html.Main(
            style=SHELL,
            children=[
                html.Header(
                    style={"display": "flex", "justifyContent": "space-between", "gap": "28px", "alignItems": "flex-start", "padding": "8px 0 30px", "borderBottom": "1px solid #1B334B"},
                    children=[
                        html.Div(children=[
                            html.Div("serverside-pivot / Dash", style=PILL),
                            html.H1("A production pivot workspace, embedded in Dash.", style={"fontSize": "clamp(30px, 4vw, 54px)", "lineHeight": 1.05, "letterSpacing": "-.055em", "maxWidth": "790px", "margin": "18px 0 14px", "color": "#F8FAFC"}),
                            html.P("Explore 12,000 revenue records with server-side grouping, virtual scrolling, visual analysis, exports, and durable view state. The pivot below is the real Dash component—not a mockup.", style={"maxWidth": "720px", "fontSize": "15px", "lineHeight": 1.65, "color": "#AABBD0", "margin": 0}),
                        ]),
                        html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(2, minmax(105px, 1fr))", "gap": "10px", "minWidth": "255px"}, children=[
                            html.Div([html.Strong("V1", style={"display": "block", "fontSize": "20px", "color": "#5EEAD4"}), html.Span("runtime protocol", style={"fontSize": "11px", "color": "#90A6BF"})], style={"padding": "13px", "border": "1px solid #1E3852", "borderRadius": "12px"}),
                            html.Div([html.Strong(f"{DEMO_ROW_COUNT // 1000}k", style={"display": "block", "fontSize": "20px", "color": "#5EEAD4"}), html.Span("demo source rows", style={"fontSize": "11px", "color": "#90A6BF"})], style={"padding": "13px", "border": "1px solid #1E3852", "borderRadius": "12px"}),
                        ]),
                    ],
                ),
                html.Section(
                    style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(210px, 1fr))", "gap": "12px", "padding": "28px 0"},
                    children=[
                        capability("Shape the model", "Drag dimensions between rows, columns, filters, and measures. Formulas and custom dimensions remain part of the saved view.", "01 / LAYOUT"),
                        capability("Move at data scale", "Virtual rows and columns request only the current window. The service can cache, prefetch, and return payload references.", "02 / SCALE"),
                        capability("Analyze visually", "Use the chart workspace, sparklines, conditional formats, totals, pinning, and cell selection to move from signal to explanation.", "03 / ANALYZE"),
                        capability("Bring work forward", "Persist layouts locally, save reports, copy selections, and export a real XLSX workbook on demand.", "04 / DELIVER"),
                    ],
                ),
                html.Section(
                    style={"border": "1px solid #1E3852", "borderRadius": "20px", "overflow": "hidden", "background": "#0B1726", "boxShadow": "0 28px 80px rgba(0,0,0,.30)"},
                    children=[
                        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "gap": "12px", "padding": "15px 18px", "borderBottom": "1px solid #1E3852", "background": "rgba(13, 28, 47, .82)"}, children=[
                            html.Div([html.Div("LIVE WORKSPACE", style={"fontFamily": "DM Mono, monospace", "fontSize": "10px", "letterSpacing": ".1em", "color": "#5EEAD4", "fontWeight": 700}), html.Div("Try the sidebar, toolbar, totals, chart launcher, and export menu.", style={"marginTop": "4px", "fontSize": "12px", "color": "#9DB0C6"})]),
                            html.Div("Dash callback adapter", style=PILL),
                        ]),
                        DashTanstackPivot(**workspace_config("dash")),
                    ],
                ),
                html.Section(
                    style={"display": "grid", "gridTemplateColumns": "1.1fr .9fr", "gap": "18px", "marginTop": "28px"},
                    children=[
                        html.Div([html.Div("Why this host is useful", style={"fontSize": "14px", "fontWeight": 800, "marginBottom": "8px"}), html.P("Dash applications keep their familiar Python callback model. The component translates those callback events to the same runtime semantics used by the direct HTTP hosts, so a view can be designed once and carried across integrations.", style={"fontSize": "13px", "lineHeight": 1.65, "color": "#9DB0C6", "margin": 0})], style={"padding": "20px", "border": "1px solid #1E3852", "borderRadius": "16px"}),
                        html.Div([html.Div("Publishing checklist", style={"fontSize": "14px", "fontWeight": 800, "marginBottom": "8px"}), html.Div("✓ Type-safe public props  ·  ✓ Server-side viewport requests  ·  ✓ Optional charts and XLSX  ·  ✓ Browser profiling", style={"fontSize": "12px", "lineHeight": 1.8, "color": "#9DB0C6"})], style={"padding": "20px", "border": "1px solid #1E3852", "borderRadius": "16px"}),
                    ],
                ),
            ],
        )
    ],
)

register_pivot_app(app, adapter_getter=get_demo_adapter, pivot_id="flagship-dash-pivot")


if __name__ == "__main__":
    app.run(debug=False, use_reloader=False, port=8050)
