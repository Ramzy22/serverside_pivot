"""
app.py - Enterprise Grade Server-Side Pivot Table
Integrates DashTanstackPivot (serverside-pivot) with pivot-engine backend.
"""
import os
import sys

# Ensure the local pivot_engine package takes priority over any stale installs.
_PE_DIR = os.path.join(os.path.dirname(__file__), os.pardir,
                       "dash_tanstack_pivot", "pivot_engine")
if os.path.isdir(_PE_DIR) and _PE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(_PE_DIR))

import pyarrow as pa
from dash import Dash, Input, Output, State, dcc, html, no_update

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

_DEBUG_OUTPUT = os.environ.get("PIVOT_DEBUG_OUTPUT", "1").lower() in {"1", "true", "yes"}


# --- 2. Data Loading (Simulation) ---
def load_initial_data(adapter):
    if _DEBUG_OUTPUT:
        print("Generating simulation data (2M rows)...")
    rows = 2000000

    # Create more diverse date range for column virtualization test
    dates = [f"2023-{m:02d}-{d:02d}" for m in range(1, 13) for d in range(1, 29, 2)]

    data_source = {
        "region": (["North", "South", "East", "West"] * (rows // 4)),
        "country": (["USA", "Canada", "Brazil", "UK", "China", "Japan", "Germany", "France"] * (rows // 8)),
        "product": (["Laptop", "Phone", "Tablet", "Monitor", "Headphones"] * (rows // 5)),
        "sales": [x % 1000 for x in range(rows)],
        "cost": [x % 800 for x in range(rows)],
        "date": (dates * (rows // len(dates)) + dates[:rows % len(dates)]),
    }
    table = pa.Table.from_pydict(data_source)

    adapter.controller.load_data_from_arrow("sales_data", table)
    if _DEBUG_OUTPUT:
        print(f"Data loaded into Pivot Engine: {rows} rows.")

    from pivot_engine.types.pivot_spec import PivotSpec, Measure
    default_spec = PivotSpec(
        table="sales_data",
        rows=["region", "country"],
        measures=[
            Measure(field="sales", agg="sum", alias="sales_sum"),
            Measure(field="cost", agg="sum", alias="cost_sum"),
        ],
    )
    if _DEBUG_OUTPUT:
        print("Pre-materializing default hierarchy...")
    adapter.controller.materialized_hierarchy_manager.create_materialized_hierarchy(default_spec)
    if _DEBUG_OUTPUT:
        print("Hierarchy materialized.")

    tenor_table = pa.Table.from_pydict(
        {
            "book": ["FI", "FI", "FI", "FI", "FI", "FX", "FX", "FX", "FX", "FX"],
            "tenor": ["1M", "2W", "1D", "6Y", "3M", "1M", "2W", "1D", "6Y", "10Y"],
            "rate": [0.0310, 0.0280, 0.0250, 0.0450, 0.0340, 0.0290, 0.0270, 0.0240, 0.0430, 0.0510],
            "notional": [120.0, 80.0, 150.0, 60.0, 100.0, 90.0, 110.0, 130.0, 70.0, 50.0],
        }
    )
    adapter.controller.load_data_from_arrow("tenor_data", tenor_table)

    curve_pillar_table = pa.Table.from_pydict(
        {
            "desk": ["Rates", "Rates", "Rates", "Rates", "Rates", "Credit", "Credit", "Credit", "Credit", "Credit"],
            "Curve Pillar": ["1M", "2W", "1D", "6Y", "3M", "1M", "2W", "1D", "6Y", "10Y"],
            "__sortkey__Curve Pillar": [30, 14, 1, 2190, 90, 30, 14, 1, 2190, 3650],
            "pv01": [0.12, 0.05, 0.01, 1.80, 0.35, 0.10, 0.04, 0.02, 1.60, 2.10],
            "dv01": [0.08, 0.03, 0.005, 1.20, 0.22, 0.07, 0.025, 0.01, 1.05, 1.40],
        }
    )
    adapter.controller.load_data_from_arrow("curve_data", curve_pillar_table)


_adapter = None


def get_adapter():
    global _adapter
    if _adapter is None:
        _adapter = create_tanstack_adapter(backend_uri=":memory:")
        load_initial_data(_adapter)
    return _adapter


# --- 3. Dash App ---
app = Dash(
    __name__,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
    ],
)

_PAGE_STYLE = {
    "fontFamily": "'Inter', ui-sans-serif, system-ui, sans-serif",
    "background": "#FAFAFA",
    "minHeight": "100vh",
    "margin": 0,
    "padding": 0,
}

_HEADER_STYLE = {
    "height": "56px",
    "background": "#ffffff",
    "borderBottom": "1px solid #E5E7EB",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
    "padding": "0 24px",
    "position": "sticky",
    "top": 0,
    "zIndex": 10,
}

_LOGO_BADGE_STYLE = {
    "width": "28px",
    "height": "28px",
    "background": "#4F46E5",
    "borderRadius": "8px",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "center",
    "marginRight": "8px",
    "fontSize": "14px",
    "color": "#fff",
    "fontWeight": "700",
    "flexShrink": "0",
    "boxShadow": "0 1px 3px rgba(79,70,229,0.3)",
}

_SECTION_HEADER_STYLE = {
    "fontSize": "11px",
    "fontWeight": "700",
    "letterSpacing": "0.06em",
    "textTransform": "uppercase",
    "color": "#94A3B8",
    "padding": "0 0 10px 0",
}

_RESTORE_BTN_STYLE = {
    "display": "inline-flex",
    "alignItems": "center",
    "gap": "6px",
    "fontSize": "13px",
    "fontWeight": "500",
    "color": "#374151",
    "background": "#ffffff",
    "border": "1px solid #E5E7EB",
    "borderRadius": "10px",
    "padding": "6px 12px",
    "cursor": "pointer",
    "boxShadow": "0 1px 2px rgba(15,23,42,0.05)",
}

_SECTION_CARD_STYLE = {"padding": "0"}

_MAIN_CONTENT_STYLE = {
    "padding": "24px",
    "display": "flex",
    "flexDirection": "column",
    "gap": "20px",
    "maxWidth": "1680px",
    "margin": "0 auto",
}

_PANEL_STYLE = {
    "background": "#FFFFFF",
    "border": "1px solid #E5E7EB",
    "borderRadius": "18px",
    "boxShadow": "0 10px 24px rgba(15,23,42,0.06), 0 2px 8px rgba(15,23,42,0.04)",
    "padding": "18px",
}

_PANEL_HEADER_STYLE = {
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
    "gap": "12px",
    "marginBottom": "14px",
}

_PANEL_TITLE_STYLE = {
    "fontSize": "18px",
    "fontWeight": "600",
    "color": "#111827",
}

_PANEL_META_STYLE = {
    "fontSize": "12px",
    "fontWeight": "500",
    "color": "#64748B",
    "background": "#F8FAFC",
    "border": "1px solid #E5E7EB",
    "borderRadius": "999px",
    "padding": "6px 10px",
}


def build_panel(section_label, title, meta, component):
    return html.Div(
        style=_SECTION_CARD_STYLE,
        children=[
            html.Div(section_label, style=_SECTION_HEADER_STYLE),
            html.Div(
                style=_PANEL_STYLE,
                children=[
                    html.Div(
                        style=_PANEL_HEADER_STYLE,
                        children=[
                            html.Div(title, style=_PANEL_TITLE_STYLE),
                            html.Div(meta, style=_PANEL_META_STYLE),
                        ],
                    ),
                    component,
                ],
            ),
        ],
    )


app.layout = html.Div(
    style=_PAGE_STYLE,
    children=[
        dcc.Store(id="saved-view-store"),
        html.Header(
            style=_HEADER_STYLE,
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center"},
                    children=[
                        html.Div("P", style=_LOGO_BADGE_STYLE),
                        html.Span(
                            "Nexus Pivot",
                            style={"fontWeight": "600", "fontSize": "15px",
                                   "color": "#111827", "letterSpacing": "-0.01em"},
                        ),
                    ],
                ),
                html.Button(
                    "Restore Saved View",
                    id="restore-view-btn",
                    style=_RESTORE_BTN_STYLE,
                ),
            ],
        ),
        html.Main(
            style=_MAIN_CONTENT_STYLE,
            children=[
                build_panel(
                    "Primary Pivot",
                    "Pivot Analysis",
                    "Server-side sales dataset",
                    DashTanstackPivot(
                        id="pivot-grid",
                        style={"height": "800px", "width": "100%"},
                        table="sales_data",
                        serverSide=True,
                        rowFields=["region", "country"],
                        colFields=[],
                        valConfigs=[{"field": "sales", "agg": "sum"}, {"field": "cost", "agg": "sum"}],
                        filters={},
                        sorting=[],
                        expanded={},
                        showRowTotals=True,
                        showColTotals=True,
                        columns=[
                            {"id": "region"}, {"id": "country"}, {"id": "product"},
                            {"id": "sales"}, {"id": "cost"}, {"id": "date"},
                        ],
                        availableFieldList=["region", "country", "product", "sales", "cost", "date"],
                        data=[],
                        rowCount=0,
                        filterOptions={},
                        validationRules={
                            "sales_sum": [{"type": "numeric"}, {"type": "min", "value": 0}],
                            "cost_sum": [{"type": "numeric"}, {"type": "min", "value": 0}],
                        },
                    ),
                ),
                build_panel(
                    "Curve Demo",
                    "Curve Pillar",
                    "Custom backend sort key",
                    DashTanstackPivot(
                        id="curve-pivot-grid",
                        style={"height": "420px", "width": "100%"},
                        table="curve_data",
                        serverSide=True,
                        rowFields=["Curve Pillar"],
                        colFields=[],
                        valConfigs=[
                            {"field": "pv01", "agg": "sum", "format": "fixed:4"},
                            {"field": "dv01", "agg": "sum", "format": "fixed:4"},
                        ],
                        sorting=[{"id": "Curve Pillar", "desc": False}],
                        sortOptions={
                            "columnOptions": {
                                "Curve Pillar": {
                                    "sortKeyField": "__sortkey__Curve Pillar",
                                }
                            }
                        },
                        filters={},
                        expanded={},
                        showRowTotals=False,
                        showColTotals=False,
                        columns=[
                            {"id": "desk"},
                            {"id": "Curve Pillar"},
                            {"id": "pv01"},
                            {"id": "dv01"},
                        ],
                        availableFieldList=["desk", "Curve Pillar", "pv01", "dv01"],
                        data=[],
                        rowCount=0,
                        filterOptions={},
                    ),
                ),
                build_panel(
                    "Rates Demo",
                    "Tenor Sort · Weighted Average",
                    "Semantic tenor ordering",
                    DashTanstackPivot(
                        id="tenor-pivot-grid",
                        style={"height": "420px", "width": "100%"},
                        table="tenor_data",
                        serverSide=True,
                        rowFields=["tenor"],
                        colFields=[],
                        valConfigs=[
                            {"field": "notional", "agg": "sum", "format": "fixed:2"},
                            {
                                "field": "rate",
                                "agg": "weighted_avg",
                                "weightField": "notional",
                                "format": "fixed:4",
                            },
                        ],
                        sorting=[{"id": "tenor", "desc": False, "semanticType": "tenor"}],
                        filters={},
                        expanded={},
                        showRowTotals=False,
                        showColTotals=False,
                        columns=[
                            {"id": "book"},
                            {"id": "tenor"},
                            {"id": "rate"},
                            {"id": "notional"},
                        ],
                        availableFieldList=["book", "tenor", "rate", "notional"],
                        data=[],
                        rowCount=0,
                        filterOptions={},
                    ),
                ),
            ],
        ),
    ],
)


# Capture saved view emitted by the component into Dash state (Store).
@app.callback(
    Output("saved-view-store", "data"),
    Input("pivot-grid", "savedView"),
    prevent_initial_call=True,
)
def persist_saved_view(saved_view):
    if not saved_view:
        return no_update
    return saved_view


# Restore the last saved view from Dash Store back into the component.
@app.callback(
    Output("pivot-grid", "viewState"),
    Input("restore-view-btn", "n_clicks"),
    State("saved-view-store", "data"),
    prevent_initial_call=True,
)
def restore_saved_view(_clicks, saved_view):
    if not saved_view:
        return no_update
    return saved_view


# --- 4. Pivot wiring (one line) ---
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="pivot-grid")
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="curve-pivot-grid")
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="tenor-pivot-grid")

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, port=8050)
