import numpy as np
import pandas as pd
from dash import Dash, html

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

np.random.seed(42)
n = 10000

df = pd.DataFrame({
    "region":   np.random.choice(["North", "South", "East", "West", "Central", "Pacific", "Atlantic", "Midwest"], n),
    "segment":  np.random.choice(["Enterprise", "SMB", "Consumer", "Government", "Education"], n),
    "channel":  np.random.choice(["Direct", "Online", "Reseller", "Partner", "Retail"], n),
    "rep":      np.random.choice([f"Rep_{i:03d}" for i in range(1, 51)], n),
    "product":  np.random.choice(["Widgets", "Gadgets", "Gizmos", "Doohickeys", "Thingamajigs", "Contraptions", "Doodads", "Whatchamacallits"], n),
    "quarter":  np.random.choice(["Q1", "Q2", "Q3", "Q4"], n),
    "sales":    np.random.randint(1000,  50000,  n).astype(float),
    "revenue":  np.random.randint(5000,  200000, n).astype(float),
    "cost":     np.random.randint(500,   30000,  n).astype(float),
    "profit":   np.random.randint(100,   50000,  n).astype(float),
    "quantity": np.random.randint(1,     200,    n).astype(float),
})

adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(df, "sales")

FIELDS = ["region", "segment", "channel", "rep", "product", "quarter",
          "sales", "revenue", "cost", "profit", "quantity"]

app = Dash(
    __name__,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap"
    ],
)

app.layout = html.Div(
    style={
        "fontFamily": "'Inter', ui-sans-serif, system-ui, sans-serif",
        "background": "#0A0D12",
        "minHeight": "100vh",
        "margin": 0,
        "padding": "24px",
        "boxSizing": "border-box",
        "color": "#F3F4F6",
    },
    children=[
        html.Div(
            style={
                "background": "#111827",
                "border": "1px solid #1F2937",
                "borderRadius": "12px",
                "boxShadow": "0 20px 25px -5px rgba(0, 0, 0, 0.2), 0 10px 10px -5px rgba(0, 0, 0, 0.1)",
                "padding": "20px",
            },
            children=[
                html.Div(
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "space-between",
                        "marginBottom": "18px",
                    },
                    children=[
                        html.Div("Sales Performance", style={
                            "fontSize": "18px",
                            "fontWeight": "600",
                            "color": "#F9FAFB",
                        }),
                        html.Div("10,000 rows · server-side · infinite scroll", style={
                            "fontSize": "11px",
                            "fontWeight": "500",
                            "color": "#9CA3AF",
                            "background": "#1F2937",
                            "border": "1px solid #374151",
                            "borderRadius": "6px",
                            "padding": "4px 10px",
                        }),
                    ],
                ),
                DashTanstackPivot(
                    id="pivot",
                    table="sales",
                    serverSide=True,
                    rowFields=["region", "segment", "channel", "rep"],
                    colFields=["quarter"],
                    valConfigs=[
                        {
                            "field": "sales",
                            "agg": "sum",
                            "format": "fixed:0",
                            "label": "Sales",
                            "sparkline": {
                                "type": "area",
                                "header": "Sales Trend",
                                "showCurrentValue": True,
                                "showDelta": True,
                                "hideColumns": False,
                                "placement": "before",
                            },
                        },
                        {"field": "revenue",  "agg": "sum", "format": "fixed:0", "label": "Revenue"},
                        {"field": "cost",     "agg": "sum", "format": "fixed:0", "label": "Cost"},
                        {"field": "profit",   "agg": "sum", "format": "fixed:0", "label": "Profit"},
                        {"field": "quantity", "agg": "sum", "format": "fixed:0", "label": "Qty"},
                    ],
                    availableFieldList=FIELDS,
                    filters={},
                    sorting=[],
                    expanded={},
                    showRowTotals=True,
                    showColTotals=True,
                    defaultTheme="blooomberg",
                    data=[],
                    style={"height": "calc(100vh - 120px)", "width": "100%"},
                ),
            ],
        )
    ],
)

register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot")

if __name__ == "__main__":
    app.run(debug=True, port=8050)
