"""
app.py - Enterprise Grade Server-Side Pivot Table
Integrates DashTanstackPivot with ScalablePivotEngine.
"""
import os
import sys

# Add the parent directory's pivot_engine folder to sys.path to ensure we can import the package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pivot_engine')))

import pyarrow as pa
from dash import Dash, Input, Output, State, dcc, html, no_update

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

_DEBUG_OUTPUT = os.environ.get("PIVOT_DEBUG_OUTPUT", "1").lower() in {"1", "true", "yes"}


# --- 2. Data Loading (Simulation) ---
def load_initial_data(adapter):
    if _DEBUG_OUTPUT:
        print("Generating simulation data (2M rows)...")
    # Generate 2M rows for stress testing
    rows = 2000000 
    
    # Create more diverse date range for column virtualization test
    dates = [f"2023-{m:02d}-{d:02d}" for m in range(1, 13) for d in range(1, 29, 2)] # ~150 unique dates
    
    data_source = {
        "region": (["North", "South", "East", "West"] * (rows // 4)),
        "country": (["USA", "Canada", "Brazil", "UK", "China", "Japan", "Germany", "France"] * (rows // 8)),
        "product": (["Laptop", "Phone", "Tablet", "Monitor", "Headphones"] * (rows // 5)),
        "sales": [x % 1000 for x in range(rows)],
        "cost": [x % 800 for x in range(rows)],
        "date": (dates * (rows // len(dates)) + dates[:rows % len(dates)])
    }
    table = pa.Table.from_pydict(data_source)
    
    # Load into the engine
    adapter.controller.load_data_from_arrow("sales_data", table)
    if _DEBUG_OUTPUT:
        print(f"Data loaded into Pivot Engine: {rows} rows.")
    
    # Pre-materialize hierarchy for the default view to ensure virtual scroll works immediately
    from pivot_engine.types.pivot_spec import PivotSpec, Measure
    default_spec = PivotSpec(
        table="sales_data",
        rows=["region", "country"],
        measures=[
            Measure(field="sales", agg="sum", alias="sales_sum"),
            Measure(field="cost", agg="sum", alias="cost_sum")
        ]
    )
    if _DEBUG_OUTPUT:
        print("Pre-materializing default hierarchy...")
    adapter.controller.materialized_hierarchy_manager.create_materialized_hierarchy(default_spec)
    if _DEBUG_OUTPUT:
        print("Hierarchy materialized.")
    # Example: load Polars directly (no manual Arrow conversion required)
    # import polars as pl
    # df = pl.read_parquet("sales_data.parquet")
    # adapter.load_data(df, "sales_data")


_adapter = None


def get_adapter():
    global _adapter
    if _adapter is None:
        _adapter = create_tanstack_adapter(backend_uri=":memory:")
        load_initial_data(_adapter)
    return _adapter


# --- 3. Dash App ---
app = Dash(__name__)


app.layout = html.Div([
    dcc.Store(id="saved-view-store"),
    html.Div(
        [
            html.Button("Restore Saved View", id="restore-view-btn"),
            html.Span(
                "Save inside the pivot via the top bar 'Save View' button.",
                style={"marginLeft": "10px", "fontSize": "12px", "color": "#555"},
            ),
        ],
        style={"padding": "8px 16px"},
    ),
    html.Div(
        DashTanstackPivot(
            id="pivot-grid",
            style={"height": "800px", "width": "100%"},
            table="sales_data",
            # Enable Server Side Mode
            serverSide=True,
            # Initial Configuration
            rowFields=["region", "country"],
            colFields=[],
            valConfigs=[{"field": "sales", "agg": "sum"}, {"field": "cost", "agg": "sum"}],
            filters={},
            sorting=[],
            expanded={},
            showRowTotals=True,
            showColTotals=True,
            # Pass ALL available fields as columns definition for the sidebar
            columns=[
                {"id": "region"}, {"id": "country"}, {"id": "product"}, 
                {"id": "sales"}, {"id": "cost"}, {"id": "date"}
            ],
            availableFieldList=["region", "country", "product", "sales", "cost", "date"],
            # Initial Data (Empty, will fetch on load)
            data=[],
            rowCount=0,
            filterOptions={},
            validationRules={
                "sales_sum": [{"type": "numeric"}, {"type": "min", "value": 0}],
                "cost_sum": [{"type": "numeric"}, {"type": "min", "value": 0}]
            },
            # Example: preload a previously saved full view snapshot.
            # viewState={
            #     "version": 1,
            #     "table": "sales_data",
            #     "state": {
            #         "rowFields": ["cost", "region", "country"],
            #         "colFields": ["date"],
            #         "valConfigs": [{"field": "sales", "agg": "sum"}],
            #         "filters": {},
            #         "sorting": [],
            #         "expanded": {},
            #         "sidebarOpen": False,
            #         "colorScaleMode": "table",
            #     },
            # },
        ),
        style={'padding': '0 16px'}
    )
])

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

if __name__ == "__main__":
    app.run(debug=True, port=8050)


