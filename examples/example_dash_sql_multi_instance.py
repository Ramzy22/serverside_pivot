"""
example_dash_sql_multi_instance.py
------------------------------------
Two-instance pivot example proving multi-instance isolation in a single Dash layout.

Each instance has distinct:
- component `id`          : "pivot-sales" vs "pivot-inventory"
- `table`                 : "sales_data" vs "inventory_data"
- `session_id`            : generated per page-load by the frontend automatically
- `client_instance`       : generated per mount by the frontend automatically

The SessionRequestGate tracks each (session_id, client_instance) composite key
independently so that interleaved requests from the two grids never cross-contaminate.

Run:
    python examples/example_dash_sql_multi_instance.py
Then open http://127.0.0.1:8050 in your browser.
"""

import pyarrow as pa
from dash import Dash, html

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import (
    DashPivotInstanceConfig,
    PivotRuntimeService,
    SessionRequestGate,
    register_dash_callbacks_for_instances,
)

# ---------------------------------------------------------------------------
# 1. Build two independent in-memory tables (simulates two SQL tables)
# ---------------------------------------------------------------------------
SALES_TABLE = "sales_data"
INVENTORY_TABLE = "inventory_data"

sales_data = pa.Table.from_pydict(
    {
        "region": ["North", "North", "South", "South", "East"],
        "product": ["Laptop", "Phone", "Laptop", "Phone", "Tablet"],
        "quarter": ["Q1", "Q2", "Q1", "Q2", "Q1"],
        "revenue": [120_000, 95_000, 80_000, 60_000, 55_000],
        "units_sold": [400, 350, 280, 220, 180],
    }
)

inventory_data = pa.Table.from_pydict(
    {
        "warehouse": ["WH-A", "WH-A", "WH-B", "WH-B", "WH-C"],
        "sku": ["SKU-001", "SKU-002", "SKU-001", "SKU-003", "SKU-002"],
        "category": ["Electronics", "Electronics", "Electronics", "Accessories", "Electronics"],
        "stock_qty": [500, 300, 450, 200, 250],
        "reorder_point": [100, 80, 100, 50, 80],
    }
)

# ---------------------------------------------------------------------------
# 2. Create the adapter and load both tables into the same in-memory DuckDB
# ---------------------------------------------------------------------------
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.controller.load_data_from_arrow(SALES_TABLE, sales_data)
adapter.controller.load_data_from_arrow(INVENTORY_TABLE, inventory_data)

# ---------------------------------------------------------------------------
# 3. Create a single shared runtime service
#    Both instances share the same adapter; each request is scoped by table=
# ---------------------------------------------------------------------------
service = PivotRuntimeService(
    adapter_getter=lambda: adapter,
    session_gate=SessionRequestGate(),
)

# ---------------------------------------------------------------------------
# 4. Build the Dash app with two DashTanstackPivot instances
# ---------------------------------------------------------------------------
app = Dash(__name__)

app.layout = html.Div(
    [
        html.H2("Multi-instance Pivot: Two Independent Grids"),
        html.P(
            "Both pivot tables run in the same Dash app with full state isolation. "
            "Changing the filter on the Sales grid does not affect the Inventory grid."
        ),
        html.H3("Sales Dashboard"),
        DashTanstackPivot(
            id="pivot-sales",
            table=SALES_TABLE,
            rowFields=["region", "product"],
            colFields=["quarter"],
            valConfigs=[
                {"field": "revenue", "agg": "sum", "alias": "Revenue"},
                {"field": "units_sold", "agg": "sum", "alias": "Units Sold"},
            ],
            showColTotals=True,
            showRowTotals=True,
        ),
        html.Hr(),
        html.H3("Inventory Dashboard"),
        DashTanstackPivot(
            id="pivot-inventory",
            table=INVENTORY_TABLE,
            rowFields=["warehouse", "sku"],
            colFields=["category"],
            valConfigs=[
                {"field": "stock_qty", "agg": "sum", "alias": "Stock Qty"},
                {"field": "reorder_point", "agg": "min", "alias": "Reorder Point"},
            ],
            showColTotals=True,
            showRowTotals=False,
        ),
    ],
    style={"padding": "20px"},
)

# ---------------------------------------------------------------------------
# 5. Register callbacks for both instances
#    Each DashPivotInstanceConfig binds one pivot_id -> one transport callback
# ---------------------------------------------------------------------------
instances = [
    DashPivotInstanceConfig(pivot_id="pivot-sales"),
    DashPivotInstanceConfig(pivot_id="pivot-inventory"),
]

register_dash_callbacks_for_instances(app, lambda: service, instances)

if __name__ == "__main__":
    app.run(debug=True)
