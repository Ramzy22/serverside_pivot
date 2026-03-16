"""
example_dash_basic.py
---------------------
Single-instance DataFrame quickstart for dash-tanstack-pivot.

Run:
    python examples/example_dash_basic.py
Then open http://127.0.0.1:8050 in your browser.
"""

import pyarrow as pa
from dash import Dash, html

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import (
    PivotRuntimeService,
    SessionRequestGate,
    register_dash_pivot_transport_callback,
)

# ---------------------------------------------------------------------------
# 1. Build a small in-memory dataset
# ---------------------------------------------------------------------------
TABLE_NAME = "sales"
DATA = pa.Table.from_pydict(
    {
        "region": ["North", "North", "South", "South", "East", "East"],
        "product": ["Laptop", "Phone", "Laptop", "Phone", "Laptop", "Phone"],
        "year": [2023, 2023, 2023, 2023, 2023, 2023],
        "sales": [120_000, 80_000, 95_000, 60_000, 110_000, 75_000],
        "units": [400, 800, 300, 600, 350, 750],
    }
)

# ---------------------------------------------------------------------------
# 2. Create the adapter and load data
# ---------------------------------------------------------------------------
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.controller.load_data_from_arrow(TABLE_NAME, DATA)

# ---------------------------------------------------------------------------
# 3. Create the runtime service (shared across callbacks)
# ---------------------------------------------------------------------------
service = PivotRuntimeService(
    adapter_getter=lambda: adapter,
    session_gate=SessionRequestGate(),
)

# ---------------------------------------------------------------------------
# 4. Build the Dash app
# ---------------------------------------------------------------------------
app = Dash(__name__)

app.layout = html.Div(
    [
        html.H2("Basic Pivot Example"),
        DashTanstackPivot(
            id="pivot-basic",
            table=TABLE_NAME,
            rowFields=["region", "product"],
            valConfigs=[
                {"field": "sales", "agg": "sum", "alias": "Total Sales"},
                {"field": "units", "agg": "sum", "alias": "Total Units"},
            ],
            showColTotals=True,
            showRowTotals=True,
        ),
    ],
    style={"padding": "20px"},
)

# ---------------------------------------------------------------------------
# 5. Register the transport callback (wires viewport -> data)
# ---------------------------------------------------------------------------
register_dash_pivot_transport_callback(app, lambda: service, pivot_id="pivot-basic")

if __name__ == "__main__":
    app.run(debug=True)
