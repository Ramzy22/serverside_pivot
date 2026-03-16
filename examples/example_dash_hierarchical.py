"""
example_dash_hierarchical.py
-----------------------------
Multi-level hierarchical pivot example for serverside-pivot.

Demonstrates:
- Three-level row hierarchy (region -> country -> city)
- Column pivoting by year
- Expand/collapse of nested rows
- Row and column totals

Run:
    python examples/example_dash_hierarchical.py
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
# 1. Build a hierarchical dataset
# ---------------------------------------------------------------------------
TABLE_NAME = "geo_sales"
DATA = pa.Table.from_pydict(
    {
        "region": ["AMER", "AMER", "AMER", "AMER", "EMEA", "EMEA", "EMEA", "EMEA"],
        "country": ["USA", "USA", "Canada", "Canada", "UK", "UK", "Germany", "Germany"],
        "city": ["NYC", "LA", "Toronto", "Vancouver", "London", "Manchester", "Berlin", "Munich"],
        "year": [2023, 2024, 2023, 2024, 2023, 2024, 2023, 2024],
        "revenue": [500_000, 550_000, 300_000, 320_000, 400_000, 430_000, 350_000, 380_000],
        "cost": [200_000, 210_000, 120_000, 130_000, 160_000, 170_000, 140_000, 150_000],
    }
)

# ---------------------------------------------------------------------------
# 2. Create the adapter and load data
# ---------------------------------------------------------------------------
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.controller.load_data_from_arrow(TABLE_NAME, DATA)

# ---------------------------------------------------------------------------
# 3. Create the runtime service
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
        html.H2("Hierarchical Pivot Example"),
        html.P(
            "Click the expand arrow on any row to drill into the hierarchy. "
            "Columns are pivoted by year."
        ),
        DashTanstackPivot(
            id="pivot-hierarchical",
            table=TABLE_NAME,
            # Three-level row hierarchy
            rowFields=["region", "country", "city"],
            # Pivot columns by year
            colFields=["year"],
            valConfigs=[
                {"field": "revenue", "agg": "sum", "alias": "Revenue"},
                {"field": "cost", "agg": "sum", "alias": "Cost"},
            ],
            showColTotals=True,
            showRowTotals=True,
            grandTotalPosition="bottom",
        ),
    ],
    style={"padding": "20px"},
)

# ---------------------------------------------------------------------------
# 5. Register the transport callback
# ---------------------------------------------------------------------------
register_dash_pivot_transport_callback(
    app, lambda: service, pivot_id="pivot-hierarchical"
)

if __name__ == "__main__":
    app.run(debug=True)
