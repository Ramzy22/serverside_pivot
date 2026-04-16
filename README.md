# serverside-pivot

Build a real server-side pivot table in Dash in about 10 lines of Python.

`serverside-pivot` is built for analytical apps that need fast aggregations, large datasets, and a serious pivot UI without building a separate frontend. It is designed to handle millions of rows with sub-second aggregations, while keeping the interaction model people expect: drag-and-drop fields, row and column grouping, totals, formulas, sorting, filtering, formatting, and virtual scrolling.

Strong point : It works with `pandas`, `polars`, `pyarrow`, and any SQL backends through Ibis, so the same component can start on a local dataframe and scale to a production database without changing the user experience.

On the Dash side, one runtime callback handles data windows, filter option requests, chart requests, and drill-through.

## Install

```bash
pip install serverside-pivot
```

For Redis-backed caching:

```bash
pip install "serverside-pivot[redis]"
```

## Quick Start

Run the included example directly:

```bash
python example_app.py
```

Then open http://127.0.0.1:8050/ in your browser.

Or copy this minimal snippet:

```python
import pandas as pd
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

df = pd.DataFrame({
    "region":  ["North", "South", "East", "West"],
    "quarter": ["Q1",    "Q1",    "Q2",   "Q2"],
    "sales":   [100,     200,     150,    180],
    "revenue": [1000,    2000,    1500,   1800],
})

adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(df, "sales")

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="pivot",
        table="sales",
        serverSide=True,
        rowFields=["region"],
        colFields=["quarter"],
        valConfigs=[
            {"field": "sales",   "agg": "sum", "format": "fixed:0", "label": "Sales"},
            {"field": "revenue", "agg": "sum", "format": "fixed:0", "label": "Revenue"},
        ],
        availableFieldList=["region", "quarter", "sales", "revenue"],
        filters={}, sorting=[], expanded={},
        showRowTotals=True, showColTotals=True,
        defaultTheme="flash",
        data=[],
        style={"height": "400px", "width": "100%"},
    )
])
register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot")

if __name__ == "__main__":
    app.run(debug=True)
```

The same `adapter.load_data(...)` call also accepts `polars`, `pyarrow`, and database-backed Ibis sources.

`register_pivot_app(...)` wires the transport callback, drill-through endpoint, and sort options in one call.

## Why Use It

- Built for Dash, so it fits naturally into a Python application.
- One API for local dataframes and database-backed data.
- Handles large tables with server-side transport and windowing.
- Includes the features people actually expect from a pivot UI, not just a demo grid.
- Lets you stay in Python instead of splitting the project across Python and React.

## Examples

- **Quick start (root):** [example_app.py](example_app.py) — 10,000 rows, sparkline, infinite scroll, flash theme. Run with `python example_app.py`.
- Basic example: [examples/example_dash_basic.py](examples/example_dash_basic.py)
- Hierarchical example: [examples/example_dash_hierarchical.py](examples/example_dash_hierarchical.py)
- Multi-instance example: [examples/example_dash_sql_multi_instance.py](examples/example_dash_sql_multi_instance.py)

## License

MIT
