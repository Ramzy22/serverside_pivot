# serverside-pivot

Build a real server-side pivot table in Dash in about 10 lines of Python.

`serverside-pivot` is built for analytical apps that need fast aggregations, large datasets, and a serious pivot UI without building a separate frontend. It is designed to handle millions of rows with sub-second aggregations, while keeping the interaction model people expect: drag-and-drop fields, row and column grouping, totals, formulas, sorting, filtering, formatting, and virtual scrolling.

It works with `pandas`, `polars`, `pyarrow`, and SQL backends through Ibis, so the same component can start on a local dataframe and scale to a production database without changing the user experience.

## Install

```bash
pip install serverside-pivot
```

For Redis-backed caching:

```bash
pip install "serverside-pivot[redis]"
```

## 10-line Quick Start

```python
import pandas as pd
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRuntimeService, SessionRequestGate, register_dash_pivot_transport_callback

df = pd.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(df, "sales")
service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

app = Dash(__name__)
app.layout = html.Div([DashTanstackPivot(id="pivot", table="sales", rowFields=["region"], valConfigs=[{"field": "sales", "agg": "sum"}])])
register_dash_pivot_transport_callback(app, lambda: service, pivot_id="pivot")

if __name__ == "__main__":
    app.run(debug=True)
```

The same `adapter.load_data(...)` call also accepts `polars`, `pyarrow`, and database-backed Ibis sources.

## Why Use It

- Built for Dash, so it fits naturally into a Python application.
- One API for local dataframes and database-backed data.
- Handles large tables with server-side transport and windowing.
- Includes the features people actually expect from a pivot UI, not just a demo grid.
- Lets you stay in Python instead of splitting the project across Python and React.

## Examples

- Basic example: [examples/example_dash_basic.py](examples/example_dash_basic.py)
- Hierarchical example: [examples/example_dash_hierarchical.py](examples/example_dash_hierarchical.py)
- Multi-instance example: [examples/example_dash_sql_multi_instance.py](examples/example_dash_sql_multi_instance.py)

## License

MIT
