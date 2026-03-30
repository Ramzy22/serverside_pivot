# serverside-pivot

`serverside-pivot` is a pivot table for Dash.

It gives you a real interactive pivot UI with row grouping, column grouping, totals, formulas, sorting, filtering, formatting, and virtual scrolling, without writing frontend code.

The main strength is that you can use the same component with:
- `pandas`
- `polars`
- `pyarrow`
- database-backed tables through Ibis

So you can start small in memory, then point the same app at larger SQL data later.

## Install

```bash
pip install serverside-pivot
```

For Redis-backed caching:

```bash
pip install "serverside-pivot[redis]"
```

## Quick Start

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

## Why Use It

- Built for Dash, so the pivot lives naturally inside a Python app.
- Works with in-memory data and backend data through the same API.
- Handles large datasets with server-side transport and virtual scrolling.
- Supports practical pivot features, not just a demo table.

## Examples

- Basic example: [examples/example_dash_basic.py](examples/example_dash_basic.py)
- Hierarchical example: [examples/example_dash_hierarchical.py](examples/example_dash_hierarchical.py)
- Multi-instance example: [examples/example_dash_sql_multi_instance.py](examples/example_dash_sql_multi_instance.py)

## License

MIT
