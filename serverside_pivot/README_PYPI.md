# ⚡ serverside-pivot

### **Turnkey Server-Side Pivot Table & Analytics Engine for Python, Dash & Streamlit**
#### *Universal SQL & DataFrame connectivity via DuckDB, PyArrow & Ibis. Billion-row hierarchical pivots with bounded virtualized responses.*

[![PyPI version](https://img.shields.io/pypi/v/serverside-pivot.svg?color=3775A9)](https://pypi.org/project/serverside-pivot/)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![DuckDB](https://img.shields.io/badge/Engine-DuckDB%20%7C%20PyArrow-yellow)](https://duckdb.org/)
[![Ibis](https://img.shields.io/badge/Pushdown-Ibis-orange)](https://ibis-project.org/)

`serverside-pivot` is a low-code, server-side analytical pivot table and 2D virtualized data grid for **Python**, **Plotly Dash**, and **Streamlit**, backed by `pivot_engine` (**DuckDB**, **PyArrow**, and **Ibis**).

> **Looking for the standalone React / TypeScript frontend package?**  
> Install [`serverside_pivot` on npm](https://www.npmjs.com/package/serverside_pivot) (`npm install serverside_pivot`) for React 18/19 and DuckDB-WASM client-side execution.

---

## 📦 Installation

```bash
pip install serverside-pivot
```

Optional extras:

```bash
# Streamlit component support
pip install "serverside-pivot[streamlit]"

# FastAPI V1 runtime HTTP server (for standalone React HttpPivotTransport)
pip install "serverside-pivot[api]"

# Redis caching / distributed export job coordination
pip install "serverside-pivot[redis]"
```

---

## ⚡ Quickstart

### 1. Plotly Dash (`DashTanstackPivot`)

```python
import dash
import pandas as pd
from dash import html
from pivot_engine import create_tanstack_adapter, register_pivot_app
from serverside_pivot import DashTanstackPivot

df = pd.DataFrame({
    "region": ["North America", "North America", "Europe", "Europe"],
    "country": ["USA", "Canada", "Germany", "France"],
    "year": ["2024", "2024", "2025", "2025"],
    "revenue": [150000, 90000, 130000, 110000],
    "units": [120, 75, 105, 88],
})

adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(df, "sales")

app = dash.Dash(__name__)
app.layout = html.Div(
    DashTanstackPivot(
        id="pivot-grid",
        table="sales",
        rowFields=["region", "country"],
        colFields=["year"],
        valConfigs=[
            {"field": "revenue", "agg": "sum", "label": "Revenue"},
            {"field": "units", "agg": "sum", "label": "Units"},
        ],
        style={"height": "720px", "width": "100%"},
    )
)

register_pivot_app(app, adapter_getter=lambda: adapter)

if __name__ == "__main__":
    app.run(debug=False)
```

### 2. Streamlit (`streamlit_pivot`)

```python
import pandas as pd
import streamlit as st
from pivot_engine import create_tanstack_adapter
from streamlit_pivot import st_serverside_pivot

st.set_page_config(layout="wide")
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(pd.read_parquet("sales.parquet"), "sales")

st_serverside_pivot(
    adapter=adapter,
    table="sales",
    row_fields=["region", "country"],
    col_fields=["year"],
    val_configs=[{"field": "revenue", "agg": "sum", "label": "Revenue"}],
    key="sales-pivot",
)
```

### 3. Cloud Data Warehouses via Ibis Pushdown

Point `backend_uri` to DuckDB files, MotherDuck, PostgreSQL, BigQuery, Snowflake, ClickHouse, Databricks, or Trino so aggregations, sorting, filtering, and window calculations execute inside your warehouse and only bounded virtual slices travel to the browser:

```python
from pivot_engine import create_tanstack_adapter

adapter = create_tanstack_adapter(backend_uri="duckdb:///analytics.db")
# Or any Ibis-supported connection URI (e.g. postgresql://..., snowflake://..., bigquery://...)
```

---

## ✨ Key Python & Engine Capabilities

- **Server-Side Pushdown (`pivot_engine`)**: Hierarchical rollup queries, level-based batching, grand totals, subtotals, multi-column sorting, set/condition filters, and top-N filtering executed in DuckDB/PyArrow/Ibis.
- **2D Virtualization**: Streams bounded row/column viewports over HTTP/Dash callbacks so multi-million-row tables keep browser RAM low.
- **Calculated Measures & Window Functions**: Calculated formulas and window expressions (`percent_of_grand_total`, running totals, diffs).
- **Streaming Async Exports**: Server-side CSV and formatted XLSX workbook exports with cancellation and job progress tracking.
- **Multi-Host UI**: Same workspace capabilities across Dash (`serverside_pivot`), Streamlit (`streamlit_pivot`), and standalone React (`serverside_pivot` on npm via FastAPI V1 runtime).
- **Read-Only Analytical Scope**: Cell editing and edit transactions are not a supported public capability; legacy editing props are ignored no-ops.

---

## 📚 Links

- **GitHub Repository & Full Documentation**: https://github.com/Ramzy22/serverside_pivot
- **React / TypeScript Package (npm)**: https://www.npmjs.com/package/serverside_pivot
- **Issue Tracker**: https://github.com/Ramzy22/serverside_pivot/issues
