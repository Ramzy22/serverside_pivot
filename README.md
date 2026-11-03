<div align="center">

# ⚡ serverside-pivot

### **The Turnkey Server-Side Pivot Table Engine for Python, React, Dash & Streamlit.**
#### *Billion-row analytics powered by DuckDB & Ibis. Zero browser memory limits. 100% Open Source.*

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8-blue)](https://www.typescriptlang.org/)
[![DuckDB](https://img.shields.io/badge/Engine-DuckDB%20%7C%20PyArrow-yellow)](https://duckdb.org/)
[![Ibis Project](https://img.shields.io/badge/Federation-Ibis-orange)](https://ibis-project.org/)
[![AG Grid Parity](https://img.shields.io/badge/Certified%20Parity-AG%20Grid%2036.2%20SSRM-green)](docs/flagship-functionality-certification.md)
[![Tested Cases](https://img.shields.io/badge/Browser%20Cases-207%20%2F%20207%20Mapped-brightgreen)](tests/flagship_requirements.json)

[**The Core Problem**](#-the-problem-with-existing-solutions) •
[**Quickstart (Dash / React / Streamlit)**](#-quickstart) •
[**Benchmark vs. Perspective & AG Grid**](#-head-to-head-breakdown) •
[**The $\mathcal{O}(D)$ Engine**](#-the-od-hierarchical-engine-advantage) •
[**Live Demos**](#-live-showcases-revenue-operations)

---

</div>

## 📌 The Problem with Existing Solutions

When developers build analytical dashboards with large enterprise datasets, they hit one of two walls:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       The Analytical Dilemma                                     │
├────────────────────────────────────────┬─────────────────────────────────────────────────────────┤
│ 1. The Client Memory Wall              │ 2. The Commercial "Empty Shell" Trap                    │
│    (FINOS Perspective, TanStack Table) │    (AG Grid Enterprise SSRM)                            │
├────────────────────────────────────────┼─────────────────────────────────────────────────────────┤
│ • Data MUST fit in browser RAM (WASM). │ • Extraordinary frontend, but ZERO backend included.   │
│ • Loading 10M–50M rows freezes the UI   │ • Developer must spend months building SQL generators,  │
│   or crashes the browser tab (OOM).    │   pivot aggregations, and windowing from scratch.       │
│ • Cannot query cloud data warehouses.  │ • Costs $1,000 to $2,500+ / developer / year.           │
└────────────────────────────────────────┴─────────────────────────────────────────────────────────┘
```

### The Solution: `serverside-pivot`

`serverside-pivot` provides the **complete full-stack engine**:
1. **No Client Memory Limit**: Data stays in your warehouse or embedded engine (**DuckDB**, **Snowflake**, **BigQuery**, **ClickHouse**, or **Postgres**). The browser only receives thin, 50-row virtualized slices on demand. Whether you have 1 million, 100 million, or **1 billion rows**, your browser uses less than 80 MB of RAM.
2. **Turnkey Backend Included**: You don't write a single line of SQL or grouping logic. DuckDB and Ibis compile your pivot layout into sub-second vector queries automatically.
3. **Tri-Host Universal Runtime**: The identical 4-stage runtime (`PivotRuntimeRoot`) runs natively inside **Plotly Dash**, **Streamlit** (zero-rerun iframe component), and standalone **React**.

```
[ Snowflake / BigQuery / DuckDB / Parquet / Postgres ]  <-- BILLIONS OF ROWS (Unlimited)
                           │
                           ▼  (Sub-second pushdown aggregation via Ibis)
                 [ serverside-pivot Engine ]
                           │
                           ▼  (Lightweight 50-row virtualized slices)
                 [ Browser Client (Dash / React / Streamlit) ]  <-- USES < 80 MB RAM (Zero Lag)
```

---

## 🥊 Head-to-Head Breakdown

| Dimension | `serverside-pivot` | FINOS Perspective | AG Grid Enterprise 36.2 (SSRM) |
| :--- | :--- | :--- | :--- |
| **Max Dataset Size** | **UNLIMITED (Billion+ rows)**<br>*(Bounded only by your database/disk)* | **~1M–5M rows**<br>*(Hard browser RAM / WASM memory crash)* | **Unlimited**<br>*(...but ONLY if you build the backend)* |
| **Client Memory Footprint** | **Constant (< 80 MB)**<br>*(Virtualized windowing)* | **Grows with data**<br>*(10M rows = 2GB–4GB = Tab Crash)* | **Constant (< 100 MB)**<br>*(Virtualized DOM)* |
| **Backend Query Engine** | **INCLUDED (Turnkey)**<br>*(DuckDB + PyArrow + Ibis)* | **None**<br>*(All data must be shipped to client)* | **NONE (You must build it)**<br>*(You write the SQL/groupby backend)* |
| **License Cost** | **FREE & Open Source (MIT)** | FREE & Open Source (Apache 2.0) | **$1,000 – $2,500+ / dev / year** |
| **Direct Warehouse Pushdown** | **YES** (Snowflake, BigQuery, DuckDB, Postgres) | **NO** (Must extract raw data to Arrow) | Depends on your custom code |
| **Supported Hosts** | **Dash, React, Streamlit** | Web Component, Jupyter | React, Angular, Vue (No Dash/Streamlit) |
| **Hierarchical Queries** | **$\mathcal{O}(D)$ Level-Based Batching** | In-memory tree walk | $\mathcal{O}(N)$ Individual Group Waterfall |
| **Query Cancellation** | **Native `cursor.interrupt()`** | Thread termination | Custom abort controller wiring |
| **Clipboard TSV Fidelity** | **RFC-compliant (retains quotes/CRLF)** | Basic TSV | Fails delimiter round-trip in SSRM |
| **Export Governance** | **Cancellable Async State Machine** | In-browser dump (Crashes tab on large data) | Client/Server custom plugins |

---

## ⚡ Quickstart

### 1. Installation

```bash
pip install serverside-pivot
```

*For distributed multi-worker caching with Redis:*
```bash
pip install "serverside-pivot[redis]"
```

*For standalone React frontend projects:*
```bash
npm install dash_tanstack_pivot
```

---

### 2. Plotly Dash (Warehouse & Local Parquet)

```python
import pandas as pd
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

# Point to local Parquet files, in-memory DuckDB, or remote Snowflake/Postgres
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data("s3://warehouse/100m_sales.parquet", "sales")

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="pivot",
        table="sales",
        serverSide=True,
        rowFields=["region", "country", "store"],
        colFields=["quarter", "month"],
        valConfigs=[
            {"field": "revenue", "agg": "sum", "format": "currency", "label": "Revenue ($)"},
            {"field": "profit",  "agg": "sum", "format": "currency", "label": "Profit ($)"},
        ],
        availableFieldList=["region", "country", "store", "quarter", "month", "revenue", "profit"],
        showRowTotals=True,
        showColTotals=True,
        style={"height": "700px", "width": "100%"},
    )
])

# Wires server-side pagination, sorting, filtering, and O(D) hierarchy batching in 1 call
register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot")

if __name__ == "__main__":
    app.run(debug=True)
```

---

### 3. Standalone React (Direct HTTP Transport)

No Python backend required inside the frontend container. Communicates with any V1 HTTP runtime service:

```tsx
import React from 'react';
import { PivotWorkspace, HttpPivotTransport, LatestViewportScheduler } from 'dash_tanstack_pivot/react';

const transport = new LatestViewportScheduler(
  new HttpPivotTransport({ endpoint: 'http://localhost:8765/v1/runtime' })
);

export function EnterpriseAnalytics() {
  return (
    <div style={{ height: '750px', width: '100%' }}>
      <PivotWorkspace
        table="multi_billion_transactions"
        transport={transport}
        rowFields={['market', 'sector', 'ticker']}
        colFields={['year', 'quarter']}
        valConfigs={[{ field: 'volume', agg: 'sum', label: 'Trading Volume' }]}
        theme="dark"
      />
    </div>
  );
}
```

---

### 4. Streamlit (Zero-Rerun V2 Component)

Pivoting, scrolling, and expanding tree nodes happen without triggering Streamlit's full Python script rerun:

```python
import streamlit as st
from streamlit_pivot import streamlit_pivot

st.set_page_config(layout="wide")
st.title("Billion-Row Transaction Intelligence")

streamlit_pivot(
    table="multi_billion_transactions",
    runtime_endpoint="http://127.0.0.1:8765/v1/runtime",
    row_fields=["market", "sector"],
    col_fields=["year"],
    val_configs=[{"field": "volume", "agg": "sum"}],
    height=650,
)
```

---

## 🏛️ The $\mathcal{O}(D)$ Hierarchical Engine Advantage

Traditional tree tables suffer from the **$N$-Query Waterfall Problem**: expanding 100 rows issues 100 separate database queries.

`serverside-pivot` solves this by grouping all open nodes by their depth level ($D$). It executes **a single SQL query per depth level** using vectorized `OR` filters and window functions:

$$\text{Traditional Grids: } \mathcal{O}(N) \text{ queries} \quad \longrightarrow \quad \text{\textbf{serverside-pivot: }} \mathcal{O}(D) \text{ queries (typically 3 to 5 queries total)}$$

Even with thousands of expanded tree nodes over hundreds of millions of raw records, data returns in sub-second time.

---

## 🛡️ Enterprise Governance: Cancellation, Memory & Security

- **Native Statement Interruption**: Query cancellation triggers DuckDB `cursor.interrupt()` with worker cleanup acknowledgement fences (`_await_execution_cleanup`)—guaranteeing no orphaned queries consume background CPU.
- **Byte-Bounded LRU Caching**: Enforces strict memory quotas using deep byte accounting (`table.nbytes` on Arrow, `memory_usage(deep=True)` on Pandas) with generation token invalidation.
- **Cancellable Large Exports**: Formal async export state machine (`queued → running → cancelling → cancelled/completed`) streaming multi-gigabyte CSV and XLSX files without leaking partial files.
- **Fail-Closed Verification**: Validated with **207 / 207 mapped browser tests** against AG Grid Enterprise 36.2.0 SSRM.

---

## 💻 Live Showcases (Revenue Operations)

To run the complete flagship showcase locally across Dash, React, and Streamlit:

```powershell
# 1. Start shared V1 Runtime Service (port 8765)
python -m uvicorn examples.flagship_runtime_server:app --port 8765

# 2. Launch Dash showcase (port 8050)
python examples/flagship_dash.py

# 3. Launch React Vite showcase (port 5174)
cd examples/flagship_react && npm run dev

# 4. Launch Streamlit showcase (port 8502)
streamlit run examples/flagship_streamlit.py --server.port 8502
```

---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for details.
