<div align="center">

# ⚡ serverside-pivot

### **Turnkey, Low-Code Server-Side Pivot Table for Python, React, Dash & Streamlit.**
#### *Connect any database in 3 lines of code. Billion-row analytics powered by DuckDB & Ibis. Zero browser memory limits.*

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8-blue)](https://www.typescriptlang.org/)
[![DuckDB](https://img.shields.io/badge/Engine-DuckDB%20%7C%20PyArrow-yellow)](https://duckdb.org/)
[![Ibis Project](https://img.shields.io/badge/Federation-Ibis-orange)](https://ibis-project.org/)
[![AG Grid Parity](https://img.shields.io/badge/Certified%20Parity-AG%20Grid%2036.2%20SSRM-green)](docs/flagship-functionality-certification.md)
[![Tested Cases](https://img.shields.io/badge/Browser%20Cases-207%20%2F%20207%20Mapped-brightgreen)](tests/flagship_requirements.json)

[**Low-Code in 3 Lines**](#-low-code-connect-any-database-in-3-lines) •
[**The Core Problem**](#-the-problem-with-existing-solutions) •
[**Quickstart (Dash / React / Streamlit)**](#-quickstart) •
[**Head-to-Head vs. Perspective & AG Grid**](#-head-to-head-breakdown) •
[**Live Demos**](#-live-showcases-revenue-operations)

---

</div>

## 🚀 The 3-Step Flow: 2 Lines of Code → Any Database → Interactive Web UI

```
┌─────────────────────────────────┐     ┌─────────────────────────────────┐     ┌─────────────────────────────────┐
│  1. THE 2-LINE CODE             │     │  2. ANY SCALE OR DATABASE       │     │  3. THE INTERACTIVE WEB UI      │
│  Minimal, elegant boilerplate   │ ──> │  Small local file or 100M+ rows │ ──> │  Virtual scrolling, sparklines, │
│  No backend to build            │     │  DuckDB, Snowflake, BigQuery    │     │  docked charts & zero RAM crash │
└─────────────────────────────────┘     └─────────────────────────────────┘     └─────────────────────────────────┘
```

Building enterprise pivot tables used to take months of backend engineering. With `serverside-pivot`, **you don't write SQL, grouping algorithms, or pagination logic**. 

Connect any SQL database, cloud warehouse, or local file in **2–3 lines of Python**:

```python
from pivot_engine import create_tanstack_adapter, register_pivot_app

# 1. Point to local Parquet/CSV files or warehouse (Snowflake, BigQuery, DuckDB, Postgres)
adapter = create_tanstack_adapter(backend_uri="duckdb:///analytics.db")  # or ":memory:", "snowflake://..."
adapter.load_data("sales.parquet", "sales")  # or pass pandas DataFrame / PyArrow Table

# 2. Attach to your Dash, Streamlit, or React app — zero custom backend needed!
register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot")
```

Whether your dataset has **10,000 rows** or **1,000,000,000+ rows**, the engine automatically compiles UI drags, group expansions, and filters into sub-second vector queries. The browser **never uses more than 80 MB of RAM**.
---

## 📌 The Problem with Existing Solutions

When developers build analytical dashboards with large enterprise datasets, they hit one of two walls:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       The Analytical Dilemma                                     │
├────────────────────────────────────────┬─────────────────────────────────────────────────────────┤
│ 1. In-Memory / Streaming Focused       │ 2. The Commercial "Empty Shell" Trap                    │
│    (FINOS Perspective, TanStack Table) │    (AG Grid Enterprise SSRM)                            │
├────────────────────────────────────────┼─────────────────────────────────────────────────────────┤
│ • Engineered for in-memory streaming   │ • Extraordinary frontend, but ZERO backend included.   │
│   tables and Arrow WebSocket feeds.    │ • Developer must spend months building SQL generators,  │
│ • Requires shipping full tables or     │   pivot aggregations, and windowing from scratch.       │
│   custom Python server hosting.        │ • Costs $1,000 to $2,500+ / developer / year.           │
│ • No turnkey pushdown to SQL/warehouse.│ • No native Python Dash or Streamlit adapters.          │
└────────────────────────────────────────┴─────────────────────────────────────────────────────────┘
```

### The Solution: `serverside-pivot`

`serverside-pivot` gives you the **complete full-stack engine out-of-the-box**:
1. **Low-Code Universal Connectors**: Built on **Ibis** and **DuckDB**, it connects natively to **Snowflake**, **BigQuery**, **ClickHouse**, **PostgreSQL**, **Polars**, **PyArrow**, or **Parquet** with zero SQL boilerplate.
2. **Zero Client Memory Limits**: Data is aggregated inside your database engine. The browser only requests lightweight 50-row virtualized slices. **Scale to Billions of rows with zero UI stutter.**
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
| **Primary Architecture** | **Pushdown SQL / Warehouse Engine**<br>*(DuckDB, Ibis, Parquet, Snowflake)* | **In-Memory C++ / WASM Streaming**<br>*(High-frequency Arrow tick stream)* | **Client-Side Shell / SSRM Protocol**<br>*(Frontend only, BYO backend)* |
| **Effort to Implement** | **LOW-CODE (3 lines of Python)**<br>*(Turnkey engine included)* | **Medium**<br>*(Feed Arrow buffers / host C++ table)* | **HIGH (Months of backend dev)**<br>*(You must build the server engine)* |
| **Pushdown Database Connectors** | **YES (via Ibis & DuckDB)**<br>*(Snowflake, BigQuery, DuckDB, Postgres)* | **External / By Design**<br>*(Expects pre-ingested Arrow tables)* | **NO**<br>*(You write custom SQL connectors)* |
| **Scale / Dataset Scope** | **Billion+ rows in-database**<br>*(Bounded only by DB disk/cluster)* | **In-Memory Bounded**<br>*(Best for RAM-sized streaming feeds)* | **Unlimited**<br>*(...but ONLY if you build the backend)* |
| **Client Memory Footprint** | **Constant (< 80 MB)**<br>*(50-row virtualized windowing)* | **Dependent on mode**<br>*(Light in Server/WS mode, heavy in WASM)* | **Constant (< 100 MB)**<br>*(Virtualized DOM)* |
| **Backend Query Engine** | **INCLUDED (Turnkey)**<br>*(DuckDB + PyArrow + Ibis)* | **In-Memory C++ Hypercube**<br>*(Excellent live streaming rollups)* | **NONE (You must build it)**<br>*(You write the SQL/groupby backend)* |
| **License Cost** | **FREE & Open Source (MIT)** | FREE & Open Source (Apache 2.0) | **$1,000 – $2,500+ / dev / year** |
| **Supported Hosts** | **Dash, React, Streamlit** | Web Component, Jupyter, Python | React, Angular, Vue (No Dash/Streamlit) |
| **Hierarchical Queries** | **$\mathcal{O}(D)$ Level-Based Batching** | In-memory hypercube rollup | $\mathcal{O}(N)$ Individual Group Waterfall |
| **Query Cancellation** | **Native `cursor.interrupt()`** | Client abort | Custom abort controller wiring |
| **Clipboard TSV Fidelity** | **RFC-compliant (retains quotes/CRLF)** | Standard TSV export | Fails delimiter round-trip in SSRM |
| **Export Governance** | **Cancellable Async State Machine** | Client / Server Arrow slice | Client/Server custom plugins |

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
adapter.load_data("sales.parquet", "sales")  # or pass pandas DataFrame / PyArrow Table

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
from streamlit_pivot import pivot_workspace

st.set_page_config(layout="wide")
st.title("Billion-Row Transaction Intelligence")

pivot_workspace(
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

Host file downloads support **CSV and XLSX** across Dash, React, and Streamlit. Clipboard actions provide **TSV and HTML**. See the [operations guide](docs/OPERATIONS.md#export-contract-and-release-qualification) for export limits and the release qualification command, and the [certification contract](docs/flagship-functionality-certification.md#export-qualification-contract) for required evidence.

---

## 💻 Live Showcases & Demos

Start with the focused Dash examples to learn the integration patterns:

- [Basic pivot](examples/example_dash_basic.py): a minimal Dash app with row grouping and aggregated measures.
- [Hierarchical pivot](examples/example_dash_hierarchical.py): expandable groups with server-side queries.
- [SQL multi-instance workspace](examples/example_dash_sql_multi_instance.py): multiple pivot instances in one Dash app.

### 🌟 1. Industry-Specific Showcases (Ready in 1 Command)

Run full-featured, interactive dashboards pre-configured with industry KPIs, customized color palettes, and realistic DuckDB datasets:

`powershell
# 📈 Trading Desk & Risk Portfolio (50,000 positions, VaR, DV01, PnL - Port 8051)
python examples/demo_trader.py

# 👥 People Analytics & Compensation (35,000 employees, retention, bonus - Port 8052)
python examples/demo_hr.py

# 🚀 B2B SaaS Growth & Cohorts (40,000 accounts, ARR, MRR, NRR, LTV/CAC - Port 8053)
python examples/demo_saas.py

# 🎛️ Unified Showcase Hub (Interactive tabbed multi-industry launcher - Port 8054)
python examples/demo_showcase_hub.py
`

---

### 🌐 2. Multi-Framework Host Architecture

To run the flagship Revenue Operations workspace (12,000 rows) across Dash, React, and Streamlit:

`powershell
# 1. Start shared V1 Runtime Service (port 8765)
python -m uvicorn examples.flagship_runtime_server:app --port 8765

# 2. Launch Dash showcase (port 8050)
python examples/flagship_dash.py

# 3. Launch React Vite showcase (port 5174)
cd examples/flagship_react && npm run dev

# 4. Launch Streamlit showcase (port 8502)
streamlit run examples/flagship_streamlit.py --server.port 8502
`
---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for details.
