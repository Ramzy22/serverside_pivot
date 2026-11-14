<div align="center">

# ⚡ serverside-pivot

### **Turnkey, Low-Code Server-Side Pivot Table for Python, Dash, Streamlit & React.**
#### *Universal database connectivity via Ibis & DuckDB. Billion-row analytical hierarchy with zero client RAM limits.*


[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8-blue)](https://www.typescriptlang.org/)
[![DuckDB](https://img.shields.io/badge/Engine-DuckDB%20%7C%20PyArrow-yellow)](https://duckdb.org/)
[![Ibis Project](https://img.shields.io/badge/Federation-Ibis-orange)](https://ibis-project.org/)
[![AG Grid Parity](https://img.shields.io/badge/Certified%20Parity-AG%20Grid%2036.2%20SSRM-green)](docs/flagship-functionality-certification.md)
[![Tested Cases](https://img.shields.io/badge/Browser%20Cases-207%20%2F%20207%20Mapped-brightgreen)](tests/flagship_requirements.json)

[**Low-Code in 3 Lines**](#-the-3-step-flow-2-lines-of-code--any-database--interactive-web-ui) •
[**Enterprise Feature Matrix**](#-enterprise-feature-matrix) •
[**Interactive Dashboard Showcase**](#-interactive-dashboard-showcase) •
[**The Core Problem**](#-the-problem-with-existing-solutions) •
[**Quickstart (Dash / React / Streamlit)**](#-quickstart) •
[**Cloud Warehouses (Ibis Pushdown)**](#-cloud-data-warehouses--multi-backend-ibis-pushdown) •
[**Client-Side DuckDB-WASM**](#-client-side-duckdb-wasm--hybrid-execution-mode) •
[**Head-to-Head vs. Perspective & AG Grid**](#-head-to-head-breakdown) •
[**Live Demos**](#-live-showcases--demos)

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

## 🖥️ Interactive Dashboard Showcase

Live interactive recordings from our showcase applications ([demo_trader.py](examples/demo_trader.py), [demo_saas.py](examples/demo_saas.py), [demo_hr.py](examples/demo_hr.py)) demonstrating fluid client interactions, zero RAM bloat, and theme versatility:

### 1. Bloomberg Black Theme — $\mathcal{O}(D)$ Hierarchical Expand & Collapse with Column Data Bars
Expand and collapse multi-level hierarchies across asset classes, trading desks, and individual instruments with instant server-side aggregation pushdown, zero client RAM pressure, and live proportional column data bars ([demo_trader.py](examples/demo_trader.py)):

![Hierarchical Expand and Collapse with Column Data Bars](docs/assets/demo_expand_collapse.gif)

### 2. Dark Theme — Interactive Graph Creation & Docked Visualizations
Launch the chart builder directly from the toolbar, preview trend, composition, and comparison presets in real time, and dock resizable chart panes side-by-side with your live pivot table ([demo_saas.py](examples/demo_saas.py)):

![Interactive Graph Creation](docs/assets/demo_graph_creation.gif)

### 3. Light Theme — Inline Cell Charts & Sparkline Drilldown
Embed high-performance inline SVG sparklines (column, area, line) directly inside table cells. Double-click any sparkline cell to open the interactive crosshair detail graph with zoom controls, or adjust chart styles dynamically via the field settings modal ([demo_hr.py](examples/demo_hr.py)):

![Inline Cell Charts and Sparkline Drilldown](docs/assets/demo_inline_cell_chart.gif)

---

## ✨ Enterprise Feature Matrix

Comprehensive analytical suite surfaced from [`docs/PIVOT_MASTER_DOCUMENTATION.md`](docs/PIVOT_MASTER_DOCUMENTATION.md):

| Capability Area | Feature Highlights | Specification / Supported APIs |
| :--- | :--- | :--- |
| **4 View & Layout Modes** | Multidimensional Pivot (`pivot`), Hierarchical Parent-Child Tree (`tree` balanced or unbalanced), Flat Tabular Record Grid (`table`), and Specialized Financial/Executive Report Mode (`report`) with custom period axes and per-level totals. | `viewMode`: `'pivot'` \| `'tree'` \| `'table'` \| `'report'` |
| **2D Virtualization (Row + Col)** | Server-side block-cached row model with deep byte-bounded LRU memory eviction. Active horizontal column virtualization (`col_start` / `col_end`) scaling smoothly to 1,000+ pivoted columns. Velocity prefetching & latest-wins request cancellation (`LatestViewportScheduler`). | Sub-second viewport, constant < 80 MB RAM |
| **Visual Analytics & Charting Suite** | Integrated **ECharts 6** & **ECharts-GL 3D**: 23+ chart types across 11 families (Stacked/Grouped Bar, Line, Area, Pie/Donut, Scatter, Bubble, Combo, Waterfall, Radar, Heatmap, 3D Bar / Line / Scatter, Sankey, Sunburst, Icicle, Treemap) with moving averages, linear trendlines, and element-click interactive editing. | Dockable (Left, Right, Top, Bottom) or Floating Panes |
| **In-Cell Visualizations** | Server-aggregated (`array_agg`) inline SVG sparklines (Line, Area, Bar) with hover detail graphs. Color scale heatmaps and interactive cell data bars with custom range thresholds. | In-cell data rendering with zero DOM bloat |
| **Formulas & Window Functions** | Excel-like calculated measures using `[Column Name]` syntax evaluated safely via AST parser. Built-in pivot window functions (`percent_of_row`, `percent_of_col`, `percent_of_grand_total`). Dynamic visual totals recalculating parent groups based on post-filter child rows. | Safe AST sandbox, dynamic server pushdown |
| **Sorting, Pinning & Spanning** | Multi-column sort, absolute value sort (`abs`), pivot column sorting (by labels A-Z or measure totals), financial tenor / natural sorting (`1M, 3M, 1Y`). Left/Right column & Top/Bottom row pinning, synchronized sticky headers, row spanning. | Full context menu + drag-and-drop toolbar |
| **Filtering & Drill-Through Surfaces** | Native `DateRangeFilter`, slider/input `NumericRangeFilter`, and searchable `MultiSelectFilter`. Drill-through raw record inspection via `DrillThroughModal`, `DetailSidePanel`, `DetailDrawer`, and `InlineDetailPanel`. | `detailMode`: `'none'` \| `'inline'` \| `'sidepanel'` \| `'drawer'` |
| **11 Built-in Themes** | Enterprise dark & light designs: `light`, `dark`, `flash`, `material`, `balham`, `bloomberg`, `bloomberg_black`, `strata`, `crystal`, `alabaster`, `satin` with complete `--sp-*` CSS custom properties and CSS variable injection. | Real-time theme switcher + CSS token overrides |
| **Tri-Host Universal Architecture** | Zero-compromise universal runtime for **Plotly Dash** (`DashTanstackPivot`), **Standalone React** (`PivotWorkspace` + `HttpPivotTransport`), and **Streamlit** (zero-rerun iframe component). | Unified Protocol V1 (`schemaVersion: 1`) |
| **Enterprise Persistence & CDC** | Real-time Change Data Capture (`PivotCDCManager`), push/poll CDC sync, durable view/report saving & restoring across reloads, L1 in-memory + L2 Redis caching, and Row-Level Security (RLS). | Multi-tenant production-ready |
| **Client-Side DuckDB-WASM & Hybrid Mode** | Zero-network in-browser analytical execution powered by **DuckDB-WASM** and AST SQL planner. Auto-detection (`execution_mode="auto"`) seamlessly switching between browser (<= 50,000 rows) and server warehouse pushdown. Full $\mathcal{O}(D)$ hierarchy, formula, and chart parity with 0 server roundtrips. | `execution_mode`: `'auto'` \| `'client'` \| `'server'`<br>`client_engine`: `'auto'` \| `'duckdb-wasm'` \| `'memory'` |
| **Cloud Warehouses & Ibis Pushdown** | Zero-copy lazy `ibis.Table` registration across **Snowflake, BigQuery, PostgreSQL, MotherDuck, ClickHouse, Databricks, Trino, SQLite, and DuckDB**. Dynamic `BackendCapabilities` introspection, dialect-portable pushdown (`ILIKE` fallback, regex tenor sort fallback), bounded paged batch export streaming, and TTL schema metadata caching. | `ibis.Table` \| `snowflake://` \| `bigquery://` \| `postgresql://` \| `md:` \| `duckdb://` |
| **Cancellable Large Exports** | Streaming multi-gigabyte CSV and XLSX downloads with progress counters, query cancellation cleanup (`cursor.interrupt()`), and RFC-compliant TSV/HTML clipboard copy. | Client SheetJS fallback & server streaming |

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
│ • Engineered for in-memory streaming   │ • Extraordinary frontend, but ZERO backend included.    │
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
npm install serverside_pivot
```

---

### 2. Plotly Dash (Warehouse & Local Parquet)

```python
import pandas as pd
from dash import Dash, html
from serverside_pivot import DashTanstackPivot
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

### 3. Enterprise Cloud Warehouses (Zero-Copy Lazy Ibis Pushdown)

Connect directly to **Snowflake, BigQuery, PostgreSQL, ClickHouse, MotherDuck, or Databricks** with **zero memory materialization** (`to_pyarrow()` or `to_pandas()` is never called on the base table). Pass a lazy `ibis.Table` expression or connection config directly:

```python
import ibis
from dash import Dash, html
from serverside_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app
from pivot_engine.data_input import normalize_data_input

# 1. Connect to Snowflake (or BigQuery, PostgreSQL, MotherDuck, Databricks, ClickHouse)
# All aggregations, filtering, and expansions are pushed down into the remote warehouse!
con = ibis.snowflake.connect(
    user="analytics_svc",
    password="***",
    account="acme-us-east-1",
    database="FINOPS_PROD/PUBLIC",
    warehouse="COMPUTE_WH",
)

# 2. Reference the remote table lazily (zero client/server RAM overhead)
lazy_table = con.table("cloud_warehouse_metrics")

# 3. Create adapter and register the lazy Ibis table expression
adapter = create_tanstack_adapter(backend_uri=":memory:")
normalize_data_input(lazy_table, "cloud_warehouse_metrics", adapter.controller)

# Alternatively, pass a lazy connection dictionary:
# adapter.load_data({
#     "connection_string": "snowflake://analytics_svc:***@acme-us-east-1/FINOPS_PROD/PUBLIC?warehouse=COMPUTE_WH",
#     "table": "cloud_warehouse_metrics",
#     "lazy": True,
# }, "cloud_warehouse_metrics")

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="cloud-pivot",
        table="cloud_warehouse_metrics",
        serverSide=True,
        rowFields=["cloud_provider", "region", "service_name"],
        colFields=["billing_quarter"],
        valConfigs=[
            {"field": "compute_cost", "agg": "sum", "format": "currency", "label": "Cost ($)"},
            {"field": "workload_count", "agg": "sum", "format": "number", "label": "Workloads"},
        ],
        showRowTotals=True,
        showColTotals=True,
        style={"height": "750px", "width": "100%"},
    )
])

register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="cloud-pivot")

if __name__ == "__main__":
    app.run(debug=True)
```

---

### 4. Standalone React (Direct HTTP Transport)

No Python backend required inside the frontend container. Communicates with any V1 HTTP runtime service:

```tsx
import React from 'react';
import {
  PivotWorkspace,
  HttpPivotTransport,
  LatestViewportScheduler,
  themes,
  injectThemeCssVars,
  type PivotRuntimeProps,
  type ValConfig,
  type FilterConfig,
  type SortConfig,
  type ChartDefinition,
} from 'serverside_pivot/react';

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
        valConfigs={[{ field: 'volume', agg: 'sum', label: 'Trading Volume' } satisfies ValConfig]}
        defaultTheme="bloomberg"
      />
    </div>
  );
}
```

---

### 5. Streamlit (Zero-Rerun V2 Component)

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

## ☁️ Cloud Data Warehouses & Multi-Backend Ibis Pushdown

`serverside-pivot` provides enterprise-grade compatibility with cloud data warehouses and distributed SQL engines powered by **[Ibis](https://ibis-project.org/)**. Unlike conventional pivot tools that pull raw tables into server memory (`to_pyarrow()` / `to_pandas()`), `serverside-pivot` executes **zero-copy lazy pushdown**: your data stays entirely within the remote warehouse, and only lightweight 50-row virtualized slices travel across the wire.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        Zero-Copy Cloud Analytical Architecture                         │
└────────────────────────────────────────────────────────────────────────────────────────┘
  [ Snowflake / BigQuery / PostgreSQL / MotherDuck / ClickHouse / Databricks / Trino ]
                                        │
             (1) Zero-Copy Lazy `ibis.Table` Registration (Zero RAM Download)
                                        ▼
                  ┌──────────────────────────────────────────┐
                  │          `IbisTableCatalog`              │
                  │  • Qualified `database.schema.table`     │
                  │  • TTL Schema Cache (Bounded 300s TTL)   │
                  │  • Dialect & Capabilities Inspection     │
                  └──────────────────────────────────────────┘
                                        │
             (2) O(D) Level-Based Grouping & Dialect-Portable AST Compilation
                                        ▼
                  ┌──────────────────────────────────────────┐
                  │          `IbisPlanner` Engine            │
                  │  • Native ILIKE / LOWER() LIKE Fallback  │
                  │  • Regex Tenor Sort Fallback             │
                  │  • Dialect Placeholder Adaptation (?/%s) │
                  │  • Non-blocking Cost Estimation          │
                  └──────────────────────────────────────────┘
                                        │
             (3) Sub-Second Pushdown Query Execution & Bounded Arrow Paging
                                        ▼
                  ┌──────────────────────────────────────────┐
                  │         Browser Client Viewport          │
                  │    Virtualized 50-Row Window (<80 MB)    │
                  └──────────────────────────────────────────┘
```

### 1. Zero-Copy Lazy Table Registration

Pass an existing lazy `ibis.Table` expression directly to `normalize_data_input` without triggering eager evaluation or memory materialization:

```python
import ibis
from pivot_engine import create_tanstack_adapter, register_pivot_app
from pivot_engine.data_input import normalize_data_input

# Connect to your cloud warehouse via Ibis
con = ibis.snowflake.connect(
    user="analytics_svc",
    password="***",
    account="acme-us-east-1",
    database="FINOPS_PROD/PUBLIC",
    warehouse="COMPUTE_WH",
)
lazy_table = con.table("cloud_warehouse_metrics")

# Registers table lazily — ZERO rows are materialized into Python memory!
adapter = create_tanstack_adapter(backend_uri=":memory:")
normalize_data_input(lazy_table, "cloud_warehouse_metrics", adapter.controller)
```

Alternatively, register via a connection dictionary:

```python
adapter.load_data({
    "connection_string": "bigquery://my-gcp-project/analytics_dw",
    "table": "workload_costs",
    "lazy": True,  # Enables zero-copy lazy catalog registration
}, "workload_costs")
```

---

### 2. Supported Cloud Dialects & Capabilities Matrix

The engine inspects dialect capabilities dynamically through `BackendCapabilities` rather than hardcoding driver checks, guaranteeing deterministic execution across heterogeneous warehouse engines:

| Warehouse / Dialect | Connection URI Scheme | Concurrency Model | Text Search (`ILIKE`) | Param Style | Rollup DDL Materialization | Write-Back Mutations |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| ❄️ **Snowflake** | `snowflake://user:pass@acct/db/schema` | Concurrent pool | Native `ILIKE` | `pyformat` (`%s`) | Disabled (Fail-closed) | Disabled (Opt-in) |
| 🔍 **Google BigQuery** | `bigquery://project-id/dataset-id` | Concurrent pool | `LOWER(col) LIKE` fallback | `qmark` (`?`) | Disabled (Fail-closed) | Disabled (Opt-in) |
| 🐘 **PostgreSQL / Supabase** | `postgresql://user:pass@host:5432/db` | Concurrent pool | Native `ILIKE` | `pyformat` (`%s`) | Disabled (Fail-closed) | Disabled (Opt-in) |
| 🦆 **MotherDuck** | `md:database_name` | Shared connection lock | Native `ILIKE` | `qmark` (`?`) | Disabled (Fail-closed) | Disabled (Opt-in) |
| ⚡ **ClickHouse** | `clickhouse://user:pass@host:8123/db` | Concurrent pool | Native `ILIKE` | `pyformat` (`%s`) | Disabled (Fail-closed) | Disabled (Opt-in) |
| 🧱 **Databricks / Spark** | `databricks://user:token@host/db` | Concurrent pool | Native `ILIKE` | `qmark` (`?`) | Disabled (Fail-closed) | Disabled (Opt-in) |
| 🌊 **Trino / Presto** | `trino://user@host:8080/catalog/schema`| Concurrent pool | Native `ILIKE` | `qmark` (`?`) | Disabled (Fail-closed) | Disabled (Opt-in) |
| 🪶 **SQLite** | `sqlite:///path/to/db.sqlite` | Shared connection lock | `LOWER(col) LIKE` fallback | `qmark` (`?`) | Enabled | Enabled |
| 🦆 **DuckDB (Embedded)** | `:memory:` or `duckdb:///file.db` | Shared connection lock | Native `ILIKE` | `qmark` (`?`) | Enabled | Enabled |

---

### 3. Dialect Portability & Resilience

- **Case-Insensitive Filtering**: On dialects that do not support native `ILIKE` (e.g. Google BigQuery, SQLite), expressions automatically fall back to `LOWER(column) LIKE LOWER(pattern)` without syntax errors.
- **Tenor & Natural Sorting Fallback**: If a database lacks regex substring extraction (`re_extract`), tenor columns (`1D`, `2W`, `6M`, `1Y`) fall back gracefully to standard lexical ordering.
- **Bounded Paged Batch Streaming**: Cloud warehouses without cursor-level Arrow batch readers export safely using bounded pagination (`.limit(chunk_rows, offset=offset).to_pyarrow()`), preventing OOM failures during multi-gigabyte exports.
- **TTL Schema Caching**: Column names, data types, and nullability are cached in `IbisTableCatalog` with a configurable TTL (`PIVOT_SCHEMA_CACHE_TTL_SECONDS=300`) to eliminate redundant `INFORMATION_SCHEMA` network round-trips.
- **Non-Blocking Cost Estimation**: Query planners bypass expensive live `COUNT(*)` network queries against cloud warehouses during optimization, defaulting to cost constants unless explicitly enabled.

---

### 4. Cloud Governance & Security Controls

Analytical cloud warehouses are configured **fail-closed** by default to protect production systems:

- **Read-Only Analytical Pushdown**: Remote warehouses reject write-back operations unless explicitly authorized via `PIVOT_ALLOW_REMOTE_MUTATIONS=1` or `allow_remote_mutations=True`.
- **DDL Protection**: Background rollup pre-computation (`MaterializedHierarchyManager`) never executes `CREATE TABLE` on remote warehouses without explicit opt-in (`PIVOT_ALLOW_REMOTE_DDL=1`).
- **Row-ID Independence**: Cloud warehouses do not support DuckDB/SQLite internal `rowid` pseudo-columns. Aggregate edits automatically use key-column aggregate fallbacks.
- **Parameter Placeholder Adaptation**: Parameterized mutations translate dynamically between `?` (`qmark`) and `%s` (`pyformat`) depending on the target warehouse driver.

#### Cloud Environment Configuration Reference

| Environment Variable | Default | Purpose |
| :--- | :--- | :--- |
| `BACKEND_URI` | `:memory:` | Target connection URI (`snowflake://`, `bigquery://`, `postgresql://`, `md:`, `duckdb://`). |
| `BACKEND_DATABASE` | `None` | Default database or catalog identifier for qualified table resolution. |
| `BACKEND_SCHEMA` | `None` | Default schema identifier (e.g. `PUBLIC`, `analytics`). |
| `BACKEND_READ_ONLY` | `False` | Forces all controller operations into strict read-only analytical mode. |
| `PIVOT_ALLOW_REMOTE_DDL` | `0` | Set `1` to allow background rollup table creation on remote warehouses. |
| `PIVOT_ALLOW_REMOTE_MUTATIONS` | `0` | Set `1` to allow SQL cell edits and row transaction write-backs to remote warehouses. |
| `PIVOT_SCHEMA_CACHE_TTL_SECONDS` | `300` | TTL in seconds for `IbisTableCatalog` schema and column metadata caching. |
| `PIVOT_ESTIMATE_ROW_COUNT_QUERY` | `0` | Set `1` to execute live network `COUNT(*)` during query planning cost estimation. |

---

## ⚡ Client-Side DuckDB-WASM & Hybrid Execution Mode

While enterprise warehouses handle billions of rows on the backend, small-to-medium analytical views (up to 50,000 rows) or offline datasets do not need server query latency. `serverside-pivot` introduces a **Zero-Network Client-Side Analytical Engine** powered by **DuckDB-WASM** and an in-browser vectorized SQL planner, providing complete bidirectional parity between browser-local and server-side execution.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        Universal Hybrid Execution Architecture                         │
└────────────────────────────────────────────────────────────────────────────────────────┘
                    Input Tabular Data / DataFrame / Parquet / URL
                                         │
                                         ▼
                     ┌───────────────────────────────────────┐
                     │    `resolve_execution_mode(...)`      │
                     │  • `execution_mode="auto"` (Default)  │
                     └───────────────────────────────────────┘
                                         │
               ┌─────────────────────────┴─────────────────────────┐
               ▼ (<= 50,000 Rows / In-Memory Data)                  ▼ (> 50,000 Rows or Warehouse Table)
 ┌───────────────────────────────────────────┐       ┌───────────────────────────────────────────┐
 │   CLIENT MODE: Browser DuckDB-WASM        │       │   SERVER MODE: Ibis Pushdown / DuckDB     │
 │ • Zero network latency (0 roundtrips)     │       │ • Unlimited dataset scale (1B+ rows)      │
 │ • In-browser SQL & Arrow AST compilation  │       │ • Remote Snowflake / BigQuery / Postgres  │
 │ • Offline & static client export ready    │       │ • Multi-worker Redis caching & RLS        │
 │ • Constant < 80 MB client RAM             │       │ • Bounded 50-row virtual viewport slices  │
 └───────────────────────────────────────────┘       └───────────────────────────────────────────┘
```

### 1. Zero-Boilerplate Hybrid Execution (`execution_mode="auto"`)

Use the `configure_pivot_execution` helper in Python to automatically configure your `DashTanstackPivot` component. The engine transparently decides whether to compute pivots inside the browser or delegate to the server:

```python
import pandas as pd
from dash import Dash, html
from serverside_pivot import DashTanstackPivot
from pivot_engine import configure_pivot_execution

df = pd.read_parquet("quarterly_sales.parquet")  # e.g. 15,000 records

# Generates normalized Dash props with auto-resolved execution mode
execution_props = configure_pivot_execution(
    data=df,                      # Pandas / Polars DataFrame, PyArrow Table, or dict list
    execution_mode="auto",        # 'auto' (default), 'client', or 'server'
    client_row_threshold=50_000,  # Runs in-browser if <= 50,000 rows
    client_engine="auto",         # 'auto', 'duckdb-wasm', or 'memory'
)

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="pivot",
        rowFields=["region", "country", "store"],
        colFields=["quarter"],
        valConfigs=[{"field": "revenue", "agg": "sum", "format": "currency"}],
        **execution_props,
    )
])
```

### 2. Full Architectural Parity in Browser

When running in client mode, `ClientPivotTransport` executes the identical Pivot Runtime Protocol V1 (`schemaVersion: 1`) entirely within WebAssembly:
- **$\mathcal{O}(D)$ Hierarchy Expansions**: Multi-level drilldowns expand instantaneously without network requests.
- **Dynamic Pivot Columns**: Column pivoting across multiple dimensions (`colFields`) with automatic row and column grand totals.
- **Calculated Measures & Formulas**: In-browser AST evaluation for calculated measures (`[revenue] - [cost]`) and window functions (`percent_of_grand_total`).
- **Interactive Visualizations & Charts**: Live 2D/3D ECharts docking directly fed from the client DuckDB instance.
- **Local TSV / Excel Exports**: Client-side SheetJS streaming exports generated without server roundtrips.

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

```powershell
# 📈 Trading Desk & Risk Portfolio (50,000 positions, VaR, DV01, PnL - Port 8051)
python examples/demo_trader.py

# 👥 People Analytics & Compensation (35,000 employees, retention, bonus - Port 8052)
python examples/demo_hr.py

# 🚀 B2B SaaS Growth & Cohorts (40,000 accounts, ARR, MRR, NRR, LTV/CAC - Port 8053)
python examples/demo_saas.py

# 🎛️ Unified Showcase Hub (Interactive tabbed multi-industry launcher - Port 8054)
python examples/demo_showcase_hub.py

# ☁️ Enterprise Cloud Warehouse & Ibis Pushdown (40,000 workloads, Snowflake/BigQuery/Postgres - Port 8055)
python examples/demo_cloud_warehouse.py

# ⚡ Client-Side DuckDB-WASM & Hybrid Auto-Detection (2,500 - 50,000 rows, zero server lag - Port 8056)
python examples/demo_clientside_version.py
```

---

### 🌐 2. Multi-Framework Host Architecture

To run the flagship Revenue Operations workspace (12,000 rows) across Dash, React, and Streamlit:

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
