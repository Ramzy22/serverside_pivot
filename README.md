<div align="center">

# ⚡ serverside-pivot

### **The Open-Source Server-Side Pivot Table Engine for Python, React, Dash & Streamlit.**
#### *Warehouse-scale analytics powered by DuckDB & Ibis. Sub-second aggregations over 100M+ rows.*

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8-blue)](https://www.typescriptlang.org/)
[![DuckDB](https://img.shields.io/badge/Engine-DuckDB%20%7C%20PyArrow-yellow)](https://duckdb.org/)
[![Ibis Project](https://img.shields.io/badge/Federation-Ibis-orange)](https://ibis-project.org/)
[![AG Grid Parity](https://img.shields.io/badge/Certified%20Parity-AG%20Grid%2036.2%20SSRM-green)](docs/flagship-functionality-certification.md)
[![Tested Cases](https://img.shields.io/badge/Browser%20Cases-207%20%2F%20207%20Mapped-brightgreen)](tests/flagship_requirements.json)

[**Live Demos**](#-live-showcases-revenue-operations) •
[**Quickstart (Dash / React / Streamlit)**](#-quickstart) •
[**Architecture**](#-architecture-the-tri-host-runtime) •
[**Benchmark vs. AG Grid & Perspective**](#-benchmarks--head-to-head) •
[**Contract Certification**](#-fail-closed-certification--ag-grid-parity)

---

</div>

## 📌 Why `serverside-pivot`?

When building data applications in Python or React with massive datasets (1M to 100M+ rows), developers face an impossible trade-off:

1. **Client-side tables (Perspective, TanStack Table)** choke or crash browser RAM when pivoting large warehouse data.
2. **Commercial grids (AG Grid Enterprise)** charge $1,000 to $2,500+ per developer/year, and **leave the entire backend implementation to you** (you have to write the SQL generators, grouping logic, and pagination yourself).

**`serverside-pivot` is the missing full-stack solution:**
An out-of-the-box analytical server engine (powered by **DuckDB** and **Ibis**) paired with a high-density, virtualized frontend grid (`PivotRuntimeRoot`) that runs identically inside **Plotly Dash**, **Streamlit**, and standalone **React**.

```
[ DuckDB / Parquet / Snowflake / Postgres ]
                     │  (sub-second aggregation via Ibis)
                     ▼
          [ serverside-pivot Engine ]
                     │  (V1 HTTP Envelopes & PyArrow Streams)
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   Plotly Dash     React      Streamlit
  (Native App)  (Pure Web)   (Zero-Rerun)
```

---

## 🚀 Key Highlights

- **⚡ Sub-Second Aggregations**: Executes complex multidimensional pivots over 10M–100M+ rows in under 800ms using vectorized DuckDB and PyArrow Compute kernels.
- **🌐 100% Host-Neutral**: Single shared headless runtime (`PivotRuntimeRoot`) with 62 specialized hooks, running identically in **Dash**, **React**, and **Streamlit** (with isolated zero-rerun iframe execution).
- **🗂️ Server-Owned Pivot Slicing**: Handles 5,000+ wide pivot columns without client lag using SHA-256 stable layout identities and token-based compact scrolling.
- **🛡️ Native Statement Interruption**: Query cancellation triggers real DuckDB `cursor.interrupt()` with worker cleanup barriers—no orphaned background queries.
- **📥 Large Cancellable Exports**: Asynchronous state-machine streaming for multi-gigabyte CSV and multi-row formatted XLSX workbooks with strict row/byte quotas.
- **📋 Exact Clipboard Escaping**: RFC-compliant TSV clipboard serializer that preserves embedded quotes, tabs, and line-breaks.

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

### 2. Plotly Dash (10 Lines of Python)

```python
import pandas as pd
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

# 1. Connect any DataFrame, Arrow table, or SQL database via Ibis
df = pd.read_parquet("sales_data.parquet")
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(df, "sales")

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="pivot",
        table="sales",
        serverSide=True,
        rowFields=["region", "country"],
        colFields=["quarter"],
        valConfigs=[
            {"field": "revenue", "agg": "sum", "format": "currency", "label": "Revenue ($)"},
            {"field": "profit",  "agg": "sum", "format": "currency", "label": "Profit ($)"},
        ],
        availableFieldList=["region", "country", "quarter", "revenue", "profit"],
        showRowTotals=True,
        showColTotals=True,
        style={"height": "650px", "width": "100%"},
    )
])

# 2. Wire the server-side windowing callback with one command
register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot")

if __name__ == "__main__":
    app.run(debug=True)
```

---

### 3. Standalone React (Direct HTTP Transport)

No Python or Dash runtime required in the browser. Connect directly to any V1 HTTP runtime:

```tsx
import React from 'react';
import { PivotWorkspace, HttpPivotTransport, LatestViewportScheduler } from 'dash_tanstack_pivot/react';

const transport = new LatestViewportScheduler(
  new HttpPivotTransport({ endpoint: 'http://localhost:8765/v1/runtime' })
);

export function AnalyticsDashboard() {
  return (
    <div style={{ height: '700px', width: '100%' }}>
      <PivotWorkspace
        table="revenue_operations"
        transport={transport}
        rowFields={['region', 'segment']}
        colFields={['quarter']}
        valConfigs={[{ field: 'sales', agg: 'sum', label: 'Total Sales' }]}
        theme="dark"
      />
    </div>
  );
}
```

---

### 4. Streamlit (Zero-Rerun V2 Component)

Scrolling, pivoting, and resizing charts remain completely client-side without re-executing your Python script:

```python
import streamlit as st
from streamlit_pivot import streamlit_pivot

st.set_page_config(layout="wide")
st.title("Enterprise Revenue Analytics")

# Renders high-performance virtualized grid inside Streamlit
streamlit_pivot(
    table="sales",
    runtime_endpoint="http://127.0.0.1:8765/v1/runtime",
    row_fields=["region", "product"],
    col_fields=["year"],
    val_configs=[{"field": "sales", "agg": "sum"}],
    height=600,
)
```

---

## 📊 Benchmarks & Head-to-Head

| Dimension | `serverside-pivot` | AG Grid Enterprise 36.2 (SSRM) | FINOS Perspective |
| :--- | :--- | :--- | :--- |
| **License** | **Open Source (MIT)** | **Commercial ($1k–$2.5k+/seat/yr)** | Open Source (Apache 2.0) |
| **Backend Query Engine** | **Included** (DuckDB + PyArrow + Ibis) | **None** (You must build it yourself) | Included (In-memory C++ / WASM) |
| **Data Scalability** | **100M+ rows** (Disk/Warehouse offload) | Unlimited (depends on your server) | ~1M–5M rows (Bounded by Browser RAM) |
| **Supported Hosts** | **Dash, React, Streamlit** | React, Angular, Vue (No Dash/Streamlit) | Web Component, React, Jupyter |
| **Hierarchical Queries** | **$\mathcal{O}(D)$ Level-Based Batching** | $\mathcal{O}(N)$ Individual Group Requests | In-memory rollups |
| **Column Slicing** | **Server-Owned SHA-256 Revisions** | Client-side drag / Pivot keys | Client-side split-by |
| **Query Cancellation** | **Native `cursor.interrupt()`** | BYOB (Custom abort required) | Thread termination |
| **Clipboard TSV Fidelity** | **RFC-compliant (retains quotes/CRLF)** | Broken delimiters in default SSRM | Basic TSV |
| **Large File Export** | **Cancellable Async State Machine** | Client-side / Custom server | In-browser dump (Crashes tab on huge data) |

---

## 🏛️ Architecture: The Tri-Host Runtime

`serverside-pivot` is built on a modular 4-stage runtime pipeline separated across 62 specialized hooks:

```
DashTanstackPivot.react.tsx (Dash)  ──OR──  PivotWorkspace.tsx (React)
                     │                                   │
                     └───────────────┬───────────────────┘
                                     ▼
                        PivotRuntimeRoot.tsx (Core Root)
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         ▼                           ▼                           ▼
 useCoreRuntimeStage       useViewportRuntimeStage     useCommandRuntimeStage
 (State & Dimensions)      (Virtual Slicing & Scroll)   (Copy, Export, Charts)
         │                           │                           │
         └───────────────────────────┼───────────────────────────┘
                                     ▼
           PivotWorkspaceShell → PivotWorkspaceCanvas → Virtualized Table
```

### The $\mathcal{O}(D)$ Level-Based Batching Advantage
Traditional hierarchical trees execute $N$ queries (one query per expanded row). `serverside-pivot` groups all expanded branches by their depth ($D$) and generates **a single query per depth level** using grouped `OR` filters and window functions:
$$\text{Queries executed: } \mathcal{O}(D) \quad (\text{typically 3--5 queries for 500+ open nodes})$$

---

## 🏆 Fail-Closed Certification & AG Grid Parity

We believe claims are meaningless without verifiable proof. `serverside-pivot` is verified against **AG Grid Enterprise 36.2.0 (SSRM)** under a versioned, fail-closed contract:

- **207 / 207 Browser Qualification Cases Mapped**: Across Dash, React, and Streamlit.
- **Cryptographic Artifact Binding**: Test runs execute against fresh wheel and npm package builds, recording SHA-256 hashes for all loaded assets.
- **Fail-Closed Philosophy**: If a driver lacks statement cancellation, it raises `CancellationUnavailableError`. If an old response arrives late, it is discarded by `layoutRevision`.
- See the full specification: [`docs/flagship-functionality-certification.md`](docs/flagship-functionality-certification.md) and [`docs/column-layout-contract.md`](docs/column-layout-contract.md).

---

## 💻 Live Showcases (Revenue Operations)

To run the complete flagship showcase locally across all three hosts (with 12,000 live rows):

```powershell
# 1. Start the shared V1 HTTP Runtime Service (port 8765)
python -m uvicorn examples.flagship_runtime_server:app --port 8765

# 2. Launch Dash (port 8050)
python examples/flagship_dash.py

# 3. Launch React Vite app (port 5174)
cd examples/flagship_react && npm run dev

# 4. Launch Streamlit (port 8502)
streamlit run examples/flagship_streamlit.py --server.port 8502
```

---

## 📖 Documentation & Guides

- **[Column Layout Contract](docs/column-layout-contract.md)**: Deep dive into server-resolved pivot ordering, layout revisions, and pinned column slicing.
- **[Flagship Certification Matrix](docs/flagship-functionality-certification.md)**: The atomic 71-requirement contract and AG Grid Enterprise parity oracles.
- **[Distributed Runtime Operations](docs/operations/distributed-runtime.md)**: Production deployment guide with Redis caching, health checks, and Prometheus metrics.
- **[Production Security Runbook](docs/operations/production-security-runbook.md)**: ASVS Level 2 compliance, dataset authorization, and JWT authentication.

---

## 🤝 Contributing

Contributions are welcome! Please ensure that:
1. All changes pass `npm run typecheck` and `python -m pytest tests/`.
2. Any new user-visible capability maps to a contract test in `tests/flagship_requirements.json`.
3. Read the [Security Policy](SECURITY.md) before submitting security-sensitive patches.

---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for details.
