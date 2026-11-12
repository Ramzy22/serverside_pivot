# ⚡ dash_tanstack_pivot

### **Turnkey Analytical Pivot Table & Virtualized Data Grid for React**
#### *Dual-Engine Architecture: Zero-Network In-Browser Analytics (DuckDB-WASM & In-Memory) + Scalable Server-Side Pushdown.*

[![npm version](https://img.shields.io/npm/v/dash_tanstack_pivot.svg?color=cb3837)](https://www.npmjs.com/package/dash_tanstack_pivot)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8-blue)](https://www.typescriptlang.org/)
[![React](https://img.shields.io/badge/React-18%20%7C%2019-61dafb)](https://react.dev/)
[![DuckDB-WASM](https://img.shields.io/badge/Engine-DuckDB--WASM%20%7C%20In--Memory-yellow)](https://duckdb.org/docs/api/wasm/overview)
[![ECharts](https://img.shields.io/badge/Charts-ECharts%206%20%2B%20GL%203D-red)](https://echarts.apache.org/)

`dash_tanstack_pivot` is an enterprise-grade, low-code analytical pivot table and 2D virtualized grid for **React**. Built on **TanStack Table v8**, **DuckDB-WASM**, and **Apache ECharts 6**, it bridges the gap between lightweight client tables and heavy server-side BI tools.

---

## 🚀 Why `dash_tanstack_pivot`?

Unlike traditional grid components that either crash browser memory with large datasets or force you to spend months building custom SQL backends:

1. **Dual Execution Engine (Hybrid)**:
   - **Client-Side Mode (`DuckDB-WASM` & In-Memory)**: Run complete multi-dimensional pivots, $\mathcal{O}(D)$ level-based hierarchy expansions, multi-column sorting, Excel formulas, window functions, and ECharts visualizations **100% inside the browser with ZERO server roundtrips and zero latency**.
   - **Server-Side Pushdown Mode**: Connect seamlessly to Python backend services (`serverside-pivot`) backed by **Snowflake, BigQuery, PostgreSQL, ClickHouse, MotherDuck, or DuckDB** to query hundreds of millions of rows with bounded 50-row virtual slices.
2. **2D Virtualization (Row + Column)**: Smoothly virtualizes both vertical rows (millions of records) and horizontal pivoted columns (1,000+ columns) with a **constant < 80 MB client RAM footprint**.
3. **Turnkey BI Analytics**: Built-in 23+ chart families (ECharts 2D/3D), inline cell sparklines, heatmaps, Excel-like calculated formulas (`[revenue] - [cost]`), window functions (`percent_of_grand_total`), 11 enterprise themes, and client-side SheetJS XLSX/CSV export.

---

## 📦 Installation

```bash
npm install dash_tanstack_pivot
# or
yarn add dash_tanstack_pivot
# or
pnpm add dash_tanstack_pivot
```

### Peer Dependencies
```bash
npm install react react-dom styled-components
```

---

## ⚡ Quickstart

### 1. Zero-Backend Client-Side Pivot (DuckDB-WASM / In-Memory)

Feed your JSON records or CSV/Parquet data directly into `PivotWorkspace`. The internal `ClientPivotTransport` executes all groupings, multi-column aggregations, and expansions in browser WebAssembly:

```tsx
import React from 'react';
import { PivotWorkspace, ClientPivotTransport } from 'dash_tanstack_pivot/react';

const salesData = [
  { region: 'North America', country: 'USA', rep: 'Alice', year: '2024', revenue: 150000, cost: 95000 },
  { region: 'North America', country: 'USA', rep: 'Bob', year: '2025', revenue: 180000, cost: 110000 },
  { region: 'North America', country: 'Canada', rep: 'Charlie', year: '2024', revenue: 90000, cost: 60000 },
  { region: 'Europe', country: 'Germany', rep: 'David', year: '2024', revenue: 130000, cost: 85000 },
  { region: 'Europe', country: 'France', rep: 'Emma', year: '2025', revenue: 140000, cost: 90000 },
  // ... up to 50,000+ rows
];

// Initialize client transport (runs 100% in-browser)
const clientTransport = new ClientPivotTransport({
  tableName: 'sales',
  data: salesData,
  clientEngine: 'auto', // 'auto' (>= 1000 rows activates DuckDB-Wasm), 'duckdb-wasm', or 'memory'
});

export function App() {
  return (
    <div style={{ height: '800px', width: '100%' }}>
      <PivotWorkspace
        table="sales"
        transport={clientTransport}
        rowFields={['region', 'country', 'rep']}
        colFields={['year']}
        valConfigs={[
          { field: 'revenue', agg: 'sum', format: 'currency', label: 'Revenue ($)' },
          { field: 'cost', agg: 'sum', format: 'currency', label: 'Cost ($)' },
          {
            field: 'profit',
            label: 'Gross Profit',
            formula: '[revenue] - [cost]',
            format: 'currency',
          },
          {
            field: 'rev_pct',
            label: '% Total Revenue',
            agg: 'sum',
            windowFn: 'percent_of_grand_total',
            format: 'percent',
          },
        ]}
        showRowTotals={true}
        showColTotals={true}
        defaultTheme="flash" // 'dark' | 'light' | 'bloomberg_black' | 'flash' | 'material' | ...
      />
    </div>
  );
}
```

---

### 2. Scalable Server-Side Pushdown Mode

For datasets with millions or billions of rows, connect to any backend implementing the standard V1 runtime protocol:

```tsx
import React from 'react';
import {
  PivotWorkspace,
  HttpPivotTransport,
  LatestViewportScheduler,
} from 'dash_tanstack_pivot/react';

// Wraps HTTP requests with auto-cancellation (latest-viewport-wins)
const transport = new LatestViewportScheduler(
  new HttpPivotTransport({
    endpoint: '/v1/runtime',
  })
);

export function EnterpriseAnalytics() {
  return (
    <div style={{ height: '100vh', width: '100%' }}>
      <PivotWorkspace
        table="warehouse_orders"
        transport={transport}
        rowFields={['department', 'category', 'product']}
        colFields={['quarter']}
        valConfigs={[
          { field: 'sales_amount', agg: 'sum', format: 'currency' },
          { field: 'order_count', agg: 'count' },
        ]}
        showRowTotals={true}
        showColTotals={true}
        defaultTheme="dark"
      />
    </div>
  );
}
```

---

## ✨ Key Enterprise Capabilities

| Feature | Description |
| :--- | :--- |
| **Dual Execution Engine** | Seamlessly toggle between 100% in-browser WebAssembly execution (`ClientPivotTransport`) and server-side warehouse pushdown (`HttpPivotTransport`). |
| **2D Virtualization** | Constant < 80 MB browser memory whether scrolling through 10,000 rows or 1,000,000,000+ rows with 1,000+ dynamic pivoted columns. |
| **$\mathcal{O}(D)$ Level-Based Hierarchy** | Multi-level tree group expansions execute one single batched query per hierarchy depth level ($D$) rather than sequential $N$-query waterfalls. |
| **Visual Analytics & Charting** | Integrated **ECharts 6** & **ECharts-GL 3D**: 23+ chart types across 11 families (Bar, Line, Area, Pie, Radar, Scatter, Waterfall, 3D Bar/Surface) linked to live pivot state. |
| **In-Cell Sparklines & Data Bars** | High-performance inline SVG sparklines (Line, Area, Bar) and proportional cell data bars rendered directly inside grid cells with zero DOM bloat. |
| **Excel AST Formulas & Window Functions** | Excel-like calculated measures using `[Field]` syntax and built-in pivot window functions (`percent_of_row`, `percent_of_col`, `percent_of_grand_total`). |
| **4 Layout Modes** | Multidimensional Pivot (`pivot`), Parent-Child Tree (`tree`), Flat Record Grid (`table`), and Financial P&L Report Mode (`report`). |
| **Client SheetJS & Clipboard** | Instant client-side Excel XLSX and CSV streaming export with merged headers and formatting preserved. RFC-compliant HTML/TSV clipboard copy. |
| **11 Enterprise Themes** | Complete theme system: `light`, `dark`, `flash`, `material`, `balham`, `bloomberg`, `bloomberg_black`, `strata`, `crystal`, `alabaster`, `satin` with full `--sp-*` CSS variables. |

---

## 🌐 Multi-Framework Ecosystem

`dash_tanstack_pivot` is part of the `serverside-pivot` ecosystem:
- **Standalone React**: `dash_tanstack_pivot` (this package)
- **Python Plotly Dash**: `pip install serverside-pivot`
- **Streamlit**: `pip install "serverside-pivot[streamlit]"`
- **Backend Pushdown**: Powered by **DuckDB**, **PyArrow**, and **Ibis** (Snowflake, BigQuery, Postgres, ClickHouse, MotherDuck, Databricks, Trino)

For comprehensive documentation, live Python/Dash showcases, and interactive examples, visit the **[GitHub Repository](https://github.com/Ramzy22/serverside_pivot)**.

---

## 📄 License

Distributed under the **MIT License**.
