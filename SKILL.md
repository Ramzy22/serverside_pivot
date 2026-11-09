---
name: serverside-pivot
description: Complete, exhaustive architectural, configuration, and API manual for AI agents to install, configure, build, and deploy serverside-pivot across Python Dash, Standalone React, and Streamlit with DuckDB, PyArrow, and Ibis cloud data warehouses (Snowflake, BigQuery, Postgres, ClickHouse, MotherDuck, Databricks, Trino). Covers view/report saving, async streaming exports, pushdown capabilities, CDC streaming, 2D virtualized analytics, and client-side in-memory mode.
---

# `serverside-pivot`: The Master AI Agent Implementation & Technical Reference

`serverside-pivot` is an enterprise-grade, turnkey, low-code analytical pivot engine and virtualized grid for **Python (Plotly Dash, Streamlit, FastAPI)** and **Standalone React**.

It offers a **dual-engine architecture**:
1. **Server-Side Mode (`serverSide=True`)**: Connects directly to DuckDB or cloud data warehouses via Ibis (Snowflake, BigQuery, Postgres, ClickHouse, MotherDuck, etc.). Compiles UI groupings and group expansions into sub-second pushdown SQL with $\mathcal{O}(D)$ level-based batching. The browser operates on a 50-row virtual window with sparse horizontal column virtualization, bounding browser RAM to **< 80 MB** across 10,000 to 1,000,000,000+ rows.
2. **Client-Side Mode (`serverSide=False`)**: Operates entirely in-browser over in-memory JSON records (`data` prop). Requires **zero backend database connections, zero Python callback registrations, and zero HTTP servers**. Features client-side multi-condition filtering, Excel-style pagination (`PaginationBar`), browser-side sorting, client-side SheetJS XLSX/CSV exports, and client-side ECharts analytics.

---

## 📑 Comprehensive Index

1. [Dual Execution Architecture: Server-Side vs. Client-Side](#1-dual-execution-architecture-server-side-vs-client-side)
2. [Client-Side Execution Modes (In-Memory & DuckDB-Wasm)](#2-client-side-execution-modes-in-memory--duckdb-wasm)
3. [Architecture & One-Way Dependency Flow](#3-architecture--one-way-dependency-flow)
4. [Runtime Wire Protocol V1 & Request Lifecycle](#4-runtime-wire-protocol-v1--request-lifecycle)
5. [Cache Coordinator & 12 Cache Namespaces](#5-cache-coordinator--12-cache-namespaces)
6. [Resource Governor & Admission Control](#6-resource-governor--admission-control)
7. [Universal Cloud Data Warehouses & Ibis Federation](#7-universal-cloud-data-warehouses--ibis-federation)
8. [View Modes & Master-Detail Inspection Surfaces](#8-view-modes--master-detail-inspection-surfaces)
9. [Tree Mode Architecture (`treeConfig`)](#9-tree-mode-architecture-treeconfig)
10. [Report Mode & Financial P&L Formatting (`reportDef`)](#10-report-mode--financial-pl-formatting-reportdef)
11. [Saving Views, Reports & State Persistence](#11-saving-views-reports--state-persistence)
12. [Column Layout Contract & 2D Virtualization](#12-column-layout-contract--2d-virtualization)
13. [Enterprise Streaming Exports & Clipboard Serialization](#13-enterprise-streaming-exports--clipboard-serialization)
14. [Change Data Capture (CDC) & Exact Streaming Operations](#14-change-data-capture-cdc--exact-streaming-operations)
15. [Distributed Multi-Worker Runtime (Redis Topology)](#15-distributed-multi-worker-runtime-redis-topology)
16. [AST Formula Engine & Safe Expression Parser](#16-ast-formula-engine--safe-expression-parser)
17. [Visual Analytics & Charting Suite (11 Families, 23+ Types)](#17-visual-analytics--charting-suite-11-families-23-types)
18. [Security, Row-Level Security (RLS) & Mutation Governance](#18-security-row-level-security-rls--mutation-governance)
19. [Host 1: Plotly Dash Integration (Server-Side & Client-Side)](#19-host-1-plotly-dash-integration-server-side--client-side)
20. [Host 2: Standalone React Host (Server-Side & Client-Side)](#20-host-2-standalone-react-host-server-side--client-side)
21. [Host 3: Streamlit Integration (Server-Side & Client-Side)](#21-host-3-streamlit-integration-server-side--client-side)
22. [Host 4: Headless FastAPI Runtime Microservice](#22-host-4-headless-fastapi-runtime-microservice)
23. [Verification, Benchmarks & Showcase Demos](#23-verification-benchmarks--showcase-demos)
24. [Troubleshooting & AI Agent Playbook](#24-troubleshooting--ai-agent-playbook)

---

## 1. Dual Execution Architecture: Server-Side vs. Client-Side

| Architectural Dimension | Server-Side Mode (`serverSide=True`) | Client-Side Mode (`serverSide=False`) |
| :--- | :--- | :--- |
| **Dataset Scale** | 100,000 to 1,000,000,000+ rows (Unlimited) | 1 to 50,000 rows (Bounded by client JS heap) |
| **Data Ingestion Prop** | `table="sales"` (registered in DuckDB/Ibis) | `data=[{"region": "North", ...}]` (raw records array) |
| **Backend Requirement** | Required (`register_pivot_app` or `/v1/runtime`) | **None** (100% self-contained in browser) |
| **Database Engine** | DuckDB, Snowflake, BigQuery, Postgres, Ibis | In-browser JavaScript engine |
| **Hierarchy Grouping** | $\mathcal{O}(D)$ level-based vectorized SQL batching | In-browser TanStack grouping / tree recursion |
| **Row Model** | Server-side block-cached virtual window (50 rows) | In-memory row model with optional `PaginationBar` |
| **Pagination** | Continuous 2D virtual scroll windowing | Built-in `<PaginationBar />` (10, 25, 50, 100, 200/page) |
| **Filter Evaluation** | Compiled SQL `WHERE` clauses pushdown | In-memory `useFilteredData` evaluation |
| **Sorting** | SQL `ORDER BY` with null handling & tenor regex | Client-side comparator `customSortingFn` |
| **Export Execution** | Async streaming worker (`PivotExportExecutor`) | Instant client-side SheetJS (`exportUtils.js`) |
| **Browser Memory Footprint** | Strictly bounded (< 80 MB RAM) | Proportional to loaded JSON records size |

---

## 2. Client-Side Execution Modes (In-Memory & DuckDB-Wasm)

`serverside-pivot` provides state-of-the-art client-side execution capabilities, ranging from lightweight in-memory filtering and Excel-style pagination up to full in-browser SQL execution via **DuckDB-Wasm**. This eliminates server roundtrips, callback latency, and backend database infrastructure for datasets up to 50,000+ rows.

### 2.1 Hybrid Execution Mode Resolution (`executionMode`)

The pivot component automatically resolves the execution environment or allows explicit overrides via `performanceConfig` or the `configure_pivot_execution()` helper:

```python
from pivot_engine import configure_pivot_execution

exec_config = configure_pivot_execution(
    data=records,                 # In-memory JSON records (list of dicts)
    table="saas_revenue",         # Registered table name (optional in pure client mode)
    adapter=adapter,              # Server-side TanStackPivotAdapter (optional)
    execution_mode="auto",        # "auto" | "client" | "server"
    client_engine="auto",         # "auto" | "duckdb-wasm" | "memory"
    client_row_threshold=50_000,  # Automatic rollover threshold (default: 50,000)
    duckdb_wasm_threshold=1_000,  # Threshold to activate DuckDB-Wasm (default: 1,000)
)
```

| `executionMode` | Trigger / Behavior | Active Transport | Roundtrips |
| :--- | :--- | :--- | :--- |
| **`"auto"`** *(Default)* | If records $\le$ `clientRowThreshold` or `dataUrl` is provided without a server table $\rightarrow$ **Client**. If records $>$ `clientRowThreshold` or backed solely by warehouse $\rightarrow$ **Server**. | Smart hybrid delegation | **0** (Client) or 1/action (Server) |
| **`"client"`** | Explicitly forces client execution in browser memory or WebAssembly worker regardless of table definition. | `ClientPivotTransport` | **0 (Zero)** |
| **`"server"`** | Explicitly forces server-side execution via Python `TanStackPivotAdapter` and Ibis/DuckDB physical planner. | `HttpPivotTransport` / Dash callback | 1 per viewport/action |

---

### 2.2 Tiered Client Execution Engines (`clientEngine`)

When running on the client, the component supports tiered engines tailored to dataset scale:

* **`"auto"`** *(Default)*: Automatically selects `"duckdb-wasm"` when dataset has $\ge$ `duckdb_wasm_threshold` (default: 1,000 rows); falls back to `"memory"` for smaller collections (< 1,000 rows).
* **`"duckdb-wasm"`**: Launches in-browser DuckDB compiled to WebAssembly inside a dedicated Web Worker thread.
* **`"memory"`**: Pure JavaScript in-memory grouping, filtering, and TanStack tree recursion.

---

### 2.3 Browser-Side DuckDB-Wasm Architecture (`ClientPivotTransport`)

For commercial-grade client execution with zero network latency, the browser runtime includes:
1. **`DuckDbWasmExecutor`**: Instantiates DuckDB-Wasm in a background Web Worker, registering the raw dataset as an in-memory Arrow/Parquet virtual table.
2. **`ClientSqlPlanner`**: Replicates the exact SQL generation pipeline of the server-side `IbisPlanner` directly in the browser (grouping sets, rollups, window functions, sorting, multi-condition filters).
3. **`ClientResponsePostProcessor`**: Packages query outputs into the exact same V1 wire contract envelope (`{ rows, totalRows, schema, meta }`), providing **100% mathematical and schema parity** with the Python backend.

#### Operations Performed With Zero Server Roundtrips:
* Multi-level hierarchy expansion & collapse ($\mathcal{O}(D)$ level-batched SQL in WASM).
* Multi-column sorting with tenor regex and null preservation.
* Multi-field column pivoting (`colFields`) with dynamic column generation.
* Window functions (`percent_of_total`, running sums, rank).
* Calculated AST formulas and custom dimension bucket rules.
* Linked visual analytics (ECharts rendered directly from WASM query results).
* Master-detail inspection and drill-through.
* Client-side SheetJS XLSX & CSV exports.

---

### 2.4 In-Memory Multi-Condition Filtering (`useFilteredData`)
Evaluates conditions entirely inside the browser across text, numeric, and set operators:
* **Text**: `contains`, `startsWith`, `endsWith`, `eq` / `equals`, `ne` / `notEquals` (with optional `caseSensitive: true/false`).
* **Numeric**: `gt` ($>$), `lt` ($<$), `gte` ($\ge$), `lte` ($\le$), `between` (inclusive range `value` and `value2`).
* **Set / Inclusion**: `in` (matches array of selected checklist items).
* **Combinators**: Multi-condition filters combined via `AND` or `OR`.
* **Dynamic Filter Checklists**: Filter popover checklists auto-extract unique values and frequencies from `data` in memory (`buildClientFilterOptionMap`).

---

### 2.5 Built-in Excel-Style Pagination (`paginationConfig`)
When `serverSide=False` and `paginationConfig={"enabled": True, "pageSize": 25}`:
* Mounts the native `<PaginationBar />` component at the bottom of the grid.
* Provides a rows-per-page dropdown: `[10, 25, 50, 100, 200]`.
* Displays current record range: `Showing 1-25 of 1,420 rows`.
* First, Previous, Next, and Last navigation buttons with direct page number selection.

---

### 2.6 Client-Side SheetJS Exports
* Full Excel XLSX and CSV export generated **100% in browser memory** using SheetJS and FileSaver (`exportUtils.js`).
* No server roundtrip or temporary file generation required.
* Numerical types, dates, currency labels, and merged column headers are preserved.

---

### 2.7 Client-Side Custom Dimensions (`applyCustomDimensionsToRows`)
Transform rows on-the-fly in browser memory without modifying the original Python data:
```python
customDimensions = [
    {
        "name": "Market Tier",
        "sourceField": "revenue",
        "type": "range",
        "rules": [
            {"label": "Tier 1", "min": 100000},
            {"label": "Tier 2", "min": 50000, "max": 100000},
            {"label": "Tier 3", "max": 50000},
        ]
    }
]
```

---

## 3. Architecture & One-Way Dependency Flow

The package follows strict, non-circular one-way dependency boundaries enforced by automated architectural tests:

### Backend Dependency Flow
```text
Dash Callback Transport / REST Gateway
  └──> Runtime Lifecycle & Router (PivotRuntimeService, RuntimeRequestCoordinator)
        └──> Request Handlers (data, export, report, tree, detail, charts)
              └──> TanStack Transport Façade (TanStackPivotAdapter)
                    └──> Query / Hierarchy / Editing Services
                          └──> Planner / Backend Adapters / CacheCoordinator
                                └──> DuckDB / Ibis / Arrow Physical Engines
```

### Frontend Dependency Flow
```text
Dash Metadata Bridge (DashTanstackPivot.react.tsx)
  └──> TypeScript Composition Root (PivotWorkspace.tsx)
        └──> Capability Controllers (useServerSideRowModel, useColumnVirtualizer)
              └──> Pure Models, Reducers, Selectors, and Browser DOM Adapters
```

* Lower layers **never** import runtime, UI, or transport modules.
* Public facades (`ScalablePivotController`, `TanStackPivotAdapter`) contain construction and delegation only; no duplicate business logic.

---

## 4. Runtime Wire Protocol V1 & Request Lifecycle

All presentation hosts (Dash, React, Streamlit) communicate with the analytical core through **Pivot Runtime Protocol V1** (`schemaVersion: 1`).

### The Request Lifecycle Envelope
Every request carries strict structural and sequencing coordinates:
* `requestId`: Unique client-generated correlation ID.
* `sessionId`: Browser tab/session identity.
* `clientInstance`: Component ID (prevents collision when multiple pivot grids share one session).
* `stateEpoch`: Monotonic integer incremented on structural changes (fields, groups, filters, formulas).
* `windowSeq`: Viewport scroll sequence inside the active epoch.
* `abortGeneration`: Hard-reset generation counter.
* `cacheKey`: Client row-cache identifier.
* `lifecycleLane`: Server cancellation lane:
  * `data`: Viewport and structural data requests (subject to latest-wins supersession).
  * `chart`: Chart compute requests.
  * `None`: Exports, drill-through, mutations (never cancelled by scrolling).

### In-Process Request Cancellation Outcomes
* `not_cancelled`: Executed to completion.
* `stale_registration_rejected`: Rejected at `SessionRequestGate` before hitting the database because a newer request was already queued.
* `superseded_cancelled`: Cancelled in-flight when a newer scroll request arrived in the same lane. Triggers native DuckDB `cursor.interrupt()`. Returns `status="stale"` (not an error).
* `stale_response_dropped`: Query finished, but was superseded while running; response dropped to prevent UI flicker.

---

## 5. Cache Coordinator & 12 Cache Namespaces

The `CacheCoordinator` (`pivot_engine.cache_coordinator`) orchestrates cache ownership, memory accounting, and invalidation across 12 distinct namespaces:

| Namespace | Owner | Key Dimensions |
| :--- | :--- | :--- |
| `runtime.payload` | Runtime Payload Store | `payloadToken`, `contentType`, `requestId`, `stateEpoch` |
| `controller.query` | Scalable Controller | `table`, `compiledSql`, `pivotSpec`, `deltaEpoch` |
| `controller.hierarchy_view` | Controller Hierarchy Cache | `table`, `specFingerprint`, `expandedPaths`, `rowWindow` |
| `controller.hierarchy_root_count` | Controller Hierarchy Cache | `table`, `rows`, `filters` |
| `controller.hierarchy_root_page` | Controller Hierarchy Cache | `table`, `rootCountKey`, `sort`, `rowWindow` |
| `controller.hierarchy_grand_total` | Controller Hierarchy Cache | `table`, `specFingerprint`, `filters`, `measures`, `customDimensions` |
| `adapter.center_columns` | TanStack Adapter Viewport Cache | `table`, `requestStructure`, `columnWindow`, `cacheGeneration` |
| `adapter.pivot_catalog` | TanStack Adapter Viewport Cache | `table`, `columns`, `filters`, `customDimensions`, `columnSortOptions`, `cacheGeneration` |
| `adapter.response_window` | TanStack Adapter Viewport Cache | `requestStructure`, `expandedPaths`, `rowWindow`, `columnWindow`, `grandTotal`, `cacheGeneration` |
| `adapter.row_block` | TanStack Adapter Viewport Cache | `requestStructure`, `expandedPaths`, `blockIndex`, `columnWindow`, `cacheGeneration` |
| `adapter.grand_total` | TanStack Adapter Viewport Cache | `requestStructure`, `expandedPaths`, `columnWindow`, `cacheGeneration` |
| `adapter.prefetch_pending` | TanStack Adapter Viewport Cache | `responseWindowKey` |

---

## 6. Resource Governor & Admission Control

The `ResourceGovernor` (`pivot_engine.resource_governor`) provides fail-fast, deterministic admission control to prevent worker out-of-memory crashes.

### Workload Accounts & Accounting Identity
Every query, export, and download is charged to an immutable `WorkloadAccount`:
* `SYSTEM_ACCOUNT`: Default bucket for unauthenticated or system queries.
* `WorkloadAccount.tenant("tenant_id")`: Resolved from authenticated principal attributes (`attributes.accounting_tenant`).

### Resource Budgeting Limits (`ResourceLimits`)
Startup validates that the memory envelope satisfies:
$$\text{DuckDB Limit} + \text{Application Resource Budget} + \text{Native Headroom} \le \text{Worker Hard Limit}$$

```env
# Hard worker ceiling (process RSS ceiling)
PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES=2147483648       # 2 GiB default
PIVOT_RESOURCE_MAX_BYTES=1073741824                   # 1 GiB total application budget
PIVOT_RESOURCE_HEADROOM_BYTES=268435456               # 256 MiB reserve headroom
PIVOT_DUCKDB_MEMORY_LIMIT="512MB"                     # DuckDB internal memory ceiling
PIVOT_NATIVE_HEADROOM_BYTES=268435456                 # 256 MiB Arrow/Python native buffer headroom

# Concurrency Slots (Worker & Tenant Fairness)
PIVOT_MAX_CONCURRENT_QUERIES=10
PIVOT_MAX_CONCURRENT_EXPORTS=2
PIVOT_MAX_CONCURRENT_DOWNLOADS=4
PIVOT_TENANT_MAX_BYTES=268435456                      # 256 MiB max per tenant
PIVOT_TENANT_MAX_CONCURRENT_QUERIES=4                 # 4 queries max per tenant
PIVOT_TENANT_MAX_CONCURRENT_EXPORTS=1                 # 1 export max per tenant
PIVOT_SYSTEM_MAX_BYTES=67108864                       # 64 MiB max for unauthenticated callers
PIVOT_SYSTEM_MAX_CONCURRENT_QUERIES=2
```

---

## 7. Universal Cloud Data Warehouses & Ibis Federation

`serverside-pivot` connects to external databases via **Ibis**, compiling analytical operations directly to native warehouse SQL.

### Connection Matrix & Capabilities

| Backend | URI / Dialect | Cloud/Remote | Shared Lock | Native ILIKE | RegEx Tenor Sort | Param Style |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Snowflake** | `snowflake://` | Yes | No (Pooled) | Yes | Yes | `pyformat` (`%s`) |
| **Google BigQuery** | `bigquery://` | Yes | No (Pooled) | Fallback (`LOWER(c) LIKE`) | Yes | `qmark` (`?`) |
| **PostgreSQL / Supabase**| `postgresql://` | Yes | No (Pooled) | Yes | Yes | `pyformat` (`%s`) |
| **MotherDuck** | `md:` or `md://` | Yes | Yes (Locked)| Yes | Yes | `qmark` (`?`) |
| **ClickHouse** | `clickhouse://` | Yes | No (Pooled) | Yes | Yes | `pyformat` (`%s`) |
| **Databricks** | `databricks://` | Yes | No (Pooled) | Yes | Yes | `qmark` (`?`) |
| **Trino / Starburst** | `trino://` | Yes | No (Pooled) | Yes | Yes | `qmark` (`?`) |
| **MySQL / MariaDB** | `mysql://` | Yes | No (Pooled) | Fallback (`LIKE`) | Yes | `pyformat` (`%s`) |
| **SQLite** | `sqlite:///` | No (Local) | Yes (Locked)| Fallback (`LIKE`) | Fallback | `qmark` (`?`) |
| **DuckDB** | `duckdb:///` or `:memory:` | No (Local) | Yes (Locked)| Yes | Yes | `qmark` (`?`) |

---

## 8. View Modes & Master-Detail Inspection Surfaces

### 4 First-Class View Modes (`viewMode`)

1. **`pivot`**: Multidimensional cross-tabulation with row groupings, pivoted columns, visual totals, and calculated formulas.
2. **`tree`**: Hierarchical records grid for parent-child data structures.
3. **`table`**: Flat, virtualized record grid for high-speed record scanning.
4. **`report`**: Executive P&L / balance-sheet mode with period axes, custom subtotals, and Top-N member ranking.

### 4 Master-Detail Surfaces (`detailMode`)

| `detailMode` | UI Manifestation | Use Case |
| :--- | :--- | :--- |
| `'none'` | Detail inspection disabled. | Standard tabular / pivot reporting. |
| `'inline'` | Slides open an embedded sub-grid or record card directly under the clicked row (`InlineDetailPanel`). | Fast row-level detail preview. |
| `'sidepanel'` | Opens a persistent right-hand drawer alongside the table (`DetailSidePanel`). | Deep metadata inspection without moving table rows. |
| `'drawer'` | Slides up an interactive bottom drawer (`DetailDrawer`). | Detailed time-series or secondary record grids. |
| **Drill Modal** | Launches a full-screen raw record modal (`DrillThroughModal` via `drillEndpoint="/api/drill-through"`). | Raw transaction audit / compliance inspection. |

---

## 9. Tree Mode Architecture (`treeConfig`)

When `viewMode="tree"`, configure tree structures using `treeConfig`:

```python
treeConfig = {
    "sourceType": "adjacency",           # 'adjacency' (id/parentId) | 'path' | 'nested'
    "idField": "employee_id",
    "parentIdField": "manager_id",
    "displayMode": "singleColumn",       # 'singleColumn' | 'multipleColumns'
    "groupDefaultExpanded": 1,          # -1 = all, 0 = root only, N = down to level N
    "defaultOpenPaths": ["101", "101|||204"],
    "levelLabels": ["Division", "Department", "Team", "Contributor"],
    "hasChildrenField": "has_subordinates",
}
```

---

## 10. Report Mode & Financial P&L Formatting (`reportDef`)

Report mode turns `serverside-pivot` into an executive financial scorecard or P&L engine:

```python
reportDef = {
    "root": {
        "field": "line_item",
        "sortBy": "amount_sum",
        "sortDir": "desc",
        "topN": 10,                 # Display Top 10 line items
        "otherBucket": True,        # Automatically bundle remaining items into "Other"
    },
    "formats": {
        "Gross Profit": {
            "bold": True,
            "showSubtotal": True,
            "rowColor": "#1e293b",
            "borderStyle": "solid",
            "borderWidth": 2,
            "borderColor": "#3b82f6",
            "numberFormat": "currency",
            "indent": 0
        }
    }
}
```

---

## 11. Saving Views, Reports & State Persistence

### Capturing and Restoring Views in Dash Callbacks

```python
from dash import Input, Output, callback

@callback(
    Output("save-notification", "children"),
    Input("pivot-grid", "savedView"),
    prevent_initial_call=True
)
def save_user_layout(saved_view):
    if not saved_view:
        return ""
    view_name = saved_view.get("name")
    view_state = saved_view.get("state")
    # Save to your database: db.save_view(user_id=current_user.id, name=view_name, state=view_state)
    return f"Saved view '{view_name}' successfully."
```

---

## 12. Column Layout Contract & 2D Virtualization

* **SHA-256 Layout IDs**: A typed pivot path and measure produce an immutable layout ID. Reordering columns or toggling metrics maintains widths and pinning across revisions.
* **Off-Window Pinning**: Center columns are virtualized using `useColumnVirtualizer.js`. Pinned left and right columns are rendered in isolated sticky DOM containers. The backend unions pinned IDs with scrolling window IDs with zero duplicate cells.

---

## 13. Enterprise Streaming Exports & Clipboard Serialization

* **Streaming Engine (`PivotExportExecutor`)**: Streamed chunk-by-chunk for CSV and XLSX without full-table materialization.
* **SheetJS Client Fallback**: Instant local exports for client-side mode or offline views.
* **RFC-Compliant Clipboard**: `Ctrl+C` / `Cmd+C` copies dual MIME payloads (`text/plain` TSV and `text/html`) into Excel/Sheets with preserved formulas and numerical formatting.

---

## 14. Change Data Capture (CDC) & Exact Streaming Operations

* **Logarithmic Accumulators**: `sum`, `count`, `avg`, `min`, `max` (using counted min/max heaps).
* **Atomic Bootstrapping**: CDC jobs start with an atomic `(source_rows, source_checkpoints)` snapshot. Missing sequence numbers fail closed (`status: "invalidated"`).
* **State Stores**: `MemoryCDCStateStore` (in-memory) and `RedisCDCStateStore` (distributed multi-worker).

---

## 15. Distributed Multi-Worker Runtime (Redis Topology)

```env
PIVOT_DISTRIBUTED_RUNTIME=1
PIVOT_RUNTIME_REDIS_URL=rediss://user:secret@redis-cluster.internal:6380/0
PIVOT_RUNTIME_REDIS_TLS_VERIFY=required
PIVOT_RUNTIME_REDIS_PREFIX=prod:pivot
PIVOT_RUNTIME_PAYLOAD_TTL_SECONDS=120
PIVOT_RUNTIME_PAYLOAD_MAX_BYTES=134217728
PIVOT_DASH_SESSION_SECRET=at-least-32-high-entropy-random-bytes
```

---

## 16. AST Formula Engine & Safe Expression Parser

```python
valConfigs = [
    {"field": "revenue", "agg": "sum", "label": "Revenue ($)"},
    {"field": "cogs", "agg": "sum", "label": "COGS ($)"},
    {
        "field": "gross_profit",
        "label": "Gross Profit ($)",
        "formula": "[revenue] - [cogs]",
        "formulaScope": "measures",  # 'measures' | 'columns'
        "format": "currency"
    },
    {
        "field": "markup",
        "label": "Markup Ratio",
        "formula": "round(([revenue] - [cogs]) / [cogs], 4)",
        "format": "percent"
    }
]
```

---

## 17. Visual Analytics & Charting Suite (11 Families, 23+ Types)

11 Families, 23+ Types:
* **Bar**: `bar` (Grouped / Stacked, Vertical / Horizontal)
* **Line / Area**: `line`, `area`, `sparkline`
* **Pie / Donut**: `pie`, `donut`
* **Scatter**: `scatter`, `bubble`
* **Combo**: `combo` (Multi-layer combination)
* **Waterfall**: `waterfall`, `range`
* **Statistical**: `histogram`, `boxplot`, `funnel`
* **Radial**: `radar`, `nightingale` (Rose chart)
* **Heatmap**: `heatmap`
* **3D (WebGL)**: `bar3d`, `line3d`, `scatter3d` (via `pivot-echarts-3d`)
* **Hierarchical**: `icicle`, `sunburst`, `sankey` (integrated with pivot tree)

---

## 18. Security, Row-Level Security (RLS) & Mutation Governance

```python
from pivot_engine import RuntimeDatasetAuthorizer
from pivot_engine.runtime import RuntimeMutationAuthorizer

dataset_policy = RuntimeDatasetAuthorizer({
    "sales_records": {
        "roles": {"viewer", "analyst", "admin"},
        "operations": {"read", "export"},
        "rowSecurity": {
            "mode": "claims",
            "claims": {"tenant_id": "organization_id"}
        }
    }
})

mutation_policy = RuntimeMutationAuthorizer({
    "sales_records": {
        "roles": {"admin", "editor"},
        "columns": {"price", "discount", "notes"},
        "operations": {"update", "upsert", "history"}
    }
})
```

---

## 19. Host 1: Plotly Dash Integration (Server-Side & Client-Side)

### Example A: Server-Side Mode (`serverSide=True`)
*Use for large datasets (100k+ rows) with DuckDB/Ibis pushdown.*

```python
import pyarrow as pa
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

data = pa.Table.from_pydict({
    "region": ["EMEA", "APAC", "AMER"] * 10000,
    "product": ["SaaS", "Hardware", "Services"] * 10000,
    "quarter": ["Q1", "Q2", "Q3"] * 10000,
    "revenue": [120000.0, 85000.0, 95000.0] * 10000,
})

adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.controller.load_data_from_arrow("financials", data)

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="dash-pivot-server",
        table="financials",
        serverSide=True,
        rowFields=["region", "product"],
        colFields=["quarter"],
        valConfigs=[{"field": "revenue", "agg": "sum", "format": "currency", "label": "Revenue ($)"}],
        showRowTotals=True,
        showColTotals=True,
        defaultTheme="bloomberg",
        style={"height": "750px", "width": "100%"},
    )
])

# Wires complete runtime transport in ONE call:
register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="dash-pivot-server")

if __name__ == "__main__":
    app.run(debug=True, port=8050)
```

### Example B: Client-Side In-Memory Mode (`serverSide=False`)
*Use for small-to-medium datasets (< 50k rows). Zero backend database or callback wiring required!*

```python
import pandas as pd
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot

# Direct Pandas DataFrame or list of dicts
df = pd.DataFrame({
    "department": ["Engineering", "Engineering", "Sales", "Sales", "Marketing", "Marketing"] * 50,
    "role": ["Backend", "Frontend", "Enterprise", "SMB", "Growth", "Brand"] * 50,
    "level": ["Senior", "Junior", "Senior", "Junior", "Senior", "Junior"] * 50,
    "headcount": [12, 8, 15, 20, 6, 4] * 50,
    "salary": [140000, 95000, 130000, 85000, 115000, 75000] * 50,
})

app = Dash(__name__)
app.layout = html.Div([
    html.H2("Workforce Analytics (Client-Side Mode)"),
    DashTanstackPivot(
        id="dash-pivot-client",
        data=df.to_dict("records"),      # Direct in-memory JSON records
        serverSide=False,                 # Client-side mode!
        rowFields=["department", "role"],
        colFields=["level"],
        valConfigs=[
            {"field": "headcount", "agg": "sum", "label": "Total Headcount"},
            {"field": "salary", "agg": "avg", "format": "currency", "label": "Avg Salary ($)"},
        ],
        paginationConfig={"enabled": True, "pageSize": 25}, # Native client-side pagination bar
        showRowTotals=True,
        showColTotals=True,
        defaultTheme="flash",
        style={"height": "600px", "width": "100%"},
    )
])

# NOTICE: No register_pivot_app() call needed! Everything executes in the browser.

if __name__ == "__main__":
    app.run(debug=True, port=8051)
```

### Example C: Client-Side DuckDB-Wasm & Hybrid Auto-Detection Mode
*Showcases dynamic auto-detection between browser-local DuckDB-Wasm and server-side Ibis execution using `configure_pivot_execution()`.*

```python
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import (
    create_tanstack_adapter,
    configure_pivot_execution,
    register_pivot_app,
)

records = [
    {"region": "North", "rep": "Alice", "quarter": "Q1", "revenue": 150000, "margin": 0.28},
    {"region": "North", "rep": "Bob", "quarter": "Q1", "revenue": 95000, "margin": 0.22},
    {"region": "South", "rep": "Charlie", "quarter": "Q1", "revenue": 180000, "margin": 0.31},
    # ... up to 50k+ rows
]

adapter = create_tanstack_adapter(backend_uri=":memory:")

# Automatic hybrid configuration:
# <= 50,000 rows resolves to browser-local DuckDB-Wasm (0 server callbacks)
# > 50,000 rows automatically shifts to server-side physical Ibis execution
exec_cfg = configure_pivot_execution(
    data=records,
    table="saas_sales",
    adapter=adapter,
    execution_mode="auto",         # "auto" | "client" | "server"
    client_engine="auto",          # "auto" | "duckdb-wasm" | "memory"
    client_row_threshold=50_000,
    duckdb_wasm_threshold=1_000,
)

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="hybrid-pivot",
        table="saas_sales",
        data=exec_cfg["data"],
        serverSide=exec_cfg["serverSide"],
        performanceConfig=exec_cfg["performanceConfig"],
        rowFields=["region", "rep"],
        colFields=["quarter"],
        valConfigs=[
            {"field": "revenue", "agg": "sum", "format": "currency", "label": "Revenue ($)"},
            {"field": "margin", "agg": "avg", "format": "percent", "label": "Avg Margin"},
        ],
        showRowTotals=True,
        showColTotals=True,
        defaultTheme="flash",
        style={"height": "750px", "width": "100%"},
    )
])

# Register fallback server adapter in case dataset grows beyond client_row_threshold
register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="hybrid-pivot")

if __name__ == "__main__":
    app.run(debug=True, port=8056)
```

---

## 20. Host 2: Standalone React Host (Server-Side & Client-Side)

### Example A: Server-Side Mode (`serverSide={true}`)

```tsx
import React from 'react';
import {
  PivotWorkspace,
  HttpPivotTransport,
  LatestViewportScheduler,
  type ValConfig,
} from 'dash_tanstack_pivot/react';

const transport = new LatestViewportScheduler(
  new HttpPivotTransport({ endpoint: 'http://localhost:8765/v1/runtime' })
);

export function ServerPivot() {
  return (
    <PivotWorkspace
      table="billion_fact_sales"
      serverSide={true}
      transport={transport}
      rowFields={['market', 'sector']}
      colFields={['year']}
      valConfigs={[{ field: 'volume', agg: 'sum', label: 'Volume' } satisfies ValConfig]}
      defaultTheme="bloomberg_black"
    />
  );
}
```

### Example B: Client-Side Mode (`serverSide={false}`)

```tsx
import React from 'react';
import { PivotWorkspace, type ValConfig } from 'dash_tanstack_pivot/react';

const localRecords = [
  { region: 'North', product: 'Laptop', quarter: 'Q1', sales: 120000 },
  { region: 'North', product: 'Phone', quarter: 'Q1', sales: 80000 },
  { region: 'South', product: 'Laptop', quarter: 'Q1', sales: 95000 },
  { region: 'South', product: 'Phone', quarter: 'Q1', sales: 60000 },
];

export function ClientPivot() {
  return (
    <PivotWorkspace
      data={localRecords}
      serverSide={false}
      rowFields={['region']}
      colFields={['quarter']}
      valConfigs={[{ field: 'sales', agg: 'sum', label: 'Sales ($)', format: 'currency' } satisfies ValConfig]}
      paginationConfig={{ enabled: true, pageSize: 25 }}
      defaultTheme="light"
      showRowTotals={true}
      showColTotals={true}
    />
  );
}
```

---

## 21. Host 3: Streamlit Integration (Server-Side & Client-Side)

### Example A: Server-Side Mode (`server_side=True`)

```python
import streamlit as st
from streamlit_pivot import pivot_workspace

st.set_page_config(layout="wide")
st.title("⚡ Enterprise Pivot Workspace (Server-Side)")

pivot_workspace(
    table="billion_transactions",
    runtime_endpoint="http://127.0.0.1:8765/v1/runtime",
    server_side=True,
    row_fields=["region", "sector"],
    col_fields=["year"],
    val_configs=[{"field": "revenue", "agg": "sum", "label": "Revenue ($)"}],
    default_theme="dark",
    height=700,
)
```

### Example B: Client-Side Mode (`server_side=False`)

```python
import streamlit as st
import pandas as pd
from streamlit_pivot import pivot_workspace

st.set_page_config(layout="wide")
st.title("Workforce Metrics (Client-Side In-Memory)")

df = pd.DataFrame({
    "department": ["Engineering", "Sales", "Support"] * 100,
    "location": ["Remote", "Office"] * 150,
    "headcount": [5, 12, 8] * 100,
})

pivot_workspace(
    table="local_table",
    server_side=False,
    data=df.to_dict("records"),
    row_fields=["department"],
    col_fields=["location"],
    val_configs=[{"field": "headcount", "agg": "sum", "label": "Headcount"}],
    pagination_config={"enabled": True, "pageSize": 20},
    default_theme="flash",
    height=600,
)
```

---

## 22. Host 4: Headless FastAPI Runtime Microservice

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pivot_engine import PivotRuntimeService, RuntimeDatasetAuthorizer, create_tanstack_adapter, register_runtime_http_transport

adapter = create_tanstack_adapter(backend_uri="duckdb:///analytics.db")
runtime_service = PivotRuntimeService(adapter_getter=lambda: adapter)

app = FastAPI(title="Analytical Pivot Engine Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

register_runtime_http_transport(
    app,
    lambda: runtime_service,
    dataset_authorizer=RuntimeDatasetAuthorizer({
        "sales": {"roles": {"viewer", "admin"}, "operations": {"read", "export"}, "rowSecurity": {"mode": "global"}}
    }),
    inline_payload_bytes=5_000_000,
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765)
```

---

## 23. Verification, Benchmarks & Showcase Demos

```powershell
# 📈 1. Trading Desk & Risk Portfolio (50k rows, VaR, DV01, PnL - Port 8051)
python examples/demo_trader.py

# 👥 2. People Analytics & Compensation (35k rows, Sparklines - Port 8052)
python examples/demo_hr.py

# 🚀 3. B2B SaaS Growth & Cohorts (40k rows, ARR, NRR, Docked Charts - Port 8053)
python examples/demo_saas.py

# 🎛️ 4. Unified Showcase Hub (Port 8054)
python examples/demo_showcase_hub.py

# ⚡ 5. Client-Side DuckDB-Wasm & Hybrid Auto-Detection Showcase (Port 8056)
python examples/demo_clientside_version.py

# ☁️ 6. Cloud Data Warehouse & Ibis Pushdown (Snowflake, BigQuery, Postgres)
python examples/demo_cloud_warehouse.py

# Run backend pytest suite
pytest

# In-process runtime benchmark (100k rows, cold/warm cache latency)
python scripts/benchmark_pivot_runtime.py --rows 100000 --repeat-runs 5

# Benchmark comparison vs. AG Grid Enterprise SSRM
python scripts/benchmark_vs_aggrid.py

# Run flagship certification suite (Selenium headless Chrome, 207 cases)
python scripts/certify_flagship.py
```

### Flagship Demo 5: Client-Side DuckDB-Wasm & Hybrid Showcase (`examples/demo_clientside_version.py`)
Run: `python examples/demo_clientside_version.py --port 8056 --rows 2500`

Demonstrates commercial-grade execution mode resolution with zero server roundtrips in client mode:
* **Dynamic Execution Mode Switcher**: Live dropdown to toggle between `auto` (auto-resolution), `client` (force DuckDB-Wasm/in-memory), and `server` (force Python Ibis/DuckDB backend).
* **Tiered Engine Selector**: Switch between `auto` (>=1,000 rows activates WASM), `duckdb-wasm`, and `memory`.
* **Configurable Auto Threshold**: Test dataset threshold boundary (e.g. 50k rows vs 1k rows) to observe seamless client-to-server rollover.
* **Live Telemetry Strip**: Displays real-time status of configured mode, resolved execution engine, active transport (`ClientPivotTransport` vs `TanStackPivotAdapter`), dataset row count, network roundtrips (**0 in client mode** vs 1 per action in server mode), and 100% mathematical parity guarantee.

---

## 24. Troubleshooting & AI Agent Playbook

### 1. Client-Side Mode vs. Server-Side Mode Confusion
* **Mistake**: Calling `register_pivot_app()` when using `serverSide=False`, or leaving `table` unregistered when `serverSide=True`.
* **Fix**:
  - In pure client-side mode: supply `data=[...]` directly to `DashTanstackPivot` and set `serverSide=False`. No `tanstack_adapter` or `register_pivot_app()` call is required.
  - In hybrid mode: use `configure_pivot_execution(data=..., table=..., adapter=...)` with `execution_mode="auto"`. Register the adapter via `register_pivot_app()` so that if the dataset scales beyond `client_row_threshold`, server execution activates seamlessly.

### 2. Client-Side Pagination Not Showing
* **Mistake**: Setting `paginationConfig={"enabled": True}` in server-side mode.
* **Fix**: `paginationConfig` only applies when `serverSide=False`. Server-side mode uses continuous 2D virtual scroll windowing. For client-side pagination, pass `serverSide=False` alongside `paginationConfig={"enabled": True, "pageSize": 25}`.

### 3. Blank Grid or Infinite "Loading..." in Dash (Server-Side)
* **Symptom**: Headers render, but data cells stay blank or show spinner.
* **Fix**: Verify that `register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="<id>")` was called **after** initializing `app.layout`.

### 4. Streamlit Rerunning on Scroll
* **Symptom**: Mouse scrolling triggers a full Streamlit Python script rerun.
* **Fix**: Do not supply `on_view_state_change` unless you specifically need to capture persistent view state in Streamlit. Viewport window requests go directly from browser to `/v1/runtime` without triggering script reruns.

### 5. DuckDB Memory Spilling & Out-of-Memory Errors
* **Symptom**: `Out of Memory Error: could not allocate...` on billion-row queries.
* **Fix**: Set `export PIVOT_DUCKDB_MEMORY_LIMIT="4GB"` or pass a file-backed database path: `create_tanstack_adapter(backend_uri="duckdb:///path/to/analytics.db")`. DuckDB will spill intermediate aggregates to disk.
