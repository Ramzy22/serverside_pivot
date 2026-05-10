# serverside-pivot — AI Agent Interface Guide

> **Purpose:** This file is the canonical entry point for any AI agent tasked with understanding, setting up, configuring, extending, or debugging the `serverside-pivot` project. Read this first; nothing else is required for 95 % of tasks.

---

## 1. What This Project Is

`serverside-pivot` is a full-stack pivot table component for [Plotly Dash](https://dash.plotly.com/). It consists of:

| Layer | Technology | Role |
|-------|-----------|------|
| **Python backend** | DuckDB + Ibis | Query execution, aggregation, caching |
| **React frontend** | TanStack Table v8 + ECharts | UI rendering, virtualization, charts |
| **Transport** | Single Dash callback | Bridges JS ↔ Python |

It is installed as two packages:
- `dash-tanstack-pivot` — the Dash component (React + auto-generated Python wrapper)
- `pivot-engine` — the Python aggregation engine (lives inside `dash_tanstack_pivot/pivot_engine/`)

---

## 2. Repo Layout (critical paths only)

```
serverside_pivot/
├── AGENT_INTERFACE.md          ← you are here
├── PIVOT_MASTER_DOCUMENTATION.md   ← full feature reference
├── CAPABILITIES.md             ← feature inventory
├── KNOWN_ISSUES.md             ← active bugs and gaps
├── example_app.py              ← quick-start (10 K rows, sparklines)
├── examples/
│   ├── example_dash_basic.py          ← minimal setup
│   ├── example_dash_hierarchical.py   ← nested row groups
│   └── example_dash_sql_multi_instance.py  ← SQL backend, multi-pivot
│
├── pivot_engine/               ← shim __init__.py (re-exports real package)
│
└── dash_tanstack_pivot/
    ├── pivot_engine/pivot_engine/   ← real Python engine
    │   ├── __init__.py              ← public API surface
    │   ├── tanstack_adapter.py      ← TanStackAdapter / create_tanstack_adapter
    │   ├── controller.py            ← PivotController
    │   ├── scalable_pivot_controller.py
    │   ├── dash_integration.py      ← register_pivot_app / register_dash_*
    │   ├── runtime/service.py       ← PivotRuntimeService
    │   ├── types/pivot_spec.py      ← PivotSpec dataclass
    │   ├── planner/ibis_planner.py  ← SQL query builder
    │   └── backends/
    │       ├── duckdb_backend.py
    │       └── ibis_backend.py
    │
    ├── dash_tanstack_pivot/         ← auto-generated Dash wrappers
    │   ├── DashTanstackPivot.py     ← main component (all props listed here)
    │   ├── MultiSelectFilter.py
    │   ├── EditSidePanel.py
    │   └── DetailDrawer.py
    │
    └── src/lib/components/          ← React source
        ├── DashTanstackPivot.react.js   ← root React component
        ├── Table/PivotTableBody.js
        ├── Sidebar/SidebarPanel.js
        ├── Charts/PivotCharts.js
        ├── Filters/FilterPopover.py
        └── PivotAppBar.js
```

---

## 3. Installation

```bash
# From PyPI (users)
pip install dash-tanstack-pivot pivot-engine

# From local source (development)
pip install -e dash_tanstack_pivot/
pip install -e dash_tanstack_pivot/pivot_engine/
```

**Dependencies:** `dash`, `pandas`, `duckdb`, `ibis-framework[duckdb]`  
**Optional:** `polars`, `pyarrow`, `redis` (for Redis cache)

---

## 4. Minimal Working App

```python
import pandas as pd
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

df = pd.DataFrame({
    "region":  ["North", "South", "East", "West"] * 250,
    "quarter": ["Q1", "Q2", "Q3", "Q4"] * 250,
    "sales":   range(1000),
    "cost":    range(500, 1500),
})

adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(df, "sales_data")

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="pivot",
        table="sales_data",
        serverSide=True,
        rowFields=["region"],
        colFields=["quarter"],
        valConfigs=[
            {"field": "sales", "agg": "sum", "label": "Total Sales", "format": "fixed:0"},
            {"field": "cost",  "agg": "avg", "label": "Avg Cost",    "format": "fixed:2"},
        ],
        availableFieldList=["region", "quarter", "sales", "cost"],
        filters={},
        sorting=[],
        expanded={},
        showRowTotals=True,
        showColTotals=True,
        defaultTheme="flash",
        data=[],
        style={"height": "500px", "width": "100%"},
    )
])

register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot")

if __name__ == "__main__":
    app.run(debug=True)
```

`register_pivot_app` is the **only** callback registration needed. It wires the full transport (data windows, filters, drill-through, charts, exports).

---

## 5. Python API Reference

### 5.1 `create_tanstack_adapter(backend_uri)`

```python
from pivot_engine import create_tanstack_adapter

adapter = create_tanstack_adapter(backend_uri=":memory:")   # in-memory DuckDB
adapter = create_tanstack_adapter(backend_uri="duckdb:///path/to/file.db")  # persistent
```

| Method | Signature | Notes |
|--------|-----------|-------|
| `load_data` | `(df, table_name)` | Accepts pandas or polars DataFrame |
| `load_data_from_arrow` | `(table_name, pa_table)` | PyArrow table |
| `load_data_from_ibis` | `(table_name, ibis_table)` | Ibis expression (lazy, any SQL backend) |
| `.controller` | property | Returns underlying `PivotController` |

### 5.2 `register_pivot_app(app, adapter_getter, pivot_id, ...)`

```python
from pivot_engine import register_pivot_app

register_pivot_app(
    app,                              # Dash app instance
    adapter_getter=lambda: adapter,   # Callable → TanStackAdapter
    pivot_id="pivot",                 # Must match DashTanstackPivot id=
    debug=False,                      # Extra logging
    drill_store_id=None,              # Custom dcc.Store ID for drill data
)
```

This registers all callbacks. Call **once per pivot component** after `app.layout` is set.

### 5.3 `PivotController` (advanced)

```python
from pivot_engine import PivotController

controller = PivotController(
    backend_uri=":memory:",
    cache="memory",      # "memory" | "redis"
    planner="ibis",
    tile_size=100,
    cache_ttl=300,       # seconds
)
```

### 5.4 `PivotRuntimeService` (multi-instance / custom wiring)

```python
from pivot_engine import (
    PivotRuntimeService,
    SessionRequestGate,
    register_dash_pivot_transport_callback,
)

service = PivotRuntimeService(
    adapter_getter=lambda: adapter,
    session_gate=SessionRequestGate(),
    request_timeout_seconds=300,
)

register_dash_pivot_transport_callback(
    app,
    service_getter=lambda: service,
    pivot_id="pivot",
)
```

Use this pattern when you need multiple independent pivot instances with separate sessions.

---

## 6. DashTanstackPivot Props Reference

All props are keyword arguments to `DashTanstackPivot(...)` in layout. Props marked **[reactive]** can be updated from Dash callbacks.

### 6.1 Core Data Props

| Prop | Type | Default | Notes |
|------|------|---------|-------|
| `id` | `str` | required | Component ID |
| `table` | `str` | required | Table name registered via `adapter.load_data(df, table_name)` |
| `serverSide` | `bool` | `True` | Must be `True` for server-side mode |
| `data` | `list[dict]` | `[]` | Set to `[]` for server-side; client-side data for offline mode |
| `availableFieldList` | `list[str]` | `[]` | All columns user can drag into rows/cols/values |

### 6.2 Layout Props [reactive]

| Prop | Type | Example |
|------|------|---------|
| `rowFields` | `list[str]` | `["region", "country"]` |
| `colFields` | `list[str]` | `["year", "quarter"]` |
| `measureAxis` | `dict` | see section 6.6; moves selected measures into Rows or Columns as virtual members |
| `valConfigs` | `list[dict]` | see §6.5 |
| `viewMode` | `"pivot" \| "table" \| "tree" \| "report"` | `"pivot"` |
| `expanded` | `dict \| bool` | `{}` = all collapsed; `True` = all expanded |
| `showRowTotals` | `bool` | `True` |
| `showColTotals` | `bool` | `True` |
| `showSubtotals` | `bool` | `False` |
| `grandTotalPosition` | `"top" \| "bottom"` | `"bottom"` |
| `headerFormatting` | `dict \| list[dict]` | Per-header color/style overrides |

### 6.3 Filter Props [reactive]

```python
filters={
    "region": {
        "type": "include",           # "include" | "exclude" | "range" | "date_range"
        "values": ["North", "East"]  # for include/exclude
    },
    "sales": {
        "type": "range",
        "min": 100,
        "max": 9999,
    },
    "date_col": {
        "type": "date_range",
        "startDate": "2024-01-01",
        "endDate":   "2024-12-31",
    },
}
```

### 6.4 Sorting Props [reactive]

```python
sorting=[
    {"id": "region",  "desc": False},
    {"id": "sales",   "desc": True},
]

sortOptions={
    "naturalSort": True,
    "caseSensitive": False,
    "columnOptions": {
        "tenor": {
            "sortKeyField": "tenor_order",   # numeric column for custom sort
            "sortSemantic": "tenor",         # "1M" < "3M" < "1Y" ordering
        }
    },
}
```

### 6.5 Value Config Structure (`valConfigs`)

Each entry in the list is a measure:

```python
{
    # Required
    "field": "sales",           # source column

    # Aggregation (choose one)
    "agg": "sum",               # sum | avg | count | min | max |
                                # weighted_avg | wavg | array_agg |
                                # percentile | formula

    # Display
    "label": "Total Sales",     # column header
    "format": "fixed:2",        # see section 6.7

    # Optional / aggregation-specific
    "alias": "sales_sum",       # internal alias (auto-generated if omitted)
    "weightField": "quantity",  # for weighted_avg / wavg
    "percentile": 0.95,         # for percentile agg (0.0–1.0)
    "formula": "[Sales] / [Quantity]",   # for formula agg (Excel-like)
    "formulaRef": ["Sales", "Quantity"], # field aliases referenced by formula

    # Window functions (post-aggregation transform)
    "windowFn": "percent_of_col",  # percent_of_row | percent_of_col |
                                   # percent_of_grand_total

    # Sparklines
    "sparkline": {
        "type": "line",            # line | area | bar
        "showCurrentValue": True,
        "showDelta": False,
        "smooth": True,
    },

    # array_agg separator
    "separator": ", ",
}
```

### 6.6 Measure Axis / Values As Rows or Columns (`measureAxis`)

Use `measureAxis` when a user asks for Excel-style "Values in Rows", "Values in Columns", "show measures as line items", or "make selected measures appear as a dimension". This is not a formula feature and it is not the same as manual category grouping.

The selected physical measures remain in `valConfigs`, but the runtime creates a virtual dimension containing one member per selected measure. That virtual dimension is placed into `rowFields` or `colFields`, and all measure values are aggregated through one virtual value field.

Interaction entry points: in the UI, use the Values area selector labeled "Values as" and choose Columns, Rows, or Column Axis. From Python/Dash or REST/TanStack clients, set the `measureAxis` prop/request field directly.

```python
DashTanstackPivot(
    id="pivot",
    table="risk_explain",
    serverSide=True,
    rowFields=["book"],
    colFields=["portfolio"],
    valConfigs=[
        {"field": "delta_explain", "agg": "sum", "alias": "delta_sum", "label": "Delta Explain", "format": "fixed:0"},
        {"field": "gamma_explain", "agg": "sum", "alias": "gamma_sum", "label": "Gamma Explain", "format": "fixed:0"},
    ],
    measureAxis={
        "placement": "rows",          # "none" | "rows" | "columns"
        "labelField": "Measure Name", # virtual dimension field shown in rows/cols
        "valueField": "Amount",       # virtual numeric value field
        "suppressEmptyMembers": True,
        "suppressZeroMembers": False,
        "totalsPolicy": "per_member",
    },
)
```

Runtime shape:

```python
measureAxis={
    "placement": "rows",
    "labelField": "Measure Name",
    "valueField": "Amount",
    "members": [
        {"measureAlias": "delta_sum", "sourceField": "delta_explain", "label": "Delta Explain", "agg": "sum", "order": 0},
        {"measureAlias": "gamma_sum", "sourceField": "gamma_explain", "label": "Gamma Explain", "agg": "sum", "order": 1},
    ],
    "suppressEmptyMembers": True,
    "suppressZeroMembers": False,
    "totalsPolicy": "per_member",
}
```

Agent decision rules:

| User asks for... | Use |
|------------------|-----|
| "Values as rows", "measures down the side", "P&L lines from selected columns" | `measureAxis={"placement": "rows"}` |
| "Values as columns", "measure names across the top" | `measureAxis={"placement": "columns"}` |
| A new numeric result like margin, spread, ratio, or difference | `valConfigs` formula measure |
| Bucketing dates/numbers or named groups such as "Short/Medium/Long" | category/grouping transform, not `measureAxis` |
| Combining multiple fact tables or DAX-style relationships/measures | data-model work, not `measureAxis` |

Planner execution modes: simple physical measures with one aggregation can use the fast raw-unpivot path. Mixed aggregations, weighted averages, planner expressions, ratio measures, and SQL-planned window measures use aggregate-before-unpivot: the backend first computes the normal pivot measures at the requested grain, then turns selected measure aliases into virtual `labelField` / `valueField` members. This keeps formulas, ratios, averages, and windows mathematically correct.

Implementation contracts agents must preserve:

- `labelField` and `valueField` are virtual fields. They may appear in `rowFields`, `colFields`, `runtimeResponse.columns`, and TanStack column IDs, but they must not be treated as physical columns on the source table.
- If the source data already has a physical column such as `Metric`, keep that field available normally and choose a different virtual `labelField`, usually `Measure Name`.
- For `placement="rows"`, the UI may send `rowFields=["book", "Measure Name"]`. The planner must group the base table by physical dimensions only, then inject `"Measure Name"` after aggregation.
- `valConfigs[*].alias` is the authoritative measure identity. If an alias is present, `measureAxis.members[*].measureAlias` must match it exactly; do not silently fall back to `"{field}_{agg}"`.
- Preserve `label`, `header`, and `format` metadata when normalizing measures, because the virtual member label and formatted cells depend on those values.
- Dynamic column/window IDs after measure-axis unpivot use the virtual value field, not the original measure alias. With `colFields=["portfolio"]` and `valueField="Amount"`, expected value column IDs are like `Book_Amount` and `Hedge_Amount`, not `Book_delta_sum`.
- Frontend column builders must render measure-axis value-field columns as real value cells. Seeing `__schema_placeholder__*` cells usually means the backend returned valid schema/data but the React column-definition logic did not recognize the virtual value-field IDs.
- Collapsed-root paging and other fast paths must opt out when the first visible row/column field is the measure-axis label field, because that field does not exist in the physical table.

Correct aggregate-first SQL shape:

```sql
WITH base AS (
  SELECT
    book,
    portfolio,
    SUM(delta_explain) AS delta_sum,
    AVG(gamma_explain) AS gamma_avg,
    SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS weighted_price
  FROM risk_explain
  GROUP BY book, portfolio
),
measure_rows AS (
  SELECT book, portfolio, 'Delta Explain' AS "Measure Name", delta_sum AS "Amount" FROM base
  UNION ALL
  SELECT book, portfolio, 'Gamma Explain' AS "Measure Name", gamma_avg AS "Amount" FROM base
  UNION ALL
  SELECT book, portfolio, 'Weighted Price' AS "Measure Name", weighted_price AS "Amount" FROM base
)
SELECT
  book,
  "Measure Name",
  SUM(CASE WHEN portfolio = 'Book' THEN "Amount" END) AS Book_Amount,
  SUM(CASE WHEN portfolio = 'Hedge' THEN "Amount" END) AS Hedge_Amount
FROM measure_rows
GROUP BY book, "Measure Name";
```

The exact SQL emitted by Ibis may differ, but the semantics must match: aggregate physical measures at the requested physical grain first, unpivot aliases into virtual measure members second, then pivot/render the virtual `valueField`.

### 6.7 Number Format Strings

| Format string | Result |
|--------------|--------|
| `"fixed:0"` | `1,234` |
| `"fixed:2"` | `1,234.56` |
| `"percent:1"` | `12.3 %` |
| `"currency:USD:2"` | `$1,234.56` |
| `"compact"` | `1.2 K`, `3.4 M` |
| `"scientific:2"` | `1.23e+3` |

### 6.8 Theme Prop

```python
defaultTheme="flash"   # 11 built-in themes:
# flash | dark | material | balham | light |
# bloomberg | bloomberg_black | alabaster | strata | crystal | satin
```

### 6.9 Detail / Drill-Through Props

| Prop | Type | Notes |
|------|------|-------|
| `detailMode` | `"none" \| "inline" \| "sidepanel" \| "drawer"` | How drill rows are shown |
| `drillEndpoint` | `str` | `/api/drill` — backend URL for raw detail rows |

### 6.10 Performance Config

```python
performanceConfig={
    "cacheBlockSize": 100,      # rows per block (default 100)
    "maxBlocksInCache": 1024,   # LRU block limit (~100 K rows)
    "blockLoadDebounceMs": 50,  # scroll debounce in ms
    "rowOverscan": 10,          # extra rendered rows above/below viewport
    "columnOverscan": 5,
    "prefetchColumns": 3,       # columns to prefetch ahead of scroll
}
```

### 6.11 Conditional Formatting

```python
conditionalFormatting=[
    {
        "field": "profit",
        "conditions": [
            {"operator": ">",  "value": 1000, "style": {"color": "green"}},
            {"operator": "<",  "value": 0,    "style": {"color": "red", "fontWeight": "bold"}},
            {"operator": "between", "value": [0, 1000], "style": {"backgroundColor": "#fffde7"}},
        ],
    }
]
```

Header color/style overrides are separate from cell conditional formatting:

```python
headerFormatting={
    "region": {"background": "#E0F2FE", "color": "#0F172A"},
    "sales_sum": {"background": "#123456", "color": "#FFFFFF"},
}

valConfigs=[
    {
        "field": "sales",
        "agg": "sum",
        "label": "Sales",
        "headerStyle": {"background": "#123456", "color": "#FFFFFF"},
    }
]
```

`headerFormatting` may be keyed by column id, field name, group label, or rendered header label. A list of rules is also accepted, for example `{"columns": ["sales_sum"], "style": {...}}`.

### 6.12 Chart Props

```python
chartDefinitions=[
    {
        "id": "chart1",
        "type": "bar",          # bar | line | area | pie | scatter | waterfall | combo
        "rowFields": ["region"],
        "colFields": [],
        "valConfigs": [{"field": "sales", "agg": "sum"}],
        "title": "Sales by Region",
    }
]
chartDefaults={
    "theme": "flash",
    "palette": ["#4e79a7", "#f28e2b", ...],
}
chartCanvasPanes=[...]   # layout positions for charts
```

---

## 7. Dash Callback Patterns

### Reading pivot output in a callback

The component fires `runtimeRequest` (output) and receives `runtimeResponse` (input). For most use cases `register_pivot_app` handles this transparently.

To **react to user state changes** (row/col fields changed by user drag-drop):

```python
from dash import callback, Input, Output

@callback(
    Output("some-store", "data"),
    Input("pivot", "rowFields"),
    Input("pivot", "colFields"),
    Input("pivot", "valConfigs"),
    prevent_initial_call=True,
)
def on_layout_change(row_fields, col_fields, val_configs):
    return {"rows": row_fields, "cols": col_fields}
```

### Pushing layout changes from Python → pivot

```python
@callback(
    Output("pivot", "rowFields"),
    Output("pivot", "colFields"),
    Output("pivot", "valConfigs"),
    Input("reset-btn", "n_clicks"),
)
def reset_pivot(_):
    return (
        ["region"],
        ["quarter"],
        [{"field": "sales", "agg": "sum", "label": "Sales", "format": "fixed:0"}],
    )
```

---

## 8. Advanced Patterns

### 8.1 SQL / Ibis Backend (not DuckDB in-memory)

```python
import ibis
from pivot_engine import create_tanstack_adapter

con = ibis.duckdb.connect("analytics.db")
tbl = con.table("fact_sales")

adapter = create_tanstack_adapter(backend_uri="duckdb:///analytics.db")
adapter.load_data_from_ibis("fact_sales", tbl)
```

### 8.2 Polars / Arrow

```python
import polars as pl

df = pl.read_parquet("data.parquet")
adapter.load_data(df, "data")          # polars DataFrame works directly

import pyarrow.parquet as pq
table = pq.read_table("data.parquet")
adapter.load_data_from_arrow("data", table)
```

### 8.3 Dynamically Updating Data

```python
# Reload data on a schedule or user action
@callback(Output("pivot", "data"), Input("refresh-btn", "n_clicks"))
def refresh(_):
    new_df = fetch_latest_data()
    adapter.load_data(new_df, "sales_data")   # overwrites table in-place
    return []  # trigger re-render (data=[] is required for server-side mode)
```

### 8.4 Weighted Average

```python
valConfigs=[
    {
        "field": "price",
        "agg": "weighted_avg",    # or "wavg"
        "weightField": "quantity",
        "label": "Avg Price (vol-weighted)",
        "format": "fixed:4",
    }
]
```

### 8.5 Formula Column

```python
valConfigs=[
    {"field": "revenue", "agg": "sum", "alias": "Revenue"},
    {"field": "cost",    "agg": "sum", "alias": "Cost"},
    {
        "field": "revenue",          # base field (ignored at runtime)
        "agg": "formula",
        "label": "Margin %",
        "format": "percent:1",
        "formula": "([Revenue] - [Cost]) / [Revenue]",
        "formulaRef": ["Revenue", "Cost"],  # aliases referenced
    },
]
```

### 8.6 Percentile Aggregation

```python
valConfigs=[
    {
        "field": "latency_ms",
        "agg": "percentile",
        "percentile": 0.99,
        "label": "p99 Latency",
        "format": "fixed:1",
    }
]
```

### 8.7 Tenor / Custom Sort Order

When sorting a field like `"1M", "3M", "6M", "1Y"` lexicographically would be wrong:

```python
# In the DataFrame, add a numeric sort key column
df["tenor_order"] = df["tenor"].map({"1M": 1, "3M": 3, "6M": 6, "1Y": 12})

sortOptions={
    "columnOptions": {
        "tenor": {"sortKeyField": "tenor_order"}
    }
}
```

---

## 9. Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `NameError: register_pivot_app` | Old import path | Use `from pivot_engine import register_pivot_app` |
| Blank pivot, no data | `register_pivot_app` not called | Call it after layout is set |
| `table not found` | Table name mismatch | `adapter.load_data(df, "name")` must match `table="name"` in component |
| Subtotals not showing | `showSubtotals` is `False` | Set `showSubtotals=True` |
| Weighted avg returns `None` | `weightField` column missing | Ensure weight column is in DataFrame and `availableFieldList` |
| Measure-axis member is missing | `members[*].measureAlias` does not match a planned measure alias | Use the measure alias/id, or provide `sourceField` + `agg` for physical measures |
| Measure-axis row/column renders as `__schema_placeholder__*` | Frontend column schema does not recognize virtual `valueField` column IDs | Check `runtimeResponse.columns`, `colSchema`, and `useColumnDefs` measure-axis handling |
| SQL error for a virtual measure-axis label/value column | A virtual measure-axis field such as `Measure Name` or `Amount` was treated as a physical source-table field | Remove virtual fields before base aggregation; inject them only after aggregate-first unpivot |
| `NameError: request_layout_mode` | Bug in older version | Update to ≥ 0.0.71 |
| `include_subtotals` order bug | Bug in older version | Fixed in 0.0.71 — update |

---

## 10. Architecture: How the Transport Works

```
User interaction (drag field, scroll, change filter)
    ↓
DashTanstackPivot.react.js  →  sets runtimeRequest prop
    ↓
Dash callback (registered by register_pivot_app)
    ↓
PivotRuntimeService.execute(request)
    ↓
IbisPlanner  →  builds SQL query
    ↓
DuckDB / Ibis backend  →  executes
    ↓
Result  →  runtimeResponse prop  →  React re-renders table
```

**All communication goes through a single callback.** The `kind` field in `runtimeRequest` routes to the correct handler:
- `"data"` — paginated row window
- `"headers"` — column headers
- `"filters"` — filter value lists
- `"chart"` — chart data
- `"drill"` — detail row data
- `"export"` — full export

---

## 11. Testing a Setup

Run the quick-start example and confirm the pivot renders:

```bash
python example_app.py
# Open http://localhost:8050
```

Confirm:
- Table shows rows and columns with aggregated values
- Drag a field from the sidebar into Rows or Columns
- Grand totals appear at bottom
- Filters work via the filter icon on column headers

For measure-axis changes, keep and run the real browser regression. It covers mixed `sum`, `avg`, and `weighted_avg` measures in one values-as-rows layout and checks the actual DOM cells:

```bash
pytest tests/test_measure_axis_browser.py -q -s
```

When the browser view is blank or wrong, debug in this order:
- Inspect `runtimeResponse.payload.data` to prove the planner returned rows.
- Inspect `runtimeResponse.payload.columns` and `runtimeResponse.payload.colSchema` to verify value column IDs such as `Book_Amount` or `Hedge_Amount`.
- Inspect rendered DOM cells. Placeholder cells with valid response data point to frontend column-definition/schema handling, not SQL.
- Check browser console logs for severe errors before changing planner code.

Focused regression set used for measure-axis work:

```bash
pytest tests/test_measure_axis_browser.py tests/test_controller.py tests/test_frontend_contract.py tests/test_dash_runtime_callbacks.py -q
npm run build:js
```

---

## 12. Where to Look for More Detail

| Topic | File |
|-------|------|
| Full prop list with types | `dash_tanstack_pivot/dash_tanstack_pivot/DashTanstackPivot.py` |
| Feature inventory | `CAPABILITIES.md` |
| Known bugs | `KNOWN_ISSUES.md` |
| Master feature guide | `PIVOT_MASTER_DOCUMENTATION.md` |
| Theme tokens / design system | `design_handoff_serverside_pivot/README.md` |
| React component source | `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js` |
| Measure-axis planner logic | `dash_tanstack_pivot/pivot_engine/pivot_engine/planner/ibis_planner.py` |
| Measure-axis browser/runtime/UI tests | `tests/test_measure_axis_browser.py`, `tests/test_controller.py`, `tests/test_frontend_contract.py` |
| SQL query generation | `dash_tanstack_pivot/pivot_engine/pivot_engine/planner/ibis_planner.py` |
| All working examples | `examples/` directory |
