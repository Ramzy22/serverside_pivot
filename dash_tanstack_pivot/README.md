# serverside-pivot

`serverside-pivot` is a Dash component library for server-side pivot tables
built on TanStack Table. It supports large analytical datasets, virtual
scrolling, grouping, totals, filters, formulas, charts, and controlled exports.

Install the component with:

```bash
pip install serverside-pivot
```

Install optional FastAPI and authentication support with:

```bash
pip install "serverside-pivot[api]"
```

The complete quick-start guide, security guidance, and examples live in the
[project repository](https://github.com/Ramzy22/serverside_pivot).

For a publishing-ready comparison of the Dash, React, and Streamlit hosts,
see the [flagship host demos](https://github.com/Ramzy22/serverside_pivot/blob/main/examples/FLAGSHIP_DEMOS.md).

## Cloud Data Warehouses & Ibis Backend Pushdown

`serverside-pivot` connects directly to enterprise cloud data warehouses and distributed SQL engines using **Ibis**:
- **Supported Dialects**: Snowflake (`snowflake://`), Google BigQuery (`bigquery://`), PostgreSQL/Supabase (`postgresql://`), MotherDuck (`md:`), ClickHouse (`clickhouse://`), Databricks (`databricks://`), Trino (`trino://`), SQLite (`sqlite://`), and DuckDB.
- **Zero-Copy Pushdown**: Pass a lazy `ibis.Table` expression directly to `normalize_data_input(lazy_table, table_name, controller)`—the base table is never materialized to Arrow or Pandas in memory. All groupings, expansions, aggregations, and filters compile directly to the warehouse's native SQL dialect.
- **Dialect Portability**: `BackendCapabilities` introspection automatically handles dialect differences (e.g., automatic `LOWER(col) LIKE` fallback for engines without native `ILIKE`, regex tenor sort fallback, parameter placeholder adaptation `%s` vs `?`, and bounded paged batch streaming for large exports).
- **Enterprise Cloud Governance**: Analytical warehouses are read-only by default. Remote DDL (`PIVOT_ALLOW_REMOTE_DDL=1`) and write-back mutations (`PIVOT_ALLOW_REMOTE_MUTATIONS=1`) are strictly opt-in, with TTL schema metadata caching (`PIVOT_SCHEMA_CACHE_TTL_SECONDS=300`).

## Client-Side DuckDB-WASM & Hybrid Execution Mode

Small-to-medium analytical views (up to 50,000 rows) or offline datasets run **100% in-browser** with zero server roundtrips:
- **Auto-Detection (`execution_mode="auto"`)**: Automatically executes queries inside the browser via `DuckDB-WASM` when records are within `client_row_threshold` (default 50,000) or when a `data_url` is provided without a server table. Automatically routes to server-side pushdown when datasets exceed the threshold or connect to remote warehouses.
- **Explicit Modes**: Force 100% browser execution with `execution_mode="client"`, or server execution with `execution_mode="server"`.
- **Python Configuration Helper**: `configure_pivot_execution(data=df, execution_mode="auto")` builds normalized component props for `DashTanstackPivot`.
- **Full Analytical Parity**: Client-side execution supports O(D) hierarchical group expansions, multi-column sorting, column pivots (`colFields`), Excel formulas, window functions (`percent_of_grand_total`), custom dimensions, live ECharts charting, and client SheetJS exports.

Cell editing and edit transactions are not a supported public capability in
this release. Legacy edit-related Dash inputs are accepted as no-ops only for
layout compatibility.

## Host-neutral runtime transport

All hosts use one wire format: Pivot Runtime Protocol V1 (`schemaVersion: 1`).
There is no V2 or host-specific variant. A `CompletePivotAPI` instance exposes
the authenticated endpoint at `POST /v1/runtime`; it validates the V1 request,
derives the session and row-level-security context from the authenticated user,
and returns the matching V1 response.

Large viewport payloads are returned as short-lived, user-scoped `payloadRef`
objects. Fetch `payloadRef.url` instead of putting rows into component state.
The endpoint emits transport profiling under `profile.transport` and supports
ETags for payload retrieval.

### Mutation security

The HTTP runtime is read-only by default, including when the authenticated
principal has an `editor` role. To enable a dataset explicitly, install a
server-owned policy when registering the route. It checks roles, dataset,
operation, and physical columns; authenticated `attributes` are applied to
every update/delete/upsert/aggregate predicate. Added and upserted rows must
include matching attribute values. Undo, redo, revert, and replacement actions
are scoped again when they replay history.

```python
from pivot_engine.runtime import RuntimeMutationAuthorizer, register_runtime_http_transport

mutation_policy = RuntimeMutationAuthorizer({
    "sales": {
        "roles": {"admin", "editor"},
        "columns": {"tenant", "region", "sales"},
        "operations": {"add", "remove", "update", "upsert", "aggregate", "history"},
    }
})

register_runtime_http_transport(
    app,
    get_runtime_service,
    current_user_dependency=get_current_user,
    mutation_authorizer=mutation_policy,
)
```

Do not treat hidden editing controls as authorization. The principal and this
policy must be supplied by the server; browser payloads cannot grant access.

For a standalone React host, use `PivotWorkspace` with `HttpPivotTransport`
(wrap it in `LatestViewportScheduler` for latest-wins virtual scrolling):

```tsx
import {
  HttpPivotTransport,
  LatestViewportScheduler,
  PivotWorkspace,
  themes,
  injectThemeCssVars,
  type PivotRuntimeProps,
  type ValConfig,
  type FilterConfig,
  type SortConfig,
  type ChartDefinition,
} from "dash_tanstack_pivot/react";

const transport = new LatestViewportScheduler(new HttpPivotTransport({
  endpoint: '/v1/runtime',
}));

<PivotWorkspace table="sales" serverSide transport={transport} />
```

`react`, `react-dom`, and `styled-components` are peer dependencies, so the
React application owns those singleton instances. The Streamlit adapter builds
from this exact React entry source; it adds only a V2 mounting/state wrapper.

Dash remains supported through its existing prop callback bridge; it adapts to
the same V1 service without changing the Dash Python API.

## Streamlit host

The optional Streamlit V2 host uses the same browser-to-`/v1/runtime` V1
transport. Virtual scrolling therefore does not rerun the Streamlit script or
place row blocks in Streamlit state:

```bash
pip install "serverside-pivot[streamlit]"
```

```python
from streamlit_pivot import pivot_workspace

pivot_workspace("sales", runtime_endpoint="/v1/runtime", key="sales-pivot")
```

The browser endpoint must be reachable from the Streamlit page and authenticate
the browser user (same-origin cookies are preferred). `view_state` is emitted
only for durable user-controlled changes; it is not a viewport data channel.

## Performance benchmarks

The runtime benchmark measures in-process Arrow load, cold/warm pivot, cache,
and viewport costs:

```bash
python scripts/benchmark_pivot_runtime.py --rows 100000 --repeat-runs 7
```

For the publishing-ready Dash, React, and Streamlit pages, see the
[flagship host performance report](../docs/flagship-performance.md). It keeps
shared-engine, direct-transport, and browser-to-paint measurements separate.

The browser benchmark launches the actual Dash component in headless Chrome,
scrolls through server-side viewport blocks, and reports end-to-end,
network/server, Dash callback, pivot service, and React-commit p50/p95
latency. It also prints the initial hydration sample separately, so a
warm-scroll result cannot hide a first-render regression. It needs Chrome and
a compatible ChromeDriver:

```bash
python scripts/benchmark_browser_runtime.py --rows 100000 --requests 30
# Two independently positioned Chrome clients issue overlapping viewport
# requests against the same local Dash server.
python scripts/benchmark_browser_runtime.py --rows 1000000 --requests 20 --clients 2
```

From `dash_tanstack_pivot/`, npm 11 requires an extra option separator when
forwarding flags:

```bash
npm run benchmark:browser -- -- --rows 100000 --requests 30 --json
```

Collapsed root hierarchies with at most 10,000 root members are materialized
once and sliced for later viewport requests when their Arrow result is at most
32 MiB. This eliminates repeated `IN (...)` aggregate queries while retaining
bounded memory. Tune or disable that optimization with
`PIVOT_COLLAPSED_ROOT_MATERIALIZATION_MAX_ROWS` and
`PIVOT_COLLAPSED_ROOT_MATERIALIZATION_MAX_BYTES` (set either to `0` to disable
it); larger results continue to use the paged-query path.
