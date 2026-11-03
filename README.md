# serverside-pivot

A high-performance, contract-governed server-side analytical pivot engine and virtualized grid for **Dash**, **React**, and **Streamlit**.

serverside-pivot connects analytical applications directly to massive data sources (via **DuckDB**, **PyArrow**, and **Ibis** backends) with sub-second aggregation over millions of rows. It delivers an enterprise-grade virtualized table with $\\mathcal{O}(D)$ level-based hierarchical batching, server-owned pivot column order resolution, native statement-level query interruption, and dual-mode exports.

### Core Capabilities

- **Host-Neutral Architecture**: Unified runtime engine (PivotRuntimeRoot) shared identically across **Dash**, standalone **React** (PivotWorkspace), and **Streamlit** (zero-rerun iframe component).
- **Warehouse-Scale Analytics**: Powered by DuckDB + Ibis federation (querying Pandas, Polars, Arrow, DuckDB, Postgres, Snowflake, BigQuery, or ClickHouse).
- **Server-Owned Layout & Slicing**: Dynamic pivot column layout resolver with SHA-256 stable identities, deterministic revision handshakes, and pinned off-window slicing.
- **Fail-Closed Assurance**: Evaluated against [AG Grid Enterprise 36.2.0 (SSRM)](docs/flagship-functionality-certification.md) with 207 mapped browser qualification cases and cryptographic artifact bindings.
- **Enterprise Governance**: Byte-bounded LRU caches, tenant memory accounting, and cancellable large exports with native DuckDB cursor.interrupt().

## Install

```bash
pip install serverside-pivot
```

For Redis-backed caching:

```bash
pip install "serverside-pivot[redis]"
```

For the optional FastAPI service and its authentication dependencies:

```bash
pip install "serverside-pivot[api]"
```

For multi-worker deployment, follow the
[distributed runtime operations guide](docs/operations/distributed-runtime.md).
It requires authenticated TLS Redis and deliberately disables editing until
durable distributed edit sessions are available.

For worker memory limits, native DuckDB settings, authenticated aggregate
health metrics, and release evidence commands, see the [operations guide](docs/OPERATIONS.md).

## Quick Start

Run the included example directly:

```bash
python example_app.py
```

Then open http://127.0.0.1:8050/ in your browser.

Or copy this minimal snippet:

```python
import pandas as pd
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

df = pd.DataFrame({
    "region":  ["North", "South", "East", "West"],
    "quarter": ["Q1",    "Q1",    "Q2",   "Q2"],
    "sales":   [100,     200,     150,    180],
    "revenue": [1000,    2000,    1500,   1800],
})

adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.load_data(df, "sales")

app = Dash(__name__)
app.layout = html.Div([
    DashTanstackPivot(
        id="pivot",
        table="sales",
        serverSide=True,
        rowFields=["region"],
        colFields=["quarter"],
        valConfigs=[
            {"field": "sales",   "agg": "sum", "format": "fixed:0", "label": "Sales"},
            {"field": "revenue", "agg": "sum", "format": "fixed:0", "label": "Revenue"},
        ],
        availableFieldList=["region", "quarter", "sales", "revenue"],
        filters={}, sorting=[], expanded={},
        showRowTotals=True, showColTotals=True,
        defaultTheme="flash",
        data=[],
        style={"height": "400px", "width": "100%"},
    )
])
register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot")

if __name__ == "__main__":
    app.run(debug=True)
```

The same `adapter.load_data(...)` call also accepts `polars`, `pyarrow`, and database-backed Ibis sources.

`register_pivot_app(...)` wires the transport callback, drill-through endpoint, and sort options in one call.

### Supported scope

Cell editing and edit transactions are not a supported public capability in
this release. The legacy edit-related Dash inputs are accepted as no-ops only,
so existing layouts can upgrade without a prop-validation failure.

### Production security

Protect Dash itself with your application's normal authentication middleware.
Dash runtime data, filter values, and exports must also be bound to the host's
authenticated principal and a server-owned table policy. In a production
environment, omitting either hook makes the runtime callback fail closed. For
the drill-through route, use both an explicit table allowlist and an
authorization callback:

```python
from pivot_engine.runtime import RuntimeDatasetAuthorizer

dataset_policy = RuntimeDatasetAuthorizer({
    "sales": {"roles": {"viewer", "editor", "admin"}, "operations": {"read", "export"}},
})

register_pivot_app(
    app,
    adapter_getter=lambda: adapter,
    pivot_id="pivot",
    runtime_dataset_authorizer=dataset_policy,
    runtime_principal_getter=lambda request: current_authenticated_principal(request),
    allowed_drill_tables={"sales"},
    drill_authorizer=lambda request, table: current_user_can_read(request, table),
)
```

The REST API additionally enforces configured API-key/JWT authentication,
dataset permissions, and row-level filters. Its runtime endpoint is
default-deny until a dataset policy is installed. Dash payload download URLs
are short-lived and bound to the issuing browser's signed Flask session; a
token alone is not sufficient to download data.

When JWT authentication is enabled, configure both values and issue tokens
with the matching `iss` claim; `exp` and `iss` are required:

```env
JWT_SECRET_KEY=replace-with-a-long-random-secret
JWT_ISSUER=https://identity.example.com/
```

For an internet-facing production service, use the rotating-key and host
security contract instead of the local single-secret example above. The
application will fail startup with `ENV=production` unless HTTPS origin/host
settings, trusted proxy CIDRs, CSP, rate-limit Redis, secure Redis, and a JWT
key ring are configured. See the [production security runbook](docs/operations/production-security-runbook.md)
for the required environment variables, edge configuration, evidence command,
and operational gates. This repository validates the software contract; TLS
issuance, backup restoration, and independent penetration testing are
operator-owned release requirements.

The release-assurance baseline is [OWASP ASVS v5.0.0 Level 2](security/certification-matrix.json).
Repository tests verify only the controls marked `verified_locally`; current
TLS/network, IdP, penetration-test, backup, incident, and sign-off evidence
must be supplied by the deployment operator and checked with
`scripts/verify_security_release_evidence.py` against the exact release tag,
commit, protected environment, public HTTPS origin, matrix digest, and complete
control scope. The included evidence JSON is deliberately a non-success
template, not a certification claim.

### Export contract

Dash, React, and Streamlit host downloads are **CSV and XLSX** (XLSX remains
the default). Copy actions provide **TSV and HTML** representations: copy
current table preserves the active table state, while copy all records includes
the complete matching hierarchy; both include headers. The runtime/API also
supports CSV, TSV, HTML, and XLSX, with legacy `xls` requests normalized to
XLSX. Host menus do not present TSV or HTML as file downloads.

Release qualification runs both ordinary and limit-rejection browser phases:

```bash
python scripts/certify_flagship.py --export-qualification
```

The command is fail-closed and publishes a sanitized `export-qualification.json`
manifest only when every host has complete evidence and rejected exports leave
no file behind. See [the operations export contract](docs/OPERATIONS.md) for
`PIVOT_EXPORT_MAX_ROWS`, `PIVOT_EXPORT_MAX_BYTES`, artifact requirements, and
the boundary between browser qualification and deployment-owned security
evidence.

## Why Use It

- Built for Dash, so it fits naturally into a Python application.
- One API for local dataframes and database-backed data.
- Handles large tables with server-side transport and windowing.
- Includes the features people actually expect from a pivot UI, not just a demo grid.
- Lets you stay in Python instead of splitting the project across Python and React.

## Examples

- **Flagship host showcases:** [Dash, React, and Streamlit](examples/FLAGSHIP_DEMOS.md) share one interactive Revenue Operations workspace. Run `powershell -File examples/launch_flagship_demos.ps1` to launch all three locally.

- **Quick start (root):** [example_app.py](example_app.py) — 10,000 rows, sparkline, infinite scroll, flash theme. Run with `python example_app.py`.
- Basic example: [examples/example_dash_basic.py](examples/example_dash_basic.py)
- Hierarchical example: [examples/example_dash_hierarchical.py](examples/example_dash_hierarchical.py)
- Multi-instance example: [examples/example_dash_sql_multi_instance.py](examples/example_dash_sql_multi_instance.py)

## License

MIT
