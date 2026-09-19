# serverside-pivot

Build a real server-side pivot table in Dash in about 10 lines of Python.

`serverside-pivot` is built for analytical apps that need fast aggregations, large datasets, and a serious pivot UI without building a separate frontend. It is designed to handle millions of rows with sub-second aggregations, while keeping the interaction model people expect: drag-and-drop fields, row and column grouping, totals, formulas, sorting, filtering, formatting, and virtual scrolling.

Strong point : It works with `pandas`, `polars`, `pyarrow`, and any SQL backends through Ibis, so the same component can start on a local dataframe and scale to a production database without changing the user experience.

On the Dash side, one runtime callback handles data windows, filter option requests, chart requests, and drill-through.

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
