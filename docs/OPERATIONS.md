# Operations guide

## Database cancellation and deadlines

The packaged DuckDB backend (direct and Ibis-backed) owns one physical cursor
per statement. Cancelling a browser request, superseding a request, or
reaching a deadline interrupts that exact cursor; the request is not reported
as cancelled until the worker has health-checked or discarded its lease.

Set `QUERY_TIMEOUT` to a positive number of seconds to apply a database
statement deadline to new `DuckDBBackend` and `IbisBackend` instances. This is
not an asyncio-only timeout: the backend interrupts the active DuckDB cursor.
Invalid, zero, or negative values fail construction. A backend without an
exact cancellation capability rejects a configured `QUERY_TIMEOUT` with
`cancellation_unavailable` rather than claiming a deadline it cannot enforce.

`PIVOT_REQUEST_TIMEOUT_SECONDS` is the outer runtime request deadline. It
cancels the backend coroutine, which then completes the same native statement
cleanup path. When both values are configured, the first deadline to expire
wins and clients receive `query_timeout` only after lease cleanup completes.

The backend exposes process-local counters through `execution_metrics()`:
`running`, `cancellation_attempts`, `cancellation_successes`,
`cancellation_failures`, `timeouts`, `healthy_checkins`, and
`discarded_leases`. Export these through the host application's metrics layer;
do not attach SQL text, parameter values, or database error messages to labels.

Client responses use only these database outcomes:

- `query_cancelled`
- `query_timeout`
- `cancellation_unavailable`
- `database_execution_failed`

Raw driver exception text remains server-side for structured logging and must
not be returned in Dash, REST, WebSocket, React, or Streamlit responses.

## Supported backends

DuckDB is the currently supported production cancellation adapter. It is
covered for direct SQL and Ibis expressions. Ibis adapters without an exact,
documented physical connection cancellation capability can run only without a
hard database deadline and advertise `cancellation_unavailable`.

PostgreSQL support requires a separate opt-in integration adapter and service
test. That adapter must target the exact driver connection, issue native
`cancel()` when available, scope `statement_timeout` to the statement or
transaction, and prove the connection is usable or discarded after timeout.
Do not enable PostgreSQL hard deadlines based only on an application-level
`asyncio.wait_for` wrapper.

## Worker memory envelope

The worker owns one declared memory envelope. `PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES`
is the container/process hard ceiling; `PIVOT_RESOURCE_MAX_BYTES` is the
application-accounted budget; `PIVOT_NATIVE_HEADROOM_BYTES` reserves space for
Arrow, DuckDB, Python, and extension allocations; and
`PIVOT_DUCKDB_MEMORY_LIMIT` configures DuckDB's native allocator. Startup
fails closed unless:

```text
DuckDB limit + application budget + native headroom <= worker hard limit
```

The direct and Ibis-backed DuckDB constructors apply the reconciled setting.
Memory errors remain behind the existing stable backend error boundary. The
admin health endpoint is authenticated and reports only aggregate current RSS,
RSS high-water, reservations, admissions, and rejection counts. Accounted
bytes are an admission-control estimate; they are not the process's native
total memory and must not be treated as one.

Recommended baseline environment:

```env
PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES=2147483648
PIVOT_RESOURCE_MAX_BYTES=1073741824
PIVOT_NATIVE_HEADROOM_BYTES=268435456
PIVOT_DUCKDB_MEMORY_LIMIT=512MB
PIVOT_WORKER_RSS_SOFT_LIMIT_BYTES=1811939328
```

Container and service limits must agree with the application envelope. For
Docker, set a hard limit and pass the same values to the process:

```bash
docker run --memory=2g --env-file=.env your-image
```

For Kubernetes, set both requests and limits on every worker pod and keep the
DuckDB limit below the pod limit after application budget and native headroom:

```yaml
resources:
  requests: {memory: 2Gi}
  limits: {memory: 2Gi}
```

For systemd, use `MemoryMax=2G` in the service unit and the equivalent
`PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES` value in its environment file. Do not
use an unlimited DuckDB setting to bypass the worker envelope.

## Release evidence

The ordinary benchmark remains small. Run the opt-in deterministic pressure
workload in the target worker/container profile and retain its JSON output:

```bash
python scripts/benchmark_pivot_runtime.py --memory-workload \
  --rows 500000 --concurrent-requests 4 --json > memory-500k.json
python scripts/benchmark_pivot_runtime.py --memory-workload \
  --rows 1000000 --concurrent-requests 4 --max-rss-bytes 2147483648 --json > memory-1m.json
```

The report includes latency, admitted/rejected work, throughput, current RSS,
and peak RSS. A non-zero exit means the requested RSS ceiling was exceeded or
the workload failed to demonstrate an admission rejection; it does not claim
that application-accounted bytes equal total native process memory. Keep the
JSON alongside the exact image/tag, deployment limits, and admin health
snapshot for release review.

## Export contract and release qualification

The public host contract is deliberately narrower than the runtime protocol:

- Dash, React, and Streamlit host downloads are **CSV and XLSX**. XLSX is the
  default when an existing export command omits a format.
- Clipboard delivery is **TSV and HTML**. Copy-current-table preserves the
  active filter, sorting, expansion, layout, and visible column order; copy-all
  records includes the complete matching hierarchy. Both include headers.
- The runtime/API export service supports CSV, TSV, HTML, and XLSX (with the
  legacy `xls` request treated as XLSX). The host menu does not advertise TSV
  or HTML as file downloads.

Every host uses the server-owned row and byte limits. `PIVOT_EXPORT_MAX_ROWS`
rejects a request before materialization when its row count is too large, and
`PIVOT_EXPORT_MAX_BYTES` rejects an encoded document that exceeds the byte
budget. A rejected export returns the stable `Export limit exceeded` error and
must leave no completed or partial download behind.

Run the release-equivalent browser qualification from a clean build with:

```bash
python scripts/certify_flagship.py --export-qualification
```

The command runs isolated `normal` and `constrained` phases for Dash, React,
and Streamlit. It fails closed on unavailable browser tooling, skipped or
failed cases, missing host evidence, a limit bypass, or any completed/partial
file after rejection. The publication directory contains the sanitized
`export-qualification.json` manifest, phase JUnit summaries, host screenshots,
browser/network evidence, parsed document summaries, and downloads. Evidence
contains no cell values, credentials, tokens, or backend exception text. Keep
the manifest with the exact wheel, frontend build, image/tag, and deployment
record used for the release; this browser qualification does not replace
operator-owned TLS, identity, recovery, or penetration-test evidence.
