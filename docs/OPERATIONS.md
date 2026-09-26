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
