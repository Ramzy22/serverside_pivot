# Database cancellation design

## Purpose

An asynchronous request cancellation or application timeout must stop the
database statement that is consuming resources, not only stop the coroutine
that awaits a `run_in_executor` future. After cancellation, the backend must
either prove the leased connection is usable or discard it before it can be
reused.

This applies when another application, a browser disconnect, or a server
timeout cancels a pivot request. It is independent of editing and CDC.

## Current failure

`IbisBackend.execute_async()` and several controller paths use
`run_in_executor()` around synchronous Ibis execution. Cancelling the asyncio
future does not stop the worker thread or its database statement. The current
`cancel_query()` only cancels that waiter, so request deadlines can leave
queries running and connections busy in the background.

## Chosen architecture

Introduce a narrow, per-statement `DatabaseExecution` handle. It owns:

- a stable query id;
- the exact physical connection or cursor executing the statement;
- a completion event shared by the worker and async caller;
- a native `cancel()` action and a post-cancel `validate_or_discard()` action.

The asynchronous facade creates the handle before scheduling work and stores
it by query id. On `CancelledError` or an application deadline it first calls
the handle's native cancellation method, then waits for the worker to finish
its cleanup before propagating a structured cancellation/timeout error. It
never returns a borrowed connection to a pool merely because the asyncio
waiter ended.

All controller paths that currently call `expr.to_pyarrow` directly through
`run_in_executor()` route through the Ibis backend execution facade. This
ensures the visible request path and the public `execute_async()` API have the
same cancellation semantics.

## Capability adapters

Cancellation is defined by an explicit physical-connection capability, not by
the nominal Ibis backend name.

1. **DuckDB**: execute on a dedicated cursor/connection lease and call
   `interrupt()` on that exact lease. An interrupted lease runs a health query
   before reuse; failure discards it.
2. **Drivers with `cancel()`**: invoke the driver's cancellation API on the
   exact statement connection. PostgreSQL additionally receives a scoped
   `statement_timeout` when a deadline is configured.
3. **Drivers with a statement timeout but no cancel API**: configure the
   backend-native statement deadline before execution and translate its
   database timeout error to the common timeout result.
4. **Unsupported adapters**: no background cancellation claim is made. A
   configured hard deadline fails closed during setup rather than silently
   exposing an application-only timeout. Without a hard deadline, the backend
   reports `cancellation_unavailable` in its capability/status data.

The initial production implementation covers DuckDB (direct and Ibis-backed),
which is the packaged default. Driver adapters are intentionally isolated so a
PostgreSQL adapter can be integration-tested without teaching generic Ibis
objects to expose unsafe private state.

## Deadline policy

`QUERY_TIMEOUT` becomes a database execution deadline, not just an HTTP or
asyncio wait limit. A timeout follows this sequence:

1. record a stable `query_timeout` reason;
2. invoke the physical cancellation capability;
3. wait for the worker's bounded cleanup acknowledgement;
4. validate or discard the lease; and
5. return a safe timeout response without database exception text.

An explicit client cancellation follows the same sequence with reason
`client_cancelled`. Cancellation is idempotent; repeated calls for an already
finished query return its terminal status and do not interrupt another query.

## Connection ownership and safety

Execution must not use a shared connection when cancellation could affect an
unrelated statement. The backend leases one physical cursor/connection per
statement. A lease is returned only after normal completion or a successful
health query. Any cancellation failure, timeout-cleanup failure, or failed
health query closes/discards the lease and decrements pool capacity so a clean
lease can be created.

The direct DuckDB pool already has health/discard mechanics; this work moves
the active lease into the execution handle and makes interruption target that
lease instead of the backend's global `self.con`.

## Errors and observability

Expose only stable application errors: `query_cancelled`, `query_timeout`,
`cancellation_unavailable`, and `database_execution_failed`. Structured logs
may contain query id, backend capability, cancellation reason, elapsed time,
and lease disposition; they must not send raw backend exception text to a
client.

Metrics track running statements, cancellation attempts/successes/failures,
timeouts, discarded leases, and cleanup latency. A request is not reported as
successfully cancelled until its worker releases or discards the lease.

## Verification

Tests must prove behavior rather than only inspect task cancellation:

- A real DuckDB long-running statement is interrupted after the worker has
  started; its future completes promptly with the structured cancellation
  result.
- A `QUERY_TIMEOUT` interrupts the same class of statement at database level.
- The exact lease executes `SELECT 1` after an interrupt, or is demonstrably
  discarded and replaced before another request obtains it.
- Repeated cancellations do not accumulate active workers or grow pool usage
  beyond its configured capacity.
- `IbisBackend.execute_async()` and controller pivot execution both use the
  cancellable facade; direct `run_in_executor(expr.to_pyarrow)` calls are not
  left on request paths.
- A capability-double proves unsupported backends reject a requested hard
  deadline rather than claiming database cancellation.
- PostgreSQL statement-timeout/cancel behavior is exercised by an opt-in CI
  integration test when its service is configured.

## Non-goals

- Killing arbitrary Python threads.
- Treating an application-only asyncio timeout as a database cancellation.
- Adding a generic connection pool for every Ibis backend without a documented
  physical cancellation capability.
- Modifying CDC, editing, public Dash props, or host-neutral transport
  contracts.
