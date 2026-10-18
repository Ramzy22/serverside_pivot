# Distributed runtime operations

This guide applies when one Pivot application is served by more than one
process or worker. It enables shared response payloads, request ordering, and
cache invalidation. It does not enable distributed editing.

## Required configuration

Set `PIVOT_DISTRIBUTED_RUNTIME=1` on every application worker. Startup fails
closed unless every worker has the same deployment-scoped Redis configuration:

```env
PIVOT_DISTRIBUTED_RUNTIME=1
PIVOT_RUNTIME_REDIS_URL=rediss://pivot-runtime:replace-with-a-secret@redis.example.net:6380/0
PIVOT_RUNTIME_REDIS_TLS_VERIFY=required
PIVOT_RUNTIME_REDIS_PREFIX=production:analytics:pivot
PIVOT_RUNTIME_PAYLOAD_TTL_SECONDS=120
PIVOT_RUNTIME_PAYLOAD_MAX_BYTES=134217728
```

`PIVOT_RUNTIME_REDIS_URL` must use authenticated `rediss://`; plain Redis,
missing credentials, disabled certificate verification, and an empty prefix
are rejected. The prefix must be unique to an environment and release
boundary. Do not reuse it between development, staging, and production.

For Dash, also configure the identical high-entropy secret on every worker:

```env
PIVOT_DASH_SESSION_SECRET=replace-with-at-least-32-random-bytes
```

The signed Dash session is part of payload ownership. A random per-worker
secret makes a token issued by one worker unreadable on another, so distributed
Dash startup rejects a missing or short `PIVOT_DASH_SESSION_SECRET`.

## Runtime behavior and recovery

Redis stores opaque response/export bytes and JSON metadata, request-ordering
keys, plus a monotonic cache generation. Workers retain only L1 cache values.
They compare their L1 generation with Redis before use; Pub/Sub reduces cache
clear latency but is not relied on for correctness.

An application-worker restart does not invalidate payloads or ordering state
while Redis retains the keys. Payloads expire at
`PIVOT_RUNTIME_PAYLOAD_TTL_SECONDS`; the byte limit is enforced by
`PIVOT_RUNTIME_PAYLOAD_MAX_BYTES`. Export files are copied to Redis bytes, not
referenced by a worker-local filesystem path.

Redis loss, a Redis flush, or a prefix change intentionally expires outstanding
payloads and request ordering. The application fails requests rather than
falling back to process memory. Treat Redis as a highly available, monitored
service: enable persistence appropriate to the payload TTL, alert on
availability and memory pressure, and restrict network access to application
workers over TLS.

## Cancellable export jobs

Redis is optional for a single worker: the default job registry and bounded
executor are process-local. V1 export `start`, `status`, and `cancel` use the
same dataset authorization and server-resolved accounting identity. A missing
in-flight job after a single-worker restart means Interrupted and fresh Retry.

Multi-worker cancellation requires the existing distributed Redis mode and
deployment namespace. Any worker can read status or request cancellation; only
the fenced lease owner can publish output. Redis holds small JSON job records,
cancel intent and heartbeat state, separately from completed payload bytes.
After owner death/heartbeat expiry another worker reports Interrupted; work is
not resumed. Redis loss fails closed rather than falling back to local jobs.

Cancelling is pending cleanup, not acknowledgement. Native execution, writers
and temporary files must stop before Cancelled and before export admission is
released. Alert on `export_cancellation_cleanup_pending`; a backend that does
not acknowledge interruption remains charged. A completed file is exposed only
after the fenced terminal transition wins; cancellation wins revoke staged
payloads. Row, byte, per-file and payload-store budgets still apply.

Bounded large exports currently support flat/table DuckDB-Ibis layouts without
pivot columns, subtotals, formula/window measures or unsafe providers. Unsupported
complex layouts reject before unbounded materialization. This is not a promise
that every pivot layout supports large jobs. Small compatible exports retain
the existing writers and fidelity rules.

An S3-compatible completed-artifact store is deferred and optional. There is no
current resumable job/download promise or object-store deployment dependency.

The real multi-process test uses `PIVOT_TEST_REDIS_URL` only in tests. An unset
URL skips the integration and provides no distributed release evidence.

## Editing is deliberately unavailable

This release does not ship a durable, cross-worker edit-session/event store.
With distributed mode enabled, `update`, transaction, undo, redo, and history
commands return the stable error
`editing_not_available_in_distributed_mode` before the adapter or in-memory
edit domain is accessed. Do not enable editing UI controls for this topology.

Single-worker editing follows the normal authorization and row-security rules,
but it is a separate supported topology. A future distributed-edit release
will require durable event/session storage and conflict handling before this
restriction can be removed.

## Deployment checklist

- Use authenticated TLS Redis and verify its server certificate.
- Give each deployment a distinct `PIVOT_RUNTIME_REDIS_PREFIX`.
- Set the same `PIVOT_DASH_SESSION_SECRET` on every Dash worker.
- Bind HTTP payload downloads to an authenticated principal and Dash downloads
  to the signed session; never expose a payload token as a bearer credential.
- Monitor Redis availability, eviction, memory, and connection saturation.
- Test a worker restart, an expired payload, and Redis fail-closed behavior in
  the target deployment.
- Keep distributed editing disabled until a durable edit-session release is
  installed.

## CI integration

The repository's Redis integration smoke uses a test-only CI connection and
constructs two independent runtime stores against it. That test input is not
an application setting and must not be copied into deployment configuration.
