# Memory protection design

## Purpose

Keep a pivot worker available under large, concurrent queries, exports,
payload downloads, caches, and CDC views. The application must reject work
predictably before it retains unbounded Python or Arrow objects. This is a
per-worker guarantee: application accounting cannot measure every native
allocation made by DuckDB, Arrow, or CPython, so database and deployment
limits remain independent required safeguards.

The intended release behavior is fail-fast overload handling. When a worker
cannot safely admit work, the request receives the stable
`resource_exhausted` result with a bounded retry hint; it is not placed in an
unbounded queue and the worker is not allowed to progress toward an OOM kill.

## Current failure modes

- `MemoryCache` has an LRU entry count but no byte budget.
- Local payload storage tracks aggregate bytes, but permits a single payload
  larger than the configured maximum. Redis payload storage limits individual
  values but not the aggregate namespace.
- The fallback export path creates a complete Arrow table and encoded output
  before `ExportLimits.ensure_bytes()` can reject it.
- Runtime requests have cancellation and deadlines but no shared admission
  limit for queries and exports.
- Exact CDC state can grow with group count and the number of distinct values
  maintained for reversible extrema.
- DuckDB accepts a `memory_limit`, but it is not coordinated with the worker
  memory envelope or observed process RSS.

## Chosen architecture

Introduce a process-local `ResourceGovernor` owned by runtime construction and
shared by the controller, export service, payload store, byte-aware caches,
and CDC state store. It is an accounting and admission boundary, not an
attempt to model every allocation in the process.

The governor has immutable, validated `ResourceLimits`:

- an accountable worker byte budget;
- a reserved headroom amount that is never admitted to application work;
- per-class resident budgets for cache, payload, and exact CDC state;
- per-class in-flight reservations for query, export, and payload download
  work;
- maximum concurrent query, export, and download slots; and
- an RSS soft threshold used to reject new work before native allocations
  approach the deployment hard limit.

`PIVOT_RESOURCE_MAX_BYTES` configures the accounted worker budget. The local
default is 1 GiB with 256 MiB reserved headroom; cache, payload, and exact CDC
resident ceilings default to 256 MiB, 128 MiB, and 128 MiB respectively. A
query can reserve at most 256 MiB, an export batch 32 MiB, and a payload
download buffer 8 MiB. Production configuration must set every one of these
values explicitly alongside the deployment memory limit. Defaults and
environment parsing reject zero, negative, non-integer, or internally
inconsistent limits rather than silently widening a budget.

### Admission and release

An operation calls `governor.reserve(kind, estimated_bytes)` before planning
or fetching result data. Admission succeeds only when all of these are true:

1. its class slot is available;
2. its reservation fits the accountable free budget after headroom;
3. the class's own in-flight budget permits it; and
4. observed RSS is below the soft threshold.

The returned lease is the only authority to retain the reservation. It records
actual batch/output bytes, may grow only while capacity remains, and releases
in a `finally` block on success, cancellation, timeout, or error. No waitlist
is maintained. Rejection raises `ResourceExhaustedError`, whose public code is
`resource_exhausted`; HTTP transports map it to a retryable 503 response with
`Retry-After` and a bounded `retryAfterMs`. It exposes class and limit names,
not row data, SQL, tenant values, driver errors, or process details.

Request budgets are conservative and explicit. A viewport/structural response
may only materialize up to its configured result byte limit. Full exports use
a record-batch streaming path; each batch is charged and released after it is
written or yielded. If a backend lacks a bounded streaming path for a request
whose configured materialization bound cannot be met, the request is rejected
before materializing the result. A row-count or output-byte check after a
complete table has been created is never considered memory protection.

All release paths compose with statement cancellation: a cancelled database
operation first retires its physical lease as specified by the database
cancellation design, then releases its resource-governor lease.

### Byte-aware resident stores

`MemoryCache` becomes byte-aware LRU. It keeps its entry ceiling for lookup
behavior, adds `max_bytes`, and maintains exact charged bytes while expiring,
replacing, deleting, clearing, and evicting entries. Size accounting uses
Arrow `nbytes`, byte/string lengths, and a safe canonical serialized form for
supported structured values. Values whose size cannot be safely measured are
not cached. A value larger than the cache's per-entry or remaining shared
budget is returned to the caller but not stored; normal cache admission never
evicts unrelated values merely to admit a single oversize value.

Hierarchy and controller-owned LRU structures use the same sizing helper and
governor cache lease rather than independently counting entries. Existing
cache-coordinator metrics remain the reporting facade but receive bytes in
every store/evict/expire event.

Payload storage enforces both per-payload and aggregate byte limits in every
mode:

- local storage rejects one oversize in-memory payload before insertion and
  evicts only according to its explicitly configured LRU policy;
- file payloads charge only bounded metadata in process memory, while disk
  size is enforced by the export/payload output limit and streamed download;
- distributed storage maintains an atomic namespace byte counter with each
  write/delete/expiry, so independently routed workers cannot each assume the
  full Redis budget is free.

Payload download uses a bounded buffer and a download slot. A stored payload
is never copied wholesale into a second unbounded response buffer.

### Exact CDC state budget

Exact CDC state receives `CDCStateLimits` with maximum groups, serialized
snapshot bytes, and distinct extrema values. The limits are checked before a
new group or distinct min/max value is retained and again on the exact encoded
snapshot before compare-and-swap. Both memory and Redis stores enforce the
same contract.

Exceeding a CDC state limit does not retain an approximate or partial view.
The job is marked invalid with stable reason `cdc_state_budget_exceeded`, its
incremental result is unavailable, and the controller schedules or requires
an authoritative recomputation. This preserves the existing exactness promise
while making cardinality growth bounded.

### Native and deployment safeguards

Application reservations are intentionally lower than the worker's actual
memory allocation. Deployment must set a container/cgroup/worker hard memory
limit and provide a margin for Python, Arrow, and database native allocations.

The DuckDB adapter receives a validated `PIVOT_DUCKDB_MEMORY_LIMIT` (or an
explicit backend equivalent). `PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES` declares
the deployment value for startup validation; the configured DuckDB limit,
accounted application budget, and native-allocation headroom must fit below
it. The RSS soft limit defaults to 85 percent of that hard limit. It
continues to use database statement deadlines and native cancellation; a
database memory exception is normalized without exposing driver text.

`psutil` samples RSS at admission and records high-water RSS. It is an early
warning and a soft admission gate, not a process hard limit. The operations
guide specifies container/Kubernetes/systemd memory controls, DuckDB memory
configuration, and alert thresholds; a deployment that omits its hard memory
limit is explicitly unsupported for production certification.

## Configuration and observability

All limits have validated constructor arguments and documented environment
counterparts. The production profile includes, at minimum:

- `PIVOT_RESOURCE_MAX_BYTES` and reserved headroom;
- per-class cache, payload, CDC, query-result, and export-batch byte limits;
- `PIVOT_MAX_CONCURRENT_QUERIES`, `PIVOT_MAX_CONCURRENT_EXPORTS`, and
  `PIVOT_MAX_CONCURRENT_DOWNLOADS`;
- `PIVOT_WORKER_RSS_SOFT_LIMIT_BYTES` and deployment worker memory hard
  limit; and
- `PIVOT_DUCKDB_MEMORY_LIMIT`.

The runtime publishes bounded metrics: admitted/rejected operations by class
and reason, active slots, reserved/actual/peak bytes by class, cache and
payload resident bytes, CDC group/state bytes, current RSS, high-water RSS,
and streamed batch counts. It never labels application-accounted bytes as
total process memory.

## Verification

Tests must demonstrate the enforcement boundary, not merely configuration
parsing:

- byte-aware cache replacement, expiration, eviction, unmeasurable values,
  and single oversize values leave resident bytes within the limit;
- local and distributed payload stores reject individual and aggregate
  oversize writes, including concurrent writers;
- concurrent queries, exports, and downloads consume their class slots and
  fail immediately with `resource_exhausted` when budgeted capacity is full;
- leases release after successful completion, exception, timeout, and
  cancellation, so a later request is admitted;
- batch-streamed export remains inside its configured in-flight byte budget;
  a non-streaming backend is rejected before unsafe materialization;
- CDC rejects excessive groups, extrema cardinality, and encoded state size,
  marks the view invalid, and routes it to authoritative recomputation;
- DuckDB configuration rejects inconsistent database/worker budgets;
- runtime/HTTP responses expose only stable overload codes and retry metadata.

An opt-in workload benchmark creates realistic 500k and 1M-row pivot data,
exercises concurrent viewport queries, exports, payload downloads, cache
pressure, and CDC cardinality growth, then records latency, throughput,
admission rejections, and RSS high-water in JSON. The benchmark has a
configured process-RSS ceiling and fails if it is exceeded or if an overload
is not rejected predictably. Its results form release evidence; small unit
tests do not claim a 1M-row memory result.

## Non-goals

- Estimating native DuckDB, Arrow, allocator, or kernel memory exactly from
  Python objects.
- Replacing deployment cgroup/container/worker memory limits.
- Globally coordinating native memory across different worker processes.
- Introducing tenant fairness quotas in this release; admission is
  worker-global and must not bypass existing tenant authorization.
- Turning a rejected full export into an implicit background job or an
  unbounded wait queue.
