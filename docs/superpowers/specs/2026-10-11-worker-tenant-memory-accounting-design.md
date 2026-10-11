# Worker and tenant memory accounting design

## Purpose and boundary

One Python worker must have one accountable memory and concurrency envelope,
even when it hosts several pivot runtime services and controllers. The worker
must also reject a workload when its authenticated accounting tenant has used
its local share. An accounting tenant owns and pays for the request; it is
independent of the row security tenants the request may read. A cross-tenant
query is charged in full to its one accounting tenant.

The guarantee remains about measured application reservations. DuckDB, Arrow,
Python allocator and other native allocations still require the existing RSS,
database and deployment hard limits. Tenant concurrency is local to a worker.
Redis durable CDC bytes are coordinated across workers; this does not imply a
deployment-wide query or export concurrency quota.

## Trusted identity

Each host transport resolves a `WorkloadAccount` after authentication. It
contains one nonempty accounting tenant identifier, supplied by a trusted
principal/account context or an authorized host resolver. No browser envelope,
query parameter, filter or row security grant may select it. A user with
access to several data tenants still has one accounting tenant for the whole
operation. An administrator or service principal without an authorized
account uses a capped `system` bucket. The direct engine API uses that bucket
unless a trusted caller explicitly supplies a workload account.

The accounting identifier is never returned in ordinary client responses or
used as a metric label. Missing or malformed identity cannot create an
unmetered path. A host may resolve an authorized account selected by a user,
but must check that selection against the principal before constructing the
account capability.

## Shared ownership and admission

A process-local governor registry creates one `ResourceGovernor` per worker
process. The default runtime service, request lifecycle, controller query
cache, hierarchy cache registry, local payload store and local CDC store use
that instance. Explicit governor injection remains available for tests and
special hosts; a composed runtime must reject a controller or store bound to
a different governor. The admin health endpoint reads the same instance.

Each lease records its resource kind, charged bytes, and accounting tenant.
Admission is atomic under the governor lock and requires capacity in the
worker's class, total-byte and slot limits and the tenant's byte and slot
limits. Failure rejects immediately with the existing stable
`resource_exhausted` response; it cannot reserve only one side. Grow and
release update both ledgers atomically. Lease release is idempotent. Query
and export leases remain held until native cancellation cleanup completes.

Tenant limits use positive, validated configuration: a total resident plus
in-flight byte ceiling and query/export/download slot ceilings. The `system`
bucket has explicit smaller limits. A tenant cannot get extra capacity by
creating another service or session. Admin metrics expose aggregate tenant
admission/rejection counts and the maximum tenant reservation, without names
or per-tenant labels.

The initial defaults are 256 MiB per accounting tenant and 64 MiB for the
system bucket, with tenant query/export/download slot limits of 4/1/2 and
system limits of 2/1/1. The environment names are
`PIVOT_TENANT_MAX_BYTES`, `PIVOT_SYSTEM_MAX_BYTES`,
`PIVOT_TENANT_MAX_CONCURRENT_QUERIES`,
`PIVOT_TENANT_MAX_CONCURRENT_EXPORTS`, and
`PIVOT_TENANT_MAX_CONCURRENT_DOWNLOADS`. Their system slot equivalents use
`PIVOT_SYSTEM_MAX_CONCURRENT_*`. Invalid or inconsistent values fail startup
rather than widening the cap. Internal `system` identity cannot be supplied as
an ordinary tenant identifier.

Cache and payload entries retain their leases until eviction, expiry,
replacement or deletion. When a cache entry is created for a scoped request,
its lease belongs to that request's accounting tenant. Internal/background
entries use the capped system bucket. A cache hit does not transfer ownership
or charge a second tenant for the same retained object. Expiration and
invalidation release the original lease. Unmeasurable values are not cached.

## CDC state

Local exact CDC snapshots remain constrained individually by group count,
extrema cardinality and encoded size. The memory store also maintains an
aggregate retained-byte ledger across all jobs and controllers through the
worker governor's `CDC` leases. A compare-and-swap replacement reserves only
the positive size delta, then atomically replaces state and its lease charge;
shrinks release the difference. Rejected changes leave the previous snapshot
and charge intact. Deletion and expiry release their charge. Clones and
serialization use a separate short-lived worker reservation, charged to the
same account, so a durable storage budget cannot hide local transient memory.

Redis exact CDC state has one namespace-wide and one accounting-tenant durable
byte budget. State bytes, each job's owner/size metadata, and both aggregate
totals change in one atomic Redis operation with version comparison. Owners
cannot silently change on replacement. Deletion and expiry use the same
accounting operation. Raw state keys do not expire independently of their
accounting metadata; job expiry runs through an indexed, atomic cleanup path.
On restart, workers recover from durable metadata. Missing or inconsistent
metadata fails closed until an atomic reconciliation rebuilds totals from
the indexed jobs. Concurrent writers cannot both spend the same remaining
bytes.

The namespace budget is `PIVOT_CDC_MAX_BYTES` (128 MiB by default); the new
`PIVOT_CDC_TENANT_MAX_BYTES` defaults to 64 MiB. Job expiry is explicit and
disabled by default for durable exact state; configured expiry and explicit
deletion both invoke the indexed cleanup operation. Local in-flight
clone/serialization reservations use worker `CDC` capacity and tenant worker
capacity independently of these durable Redis totals.

When a CDC budget is exhausted, no partial exact state is published. The job
becomes invalidated with `cdc_state_budget_exceeded` and requires authoritative
recomputation, preserving the existing freshness contract.

## Verification

Tests must exercise two runtime services sharing a default governor, a
controller query cache and hierarchy cache sharing that governor, and two
tenants competing for bytes and slots. They must prove that a multi-data-
tenant request charges only its trusted accounting tenant; an untrusted
payload tenant cannot change the charge; admin/system work is capped; and
failed admission leaves both ledgers unchanged.

CDC tests must cover several jobs whose combined bytes exceed the budget,
replacement growth and shrink, deletion, expiry, replay/version conflict,
restart recovery and simultaneous Redis writers. A Redis durable charge and
a local clone/serialization reservation must be visible as distinct domains.
Cancellation tests must confirm that the worker and tenant leases remain
until database cleanup acknowledges completion. The existing full Python
suite, frontend typecheck/build, and memory pressure benchmark remain release
checks.
