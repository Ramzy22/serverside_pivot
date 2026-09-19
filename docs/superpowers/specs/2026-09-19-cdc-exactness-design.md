# Exact, High-Performance CDC Aggregation Design

## Status

Approved direction: implement exact, reversible incremental aggregation for
trusted CDC streams.  The product must not present a stale streaming aggregate
as authoritative when a change cannot be safely applied.

## Goal

Maintain pivot aggregate results at event speed without recomputing a complete
view for normal inserts, updates, or deletes.  A deletion of an extremum and
an update that replaces an extremum must be exact immediately, including
duplicate values, grouping changes, filters, empty groups, replay, and process
restart.

The design applies to database CDC and upstream event feeds.  It is independent
of browser editing, which remains outside the release feature set.

## Problem

The current streaming processor reverses `sum`, `count`, and `avg`, but its
`min`/`max` reversal is a no-op.  The incremental materialized-view manager
contains the same limitation.  Therefore a group with values `10, 20, 30`
continues to expose minimum `10` after `10` is deleted.

Polling CDC can additionally report only that a table changed.  A placeholder
row has neither the prior value nor the next value, so it cannot safely update
any grouped aggregate.  The current CDC-to-view adapter also contains an
interface mismatch that can be hidden by broad exception handling.

## Supported CDC contract

The exact fast path accepts a versioned change envelope with:

- `table`, operation (`INSERT`, `UPDATE`, or `DELETE`), source identifier,
  event identifier, and monotonically ordered source position;
- a complete `new_row` for inserts;
- a complete `old_row` for deletes;
- both complete images for updates;
- a stable row identity, or a source position whose ordering and delivery
  semantics make the event uniquely replay-safe.

`UPDATE` is modeled as removal of the old row followed by insertion of the new
row.  This naturally handles a value change, a group move, entering/leaving a
filter, and combinations of those cases.

Providers label a change that cannot meet this contract as `invalidating`.
Polling snapshots produce invalidating events instead of synthetic row-level
inserts, updates, or deletes.  A native/logical/push CDC provider with full row
images can use the exact path.

The source position is persisted per source partition.  Duplicate events at or
before the checkpoint are ignored; an out-of-order event is not applied and
invalidates the affected result.  This gives deterministic replay and restart
behavior without guessing from row values.

## Exact aggregate state

Each streaming job holds independent state for each logical pivot group.  A
group state contains one accumulator per measure:

- `sum`: running numeric sum;
- `count`: row count;
- `avg`: numeric sum plus count;
- `min` and `max`: a counted scalar multiset and a min or max heap.  Removing a
  value decrements its count; heap cleanup removes zero-count entries lazily.

The multiset, rather than one remembered extremum, makes duplicate values
correct.  For example, removing one of two values equal to `10` preserves
minimum `10`; removing the final `10` exposes `20` without a source query.

Scalar values use a versioned, type-preserving canonical codec.  It supports
the Arrow/Ibis scalar types accepted for pivot measures and never uses pickle.
Canonical values are used as multiset keys; their original typed representation
is retained for result delivery and comparison.

Every row image is evaluated against the job's saved filters before it affects
state.  Its grouping tuple selects the accumulator.  A group whose contributing
row count becomes zero is removed.  Exact state uses only the affected old and
new groups, so per-event work is proportional to the number of measures, with
`min`/`max` heap work logarithmic in distinct values for that group.

## State, concurrency, and recovery

`CDCStateStore` is a narrow persistence port with atomic group-state update,
checkpoint advancement, and snapshot load.  The production implementation uses
the existing authenticated, TLS Redis runtime configuration, deployment-scoped
keys, and versioned JSON/binary values.  It must not use the generic legacy
Redis cache serializer or pickle.  Memory storage is development/test-only and
does not claim multi-worker or restart recovery.

For throughput, events are partitioned by the source partition and grouped into
bounded write batches.  A batch atomically records the affected group snapshots
and the corresponding source checkpoint.  On restart, a worker restores group
state and resumes after that checkpoint.  State updates for the same group are
serialized by the store's compare-and-set version; a collision retries from the
current snapshot rather than overwriting another worker's update.

The manager exposes a read-only freshness status.  A result is served only when
its group state is current.  An invalidating event clears the relevant result
generation before callbacks or reads can observe it.  It never returns a
plausible but stale aggregate.

## Invalidating fallback

An invalidating event does not enter the reversible accumulator.  It:

1. advances the source checkpoint and records the invalidation reason;
2. increments the same table/cache generation used by runtime CDC invalidation;
3. clears derived streaming/materialized-view result caches; and
4. schedules an authoritative refresh from the saved Ibis specification.

Until refresh succeeds, result retrieval reports `invalidated` rather than
returning old data.  The fallback is intentionally exceptional: polling CDC is
correct but not suitable for the advertised extreme-performance mode.  Product
documentation will require native or push CDC with row images for that mode.

## Component boundaries

### CDC providers

Providers produce validated `ChangeEnvelope` values.  `PollingCDCProvider`
emits one invalidating table event when its content fingerprint changes; it no
longer fabricates row changes from row counts.  Push/native providers must
provide complete before/after images and source ordering metadata for exact
processing.

### CDC manager

`PivotCDCManager` validates a change once, routes exact changes to registered
stream and materialized-view consumers, and routes invalidating changes to their
explicit invalidation method.  Consumer interface names are unified; failures
are logged with safe structured context and change processing is marked failed
rather than silently swallowed.

### Stream aggregation processor

`StreamAggregationProcessor` owns job specifications, row-to-group evaluation,
and exact accumulator transitions.  It uses `CDCStateStore` for snapshots and
checkpoints and exposes only fresh results.  Its fast path is independent of
the materialized-view cache.

### Incremental materialized-view manager

`IncrementalMaterializedViewManager` maps a source table to its registered view
names and saved specifications.  It consumes the same exact envelope contract.
For invalidation it rebuilds from the authoritative Ibis expression before
marking the view fresh.  It never guesses a min/max result from the cached
summary row.

## Error handling and observability

- Invalid or incomplete exact events are converted to invalidations with a
  reason code, never partially applied.
- A persistence, compare-and-set, or refresh failure leaves the result
  invalidated and emits a safe structured error; backend exception text is not
  sent to clients.
- Metrics record exact events, invalidating events by reason, ignored replays,
  out-of-order events, state update latency, refresh latency, and stale-result
  denials.
- Callbacks receive a freshness-bearing result object, not an unqualified raw
  dictionary.

## Acceptance criteria

- Deleting a unique or duplicated min/max returns the exact next extremum.
- Updating `10` to `25` removes `10` then adds `25` exactly.
- A filter transition and a grouping change update only the old/new affected
  groups.
- Empty groups disappear; `avg`, `sum`, and `count` preserve their defined
  empty semantics.
- A duplicate, replayed, or stale source event does not modify state twice.
- Recreating the processor over the same durable state resumes from its
  checkpoint and returns the same result.
- Placeholder polling changes invalidate, refresh authoritatively, and never
  alter a reversible accumulator.
- The CDC manager reaches the real materialized-view consumer interface;
  consumer failures are observable in tests and logs.
- Existing cache invalidation still occurs for all CDC events.

## Non-goals

- Browser editing, edit history, undo/redo, and distributed edit sessions.
- Pretending polling CDC can provide event-speed exact aggregates without row
  images.
- A general event broker implementation; the contract supports existing push
  and future native providers.
