# Exact CDC operations

## What is authoritative

Exact CDC is the event-speed aggregate path for a source that can provide a
source-consistent snapshot and a complete, ordered change stream. It supports
`sum`, `count`, `avg`, `min`, and `max`. The `min` and `max` accumulators retain
counted values, so duplicate extrema and deletion of the final extremum are
exact without a full query.

Each exact event must contain all of the following:

- `table`, `type` (`INSERT`, `UPDATE`, or `DELETE`), and a stable source name;
- `event_id`, `partition`, and an integer `sequence` for the source event;
- `row_identity` that is stable for replay protection;
- a complete `new_row` for `INSERT`, a complete `old_row` for `DELETE`, and
  both `old_row` and `new_row` for `UPDATE`.

`sequence` is a contiguous event counter within each partition. The next event
after a snapshot checkpoint must have `sequence == checkpoint + 1`. A duplicate
at the saved position is ignored; an older/conflicting event or a sequence gap
is never applied and invalidates the result. Sources with sparse log positions
must expose a contiguous per-partition event counter alongside their native
offset.

## Starting an exact job

The source scan and its `source_checkpoints` must be obtained atomically by the
CDC source. Pass those exact `source_rows` with the watermarks; the controller
does not scan the table after receiving a checkpoint, because that would leave
an unobservable window for missed changes. It builds exact state and records
the supplied snapshot/watermarks in one compare-and-set write. Supplying only
rows or only a checkpoint is rejected.

```python
job = await controller.run_streaming_aggregation(
    spec,
    source_rows=rows_captured_with_the_watermark,
    source_checkpoints={"0": (1042, "snapshot-1042")},
)
assert job["status"] == "fresh"
```

The source's next event for partition `0` must therefore be sequence `1043`.
The snapshot may be large; that one-time source scan establishes the baseline.
After it succeeds, an exact event changes only the affected group(s): work is
linear in the number of measures, plus logarithmic heap maintenance for `min`
and `max` values. It does not recompute the whole pivot for normal events.

A new job without `source_checkpoints`, or an old state marked fresh without
watermarks, returns `status: "unavailable"` with
`authoritative_snapshot_required`. Do not display its values as current. To
recover an `invalidated` job, obtain a new atomic source snapshot/watermark and
call `run_streaming_aggregation` again with the same specification.

## Incomplete sources and polling

`PollingCDCProvider` is correctness-safe but is not the event-speed path. It
can establish that table contents changed but cannot supply trustworthy
`old_row`/`new_row` images, so it emits one invalidating event instead of
inventing row deltas. An incomplete push event is accepted as
`accepted_invalidating`, never as exact.

When an event is incomplete, out of order, a sequence gap, a source refresh
failure, or a state conflict occurs, the affected aggregate returns
`status: "invalidated"` and no aggregate values. Applications must recompute
from an authoritative snapshot before presenting it as fresh. The historic
`update_view_incrementally()` cache-arithmetic path is redirected to a single
authoritative batch refresh; untyped legacy CDC consumers are rejected.

## Recovery and multi-worker deployments

`memory` is suitable only for development and tests. It loses CDC aggregates
and checkpoints on process restart and must not be presented as multi-worker
recovery.

For durable recovery, set the following production controls and construct the
controller with `cdc_exact_state_store="redis"`:

```env
CDC_EXACT_STATE_STORE=redis
PIVOT_DISTRIBUTED_RUNTIME=1
PIVOT_RUNTIME_REDIS_URL=rediss://user:password@redis.example.com:6380/0
PIVOT_RUNTIME_REDIS_PREFIX=production-pivot
PIVOT_RUNTIME_REDIS_TLS_VERIFY=required
```

The validated Redis runtime persists versioned JSON state and source
checkpoints. A restarted processor that recreates the same specification reads
that state, does not overwrite it with an uncoordinated scan, and ignores a
replayed `event_id` at the saved sequence. Redis unavailability is a failure,
not a fallback to memory, when this mode is selected.

## Operational checks

Before enabling exact CDC in production, verify that the provider can:

1. acquire the source snapshot and watermark atomically;
2. retain complete before/after images for every operation;
3. supply stable identities and contiguous partition sequences after reconnect;
4. replay the latest event after restart without changing the aggregate; and
5. trigger a fresh source snapshot after an `invalidated` or `unavailable`
   result.

Use polling only when authoritative refresh latency is acceptable. Use native
or push CDC with the above envelope for the advertised high-throughput path.
