from datetime import date, datetime, timezone
from decimal import Decimal

import fakeredis
import pytest

from pivot_engine.runtime.distributed import (
    DistributedRuntimeSettings,
    RedisRuntimeState,
)
from pivot_engine.streaming.exact_state import (
    CDCJobSnapshot,
    CDCStateBudgetExceeded,
    CDCStateLimits,
    CanonicalScalarCodec,
    ExactCDCStateError,
    ExactMeasureAccumulator,
    ExactGroupState,
    MemoryCDCStateStore,
    RedisCDCStateStore,
    StateVersionConflict,
)


@pytest.fixture
def fake_runtime_state():
    return RedisRuntimeState(
        DistributedRuntimeSettings(
            enabled=True,
            redis_url="rediss://user:password@example.test/0",
            key_prefix="test-cdc-state",
        ),
        client=fakeredis.FakeRedis(decode_responses=False),
    )


def test_minimum_removal_uses_counted_multiset_for_duplicate_values():
    accumulator = ExactMeasureAccumulator("min")
    for value in (10, 10, 20, 30):
        accumulator.add(value)

    accumulator.remove(10)
    assert accumulator.result() == 10

    accumulator.remove(10)
    assert accumulator.result() == 20


def test_update_extremum_is_exact_after_remove_then_add():
    accumulator = ExactMeasureAccumulator("max")
    for value in (10, 20, 30):
        accumulator.add(value)

    accumulator.remove(30)
    accumulator.add(25)

    assert accumulator.result() == 25


def test_empty_and_unmatched_removal_are_explicit():
    accumulator = ExactMeasureAccumulator("min")

    assert accumulator.result() is None
    with pytest.raises(ExactCDCStateError, match="matching contribution"):
        accumulator.remove(10)


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        42,
        1.25,
        Decimal("12.50"),
        "north",
        b"bytes",
        date(2026, 9, 19),
        datetime(2026, 9, 19, 12, 30, tzinfo=timezone.utc),
    ],
)
def test_canonical_scalar_codec_round_trips_supported_types(value):
    encoded = CanonicalScalarCodec.encode(value)

    assert CanonicalScalarCodec.decode(encoded) == value


def test_group_state_maintains_sum_count_average_and_extrema():
    measures = [
        {"field": "sales", "agg": "sum", "alias": "total_sales"},
        {"field": "sales", "agg": "count", "alias": "sale_count"},
        {"field": "sales", "agg": "avg", "alias": "average_sales"},
        {"field": "sales", "agg": "min", "alias": "minimum_sales"},
        {"field": "sales", "agg": "max", "alias": "maximum_sales"},
    ]
    group = ExactGroupState()
    group.apply({"sales": 10}, 1, measures)
    group.apply({"sales": 20}, 1, measures)
    group.apply({"sales": 10}, -1, measures)

    assert group.result() == {
        "total_sales": 20,
        "sale_count": 1,
        "average_sales": 20,
        "minimum_sales": 20,
        "maximum_sales": 20,
    }


@pytest.mark.asyncio
async def test_memory_store_rejects_stale_compare_and_swap():
    store = MemoryCDCStateStore()
    initial = await store.load("job-sales")
    next_snapshot = initial.with_checkpoint("0", 1, "evt-1")

    persisted = await store.compare_and_swap("job-sales", initial.version, next_snapshot)

    with pytest.raises(StateVersionConflict):
        await store.compare_and_swap("job-sales", initial.version, next_snapshot)
    assert persisted.version == 1


@pytest.mark.asyncio
async def test_memory_store_rejects_an_oversized_snapshot_without_persisting_it():
    """A store CAS must be a final guard even when callers bypass a processor."""
    store = MemoryCDCStateStore(
        limits=CDCStateLimits(max_groups=10, max_bytes=1, max_extrema_values=10)
    )
    snapshot = await store.load("job-sales")

    with pytest.raises(CDCStateBudgetExceeded):
        await store.compare_and_swap(
            "job-sales", snapshot.version, snapshot.with_checkpoint("0", 1, "evt-1")
        )

    persisted = await store.load("job-sales")
    assert persisted.version == 0
    assert persisted.checkpoints == {}


@pytest.mark.asyncio
async def test_redis_snapshot_persists_checkpoint_across_store_recreation(fake_runtime_state):
    first = RedisCDCStateStore(fake_runtime_state)
    snapshot = await first.load("job-sales")
    persisted = await first.compare_and_swap(
        "job-sales",
        snapshot.version,
        snapshot.with_checkpoint("0", 4, "evt-4"),
    )

    second = RedisCDCStateStore(fake_runtime_state)

    assert (await second.load("job-sales")).checkpoint("0") == (4, "evt-4")
    assert persisted.version == 1


@pytest.mark.asyncio
async def test_redis_store_rejects_an_oversized_snapshot_before_its_cas_write(
    fake_runtime_state,
):
    store = RedisCDCStateStore(
        fake_runtime_state,
        limits=CDCStateLimits(max_groups=10, max_bytes=1, max_extrema_values=10),
    )
    snapshot = await store.load("job-sales")

    with pytest.raises(CDCStateBudgetExceeded):
        await store.compare_and_swap(
            "job-sales", snapshot.version, snapshot.with_checkpoint("0", 1, "evt-1")
        )

    persisted = await store.load("job-sales")
    assert persisted.version == 0
    assert persisted.checkpoints == {}


def test_checkpoint_distinguishes_replay_from_out_of_order_source_events():
    snapshot = CDCJobSnapshot.empty("job-sales").with_checkpoint("0", 4, "evt-4")

    assert snapshot.accept("0", 4, "evt-4") == "replay"
    assert snapshot.accept("0", 3, "evt-3") == "replay"
    assert snapshot.accept("0", 4, "conflicting-evt-4") == "out_of_order"
    assert snapshot.accept("0", 5, "evt-5") == "accept"
    assert snapshot.accept("0", 6, "evt-6") == "gap"
