from datetime import date, datetime, timezone
from decimal import Decimal
import asyncio
from concurrent.futures import ThreadPoolExecutor

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
    _encoded_snapshot_size,
)
from pivot_engine.resource_governor import ResourceGovernor, ResourceLimits, WorkloadAccount, current_workload_account


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
async def test_memory_cdc_accounts_retained_jobs_together_and_releases_on_delete():
    sample = CDCJobSnapshot.empty("one").with_checkpoint("0", 1, "evt")
    size = _encoded_snapshot_size(sample)
    governor = ResourceGovernor(
        ResourceLimits.for_testing(total_bytes=4 * size, cdc_bytes=3 * size - 1,
                                   tenant_bytes=4 * size, system_bytes=4 * size),
        rss_bytes_getter=lambda: 0,
    )
    store = MemoryCDCStateStore(governor=governor)
    first = await store.compare_and_swap("one", 0, sample)
    assert first.version == 1
    assert governor.snapshot()["reserved_bytes_by_kind"]["cdc"] == size + len(str(first.version)) - len(str(sample.version))
    second = CDCJobSnapshot.empty("two").with_checkpoint("0", 1, "evt")
    with pytest.raises(CDCStateBudgetExceeded):
        await store.compare_and_swap("two", 0, second)
    assert (await store.load("two")).version == 0
    await store.delete("one")
    assert governor.snapshot()["reserved_bytes_by_kind"]["cdc"] == 0


@pytest.mark.asyncio
async def test_memory_cdc_expiry_releases_retained_charge():
    clock = [100.0]
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    store = MemoryCDCStateStore(governor=governor, job_ttl_seconds=5, clock=lambda: clock[0])
    await store.compare_and_swap("expiring", 0, CDCJobSnapshot.empty("expiring"))
    assert governor.snapshot()["reserved_bytes_by_kind"]["cdc"] > 0
    clock[0] = 106.0
    assert (await store.load("expiring")).version == 0
    assert governor.snapshot()["reserved_bytes_by_kind"]["cdc"] == 0


@pytest.mark.asyncio
async def test_memory_cdc_expiry_index_stays_bounded_under_replacements():
    store = MemoryCDCStateStore(job_ttl_seconds=3600, clock=lambda: 100.0)
    snapshot = CDCJobSnapshot.empty("hot")
    for version in range(1100):
        snapshot = await store.compare_and_swap("hot", version, snapshot)
    assert len(store._expiry_heap) <= 1024
    await store.delete("hot")


def test_memory_cdc_store_is_thread_safe_across_event_loops():
    sample = CDCJobSnapshot.empty("one").with_checkpoint("0", 1, "evt")
    size = _encoded_snapshot_size(sample)
    governor = ResourceGovernor(ResourceLimits.for_testing(
        total_bytes=4 * size, cdc_bytes=3 * size - 1,
        tenant_bytes=4 * size, system_bytes=4 * size,
    ), rss_bytes_getter=lambda: 0)
    store = MemoryCDCStateStore(governor=governor)

    def write(job):
        try:
            return asyncio.run(store.compare_and_swap(
                job, 0, CDCJobSnapshot.empty(job).with_checkpoint("0", 1, "evt")
            ))
        except CDCStateBudgetExceeded:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, ("one", "two")))
    assert sum(result is not None for result in results) == 1


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
async def test_redis_cdc_tenant_and_namespace_budgets_are_atomic_across_jobs(fake_runtime_state):
    sample = CDCJobSnapshot.empty("one").with_checkpoint("0", 1, "evt")
    size = _encoded_snapshot_size(sample)
    account = WorkloadAccount.tenant("payer")
    token = current_workload_account.set(account)
    try:
        first = RedisCDCStateStore(fake_runtime_state, tenant_max_bytes=2 * size - 1,
                                   namespace_max_bytes=4 * size)
        await first.compare_and_swap("one", 0, sample)
        second = RedisCDCStateStore(fake_runtime_state, tenant_max_bytes=2 * size - 1,
                                    namespace_max_bytes=4 * size)
        with pytest.raises(CDCStateBudgetExceeded):
            await second.compare_and_swap("two", 0, CDCJobSnapshot.empty("two").with_checkpoint("0", 1, "evt"))
        assert (await second.load("two")).version == 0
        await first.delete("one")
        await second.compare_and_swap("two", 0, CDCJobSnapshot.empty("two").with_checkpoint("0", 1, "evt"))
    finally:
        current_workload_account.reset(token)


@pytest.mark.asyncio
async def test_redis_cdc_rebuilds_durable_totals_after_restart(fake_runtime_state):
    store = RedisCDCStateStore(fake_runtime_state)
    snapshot = CDCJobSnapshot.empty("one").with_checkpoint("0", 1, "evt")
    await store.compare_and_swap("one", 0, snapshot)
    _, total_key, tenants_key = store._ledger_keys()
    fake_runtime_state.client.delete(total_key, tenants_key)
    reopened = RedisCDCStateStore(fake_runtime_state)
    assert (await reopened.load("one")).version == 1
    assert int(fake_runtime_state.client.get(total_key)) > 0


@pytest.mark.asyncio
async def test_redis_cdc_rejects_unindexed_state_after_restart(fake_runtime_state):
    store = RedisCDCStateStore(fake_runtime_state)
    fake_runtime_state.client.set(
        store._key("orphan"), store._encode(CDCJobSnapshot.empty("orphan"))
    )
    reopened = RedisCDCStateStore(fake_runtime_state)
    with pytest.raises(ExactCDCStateError, match="unindexed"):
        await reopened.load("orphan")


@pytest.mark.asyncio
async def test_redis_cdc_expiry_removes_state_and_durable_totals_on_recovery(fake_runtime_state):
    clock = [100.0]
    first = RedisCDCStateStore(fake_runtime_state, job_ttl_seconds=5, clock=lambda: clock[0])
    await first.compare_and_swap("expiring", 0, CDCJobSnapshot.empty("expiring"))
    clock[0] = 106.0
    reopened = RedisCDCStateStore(fake_runtime_state, job_ttl_seconds=5, clock=lambda: clock[0])
    assert (await reopened.load("expiring")).version == 0
    assert int(fake_runtime_state.client.get(reopened._ledger_keys()[1])) == 0


@pytest.mark.asyncio
async def test_redis_concurrent_jobs_cannot_overspend_one_namespace(fake_runtime_state):
    sample = CDCJobSnapshot.empty("one").with_checkpoint("0", 1, "evt")
    size = _encoded_snapshot_size(sample)
    store = RedisCDCStateStore(fake_runtime_state, namespace_max_bytes=2 * size - 1,
                               tenant_max_bytes=2 * size)
    await store.load("warmup")

    def write(job):
        token = current_workload_account.set(WorkloadAccount.tenant("payer"))
        try:
            return asyncio.run(store.compare_and_swap(
                job, 0, CDCJobSnapshot.empty(job).with_checkpoint("0", 1, "evt")
            ))
        except (CDCStateBudgetExceeded, StateVersionConflict):
            return None
        finally:
            current_workload_account.reset(token)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, ("one", "two")))
    assert sum(result is not None for result in results) == 1
    assert int(fake_runtime_state.client.get(store._ledger_keys()[1])) == size


@pytest.mark.asyncio
async def test_redis_cdc_charges_each_owner_without_splitting_namespace(fake_runtime_state):
    sample = CDCJobSnapshot.empty("one").with_checkpoint("0", 1, "evt")
    size = _encoded_snapshot_size(sample)
    store = RedisCDCStateStore(fake_runtime_state, namespace_max_bytes=2 * size,
                               tenant_max_bytes=size)
    for account_name, job in (("payer-a", "one"), ("payer-b", "two")):
        token = current_workload_account.set(WorkloadAccount.tenant(account_name))
        try:
            await store.compare_and_swap(
                job, 0, CDCJobSnapshot.empty(job).with_checkpoint("0", 1, "evt")
            )
        finally:
            current_workload_account.reset(token)
    assert int(fake_runtime_state.client.get(store._ledger_keys()[1])) == 2 * size


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
