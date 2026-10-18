from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import importlib
import json
import threading

import fakeredis
import pytest

from pivot_engine.runtime.distributed import (
    DistributedRuntimeSettings, DistributedRuntimeUnavailable, RedisRuntimeState,
)
from pivot_engine.runtime.export_jobs import ExportJobSnapshot


class Clock:
    now = 100.0

    def __call__(self):
        return self.now


def job(job_id="job-a"):
    return ExportJobSnapshot(job_id, "alice", "tenant-a", "sales")


@pytest.fixture(params=["local", "redis"])
def stores(request):
    clock = Clock()
    if request.param == "local":
        cls = importlib.import_module("pivot_engine.runtime.export_job_store").LocalExportJobStore
        first = cls(clock=clock, lease_seconds=10, ttl_seconds=30)
        return first, first, clock, None
    cls = importlib.import_module("pivot_engine.runtime.redis_export_job_store").RedisExportJobStore
    server = fakeredis.FakeServer()
    settings = DistributedRuntimeSettings(enabled=True, key_prefix="export-tests")
    states = [RedisRuntimeState(settings, client=fakeredis.FakeRedis(server=server)) for _ in range(2)]
    return (*(cls(state, clock=clock, lease_seconds=10, ttl_seconds=30) for state in states), clock, server)


def test_create_replay_keeps_identity_and_observed_state(stores):
    first, second, _, _ = stores
    assert first.create(job()).phase == "queued"
    claimed = second.claim("job-a", "worker-a")
    assert claimed.phase == "running"
    assert claimed.owner_epoch == 1
    assert first.create(job()) == claimed
    assert first.claim("job-a", "worker-a") is None
    with pytest.raises(ValueError):
        first.create(replace(job(), subject="mallory"))


def test_unknown_and_unauthorized_lookups_and_cancels_are_indistinguishable(stores):
    first, second, _, _ = stores
    first.create(job())
    assert second.get("job-a", "alice", "sales").table == "sales"
    for job_id, subject, table in [("missing", "alice", "sales"), ("job-a", "bob", "sales"), ("job-a", "alice", "other")]:
        assert second.get(job_id, subject, table) is None
        assert second.request_cancel(job_id, subject=subject, table=table) is None
    assert first.get("job-a", "alice", "sales").phase == "queued"


def test_progress_is_monotonic_and_owner_cannot_change_its_epoch(stores):
    first, second, _, _ = stores
    first.create(job())
    claimed = first.claim("job-a", "worker-a")
    progress = second.advance("job-a", claimed.owner_epoch, {"rows_written": 7, "bytes_written": 18})
    assert (progress.rows_written, progress.bytes_written, progress.total_rows) == (7, 18, None)
    with pytest.raises(ValueError):
        first.advance("job-a", claimed.owner_epoch, {"rows_written": 6})
    with pytest.raises(ValueError):
        first.advance("job-a", claimed.owner_epoch, {"owner_epoch": 999})
    assert first.advance("job-a", 99, "failed") is None


def test_cancel_before_claim_never_starts_execution(stores):
    first, second, _, _ = stores
    first.create(job())
    assert second.request_cancel("job-a").phase == "cancelled"
    executions = 0
    for _ in range(2):
        if first.claim("job-a", "worker-a") is not None:
            executions += 1
    assert executions == 0
    assert second.request_cancel("job-a").phase == "cancelled"


def test_cross_worker_cancel_stays_cancelling_until_owner_acknowledges(stores):
    first, second, _, _ = stores
    first.create(job())
    claimed = first.claim("job-a", "worker-a")
    assert second.request_cancel("job-a").phase == "cancelling"
    assert first.heartbeat("job-a", claimed.owner_epoch)
    assert first.advance("job-a", claimed.owner_epoch, {"phase": "completed", "payload_ref": {"id": "payload"}}) is None
    assert first.get("job-a", "alice", "sales").payload_ref is None
    assert first.advance("job-a", claimed.owner_epoch, "cancelled").phase == "cancelled"
    assert first.advance("job-a", claimed.owner_epoch, "cancelled").phase == "cancelled"
    assert first.advance("job-a", claimed.owner_epoch, "failed") is None


def test_atomic_cancel_completion_race_has_one_terminal_outcome(stores):
    first, second, _, _ = stores
    for iteration in range(12):
        job_id = f"race-{iteration}"
        first.create(job(job_id))
        claimed = first.claim(job_id, "worker-a")
        barrier = threading.Barrier(2)

        def cancel():
            barrier.wait()
            return second.request_cancel(job_id)

        def complete():
            barrier.wait()
            return first.advance(job_id, claimed.owner_epoch, {"phase": "completed", "payload_ref": {"id": "payload"}})

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(cancel), pool.submit(complete)]
            cancelled, completed = [future.result() for future in futures]
        current = second.get(job_id, "alice", "sales")
        if current.phase == "completed":
            assert completed.phase == cancelled.phase == "completed"
        else:
            assert current.phase == cancelled.phase == "cancelling"
            assert completed is None
            assert current.payload_ref is None


def test_only_one_worker_can_claim_a_job(stores):
    first, second, _, _ = stores
    first.create(job())
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda pair: pair[0].claim("job-a", pair[1]), [(first, "a"), (second, "b")]))
    assert sum(result is not None for result in results) == 1


@pytest.mark.parametrize("sweep_first", [False, True])
def test_lease_expiry_fences_old_owner_before_it_can_publish(stores, sweep_first):
    first, second, clock, _ = stores
    first.create(job())
    claimed = first.claim("job-a", "worker-a")
    clock.now = 110
    if sweep_first:
        assert [entry.phase for entry in second.interrupt_expired(clock.now)] == ["interrupted"]
    assert first.advance("job-a", claimed.owner_epoch, {"phase": "completed", "payload_ref": {"id": "late"}}) is None
    assert not first.heartbeat("job-a", claimed.owner_epoch)
    observed = second.get("job-a", "alice", "sales")
    assert observed.phase == "interrupted"
    assert observed.owner_epoch > claimed.owner_epoch
    assert observed.payload_ref is None
    assert second.claim("job-a", "new-worker") is None
    assert second.interrupt_expired(clock.now) == []


def test_heartbeat_extends_lease_and_terminal_retention_expires(stores):
    first, second, clock, _ = stores
    first.create(job())
    claimed = first.claim("job-a", "worker-a")
    clock.now = 109
    assert second.heartbeat("job-a", claimed.owner_epoch)
    clock.now = 115
    assert first.interrupt_expired(clock.now) == []
    assert first.advance("job-a", claimed.owner_epoch, "failed").phase == "failed"
    clock.now = 144
    assert second.get("job-a", "alice", "sales").phase == "failed"
    clock.now = 145
    assert second.get("job-a", "alice", "sales") is None


def test_unclaimed_job_eventually_becomes_interrupted(stores):
    first, second, clock, _ = stores
    first.create(job())
    clock.now = 130
    assert [entry.phase for entry in second.interrupt_expired(clock.now)] == ["interrupted"]
    assert first.claim("job-a", "worker-a") is None


def test_redis_job_records_are_json_control_state_without_output_bytes(stores):
    first, _, _, server = stores
    if server is None:
        pytest.skip("Redis serialization contract.")
    first.create(job())
    client = fakeredis.FakeRedis(server=server)
    keys = list(client.scan_iter())
    assert keys
    for key in keys:
        value = json.loads(client.get(key))
        assert value["snapshot"]["subject"] == "alice"
        assert "body" not in value and "file_path" not in value


@pytest.mark.parametrize("operation", ["create", "get", "claim", "request_cancel", "advance", "heartbeat", "interrupt_expired"])
def test_redis_outage_fails_closed_for_every_operation(stores, operation):
    first, _, _, server = stores
    if server is None:
        pytest.skip("Redis availability contract.")
    first.create(job())
    server.connected = False
    arguments = {
        "create": (job("new"),), "get": ("job-a", "alice", "sales"),
        "claim": ("job-a", "worker-a"), "request_cancel": ("job-a",),
        "advance": ("job-a", 1, "failed"), "heartbeat": ("job-a", 1),
        "interrupt_expired": (200,),
    }
    with pytest.raises(DistributedRuntimeUnavailable):
        getattr(first, operation)(*arguments[operation])
