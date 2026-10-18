"""Real process death and other-worker cancellation; never substitute fakeredis."""
import asyncio
import multiprocessing
import os
import time
from uuid import uuid4

import pytest


def _store(url, prefix):
    import redis
    from pivot_engine.runtime.distributed import DistributedRuntimeSettings, RedisRuntimeState
    from pivot_engine.runtime.redis_export_job_store import RedisExportJobStore

    # Test-only injected connection, identical to existing Redis CI tests.
    state = RedisRuntimeState(DistributedRuntimeSettings(
        enabled=True, redis_url="rediss://test:secret@example.invalid/0", key_prefix=prefix),
        client=redis.Redis.from_url(url, decode_responses=False))
    return RedisExportJobStore(state, lease_seconds=.6)


def _owner(url, prefix, job_id, directory, observations):
    from pivot_engine.backends.duckdb_backend import DuckDBBackend
    from pivot_engine.runtime import PivotRuntimeService, PivotRequestContext, PivotViewState
    from pivot_engine.runtime.export_jobs import ExportJobSnapshot
    from pivot_engine.runtime.export_job_runner import ExportJobRunner
    from pivot_engine.runtime.payload_store import RuntimePayloadStore
    from pivot_engine.tanstack_adapter import TanStackResponse

    backend = DuckDBBackend(":memory:", max_connections=1)
    class Native:
        async def handle_request(self, *_args, **_kwargs):
            await backend.execute_async({"sql": "SELECT sum(i) FROM range(10000000000) t(i)"})
            return TanStackResponse(data=[{"value": 7}], columns=[{"id": "value"}], total_rows=1)
        handle_virtual_scroll_request = handle_request
    service = PivotRuntimeService(lambda: Native())
    governor = service._router.lifecycle.resource_governor
    store = _store(url, prefix)
    payloads = RuntimePayloadStore(governor=governor)
    runner = ExportJobRunner(service, store, payloads, temp_directory=directory, poll_interval_seconds=.02)
    context = PivotRequestContext(table="sales")
    job = store.create(ExportJobSnapshot(job_id, "alice", "system", "sales"))
    runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
    deadline = time.monotonic() + 15
    try:
        announced = False
        while time.monotonic() < deadline:
            current = store.get(job_id, "alice", "sales")
            if backend.execution_metrics()["running"] and not announced:
                observations.put({"phase": "native-running", "slots": governor.snapshot()["active_slots"]["export"]})
                announced = True
            if current.phase == "cancelled":
                observations.put({"phase": "cancelled", "native": backend.execution_metrics(),
                                  "slots": governor.snapshot()["active_slots"]["export"],
                                  "reserved": governor.snapshot()["reserved_bytes"],
                                  "payload": current.payload_ref,
                                  "connectionReusable": backend.execute({"sql": "SELECT 7 AS healthy"}).to_pylist()})
                return
            time.sleep(.01)
        raise AssertionError("Native owner did not reach expected state")
    finally:
        runner.close()
        payloads.clear()
        backend.close()


@pytest.mark.skipif(not os.environ.get("PIVOT_TEST_REDIS_URL"), reason="Real Redis required; skipped integration is not release evidence")
def test_export_owner_kill_restart_and_other_worker_native_cancel(tmp_path):
    url = os.environ["PIVOT_TEST_REDIS_URL"]
    prefix = f"pivot-export-qualification-{uuid4().hex}"
    store = _store(url, prefix)
    context = multiprocessing.get_context("spawn")
    observations = context.Queue()
    owner = context.Process(target=_owner, args=(url, prefix, "killed", str(tmp_path / "first"), observations))
    owner.start()
    restarted = None
    try:
        assert observations.get(timeout=15) == {"phase": "native-running", "slots": 1}
        owner.kill()
        owner.join(5)
        assert not owner.is_alive()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            interrupted = store.get("killed", "alice", "sales")
            if interrupted.phase == "interrupted":
                break
            time.sleep(.05)
        assert interrupted.phase == "interrupted" and interrupted.payload_ref is None
        assert store.advance("killed", interrupted.owner_epoch - 1, {"phase": "completed", "payload_ref": {"id": "late"}}) is None
        restarted = context.Process(target=_owner, args=(url, prefix, "fresh-retry", str(tmp_path / "second"), observations))
        restarted.start()
        assert observations.get(timeout=15) == {"phase": "native-running", "slots": 1}
        assert store.request_cancel("fresh-retry").phase == "cancelling"
        result = observations.get(timeout=15)
        assert result["phase"] == "cancelled" and result["payload"] is None
        assert result["slots"] == result["reserved"] == result["native"]["running"] == 0
        assert result["native"]["cancellation_attempts"] > 0
        assert result["connectionReusable"] == [{"healthy": 7}]
        restarted.join(5)
        assert restarted.exitcode == 0
        assert not list(tmp_path.rglob("pivot_export_*"))
    finally:
        for process in (owner, restarted):
            if process is not None and process.is_alive():
                process.kill()
                process.join(5)
        # Unique test-owned namespace only; no database-wide flush.
        for key in store._runtime_state.client.scan_iter(match=f"{prefix}:*"):
            store._runtime_state.client.delete(key)
