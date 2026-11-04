"""Real process death and other-worker cancellation; never substitute fakeredis."""
import asyncio
from contextlib import contextmanager
import multiprocessing
from multiprocessing.connection import wait
import os
import time
from uuid import uuid4

import pytest


@contextmanager
def _worker(context, target, *args):
    # Each owner gets a fresh synchronous channel. A killed producer cannot
    # leave a Queue feeder or shared writer lock blocking its replacement.
    observations, sender = context.Pipe(duplex=False)
    process = context.Process(target=target, args=(*args, sender))
    try:
        process.start()
        sender.close()
        yield process, observations
    finally:
        sender.close()
        if process.pid is not None:
            if process.is_alive():
                process.kill()
            process.join(5)
        observations.close()


def _receive_observation(process, observations, phase, timeout=15):
    deadline = time.monotonic() + timeout
    ready = wait([observations, process.sentinel], timeout=timeout)
    if observations in ready:
        try:
            return observations.recv()
        except EOFError:
            # EOF may arrive just before the child sentinel. Reap it within
            # the original budget so startup failures include the exit code.
            wait([process.sentinel], timeout=max(0, deadline - time.monotonic()))
            process.join(0)
            raise AssertionError(
                f"Export owner pid={process.pid} exitcode={process.exitcode} closed observations before {phase}"
            ) from None
    if process.sentinel in ready:
        process.join(0)
        raise AssertionError(f"Export owner pid={process.pid} exitcode={process.exitcode} before {phase}")
    raise AssertionError(f"Export owner pid={process.pid} exitcode={process.exitcode} timed out waiting for {phase}")


def _exit_before_observation(observations):
    os._exit(23)


def _announce_and_wait(observations):
    observations.send({"phase": "native-running"})
    time.sleep(15)


def test_observation_wait_reports_early_worker_exit():
    context = multiprocessing.get_context("spawn")
    with _worker(context, _exit_before_observation) as (process, observations):
        with pytest.raises(AssertionError, match="exitcode=23.*native-running"):
            _receive_observation(process, observations, "native-running")


def test_restarted_worker_has_fresh_observations_after_owner_kill():
    context = multiprocessing.get_context("spawn")
    with _worker(context, _announce_and_wait) as (first, observations):
        assert _receive_observation(first, observations, "native-running") == {"phase": "native-running"}
        first.kill()
        first.join(5)
        assert not first.is_alive()
        observations.close()
        with _worker(context, _announce_and_wait) as (second, restarted_observations):
            assert _receive_observation(second, restarted_observations, "native-running") == {"phase": "native-running"}


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
                observations.send({"phase": "native-running", "slots": governor.snapshot()["active_slots"]["export"]})
                announced = True
            if current.phase == "cancelled":
                observations.send({"phase": "cancelled", "native": backend.execution_metrics(),
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
    try:
        with _worker(context, _owner, url, prefix, "killed", str(tmp_path / "first")) as (owner, observations):
            assert _receive_observation(owner, observations, "native-running") == {"phase": "native-running", "slots": 1}
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
        with _worker(context, _owner, url, prefix, "fresh-retry", str(tmp_path / "second")) as (restarted, observations):
            assert _receive_observation(restarted, observations, "native-running") == {"phase": "native-running", "slots": 1}
            assert store.request_cancel("fresh-retry").phase == "cancelling"
            result = _receive_observation(restarted, observations, "cancelled")
            assert result["phase"] == "cancelled" and result["payload"] is None
            assert result["slots"] == result["reserved"] == result["native"]["running"] == 0
            assert result["native"]["cancellation_attempts"] > 0
            assert result["connectionReusable"] == [{"healthy": 7}]
            restarted.join(5)
            assert restarted.exitcode == 0
            assert not list(tmp_path.rglob("pivot_export_*"))
    finally:
        # Unique test-owned namespace only; no database-wide flush.
        for key in store._runtime_state.client.scan_iter(match=f"{prefix}:*"):
            store._runtime_state.client.delete(key)
