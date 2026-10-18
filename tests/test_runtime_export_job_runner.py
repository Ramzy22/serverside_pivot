"""Export jobs must retain admission until native and file cleanup finish."""
import asyncio
import importlib
import os
from pathlib import Path
import threading
import time

import pytest

from pivot_engine.backends.duckdb_backend import DuckDBBackend
from pivot_engine.resource_governor import ResourceGovernor, ResourceLimits, ResourceExhaustedError, WorkloadAccount
from pivot_engine.runtime import PivotRuntimeService, PivotRequestContext, PivotViewState
from pivot_engine.runtime.export_jobs import ExportJobSnapshot
from pivot_engine.runtime.export_job_store import LocalExportJobStore
from pivot_engine.runtime.payload_store import RuntimePayloadStore
from pivot_engine.tanstack_adapter import TanStackResponse
from tests.export_qualification_fixture import (qualification_response, qualification_state, expected_export,
                                               read_csv_export, read_xlsx_export, EXPECTED_ORDER)


def wait_for(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.005)
    raise AssertionError("export did not reach expected observation")


class Adapter:
    async def handle_request(self, *_args, **_kwargs):
        return TanStackResponse(data=[{"value": 1}], columns=[{"id": "value"}], total_rows=1)
    handle_virtual_scroll_request = handle_request


def setup_runner(tmp_path, adapter=None, store=None, export_slots=1, governor=None, **options):
    cls = importlib.import_module("pivot_engine.runtime.export_job_runner").ExportJobRunner
    governor = governor or ResourceGovernor(ResourceLimits.for_testing(max_exports=export_slots, export_bytes=100_000,
                               total_bytes=10_000_000, tenant_bytes=1_000_000, system_bytes=1_000_000,
                               payload_bytes=100_000, worker_hard_limit_bytes=20_000_000), rss_bytes_getter=lambda: 0)
    service = PivotRuntimeService(lambda: adapter or Adapter(), resource_governor=governor)
    store = store or LocalExportJobStore(lease_seconds=.3)
    payloads = RuntimePayloadStore(governor=governor)
    runner = cls(service, store, payloads, temp_directory=tmp_path, poll_interval_seconds=.01, **options)
    context = PivotRequestContext(table="sales", accounting_account=WorkloadAccount.tenant("trusted"))
    job = store.create(ExportJobSnapshot("job-a", "alice", "trusted", "sales"))
    return runner, store, payloads, governor, service, context, job


def snapshot(store, job_id="job-a"):
    return store.get(job_id, "alice", "sales")


def test_success_hands_off_one_lease_and_authenticated_file(tmp_path):
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path)
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        done = wait_for(lambda: (item := snapshot(store)).phase == "completed" and item)
        assert done.rows_written == 1 and done.bytes_written > 0 and done.total_rows is None
        item = payloads.get(done.payload_ref["id"])
        assert item.metadata["subject"] == "alice" and item.metadata["table"] == "sales"
        assert Path(item.file_path).read_text(encoding="utf-8-sig").splitlines() == ["Value", "1"]
        wait_for(lambda: governor.snapshot()["active_slots"]["export"] == 0)
    finally:
        runner.close()
        payloads.clear()
    assert list(tmp_path.iterdir()) == []


def test_native_cancel_keeps_slot_until_cursor_checkin_then_retry(tmp_path, monkeypatch):
    backend = DuckDBBackend(":memory:", max_connections=1)
    cleaning, release = threading.Event(), threading.Event()
    original = backend.pool.checkin
    def checkin(*args, **kwargs):
        cleaning.set()
        assert release.wait(5)
        return original(*args, **kwargs)
    monkeypatch.setattr(backend.pool, "checkin", checkin)
    class NativeAdapter(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            await backend.execute_async({"sql": "SELECT sum(i) FROM range(10000000000) t(i)"})
            return await super().handle_request()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, NativeAdapter())
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        wait_for(lambda: backend.execution_metrics()["running"])
        store.request_cancel(job.job_id)  # cancellation can arrive on another worker
        assert cleaning.wait(3)
        assert snapshot(store).phase == "cancelling"
        assert governor.snapshot()["active_slots"]["export"] == 1
        with pytest.raises(ResourceExhaustedError):
            governor.reserve("export", 1, account=context.accounting_account)
        release.set()
        wait_for(lambda: snapshot(store).phase == "cancelled")
        assert governor.snapshot()["active_slots"]["export"] == 0
        assert backend.execution_metrics()["running"] == 0
        assert backend.execute({"sql": "SELECT 7 AS healthy"}).to_pylist() == [{"healthy": 7}]
        service._router._adapter_getter = lambda: Adapter()
        retry = store.create(ExportJobSnapshot("retry", "alice", "trusted", "sales"))
        runner.start(retry, context, PivotViewState(export_request={"format": "csv"}))
        wait_for(lambda: snapshot(store, "retry").phase == "completed")
    finally:
        release.set()
        runner.close()
        payloads.clear()
        backend.close()


def test_cancel_without_native_acknowledgement_remains_charged(tmp_path):
    started, release, cleaned = threading.Event(), threading.Event(), threading.Event()
    class RefusingAdapter(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                while not release.is_set():
                    await asyncio.sleep(.005)
                cleaned.set()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, RefusingAdapter())
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        assert started.wait(2)
        runner.signal_cancel(job.job_id)
        time.sleep(.1)
        assert snapshot(store).phase == "cancelling"
        assert governor.snapshot()["active_slots"]["export"] == 1
        assert not cleaned.is_set()
        release.set()
        wait_for(lambda: snapshot(store).phase == "cancelled")
        assert cleaned.is_set() and governor.snapshot()["reserved_bytes"] == 0
    finally:
        release.set()
        runner.close()


def test_cancel_before_claim_owns_no_admission(tmp_path):
    entered, release = threading.Event(), threading.Event()
    class DelayedStore(LocalExportJobStore):
        def claim(self, *args):
            entered.set()
            assert release.wait(3)
            return super().claim(*args)
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, store=DelayedStore())
    starter = threading.Thread(target=runner.start, args=(job, context, PivotViewState(export_request={"format": "csv"})))
    try:
        starter.start()
        assert entered.wait(2)
        assert governor.snapshot()["active_slots"]["export"] == 0
        store.request_cancel(job.job_id)
        release.set()
        starter.join(2)
        assert not starter.is_alive()
        assert governor.snapshot()["admitted"]["export"] == 0
        assert snapshot(store).phase == "cancelled"
        assert payloads._size_snapshot()["entries"] == 0
    finally:
        release.set()
        starter.join(2)
        runner.close()


def test_cancellation_wins_during_payload_staging_and_revokes_file(tmp_path, monkeypatch):
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path)
    put = payloads.put_file
    def race(*args, **kwargs):
        ref = put(*args, **kwargs)
        store.request_cancel(job.job_id)
        return ref
    monkeypatch.setattr(payloads, "put_file", race)
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        wait_for(lambda: snapshot(store).phase == "cancelled")
        assert snapshot(store).payload_ref is None
        assert payloads._size_snapshot()["entries"] == 0
        assert list(tmp_path.iterdir()) == []
        assert governor.snapshot()["active_slots"]["export"] == 0
    finally:
        runner.close()


@pytest.mark.parametrize("fmt,reader", [("csv", read_csv_export), ("xlsx", read_xlsx_export)])
def test_job_preserves_export_fixture_values_and_headers(tmp_path, fmt, reader):
    class FixtureAdapter(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            return qualification_response()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, FixtureAdapter())
    state = qualification_state()
    state.export_request = {"format": fmt, "colIds": EXPECTED_ORDER}
    try:
        runner.start(job, context, state)
        done = wait_for(lambda: (item := snapshot(store)).phase == "completed" and item)
        assert reader(payloads.get(done.payload_ref["id"]).file_path) == expected_export(fmt)
    finally:
        runner.close()
        payloads.clear()


@pytest.mark.parametrize("limit", ["max_rows", "max_bytes"])
def test_failed_budget_removes_output_and_releases_admission(tmp_path, limit):
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path)
    from pivot_engine.runtime.export_service import RuntimeExportService
    class Rows(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            return TanStackResponse(data=[{"value": 1}, {"value": 2}], columns=[{"id": "value"}], total_rows=2)
        handle_virtual_scroll_request = handle_request
    service._router._adapter_getter = lambda: Rows()
    service._router.export_service = RuntimeExportService(**{limit: 1})
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        wait_for(lambda: snapshot(store).phase == "failed")
        assert governor.snapshot()["active_slots"]["export"] == 0
        assert list(tmp_path.iterdir()) == []
    finally:
        runner.close()


def test_job_heartbeats_while_waiting_and_fenced_owner_cannot_publish(tmp_path):
    started, release = threading.Event(), threading.Event()
    class Slow(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            started.set()
            while not release.is_set():
                await asyncio.sleep(.01)
            return await super().handle_request()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, Slow())
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        assert started.wait(2)
        time.sleep(.4)
        assert snapshot(store).phase == "running"
        assert store.interrupt_expired(time.time() + 2)[0].phase == "interrupted"
        release.set()
        wait_for(lambda: governor.snapshot()["active_slots"]["export"] == 0)
        assert snapshot(store).phase == "interrupted"
        assert payloads._size_snapshot()["entries"] == 0
    finally:
        release.set()
        runner.close()


def test_cancel_during_csv_writer_batch_preserves_charge_and_removes_partial_file(tmp_path, monkeypatch):
    import pivot_engine.runtime.export_service as writers
    writing, release = threading.Event(), threading.Event()
    original = writers.csv.writer
    class PausedWriter:
        def __init__(self, *args, **kwargs):
            self.writer = original(*args, **kwargs)
            self.rows = 0
        def writerow(self, row):
            self.writer.writerow(row)
            self.rows += 1
            if self.rows == 130:
                writing.set()
                assert release.wait(5)
    class ManyRows(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            return TanStackResponse(data=[{"value": i} for i in range(500)], columns=[{"id": "value"}], total_rows=500)
        handle_virtual_scroll_request = handle_request
    monkeypatch.setattr(writers.csv, "writer", PausedWriter)
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, ManyRows())
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        assert writing.wait(2)
        assert snapshot(store).rows_written == 128
        assert snapshot(store).bytes_written > 0
        runner.signal_cancel(job.job_id)
        assert snapshot(store).phase == "cancelling"
        assert governor.snapshot()["active_slots"]["export"] == 1
        assert len(list(tmp_path.iterdir())) == 1
        release.set()
        wait_for(lambda: snapshot(store).phase == "cancelled")
        assert list(tmp_path.iterdir()) == []
        assert governor.snapshot()["active_slots"]["export"] == 0
    finally:
        release.set()
        runner.close()


def test_startup_sweeps_only_expired_writer_files(tmp_path):
    stale = tmp_path / "pivot_export_orphan.csv"
    recent = tmp_path / "pivot_stream_recent.csv"
    unrelated = tmp_path / "important.txt"
    stale.write_text("partial")
    recent.write_text("partial")
    unrelated.write_text("keep")
    os.utime(stale, (1, 1))
    runner, *_ = setup_runner(tmp_path)
    try:
        assert not stale.exists()
        assert recent.exists() and unrelated.read_text() == "keep"
    finally:
        runner.close()


def test_executor_rejects_before_scheduling_and_preserves_first_lease(tmp_path):
    entered, release = threading.Event(), threading.Event()
    class Slow(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            entered.set()
            while not release.is_set():
                await asyncio.sleep(.01)
            return await super().handle_request()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, Slow(), max_workers=1, max_pending=0)
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        assert entered.wait(2)
        other = store.create(ExportJobSnapshot("second", "alice", "trusted", "sales"))
        with pytest.raises(ResourceExhaustedError) as error:
            runner.start(other, context, PivotViewState(export_request={"format": "csv"}))
        assert error.value.reason == "export_executor_full"
        assert snapshot(store, "second").phase == "failed"
        assert governor.snapshot()["active_slots"]["export"] == 1
    finally:
        release.set()
        runner.close()
        payloads.clear()


def test_payload_registration_failure_removes_file_without_completed_job(tmp_path):
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path)
    payloads.max_bytes = 1
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        wait_for(lambda: snapshot(store).phase == "failed")
        assert snapshot(store).payload_ref is None
        assert list(tmp_path.iterdir()) == []
        assert governor.snapshot()["active_slots"]["export"] == 0
    finally:
        runner.close()


def test_cross_worker_redis_cancellation_stops_owner(tmp_path):
    import fakeredis
    from pivot_engine.runtime.distributed import DistributedRuntimeSettings, RedisRuntimeState
    from pivot_engine.runtime.redis_export_job_store import RedisExportJobStore
    server = fakeredis.FakeServer()
    settings = DistributedRuntimeSettings(enabled=True, key_prefix="runner-test")
    first, other = [RedisExportJobStore(RedisRuntimeState(settings, client=fakeredis.FakeRedis(server=server))) for _ in range(2)]
    entered = threading.Event()
    class Slow(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            entered.set()
            await asyncio.Event().wait()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, Slow(), store=first)
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        assert entered.wait(3)
        assert other.request_cancel(job.job_id).phase == "cancelling"
        wait_for(lambda: snapshot(other).phase == "cancelled")
        assert governor.snapshot()["active_slots"]["export"] == 0
        assert list(tmp_path.iterdir()) == []
    finally:
        runner.close()


def test_csv_byte_limit_stops_encoding_at_batch_boundary(tmp_path, monkeypatch):
    import pivot_engine.runtime.export_service as writers
    original = writers.csv.writer
    written = []
    class CountWriter:
        def __init__(self, *args, **kwargs):
            self.writer = original(*args, **kwargs)
        def writerow(self, row):
            written.append(row)
            return self.writer.writerow(row)
    class ManyRows(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            return TanStackResponse(data=[{"value": "long row"} for _ in range(500)], columns=[{"id": "value"}], total_rows=500)
        handle_virtual_scroll_request = handle_request
    monkeypatch.setattr(writers.csv, "writer", CountWriter)
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, ManyRows())
    service._router.export_service = writers.RuntimeExportService(max_bytes=32)
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        wait_for(lambda: snapshot(store).phase == "failed")
        assert len(written) <= 129
        assert list(tmp_path.iterdir()) == []
    finally:
        runner.close()


def test_orphan_sweep_respects_payload_lifetime_not_short_job_ttl(tmp_path):
    old = tmp_path / "pivot_export_valid.csv"
    old.write_text("Value\n1\n")
    os.utime(old, (time.time() - 10, time.time() - 10))
    runner, store, payloads, *_ = setup_runner(tmp_path, store=LocalExportJobStore(ttl_seconds=1))
    try:
        assert old.exists()
    finally:
        runner.close()


def test_stuck_cancellation_emits_operator_signal_without_freeing_slot(tmp_path, caplog):
    entered, release = threading.Event(), threading.Event()
    class Stuck(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                while not release.is_set():
                    await asyncio.sleep(.005)
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(
        tmp_path, Stuck(), cancellation_warning_seconds=.03,
    )
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        assert entered.wait(2)
        runner.signal_cancel(job.job_id)
        wait_for(lambda: "export_cancellation_cleanup_pending" in caplog.text)
        assert snapshot(store).phase == "cancelling"
        assert governor.snapshot()["active_slots"]["export"] == 1
    finally:
        release.set()
        runner.close()


def test_close_stops_local_execution_even_when_cancel_registry_is_unavailable(tmp_path):
    from pivot_engine.runtime.distributed import DistributedRuntimeUnavailable
    class OfflineStore(LocalExportJobStore):
        offline = False
        def request_cancel(self, *args, **kwargs):
            if self.offline:
                raise DistributedRuntimeUnavailable()
            return super().request_cancel(*args, **kwargs)
    entered = threading.Event()
    class Slow(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            entered.set()
            await asyncio.Event().wait()
        handle_virtual_scroll_request = handle_request
    store = OfflineStore()
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, Slow(), store=store)
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        assert entered.wait(2)
        store.offline = True
        runner.close()
        assert governor.snapshot()["active_slots"]["export"] == 0
        assert snapshot(store).phase != "completed"
    finally:
        store.offline = False
        runner.close()


def test_pending_future_cancel_frees_slot_before_acknowledgement(tmp_path):
    entered, release = threading.Event(), threading.Event()
    calls = []
    class Slow(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            calls.append(1)
            entered.set()
            while not release.is_set():
                await asyncio.sleep(.01)
            return await super().handle_request()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(
        tmp_path, Slow(), export_slots=2, max_workers=1, max_pending=1,
    )
    try:
        state = PivotViewState(export_request={"format": "csv"})
        runner.start(job, context, state)
        assert entered.wait(2)
        pending = store.create(ExportJobSnapshot("pending", "alice", "trusted", "sales"))
        runner.start(pending, context, state)
        assert governor.snapshot()["active_slots"]["export"] == 2
        store.request_cancel(pending.job_id)
        wait_for(lambda: snapshot(store, "pending").phase == "cancelled")
        assert governor.snapshot()["active_slots"]["export"] == 1
        assert calls == [1]
        replacement = store.create(ExportJobSnapshot("replacement", "alice", "trusted", "sales"))
        runner.start(replacement, context, state)
        assert governor.snapshot()["active_slots"]["export"] == 2
        runner.signal_cancel(replacement.job_id)
        wait_for(lambda: snapshot(store, "replacement").phase == "cancelled")
        assert governor.snapshot()["active_slots"]["export"] == 1
        assert calls == [1]
    finally:
        release.set()
        runner.close()
        payloads.clear()


def test_periodic_sweep_removes_newly_expired_orphan_and_preserves_unrelated_file(tmp_path):
    runner, *_ = setup_runner(tmp_path)
    stale = tmp_path / "pivot_export_abandoned.csv"
    unrelated = tmp_path / "keep.txt"
    recent = tmp_path / "pivot_export_fresh.csv"
    try:
        stale.write_text("partial")
        unrelated.write_text("keep")
        recent.write_text("fresh")
        os.utime(stale, (1, 1))
        wait_for(lambda: not stale.exists(), timeout=2)
        assert unrelated.read_text() == "keep" and recent.exists()
    finally:
        runner.close()


@pytest.mark.parametrize("fmt", ["csv", "xlsx"])
def test_real_flat_all_rows_exports_use_bounded_arrow_pages_before_python_conversion(tmp_path, monkeypatch, fmt):
    import pyarrow as pa
    from pivot_engine import create_tanstack_adapter
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("sales", pa.table({
        "region": [f"R{i:04d}" for i in range(350)], "sales": list(range(350)),
    }))
    original_query = adapter.controller.run_pivot_async
    limits = []
    async def checked_query(spec, **kwargs):
        limits.append(spec.limit)
        assert 0 < spec.limit <= 128
        assert kwargs["return_format"] == "arrow"
        return await original_query(spec, **kwargs)
    monkeypatch.setattr(adapter.controller, "run_pivot_async", checked_query)
    original_convert = adapter.convert_pivot_result_to_tanstack_format
    seen = []
    def checked_convert(table, *args, **kwargs):
        assert isinstance(table, pa.Table)
        assert table.num_rows <= 128 and table.nbytes <= 6250
        seen.append(table.num_rows)
        return original_convert(table, *args, **kwargs)
    monkeypatch.setattr(adapter, "convert_pivot_result_to_tanstack_format", checked_convert)
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, adapter, governor=adapter.controller.resource_governor)
    state = PivotViewState(row_fields=["region"], val_configs=[{"field": "sales", "agg": "sum"}],
                           layout_mode="tabular", show_subtotals=False, show_col_totals=False,
                           export_request={"format": fmt, "allRows": True})
    try:
        runner.start(job, context, state)
        done = wait_for(lambda: (item := snapshot(store)).phase in {"failed", "completed"} and item, timeout=10)
        assert done.phase == "completed"
        assert done.rows_written == 350
        assert len(limits) >= 3 and sum(seen) == 350
        item = payloads.get(done.payload_ref["id"])
        if fmt == "csv":
            import csv
            with open(item.file_path, encoding="utf-8-sig", newline="") as source:
                rows = list(csv.reader(source))
            assert len(rows) == 351
            assert rows[0] == ["Region", "sales"]
            assert sorted((row[0], float(row[1])) for row in rows[1:]) == [(f"R{i:04d}", float(i)) for i in range(350)]
        else:
            document = read_xlsx_export(item.file_path)
            assert len(document) == 351
            assert sorted((row[0], float(row[1])) for row in document[1:]) == [(f"R{i:04d}", float(i)) for i in range(350)]
    finally:
        runner.close()
        payloads.clear()
        adapter.controller.close()


def test_unsupported_hierarchical_all_rows_rejects_before_adapter_execution(tmp_path):
    called = []
    class Unsafe(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            called.append(True)
            return await super().handle_request()
        handle_hierarchical_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, Unsafe())
    try:
        runner.start(job, context, PivotViewState(row_fields=["region"], layout_mode="hierarchy",
                                                export_request={"format": "csv", "allRows": True}))
        wait_for(lambda: snapshot(store).phase in {"failed", "completed"})
        assert called == []
        assert snapshot(store).phase == "failed"
    finally:
        runner.close()
        payloads.clear()


@pytest.mark.parametrize("all_rows", [False, True])
def test_overwide_arrow_page_rejected_before_to_pylist(tmp_path, monkeypatch, all_rows):
    import pyarrow as pa
    from pivot_engine import create_tanstack_adapter
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("sales", pa.table({"region": ["x" * 100_001], "sales": [1]}))
    conversions = []
    original = adapter.convert_pivot_result_to_tanstack_format
    def convert(*args, **kwargs):
        conversions.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(adapter, "convert_pivot_result_to_tanstack_format", convert)
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, adapter, governor=adapter.controller.resource_governor)
    from pivot_engine.runtime.export_service import RuntimeExportService
    service._router.export_service = RuntimeExportService(max_bytes=6250)
    try:
        runner.start(job, context, PivotViewState(row_fields=["region"], val_configs=[{"field": "sales", "agg": "sum"}],
                                                layout_mode="tabular", show_subtotals=False, show_col_totals=False,
                                                export_request={"format": "csv", "allRows": all_rows, "rowStart": 0, "rowEnd": 1}))
        wait_for(lambda: snapshot(store).phase in {"failed", "completed"})
        assert conversions == []
        assert snapshot(store).phase == "failed"
    finally:
        runner.close()
        payloads.clear()
        adapter.controller.close()


@pytest.mark.parametrize("part", [False, True])
def test_real_bounded_export_preserves_sort_filter_totals_and_part_range(tmp_path, part):
    import pyarrow as pa
    from pivot_engine import create_tanstack_adapter
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("sales", pa.table({
        "region": ["Zürich", "東京", "Éclair", "Excluded"], "sales": [7.5, 8.25, 9.75, 1000.0],
    }))
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, adapter, governor=adapter.controller.resource_governor)
    state = PivotViewState(row_fields=["region"], val_configs=[{"field": "sales", "agg": "sum"}],
                           filters={"region": {"type": "in", "value": ["Zürich", "東京", "Éclair"]}},
                           sorting=[{"id": "region", "desc": True}], layout_mode="tabular", show_subtotals=False,
                           show_col_totals=True, export_request={"format": "csv", "allRows": not part})
    if part:
        state.export_request.update({"rowStart": 1, "rowEnd": 2})
    try:
        runner.start(job, context, state)
        done = wait_for(lambda: (item := snapshot(store)).phase in {"completed", "failed"} and item)
        assert done.phase == "completed"
        rows = read_csv_export(payloads.get(done.payload_ref["id"]).file_path)
        if part:
            assert rows[1:] == [["Éclair", "9.75"]]
        else:
            assert rows[1:] == [["東京", "8.25"], ["Éclair", "9.75"], ["Zürich", "7.5"], ["Grand Total", "25.5"]]
    finally:
        runner.close()
        payloads.clear()
        adapter.controller.close()


def test_row_overflow_rejects_native_page_before_python_conversion(tmp_path, monkeypatch):
    import pyarrow as pa
    from pivot_engine import create_tanstack_adapter
    from pivot_engine.runtime.export_service import RuntimeExportService
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("sales", pa.table({"region": [str(i) for i in range(20)], "sales": list(range(20))}))
    conversions = []
    original = adapter.convert_pivot_result_to_tanstack_format
    def convert(*args, **kwargs):
        conversions.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(adapter, "convert_pivot_result_to_tanstack_format", convert)
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, adapter, governor=adapter.controller.resource_governor)
    service._router.export_service = RuntimeExportService(max_rows=5)
    try:
        runner.start(job, context, PivotViewState(row_fields=["region"], val_configs=[{"field": "sales", "agg": "sum"}],
                                                layout_mode="tabular", show_subtotals=False, show_col_totals=False,
                                                export_request={"format": "csv", "allRows": True}))
        wait_for(lambda: snapshot(store).phase in {"completed", "failed"})
        assert snapshot(store).phase == "failed" and conversions == []
        assert list(tmp_path.iterdir()) == []
    finally:
        runner.close()
        payloads.clear()
        adapter.controller.close()


def test_cancel_between_owner_claim_and_submission_never_executes_adapter(tmp_path):
    class CancellingStore(LocalExportJobStore):
        def claim(self, *args):
            claimed = super().claim(*args)
            self.request_cancel(args[0])
            return claimed
    invoked = []
    class Checked(Adapter):
        async def handle_request(self, *_args, **_kwargs):
            invoked.append(True)
            return await super().handle_request()
        handle_virtual_scroll_request = handle_request
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, Checked(), store=CancellingStore())
    try:
        runner.start(job, context, PivotViewState(export_request={"format": "csv"}))
        wait_for(lambda: snapshot(store).phase == "cancelled")
        assert invoked == []
        assert governor.snapshot()["active_slots"]["export"] == 0
    finally:
        runner.close()
        payloads.clear()


@pytest.mark.parametrize("fmt", ["csv", "xlsx"])
@pytest.mark.parametrize("all_rows", [False, True])
def test_empty_bounded_export_preserves_column_headers(tmp_path, fmt, all_rows):
    import pyarrow as pa
    from pivot_engine import create_tanstack_adapter
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("sales", pa.table({
        "region": pa.array([], type=pa.string()), "sales": pa.array([], type=pa.float64()),
    }))
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, adapter, governor=adapter.controller.resource_governor)
    state = PivotViewState(row_fields=["region"], val_configs=[{"field": "sales", "agg": "sum"}],
                           layout_mode="tabular", show_subtotals=False, show_col_totals=False,
                           export_request={"format": fmt, "allRows": all_rows})
    try:
        runner.start(job, context, state)
        done = wait_for(lambda: (item := snapshot(store)).phase in {"completed", "failed"} and item)
        assert done.phase == "completed" and done.rows_written == 0
        item = payloads.get(done.payload_ref["id"])
        document = read_csv_export(item.file_path) if fmt == "csv" else read_xlsx_export(item.file_path)
        assert document == [["Region", "sales"]]
    finally:
        runner.close()
        payloads.clear()
        adapter.controller.close()


@pytest.mark.parametrize("fmt", ["csv", "xlsx"])
@pytest.mark.parametrize("window_fn", ["percent_of_grand_total", " PERCENT_OF_TOTAL ", "percent_of_col", "percent_of_row"])
def test_bounded_export_rejects_normalized_window_functions_before_native_execution(tmp_path, monkeypatch, fmt, window_fn):
    import pyarrow as pa
    from pivot_engine import create_tanstack_adapter
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("sales", pa.table({
        "region": [f"R{i:04d}" for i in range(350)], "sales": [1.0] * 350,
    }))
    native_calls = []
    original = adapter.controller.run_pivot_async
    async def execute(*args, **kwargs):
        native_calls.append(True)
        return await original(*args, **kwargs)
    monkeypatch.setattr(adapter.controller, "run_pivot_async", execute)
    runner, store, payloads, governor, service, context, job = setup_runner(tmp_path, adapter, governor=adapter.controller.resource_governor)
    state = PivotViewState(row_fields=["region"], val_configs=[{"field": "sales", "agg": "sum", "windowFn": window_fn}],
                           layout_mode="tabular", show_subtotals=False, show_col_totals=False,
                           export_request={"format": fmt, "allRows": True})
    try:
        runner.start(job, context, state)
        done = wait_for(lambda: (item := snapshot(store)).phase in {"completed", "failed"} and item)
        assert native_calls == []
        assert done.phase == "failed" and done.payload_ref is None
        assert list(tmp_path.iterdir()) == []
    finally:
        runner.close()
        payloads.clear()
        adapter.controller.close()
