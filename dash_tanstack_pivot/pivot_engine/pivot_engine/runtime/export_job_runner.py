"""Bounded worker ownership of export execution, cancellation and file delivery."""
from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
import logging
import math
import os
from pathlib import Path
import tempfile
import threading
import time
import uuid

from ..resource_governor import ResourceExhaustedError, ResourceKind, ResourceLease, current_workload_account
from .export_jobs import ExportJobSnapshot, EXPORT_JOB_TERMINAL_PHASES
from .models import PivotRequestContext, PivotViewState
from .export_service import ExportWriteControl, current_export_write


log = logging.getLogger(__name__)


@dataclass
class _Execution:
    job: ExportJobSnapshot
    context: PivotRequestContext
    lease: ResourceLease
    control: ExportWriteControl
    epoch: int = 0
    loop: asyncio.AbstractEventLoop | None = None
    task: asyncio.Task | None = None
    cancel_sent: bool = False
    cancel_started: float | None = None
    cancel_warned: bool = False
    future: Future | None = None
    resources_released: bool = False
    finalized: bool = False
    cleanup_last_logged: float = 0


class ExportJobRunner:
    """One preadmitted lease per submitted job; status stores never own leases.

    ``service`` is a PivotRuntimeService. start expects an already-created job
    with authenticated context; public authorization belongs to its gateway.
    close waits for native cleanup, including backends that refuse interruption.
    """

    def __init__(self, service, job_store, payload_store, *, max_workers=2, max_pending=2,
                 temp_directory=None, poll_interval_seconds=.1, cancellation_warning_seconds=5,
                 payload_metadata=None):
        if (max_workers < 1 or max_pending < 0
                or any(not math.isfinite(value) or value <= 0
                       for value in (poll_interval_seconds, cancellation_warning_seconds))):
            raise ValueError("Invalid export executor bounds.")
        self.service, self.job_store, self.payload_store = service, job_store, payload_store
        self._payload_metadata = dict(payload_metadata or {})
        self.owner_id = uuid.uuid4().hex
        self.directory = Path(temp_directory or Path(tempfile.gettempdir()) / "pivot_export_jobs").resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.job_store.interrupt_expired()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="pivot-export")
        self._capacity = threading.BoundedSemaphore(max_workers + max_pending)
        self._lock = threading.RLock()
        self._active = {}
        self._sweep_orphans()
        self._next_sweep = time.monotonic() + 1
        self._closed = False
        self._stop = threading.Event()
        self._poll = min(poll_interval_seconds, job_store.lease_seconds / 3)
        self._cancel_warning = cancellation_warning_seconds
        self._monitor = threading.Thread(target=self._monitor_jobs, name="pivot-export-control", daemon=True)
        self._monitor.start()

    def start(self, job: ExportJobSnapshot, context: PivotRequestContext, state: PivotViewState) -> None:
        if context.table != job.table or (context.accounting_account.tenant_id or "system") != job.accounting_account_id:
            raise ValueError("Export context does not match the trusted job identity.")
        with self._lock:
            if self._closed:
                raise RuntimeError("Export runner is closed.")
            if job.job_id in self._active:
                return
            # Establish ownership before admission: cancellation of an unowned
            # queued record may acknowledge immediately, so it must own no slot.
            claimed = self.job_store.claim(job.job_id, self.owner_id)
            if claimed is None:
                return
            if not self._capacity.acquire(blocking=False):
                self._acknowledge_stopped(job, claimed.owner_epoch)
                raise ResourceExhaustedError(ResourceKind.EXPORT, reason="export_executor_full")
            lease = None
            try:
                lifecycle = self.service._router.lifecycle
                lease = lifecycle.resource_governor.reserve(
                    ResourceKind.EXPORT, lifecycle.reservation_bytes(ResourceKind.EXPORT),
                    account=context.accounting_account,
                )
                control = ExportWriteControl(str(self.directory), lease, lambda rows, size: None,
                                             ensure_bytes=self.service._router.export_service.limits.ensure_bytes)
                execution = _Execution(job, context, lease, control, epoch=claimed.owner_epoch)
                control.progress = lambda rows, size: self._progress(execution, rows, size)
                self._active[job.job_id] = execution
                execution.future = self._executor.submit(self._run, execution, deepcopy(state))
                execution.future.add_done_callback(
                    lambda future: self._finalize(execution) if future.cancelled() else None,
                )
            except BaseException:
                self._active.pop(job.job_id, None)
                if lease is not None:
                    lease.release()
                self._capacity.release()
                self._acknowledge_stopped(job, claimed.owner_epoch)
                raise

    def signal_cancel(self, job_id: str) -> None:
        try:
            self.job_store.request_cancel(job_id)
        finally:
            with self._lock:
                execution = self._active.get(job_id)
                if execution:
                    self._cancel(execution)

    def _cancel(self, execution):
        execution.control.cancelled.set()
        if execution.cancel_started is None:
            execution.cancel_started = time.monotonic()
        if execution.future is not None and execution.future.cancel():
            return  # done callback releases admission before acknowledgement
        if execution.loop is not None and execution.task is not None and not execution.cancel_sent:
            execution.cancel_sent = True
            try:
                execution.loop.call_soon_threadsafe(execution.task.cancel)
            except RuntimeError:
                pass  # the loop has exited; synchronous final cleanup owns it

    def _monitor_jobs(self):
        while not self._stop.wait(self._poll):
            try:
                if time.monotonic() >= self._next_sweep:
                    self._sweep_orphans()
                    self._next_sweep = time.monotonic() + 1
                self.job_store.interrupt_expired()
                with self._lock:
                    executions = list(self._active.values())
                for execution in executions:
                    if execution.future is not None and execution.future.done():
                        # Cleanup can outlive the database task. Keep the
                        # cancelling owner lease alive until local unlink and
                        # terminal acknowledgement really finish.
                        if execution.epoch:
                            self.job_store.heartbeat(execution.job.job_id, execution.epoch)
                        self._finalize(execution)
                        continue
                    if not execution.epoch:
                        continue
                    valid = self.job_store.heartbeat(execution.job.job_id, execution.epoch)
                    for path in list(execution.control.paths):
                        try:
                            os.utime(path, None)
                        except FileNotFoundError:
                            pass
                    current = self.job_store.get(execution.job.job_id, execution.job.subject, execution.job.table)
                    if not valid or current is None or current.phase != "running":
                        with self._lock:
                            self._cancel(execution)
                    if (execution.cancel_started is not None and not execution.cancel_warned
                            and time.monotonic() - execution.cancel_started >= self._cancel_warning):
                        execution.cancel_warned = True
                        log.error("export_cancellation_cleanup_pending: %s", execution.job.job_id)
            except Exception:
                log.exception("Export coordination unavailable; stopping local owners")
                with self._lock:
                    for execution in list(self._active.values()):
                        self._cancel(execution)
            with self._lock:
                if self._closed and not self._active:
                    self._stop.set()

    def _sweep_orphans(self):
        cutoff = time.time() - max(self.job_store.ttl_seconds, self.job_store.lease_seconds,
                                   self.payload_store.default_ttl_seconds)
        with self._lock:
            live_paths = {path for execution in self._active.values() for path in execution.control.paths}
        for path in self.directory.iterdir():
            try:
                if (str(path) not in live_paths and not path.is_symlink() and path.is_file()
                        and path.name.startswith(("pivot_export_", "pivot_stream_"))
                        and path.stat().st_mtime < cutoff):
                    path.unlink(missing_ok=True)
            except FileNotFoundError:
                pass

    def _progress(self, execution, rows, size):
        event = {}
        if rows is not None:
            event["rows_written"] = rows
        if size is not None:
            event["bytes_written"] = size
        if self.job_store.advance(execution.job.job_id, execution.epoch, event) is None:
            execution.control.cancelled.set()
        execution.control.checkpoint()

    def publish_completed(self, job_id, content_path, metadata):
        """Stage privately, commit the fenced reference, revoke on a lost race."""
        with self._lock:
            execution = self._active[job_id]
        reference = None
        try:
            os.utime(content_path, None)
            reference = self.payload_store.put_file(
                content_path, content_type=metadata.get("contentType", "application/octet-stream"),
                metadata={**self._payload_metadata,
                          **{k: v for k, v in metadata.items() if k not in {"contentPath", "contentLength"}},
                          "subject": execution.job.subject, "table": execution.job.table,
                          "accountingAccountId": execution.job.accounting_account_id,
                          **({"dashSessionSubject": execution.context.session_id}
                             if execution.context.session_id != f"principal:{execution.job.subject}" else {})},
            )
            result = self.job_store.advance(job_id, execution.epoch, {"phase": "completed", "payload_ref": reference})
            if result is not None and result.phase == "completed":
                execution.control.paths.discard(content_path)  # payload store now owns it
                return result
        finally:
            if reference is not None and content_path in execution.control.paths:
                self.payload_store.delete(reference["id"])
        return None

    async def _execute(self, execution, state):
        with self._lock:
            execution.loop = asyncio.get_running_loop()
            execution.task = asyncio.current_task()
            if execution.control.cancelled.is_set():
                self._cancel(execution)
        execution.control.checkpoint()
        current = self.job_store.get(execution.job.job_id, execution.job.subject, execution.job.table)
        if current is None or current.owner_epoch != execution.epoch or current.phase != "running":
            execution.control.cancelled.set()
            execution.control.checkpoint()
        response = await self.service._router.process_async(state, execution.context, export_lease=execution.lease)
        if response.status != "export" or not response.export_payload:
            raise RuntimeError("Export failed before publication")
        execution.control.checkpoint()
        payload = response.export_payload
        self.publish_completed(execution.job.job_id, payload["contentPath"], payload)

    def _run(self, execution, state):
        job_id = execution.job.job_id
        token = current_export_write.set(execution.control)
        account_token = current_workload_account.set(execution.context.accounting_account)
        try:
            asyncio.run(self._execute(execution, state))
        except BaseException:
            if not execution.control.cancelled.is_set():
                log.exception("Export execution failed: %s", job_id)
        finally:
            self._finalize(execution)
            current_export_write.reset(token)
            current_workload_account.reset(account_token)

    def _acknowledge_stopped(self, job, epoch):
        current = self.job_store.get(job.job_id, job.subject, job.table)
        if current and current.phase not in EXPORT_JOB_TERMINAL_PHASES:
            self.job_store.advance(job.job_id, epoch, "cancelled" if current.phase == "cancelling" else "failed")

    def _finalize(self, execution):
        with self._lock:
            if execution.finalized:
                return
            try:
                # Either asyncio.run awaited native/writer cleanup, or the
                # future was cancelled before any execution began.
                if not execution.resources_released:
                    for path in list(execution.control.paths):
                        Path(path).unlink(missing_ok=True)
                        execution.control.paths.discard(path)
                    execution.lease.release()
                    self._capacity.release()
                    execution.resources_released = True
                self._acknowledge_stopped(execution.job, execution.epoch)
                self._active.pop(execution.job.job_id, None)
                execution.finalized = True
            except Exception:
                now = time.monotonic()
                if now - execution.cleanup_last_logged >= 5:
                    log.exception("Export cleanup/terminal acknowledgement failed: %s", execution.job.job_id)
                    execution.cleanup_last_logged = now

    def close(self) -> None:
        with self._lock:
            self._closed = True
            job_ids = list(self._active)
        for job_id in job_ids:
            try:
                self.signal_cancel(job_id)
            except Exception:
                log.exception("Export registry unavailable during shutdown: %s", job_id)
        self._executor.shutdown(wait=True)
        # Do not let process shutdown abandon locked partial files or an
        # unacknowledged cancellation. The worker threads are finished, but
        # final unlink/registry work can still require a retry (notably on
        # Windows while a file handle is being released).
        while True:
            with self._lock:
                for execution in list(self._active.values()):
                    if execution.future is not None and execution.future.done():
                        self._finalize(execution)
                if not self._active:
                    self._stop.set()
                    break
            time.sleep(self._poll)
        self._monitor.join()
