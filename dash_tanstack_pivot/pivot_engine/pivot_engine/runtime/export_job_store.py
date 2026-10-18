"""Small export control records; execution, files and admission belong to the runner.

Job IDs are server-generated, unique identifiers and must never be recycled.
Only trusted owners use claim/advance/heartbeat. Public callers use authorized
get/request_cancel. A refused claim or fenced write returns ``None``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
import math
import threading
import time
from typing import Any, TypeVar

from .export_jobs import ExportJobSnapshot, EXPORT_JOB_TERMINAL_PHASES, transition_export_job


T = TypeVar("T")


@dataclass(frozen=True)
class _JobRecord:
    snapshot: ExportJobSnapshot
    owner_id: str | None
    deadline: float


class ExportJobStore:
    """Common lifecycle semantics over an atomic single-record transaction.

    ``ttl_seconds`` retains terminal observations and bounds unclaimed queue
    lifetime. A running lease expires after ``lease_seconds`` without heartbeat.
    Lease expiry interrupts and increments the epoch before any owner write.
    It never acknowledges native cleanup or releases an admission reservation.
    """

    def __init__(self, *, ttl_seconds: float = 120, lease_seconds: float = 30,
                 clock: Callable[[], float] = time.time) -> None:
        if any(not math.isfinite(value) or value <= 0 for value in (ttl_seconds, lease_seconds)):
            raise ValueError("Export job TTL and lease must be positive finite durations.")
        self.ttl_seconds = ttl_seconds
        self.lease_seconds = lease_seconds
        self._clock = clock

    def _transaction(self, job_id: str, update: Callable[[_JobRecord | None], tuple[_JobRecord | None, T]]) -> T:
        raise NotImplementedError

    def _job_ids(self) -> list[str]:
        raise NotImplementedError

    def _expire(self, record: _JobRecord | None, now: float) -> _JobRecord | None:
        if record is None or now < record.deadline:
            return record
        if record.snapshot.phase in EXPORT_JOB_TERMINAL_PHASES:
            return None
        snapshot = transition_export_job(record.snapshot, {
            "phase": "interrupted", "owner_epoch": record.snapshot.owner_epoch + 1,
        })
        return _JobRecord(snapshot, None, now + self.ttl_seconds)

    def _run(self, job_id: str, operation: Callable[[_JobRecord | None, float], tuple[_JobRecord | None, T]]) -> T:
        def update(record):
            now = self._clock()
            return operation(self._expire(record, now), now)
        return self._transaction(job_id, update)

    def create(self, snapshot: ExportJobSnapshot) -> ExportJobSnapshot:
        if snapshot.phase != "queued" or snapshot.owner_epoch != 0:
            raise ValueError("Only a new queued export may be created.")

        def create(record, now):
            if record is not None:
                current = record.snapshot
                if (current.subject, current.accounting_account_id, current.table) != (
                    snapshot.subject, snapshot.accounting_account_id, snapshot.table
                ):
                    raise ValueError("Export job ID is already in use.")
                return record, current
            return _JobRecord(snapshot, None, now + self.ttl_seconds), snapshot
        return self._run(snapshot.job_id, create)

    @staticmethod
    def _authorized(snapshot: ExportJobSnapshot, subject: str | None, table: str | None,
                    accounting_account_id: str | None = None) -> bool:
        return ((subject is None or snapshot.subject == subject)
                and (table is None or snapshot.table == table)
                and (accounting_account_id is None or snapshot.accounting_account_id == accounting_account_id))

    def get(self, job_id: str, subject: str, table: str, *,
            accounting_account_id: str | None = None) -> ExportJobSnapshot | None:
        def get(record, _now):
            snapshot = record.snapshot if record else None
            return record, snapshot if snapshot and self._authorized(snapshot, subject, table, accounting_account_id) else None
        return self._run(job_id, get)

    def claim(self, job_id: str, owner_id: str) -> ExportJobSnapshot | None:
        if not isinstance(owner_id, str) or not owner_id:
            raise ValueError("An export owner ID is required.")

        def claim(record, now):
            if record is None or record.snapshot.phase != "queued":
                return record, None
            snapshot = transition_export_job(record.snapshot, {
                "phase": "running", "owner_epoch": record.snapshot.owner_epoch + 1,
            })
            return _JobRecord(snapshot, owner_id, now + self.lease_seconds), snapshot
        return self._run(job_id, claim)

    def request_cancel(self, job_id: str, *, subject: str | None = None, table: str | None = None,
                       accounting_account_id: str | None = None) -> ExportJobSnapshot | None:
        """Record cancellation; queued jobs have no native owner to stop.

        Omitted identity arguments are for trusted internal callers only.
        The gateway supplies authenticated subject/table/account together.
        """
        def cancel(record, now):
            if record is None or not self._authorized(record.snapshot, subject, table, accounting_account_id):
                return record, None
            phase = record.snapshot.phase
            if phase not in {"queued", "running"}:
                return record, record.snapshot
            snapshot = transition_export_job(record.snapshot, "cancelled" if phase == "queued" else "cancelling")
            updated = replace(record, snapshot=snapshot,
                              deadline=now + self.ttl_seconds if phase == "queued" else record.deadline)
            return updated, snapshot
        return self._run(job_id, cancel)

    def advance(self, job_id: str, owner_epoch: int, event: Mapping[str, Any] | str) -> ExportJobSnapshot | None:
        fields = {"phase": event} if isinstance(event, str) else dict(event)
        if "owner_epoch" in fields:
            raise ValueError("Only the store may change an export owner epoch.")

        def advance(record, now):
            if record is None or record.owner_id is None or record.snapshot.owner_epoch != owner_epoch:
                return record, None
            snapshot = record.snapshot
            # Cancellation/completion and terminal races are ordinary CAS losses.
            if snapshot.phase in EXPORT_JOB_TERMINAL_PHASES or (
                snapshot.phase == "cancelling" and fields.get("phase") == "completed"
            ):
                try:
                    updated = transition_export_job(snapshot, fields)
                except ValueError:
                    return record, None
                return record, updated
            updated = transition_export_job(snapshot, fields)
            deadline = now + self.ttl_seconds if updated.phase in EXPORT_JOB_TERMINAL_PHASES else record.deadline
            return replace(record, snapshot=updated, deadline=deadline), updated
        return self._run(job_id, advance)

    def heartbeat(self, job_id: str, owner_epoch: int) -> bool:
        def heartbeat(record, now):
            if (record is None or record.owner_id is None or record.snapshot.owner_epoch != owner_epoch
                    or record.snapshot.phase not in {"running", "cancelling"}):
                return record, False
            return replace(record, deadline=now + self.lease_seconds), True
        return self._run(job_id, heartbeat)

    def interrupt_expired(self, now: float | None = None) -> list[ExportJobSnapshot]:
        timestamp = self._clock() if now is None else now
        interrupted = []
        for job_id in self._job_ids():
            def expire(record):
                updated = self._expire(record, timestamp)
                changed = updated is not None and record is not None and updated.snapshot != record.snapshot
                return updated, updated.snapshot if changed else None
            snapshot = self._transaction(job_id, expire)
            if snapshot is not None:
                interrupted.append(snapshot)
        return interrupted


class LocalExportJobStore(ExportJobStore):
    """Thread-safe registry for a single worker, without a Redis dependency."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._records: dict[str, _JobRecord] = {}
        self._lock = threading.RLock()

    def _transaction(self, job_id, update):
        with self._lock:
            record, result = update(self._records.get(job_id))
            if record is None:
                self._records.pop(job_id, None)
            else:
                self._records[job_id] = record
            return result

    def _job_ids(self):
        with self._lock:
            return list(self._records)
