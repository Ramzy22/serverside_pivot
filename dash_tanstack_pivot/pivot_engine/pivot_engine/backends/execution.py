"""Statement-owned cancellation primitives for physical database leases."""

from __future__ import annotations

import threading
from typing import Any, Literal, Optional


class DatabaseExecutionError(RuntimeError):
    """A database execution failed without exposing driver details to callers."""

    code = "database_execution_failed"

    def __init__(self, detail: Optional[str] = None) -> None:
        super().__init__(self.code)
        self.detail = detail


class QueryCancelledError(DatabaseExecutionError):
    """The active physical statement was cancelled by a client request."""

    code = "query_cancelled"


class QueryTimeoutError(DatabaseExecutionError):
    """The active physical statement exceeded its configured deadline."""

    code = "query_timeout"


class CancellationUnavailableError(DatabaseExecutionError):
    """The backend cannot make the requested database-level cancellation guarantee."""

    code = "cancellation_unavailable"


CancellationReason = Literal["client_cancelled", "query_timeout"]


class DatabaseExecution:
    """Own one statement's cancellation state and exact physical lease.

    A cancellation can arrive before the executor thread obtains its lease. In
    that case ``attach_lease`` applies the already-recorded cancellation to the
    newly leased cursor before it can be reused for normal work.
    """

    def __init__(self, query_id: str) -> None:
        self.query_id = str(query_id)
        self._lock = threading.RLock()
        self._completion = threading.Event()
        self._lease: Optional[Any] = None
        self._reason: Optional[CancellationReason] = None
        self._finished = False
        self._cancel_invoked = False
        self._cancel_succeeded = False
        self._cancel_error: Optional[BaseException] = None

    @property
    def reason(self) -> Optional[CancellationReason]:
        with self._lock:
            return self._reason

    @property
    def cancellation_requested(self) -> bool:
        return self.reason is not None

    @property
    def cancellation_succeeded(self) -> bool:
        with self._lock:
            return self._cancel_succeeded

    @property
    def cancel_error(self) -> Optional[BaseException]:
        with self._lock:
            return self._cancel_error

    @property
    def finished(self) -> bool:
        with self._lock:
            return self._finished

    def attach_lease(self, lease: Any) -> None:
        """Attach the physical statement lease and honour a pending cancel."""
        with self._lock:
            if self._lease is not None:
                raise RuntimeError("A database execution already owns a lease.")
            self._lease = lease
            cancellation = self._take_pending_cancellation_locked()
        if cancellation is not None:
            self._invoke_native_cancel(*cancellation)

    def request_cancel(self, reason: CancellationReason) -> bool:
        """Request one idempotent cancellation of this execution's exact lease."""
        if reason not in {"client_cancelled", "query_timeout"}:
            raise ValueError("Unsupported database cancellation reason.")
        with self._lock:
            if self._finished or self._reason is not None:
                return False
            self._reason = reason
            cancellation = self._take_pending_cancellation_locked()
        if cancellation is not None:
            self._invoke_native_cancel(*cancellation)
        return True

    def finish(self) -> None:
        """Acknowledge worker cleanup after it returned or retired its lease."""
        with self._lock:
            self._finished = True
            self._completion.set()

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait until the worker has returned or discarded the statement lease."""
        return self._completion.wait(timeout)

    def _take_pending_cancellation_locked(self) -> Optional[tuple[Any, Any]]:
        if (
            self._reason is None
            or self._lease is None
            or self._cancel_invoked
        ):
            return None
        self._cancel_invoked = True
        cancel = getattr(self._lease, "interrupt", None)
        if not callable(cancel):
            cancel = getattr(self._lease, "cancel", None)
        return self._lease, cancel

    def _invoke_native_cancel(self, _lease: Any, cancel: Any) -> None:
        if not callable(cancel):
            with self._lock:
                self._cancel_error = CancellationUnavailableError()
            return
        try:
            cancel()
        except BaseException as exc:  # The worker owns safe lease retirement.
            with self._lock:
                self._cancel_error = exc
        else:
            with self._lock:
                self._cancel_succeeded = True
