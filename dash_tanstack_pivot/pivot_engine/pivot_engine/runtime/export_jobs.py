"""Immutable export observations and lifecycle rules, with no storage or I/O.

Identity and accounting are supplied by trusted server context. This model
never derives either from a request envelope. Stores serialize private fields
separately; only ``to_payload`` is suitable for a public runtime response.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Optional


EXPORT_JOB_PHASES = frozenset({"queued", "running", "cancelling", "cancelled", "completed", "failed", "interrupted"})
EXPORT_JOB_TERMINAL_PHASES = frozenset({"cancelled", "completed", "failed", "interrupted"})
_TRANSITIONS = {
    "queued": frozenset({"running", "cancelled", "failed", "interrupted"}),
    "running": frozenset({"cancelling", "completed", "failed", "interrupted"}),
    "cancelling": frozenset({"cancelled", "failed", "interrupted"}),
}


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise ValueError("Payload references must contain only JSON values.")


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _nonnegative_integer(name: str, value: Any) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")


@dataclass(frozen=True)
class ExportJobSnapshot:
    job_id: str
    subject: str
    accounting_account_id: str
    table: str
    phase: str = "queued"
    rows_written: int = 0
    bytes_written: int = 0
    total_rows: Optional[int] = None
    owner_epoch: int = 0
    payload_ref: Optional[Mapping[str, Any]] = None

    def __post_init__(self) -> None:
        for name in ("job_id", "subject", "accounting_account_id", "table"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a non-empty string.")
        if self.phase not in EXPORT_JOB_PHASES:
            raise ValueError("Unknown export job phase.")
        for name in ("rows_written", "bytes_written", "owner_epoch"):
            _nonnegative_integer(name, getattr(self, name))
        if self.total_rows is not None:
            _nonnegative_integer("total_rows", self.total_rows)
            if self.total_rows < self.rows_written:
                raise ValueError("Observed rows cannot exceed the measured total.")
        if self.phase == "completed":
            if not isinstance(self.payload_ref, Mapping) or not self.payload_ref:
                raise ValueError("A completed job requires a payload reference.")
        elif self.payload_ref is not None:
            raise ValueError("Only a completed job may expose a payload reference.")
        if self.payload_ref is not None:
            object.__setattr__(self, "payload_ref", _freeze(self.payload_ref))

    def to_payload(self) -> dict[str, Any]:
        """Return detached public observations, excluding authorization identity."""
        payload = {"jobId": self.job_id, "phase": self.phase,
                   "rowsWritten": self.rows_written, "bytesWritten": self.bytes_written,
                   "totalRows": self.total_rows}
        if self.payload_ref is not None:
            payload["payloadRef"] = _thaw(self.payload_ref)
        return payload


def transition_export_job(snapshot: ExportJobSnapshot, event: Mapping[str, Any] | str) -> ExportJobSnapshot:
    """Apply an observation atomically in the caller's store transaction.

    ``event`` is a phase name or a mapping of phase and observed snake-case
    fields. The store must compare its owner lease/epoch before invoking this
    function; this model additionally refuses epoch regression. Cancellation
    acknowledgement is supplied only by the owner after execution cleanup.
    Exact terminal replays are idempotent; terminal mutations are fenced.
    """
    fields = {"phase": event} if isinstance(event, str) else dict(event)
    allowed = {"phase", "rows_written", "bytes_written", "total_rows", "owner_epoch", "payload_ref"}
    if set(fields) - allowed:
        raise ValueError("An export event cannot change job identity or accounting.")
    phase = fields.get("phase", snapshot.phase)
    if phase != snapshot.phase and phase not in _TRANSITIONS.get(snapshot.phase, ()):
        raise ValueError(f"Invalid export transition {snapshot.phase} -> {phase}.")
    updated = replace(snapshot, **fields)
    for name in ("rows_written", "bytes_written", "owner_epoch"):
        if getattr(updated, name) < getattr(snapshot, name):
            raise ValueError(f"{name} cannot decrease.")
    if snapshot.phase in EXPORT_JOB_TERMINAL_PHASES and updated != snapshot:
        raise ValueError("Terminal export jobs cannot be changed.")
    return updated
