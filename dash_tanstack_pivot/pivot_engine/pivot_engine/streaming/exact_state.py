"""Exact, reversible aggregate state for trusted CDC streams.

The module deliberately serializes only explicit JSON values.  It is separate
from the legacy cache state so durable CDC checkpoints cannot inherit a broad
object deserializer.
"""

from __future__ import annotations

import base64
import copy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import heapq
import json
import math
import os
from threading import RLock
import time
import weakref
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence

from ..resource_governor import (
    ResourceExhaustedError, ResourceGovernor, ResourceKind, ResourceLease,
    SYSTEM_ACCOUNT, WorkloadAccount,
    current_workload_account, get_worker_resource_governor,
)


_STATE_FORMAT = b"pivot:cdc-exact-state:v1\x00"
_SUPPORTED_AGGREGATIONS = frozenset({"sum", "count", "avg", "min", "max"})
_DEFAULT_CDC_MAX_GROUPS = 100_000
_DEFAULT_CDC_MAX_BYTES = 128 * 1024 * 1024
_DEFAULT_CDC_MAX_EXTREMA_VALUES = 1_000_000


def _configured_job_ttl(value: Optional[float]) -> Optional[float]:
    if value is None:
        raw = os.getenv("PIVOT_CDC_JOB_TTL_SECONDS")
        if raw is None:
            return None
        try:
            value = float(raw)
        except ValueError as exc:
            raise ValueError("PIVOT_CDC_JOB_TTL_SECONDS must be positive") from exc
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("CDC job TTL must be positive")
    return float(value)


class ExactCDCStateError(ValueError):
    """Raised when an event cannot be represented in exact aggregate state."""


@dataclass(frozen=True)
class CDCStateLimits:
    """Hard caps for one exact CDC snapshot.

    Extrema state tracks each distinct, canonically encoded value.  Counting
    those entries (rather than rows) preserves exact duplicate handling while
    bounding the part of CDC state that can otherwise grow indefinitely.
    """

    max_groups: int = _DEFAULT_CDC_MAX_GROUPS
    max_bytes: int = _DEFAULT_CDC_MAX_BYTES
    max_extrema_values: int = _DEFAULT_CDC_MAX_EXTREMA_VALUES

    def __post_init__(self) -> None:
        for name in ("max_groups", "max_bytes", "max_extrema_values"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


class CDCStateBudgetExceeded(ExactCDCStateError):
    """Raised before exact CDC state exceeds a configured hard limit."""

    code = "cdc_state_budget_exceeded"


class StateVersionConflict(ExactCDCStateError):
    """Raised when a state snapshot was superseded before it could be saved."""


class _ReverseOrder:
    """Heap wrapper that reverses tuple ordering without mutating values."""

    __slots__ = ("value",)

    def __init__(self, value: tuple[Any, ...]) -> None:
        self.value = value

    def __lt__(self, other: "_ReverseOrder") -> bool:
        return self.value > other.value


class CanonicalScalarCodec:
    """Versioned, type-preserving codec for supported pivot scalar values."""

    version = 1

    @classmethod
    def encode(cls, value: Any) -> str:
        if value is None:
            type_name, encoded_value = "none", None
        elif isinstance(value, bool):
            type_name, encoded_value = "bool", value
        elif isinstance(value, int):
            type_name, encoded_value = "int", str(value)
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ExactCDCStateError("CDC scalar floats must be finite.")
            type_name, encoded_value = "float", repr(value)
        elif isinstance(value, Decimal):
            type_name, encoded_value = "decimal", str(value)
        elif isinstance(value, str):
            type_name, encoded_value = "str", value
        elif isinstance(value, bytes):
            type_name = "bytes"
            encoded_value = base64.b64encode(value).decode("ascii")
        elif isinstance(value, datetime):
            type_name, encoded_value = "datetime", value.isoformat()
        elif isinstance(value, date):
            type_name, encoded_value = "date", value.isoformat()
        else:
            raise ExactCDCStateError(
                "CDC scalar type is not supported for exact aggregation."
            )
        return json.dumps(
            {"version": cls.version, "type": type_name, "value": encoded_value},
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _payload(cls, encoded: str) -> Dict[str, Any]:
        try:
            payload = json.loads(encoded)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExactCDCStateError("CDC scalar payload is invalid.") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("version") != cls.version
            or not isinstance(payload.get("type"), str)
            or "value" not in payload
        ):
            raise ExactCDCStateError("CDC scalar payload has an unsupported format.")
        return payload

    @classmethod
    def decode(cls, encoded: str) -> Any:
        payload = cls._payload(encoded)
        type_name = payload["type"]
        value = payload["value"]
        try:
            if type_name == "none" and value is None:
                return None
            if type_name == "bool" and isinstance(value, bool):
                return value
            if type_name == "int" and isinstance(value, str):
                return int(value)
            if type_name == "float" and isinstance(value, str):
                decoded = float(value)
                if not math.isfinite(decoded):
                    raise ValueError("non-finite float")
                return decoded
            if type_name == "decimal" and isinstance(value, str):
                return Decimal(value)
            if type_name == "str" and isinstance(value, str):
                return value
            if type_name == "bytes" and isinstance(value, str):
                return base64.b64decode(value.encode("ascii"), validate=True)
            if type_name == "date" and isinstance(value, str):
                return date.fromisoformat(value)
            if type_name == "datetime" and isinstance(value, str):
                return datetime.fromisoformat(value)
        except (ValueError, InvalidOperation, TypeError) as exc:
            raise ExactCDCStateError("CDC scalar payload value is invalid.") from exc
        raise ExactCDCStateError("CDC scalar payload type is invalid.")

    @classmethod
    def order_key(cls, encoded: str) -> tuple[Any, ...]:
        payload = cls._payload(encoded)
        type_name = payload["type"]
        value = cls.decode(encoded)
        if type_name in {"int", "float", "decimal"}:
            return ("number", Decimal(str(value)))
        if type_name == "none":
            return ("none", "")
        if type_name == "bool":
            return ("bool", int(value))
        if type_name == "bytes":
            return ("bytes", value)
        if type_name in {"date", "datetime"}:
            return ("temporal", value.isoformat())
        return (type_name, value)


class ExactMeasureAccumulator:
    """Reversible state for one aggregate measure in one logical group."""

    def __init__(self, aggregation: str) -> None:
        normalized = str(aggregation or "").strip().lower()
        if normalized not in _SUPPORTED_AGGREGATIONS:
            raise ExactCDCStateError("CDC aggregation is not supported.")
        self.aggregation = normalized
        self.total: Any = 0
        self.count = 0
        self.value_counts: Dict[str, int] = {}
        self._min_heap: list[tuple[tuple[Any, ...], str]] = []
        self._max_heap: list[tuple[_ReverseOrder, str]] = []

    @staticmethod
    def _numeric(value: Any) -> Any:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise ExactCDCStateError(
                "CDC sum and average measures require numeric values."
            )
        if isinstance(value, float) and not math.isfinite(value):
            raise ExactCDCStateError("CDC numeric values must be finite.")
        return value

    def add(self, value: Any) -> None:
        if self.aggregation == "count":
            self.count += 1
            return
        if self.aggregation in {"sum", "avg"}:
            if value is None:
                return
            numeric = self._numeric(value)
            self.total += numeric
            self.count += 1
            return
        if value is None:
            return
        encoded = CanonicalScalarCodec.encode(value)
        previous_count = self.value_counts.get(encoded, 0)
        self.value_counts[encoded] = previous_count + 1
        if previous_count == 0:
            order_key = CanonicalScalarCodec.order_key(encoded)
            heapq.heappush(self._min_heap, (order_key, encoded))
            heapq.heappush(self._max_heap, (_ReverseOrder(order_key), encoded))

    def remove(self, value: Any) -> None:
        if self.aggregation == "count":
            if self.count <= 0:
                raise ExactCDCStateError("CDC removal has no matching contribution.")
            self.count -= 1
            return
        if self.aggregation in {"sum", "avg"}:
            if value is None:
                return
            if self.count <= 0:
                raise ExactCDCStateError("CDC removal has no matching contribution.")
            numeric = self._numeric(value)
            self.total -= numeric
            self.count -= 1
            return
        if value is None:
            return
        encoded = CanonicalScalarCodec.encode(value)
        remaining = self.value_counts.get(encoded, 0) - 1
        if remaining < 0:
            raise ExactCDCStateError("CDC removal has no matching contribution.")
        if remaining:
            self.value_counts[encoded] = remaining
        else:
            # Heaps retain a stale entry until their next lookup, but retaining
            # a zero-count key would make historical extrema unbounded.
            self.value_counts.pop(encoded, None)

    def _extremum(self, heap: list[Any]) -> Any:
        while heap:
            encoded = heap[0][1]
            if self.value_counts.get(encoded, 0) > 0:
                return CanonicalScalarCodec.decode(encoded)
            heapq.heappop(heap)
        return None

    def result(self) -> Any:
        if self.aggregation == "sum":
            return self.total
        if self.aggregation == "count":
            return self.count
        if self.aggregation == "avg":
            return self.total / self.count if self.count else 0
        if self.aggregation == "min":
            return self._extremum(self._min_heap)
        return self._extremum(self._max_heap)

    def is_empty(self) -> bool:
        if self.aggregation in {"sum", "count", "avg"}:
            return self.count == 0
        return not any(count > 0 for count in self.value_counts.values())

    def to_payload(self) -> Dict[str, Any]:
        return {
            "aggregation": self.aggregation,
            "total": CanonicalScalarCodec.encode(self.total),
            "count": self.count,
            "value_counts": dict(self.value_counts),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ExactMeasureAccumulator":
        accumulator = cls(str(payload.get("aggregation") or ""))
        total = payload.get("total")
        accumulator.total = CanonicalScalarCodec.decode(total) if isinstance(total, str) else 0
        accumulator.count = int(payload.get("count") or 0)
        raw_counts = payload.get("value_counts")
        if not isinstance(raw_counts, dict):
            raise ExactCDCStateError("CDC accumulator value counts are invalid.")
        for encoded, count in raw_counts.items():
            if not isinstance(encoded, str) or not isinstance(count, int) or count < 0:
                raise ExactCDCStateError("CDC accumulator value counts are invalid.")
            accumulator.value_counts[encoded] = count
            if count > 0:
                order_key = CanonicalScalarCodec.order_key(encoded)
                heapq.heappush(accumulator._min_heap, (order_key, encoded))
                heapq.heappush(accumulator._max_heap, (_ReverseOrder(order_key), encoded))
        return accumulator


@dataclass
class ExactGroupState:
    """All measure accumulators for one grouping tuple."""

    row_count: int = 0
    measures: Dict[str, ExactMeasureAccumulator] = field(default_factory=dict)

    @staticmethod
    def _measure_value(measure: Any, name: str) -> Any:
        if isinstance(measure, Mapping):
            return measure.get(name)
        return getattr(measure, name, None)

    def apply(self, row: Mapping[str, Any], sign: int, measures: Sequence[Any]) -> None:
        if sign not in {-1, 1}:
            raise ExactCDCStateError("CDC group transitions require sign -1 or 1.")
        next_row_count = self.row_count + sign
        if next_row_count < 0:
            raise ExactCDCStateError("CDC group removal has no matching contribution.")
        for measure in measures:
            field_name = str(self._measure_value(measure, "field") or "")
            alias = str(self._measure_value(measure, "alias") or field_name)
            aggregation = str(self._measure_value(measure, "agg") or "").lower()
            if not field_name or field_name not in row:
                continue
            accumulator = self.measures.get(alias)
            if accumulator is None:
                accumulator = ExactMeasureAccumulator(aggregation)
                self.measures[alias] = accumulator
            elif accumulator.aggregation != aggregation:
                raise ExactCDCStateError("CDC measure alias has conflicting aggregation.")
            if sign > 0:
                accumulator.add(row[field_name])
            else:
                accumulator.remove(row[field_name])
        self.row_count = next_row_count

    def result(self) -> Dict[str, Any]:
        return {alias: accumulator.result() for alias, accumulator in self.measures.items()}

    def to_payload(self) -> Dict[str, Any]:
        return {
            "row_count": self.row_count,
            "measures": {
                alias: accumulator.to_payload()
                for alias, accumulator in self.measures.items()
            },
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ExactGroupState":
        group = cls(row_count=int(payload.get("row_count") or 0))
        if group.row_count < 0:
            raise ExactCDCStateError("CDC group row count is invalid.")
        raw_measures = payload.get("measures")
        if not isinstance(raw_measures, dict):
            raise ExactCDCStateError("CDC group measures are invalid.")
        group.measures = {
            str(alias): ExactMeasureAccumulator.from_payload(value)
            for alias, value in raw_measures.items()
            if isinstance(alias, str) and isinstance(value, Mapping)
        }
        if len(group.measures) != len(raw_measures):
            raise ExactCDCStateError("CDC group measures are invalid.")
        return group


@dataclass
class CDCJobSnapshot:
    """Persisted job state and source checkpoints."""

    job_id: str
    version: int = 0
    groups: Dict[str, ExactGroupState] = field(default_factory=dict)
    checkpoints: Dict[str, tuple[int, str]] = field(default_factory=dict)
    status: str = "uninitialized"
    invalidation_reason: Optional[str] = None

    @classmethod
    def empty(cls, job_id: str) -> "CDCJobSnapshot":
        return cls(job_id=str(job_id))

    def clone(self) -> "CDCJobSnapshot":
        return self.from_payload(self.job_id, self.to_payload())

    def checkpoint(self, partition: str) -> Optional[tuple[int, str]]:
        return self.checkpoints.get(str(partition or "0"))

    def accept(self, partition: str, sequence: int, event_id: str) -> str:
        existing = self.checkpoint(partition)
        if existing is None:
            return "accept"
        previous_sequence, previous_event_id = existing
        # Once a position has been accepted, every lower position was already
        # applied: strict contiguous processing rejects gaps before they can
        # advance the checkpoint. Reconnects commonly replay a whole tail, so
        # those older positions must be idempotent rather than invalidating.
        if sequence < previous_sequence:
            return "replay"
        if sequence == previous_sequence + 1:
            return "accept"
        if sequence > previous_sequence + 1:
            return "gap"
        if sequence == previous_sequence and event_id == previous_event_id:
            return "replay"
        return "out_of_order"

    def with_checkpoint(self, partition: str, sequence: int, event_id: str) -> "CDCJobSnapshot":
        cloned = self.clone()
        cloned.checkpoints[str(partition or "0")] = (int(sequence), str(event_id))
        return cloned

    def advance(self, change: Any) -> "CDCJobSnapshot":
        return self.with_checkpoint(change.partition, change.sequence, change.event_id)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "groups": {key: group.to_payload() for key, group in self.groups.items()},
            "checkpoints": {
                partition: [sequence, event_id]
                for partition, (sequence, event_id) in self.checkpoints.items()
            },
            "status": self.status,
            "invalidation_reason": self.invalidation_reason,
        }

    @classmethod
    def from_payload(cls, job_id: str, payload: Mapping[str, Any]) -> "CDCJobSnapshot":
        raw_groups = payload.get("groups", {})
        raw_checkpoints = payload.get("checkpoints", {})
        if not isinstance(raw_groups, dict) or not isinstance(raw_checkpoints, dict):
            raise ExactCDCStateError("CDC job snapshot is invalid.")
        groups = {
            str(key): ExactGroupState.from_payload(value)
            for key, value in raw_groups.items()
            if isinstance(key, str) and isinstance(value, Mapping)
        }
        checkpoints: Dict[str, tuple[int, str]] = {}
        for partition, value in raw_checkpoints.items():
            if (
                not isinstance(partition, str)
                or not isinstance(value, list)
                or len(value) != 2
                or not isinstance(value[0], int)
                or not isinstance(value[1], str)
            ):
                raise ExactCDCStateError("CDC checkpoint is invalid.")
            checkpoints[partition] = (value[0], value[1])
        return cls(
            job_id=str(job_id),
            version=int(payload.get("version") or 0),
            groups=groups,
            checkpoints=checkpoints,
            status=str(payload.get("status") or "uninitialized"),
            invalidation_reason=(
                str(payload["invalidation_reason"])
                if payload.get("invalidation_reason") is not None
                else None
            ),
        )


def _encoded_snapshot_size(snapshot: CDCJobSnapshot) -> int:
    """Return the durable Redis envelope size used by every state backend."""

    body = json.dumps(
        {"version": 1, "snapshot": snapshot.to_payload()},
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return len(_STATE_FORMAT) + len(body)


def _extrema_value_count(snapshot: CDCJobSnapshot) -> int:
    return sum(
        len(accumulator.value_counts)
        for group in snapshot.groups.values()
        for accumulator in group.measures.values()
        if accumulator.aggregation in {"min", "max"}
    )


def validate_cdc_snapshot_limits(
    snapshot: CDCJobSnapshot, limits: CDCStateLimits
) -> None:
    """Reject an unpersisted snapshot that would exceed any exact-state cap."""

    if len(snapshot.groups) > limits.max_groups:
        raise CDCStateBudgetExceeded("CDC group state exceeds its configured budget.")
    if _extrema_value_count(snapshot) > limits.max_extrema_values:
        raise CDCStateBudgetExceeded("CDC extrema state exceeds its configured budget.")
    if _encoded_snapshot_size(snapshot) > limits.max_bytes:
        raise CDCStateBudgetExceeded("CDC encoded state exceeds its configured budget.")


class CDCStateStore:
    """Narrow asynchronous persistence interface for exact CDC job state."""

    async def load(self, job_id: str) -> CDCJobSnapshot:
        raise NotImplementedError

    async def compare_and_swap(
        self,
        job_id: str,
        expected_version: int,
        snapshot: CDCJobSnapshot,
    ) -> CDCJobSnapshot:
        raise NotImplementedError


class MemoryCDCStateStore(CDCStateStore):
    """Process-local store for tests and explicit development-only use."""

    def __init__(self, limits: Optional[CDCStateLimits] = None, *,
                 governor: Optional[ResourceGovernor] = None,
                 job_ttl_seconds: Optional[float] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self._snapshots: Dict[str, CDCJobSnapshot] = {}
        self._leases: Dict[str, ResourceLease] = {}
        self._expires_at: Dict[str, float] = {}
        self._expiry_heap: list[tuple[float, str]] = []
        self._finalizer = weakref.finalize(self, self._release_leases, self._leases)
        self._lock = RLock()
        self.limits = limits or CDCStateLimits()
        self.governor = governor or get_worker_resource_governor()
        self.job_ttl_seconds = _configured_job_ttl(job_ttl_seconds)
        self._clock = clock

    @staticmethod
    def _release_leases(leases: Dict[str, ResourceLease]) -> None:
        for lease in leases.values():
            lease.release()

    async def load(self, job_id: str) -> CDCJobSnapshot:
        with self._lock:
            self._expire_locked()
            snapshot = self._snapshots.get(str(job_id))
            if snapshot is None:
                return CDCJobSnapshot.empty(str(job_id))
            if self._leases[str(job_id)].account != current_workload_account.get():
                raise ExactCDCStateError("CDC job accounting owner cannot change.")
            try:
                with self.governor.reserve(
                    ResourceKind.CDC, _encoded_snapshot_size(snapshot),
                    account=self._leases[str(job_id)].account,
                ):
                    return snapshot.clone()
            except ResourceExhaustedError as exc:
                raise CDCStateBudgetExceeded("CDC transient state budget exceeded.") from exc

    async def compare_and_swap(
        self,
        job_id: str,
        expected_version: int,
        snapshot: CDCJobSnapshot,
    ) -> CDCJobSnapshot:
        key = str(job_id)
        with self._lock:
            self._expire_locked()
            current = self._snapshots.get(key, CDCJobSnapshot.empty(key))
            if current.version != int(expected_version):
                raise StateVersionConflict("CDC state version was superseded.")
            account = self._leases[key].account if key in self._leases else current_workload_account.get()
            if key in self._leases and account != current_workload_account.get():
                raise ExactCDCStateError("CDC job accounting owner cannot change.")
            try:
                with self.governor.reserve(
                    ResourceKind.CDC, _encoded_snapshot_size(snapshot), account=account,
                ):
                    persisted = snapshot.clone()
                    persisted.job_id = key
                    persisted.version = current.version + 1
                    validate_cdc_snapshot_limits(persisted, self.limits)
                    size = _encoded_snapshot_size(persisted)
                    lease = self._leases.get(key)
                    if lease is None:
                        lease = self.governor.reserve(ResourceKind.CDC, size, account=account)
                    else:
                        lease.resize(size)
                    self._snapshots[key] = persisted
                    self._leases[key] = lease
                    if self.job_ttl_seconds is not None:
                        deadline = self._clock() + self.job_ttl_seconds
                        self._expires_at[key] = deadline
                        heapq.heappush(self._expiry_heap, (deadline, key))
                        if len(self._expiry_heap) > max(1024, 4 * len(self._expires_at)):
                            self._expiry_heap = [
                                (expiry, job) for job, expiry in self._expires_at.items()
                            ]
                            heapq.heapify(self._expiry_heap)
                    return persisted.clone()
            except ResourceExhaustedError as exc:
                raise CDCStateBudgetExceeded("CDC aggregate state budget exceeded.") from exc

    async def delete(self, job_id: str) -> None:
        with self._lock:
            key = str(job_id)
            self._snapshots.pop(key, None)
            self._expires_at.pop(key, None)
            lease = self._leases.pop(key, None)
            if lease is not None:
                lease.release()

    async def expire_jobs(self) -> int:
        with self._lock:
            return self._expire_locked()

    def _expire_locked(self) -> int:
        count = 0
        now = self._clock()
        while self._expiry_heap and self._expiry_heap[0][0] <= now:
            deadline, key = heapq.heappop(self._expiry_heap)
            if self._expires_at.get(key) != deadline:
                continue
            self._expires_at.pop(key, None)
            self._snapshots.pop(key, None)
            lease = self._leases.pop(key, None)
            if lease is not None:
                lease.release()
            count += 1
        return count


class RedisCDCStateStore(CDCStateStore):
    """Redis-backed state using one explicit versioned JSON envelope per job."""

    def __init__(
        self, runtime_state: Any, limits: Optional[CDCStateLimits] = None, *,
        governor: Optional[ResourceGovernor] = None,
        tenant_max_bytes: Optional[int] = None,
        namespace_max_bytes: Optional[int] = None,
        job_ttl_seconds: Optional[float] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.runtime_state = runtime_state
        self.limits = limits or CDCStateLimits()
        self.governor = governor or get_worker_resource_governor()
        self.namespace_max_bytes = (
            namespace_max_bytes if namespace_max_bytes is not None else self.limits.max_bytes
        )
        self.tenant_max_bytes = (
            tenant_max_bytes if tenant_max_bytes is not None else int(os.getenv(
                "PIVOT_CDC_TENANT_MAX_BYTES", str(64 * 1024 * 1024)
            ))
        )
        if (not isinstance(self.namespace_max_bytes, int) or not isinstance(self.tenant_max_bytes, int)
                or self.namespace_max_bytes <= 0 or self.tenant_max_bytes <= 0):
            raise ValueError("CDC aggregate byte budgets must be positive")
        self.job_ttl_seconds = _configured_job_ttl(job_ttl_seconds)
        self._clock = clock
        self._verified = False

    def _ensure_recovered(self) -> None:
        """Reconcile durable counters once after a worker starts.

        Indexed state is authoritative. An unindexed state has no trustworthy
        owner, so it cannot be imported into a tenant budget automatically.
        """
        if self._verified:
            return
        client = self.runtime_state.client
        ledger, total_key, tenants_key = self._ledger_keys()
        prefix = self.runtime_state.key("cdc", "job") + ":"
        for _attempt in range(32):
            try:
                with client.pipeline() as pipe:
                    pipe.watch(ledger, total_key, tenants_key)
                    records = pipe.hgetall(ledger)
                    indexed = {}
                    totals: dict[str, int] = {}
                    total = 0
                    expired = []
                    for raw_job, raw_meta in records.items():
                        job = raw_job.decode("utf-8") if isinstance(raw_job, bytes) else str(raw_job)
                        owner, size, expires_at = self._metadata(raw_meta)
                        key = self._key(job)
                        pipe.watch(key)
                        account = (
                            SYSTEM_ACCOUNT if owner == "s:"
                            else WorkloadAccount.tenant(owner[2:])
                        )
                        with self.governor.reserve(ResourceKind.CDC, size, account=account):
                            raw_state = pipe.get(key)
                            if raw_state is None or len(raw_state) != size:
                                raise ExactCDCStateError("CDC indexed state is missing or inconsistent.")
                        indexed[key] = size
                        if expires_at is not None and expires_at <= self._clock():
                            expired.append((job, key))
                            continue
                        total += size
                        totals[owner] = totals.get(owner, 0) + size
                    for raw_key in client.scan_iter(match=prefix + "*"):
                        state_key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
                        if state_key not in indexed:
                            raise ExactCDCStateError("CDC unindexed state has no accounting owner.")
                    existing_total = pipe.get(total_key)
                    existing_tenants = pipe.hgetall(tenants_key)
                    actual_tenants = {
                        k.decode("utf-8") if isinstance(k, bytes) else str(k): self._counter(v)
                        for k, v in existing_tenants.items()
                    }
                    if total > self.namespace_max_bytes or any(
                        size > self.tenant_max_bytes for size in totals.values()
                    ):
                        raise CDCStateBudgetExceeded("Recovered CDC state exceeds the configured budget.")
                    pipe.multi()
                    if existing_total is None or self._counter(existing_total) != total or actual_tenants != totals:
                        pipe.set(total_key, total)
                        pipe.delete(tenants_key)
                        if totals:
                            pipe.hset(tenants_key, mapping=totals)
                    else:
                        pipe.ping()
                    for job, key in expired:
                        pipe.delete(key)
                        pipe.hdel(ledger, job)
                    pipe.execute()
                    self._verified = True
                    return
            except Exception as exc:
                if exc.__class__.__name__ == "WatchError":
                    continue
                raise
        raise StateVersionConflict("CDC budget recovery was superseded.")

    def _ledger_keys(self) -> tuple[str, str, str]:
        return (
            self.runtime_state.key("cdc", "budget", "jobs"),
            self.runtime_state.key("cdc", "budget", "total"),
            self.runtime_state.key("cdc", "budget", "tenants"),
        )

    @staticmethod
    def _account_key(account: Any) -> str:
        return "s:" if account.tenant_id is None else "t:" + account.tenant_id

    @staticmethod
    def _metadata(raw: Any) -> tuple[str, int, float | None] | None:
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
            owner, size = payload["owner"], payload["size"]
            expires_at = payload.get("expiresAt")
            if not isinstance(owner, str) or not isinstance(size, int) or size <= 0:
                raise ValueError
            if owner != "s:" and (not owner.startswith("t:") or not owner[2:]):
                raise ValueError
            if expires_at is not None and (
                not isinstance(expires_at, (int, float))
                or not math.isfinite(expires_at) or expires_at <= 0
            ):
                raise ValueError
            return owner, size, expires_at
        except (ValueError, TypeError, KeyError) as exc:
            raise ExactCDCStateError("CDC budget metadata is invalid.") from exc

    @staticmethod
    def _counter(raw: Any) -> int:
        if raw is None:
            return 0
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ExactCDCStateError("CDC budget counter is invalid.") from exc
        if value < 0:
            raise ExactCDCStateError("CDC budget counter is invalid.")
        return value

    def _key(self, job_id: str) -> str:
        return self.runtime_state.key("cdc", "job", str(job_id))

    @staticmethod
    def _encode(snapshot: CDCJobSnapshot) -> bytes:
        body = json.dumps(
            {"version": 1, "snapshot": snapshot.to_payload()},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return _STATE_FORMAT + body

    @staticmethod
    def _decode(job_id: str, raw: Any) -> CDCJobSnapshot:
        if raw is None:
            return CDCJobSnapshot.empty(job_id)
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        if not isinstance(raw, bytes) or not raw.startswith(_STATE_FORMAT):
            raise ExactCDCStateError("CDC Redis state has an unsupported format.")
        try:
            payload = json.loads(raw[len(_STATE_FORMAT):].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExactCDCStateError("CDC Redis state is invalid.") from exc
        if not isinstance(payload, dict) or payload.get("version") != 1:
            raise ExactCDCStateError("CDC Redis state has an unsupported format.")
        snapshot = payload.get("snapshot")
        if not isinstance(snapshot, Mapping):
            raise ExactCDCStateError("CDC Redis state is invalid.")
        return CDCJobSnapshot.from_payload(job_id, snapshot)

    async def load(self, job_id: str) -> CDCJobSnapshot:
        key = self._key(job_id)
        try:
            self._ensure_recovered()
            ledger, _, _ = self._ledger_keys()
            with self.runtime_state.client.pipeline() as pipe:
                pipe.multi()
                pipe.get(key)
                pipe.hget(ledger, str(job_id))
                raw, metadata_raw = pipe.execute()
            metadata = self._metadata(metadata_raw)
            if (raw is None) != (metadata is None):
                raise ExactCDCStateError("CDC state and budget metadata disagree.")
            if raw is not None:
                if metadata[2] is not None and metadata[2] <= self._clock():
                    if await self.delete(job_id, expired_only=True):
                        return CDCJobSnapshot.empty(str(job_id))
                    return await self.load(job_id)
                if len(raw) != metadata[1]:
                    raise ExactCDCStateError("CDC state and budget size disagree.")
                owner = metadata[0]
                account = current_workload_account.get()
                if owner != self._account_key(account):
                    raise ExactCDCStateError("CDC job accounting owner cannot change.")
                with self.governor.reserve(ResourceKind.CDC, len(raw), account=account):
                    return self._decode(str(job_id), raw)
        except Exception as exc:
            if isinstance(exc, (ExactCDCStateError, ResourceExhaustedError)):
                raise
            raise ExactCDCStateError("CDC durable state is unavailable.") from exc
        return CDCJobSnapshot.empty(str(job_id))

    async def compare_and_swap(
        self,
        job_id: str,
        expected_version: int,
        snapshot: CDCJobSnapshot,
    ) -> CDCJobSnapshot:
        key = self._key(job_id)
        ledger, total_key, tenants_key = self._ledger_keys()
        client = self.runtime_state.client
        account = current_workload_account.get()
        owner = self._account_key(account)
        try:
            self._ensure_recovered()
            if self.job_ttl_seconds is not None:
                await self.delete(job_id, expired_only=True)
            with self.governor.reserve(
                ResourceKind.CDC, _encoded_snapshot_size(snapshot), account=account,
            ):
                with client.pipeline() as pipe:
                    pipe.watch(key, ledger, total_key, tenants_key)
                    old_raw = pipe.get(key)
                    metadata = self._metadata(pipe.hget(ledger, str(job_id)))
                    if (old_raw is None) != (metadata is None):
                        raise ExactCDCStateError("CDC state and budget metadata disagree.")
                    if metadata is not None and (metadata[0] != owner or len(old_raw) != metadata[1]):
                        raise ExactCDCStateError("CDC job accounting owner or size disagrees.")
                    current = self._decode(str(job_id), old_raw)
                    if current.version != int(expected_version):
                        raise StateVersionConflict("CDC state version was superseded.")
                    persisted = snapshot.clone()
                    persisted.job_id = str(job_id)
                    persisted.version = current.version + 1
                    validate_cdc_snapshot_limits(persisted, self.limits)
                    encoded = self._encode(persisted)
                    old_size = metadata[1] if metadata else 0
                    total_raw = pipe.get(total_key)
                    if total_raw is None and pipe.hlen(ledger):
                        raise ExactCDCStateError("CDC budget totals require recovery.")
                    total = self._counter(total_raw)
                    tenant_raw = pipe.hget(tenants_key, owner)
                    if metadata is not None and tenant_raw is None:
                        raise ExactCDCStateError("CDC tenant budget total is missing.")
                    tenant_total = self._counter(tenant_raw)
                    next_total = total - old_size + len(encoded)
                    next_tenant = tenant_total - old_size + len(encoded)
                    if next_total < 0 or next_tenant < 0:
                        raise ExactCDCStateError("CDC budget totals are inconsistent.")
                    if next_total > self.namespace_max_bytes or next_tenant > self.tenant_max_bytes:
                        raise CDCStateBudgetExceeded("CDC aggregate state budget exceeded.")
                    pipe.multi()
                    pipe.set(key, encoded)
                    pipe.hset(ledger, str(job_id), json.dumps({
                        "owner": owner, "size": len(encoded),
                        "expiresAt": self._clock() + self.job_ttl_seconds
                        if self.job_ttl_seconds is not None else None,
                    }))
                    pipe.set(total_key, next_total)
                    pipe.hset(tenants_key, owner, next_tenant)
                    pipe.execute()
                    return persisted
        except (StateVersionConflict, CDCStateBudgetExceeded):
            raise
        except ResourceExhaustedError as exc:
            raise CDCStateBudgetExceeded("CDC transient state budget exceeded.") from exc
        except ExactCDCStateError:
            raise
        except Exception as exc:
            if exc.__class__.__name__ == "WatchError":
                raise StateVersionConflict("CDC state version was superseded.") from exc
            raise ExactCDCStateError("CDC durable state is unavailable.") from exc

    async def delete(self, job_id: str, *, expired_only: bool = False) -> bool:
        key = self._key(job_id)
        ledger, total_key, tenants_key = self._ledger_keys()
        try:
            self._ensure_recovered()
            with self.runtime_state.client.pipeline() as pipe:
                pipe.watch(key, ledger, total_key, tenants_key)
                raw = pipe.get(key)
                metadata = self._metadata(pipe.hget(ledger, str(job_id)))
                if raw is None and metadata is None:
                    return False
                if raw is None or metadata is None or len(raw) != metadata[1]:
                    raise ExactCDCStateError("CDC state and budget metadata disagree.")
                if expired_only and (metadata[2] is None or metadata[2] > self._clock()):
                    return False
                if not expired_only and metadata[0] != self._account_key(current_workload_account.get()):
                    raise ExactCDCStateError("CDC job accounting owner cannot change.")
                total = self._counter(pipe.get(total_key))
                tenant_total = self._counter(pipe.hget(tenants_key, metadata[0]))
                if total < metadata[1] or tenant_total < metadata[1]:
                    raise ExactCDCStateError("CDC budget totals are inconsistent.")
                pipe.multi()
                pipe.delete(key)
                pipe.hdel(ledger, str(job_id))
                pipe.set(total_key, total - metadata[1])
                pipe.hset(tenants_key, metadata[0], tenant_total - metadata[1])
                pipe.execute()
                return True
        except ExactCDCStateError:
            raise
        except Exception as exc:
            if exc.__class__.__name__ == "WatchError":
                raise StateVersionConflict("CDC state version was superseded.") from exc
            raise ExactCDCStateError("CDC durable state is unavailable.") from exc

    async def expire_jobs(self) -> int:
        """Remove expired indexed jobs through the same atomic accounting path."""
        self._ensure_recovered()
        ledger, _, _ = self._ledger_keys()
        records = self.runtime_state.client.hgetall(ledger)
        expired = [
            job.decode("utf-8") if isinstance(job, bytes) else str(job)
            for job, raw in records.items()
            if (metadata := self._metadata(raw))[2] is not None
            and metadata[2] <= self._clock()
        ]
        removed = 0
        for job in expired:
            removed += int(await self.delete(job, expired_only=True))
        return removed


__all__ = [
    "CDCJobSnapshot",
    "CDCStateBudgetExceeded",
    "CDCStateLimits",
    "CDCStateStore",
    "CanonicalScalarCodec",
    "ExactCDCStateError",
    "ExactGroupState",
    "ExactMeasureAccumulator",
    "MemoryCDCStateStore",
    "RedisCDCStateStore",
    "StateVersionConflict",
    "validate_cdc_snapshot_limits",
]
