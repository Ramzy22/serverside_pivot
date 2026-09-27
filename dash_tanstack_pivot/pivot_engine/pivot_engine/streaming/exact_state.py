"""Exact, reversible aggregate state for trusted CDC streams.

The module deliberately serializes only explicit JSON values.  It is separate
from the legacy cache state so durable CDC checkpoints cannot inherit a broad
object deserializer.
"""

from __future__ import annotations

import asyncio
import base64
import copy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import heapq
import json
import math
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence


_STATE_FORMAT = b"pivot:cdc-exact-state:v1\x00"
_SUPPORTED_AGGREGATIONS = frozenset({"sum", "count", "avg", "min", "max"})
_DEFAULT_CDC_MAX_GROUPS = 100_000
_DEFAULT_CDC_MAX_BYTES = 128 * 1024 * 1024
_DEFAULT_CDC_MAX_EXTREMA_VALUES = 1_000_000


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

    def __init__(self, limits: Optional[CDCStateLimits] = None) -> None:
        self._snapshots: Dict[str, CDCJobSnapshot] = {}
        self._lock = asyncio.Lock()
        self.limits = limits or CDCStateLimits()

    async def load(self, job_id: str) -> CDCJobSnapshot:
        async with self._lock:
            snapshot = self._snapshots.get(str(job_id))
            return snapshot.clone() if snapshot is not None else CDCJobSnapshot.empty(str(job_id))

    async def compare_and_swap(
        self,
        job_id: str,
        expected_version: int,
        snapshot: CDCJobSnapshot,
    ) -> CDCJobSnapshot:
        key = str(job_id)
        async with self._lock:
            current = self._snapshots.get(key, CDCJobSnapshot.empty(key))
            if current.version != int(expected_version):
                raise StateVersionConflict("CDC state version was superseded.")
            persisted = snapshot.clone()
            persisted.job_id = key
            persisted.version = current.version + 1
            validate_cdc_snapshot_limits(persisted, self.limits)
            self._snapshots[key] = persisted
            return persisted.clone()


class RedisCDCStateStore(CDCStateStore):
    """Redis-backed state using one explicit versioned JSON envelope per job."""

    def __init__(
        self, runtime_state: Any, limits: Optional[CDCStateLimits] = None
    ) -> None:
        self.runtime_state = runtime_state
        self.limits = limits or CDCStateLimits()

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
            raw = self.runtime_state.client.get(key)
        except Exception as exc:
            raise ExactCDCStateError("CDC durable state is unavailable.") from exc
        return self._decode(str(job_id), raw)

    async def compare_and_swap(
        self,
        job_id: str,
        expected_version: int,
        snapshot: CDCJobSnapshot,
    ) -> CDCJobSnapshot:
        key = self._key(job_id)
        client = self.runtime_state.client
        try:
            with client.pipeline() as pipe:
                pipe.watch(key)
                current = self._decode(str(job_id), pipe.get(key))
                if current.version != int(expected_version):
                    pipe.unwatch()
                    raise StateVersionConflict("CDC state version was superseded.")
                persisted = snapshot.clone()
                persisted.job_id = str(job_id)
                persisted.version = current.version + 1
                validate_cdc_snapshot_limits(persisted, self.limits)
                pipe.multi()
                pipe.set(key, self._encode(persisted))
                pipe.execute()
                return persisted
        except (StateVersionConflict, CDCStateBudgetExceeded):
            raise
        except Exception as exc:
            if exc.__class__.__name__ == "WatchError":
                raise StateVersionConflict("CDC state version was superseded.") from exc
            raise ExactCDCStateError("CDC durable state is unavailable.") from exc


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
