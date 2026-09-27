"""Process-local admission control for bounded pivot-engine resources.

The governor deliberately accounts only for work that opts into a lease.  RSS
is sampled as a separate soft guard because Arrow, DuckDB, and Python native
allocations cannot be accounted for precisely from Python.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, fields
from enum import StrEnum
import os
from threading import RLock
from typing import Any, Callable, Mapping

import psutil

from .export_limits import positive_env_int


_MIB = 1024 * 1024
_GIB = 1024 * _MIB
_RETRY_AFTER_MS = 1_000
_MAX_RETRY_AFTER_MS = 60_000


class ResourceKind(StrEnum):
    CACHE = "cache"
    PAYLOAD = "payload"
    QUERY = "query"
    EXPORT = "export"
    DOWNLOAD = "download"
    CDC = "cdc"


class ResourceExhaustedError(RuntimeError):
    """A fail-fast rejection that transports can map to a retryable 503."""

    code = "resource_exhausted"

    def __init__(
        self,
        resource_kind: ResourceKind,
        *,
        reason: str,
        retry_after_ms: int = _RETRY_AFTER_MS,
    ) -> None:
        self.resource_kind = resource_kind
        self.reason = reason
        self.retry_after_ms = min(
            _MAX_RETRY_AFTER_MS,
            max(1, int(retry_after_ms)),
        )
        super().__init__(
            f"{self.code}: {resource_kind.value} capacity is unavailable ({reason})"
        )


@dataclass(frozen=True)
class ResourceLimits:
    """Validated limits used by one worker-local :class:`ResourceGovernor`."""

    total_bytes: int = _GIB
    headroom_bytes: int = 256 * _MIB
    cache_bytes: int = 256 * _MIB
    payload_bytes: int = 128 * _MIB
    cdc_bytes: int = 128 * _MIB
    query_bytes: int = 256 * _MIB
    export_bytes: int = 32 * _MIB
    download_bytes: int = 8 * _MIB
    max_queries: int = 10
    max_exports: int = 2
    max_downloads: int = 4
    worker_hard_limit_bytes: int = 2 * _GIB
    rss_soft_limit_bytes: int = int(2 * _GIB * 0.85)
    native_headroom_bytes: int = 256 * _MIB
    duckdb_memory_limit_bytes: int = 512 * _MIB

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{item.name} must be a positive integer")

        if self.headroom_bytes >= self.total_bytes:
            raise ValueError("headroom_bytes must be smaller than total_bytes")
        if self.rss_soft_limit_bytes > self.worker_hard_limit_bytes:
            raise ValueError(
                "rss_soft_limit_bytes must not exceed worker memory hard limit"
            )
        if (
            self.duckdb_memory_limit_bytes
            + self.total_bytes
            + self.native_headroom_bytes
            > self.worker_hard_limit_bytes
        ):
            raise ValueError(
                "DuckDB memory limit plus resource budget and native headroom "
                "must fit within the worker memory hard limit"
            )

    @classmethod
    def from_environment(
        cls, environ: Mapping[str, str] | None = None
    ) -> "ResourceLimits":
        """Load positive policy values from an environment mapping.

        The normal path uses the shared ``positive_env_int`` parser.  An
        explicit mapping is supported for deterministic startup/tests without
        mutating process environment.
        """

        source: Mapping[str, str] = os.environ if environ is None else environ
        defaults = cls()

        def value(name: str, default: int) -> int:
            if environ is None:
                return positive_env_int(name, default)
            try:
                parsed = int(source.get(name, default))
            except (TypeError, ValueError):
                return default
            return parsed if parsed > 0 else default

        worker_hard_limit = value(
            "PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES", defaults.worker_hard_limit_bytes
        )
        return cls(
            total_bytes=value("PIVOT_RESOURCE_MAX_BYTES", defaults.total_bytes),
            headroom_bytes=value(
                "PIVOT_RESOURCE_HEADROOM_BYTES", defaults.headroom_bytes
            ),
            cache_bytes=value("PIVOT_CACHE_MAX_BYTES", defaults.cache_bytes),
            payload_bytes=value("PIVOT_PAYLOAD_MAX_BYTES", defaults.payload_bytes),
            cdc_bytes=value("PIVOT_CDC_MAX_BYTES", defaults.cdc_bytes),
            query_bytes=value("PIVOT_QUERY_MAX_BYTES", defaults.query_bytes),
            export_bytes=value(
                "PIVOT_EXPORT_BATCH_MAX_BYTES", defaults.export_bytes
            ),
            download_bytes=value(
                "PIVOT_DOWNLOAD_MAX_BYTES", defaults.download_bytes
            ),
            max_queries=value("PIVOT_MAX_CONCURRENT_QUERIES", defaults.max_queries),
            max_exports=value("PIVOT_MAX_CONCURRENT_EXPORTS", defaults.max_exports),
            max_downloads=value(
                "PIVOT_MAX_CONCURRENT_DOWNLOADS", defaults.max_downloads
            ),
            worker_hard_limit_bytes=worker_hard_limit,
            rss_soft_limit_bytes=value(
                "PIVOT_WORKER_RSS_SOFT_LIMIT_BYTES", int(worker_hard_limit * 0.85)
            ),
            native_headroom_bytes=value(
                "PIVOT_NATIVE_HEADROOM_BYTES", defaults.native_headroom_bytes
            ),
            duckdb_memory_limit_bytes=value(
                "PIVOT_DUCKDB_MEMORY_LIMIT", defaults.duckdb_memory_limit_bytes
            ),
        )

    @classmethod
    def for_testing(cls, **overrides: int) -> "ResourceLimits":
        """Return valid, small-friendly defaults with selected overrides."""

        base = {
            "total_bytes": 1024,
            "headroom_bytes": 1,
            "cache_bytes": 1024,
            "payload_bytes": 1024,
            "cdc_bytes": 1024,
            "query_bytes": 1024,
            "export_bytes": 1024,
            "download_bytes": 1024,
            "max_queries": 10,
            "max_exports": 2,
            "max_downloads": 4,
            "worker_hard_limit_bytes": 4096,
            "rss_soft_limit_bytes": 3481,
            "native_headroom_bytes": 1,
            "duckdb_memory_limit_bytes": 1,
        }
        unknown = set(overrides).difference(base)
        if unknown:
            raise TypeError(f"Unknown ResourceLimits override(s): {sorted(unknown)!r}")
        base.update(overrides)
        return cls(**base)

    def limit_for(self, kind: ResourceKind) -> int:
        return {
            ResourceKind.CACHE: self.cache_bytes,
            ResourceKind.PAYLOAD: self.payload_bytes,
            ResourceKind.QUERY: self.query_bytes,
            ResourceKind.EXPORT: self.export_bytes,
            ResourceKind.DOWNLOAD: self.download_bytes,
            ResourceKind.CDC: self.cdc_bytes,
        }[kind]

    def slots_for(self, kind: ResourceKind) -> int | None:
        return {
            ResourceKind.QUERY: self.max_queries,
            ResourceKind.EXPORT: self.max_exports,
            ResourceKind.DOWNLOAD: self.max_downloads,
        }.get(kind)


def rss_bytes() -> int:
    """Return this worker's resident set size in bytes."""

    return int(psutil.Process().memory_info().rss)


def estimate_value_bytes(value: Any) -> int | None:
    """Safely estimate values that can be retained without serializing them."""

    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, memoryview):
        return value.nbytes
    if isinstance(value, (bytes, bytearray)):
        return len(value)
    try:
        size = getattr(value, "nbytes")
    except Exception:
        return None
    try:
        measured = int(size)
    except (TypeError, ValueError, OverflowError):
        return None
    return measured if measured >= 0 else None


class ResourceLease:
    """The sole authority for a reservation held by a governor."""

    def __init__(
        self, governor: "ResourceGovernor", kind: ResourceKind, reserved_bytes: int
    ) -> None:
        self._governor = governor
        self.kind = kind
        self.reserved_bytes = reserved_bytes
        self._released = False

    def grow(self, additional_bytes: int) -> None:
        if not isinstance(additional_bytes, int) or isinstance(additional_bytes, bool):
            raise ValueError("additional_bytes must be a non-negative integer")
        if additional_bytes < 0:
            raise ValueError("additional_bytes must be a non-negative integer")
        if additional_bytes == 0:
            return
        self._governor._grow(self, additional_bytes)

    def release(self) -> None:
        self._governor._release(self)

    def __enter__(self) -> "ResourceLease":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()


class ResourceGovernor:
    """Thread-safe, non-blocking admission controller for one worker."""

    def __init__(
        self,
        limits: ResourceLimits,
        *,
        rss_bytes_getter: Callable[[], int] = rss_bytes,
    ) -> None:
        self.limits = limits
        self._rss_bytes_getter = rss_bytes_getter
        self._lock = RLock()
        self._reserved_by_kind: Counter[ResourceKind] = Counter()
        self._active_slots: Counter[ResourceKind] = Counter()
        self._admitted: Counter[ResourceKind] = Counter()
        self._rejected: Counter[tuple[ResourceKind, str]] = Counter()
        self._high_water_rss_bytes = 0

    def reserve(self, kind: ResourceKind, estimated_bytes: int) -> ResourceLease:
        if not isinstance(kind, ResourceKind):
            kind = ResourceKind(kind)
        self._validate_requested_bytes(estimated_bytes)
        with self._lock:
            self._ensure_capacity(kind, estimated_bytes, include_slot=True)
            self._reserved_by_kind[kind] += estimated_bytes
            if self.limits.slots_for(kind) is not None:
                self._active_slots[kind] += 1
            self._admitted[kind] += 1
            return ResourceLease(self, kind, estimated_bytes)

    def _grow(self, lease: ResourceLease, additional_bytes: int) -> None:
        with self._lock:
            if lease._released:
                raise RuntimeError("Cannot grow a released resource lease")
            self._ensure_capacity(lease.kind, additional_bytes, include_slot=False)
            self._reserved_by_kind[lease.kind] += additional_bytes
            lease.reserved_bytes += additional_bytes

    def _release(self, lease: ResourceLease) -> None:
        with self._lock:
            if lease._released:
                return
            self._reserved_by_kind[lease.kind] -= lease.reserved_bytes
            if self._reserved_by_kind[lease.kind] <= 0:
                self._reserved_by_kind.pop(lease.kind, None)
            if self.limits.slots_for(lease.kind) is not None:
                self._active_slots[lease.kind] -= 1
                if self._active_slots[lease.kind] <= 0:
                    self._active_slots.pop(lease.kind, None)
            lease._released = True

    def _ensure_capacity(
        self, kind: ResourceKind, requested_bytes: int, *, include_slot: bool
    ) -> None:
        rss = self._sample_rss()
        if rss >= self.limits.rss_soft_limit_bytes:
            self._reject(kind, "rss_soft_limit")

        slot_limit = self.limits.slots_for(kind)
        if include_slot and slot_limit is not None and self._active_slots[kind] >= slot_limit:
            self._reject(kind, "slot")

        class_limit = self.limits.limit_for(kind)
        if self._reserved_by_kind[kind] + requested_bytes > class_limit:
            self._reject(kind, "class_bytes")

        total_reserved = sum(self._reserved_by_kind.values())
        if total_reserved + requested_bytes > self.limits.total_bytes - self.limits.headroom_bytes:
            self._reject(kind, "total_bytes")

    def _sample_rss(self) -> int:
        try:
            current = max(0, int(self._rss_bytes_getter()))
        except Exception:
            current = 0
        self._high_water_rss_bytes = max(self._high_water_rss_bytes, current)
        return current

    def _reject(self, kind: ResourceKind, reason: str) -> None:
        self._rejected[(kind, reason)] += 1
        raise ResourceExhaustedError(kind, reason=reason)

    @staticmethod
    def _validate_requested_bytes(estimated_bytes: int) -> None:
        if (
            not isinstance(estimated_bytes, int)
            or isinstance(estimated_bytes, bool)
            or estimated_bytes <= 0
        ):
            raise ValueError("estimated_bytes must be a positive integer")

    def snapshot(self) -> dict[str, Any]:
        """Return safe, aggregate admission and pressure metrics."""

        with self._lock:
            current_rss = self._sample_rss()
            kinds = tuple(ResourceKind)
            rejected = {kind.value: {} for kind in kinds}
            for (kind, reason), count in self._rejected.items():
                rejected[kind.value][reason] = count
            return {
                "reserved_bytes": sum(self._reserved_by_kind.values()),
                "reserved_bytes_by_kind": {
                    kind.value: self._reserved_by_kind[kind] for kind in kinds
                },
                "active_slots": {
                    kind.value: self._active_slots[kind] for kind in kinds
                },
                "admitted": {kind.value: self._admitted[kind] for kind in kinds},
                "rejected": rejected,
                "rss_bytes": current_rss,
                "high_water_rss_bytes": self._high_water_rss_bytes,
            }
