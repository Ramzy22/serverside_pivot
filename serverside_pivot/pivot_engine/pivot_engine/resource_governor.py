"""Process-local admission control for bounded pivot-engine resources.

The governor deliberately accounts only for work that opts into a lease.  RSS
is sampled as a separate soft guard because Arrow, DuckDB, and Python native
allocations cannot be accounted for precisely from Python.
"""

from __future__ import annotations

from collections import Counter
from contextvars import ContextVar
from dataclasses import dataclass, fields
from numbers import Real
import os
from threading import RLock
from typing import Any, Callable, Mapping

import psutil

from .export_limits import positive_env_int

try:
    from enum import StrEnum
except ImportError:  # Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        """String-valued enum with the Python 3.11 string display behavior."""

        __str__ = str.__str__
        __format__ = str.__format__


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


@dataclass(frozen=True)
class WorkloadAccount:
    """Server-owned accounting identity, distinct from data access scope."""

    tenant_id: str | None = None

    def __post_init__(self) -> None:
        value = self.tenant_id
        if value is not None and (
            not isinstance(value, str) or not value.strip()
            or value.strip() != value or value.lower() == "system"
        ):
            raise ValueError("accounting tenant must be a nonempty non-system identifier")

    @classmethod
    def tenant(cls, value: str) -> "WorkloadAccount":
        if not isinstance(value, str) or not value.strip() or value.strip().lower() == "system":
            raise ValueError("accounting tenant must be a nonempty non-system identifier")
        return cls(value.strip())


SYSTEM_ACCOUNT = WorkloadAccount()
current_workload_account: ContextVar[WorkloadAccount] = ContextVar(
    "pivot_current_workload_account", default=SYSTEM_ACCOUNT
)


def account_for_principal(principal: Any) -> WorkloadAccount:
    """Resolve billing identity from the authenticated principal, never row scope.

    A host may put an authorized account in principal attributes during
    authentication. Data-tenant claims deliberately do not provide a fallback.
    """
    attributes = getattr(principal, "attributes", None)
    value = attributes.get("accounting_tenant") if isinstance(attributes, Mapping) else None
    if value is None:
        return SYSTEM_ACCOUNT
    return WorkloadAccount.tenant(value)


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
    tenant_bytes: int = 256 * _MIB
    system_bytes: int = 64 * _MIB
    tenant_max_queries: int = 4
    tenant_max_exports: int = 1
    tenant_max_downloads: int = 2
    system_max_queries: int = 2
    system_max_exports: int = 1
    system_max_downloads: int = 1
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

        def strict_tenant_value(name: str, default: int) -> int:
            raw = source.get(name)
            if raw is None:
                return default
            try:
                parsed = int(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be a positive integer") from exc
            if parsed <= 0:
                raise ValueError(f"{name} must be a positive integer")
            return parsed

        worker_hard_limit = value(
            "PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES", defaults.worker_hard_limit_bytes
        )
        resolved = cls(
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
            tenant_bytes=strict_tenant_value("PIVOT_TENANT_MAX_BYTES", defaults.tenant_bytes),
            system_bytes=strict_tenant_value("PIVOT_SYSTEM_MAX_BYTES", defaults.system_bytes),
            tenant_max_queries=strict_tenant_value("PIVOT_TENANT_MAX_CONCURRENT_QUERIES", defaults.tenant_max_queries),
            tenant_max_exports=strict_tenant_value("PIVOT_TENANT_MAX_CONCURRENT_EXPORTS", defaults.tenant_max_exports),
            tenant_max_downloads=strict_tenant_value("PIVOT_TENANT_MAX_CONCURRENT_DOWNLOADS", defaults.tenant_max_downloads),
            system_max_queries=strict_tenant_value("PIVOT_SYSTEM_MAX_CONCURRENT_QUERIES", defaults.system_max_queries),
            system_max_exports=strict_tenant_value("PIVOT_SYSTEM_MAX_CONCURRENT_EXPORTS", defaults.system_max_exports),
            system_max_downloads=strict_tenant_value("PIVOT_SYSTEM_MAX_CONCURRENT_DOWNLOADS", defaults.system_max_downloads),
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
        available = resolved.total_bytes - resolved.headroom_bytes
        if resolved.tenant_bytes > available or resolved.system_bytes > available:
            raise ValueError("account byte budgets must fit within worker available bytes")
        return resolved

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
            "tenant_bytes": 1024,
            "system_bytes": 1024,
            "tenant_max_queries": 10,
            "tenant_max_exports": 2,
            "tenant_max_downloads": 4,
            "system_max_queries": 10,
            "system_max_exports": 2,
            "system_max_downloads": 4,
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

    def account_slots_for(self, account: WorkloadAccount, kind: ResourceKind) -> int | None:
        if account == SYSTEM_ACCOUNT:
            return {
                ResourceKind.QUERY: self.system_max_queries,
                ResourceKind.EXPORT: self.system_max_exports,
                ResourceKind.DOWNLOAD: self.system_max_downloads,
            }.get(kind)
        return {
            ResourceKind.QUERY: self.tenant_max_queries,
            ResourceKind.EXPORT: self.tenant_max_exports,
            ResourceKind.DOWNLOAD: self.tenant_max_downloads,
        }.get(kind)


def rss_bytes() -> int:
    """Return this worker's resident set size in bytes."""

    return int(psutil.Process().memory_info().rss)


def estimate_value_bytes(value: Any) -> int | None:
    """Safely estimate values that can be retained without serializing them."""

    if isinstance(value, str):
        total = 0
        for character in value:
            code_point = ord(character)
            if code_point <= 0x7F:
                total += 1
            elif code_point <= 0x7FF:
                total += 2
            elif 0xD800 <= code_point <= 0xDFFF:
                # A surrogate has no standalone UTF-8 representation.
                return None
            elif code_point <= 0xFFFF:
                total += 3
            else:
                total += 4
        return total
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
        self, governor: "ResourceGovernor", kind: ResourceKind, reserved_bytes: int,
        account: WorkloadAccount = SYSTEM_ACCOUNT,
    ) -> None:
        self._governor = governor
        self.kind = kind
        self.reserved_bytes = reserved_bytes
        self.account = account
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

    def resize(self, new_bytes: int) -> None:
        if not isinstance(new_bytes, int) or isinstance(new_bytes, bool) or new_bytes < 0:
            raise ValueError("new_bytes must be a non-negative integer")
        self._governor._resize(self, new_bytes)

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
        self._reserved_by_account: Counter[WorkloadAccount] = Counter()
        self._high_water_account_reserved_bytes = 0
        self._active_slots_by_account: Counter[tuple[WorkloadAccount, ResourceKind]] = Counter()
        self._admitted: Counter[ResourceKind] = Counter()
        self._rejected: Counter[tuple[ResourceKind, str]] = Counter()
        self._lease_records: dict[int, tuple[ResourceLease, ResourceKind, int, WorkloadAccount]] = {}
        self._high_water_rss_bytes = 0

    def reserve(
        self, kind: ResourceKind, estimated_bytes: int, *,
        account: WorkloadAccount = SYSTEM_ACCOUNT,
    ) -> ResourceLease:
        if not isinstance(kind, ResourceKind):
            kind = ResourceKind(kind)
        if not isinstance(account, WorkloadAccount):
            raise TypeError("account must be a trusted WorkloadAccount")
        self._validate_requested_bytes(estimated_bytes)
        with self._lock:
            self._ensure_capacity(kind, estimated_bytes, account=account, include_slot=True)
            self._reserved_by_kind[kind] += estimated_bytes
            self._reserved_by_account[account] += estimated_bytes
            self._high_water_account_reserved_bytes = max(
                self._high_water_account_reserved_bytes, self._reserved_by_account[account]
            )
            if self.limits.slots_for(kind) is not None:
                self._active_slots[kind] += 1
                self._active_slots_by_account[(account, kind)] += 1
            self._admitted[kind] += 1
            lease = ResourceLease(self, kind, estimated_bytes, account)
            self._lease_records[id(lease)] = (lease, kind, estimated_bytes, account)
            return lease

    def _grow(self, lease: ResourceLease, additional_bytes: int) -> None:
        with self._lock:
            record = self._lease_records.get(id(lease))
            if record is None or record[0] is not lease:
                raise RuntimeError("Cannot grow an inactive resource lease")
            _, kind, recorded_bytes, account = record
            self._ensure_capacity(kind, additional_bytes, account=account, include_slot=False)
            self._reserved_by_kind[kind] += additional_bytes
            self._reserved_by_account[account] += additional_bytes
            self._high_water_account_reserved_bytes = max(
                self._high_water_account_reserved_bytes, self._reserved_by_account[account]
            )
            updated_bytes = recorded_bytes + additional_bytes
            self._lease_records[id(lease)] = (lease, kind, updated_bytes, account)
            lease.reserved_bytes = updated_bytes

    def _resize(self, lease: ResourceLease, new_bytes: int) -> None:
        with self._lock:
            record = self._lease_records.get(id(lease))
            if record is None or record[0] is not lease:
                raise RuntimeError("Cannot resize an inactive resource lease")
            _, kind, old_bytes, account = record
            delta = new_bytes - old_bytes
            if delta > 0:
                self._ensure_capacity(kind, delta, account=account, include_slot=False)
            self._reserved_by_kind[kind] += delta
            self._reserved_by_account[account] += delta
            self._high_water_account_reserved_bytes = max(
                self._high_water_account_reserved_bytes, self._reserved_by_account[account]
            )
            if self._reserved_by_kind[kind] <= 0:
                self._reserved_by_kind.pop(kind, None)
            if self._reserved_by_account[account] <= 0:
                self._reserved_by_account.pop(account, None)
            self._lease_records[id(lease)] = (lease, kind, new_bytes, account)
            lease.reserved_bytes = new_bytes

    def _release(self, lease: ResourceLease) -> None:
        with self._lock:
            record = self._lease_records.pop(id(lease), None)
            if record is None or record[0] is not lease:
                lease._released = True
                return
            _, kind, recorded_bytes, account = record
            self._reserved_by_kind[kind] -= recorded_bytes
            self._reserved_by_account[account] -= recorded_bytes
            if self._reserved_by_kind[kind] <= 0:
                self._reserved_by_kind.pop(kind, None)
            if self._reserved_by_account[account] <= 0:
                self._reserved_by_account.pop(account, None)
            if self.limits.slots_for(kind) is not None:
                self._active_slots[kind] -= 1
                self._active_slots_by_account[(account, kind)] -= 1
                if self._active_slots[kind] <= 0:
                    self._active_slots.pop(kind, None)
                if self._active_slots_by_account[(account, kind)] <= 0:
                    self._active_slots_by_account.pop((account, kind), None)
            lease.reserved_bytes = recorded_bytes
            lease._released = True

    def _ensure_capacity(
        self, kind: ResourceKind, requested_bytes: int, *,
        account: WorkloadAccount, include_slot: bool,
    ) -> None:
        rss = self._sample_rss()
        if rss is None:
            self._reject(kind, "rss_unavailable")
        if rss >= self.limits.rss_soft_limit_bytes:
            self._reject(kind, "rss_soft_limit")

        slot_limit = self.limits.slots_for(kind)
        if include_slot and slot_limit is not None and self._active_slots[kind] >= slot_limit:
            self._reject(kind, "slot")
        account_slot_limit = self.limits.account_slots_for(account, kind)
        if (include_slot and account_slot_limit is not None
                and self._active_slots_by_account[(account, kind)] >= account_slot_limit):
            self._reject(kind, "tenant_slot")

        class_limit = self.limits.limit_for(kind)
        if self._reserved_by_kind[kind] + requested_bytes > class_limit:
            self._reject(kind, "class_bytes")

        total_reserved = sum(self._reserved_by_kind.values())
        if total_reserved + requested_bytes > self.limits.total_bytes - self.limits.headroom_bytes:
            self._reject(kind, "total_bytes")
        account_limit = self.limits.system_bytes if account == SYSTEM_ACCOUNT else self.limits.tenant_bytes
        if self._reserved_by_account[account] + requested_bytes > account_limit:
            self._reject(kind, "tenant_bytes")

    def _sample_rss(self) -> int | None:
        """Accept only non-negative, integral-valued real numeric readings."""
        try:
            reported = self._rss_bytes_getter()
            if isinstance(reported, bool) or not isinstance(reported, Real):
                return None
            # Check before conversion: int() truncates fractional byte counts.
            # Non-finite remainders also fail this exact-integrality check.
            if reported < 0 or reported % 1 != 0:
                return None
            current = int(reported)
        except Exception:
            return None
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
                "max_tenant_reserved_bytes": max(self._reserved_by_account.values(), default=0),
                "high_water_tenant_reserved_bytes": self._high_water_account_reserved_bytes,
                "active_account_count": len(self._reserved_by_account),
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


_worker_governor_lock = RLock()
_worker_governor: ResourceGovernor | None = None
_worker_governor_pid: int | None = None


def _reset_worker_governor_after_fork() -> None:
    global _worker_governor_lock, _worker_governor, _worker_governor_pid
    _worker_governor_lock = RLock()
    _worker_governor = None
    _worker_governor_pid = None


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_worker_governor_after_fork)


def get_worker_resource_governor() -> ResourceGovernor:
    """Return the sole default admission owner for this Python worker process."""
    global _worker_governor, _worker_governor_pid
    with _worker_governor_lock:
        pid = os.getpid()
        if _worker_governor is None or _worker_governor_pid != pid:
            _worker_governor = ResourceGovernor(ResourceLimits.from_environment())
            _worker_governor_pid = pid
        return _worker_governor
