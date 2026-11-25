"""Immutable baseline and frozen-snapshot provider with quotas and ref-counting.

This module owns Component 2 of the streaming and analytical diff architecture:
- Honest backend snapshot strategies (IMMUTABLE_ARROW_REF, DUCKDB_PERSISTENT_SNAPSHOT,
  NATIVE_TIME_TRAVEL, EXTERNAL_REFERENCE_TABLE)
- Cross-connection visibility (persistent catalog tables or immutable Arrow tables,
  never connection-local TEMP tables)
- Pre-reserved ResourceGovernor leases with atomic rollback (zero orphan tables/leaks)
- Coherent `(source_revision, schema_version, rows)` capture with RLS enforcement
- Active reference counting, TTL expiry, and fail-closed `SnapshotExpiredError`
"""

from __future__ import annotations

# Security note (bandit): every ``# nosec B608`` in this module formats only identifiers that went
# through quote_ident (embedded quotes doubled) and ``?`` placeholders; all values are bound parameters.

from contextlib import contextmanager
import logging
from dataclasses import dataclass, field
import re
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

try:
    from enum import StrEnum
except ImportError:  # Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        __str__ = str.__str__
        __format__ = str.__format__

import ibis
import pyarrow as pa

from ..resource_governor import (
    ResourceExhaustedError,
    ResourceGovernor,
    ResourceKind,
    ResourceLease,
    SYSTEM_ACCOUNT,
    WorkloadAccount,
    account_for_principal,
    get_worker_resource_governor,
)
from ..streaming.versioned_source_adapter import VersionedSourceAdapter, _extract_raw_duckdb_connection
from ..util.sql_builder import quote_ident

_LOG = logging.getLogger(__name__)
_META_TABLE = "__pivot_baselines_meta"
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_]+$")
_DEFAULT_SNAPSHOT_TTL_SECONDS = 3600.0
_MIN_RESERVATION_BYTES = 64 * 1024


class SnapshotStrategy(StrEnum):
    IMMUTABLE_ARROW_REF = "immutable_arrow_ref"
    DUCKDB_PERSISTENT_SNAPSHOT = "duckdb_persistent_snapshot"
    NATIVE_TIME_TRAVEL = "native_time_travel"
    EXTERNAL_REFERENCE_TABLE = "external_reference_table"


class SnapshotError(RuntimeError):
    code = "snapshot_error"


class SnapshotExpiredError(SnapshotError):
    """Fail-closed error when a pinned baseline or frozen snapshot has expired."""

    code = "snapshot_expired"


class SnapshotAuthorizationError(SnapshotError):
    """Fail-closed error when a caller lacks access to a snapshot's RLS scope."""

    code = "snapshot_unauthorized"


@dataclass(frozen=True)
class SnapshotDescriptor:
    """Immutable metadata describing one retained baseline or frozen snapshot."""

    snapshot_id: str
    table: str
    label: str
    kind: str  # "baseline" | "freeze_snapshot"
    strategy: SnapshotStrategy
    source_revision: int
    schema_version: str
    created_at: float
    expires_at: float
    owner_subject: str
    row_scope_fingerprint: str
    policy_version: str
    byte_size: int
    row_count: int
    immutable_verified: bool
    physical_table_name: Optional[str] = None
    time_travel_token: Optional[str] = None

    def to_wire_dict(self) -> Dict[str, Any]:
        return {
            "snapshotId": self.snapshot_id,
            "baselineId": self.snapshot_id,
            "table": self.table,
            "label": self.label,
            "kind": self.kind,
            "strategy": self.strategy.value,
            "sourceRevision": self.source_revision,
            "schemaVersion": self.schema_version,
            "createdAt": self.created_at,
            "expiresAt": self.expires_at,
            "byteSize": self.byte_size,
            "rowCount": self.row_count,
            "immutableVerified": self.immutable_verified,
        }


@dataclass
class _ManagedSnapshotEntry:
    descriptor: SnapshotDescriptor
    lease: Optional[ResourceLease]
    account: WorkloadAccount
    arrow_table: Optional[pa.Table] = None
    ref_count: int = 0
    marked_for_deletion: bool = False


def _principal_subject(principal: Any) -> str:
    if principal is None:
        return "anonymous"
    return str(getattr(principal, "id", None) or "anonymous").strip() or "anonymous"


def _row_scope_fingerprint(row_scope: Any) -> str:
    if row_scope is None:
        return "global"
    fp = getattr(row_scope, "scope_fingerprint", None)
    if fp:
        return str(fp)
    return "global"


def _policy_version(row_scope: Any) -> str:
    if row_scope is None:
        return "v1"
    return str(getattr(row_scope, "policy_version", None) or "v1")


def _build_rls_where_sql(row_scope: Any) -> Tuple[str, List[Any]]:
    """Build parameterized SQL WHERE clauses for a RowScope if claim filters exist."""
    if row_scope is None:
        return "", []
    if isinstance(getattr(row_scope, "field_values", None), Mapping):
        # Server-issued grant: exact, typed, NULL-aware (REQ-SEC-003).
        from ..core.authority.scope import scope_where_sql

        return scope_where_sql(row_scope, lambda name: quote_ident(name))
    filters = getattr(row_scope, "filters", None)
    if not filters:
        apply_fn = getattr(row_scope, "apply_to_filters", None)
        if callable(apply_fn):
            applied = apply_fn({})
            if isinstance(applied, Mapping) and applied:
                filters = applied
    if not isinstance(filters, Mapping) or not filters:
        return "", []

    clauses: List[str] = []
    params: List[Any] = []
    for col, spec in filters.items():
        quoted_col = quote_ident(str(col))
        if isinstance(spec, Mapping):
            op = str(spec.get("op") or "=").strip().lower()
            val = spec.get("value")
            if op in {"=", "eq"}:
                clauses.append(f"{quoted_col} = ?")
                params.append(val)
            elif op == "in" and isinstance(val, (list, tuple, set)):
                vals = list(val)
                if not vals:
                    clauses.append("1 = 0")
                else:
                    placeholders = ", ".join("?" for _ in vals)
                    clauses.append(f"{quoted_col} IN ({placeholders})")
                    params.extend(vals)
            elif "conditions" in spec and isinstance(spec["conditions"], list):
                sub_parts: List[str] = []
                for cond in spec["conditions"]:
                    if not isinstance(cond, Mapping):
                        continue
                    c_op = str(cond.get("op") or cond.get("type") or "=").strip().lower()
                    c_val = cond.get("value")
                    if c_op in {"=", "eq", "equals"}:
                        sub_parts.append(f"{quoted_col} = ?")
                        params.append(c_val)
                    elif c_op == "in" and isinstance(c_val, (list, tuple, set)):
                        vals = list(c_val)
                        if not vals:
                            sub_parts.append("1 = 0")
                        else:
                            placeholders = ", ".join("?" for _ in vals)
                            sub_parts.append(f"{quoted_col} IN ({placeholders})")
                            params.extend(vals)
                if sub_parts:
                    joiner = " OR " if str(spec.get("operator") or "AND").upper() == "OR" else " AND "
                    clauses.append("(" + joiner.join(sub_parts) + ")")
        elif isinstance(spec, (list, tuple, set)):
            vals = list(spec)
            if not vals:
                clauses.append("1 = 0")
            else:
                placeholders = ", ".join("?" for _ in vals)
                clauses.append(f"{quoted_col} IN ({placeholders})")
                params.extend(vals)
        else:
            clauses.append(f"{quoted_col} = ?")
            params.append(spec)

    if not clauses:
        return "", []
    return " WHERE " + " AND ".join(clauses), params


class SnapshotProvider:
    """Component 2: Coherent, quota-governed baseline & frozen snapshot lifecycle."""

    def __init__(
        self,
        controller: Any,
        source_adapter: VersionedSourceAdapter,
        *,
        resource_governor: Optional[ResourceGovernor] = None,
        default_ttl_seconds: float = _DEFAULT_SNAPSHOT_TTL_SECONDS,
        max_snapshots_per_table: int = 16,
    ) -> None:
        self._controller = controller
        self._source_adapter = source_adapter
        self._governor = (
            resource_governor
            or getattr(controller, "resource_governor", None)
            or get_worker_resource_governor()
        )
        self._default_ttl_seconds = max(1.0, float(default_ttl_seconds))
        self._max_snapshots_per_table = max(1, int(max_snapshots_per_table))
        self._lock = threading.RLock()
        self._entries: Dict[str, _ManagedSnapshotEntry] = {}

    def _raw_duckdb(self) -> Any:
        return _extract_raw_duckdb_connection(self._ibis_con())

    def _ibis_con(self) -> Any:
        con = getattr(self._controller, "con", None)
        return con if con is not None else getattr(getattr(self._controller, "backend", None), "con", None)

    def _ensure_meta_table_locked(self, raw_con: Any) -> None:
        raw_con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_ident(_META_TABLE)} (
                snapshot_id VARCHAR PRIMARY KEY,
                source_table VARCHAR NOT NULL,
                physical_table VARCHAR,
                source_revision BIGINT NOT NULL,
                schema_version VARCHAR NOT NULL,
                created_at DOUBLE NOT NULL,
                expires_at DOUBLE NOT NULL,
                byte_size BIGINT NOT NULL
            )
            """
        )

    def _estimate_duckdb_table_bytes(self, raw_con: Any, table: str) -> Tuple[int, int]:
        quoted = quote_ident(table)
        count_row = raw_con.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()  # nosec B608
        row_count = int(count_row[0]) if count_row else 0
        cols_desc = raw_con.execute(f"DESCRIBE {quoted}").fetchall()
        col_count = max(1, len(cols_desc))
        estimated_bytes = max(_MIN_RESERVATION_BYTES, row_count * col_count * 16)
        return row_count, estimated_bytes

    @contextmanager
    def _duckdb_cursor(self):
        base_con = self._raw_duckdb()
        if base_con is None:
            yield None
            return
        cursor_fn = getattr(base_con, "cursor", None)
        if callable(cursor_fn):
            cur = cursor_fn()
            try:
                yield cur
            finally:
                try:
                    cur.close()
                except Exception:
                    _LOG.debug("cursor close failed", exc_info=True)
        else:
            yield base_con

    def _drop_physical_table_safe(self, snapshot_id: str, physical_table: Optional[str]) -> None:
        with self._duckdb_cursor() as raw_con:
            if raw_con is None:
                return
            try:
                if physical_table:
                    raw_con.execute(f"DROP TABLE IF EXISTS {quote_ident(physical_table)}")
                self._ensure_meta_table_locked(raw_con)
                raw_con.execute(
                    f"DELETE FROM {quote_ident(_META_TABLE)} WHERE snapshot_id = ?",  # nosec B608
                    [snapshot_id],
                )
            except Exception:
                _LOG.debug("snapshot table cleanup failed", exc_info=True)

    def _evict_expired_locked(self) -> None:
        now = time.time()
        to_remove: List[Tuple[str, _ManagedSnapshotEntry]] = []
        for snap_id, entry in list(self._entries.items()):
            if entry.ref_count <= 0 and (
                entry.marked_for_deletion or entry.descriptor.expires_at <= now
            ):
                to_remove.append((snap_id, entry))
        for snap_id, entry in to_remove:
            self._entries.pop(snap_id, None)
            self._drop_physical_table_safe(snap_id, entry.descriptor.physical_table_name)
            if entry.lease is not None:
                entry.lease.release()

    def capture_snapshot(
        self,
        table: str,
        *,
        principal: Any = None,
        row_scope: Any = None,
        label: Optional[str] = None,
        kind: str = "baseline",
        ttl_seconds: Optional[float] = None,
        external_table: Optional[str] = None,
        time_travel_token: Optional[str] = None,
    ) -> SnapshotDescriptor:
        """Atomically capture a baseline or frozen snapshot under quota enforcement."""
        if kind not in {"baseline", "freeze_snapshot"}:
            raise SnapshotError(f"Unsupported snapshot kind '{kind}'.")
        with self._lock:
            self._evict_expired_locked()

        now = time.time()
        effective_ttl = self._default_ttl_seconds if ttl_seconds is None else max(0.1, float(ttl_seconds))
        expires_at = now + effective_ttl
        snapshot_id = f"snap_{uuid.uuid4().hex[:16]}"
        owner_subject = _principal_subject(principal)
        scope_fp = _row_scope_fingerprint(row_scope)
        pol_ver = _policy_version(row_scope)
        account = account_for_principal(principal)
        resolved_label = label or (
            f"Snapshot {time.strftime('%H:%M:%S', time.localtime(now))}"
            if kind == "baseline"
            else f"Frozen {time.strftime('%H:%M:%S', time.localtime(now))}"
        )

        # Strategy 1: External user-provided reference table (explicitly non-immutable).
        if external_table:
            ext_rev, ext_schema = self._source_adapter.read_coherent_revision(external_table)
            descriptor = SnapshotDescriptor(
                snapshot_id=snapshot_id,
                table=table,
                label=resolved_label,
                kind=kind,
                strategy=SnapshotStrategy.EXTERNAL_REFERENCE_TABLE,
                source_revision=ext_rev,
                schema_version=ext_schema,
                created_at=now,
                expires_at=expires_at,
                owner_subject=owner_subject,
                row_scope_fingerprint=scope_fp,
                policy_version=pol_ver,
                byte_size=0,
                row_count=0,
                immutable_verified=False,
                physical_table_name=external_table,
            )
            with self._lock:
                self._entries[snapshot_id] = _ManagedSnapshotEntry(
                    descriptor=descriptor, lease=None, account=account
                )
            return descriptor

        # Strategy 2: Native warehouse time-travel token (zero data copy).
        if time_travel_token:
            rev, schema_ver = self._source_adapter.read_coherent_revision(table)
            descriptor = SnapshotDescriptor(
                snapshot_id=snapshot_id,
                table=table,
                label=resolved_label,
                kind=kind,
                strategy=SnapshotStrategy.NATIVE_TIME_TRAVEL,
                source_revision=rev,
                schema_version=schema_ver,
                created_at=now,
                expires_at=expires_at,
                owner_subject=owner_subject,
                row_scope_fingerprint=scope_fp,
                policy_version=pol_ver,
                byte_size=0,
                row_count=0,
                immutable_verified=True,
                time_travel_token=str(time_travel_token),
            )
            with self._lock:
                self._entries[snapshot_id] = _ManagedSnapshotEntry(
                    descriptor=descriptor, lease=None, account=account
                )
            return descriptor

        # Strategy 3: True zero-copy immutable PyArrow reference (ONLY if unmutated).
        # The Arrow buffers are already resident in memory; reserve a lightweight
        # metadata lease so zero-copy snapshots do not double-charge tenant RAM.
        arrow_source = self._source_adapter.get_immutable_arrow_source(table)
        if arrow_source is not None and scope_fp == "global":
            src_rev, schema_ver, pa_table = arrow_source
            byte_size = int(pa_table.nbytes)
            lease = self._governor.reserve(ResourceKind.CACHE, _MIN_RESERVATION_BYTES, account=account)
            descriptor = SnapshotDescriptor(
                snapshot_id=snapshot_id,
                table=table,
                label=resolved_label,
                kind=kind,
                strategy=SnapshotStrategy.IMMUTABLE_ARROW_REF,
                source_revision=src_rev,
                schema_version=schema_ver,
                created_at=now,
                expires_at=expires_at,
                owner_subject=owner_subject,
                row_scope_fingerprint=scope_fp,
                policy_version=pol_ver,
                byte_size=byte_size,
                row_count=int(pa_table.num_rows),
                immutable_verified=True,
            )
            with self._lock:
                self._entries[snapshot_id] = _ManagedSnapshotEntry(
                    descriptor=descriptor,
                    lease=lease,
                    account=account,
                    arrow_table=pa_table,
                )
            return descriptor

        # Strategy 4: Persistent DuckDB catalog snapshot (cross-connection visible).
        if self._raw_duckdb() is None:
            raise SnapshotError(
                f"Backend for table '{table}' requires time_travel_token or external_table for baselines."
            )

        physical_name = f"__pivot_{snapshot_id}"
        lease: Optional[ResourceLease] = None
        table_created = False

        with self._source_adapter._execution_locks.lock_for(table):
            with self._duckdb_cursor() as raw_con:
                try:
                    row_count, estimated_bytes = self._estimate_duckdb_table_bytes(raw_con, table)
                    # Pre-reserve capacity BEFORE creating the snapshot table.
                    lease = self._governor.reserve(
                        ResourceKind.CACHE, estimated_bytes, account=account
                    )

                    where_sql, where_params = _build_rls_where_sql(row_scope)
                    src_rev, schema_ver = self._source_adapter.read_coherent_revision(table)

                    raw_con.execute("BEGIN TRANSACTION")
                    try:
                        self._ensure_meta_table_locked(raw_con)
                        raw_con.execute(
                            f"CREATE TABLE {quote_ident(physical_name)} AS "  # nosec B608
                            f"SELECT * FROM {quote_ident(table)}{where_sql}",
                            where_params,
                        )
                        table_created = True
                        snap_count_row = raw_con.execute(
                            f"SELECT COUNT(*) FROM {quote_ident(physical_name)}"  # nosec B608
                        ).fetchone()
                        actual_rows = int(snap_count_row[0]) if snap_count_row else 0
                        actual_bytes = max(
                            _MIN_RESERVATION_BYTES,
                            int(estimated_bytes * (actual_rows / max(1, row_count))),
                        )
                        raw_con.execute(
                            f"INSERT INTO {quote_ident(_META_TABLE)} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",  # nosec B608
                            [
                                snapshot_id,
                                table,
                                physical_name,
                                src_rev,
                                schema_ver,
                                now,
                                expires_at,
                                actual_bytes,
                            ],
                        )
                        raw_con.execute("COMMIT")
                    except Exception:
                        try:
                            raw_con.execute("ROLLBACK")
                        except Exception:
                            _LOG.debug("rollback after failed capture failed", exc_info=True)
                        if table_created:
                            self._drop_physical_table_safe(snapshot_id, physical_name)
                        raise

                    # Adjust reservation to actual bytes if larger.
                    if lease is not None and actual_bytes > estimated_bytes:
                        try:
                            lease.grow(actual_bytes - estimated_bytes)
                        except ResourceExhaustedError:
                            self._drop_physical_table_safe(snapshot_id, physical_name)
                            raise

                    descriptor = SnapshotDescriptor(
                        snapshot_id=snapshot_id,
                        table=table,
                        label=resolved_label,
                        kind=kind,
                        strategy=SnapshotStrategy.DUCKDB_PERSISTENT_SNAPSHOT,
                        source_revision=src_rev,
                        schema_version=schema_ver,
                        created_at=now,
                        expires_at=expires_at,
                        owner_subject=owner_subject,
                        row_scope_fingerprint=scope_fp,
                        policy_version=pol_ver,
                        byte_size=actual_bytes,
                        row_count=actual_rows,
                        immutable_verified=True,
                        physical_table_name=physical_name,
                    )
                    with self._lock:
                        self._entries[snapshot_id] = _ManagedSnapshotEntry(
                            descriptor=descriptor,
                            lease=lease,
                            account=account,
                        )
                    return descriptor
                except Exception:
                    if lease is not None:
                        lease.release()
                    if table_created:
                        self._drop_physical_table_safe(snapshot_id, physical_name)
                    raise

    def get_descriptor(
        self,
        snapshot_id: str,
        *,
        principal: Any = None,
        row_scope: Any = None,
        require_unexpired: bool = True,
    ) -> SnapshotDescriptor:
        with self._lock:
            entry = self._entries.get(snapshot_id)
            if entry is None:
                raise SnapshotExpiredError(f"Snapshot '{snapshot_id}' does not exist or has expired.")
            if require_unexpired and entry.descriptor.expires_at <= time.time():
                self._evict_expired_locked()
                raise SnapshotExpiredError(f"Snapshot '{snapshot_id}' has expired.")
            caller_scope = _row_scope_fingerprint(row_scope)
            if (
                entry.descriptor.row_scope_fingerprint != "global"
                and caller_scope != entry.descriptor.row_scope_fingerprint
            ):
                raise SnapshotAuthorizationError(
                    f"Caller authorization scope cannot access snapshot '{snapshot_id}'."
                )
            return entry.descriptor

    def acquire_snapshot(
        self,
        snapshot_id: str,
        *,
        principal: Any = None,
        row_scope: Any = None,
    ) -> SnapshotDescriptor:
        with self._lock:
            descriptor = self.get_descriptor(
                snapshot_id,
                principal=principal,
                row_scope=row_scope,
                require_unexpired=True,
            )
            self._entries[snapshot_id].ref_count += 1
            return descriptor

    def release_snapshot(self, snapshot_id: str) -> None:
        with self._lock:
            entry = self._entries.get(snapshot_id)
            if entry is None:
                return
            entry.ref_count = max(0, entry.ref_count - 1)
            self._evict_expired_locked()

    def delete_snapshot(self, snapshot_id: str) -> bool:
        with self._lock:
            entry = self._entries.get(snapshot_id)
            if entry is None:
                return False
            entry.marked_for_deletion = True
            self._evict_expired_locked()
            return True

    def list_snapshots(
        self,
        table: Optional[str] = None,
        *,
        row_scope: Any = None,
    ) -> List[SnapshotDescriptor]:
        caller_scope = _row_scope_fingerprint(row_scope)
        now = time.time()
        with self._lock:
            self._evict_expired_locked()
            result: List[SnapshotDescriptor] = []
            for entry in self._entries.values():
                desc = entry.descriptor
                if desc.expires_at <= now:
                    continue
                if table is not None and desc.table != table:
                    continue
                if desc.row_scope_fingerprint != "global" and desc.row_scope_fingerprint != caller_scope:
                    continue
                result.append(desc)
            return sorted(result, key=lambda d: d.created_at, reverse=True)

    def resolve_snapshot_table(
        self,
        snapshot_id: str,
        *,
        principal: Any = None,
        row_scope: Any = None,
        connection: Optional[Any] = None,
    ) -> Tuple[Any, SnapshotDescriptor]:
        """Resolve a snapshot to a queryable Ibis Table expression on any connection."""
        with self._lock:
            descriptor = self.get_descriptor(
                snapshot_id,
                principal=principal,
                row_scope=row_scope,
                require_unexpired=True,
            )
            entry = self._entries[snapshot_id]
            arrow_table = entry.arrow_table

        if descriptor.strategy == SnapshotStrategy.IMMUTABLE_ARROW_REF and arrow_table is not None:
            return ibis.memtable(arrow_table), descriptor

        con = connection if connection is not None else self._ibis_con()
        if con is None:
            raise SnapshotError("No backend connection available to resolve snapshot.")

        if descriptor.strategy in {
            SnapshotStrategy.DUCKDB_PERSISTENT_SNAPSHOT,
            SnapshotStrategy.EXTERNAL_REFERENCE_TABLE,
        }:
            if not descriptor.physical_table_name:
                raise SnapshotError(f"Snapshot '{snapshot_id}' has no physical table name.")
            planning_lock = getattr(self._controller, "planning_lock", None)
            if planning_lock is not None:
                with planning_lock:
                    return con.table(descriptor.physical_table_name), descriptor
            return con.table(descriptor.physical_table_name), descriptor

        if descriptor.strategy == SnapshotStrategy.NATIVE_TIME_TRAVEL:
            planning_lock = getattr(self._controller, "planning_lock", None)
            if planning_lock is not None:
                with planning_lock:
                    return self._controller.resolve_table(descriptor.table), descriptor
            return self._controller.resolve_table(descriptor.table), descriptor

        raise SnapshotError(f"Unsupported snapshot strategy '{descriptor.strategy}'.")

    def close(self) -> None:
        with self._lock:
            for snap_id, entry in list(self._entries.items()):
                self._drop_physical_table_safe(snap_id, entry.descriptor.physical_table_name)
                if entry.lease is not None:
                    entry.lease.release()
            self._entries.clear()
