"""Versioned source adapter with transactional revisions and capability tiers.

This module owns Component 1 of the streaming and analytical diff architecture:
- Single DuckDB runtime owner enforcement for local file-backed databases
- Explicit four-tier SourceChangeProvider capability reporting
- Atomic SQL mutations (BEGIN -> pre-image lookup -> mutate -> bump revision -> COMMIT)
- Lock-free-computation publication via a microsecond pointer-only `_publish_lock`
- Bounded per-table commit ring buffers and per-subscription bounded inboxes
- Process-boot `server_epoch_id` for crash/restart discontinuity detection
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
import datetime as dt
from decimal import Decimal
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from typing import Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from enum import StrEnum
except ImportError:  # Python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        __str__ = str.__str__
        __format__ = str.__format__

import pyarrow as pa

from ..cdc.models import Change
from ..execution_locks import TableExecutionLockRegistry
from ..util.sql_builder import quote_ident

_LOGGER = logging.getLogger(__name__)
_REVISIONS_TABLE = "__pivot_table_revisions"
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ACTIVE_FILE_OWNERS_LOCK = threading.RLock()
_ACTIVE_FILE_OWNERS: Dict[str, str] = {}


class SourceCapabilityTier(StrEnum):
    """Explicit backend change-detection capability levels."""

    TIER_1_EXACT_CDC = "exact_cdc"
    TIER_2_CHANGE_TOKEN = "version_tracked"
    TIER_3_TIME_TRAVEL_ONLY = "snapshot_pinned"
    TIER_4_PERIODIC_REFRESH = "periodic_refresh"


class DuckDBSingleOwnerError(RuntimeError):
    """Raised when unsafe concurrent file-backed DuckDB write ownership is attempted."""

    code = "duckdb_single_owner_violation"


class SourceMutationError(RuntimeError):
    """Raised when an atomic source mutation fails validation or SQL execution."""


@dataclass(frozen=True)
class SourceCapabilities:
    """Authoritative freshness and streaming capabilities for one logical table."""

    table: str
    tier: SourceCapabilityTier
    backend_name: str
    supports_exact_row_deltas: bool
    supports_time_travel: bool
    supports_persistent_snapshots: bool
    single_writer_owner: bool
    reason: str = ""

    def to_wire_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "freshnessMode": self.tier.value,
            "backendName": self.backend_name,
            "supportsExactRowDeltas": self.supports_exact_row_deltas,
            "supportsTimeTravel": self.supports_time_travel,
            "supportsPersistentSnapshots": self.supports_persistent_snapshots,
            "singleWriterOwner": self.single_writer_owner,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CommittedChangeBatch:
    """One atomically committed source revision and its row transitions."""

    table: str
    source_revision: int
    schema_version: str
    committed_at: float
    server_epoch_id: str
    old_rows: Tuple[Dict[str, Any], ...] = ()
    new_rows: Tuple[Dict[str, Any], ...] = ()
    changes: Tuple[Change, ...] = ()
    row_deltas_complete: bool = True
    schema_changed: bool = False
    mode: str = "upsert"


@dataclass
class SubscriptionInboxItem:
    """One enqueued batch inside a per-subscription ordered mailbox."""

    batch: CommittedChangeBatch
    is_post_handoff: bool = False
    is_handoff_seal: bool = False


class SubscriptionInbox:
    """Bounded per-subscription ordered queue for atomic bootstrap and handoff.

    All mutations to queue membership and handoff flags occur under the adapter's
    lightweight `_publish_lock` (O(1) pointer/deque ops only; never SQL or
    aggregation). Overflow sets `overflowed = True` and forces a clean RESYNC.
    """

    def __init__(
        self,
        *,
        inbox_id: str,
        table: str,
        floor_revision: int,
        max_batches: int = 128,
        on_notify: Optional[Callable[[], None]] = None,
    ) -> None:
        self.inbox_id = inbox_id
        self.table = table
        self.floor_revision = int(floor_revision)
        self.max_batches = max(1, int(max_batches))
        self._items: Deque[SubscriptionInboxItem] = deque()
        self.overflowed: bool = False
        self.live_routed: bool = False
        self.handoff_revision: Optional[int] = None
        self.closed: bool = False
        self._on_notify = on_notify
        self._event = threading.Event()

    def push_locked(self, batch: CommittedChangeBatch) -> None:
        """Append one committed batch while `_publish_lock` is held (O(1))."""
        if self.closed:
            return
        if len(self._items) >= self.max_batches:
            self.overflowed = True
            self._items.clear()
            self._event.set()
            if self._on_notify is not None:
                try:
                    self._on_notify()
                except Exception:  # pragma: no cover - callback isolation
                    pass
            return
        self._items.append(
            SubscriptionInboxItem(
                batch=batch,
                is_post_handoff=self.live_routed,
                is_handoff_seal=False,
            )
        )
        self._event.set()
        if self._on_notify is not None:
            try:
                self._on_notify()
            except Exception:  # pragma: no cover - callback isolation
                pass

    def pop_locked(self) -> Optional[SubscriptionInboxItem]:
        if not self._items:
            self._event.clear()
            return None
        item = self._items.popleft()
        if not self._items:
            self._event.clear()
        return item

    def peek_tail_locked(self) -> Optional[SubscriptionInboxItem]:
        return self._items[-1] if self._items else None

    def __len__(self) -> int:
        return len(self._items)

    def wait(self, timeout: Optional[float] = None) -> bool:
        return self._event.wait(timeout=timeout)


class SourceChangeProvider:
    """Resolves verified backend change-tracking capabilities without false claims."""

    def __init__(
        self,
        *,
        change_token_providers: Optional[Mapping[str, Callable[[str], Optional[str]]]] = None,
    ) -> None:
        self._change_token_providers: Dict[str, Callable[[str], Optional[str]]] = dict(
            change_token_providers or {}
        )

    def register_change_token_provider(
        self, table: str, provider: Callable[[str], Optional[str]]
    ) -> None:
        self._change_token_providers[str(table)] = provider

    def resolve_capabilities(
        self,
        table: str,
        *,
        backend_name: str,
        managed_local_duckdb: bool,
        has_exact_push_cdc: bool = False,
        supports_time_travel: bool = False,
    ) -> SourceCapabilities:
        normalized_backend = str(backend_name or "unknown").strip().lower()
        if managed_local_duckdb or has_exact_push_cdc:
            return SourceCapabilities(
                table=table,
                tier=SourceCapabilityTier.TIER_1_EXACT_CDC,
                backend_name=normalized_backend,
                supports_exact_row_deltas=True,
                supports_time_travel=supports_time_travel,
                supports_persistent_snapshots=normalized_backend == "duckdb",
                single_writer_owner=normalized_backend == "duckdb",
                reason="transactional_versioned_source",
            )
        if table in self._change_token_providers:
            return SourceCapabilities(
                table=table,
                tier=SourceCapabilityTier.TIER_2_CHANGE_TOKEN,
                backend_name=normalized_backend,
                supports_exact_row_deltas=False,
                supports_time_travel=supports_time_travel,
                supports_persistent_snapshots=False,
                single_writer_owner=False,
                reason="explicit_verified_change_token",
            )
        if supports_time_travel:
            return SourceCapabilities(
                table=table,
                tier=SourceCapabilityTier.TIER_3_TIME_TRAVEL_ONLY,
                backend_name=normalized_backend,
                supports_exact_row_deltas=False,
                supports_time_travel=True,
                supports_persistent_snapshots=False,
                single_writer_owner=False,
                reason="native_warehouse_time_travel",
            )
        return SourceCapabilities(
            table=table,
            tier=SourceCapabilityTier.TIER_4_PERIODIC_REFRESH,
            backend_name=normalized_backend,
            supports_exact_row_deltas=False,
            supports_time_travel=False,
            supports_persistent_snapshots=False,
            single_writer_owner=False,
            reason="no_verified_table_change_signal",
        )

    def read_external_change_token(self, table: str) -> Optional[str]:
        provider = self._change_token_providers.get(table)
        if provider is None:
            return None
        token = provider(table)
        return None if token is None else str(token)


def _extract_raw_duckdb_connection(con: Any) -> Any:
    """Unwrap an Ibis DuckDB backend to its raw duckdb.DuckDBPyConnection if present."""
    if con is None:
        return None
    if hasattr(con, "con") and getattr(con, "name", "").lower() == "duckdb":
        return con.con
    if hasattr(con, "execute") and hasattr(con, "register") and hasattr(con, "sql"):
        return con
    return None


def _normalize_sql_param(value: Any) -> Any:
    if isinstance(value, Decimal):
        return value
    return value


def _compute_schema_version(column_types: Sequence[Tuple[str, str]]) -> str:
    canonical = json.dumps(
        [[str(name), str(dtype)] for name, dtype in column_types],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


def _extract_backend_file_path(controller: Any) -> Optional[str]:
    uri = str(
        getattr(controller, "backend_uri", None)
        or getattr(getattr(controller, "backend", None), "connection_uri", None)
        or ""
    ).strip()
    if not uri or uri == ":memory:" or uri.startswith((":memory:", "duckdb://:memory:")):
        return None
    lower = uri.lower()
    if lower.startswith("duckdb:///"):
        raw_path = uri[len("duckdb:///"):]
    elif lower.startswith("duckdb://"):
        raw_path = uri[len("duckdb://"):]
    elif "://" not in uri and lower.endswith((".db", ".duckdb")):
        raw_path = uri
    else:
        return None
    cleaned = raw_path.split("?", 1)[0].strip()
    if len(cleaned) >= 3 and cleaned[0] == "/" and cleaned[2] == ":":
        cleaned = cleaned[1:]
    if not cleaned or cleaned == ":memory:" or cleaned.startswith(":memory:"):
        return None
    return os.path.abspath(cleaned)


class VersionedSourceAdapter:
    """Component 1: Transactional source revisions, immutable Arrow refs, and inbox fanout."""

    def __init__(
        self,
        controller: Any,
        *,
        execution_locks: Optional[TableExecutionLockRegistry] = None,
        distributed_runtime: Optional[Any] = None,
        change_provider: Optional[SourceChangeProvider] = None,
        max_retained_batches: int = 128,
        allow_multi_worker_duckdb: bool = False,
    ) -> None:
        self._controller = controller
        self._execution_locks = (
            execution_locks
            or getattr(controller, "execution_locks", None)
            or TableExecutionLockRegistry()
        )
        self._distributed_runtime = (
            distributed_runtime
            if distributed_runtime is not None
            else getattr(controller, "distributed_runtime", None)
        )
        self.change_provider = change_provider or SourceChangeProvider()
        self.max_retained_batches = max(8, int(max_retained_batches))
        self.server_epoch_id: str = uuid.uuid4().hex
        self._owner_token: str = uuid.uuid4().hex

        # Enforce single DuckDB runtime owner for file-backed DuckDB databases.
        self._duckdb_file_path = _extract_backend_file_path(controller)
        if self._duckdb_file_path is not None:
            if self._distributed_runtime is not None and not allow_multi_worker_duckdb:
                raise DuckDBSingleOwnerError(
                    "File-backed DuckDB requires a Single DuckDB Runtime Owner and cannot "
                    "run concurrent multi-worker writers under PIVOT_DISTRIBUTED_RUNTIME."
                )
            with _ACTIVE_FILE_OWNERS_LOCK:
                existing_owner = _ACTIVE_FILE_OWNERS.get(self._duckdb_file_path)
                if existing_owner is not None and not allow_multi_worker_duckdb:
                    raise DuckDBSingleOwnerError(
                        f"File-backed DuckDB source '{self._duckdb_file_path}' is already owned "
                        "by another VersionedSourceAdapter instance."
                    )
                _ACTIVE_FILE_OWNERS[self._duckdb_file_path] = self._owner_token

        # Lightweight pointer-only lock: NEVER holds SQL execution or aggregation!
        self._publish_lock = threading.RLock()
        self._ring_buffers: Dict[str, Deque[CommittedChangeBatch]] = {}
        self._inboxes: Dict[str, Dict[str, SubscriptionInbox]] = {}
        self._last_known_revisions: Dict[str, int] = {}
        self._last_known_schemas: Dict[str, str] = {}
        # Immutable PyArrow table references retained when loaded via Arrow (true zero-copy).
        self._immutable_arrow_sources: Dict[str, Tuple[int, str, pa.Table]] = {}
        # Track failed Redis observation publishes so reconciler can recover cleanly.
        self._pending_redis_reconciliations: Dict[str, int] = {}
        self._external_tokens: Dict[str, Optional[str]] = {}
        self._closed = False

    def close(self) -> None:
        with self._publish_lock:
            self._closed = True
            for table_inboxes in self._inboxes.values():
                for inbox in table_inboxes.values():
                    inbox.closed = True
                    inbox._event.set()
            self._inboxes.clear()
        if self._duckdb_file_path is not None:
            with _ACTIVE_FILE_OWNERS_LOCK:
                if _ACTIVE_FILE_OWNERS.get(self._duckdb_file_path) == self._owner_token:
                    _ACTIVE_FILE_OWNERS.pop(self._duckdb_file_path, None)

    def _ibis_connection(self) -> Any:
        planner_con = getattr(getattr(self._controller, "planner", None), "con", None)
        if planner_con is not None:
            return planner_con
        return getattr(getattr(self._controller, "backend", None), "con", None)

    def _raw_duckdb(self) -> Any:
        return _extract_raw_duckdb_connection(self._ibis_connection())

    @contextmanager
    def _duckdb_cursor(self):
        """Yield an isolated DuckDB cursor (thread-safe ClientContext) when available."""
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
                    pass
        else:
            yield base_con

    def _backend_name(self) -> str:
        con = self._ibis_connection()
        return str(getattr(con, "name", "") or "unknown").strip().lower()

    def _ensure_revisions_table_locked(self, raw_con: Any) -> None:
        raw_con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_ident(_REVISIONS_TABLE)} (
                table_name VARCHAR PRIMARY KEY,
                data_revision BIGINT NOT NULL,
                schema_version VARCHAR NOT NULL,
                committed_at DOUBLE NOT NULL
            )
            """
        )

    def _inspect_schema_locked(self, table: str, cur: Any = None) -> Tuple[str, List[str], List[Tuple[str, str]]]:
        if cur is not None:
            quoted = quote_ident(table)
            desc = cur.execute(f"DESCRIBE {quoted}").fetchall()
            col_types = [(str(row[0]), str(row[1])) for row in desc]
            cols = [name for name, _ in col_types]
            return _compute_schema_version(col_types), cols, col_types

        with self._duckdb_cursor() as local_cur:
            if local_cur is not None:
                quoted = quote_ident(table)
                desc = local_cur.execute(f"DESCRIBE {quoted}").fetchall()
                col_types = [(str(row[0]), str(row[1])) for row in desc]
                cols = [name for name, _ in col_types]
                return _compute_schema_version(col_types), cols, col_types

        ibis_con = self._ibis_connection()
        if ibis_con is None:
            raise SourceMutationError(f"No backend connection available for table '{table}'.")
        planning_lock = getattr(self._controller, "planning_lock", None)
        if planning_lock is not None:
            with planning_lock:
                tbl = self._controller.resolve_table(table)
                schema = tbl.schema()
        else:
            tbl = self._controller.resolve_table(table)
            schema = tbl.schema()
        col_types = [(str(name), str(dtype)) for name, dtype in zip(schema.names, schema.types)]
        cols = [name for name, _ in col_types]
        return _compute_schema_version(col_types), cols, col_types

    def get_source_capabilities(self, table: str) -> SourceCapabilities:
        backend_name = self._backend_name()
        managed_local_duckdb = self._raw_duckdb() is not None
        supports_time_travel = backend_name in {"snowflake", "bigquery", "databricks"}
        return self.change_provider.resolve_capabilities(
            table,
            backend_name=backend_name,
            managed_local_duckdb=managed_local_duckdb,
            supports_time_travel=supports_time_travel,
        )

    def register_immutable_arrow_source(
        self,
        table: str,
        arrow_table: pa.Table,
        *,
        revision: Optional[int] = None,
    ) -> Tuple[int, str]:
        """Record an immutable pa.Table reference and sync its DuckDB revision row."""
        col_types = [(str(f.name), str(f.type)) for f in arrow_table.schema]
        schema_version = _compute_schema_version(col_types)
        with self._execution_locks.lock_for(table):
            with self._duckdb_cursor() as raw_con:
                committed_rev = 0 if revision is None else int(revision)
                if raw_con is not None:
                    try:
                        schema_version, _, _ = self._inspect_schema_locked(table, cur=raw_con)
                    except Exception:
                        pass
                    self._ensure_revisions_table_locked(raw_con)
                    row = raw_con.execute(
                        f"SELECT data_revision FROM {quote_ident(_REVISIONS_TABLE)} WHERE table_name = ?",
                        [table],
                    ).fetchone()
                    if row is None:
                        committed_rev = max(1, committed_rev)
                        raw_con.execute(
                            f"INSERT INTO {quote_ident(_REVISIONS_TABLE)} VALUES (?, ?, ?, ?)",
                            [table, committed_rev, schema_version, time.time()],
                        )
                    else:
                        committed_rev = int(row[0]) + 1 if revision is None else int(revision)
                        raw_con.execute(
                            f"UPDATE {quote_ident(_REVISIONS_TABLE)} "
                            "SET data_revision = ?, schema_version = ?, committed_at = ? "
                            "WHERE table_name = ?",
                            [committed_rev, schema_version, time.time(), table],
                        )
                else:
                    committed_rev = max(1, self._last_known_revisions.get(table, 0) + 1)

            with self._publish_lock:
                self._immutable_arrow_sources[table] = (committed_rev, schema_version, arrow_table)
                self._last_known_revisions[table] = committed_rev
                self._last_known_schemas[table] = schema_version
            return committed_rev, schema_version

    def get_immutable_arrow_source(self, table: str) -> Optional[Tuple[int, str, pa.Table]]:
        """Return retained immutable pa.Table if the table has not been mutated in-place."""
        with self._publish_lock:
            return self._immutable_arrow_sources.get(table)

    def read_coherent_revision(self, table: str) -> Tuple[int, str]:
        """Read `(source_revision, schema_version)` atomically under the table lock."""
        with self._execution_locks.lock_for(table):
            with self._duckdb_cursor() as raw_con:
                schema_version, _, _ = self._inspect_schema_locked(table, cur=raw_con)
                if raw_con is not None:
                    self._ensure_revisions_table_locked(raw_con)
                    row = raw_con.execute(
                        f"SELECT data_revision, schema_version FROM {quote_ident(_REVISIONS_TABLE)} "
                        "WHERE table_name = ?",
                        [table],
                    ).fetchone()
                    if row is None:
                        rev = max(1, self._last_known_revisions.get(table, 0))
                        raw_con.execute(
                            f"INSERT INTO {quote_ident(_REVISIONS_TABLE)} VALUES (?, ?, ?, ?)",
                            [table, rev, schema_version, time.time()],
                        )
                    else:
                        rev = int(row[0])
                        stored_schema = str(row[1])
                        if stored_schema != schema_version:
                            rev += 1
                            raw_con.execute(
                                f"UPDATE {quote_ident(_REVISIONS_TABLE)} "
                                "SET data_revision = ?, schema_version = ?, committed_at = ? "
                                "WHERE table_name = ?",
                                [rev, schema_version, time.time(), table],
                            )
                    with self._publish_lock:
                        self._last_known_revisions[table] = rev
                        self._last_known_schemas[table] = schema_version
                    return rev, schema_version

            # Non-DuckDB / external source: reconcile via explicit SourceChangeProvider token.
            token = self.change_provider.read_external_change_token(table)
            with self._publish_lock:
                prev_token = self._external_tokens.get(table)
                rev = self._last_known_revisions.get(table, 1)
                prev_schema = self._last_known_schemas.get(table)
                if (token is not None and token != prev_token) or (
                    prev_schema is not None and prev_schema != schema_version
                ):
                    rev += 1
                self._external_tokens[table] = token
                self._last_known_revisions[table] = rev
                self._last_known_schemas[table] = schema_version
                return rev, schema_version

    def current_revision_hint(self, table: str) -> int:
        with self._publish_lock:
            return self._last_known_revisions.get(table, 0)

    def register_inbox_cursor(
        self,
        table: str,
        *,
        max_batches: int = 128,
        on_notify: Optional[Callable[[], None]] = None,
    ) -> SubscriptionInbox:
        """Phase 1 of bootstrap: O(1) inbox registration under `_publish_lock`."""
        with self._publish_lock:
            floor_rev = self._last_known_revisions.get(table, 0)
            inbox = SubscriptionInbox(
                inbox_id=uuid.uuid4().hex,
                table=table,
                floor_revision=floor_rev,
                max_batches=max_batches,
                on_notify=on_notify,
            )
            self._inboxes.setdefault(table, {})[inbox.inbox_id] = inbox
            return inbox

    def unregister_inbox_cursor(self, inbox: SubscriptionInbox) -> None:
        with self._publish_lock:
            inbox.closed = True
            inbox._event.set()
            table_inboxes = self._inboxes.get(inbox.table)
            if table_inboxes is not None:
                table_inboxes.pop(inbox.inbox_id, None)

    def pop_inbox_item(self, inbox: SubscriptionInbox) -> Optional[SubscriptionInboxItem]:
        with self._publish_lock:
            return inbox.pop_locked()

    def seal_atomic_handoff(
        self,
        inbox: SubscriptionInbox,
        last_applied_revision: int,
    ) -> int:
        """Blocker 1B: <5µs lock-only pointer flip for ATOMIC_HANDOFF.

        Zero SQL, zero aggregation, and zero serialization happen inside
        `_publish_lock`. If any batch arrived in the microsecond before the
        lock was acquired, it is tagged as `is_handoff_seal = True` so the
        subscription worker drains it outside the lock before emitting the
        sealed SNAPSHOT at `R_handoff`.
        """
        with self._publish_lock:
            tail = inbox.peek_tail_locked()
            if tail is not None and not tail.is_post_handoff:
                tail.is_handoff_seal = True
                handoff_rev = int(tail.batch.source_revision)
            else:
                handoff_rev = int(last_applied_revision)
            inbox.handoff_revision = handoff_rev
            inbox.live_routed = True
            return handoff_rev

    def get_replay_batches(
        self, table: str, after_revision: int
    ) -> Optional[List[CommittedChangeBatch]]:
        """Return contiguous batches `(after_revision, latest]` or None if history expired."""
        with self._publish_lock:
            ring = self._ring_buffers.get(table)
            latest_rev = self._last_known_revisions.get(table, 0)
            if after_revision == latest_rev:
                return []
            if not ring:
                return None
            min_rev = ring[0].source_revision
            if after_revision < min_rev - 1:
                return None
            batches = [b for b in ring if b.source_revision > after_revision]
            # Verify strict contiguity (no missing revisions after crash/restart).
            expected = after_revision + 1
            for batch in batches:
                if batch.source_revision != expected:
                    return None
                expected += 1
            return batches

    def _validate_columns(
        self, allowed_columns: Sequence[str], candidate_columns: Iterable[str]
    ) -> List[str]:
        allowed_set = set(allowed_columns)
        validated: List[str] = []
        for col in candidate_columns:
            col_name = str(col or "").strip()
            if not col_name or col_name not in allowed_set:
                raise SourceMutationError(f"Column '{col_name}' is not in table allow-list.")
            if not _SAFE_IDENTIFIER_RE.match(col_name):
                raise SourceMutationError(f"Unsafe column identifier '{col_name}'.")
            validated.append(col_name)
        return validated

    def _row_identity_key(
        self, row: Mapping[str, Any], key_fields: Sequence[str]
    ) -> Tuple[Any, ...]:
        return tuple(row.get(k) for k in key_fields)

    def _fetch_existing_rows_by_keys_locked(
        self,
        raw_con: Any,
        table: str,
        table_columns: Sequence[str],
        key_fields: Sequence[str],
        key_tuples: Sequence[Tuple[Any, ...]],
    ) -> List[Dict[str, Any]]:
        if not key_tuples or not key_fields:
            return []
        quoted_table = quote_ident(table)
        select_cols = ", ".join(quote_ident(c) for c in table_columns)
        if len(key_fields) == 1:
            key_col = quote_ident(key_fields[0])
            unique_vals = list(dict.fromkeys(t[0] for t in key_tuples if t[0] is not None))
            if not unique_vals:
                return []
            placeholders = ", ".join("?" for _ in unique_vals)
            sql = f"SELECT {select_cols} FROM {quoted_table} WHERE {key_col} IN ({placeholders})"
            fetched = raw_con.execute(sql, [_normalize_sql_param(v) for v in unique_vals]).fetchall()
        else:
            unique_tuples = list(dict.fromkeys(key_tuples))
            clauses: List[str] = []
            params: List[Any] = []
            for tup in unique_tuples:
                parts: List[str] = []
                for col, val in zip(key_fields, tup):
                    if val is None:
                        parts.append(f"{quote_ident(col)} IS NULL")
                    else:
                        parts.append(f"{quote_ident(col)} = ?")
                        params.append(_normalize_sql_param(val))
                clauses.append("(" + " AND ".join(parts) + ")")
            sql = f"SELECT {select_cols} FROM {quoted_table} WHERE " + " OR ".join(clauses)
            fetched = raw_con.execute(sql, params).fetchall()
        return [dict(zip(table_columns, row)) for row in fetched]

    def _delete_by_keys_locked(
        self,
        raw_con: Any,
        table: str,
        key_fields: Sequence[str],
        key_tuples: Sequence[Tuple[Any, ...]],
    ) -> None:
        if not key_tuples or not key_fields:
            return
        quoted_table = quote_ident(table)
        if len(key_fields) == 1:
            key_col = quote_ident(key_fields[0])
            unique_vals = list(dict.fromkeys(t[0] for t in key_tuples if t[0] is not None))
            if not unique_vals:
                return
            placeholders = ", ".join("?" for _ in unique_vals)
            raw_con.execute(
                f"DELETE FROM {quoted_table} WHERE {key_col} IN ({placeholders})",
                [_normalize_sql_param(v) for v in unique_vals],
            )
            return
        unique_tuples = list(dict.fromkeys(key_tuples))
        clauses: List[str] = []
        params: List[Any] = []
        for tup in unique_tuples:
            parts: List[str] = []
            for col, val in zip(key_fields, tup):
                if val is None:
                    parts.append(f"{quote_ident(col)} IS NULL")
                else:
                    parts.append(f"{quote_ident(col)} = ?")
                    params.append(_normalize_sql_param(val))
            clauses.append("(" + " AND ".join(parts) + ")")
        raw_con.execute(
            f"DELETE FROM {quoted_table} WHERE " + " OR ".join(clauses),
            params,
        )

    def _insert_rows_locked(
        self,
        raw_con: Any,
        table: str,
        table_columns: Sequence[str],
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        if not rows:
            return
        quoted_table = quote_ident(table)
        col_list = ", ".join(quote_ident(c) for c in table_columns)
        placeholders = ", ".join("?" for _ in table_columns)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        param_rows = [
            [_normalize_sql_param(row.get(c)) for c in table_columns]
            for row in rows
        ]
        raw_con.executemany(sql, param_rows)

    def apply_mutation_batch(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        mode: str = "upsert",
        key_fields: Optional[Sequence[str]] = None,
        source: str = "push",
    ) -> CommittedChangeBatch:
        """Execute an atomic SQL mutation and publish one committed revision.

        Contract:
            BEGIN -> PK pre-image lookup -> mutate -> bump __pivot_table_revisions
            -> COMMIT -> publish under `_publish_lock` (<5µs).
        """
        normalized_mode = str(mode or "upsert").strip().lower()
        if normalized_mode not in {"insert", "upsert", "update", "delete", "replace"}:
            raise SourceMutationError(f"Unsupported stream mutation mode: '{mode}'.")
        if not rows and normalized_mode != "replace":
            rev, schema_ver = self.read_coherent_revision(table)
            return CommittedChangeBatch(
                table=table,
                source_revision=rev,
                schema_version=schema_ver,
                committed_at=time.time(),
                server_epoch_id=self.server_epoch_id,
                mode=normalized_mode,
            )

        if self._raw_duckdb() is None:
            raise SourceMutationError(
                f"Atomic local mutation is only supported on managed DuckDB tables (table='{table}')."
            )

        with self._execution_locks.lock_for(table):
            with self._duckdb_cursor() as raw_con:
                self._ensure_revisions_table_locked(raw_con)
                prev_schema_ver, table_columns, _ = self._inspect_schema_locked(table, cur=raw_con)
                if not table_columns:
                    raise SourceMutationError(f"Table '{table}' has no columns.")

                resolved_keys: List[str] = []
                if key_fields:
                    resolved_keys = self._validate_columns(table_columns, key_fields)
                elif normalized_mode in {"upsert", "update", "delete"}:
                    if "id" in table_columns:
                        resolved_keys = ["id"]
                    else:
                        raise SourceMutationError(
                            f"Mode '{normalized_mode}' requires explicit key_fields when table '{table}' has no 'id' column."
                        )

                normalized_rows: List[Dict[str, Any]] = []
                for raw_row in rows:
                    if not isinstance(raw_row, Mapping):
                        raise SourceMutationError("Stream mutation rows must be mappings.")
                    # Validate all incoming keys against table allow-list.
                    for k in raw_row.keys():
                        if str(k) not in table_columns:
                            raise SourceMutationError(
                                f"Unknown column '{k}' in mutation row for table '{table}'."
                            )
                    normalized_rows.append(dict(raw_row))

                in_txn = False
                try:
                    raw_con.execute("BEGIN TRANSACTION")
                    in_txn = True

                    old_rows: List[Dict[str, Any]] = []
                    new_rows: List[Dict[str, Any]] = []
                    changes: List[Change] = []

                    if normalized_mode == "replace":
                        quoted_table = quote_ident(table)
                        select_cols = ", ".join(quote_ident(c) for c in table_columns)
                        existing_all = raw_con.execute(
                            f"SELECT {select_cols} FROM {quoted_table}"
                        ).fetchall()
                        old_rows = [dict(zip(table_columns, r)) for r in existing_all]
                        raw_con.execute(f"DELETE FROM {quoted_table}")
                        full_new = [
                            {c: row.get(c) for c in table_columns} for row in normalized_rows
                        ]
                        self._insert_rows_locked(raw_con, table, table_columns, full_new)
                        new_rows = full_new
                    elif normalized_mode == "insert":
                        full_new = [
                            {c: row.get(c) for c in table_columns} for row in normalized_rows
                        ]
                        self._insert_rows_locked(raw_con, table, table_columns, full_new)
                        new_rows = full_new
                    else:
                        # upsert / update / delete: deduplicate incoming batch by key (last-wins)
                        key_order: List[Tuple[Any, ...]] = []
                        incoming_by_key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
                        for row in normalized_rows:
                            key_tuple = self._row_identity_key(row, resolved_keys)
                            if key_tuple not in incoming_by_key:
                                key_order.append(key_tuple)
                            incoming_by_key[key_tuple] = row

                        existing_rows = self._fetch_existing_rows_by_keys_locked(
                            raw_con, table, table_columns, resolved_keys, key_order
                        )
                        existing_by_key: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
                        for erow in existing_rows:
                            kt = self._row_identity_key(erow, resolved_keys)
                            existing_by_key.setdefault(kt, []).append(erow)

                        keys_to_delete: List[Tuple[Any, ...]] = []
                        rows_to_insert: List[Dict[str, Any]] = []

                        for kt in key_order:
                            inc = incoming_by_key[kt]
                            matched_old = existing_by_key.get(kt, [])
                            if normalized_mode == "delete":
                                if matched_old:
                                    keys_to_delete.append(kt)
                                    old_rows.extend(matched_old)
                            elif normalized_mode == "update":
                                if not matched_old:
                                    continue
                                keys_to_delete.append(kt)
                                for erow in matched_old:
                                    merged = {**erow, **inc}
                                    old_rows.append(erow)
                                    rows_to_insert.append(merged)
                                    new_rows.append(merged)
                            else:  # upsert
                                if matched_old:
                                    keys_to_delete.append(kt)
                                    for erow in matched_old:
                                        merged = {**erow, **inc}
                                        old_rows.append(erow)
                                        rows_to_insert.append(merged)
                                        new_rows.append(merged)
                                else:
                                    full_row = {c: inc.get(c) for c in table_columns}
                                    rows_to_insert.append(full_row)
                                    new_rows.append(full_row)

                        if keys_to_delete:
                            self._delete_by_keys_locked(raw_con, table, resolved_keys, keys_to_delete)
                        if rows_to_insert:
                            self._insert_rows_locked(raw_con, table, table_columns, rows_to_insert)

                    # Bump authoritative revision inside the same transaction.
                    now = time.time()
                    post_schema_ver, _, _ = self._inspect_schema_locked(table, cur=raw_con)
                    row_rev = raw_con.execute(
                        f"SELECT data_revision, schema_version FROM {quote_ident(_REVISIONS_TABLE)} "
                        "WHERE table_name = ?",
                        [table],
                    ).fetchone()
                    if row_rev is None:
                        next_rev = max(1, self._last_known_revisions.get(table, 0) + 1)
                        schema_changed = prev_schema_ver != post_schema_ver
                        raw_con.execute(
                            f"INSERT INTO {quote_ident(_REVISIONS_TABLE)} VALUES (?, ?, ?, ?)",
                            [table, next_rev, post_schema_ver, now],
                        )
                    else:
                        next_rev = int(row_rev[0]) + 1
                        schema_changed = str(row_rev[1]) != post_schema_ver
                        raw_con.execute(
                            f"UPDATE {quote_ident(_REVISIONS_TABLE)} "
                            "SET data_revision = ?, schema_version = ?, committed_at = ? "
                            "WHERE table_name = ?",
                            [next_rev, post_schema_ver, now, table],
                        )

                    raw_con.execute("COMMIT")
                    in_txn = False
                except Exception as exc:
                    if in_txn:
                        try:
                            raw_con.execute("ROLLBACK")
                        except Exception:
                            pass
                    raise SourceMutationError(f"Atomic mutation failed for table '{table}': {exc}") from exc

            # Build exact Change events post-commit (before releasing table lock).
            if normalized_mode == "insert":
                for idx, nr in enumerate(new_rows):
                    changes.append(
                        Change(
                            table=table,
                            type="INSERT",
                            new_row=nr,
                            source=source,
                            event_id=f"{next_rev}:{idx}",
                            sequence=next_rev,
                            row_identity=str(
                                self._row_identity_key(nr, resolved_keys) if resolved_keys else idx
                            ),
                            row_images_complete=True,
                        )
                    )
            elif normalized_mode == "delete":
                for idx, orow in enumerate(old_rows):
                    changes.append(
                        Change(
                            table=table,
                            type="DELETE",
                            old_row=orow,
                            source=source,
                            event_id=f"{next_rev}:{idx}",
                            sequence=next_rev,
                            row_identity=str(self._row_identity_key(orow, resolved_keys)),
                            row_images_complete=True,
                        )
                    )
            elif normalized_mode in {"update", "upsert"}:
                old_map = (
                    {self._row_identity_key(r, resolved_keys): r for r in old_rows}
                    if resolved_keys
                    else {}
                )
                for idx, nr in enumerate(new_rows):
                    kt = self._row_identity_key(nr, resolved_keys) if resolved_keys else (idx,)
                    orow = old_map.get(kt)
                    changes.append(
                        Change(
                            table=table,
                            type="UPDATE" if orow is not None else "INSERT",
                            old_row=orow,
                            new_row=nr,
                            source=source,
                            event_id=f"{next_rev}:{idx}",
                            sequence=next_rev,
                            row_identity=str(kt),
                            row_images_complete=True,
                        )
                    )

            batch = CommittedChangeBatch(
                table=table,
                source_revision=next_rev,
                schema_version=post_schema_ver,
                committed_at=now,
                server_epoch_id=self.server_epoch_id,
                old_rows=tuple(old_rows),
                new_rows=tuple(new_rows),
                changes=tuple(changes),
                row_deltas_complete=True,
                schema_changed=schema_changed,
                mode=normalized_mode,
            )

            # Physical table mutated: invalidate any previously retained immutable Arrow ref.
            with self._publish_lock:
                self._immutable_arrow_sources.pop(table, None)
                self._last_known_revisions[table] = next_rev
                self._last_known_schemas[table] = post_schema_ver
                ring = self._ring_buffers.setdefault(
                    table, deque(maxlen=self.max_retained_batches)
                )
                ring.append(batch)
                for inbox in tuple(self._inboxes.get(table, {}).values()):
                    inbox.push_locked(batch)

            # Best-effort distributed observation publish (never overrides authoritative sourceRevision).
            self._publish_distributed_observation(batch)

            # Notify controller cache coordinator so standard non-streaming caches stay coherent.
            cache_coord = getattr(self._controller, "cache_coordinator", None)
            if cache_coord is not None:
                try:
                    cache_coord.invalidate(
                        event="stream_revision",
                        reason=f"rev:{next_rev}:{normalized_mode}",
                        table=table,
                        structural=schema_changed,
                    )
                except Exception:  # pragma: no cover
                    _LOGGER.debug("Cache invalidation failed for table=%s", table, exc_info=True)

            return batch

    def notify_schema_change(self, table: str) -> CommittedChangeBatch:
        """Record an out-of-band DDL/schema evolution and broadcast INVALIDATE to subscribers."""
        with self._execution_locks.lock_for(table):
            new_schema_ver, _, _ = self._inspect_schema_locked(table)
            raw_con = self._raw_duckdb()
            now = time.time()
            if raw_con is not None:
                self._ensure_revisions_table_locked(raw_con)
                row = raw_con.execute(
                    f"SELECT data_revision FROM {quote_ident(_REVISIONS_TABLE)} WHERE table_name = ?",
                    [table],
                ).fetchone()
                next_rev = (int(row[0]) + 1) if row is not None else max(1, self._last_known_revisions.get(table, 0) + 1)
                if row is None:
                    raw_con.execute(
                        f"INSERT INTO {quote_ident(_REVISIONS_TABLE)} VALUES (?, ?, ?, ?)",
                        [table, next_rev, new_schema_ver, now],
                    )
                else:
                    raw_con.execute(
                        f"UPDATE {quote_ident(_REVISIONS_TABLE)} "
                        "SET data_revision = ?, schema_version = ?, committed_at = ? "
                        "WHERE table_name = ?",
                        [next_rev, new_schema_ver, now, table],
                    )
            else:
                next_rev = self._last_known_revisions.get(table, 0) + 1

            batch = CommittedChangeBatch(
                table=table,
                source_revision=next_rev,
                schema_version=new_schema_ver,
                committed_at=now,
                server_epoch_id=self.server_epoch_id,
                row_deltas_complete=False,
                schema_changed=True,
                mode="schema_change",
            )
            with self._publish_lock:
                self._immutable_arrow_sources.pop(table, None)
                self._last_known_revisions[table] = next_rev
                self._last_known_schemas[table] = new_schema_ver
                ring = self._ring_buffers.setdefault(
                    table, deque(maxlen=self.max_retained_batches)
                )
                ring.append(batch)
                for inbox in tuple(self._inboxes.get(table, {}).values()):
                    inbox.push_locked(batch)

            catalog = getattr(getattr(getattr(self._controller, "planner", None), "builder", None), "catalog", None)
            if catalog is not None:
                catalog.invalidate(table)
            cache_coord = getattr(self._controller, "cache_coordinator", None)
            if cache_coord is not None:
                cache_coord.invalidate(
                    event="schema_change",
                    reason=f"schema:{new_schema_ver}",
                    table=table,
                    structural=True,
                )
            return batch

    def _publish_distributed_observation(self, batch: CommittedChangeBatch) -> None:
        if self._distributed_runtime is None:
            return
        payload = {
            "table": batch.table,
            "observedSourceRevision": batch.source_revision,
            "schemaVersion": batch.schema_version,
            "serverEpochId": batch.server_epoch_id,
            "committedAt": batch.committed_at,
            "schemaChanged": batch.schema_changed,
        }
        try:
            client = getattr(self._distributed_runtime, "client", None)
            settings = getattr(self._distributed_runtime, "settings", None)
            prefix = getattr(settings, "key_prefix", "pivot") if settings is not None else "pivot"
            if client is not None and hasattr(client, "xadd"):
                stream_key = f"{prefix}:stream:observations:{batch.table}"
                client.xadd(
                    stream_key,
                    {"payload": json.dumps(payload, separators=(",", ":"))},
                    maxlen=1000,
                    approximate=True,
                )
            with self._publish_lock:
                self._pending_redis_reconciliations.pop(batch.table, None)
        except Exception as exc:
            _LOGGER.warning(
                "Distributed observation publish failed table=%s rev=%s err=%s",
                batch.table,
                batch.source_revision,
                type(exc).__name__,
            )
            with self._publish_lock:
                self._pending_redis_reconciliations[batch.table] = batch.source_revision

    def reconcile_pending_observations(self) -> List[str]:
        """Retry failed distributed observation publishes without inventing revisions."""
        with self._publish_lock:
            pending = dict(self._pending_redis_reconciliations)
        recovered: List[str] = []
        for table, rev in pending.items():
            current_rev, schema_ver = self.read_coherent_revision(table)
            batch = CommittedChangeBatch(
                table=table,
                source_revision=max(rev, current_rev),
                schema_version=schema_ver,
                committed_at=time.time(),
                server_epoch_id=self.server_epoch_id,
                row_deltas_complete=False,
                mode="reconcile",
            )
            self._publish_distributed_observation(batch)
            with self._publish_lock:
                if table not in self._pending_redis_reconciliations:
                    recovered.append(table)
        return recovered
