"""Pure, authorization-aware ordering of pivot column identities.

The layout identity is separate from the current cell key: labels and viewport
positions can change without changing the saved preference. A server can apply
the same resolver to a viewport, clipboard document, or export job.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import OrderedDict
import hashlib
import json
import math
import re
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from threading import RLock
import time
from typing import Any, Iterable, Mapping, Sequence


MAX_LAYOUT_MANIFEST_IDS = 20_000
MAX_LAYOUT_MANIFEST_BYTES = 512 * 1024
MAX_PINNED_LAYOUT_IDS = 64
_LAYOUT_ID = re.compile(r"^[pt]:[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{64}$")


def _canonical_json(value: Any) -> bytes:
    def encode(item: Any) -> Any:
        if isinstance(item, (datetime, date, datetime_time)):
            return {"$type": type(item).__name__, "$value": item.isoformat()}
        if isinstance(item, Decimal):
            return {"$type": "Decimal", "$value": str(item)}
        if isinstance(item, bytes):
            return {"$type": "bytes", "$value": item.hex()}
        if isinstance(item, float) and not math.isfinite(item):
            return {"$type": "float", "$value": str(item)}
        if isinstance(item, tuple):
            return [encode(child) for child in item]
        if isinstance(item, list):
            return [encode(child) for child in item]
        if isinstance(item, dict):
            return {key: encode(child) for key, child in item.items()}
        return item
    return json.dumps(
        encode(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


@dataclass(frozen=True)
class PivotColumnIdentity:
    """A value column identified by its source path and measure, not its label."""

    pivot_path: tuple[tuple[str, Any], ...]
    measure_key: str
    cell_id: str
    label: str = field(default="", compare=False)
    is_row_total: bool = False
    layout_measure_key: str | None = field(default=None, compare=False)

    @property
    def group_id(self) -> str:
        if self.is_row_total:
            return "total"
        digest = hashlib.sha256(_canonical_json(self.pivot_path)).hexdigest()
        return f"g:{digest}"

    @property
    def layout_id(self) -> str:
        digest = hashlib.sha256(_canonical_json({
            "path": self.pivot_path,
            "measure": self.layout_measure_key or self.measure_key,
            "rowTotal": self.is_row_total,
        })).hexdigest()
        return f"{'t' if self.is_row_total else 'p'}:{digest}"


def catalog_from_pivot_values(
    values: Iterable[dict[str, Any]], column_fields: Sequence[str],
    measure_keys: Sequence[str], *, row_totals: bool = False,
    layout_measure_keys: Mapping[str, str] | None = None,
) -> tuple[PivotColumnIdentity, ...]:
    """Build stable identities from the actual typed planner values."""
    columns: list[PivotColumnIdentity] = []
    seen_cell_paths: dict[str, bytes] = {}
    for entry in values:
        cell_key = str(entry["_col_key"])
        path = tuple((field, entry[field]) for field in column_fields)
        encoded_path = _canonical_json(path)
        previous = seen_cell_paths.setdefault(cell_key, encoded_path)
        if previous != encoded_path:
            raise ValueError("ambiguous planner pivot cell key")
        for measure in measure_keys:
            columns.append(PivotColumnIdentity(
                path, measure, f"{cell_key}_{measure}",
                layout_measure_key=(layout_measure_keys or {}).get(measure),
            ))
    if row_totals:
        for measure in measure_keys:
            columns.append(PivotColumnIdentity(
                (), measure, f"__RowTotal__{measure}", is_row_total=True,
                layout_measure_key=(layout_measure_keys or {}).get(measure),
            ))
    return tuple(columns)


@dataclass(frozen=True)
class ResolvedColumnLayout:
    columns: tuple[PivotColumnIdentity, ...]
    revision: str


class UnknownLayoutRevision(LookupError):
    """A worker needs the bounded layout manifest before serving this revision."""


class LayoutRevisionStore:
    """Bounded worker-local layout registry with explicit cross-worker recovery.

    A token is a cache reference, never an authorization capability. Callers
    resolve the trusted owner and structural query scope before each lookup.
    """

    def __init__(self, *, max_entries: int = 256, max_bytes: int = 16 * 1024 * 1024,
                 ttl_seconds: float = 300):
        if max_entries < 1 or max_bytes < 1 or ttl_seconds <= 0:
            raise ValueError("layout store limits must be positive")
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._ttl_seconds = ttl_seconds
        self._entries: OrderedDict[tuple[str, str, str], tuple[tuple[str, ...], int, float]] = OrderedDict()
        self._bytes = 0
        self._lock = RLock()

    def _evict_expired(self, now: float) -> None:
        for key, (_, size, expires_at) in list(self._entries.items()):
            if expires_at <= now:
                del self._entries[key]
                self._bytes -= size

    def put(self, owner_scope: str, structural_key: str, revision: str,
            saved_ids: Sequence[str]) -> None:
        manifest = _normalize_manifest(saved_ids)
        if not owner_scope or not revision:
            raise ValueError("layout store requires owner scope and revision")
        key = (owner_scope, structural_key, revision)
        size = len(_canonical_json([*key, manifest]))
        if size > self._max_bytes:
            raise ValueError("layout manifest exceeds store byte limit")
        now = time.monotonic()
        with self._lock:
            self._evict_expired(now)
            previous = self._entries.pop(key, None)
            if previous is not None:
                self._bytes -= previous[1]
            self._entries[key] = (manifest, size, now + self._ttl_seconds)
            self._bytes += size
            while len(self._entries) > self._max_entries or self._bytes > self._max_bytes:
                _, (_, evicted_size, _) = self._entries.popitem(last=False)
                self._bytes -= evicted_size

    def get(self, owner_scope: str, structural_key: str, revision: str) -> tuple[str, ...]:
        key = (owner_scope, structural_key, revision)
        now = time.monotonic()
        with self._lock:
            self._evict_expired(now)
            entry = self._entries.get(key)
            if entry is None:
                raise UnknownLayoutRevision("layout manifest required")
            self._entries.move_to_end(key)
            return entry[0]


def _normalize_manifest(saved_ids: Sequence[str]) -> tuple[str, ...]:
    if len(saved_ids) > MAX_LAYOUT_MANIFEST_IDS:
        raise ValueError("layout manifest count exceeds limit")
    if any(not isinstance(identity, str) or not identity for identity in saved_ids):
        raise ValueError("layout manifest contains an invalid identity")
    if len(_canonical_json(saved_ids)) > MAX_LAYOUT_MANIFEST_BYTES:
        raise ValueError("layout manifest bytes exceed limit")
    seen: set[str] = set()
    normalized: list[str] = []
    for identity in saved_ids:
        if identity not in seen:
            normalized.append(identity)
            seen.add(identity)
    return tuple(normalized)


def parse_layout_metadata(viewport: dict) -> tuple[str | None, tuple[str, ...] | None, tuple[str, ...]]:
    """Parse the bounded V1 layout fields without treating a token as authority."""
    raw_revision = viewport.get("layoutRevision")
    revision = None
    if raw_revision is not None:
        if not isinstance(raw_revision, str) or not _REVISION.fullmatch(raw_revision):
            raise ValueError("invalid layout revision")
        revision = raw_revision

    raw_manifest = viewport.get("layoutManifest")
    manifest = None
    if raw_manifest is not None:
        if not isinstance(raw_manifest, list):
            raise ValueError("invalid layout manifest")
        manifest = _normalize_manifest(raw_manifest)
        if any(not _LAYOUT_ID.fullmatch(identity) for identity in manifest):
            raise ValueError("invalid layout identity")

    raw_pinned = viewport.get("pinnedColumnIds", [])
    if not isinstance(raw_pinned, list) or len(raw_pinned) > MAX_PINNED_LAYOUT_IDS:
        raise ValueError("invalid pinned column count")
    pinned = _normalize_manifest(raw_pinned)
    if any(not _LAYOUT_ID.fullmatch(identity) for identity in pinned):
        raise ValueError("invalid pinned column identity")
    return revision, manifest, pinned


def resolve_column_layout(
    default_columns: Iterable[PivotColumnIdentity],
    saved_ids: Sequence[str],
    *,
    allowed_ids: set[str] | None = None,
    structural_key: str = "",
) -> ResolvedColumnLayout:
    """Reconcile a saved leaf order with currently authorized, discovered columns.

    First occurrence of a saved group's leaf sets the group rank. Leaves remain
    contiguous within that group; unseen leaves and groups append in default
    order. Absent saved identities retain their rank in ``saved_ids`` but do not
    occupy output slots. Totals always follow ordinary pivot groups.
    """
    manifest = _normalize_manifest(saved_ids)
    available: dict[str, PivotColumnIdentity] = {}
    default_group_order: list[str] = []
    default_leaves: dict[str, list[str]] = {}
    for column in default_columns:
        identity = column.layout_id
        if allowed_ids is not None and identity not in allowed_ids:
            continue
        if identity in available:
            raise ValueError("duplicate current pivot column identity")
        available[identity] = column
        group = column.group_id
        if group not in default_leaves:
            default_group_order.append(group)
            default_leaves[group] = []
        default_leaves[group].append(identity)

    ordered_groups: list[str] = []
    ordered_leaves: dict[str, list[str]] = {}
    for identity in manifest:
        column = available.get(identity)
        if column is None:
            continue
        group = column.group_id
        if group not in ordered_leaves:
            ordered_groups.append(group)
            ordered_leaves[group] = []
        ordered_leaves[group].append(identity)
    for group in default_group_order:
        if group not in ordered_leaves:
            ordered_groups.append(group)
            ordered_leaves[group] = []
        existing = set(ordered_leaves[group])
        ordered_leaves[group].extend(
            identity for identity in default_leaves[group] if identity not in existing
        )
    ordered_groups = [group for group in ordered_groups if group != "total"] + (
        ["total"] if "total" in ordered_leaves else []
    )
    columns = tuple(
        available[identity]
        for group in ordered_groups
        for identity in ordered_leaves[group]
    )
    revision = hashlib.sha256(_canonical_json({
        "contract": "pivot-layout-v1",
        "structural": structural_key,
        "saved": manifest,
        # Saved positions survive aggregation changes, but the corresponding
        # cell keys can change. Bind both to prevent sparse schemas retaining
        # keys from an earlier aggregation under an identical revision.
        "resolved": [(column.layout_id, column.cell_id) for column in columns],
    })).hexdigest()
    return ResolvedColumnLayout(columns, revision)
