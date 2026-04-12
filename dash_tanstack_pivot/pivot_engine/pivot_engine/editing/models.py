from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


GRAND_TOTAL_SCOPE_ID = "__grand_total__"

OverlayCellEntry = Dict[str, Any]
OverlayIndex = Dict[str, Dict[str, OverlayCellEntry]]
OverlayIndexByGrouping = Dict[str, OverlayIndex]


@dataclass
class ScopeLock:
    scope_id: str
    measure_id: str
    lock_mode: str
    owner_event_id: str


@dataclass
class ResolvedScopeTarget:
    scope_id: str
    measure_id: str
    lock_mode: str
    row_id: Optional[str] = None
    col_id: Optional[str] = None
    is_aggregate: bool = False
    source: str = "update"


@dataclass
class SessionEventRecord:
    event_id: str
    session_key: str
    session_version: int
    source: str
    created_at: float
    normalized_transaction: Dict[str, Any]
    inverse_transaction: Optional[Dict[str, Any]] = None
    redo_transaction: Optional[Dict[str, Any]] = None
    original_updates: List[Dict[str, Any]] = field(default_factory=list)
    affected_cells: Dict[str, List[str]] = field(default_factory=lambda: {"direct": [], "propagated": []})
    impacted_scope_ids: List[str] = field(default_factory=list)
    grouping_fields: List[str] = field(default_factory=list)
    scope_value_changes: List[Dict[str, Any]] = field(default_factory=list)
    scope_locks: List[ScopeLock] = field(default_factory=list)
    active: bool = True


@dataclass
class EditSessionState:
    session_key: str
    table: str
    session_id: str
    client_instance: str
    base_snapshot_version: int = 0
    session_version: int = 0
    status: str = "active"
    _active_event_ids: Tuple[str, ...] = field(default_factory=tuple, repr=False)
    _undone_event_ids: Tuple[str, ...] = field(default_factory=tuple, repr=False)
    scope_locks_cache: List[ScopeLock] = field(default_factory=list)
    overlay_index_by_grouping: OverlayIndexByGrouping = field(default_factory=dict)

    @property
    def active_event_ids(self) -> Tuple[str, ...]:
        return tuple(self._active_event_ids)

    @property
    def undone_event_ids(self) -> Tuple[str, ...]:
        return tuple(self._undone_event_ids)


@dataclass
class PreparedEventAction:
    action: str
    session_key: str
    source: str
    normalized_transaction: Dict[str, Any]
    target_event_ids: List[str] = field(default_factory=list)
    replacement_updates: List[Dict[str, Any]] = field(default_factory=list)
    record_normalized_transaction: Optional[Dict[str, Any]] = None
    record_original_updates: List[Dict[str, Any]] = field(default_factory=list)
