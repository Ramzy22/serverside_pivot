from __future__ import annotations

import copy
from threading import RLock
from typing import Dict, List, Optional

from .event_store import EventStore
from .models import EditSessionState, OverlayIndex, OverlayIndexByGrouping, SessionEventRecord, ScopeLock


class EditSessionManager:
    def __init__(self) -> None:
        self._sessions: Dict[str, EditSessionState] = {}
        self._lock = RLock()
        self.event_store = EventStore()

    @staticmethod
    def build_session_key(table: str, session_id: str, client_instance: str) -> str:
        return f"{table}::{session_id}::{client_instance}"

    def get_or_create_session(self, *, table: str, session_id: str, client_instance: str) -> EditSessionState:
        session_key = self.build_session_key(table, session_id, client_instance)
        with self._lock:
            session = self._sessions.get(session_key)
            if session is None:
                session = EditSessionState(
                    session_key=session_key,
                    table=table,
                    session_id=session_id,
                    client_instance=client_instance,
                )
                self._sessions[session_key] = session
            return session

    @staticmethod
    def _grouping_signature(grouping_fields: List[str]) -> str:
        signature = "|||".join(
            str(field).strip()
            for field in (grouping_fields or [])
            if str(field).strip()
        )
        return signature or "__flat__"

    def _active_events_locked(self, session: EditSessionState) -> List[SessionEventRecord]:
        events: List[SessionEventRecord] = []
        for event_id in list(session.active_event_ids):
            event = self.event_store.get(event_id)
            if event is None or not event.active:
                continue
            events.append(event)
        events.sort(key=lambda event: int(event.session_version or 0))
        return events

    def _overlay_event_sort_key(self, event_id: str) -> int:
        event = self.event_store.get(event_id)
        if event is None:
            return 0
        return int(event.session_version or 0)

    def _apply_event_to_derived_state_locked(self, session: EditSessionState, event: SessionEventRecord) -> None:
        if event is None or not event.active:
            return

        session.scope_locks_cache.extend(list(event.scope_locks or []))

        grouping_signature = self._grouping_signature(list(event.grouping_fields or []))
        grouping_overlay = session.overlay_index_by_grouping.setdefault(grouping_signature, {})
        for change in list(event.scope_value_changes or []):
            if not isinstance(change, dict):
                continue
            scope_id = str(change.get("scopeId") or "").strip()
            measure_id = str(change.get("measureId") or "").strip()
            if not scope_id or not measure_id:
                continue
            row_overlay = grouping_overlay.setdefault(scope_id, {})
            cell_entry = row_overlay.setdefault(
                measure_id,
                {
                    "rowId": scope_id,
                    "colId": measure_id,
                    "originalValue": copy.deepcopy(change.get("beforeValue")),
                    "directEventIds": [],
                    "propagatedEventIds": [],
                    "_originalValueByEventId": {},
                },
            )
            original_value_by_event_id = cell_entry.setdefault("_originalValueByEventId", {})
            original_value_by_event_id[event.event_id] = copy.deepcopy(change.get("beforeValue"))
            earliest_event_id = min(
                original_value_by_event_id,
                key=self._overlay_event_sort_key,
                default=event.event_id,
            )
            cell_entry["originalValue"] = copy.deepcopy(original_value_by_event_id.get(earliest_event_id))
            event_list_key = "directEventIds" if str(change.get("role") or "") == "direct" else "propagatedEventIds"
            if event.event_id not in cell_entry[event_list_key]:
                cell_entry[event_list_key].append(event.event_id)
                cell_entry[event_list_key].sort(key=self._overlay_event_sort_key)

    def _remove_event_from_derived_state_locked(self, session: EditSessionState, event_id: str) -> None:
        if not event_id:
            return
        session.scope_locks_cache = [
            lock
            for lock in (session.scope_locks_cache or [])
            if getattr(lock, "owner_event_id", None) != event_id
        ]

        empty_groupings: List[str] = []
        for grouping_signature, grouping_overlay in list((session.overlay_index_by_grouping or {}).items()):
            empty_scopes: List[str] = []
            for scope_id, row_overlay in list(grouping_overlay.items()):
                empty_measures: List[str] = []
                for measure_id, cell_entry in list(row_overlay.items()):
                    if not isinstance(cell_entry, dict):
                        continue
                    cell_entry["directEventIds"] = [
                        existing_id
                        for existing_id in list(cell_entry.get("directEventIds") or [])
                        if existing_id != event_id
                    ]
                    cell_entry["propagatedEventIds"] = [
                        existing_id
                        for existing_id in list(cell_entry.get("propagatedEventIds") or [])
                        if existing_id != event_id
                    ]
                    original_value_by_event_id = cell_entry.get("_originalValueByEventId")
                    if isinstance(original_value_by_event_id, dict):
                        original_value_by_event_id.pop(event_id, None)
                        remaining_event_ids = set(cell_entry["directEventIds"]) | set(cell_entry["propagatedEventIds"])
                        for stale_event_id in set(original_value_by_event_id) - remaining_event_ids:
                            original_value_by_event_id.pop(stale_event_id, None)
                        earliest_event_id = min(
                            original_value_by_event_id,
                            key=self._overlay_event_sort_key,
                            default=None,
                        )
                        if earliest_event_id is not None:
                            cell_entry["originalValue"] = copy.deepcopy(original_value_by_event_id.get(earliest_event_id))
                    if not cell_entry["directEventIds"] and not cell_entry["propagatedEventIds"]:
                        empty_measures.append(measure_id)
                for measure_id in empty_measures:
                    row_overlay.pop(measure_id, None)
                if not row_overlay:
                    empty_scopes.append(scope_id)
            for scope_id in empty_scopes:
                grouping_overlay.pop(scope_id, None)
            if not grouping_overlay:
                empty_groupings.append(grouping_signature)
        for grouping_signature in empty_groupings:
            session.overlay_index_by_grouping.pop(grouping_signature, None)

    def _rebuild_derived_state_locked(self, session: EditSessionState) -> None:
        active_events = self._active_events_locked(session)
        session.scope_locks_cache = [
            lock
            for event in active_events
            for lock in list(event.scope_locks or [])
        ]

        overlay_index_by_grouping: OverlayIndexByGrouping = {}
        for event in active_events:
            grouping_signature = self._grouping_signature(list(event.grouping_fields or []))
            if not grouping_signature:
                continue
            grouping_overlay = overlay_index_by_grouping.setdefault(grouping_signature, {})
            for change in list(event.scope_value_changes or []):
                if not isinstance(change, dict):
                    continue
                scope_id = str(change.get("scopeId") or "").strip()
                measure_id = str(change.get("measureId") or "").strip()
                if not scope_id or not measure_id:
                    continue
                row_overlay = grouping_overlay.setdefault(scope_id, {})
                cell_entry = row_overlay.setdefault(
                    measure_id,
                    {
                        "rowId": scope_id,
                        "colId": measure_id,
                        "originalValue": copy.deepcopy(change.get("beforeValue")),
                        "directEventIds": [],
                        "propagatedEventIds": [],
                        "_originalValueByEventId": {},
                    },
                )
                cell_entry.setdefault("_originalValueByEventId", {})[event.event_id] = copy.deepcopy(change.get("beforeValue"))
                event_list_key = "directEventIds" if str(change.get("role") or "") == "direct" else "propagatedEventIds"
                if event.event_id not in cell_entry[event_list_key]:
                    cell_entry[event_list_key].append(event.event_id)

        session.overlay_index_by_grouping = overlay_index_by_grouping

    def current_locks(self, session: EditSessionState) -> List[ScopeLock]:
        with self._lock:
            return list(session.scope_locks_cache or [])

    def active_events(self, session: EditSessionState) -> List[SessionEventRecord]:
        with self._lock:
            return list(self._active_events_locked(session))

    def active_event_ids(self, session: EditSessionState) -> tuple[str, ...]:
        with self._lock:
            return session.active_event_ids

    def latest_active_event_id(self, session: EditSessionState) -> Optional[str]:
        with self._lock:
            active_event_ids = session.active_event_ids
            return active_event_ids[-1] if active_event_ids else None

    def undone_event_ids(self, session: EditSessionState) -> tuple[str, ...]:
        with self._lock:
            return session.undone_event_ids

    def current_overlay_index(self, session: EditSessionState, *, grouping_fields: List[str]) -> OverlayIndex:
        grouping_signature = self._grouping_signature(grouping_fields)
        if not grouping_signature:
            return {}
        with self._lock:
            return session.overlay_index_by_grouping.get(grouping_signature) or {}

    def register_event(self, session: EditSessionState, event: SessionEventRecord) -> EditSessionState:
        with self._lock:
            session.session_version = max(session.session_version, int(event.session_version or 0))
            session._active_event_ids = (*session.active_event_ids, event.event_id)
            session._undone_event_ids = ()
            self.event_store.save(event)
            self._apply_event_to_derived_state_locked(session, event)
            return session

    def deactivate_events(self, session: EditSessionState, event_ids: List[str]) -> None:
        with self._lock:
            for event_id in event_ids:
                self.event_store.mark_active(event_id, False)
                if event_id in session.active_event_ids:
                    session._active_event_ids = tuple(existing for existing in session.active_event_ids if existing != event_id)
                self._remove_event_from_derived_state_locked(session, event_id)

    def push_undone_events(self, session: EditSessionState, event_ids: List[str]) -> None:
        with self._lock:
            for event_id in event_ids:
                self.event_store.mark_active(event_id, False)
                if event_id in session.active_event_ids:
                    session._active_event_ids = tuple(existing for existing in session.active_event_ids if existing != event_id)
                if event_id not in session.undone_event_ids:
                    session._undone_event_ids = (*session.undone_event_ids, event_id)
                self._remove_event_from_derived_state_locked(session, event_id)

    def peek_redo_event(self, session: EditSessionState, event_id: Optional[str] = None) -> Optional[SessionEventRecord]:
        with self._lock:
            candidate_id = event_id or (session.undone_event_ids[-1] if session.undone_event_ids else None)
            if not candidate_id or candidate_id not in session.undone_event_ids:
                return None
            return self.event_store.get(candidate_id)

    def activate_redo_events(self, session: EditSessionState, event_ids: List[str]) -> None:
        with self._lock:
            for event_id in event_ids:
                if not event_id or event_id not in session.undone_event_ids:
                    continue
                session._undone_event_ids = tuple(existing for existing in session.undone_event_ids if existing != event_id)
                event = self.event_store.mark_active(event_id, True)
                if event is not None and event_id not in session.active_event_ids:
                    session._active_event_ids = (*session.active_event_ids, event_id)
                    self._apply_event_to_derived_state_locked(session, event)

    def get_event(self, event_id: str) -> Optional[SessionEventRecord]:
        return self.event_store.get(event_id)

    def has_overlay_index(self, *, table: str, session_id: str, client_instance: str, grouping_fields: List[str]) -> bool:
        session_key = self.build_session_key(table, session_id, client_instance)
        grouping_signature = self._grouping_signature(grouping_fields)
        with self._lock:
            session = self._sessions.get(session_key)
            if session is None:
                return False
            return bool((session.overlay_index_by_grouping or {}).get(grouping_signature))
