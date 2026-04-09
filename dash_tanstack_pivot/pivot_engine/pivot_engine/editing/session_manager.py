from __future__ import annotations

import copy
from threading import RLock
from typing import Dict, List, Optional

from .event_store import EventStore
from .models import EditSessionState, SessionEventRecord, ScopeLock


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

    def _rebuild_derived_state_locked(self, session: EditSessionState) -> None:
        active_events = self._active_events_locked(session)
        session.scope_locks_cache = [
            lock
            for event in active_events
            for lock in list(event.scope_locks or [])
        ]

        overlay_index_by_grouping: Dict[str, Dict[str, Dict[str, Dict[str, object]]]] = {}
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
                    },
                )
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

    def current_overlay_index(self, session: EditSessionState, *, grouping_fields: List[str]) -> Dict[str, Dict[str, Dict[str, object]]]:
        grouping_signature = self._grouping_signature(grouping_fields)
        if not grouping_signature:
            return {}
        with self._lock:
            return session.overlay_index_by_grouping.get(grouping_signature) or {}

    def register_event(self, session: EditSessionState, event: SessionEventRecord) -> EditSessionState:
        with self._lock:
            session.session_version = max(session.session_version, int(event.session_version or 0))
            session.active_event_ids.append(event.event_id)
            session.undone_event_ids = []
            self.event_store.save(event)
            self._rebuild_derived_state_locked(session)
            return session

    def deactivate_events(self, session: EditSessionState, event_ids: List[str]) -> None:
        with self._lock:
            for event_id in event_ids:
                self.event_store.mark_active(event_id, False)
                if event_id in session.active_event_ids:
                    session.active_event_ids = [existing for existing in session.active_event_ids if existing != event_id]
            self._rebuild_derived_state_locked(session)

    def push_undone_events(self, session: EditSessionState, event_ids: List[str]) -> None:
        with self._lock:
            for event_id in event_ids:
                self.event_store.mark_active(event_id, False)
                if event_id in session.active_event_ids:
                    session.active_event_ids = [existing for existing in session.active_event_ids if existing != event_id]
                if event_id not in session.undone_event_ids:
                    session.undone_event_ids.append(event_id)
            self._rebuild_derived_state_locked(session)

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
                session.undone_event_ids = [existing for existing in session.undone_event_ids if existing != event_id]
                event = self.event_store.mark_active(event_id, True)
                if event is not None and event_id not in session.active_event_ids:
                    session.active_event_ids.append(event_id)
            self._rebuild_derived_state_locked(session)

    def get_event(self, event_id: str) -> Optional[SessionEventRecord]:
        return self.event_store.get(event_id)
