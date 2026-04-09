from __future__ import annotations

from threading import RLock
from typing import Dict, Optional

from .models import SessionEventRecord


class EventStore:
    def __init__(self) -> None:
        self._events: Dict[str, SessionEventRecord] = {}
        self._lock = RLock()

    def save(self, event: SessionEventRecord) -> None:
        with self._lock:
            self._events[event.event_id] = event

    def get(self, event_id: str) -> Optional[SessionEventRecord]:
        with self._lock:
            return self._events.get(str(event_id or ""))

    def mark_active(self, event_id: str, active: bool) -> Optional[SessionEventRecord]:
        with self._lock:
            event = self._events.get(str(event_id or ""))
            if event is None:
                return None
            event.active = bool(active)
            return event
