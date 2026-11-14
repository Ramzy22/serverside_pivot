from __future__ import annotations

import logging
from collections import OrderedDict
from threading import RLock
from typing import Dict, Optional

from .models import SessionEventRecord


class EventStore:
    """Stores edit session events with bounded memory.

    Fixes C1 — EventStore memory leak: previously `_events` grew without bound.
    Now capped at `max_events`; oldest events are evicted when the limit is reached.
    """

    def __init__(self, max_events: int = 1000) -> None:
        # OrderedDict preserves insertion order so we can evict the oldest.
        self._events: OrderedDict[str, SessionEventRecord] = OrderedDict()
        self._max_events = max_events
        self._lock = RLock()

    def save(self, event: SessionEventRecord) -> None:
        with self._lock:
            self._events[event.event_id] = event
            self._events.move_to_end(event.event_id)
            # Evict oldest events when the store exceeds the limit.
            while len(self._events) > self._max_events:
                self._events.popitem(last=False)

    def get(self, event_id: str) -> Optional[SessionEventRecord]:
        with self._lock:
            record = self._events.get(str(event_id or ""))
            if record is not None:
                # Access promotes the event to most-recently-used.
                self._events.move_to_end(str(event_id or ""))
            return record

    def mark_active(self, event_id: str, active: bool) -> Optional[SessionEventRecord]:
        with self._lock:
            event = self._events.get(str(event_id or ""))
            if event is None:
                # Fix L9: log warning when attempting to mark a non-existent event
                logging.warning("EventStore.mark_active: event_id=%s not found; skipping", event_id)
                return None
            event.active = bool(active)
            return event

    def clear(self) -> None:
        """Remove all stored events."""
        with self._lock:
            self._events.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
