"""Runtime request lifecycle coordination for server-side viewport work."""

from __future__ import annotations

import asyncio
import threading
from typing import Dict, Optional

from .models import PivotRequestContext
from .session_gate import SessionRequestGate


class RuntimeRequestCoordinator:
    """Single owner for runtime stale-gating and in-process cancellation.

    SessionRequestGate owns causal ordering across workers. This coordinator
    adds per-process task supersession so a newer viewport/chart request can
    cancel older backend work in the same session/client/lane.
    """

    def __init__(self, session_gate: Optional[SessionRequestGate] = None) -> None:
        self.session_gate = session_gate or SessionRequestGate()
        self._active_request_lock = threading.Lock()
        self._active_request_tasks: Dict[tuple[str, str, str], asyncio.Task] = {}
        self._superseded_tasks: set[asyncio.Task] = set()

    @staticmethod
    def active_request_key(context: PivotRequestContext) -> Optional[tuple[str, str, str]]:
        if context.intent == "chart":
            lane = "chart"
        elif context.intent in {"viewport", "structural"}:
            lane = "data"
        else:
            return None
        return (
            str(context.session_id or "anonymous"),
            str(context.client_instance or "default"),
            lane,
        )

    def register_request(self, context: PivotRequestContext) -> bool:
        return self.session_gate.register_request(
            session_id=context.session_id,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            abort_generation=context.abort_generation,
            intent=context.intent,
            client_instance=context.client_instance,
        )

    def response_is_current(self, context: PivotRequestContext) -> bool:
        return self.session_gate.response_is_current(
            session_id=context.session_id,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            abort_generation=context.abort_generation,
            intent=context.intent,
            client_instance=context.client_instance,
        )

    def replace_active_request_task(self, context: PivotRequestContext) -> bool:
        key = self.active_request_key(context)
        current_task = asyncio.current_task()
        if key is None or current_task is None:
            return False
        with self._active_request_lock:
            previous_task = self._active_request_tasks.get(key)
            if previous_task is not None and previous_task is not current_task and not previous_task.done():
                self._superseded_tasks.add(previous_task)
                previous_task.cancel()
            self._active_request_tasks[key] = current_task
        return True

    def release_active_request_task(self, context: PivotRequestContext) -> None:
        key = self.active_request_key(context)
        current_task = asyncio.current_task()
        if key is None or current_task is None:
            return
        with self._active_request_lock:
            if self._active_request_tasks.get(key) is current_task:
                self._active_request_tasks.pop(key, None)
            self._superseded_tasks.discard(current_task)

    def consume_superseded_cancel(self) -> bool:
        current_task = asyncio.current_task()
        if current_task is None:
            return False
        with self._active_request_lock:
            if current_task not in self._superseded_tasks:
                return False
            self._superseded_tasks.discard(current_task)
            return True
