"""Frontend-agnostic request gate for causal ordering in multi-instance sessions."""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict

from .models import safe_int

try:
    import redis as _redis  # Optional: cross-worker state storage
except Exception:  # pragma: no cover - optional dependency
    _redis = None


class SessionRequestGate:
    """
    Per-session causal gate for viewport/structural ordering.
    Supports optional Redis backing for multi-worker deployments.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._ttl_seconds = safe_int(os.environ.get("PIVOT_SESSION_STATE_TTL_SECONDS"), 900)
        self._redis = None
        redis_url = os.environ.get("PIVOT_SESSION_REDIS_URL")
        if redis_url and _redis is not None:
            try:
                self._redis = _redis.from_url(redis_url, decode_responses=True)
                self._redis.ping()
            except Exception:
                self._redis = None

    def _now(self) -> int:
        return int(time.time())

    def _session_key(self, session_id: str, client_instance: str = "default") -> str:
        return f"pivot:session:{session_id}:{client_instance}"

    def _cleanup_local(self, now_ts: int):
        stale_ids = [
            sid
            for sid, state in self._sessions.items()
            if now_ts - state.get("updated_at", now_ts) > self._ttl_seconds
        ]
        for sid in stale_ids:
            self._sessions.pop(sid, None)

    def _read_state(self, session_id: str, client_instance: str = "default") -> Dict[str, Any]:
        if self._redis is not None:
            key = self._session_key(session_id, client_instance)
            raw = self._redis.hgetall(key) or {}
            return {
                "state_epoch": safe_int(raw.get("state_epoch"), 0),
                "window_seq": safe_int(raw.get("window_seq"), 0),
                "structural_seq": safe_int(raw.get("structural_seq"), 0),
                "chart_seq": safe_int(raw.get("chart_seq"), 0),
                "abort_generation": safe_int(raw.get("abort_generation"), 0),
                "updated_at": safe_int(raw.get("updated_at"), self._now()),
            }

        now_ts = self._now()
        with self._lock:
            self._cleanup_local(now_ts)
            session_key = f"{session_id}:{client_instance}"
            return self._sessions.get(
                session_key,
                {
                    "state_epoch": 0,
                    "window_seq": 0,
                    "structural_seq": 0,
                    "chart_seq": 0,
                    "abort_generation": 0,
                    "updated_at": now_ts,
                },
            ).copy()

    def _write_state(self, session_id: str, state: Dict[str, Any], client_instance: str = "default"):
        next_state = {
            "state_epoch": safe_int(state.get("state_epoch"), 0),
            "window_seq": safe_int(state.get("window_seq"), 0),
            "structural_seq": safe_int(state.get("structural_seq"), 0),
            "chart_seq": safe_int(state.get("chart_seq"), 0),
            "abort_generation": safe_int(state.get("abort_generation"), 0),
            "updated_at": safe_int(state.get("updated_at"), self._now()),
        }
        if self._redis is not None:
            key = self._session_key(session_id, client_instance)
            self._redis.hset(key, mapping=next_state)
            self._redis.expire(key, self._ttl_seconds)
            return

        with self._lock:
            session_key = f"{session_id}:{client_instance}"
            self._sessions[session_key] = next_state

    def register_request(
        self,
        session_id: str,
        state_epoch: int,
        window_seq: int,
        abort_generation: int,
        intent: str,
        client_instance: str = "default",
    ) -> bool:
        current = self._read_state(session_id, client_instance)
        next_state = current.copy()
        now_ts = self._now()

        if abort_generation < current["abort_generation"]:
            return False
        if state_epoch < current["state_epoch"]:
            return False

        if abort_generation > current["abort_generation"]:
            next_state["abort_generation"] = abort_generation

        if state_epoch > current["state_epoch"]:
            next_state["state_epoch"] = state_epoch
            next_state["window_seq"] = window_seq
            next_state["structural_seq"] = 0
            next_state["chart_seq"] = window_seq if intent == "chart" else 0
        elif state_epoch == current["state_epoch"]:
            if intent == "viewport":
                if window_seq <= current["window_seq"]:
                    return False
                if window_seq <= current["structural_seq"]:
                    return False
                next_state["window_seq"] = window_seq
            elif intent == "chart":
                if window_seq <= current.get("chart_seq", 0):
                    return False
                next_state["chart_seq"] = window_seq
            else:
                next_state["structural_seq"] = max(current["structural_seq"], window_seq)
                next_state["window_seq"] = max(current["window_seq"], window_seq)

        next_state["updated_at"] = now_ts
        self._write_state(session_id, next_state, client_instance)
        return True

    def response_is_current(
        self,
        session_id: str,
        state_epoch: int,
        window_seq: int,
        abort_generation: int,
        intent: str,
        client_instance: str = "default",
    ) -> bool:
        current = self._read_state(session_id, client_instance)
        if abort_generation < current["abort_generation"]:
            return False
        if state_epoch < current["state_epoch"]:
            return False
        if state_epoch > current["state_epoch"]:
            return False
        if intent == "viewport":
            structural_seq = current.get("structural_seq", 0)
            return window_seq == current["window_seq"] and window_seq > structural_seq
        if intent == "chart":
            return window_seq == current.get("chart_seq", 0)
        return True
