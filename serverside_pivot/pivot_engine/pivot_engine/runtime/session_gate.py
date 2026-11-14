"""Frontend-agnostic request gate for causal ordering in multi-instance sessions."""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, Optional

from .models import safe_int
from .distributed import (
    RedisRuntimeState,
    distributed_runtime_from_env,
)

try:
    import redis as _redis  # Optional: cross-worker state storage
except Exception:  # pragma: no cover - optional dependency
    _redis = None


class SessionRequestGate:
    """
    Per-session causal gate for viewport/structural ordering.
    Supports optional Redis backing for multi-worker deployments.
    """

    _REGISTER_LUA_SCRIPT = """
local key = KEYS[1]
local state_epoch = tonumber(ARGV[1])
local window_seq = tonumber(ARGV[2])
local abort_generation = tonumber(ARGV[3])
local intent = ARGV[4]
local now_ts = ARGV[5]
local ttl_seconds = tonumber(ARGV[6])

local cur = redis.call(
    'HMGET', key,
    'state_epoch', 'window_seq', 'structural_seq', 'chart_seq', 'abort_generation'
)
local cur_state_epoch = tonumber(cur[1]) or 0
local cur_window_seq = tonumber(cur[2]) or 0
local cur_structural_seq = tonumber(cur[3]) or 0
local cur_chart_seq = tonumber(cur[4]) or 0
local cur_abort_generation = tonumber(cur[5]) or 0

if abort_generation < cur_abort_generation or state_epoch < cur_state_epoch then
    return 0
end

local next_state_epoch = cur_state_epoch
local next_window_seq = cur_window_seq
local next_structural_seq = cur_structural_seq
local next_chart_seq = cur_chart_seq
local next_abort_generation = math.max(cur_abort_generation, abort_generation)

if state_epoch > cur_state_epoch then
    next_state_epoch = state_epoch
    next_window_seq = window_seq
    next_structural_seq = 0
    if intent == 'chart' then
        next_chart_seq = window_seq
    else
        next_chart_seq = 0
    end
elseif intent == 'viewport' then
    if window_seq <= cur_window_seq or window_seq <= cur_structural_seq then
        return 0
    end
    next_window_seq = window_seq
elseif intent == 'chart' then
    if window_seq <= cur_chart_seq then
        return 0
    end
    next_chart_seq = window_seq
else
    next_structural_seq = math.max(cur_structural_seq, window_seq)
    next_window_seq = math.max(cur_window_seq, window_seq)
end

redis.call(
    'HSET', key,
    'state_epoch', next_state_epoch,
    'window_seq', next_window_seq,
    'structural_seq', next_structural_seq,
    'chart_seq', next_chart_seq,
    'abort_generation', next_abort_generation,
    'updated_at', now_ts
)
redis.call('EXPIRE', key, ttl_seconds)
return 1
"""

    def __init__(self, runtime_state: Optional[RedisRuntimeState] = None):
        self._lock = threading.Lock()
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._ttl_seconds = safe_int(os.environ.get("PIVOT_SESSION_STATE_TTL_SECONDS"), 900)
        self._runtime_state = runtime_state or distributed_runtime_from_env()
        self._redis = None
        if self._runtime_state is not None:
            self._redis = self._runtime_state.client
        else:
            redis_url = os.environ.get("PIVOT_SESSION_REDIS_URL")
            if redis_url and _redis is not None:
                try:
                    self._redis = _redis.from_url(redis_url, decode_responses=True)
                    self._redis.ping()
                except Exception:
                    self._redis = None

    def _redis_call(self, operation):
        if self._runtime_state is not None:
            return self._runtime_state._call(operation)
        return operation()

    def _session_key(self, session_id: str, client_instance: str = "default") -> str:
        if self._runtime_state is not None:
            return self._runtime_state.key("session", session_id, client_instance)
        return f"pivot:session:{session_id}:{client_instance}"

    def _now(self) -> int:
        return int(time.time())

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
            raw = self._redis_call(lambda: self._redis.hgetall(key)) or {}
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

    @staticmethod
    def _compute_next_state(
        current: Dict[str, Any],
        *,
        state_epoch: int,
        window_seq: int,
        abort_generation: int,
        intent: str,
    ) -> Optional[Dict[str, Any]]:
        if abort_generation < current["abort_generation"]:
            return None
        if state_epoch < current["state_epoch"]:
            return None

        next_state = current.copy()
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
                    return None
                if window_seq <= current["structural_seq"]:
                    return None
                next_state["window_seq"] = window_seq
            elif intent == "chart":
                if window_seq <= current.get("chart_seq", 0):
                    return None
                next_state["chart_seq"] = window_seq
            else:
                next_state["structural_seq"] = max(current["structural_seq"], window_seq)
                next_state["window_seq"] = max(current["window_seq"], window_seq)

        return next_state

    def register_request(
        self,
        session_id: str,
        state_epoch: int,
        window_seq: int,
        abort_generation: int,
        intent: str,
        client_instance: str = "default",
    ) -> bool:
        if self._redis is not None:
            result = self._redis_call(
                lambda: self._redis.eval(
                    self._REGISTER_LUA_SCRIPT,
                    1,
                    self._session_key(session_id, client_instance),
                    int(state_epoch),
                    int(window_seq),
                    int(abort_generation),
                    str(intent),
                    str(self._now()),
                    str(self._ttl_seconds),
                )
            )
            return bool(int(result))

        now_ts = self._now()
        session_key = f"{session_id}:{client_instance}"
        with self._lock:
            self._cleanup_local(now_ts)
            current = self._sessions.get(
                session_key,
                {
                    "state_epoch": 0,
                    "window_seq": 0,
                    "structural_seq": 0,
                    "chart_seq": 0,
                    "abort_generation": 0,
                    "updated_at": now_ts,
                },
            )
            next_state = self._compute_next_state(
                current,
                state_epoch=state_epoch,
                window_seq=window_seq,
                abort_generation=abort_generation,
                intent=intent,
            )
            if next_state is None:
                return False
            next_state["updated_at"] = now_ts
            self._sessions[session_key] = next_state
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
