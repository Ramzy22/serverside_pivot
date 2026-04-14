"""Runtime resilience primitives for backend pivot calls."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, Hashable, Optional


class CircuitBreakerOpen(RuntimeError):
    """Raised when a backend call is rejected by an open circuit."""


class PivotRequestTimeout(TimeoutError):
    """Raised when a backend request exceeds its configured timeout."""


@dataclass
class _CircuitState:
    failures: int = 0
    opened_at: Optional[float] = None


class CircuitBreaker:
    """Small per-process circuit breaker keyed by backend/table lane."""

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        cooldown_seconds: float = 30.0,
        clock=time.monotonic,
    ) -> None:
        self.failure_threshold = max(1, int(failure_threshold))
        self.cooldown_seconds = max(0.1, float(cooldown_seconds))
        self._clock = clock
        self._states: Dict[Hashable, _CircuitState] = {}
        self._lock = threading.RLock()

    def before_request(self, key: Hashable) -> None:
        now = self._clock()
        with self._lock:
            state = self._states.get(key)
            if state is None or state.opened_at is None:
                return
            if now - state.opened_at >= self.cooldown_seconds:
                state.opened_at = None
                state.failures = max(0, self.failure_threshold - 1)
                return
            raise CircuitBreakerOpen("Backend circuit is open for this pivot request lane.")

    def record_success(self, key: Hashable) -> None:
        with self._lock:
            self._states.pop(key, None)

    def record_failure(self, key: Hashable) -> None:
        now = self._clock()
        with self._lock:
            state = self._states.setdefault(key, _CircuitState())
            state.failures += 1
            if state.failures >= self.failure_threshold:
                state.opened_at = now

    def snapshot(self, key: Hashable) -> Dict[str, object]:
        with self._lock:
            state = self._states.get(key)
            if state is None:
                return {"failures": 0, "open": False}
            return {
                "failures": state.failures,
                "open": state.opened_at is not None,
                "openedAt": state.opened_at,
            }
