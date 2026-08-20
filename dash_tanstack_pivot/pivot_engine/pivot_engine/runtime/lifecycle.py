"""Ownership boundary for runtime request lifecycle and backend resilience."""

from __future__ import annotations

import asyncio
import inspect
import os
from typing import Any, Awaitable, Callable, Optional, Tuple

from .models import PivotRequestContext
from .request_coordinator import RuntimeRequestCoordinator
from .resilience import CircuitBreaker, PivotRequestTimeout
from .session_gate import SessionRequestGate


def _positive_env_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


class RuntimeRequestLifecycle:
    """Coordinate freshness, cancellation, timeout, and circuit breaking.

    The runtime service decides *which* capability operation to invoke. This
    object owns the mechanics that apply uniformly to every backend operation.
    """

    def __init__(
        self,
        *,
        session_gate: Optional[SessionRequestGate] = None,
        request_coordinator: Optional[RuntimeRequestCoordinator] = None,
        request_timeout_seconds: Optional[float] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        circuit_breaker_failures: int = 5,
        circuit_breaker_cooldown_seconds: float = 30.0,
    ) -> None:
        self.coordinator = request_coordinator or RuntimeRequestCoordinator(
            session_gate or SessionRequestGate()
        )
        try:
            explicit_timeout = (
                float(request_timeout_seconds)
                if request_timeout_seconds is not None
                else None
            )
        except (TypeError, ValueError):
            explicit_timeout = None
        self.request_timeout_seconds = (
            explicit_timeout
            if explicit_timeout and explicit_timeout > 0
            else _positive_env_float("PIVOT_REQUEST_TIMEOUT_SECONDS", 300.0)
        )
        self.circuit_breaker = circuit_breaker or CircuitBreaker(
            failure_threshold=circuit_breaker_failures,
            cooldown_seconds=circuit_breaker_cooldown_seconds,
        )

    @staticmethod
    def circuit_key(
        context: PivotRequestContext,
        trigger_kind: Optional[str],
    ) -> Tuple[str, str]:
        intent = trigger_kind or context.intent or "structural"
        return (str(context.table or "default"), str(intent))

    @staticmethod
    def lane(context: PivotRequestContext) -> Optional[str]:
        key = RuntimeRequestCoordinator.active_request_key(context)
        return key[2] if key is not None else None

    def register_request(self, context: PivotRequestContext) -> bool:
        return self.coordinator.register_request(context)

    def response_is_current(self, context: PivotRequestContext) -> bool:
        return self.coordinator.response_is_current(context)

    def replace_active_request_task(self, context: PivotRequestContext) -> bool:
        return self.coordinator.replace_active_request_task(context)

    def release_active_request_task(self, context: PivotRequestContext) -> None:
        self.coordinator.release_active_request_task(context)

    def consume_superseded_cancel(self) -> bool:
        return self.coordinator.consume_superseded_cancel()

    async def run_backend_operation(
        self,
        context: PivotRequestContext,
        trigger_kind: Optional[str],
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        circuit_key = self.circuit_key(context, trigger_kind)
        self.circuit_breaker.before_request(circuit_key)
        try:
            awaitable = operation()
            if not inspect.isawaitable(awaitable):
                self.circuit_breaker.record_success(circuit_key)
                return awaitable
            if self.request_timeout_seconds:
                result = await asyncio.wait_for(
                    awaitable,
                    timeout=self.request_timeout_seconds,
                )
            else:
                result = await awaitable
        except asyncio.TimeoutError as exc:
            self.circuit_breaker.record_failure(circuit_key)
            raise PivotRequestTimeout(
                "Pivot backend request exceeded "
                f"{self.request_timeout_seconds:g}s timeout."
            ) from exc
        except asyncio.CancelledError:
            raise
        except Exception:
            self.circuit_breaker.record_failure(circuit_key)
            raise
        else:
            self.circuit_breaker.record_success(circuit_key)
            return result
