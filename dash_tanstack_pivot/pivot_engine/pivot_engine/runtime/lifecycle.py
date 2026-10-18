"""Ownership boundary for runtime request lifecycle and backend resilience."""

from __future__ import annotations

import asyncio
import inspect
import os
from typing import Any, Awaitable, Callable, Optional, Tuple

from ..export_limits import ExportLimitError
from ..resource_governor import (
    ResourceExhaustedError, ResourceGovernor, ResourceKind, ResourceLease,
    current_workload_account, get_worker_resource_governor,
)
from ..backends.execution import (
    CancellationUnavailableError,
    DatabaseExecutionError,
    QueryTimeoutError,
)
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
        resource_governor: Optional[ResourceGovernor] = None,
    ) -> None:
        self.resource_governor = resource_governor or get_worker_resource_governor()
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
        *,
        resource_kind: Optional[ResourceKind] = None,
        reservation_bytes: Optional[int] = None,
        export_lease: Optional[ResourceLease] = None,
    ) -> Any:
        # Legacy callers still participate in admission. Export uses its own
        # class; every other backend operation defaults to the query class.
        kind = ResourceKind(resource_kind) if resource_kind is not None else (
            ResourceKind.EXPORT if trigger_kind == "export" else ResourceKind.QUERY
        )
        amount = self.reservation_bytes(kind) if reservation_bytes is None else reservation_bytes
        if export_lease is not None and (
            kind != ResourceKind.EXPORT or export_lease.kind != kind
            or export_lease.account != context.accounting_account or export_lease._released
            or export_lease._governor is not self.resource_governor
        ):
            raise ValueError("An export lease must belong to this governor and account.")
        lease = export_lease or self.resource_governor.reserve(kind, amount, account=context.accounting_account)
        account_token = current_workload_account.set(context.accounting_account)
        try:
            return await self._run_admitted_operation(context, trigger_kind, operation)
        finally:
            # The awaited operation includes native cancellation acknowledgement.
            current_workload_account.reset(account_token)
            if export_lease is None:
                lease.release()

    def reservation_bytes(self, kind: ResourceKind) -> int:
        """Reserve a positive share of the configured class budget per slot."""
        limits = self.resource_governor.limits
        return max(1, limits.limit_for(kind) // (limits.slots_for(kind) or 1))

    async def _await_backend(self, awaitable: Awaitable[Any]) -> Any:
        task = asyncio.ensure_future(awaitable)
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=self.request_timeout_seconds)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            if not task.done():
                task.cancel()
            # Repeated client cancellation must not interrupt native cleanup or
            # release the resource lease while its worker still owns a cursor.
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not task.cancelled():
                cleanup_error = task.exception()
                if cleanup_error is not None:
                    raise cleanup_error
            raise

    async def _run_admitted_operation(
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
            result = await self._await_backend(awaitable)
        except (asyncio.TimeoutError, QueryTimeoutError) as exc:
            self.circuit_breaker.record_failure(circuit_key)
            raise PivotRequestTimeout() from exc
        except asyncio.CancelledError:
            raise
        except CancellationUnavailableError:
            # A hard deadline was rejected before execution; this is a stable
            # capability outcome, not evidence that the database is unhealthy.
            raise
        except DatabaseExecutionError:
            self.circuit_breaker.record_failure(circuit_key)
            raise
        except (ExportLimitError, ResourceExhaustedError):
            # A caller requested an export outside the configured resource
            # budget.  That is a deterministic validation result, not a
            # backend outage, so it must never trip the shared circuit.
            raise
        except Exception:
            self.circuit_breaker.record_failure(circuit_key)
            raise
        else:
            self.circuit_breaker.record_success(circuit_key)
            return result
