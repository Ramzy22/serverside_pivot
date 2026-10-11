"""Transport-agnostic runtime service for pivot requests."""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional
from ..resource_governor import ResourceGovernor, get_worker_resource_governor

from .compatibility import RuntimeCompatibilityMixin
from .models import PivotRequestContext, PivotServiceResponse, PivotViewState
from .resilience import CircuitBreaker
from .request_coordinator import RuntimeRequestCoordinator
from .session_gate import SessionRequestGate
from .distributed import RedisRuntimeState, distributed_runtime_from_env
from .detail_service import DetailRuntimeService
from .drill_service import RuntimeDrillService
from .export_service import RuntimeExportService
from .lifecycle import RuntimeRequestLifecycle
from .report_service import RuntimeReportService
from .router import RuntimeRequestRouter
from .tree_service import TreeRuntimeService


class PivotRuntimeService(RuntimeCompatibilityMixin):
    """Executes pivot operations using generic state/context contracts."""

    def __init__(
        self,
        adapter_getter: Callable[[], Any],
        session_gate: Optional[SessionRequestGate] = None,
        debug: bool = False,
        request_timeout_seconds: Optional[float] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        circuit_breaker_failures: int = 5,
        circuit_breaker_cooldown_seconds: float = 30.0,
        request_coordinator: Optional[RuntimeRequestCoordinator] = None,
        distributed_runtime: Optional[RedisRuntimeState] = None,
        resource_governor: Optional[ResourceGovernor] = None,
    ):
        # One service-scoped owner is shared by all sessions and transports in
        # this worker. Hosts may inject their worker governor explicitly.
        self.resource_governor = resource_governor or get_worker_resource_governor()
        try:
            from ..admin_api import register_resource_governor

            register_resource_governor(self.resource_governor)
        except (ImportError, AttributeError):
            # The core runtime remains usable in hosts that do not mount the
            # optional admin router.
            pass
        self.distributed_runtime = distributed_runtime or distributed_runtime_from_env()
        if request_coordinator is not None and session_gate is not None:
            if request_coordinator.session_gate is not session_gate:
                raise ValueError(
                    "request_coordinator and session_gate must use the same gate."
                )
        if request_coordinator is not None and session_gate is None:
            resolved_session_gate = request_coordinator.session_gate
        else:
            resolved_session_gate = session_gate or SessionRequestGate(
                runtime_state=self.distributed_runtime
            )
        if self.distributed_runtime is not None:
            gate_state = getattr(resolved_session_gate, "_runtime_state", None)
            if gate_state is not self.distributed_runtime:
                raise ValueError(
                    "Distributed runtime services require the coordinator to use "
                    "their shared Redis runtime state."
                )
        def checked_adapter_getter():
            adapter = adapter_getter()
            controller = getattr(adapter, "controller", None)
            bound_governor = getattr(controller, "resource_governor", None)
            if bound_governor is not None and bound_governor is not self.resource_governor:
                raise ValueError("Runtime controller must use its worker governor")
            return adapter

        self._adapter_getter = checked_adapter_getter
        self._lifecycle = RuntimeRequestLifecycle(
            resource_governor=self.resource_governor,
            session_gate=resolved_session_gate,
            request_coordinator=request_coordinator,
            request_timeout_seconds=request_timeout_seconds,
            circuit_breaker=circuit_breaker,
            circuit_breaker_failures=circuit_breaker_failures,
            circuit_breaker_cooldown_seconds=circuit_breaker_cooldown_seconds,
        )
        # Compatibility aliases for diagnostics and integrations. Lifecycle
        # mutation is owned by RuntimeRequestLifecycle.
        self._request_coordinator = self._lifecycle.coordinator
        self._request_timeout_seconds = self._lifecycle.request_timeout_seconds
        self._circuit_breaker = self._lifecycle.circuit_breaker
        self._debug = debug
        self._tree_service = TreeRuntimeService(debug=debug)
        self._detail_service = DetailRuntimeService(self._tree_service, debug=debug)
        self._report_service = RuntimeReportService()
        self._export_service = RuntimeExportService()
        self._drill_service = RuntimeDrillService()
        self._router = RuntimeRequestRouter(
            adapter_getter=self._adapter_getter,
            lifecycle=self._lifecycle,
            tree_service=self._tree_service,
            detail_service=self._detail_service,
            report_service=self._report_service,
            export_service=self._export_service,
            drill_service=self._drill_service,
            distributed_runtime=self.distributed_runtime,
            debug=debug,
        )

    async def process_async(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        """Process one pivot request and return a transport-neutral response."""
        return await self._router.process_async(state, context)

    def process(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        """Sync wrapper for sync transports."""
        return asyncio.run(self.process_async(state, context))
