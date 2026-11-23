"""
pivot_engine package - enhanced with scalable capabilities

Exposes the pivot controller (``ScalablePivotController``; ``PivotController`` is
the same class under its historical name) and the edges built on it.
"""
from .scalable_pivot_controller import ScalablePivotController

PivotController = ScalablePivotController

# Import tanstack adapter to make it available
from .tanstack_adapter import create_tanstack_adapter, TanStackRequest, TanStackOperation, TanStackPivotAdapter
from .dash_integration import register_pivot_app
from .client_mode import (
    configure_pivot_execution,
    resolve_client_engine_kind,
    resolve_execution_mode,
)
from .runtime import (
    DashPivotInstanceConfig,
    PivotRequestContext,
    PivotRuntimeService,
    PivotServiceResponse,
    PivotViewState,
    RuntimeDatasetAuthorizer,
    RuntimeDatasetAuthorizationError,
    RuntimeMutationAuthorizer,
    RuntimeHttpGateway,
    SessionRequestGate,
    register_dash_callbacks_for_instances,
    register_dash_drill_modal_callback,
    register_dash_pivot_transport_callback,
    register_runtime_http_transport,
)

__all__ = [
    "register_pivot_app",
    "PivotController",
    "ScalablePivotController",
    "create_tanstack_adapter",
    "TanStackRequest",
    "TanStackOperation",
    "TanStackPivotAdapter",
    "PivotRequestContext",
    "PivotRuntimeService",
    "PivotServiceResponse",
    "PivotViewState",
    "RuntimeDatasetAuthorizer",
    "RuntimeDatasetAuthorizationError",
    "RuntimeMutationAuthorizer",
    "RuntimeHttpGateway",
    "SessionRequestGate",
    "DashPivotInstanceConfig",
    "register_dash_pivot_transport_callback",
    "register_dash_drill_modal_callback",
    "register_dash_callbacks_for_instances",
    "register_runtime_http_transport",
    "resolve_execution_mode",
    "resolve_client_engine_kind",
    "configure_pivot_execution",
]
