"""
pivot_engine package - enhanced with scalable capabilities

Expose controllers for both basic and scalable pivot operations.
"""
from .controller import PivotController
from .scalable_pivot_controller import ScalablePivotController

# Import tanstack adapter to make it available
from .tanstack_adapter import create_tanstack_adapter, TanStackRequest, TanStackOperation, TanStackPivotAdapter
from .dash_integration import register_pivot_app
from .runtime import (
    DashPivotInstanceConfig,
    PivotRequestContext,
    PivotRuntimeService,
    PivotServiceResponse,
    PivotViewState,
    SessionRequestGate,
    register_dash_callbacks_for_instances,
    register_dash_drill_modal_callback,
    register_dash_pivot_transport_callback,
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
    "SessionRequestGate",
    "DashPivotInstanceConfig",
    "register_dash_pivot_transport_callback",
    "register_dash_drill_modal_callback",
    "register_dash_callbacks_for_instances",
]
