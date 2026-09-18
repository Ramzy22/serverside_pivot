"""Compatibility wrapper for the package layout used by source checkouts.

The real implementation lives in ``pivot_engine/pivot_engine``.  This wrapper
keeps the documented ``import pivot_engine`` API without executing package code
from a file string, so import analysis and installed/source behavior are normal
Python imports.
"""
import os as _os

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_INNER = _os.path.join(_HERE, "pivot_engine")

__path__ = [_INNER]

from .controller import PivotController
from .scalable_pivot_controller import ScalablePivotController
from .tanstack_adapter import (
    TanStackOperation,
    TanStackPivotAdapter,
    TanStackRequest,
    create_tanstack_adapter,
)
from .dash_integration import register_pivot_app
from .runtime import (
    DashPivotInstanceConfig,
    PivotRequestContext,
    PivotRuntimeService,
    PivotServiceResponse,
    PivotViewState,
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
    "RuntimeMutationAuthorizer",
    "RuntimeHttpGateway",
    "SessionRequestGate",
    "DashPivotInstanceConfig",
    "register_dash_pivot_transport_callback",
    "register_dash_drill_modal_callback",
    "register_dash_callbacks_for_instances",
    "register_runtime_http_transport",
]
