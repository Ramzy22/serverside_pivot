"""
Repo-local pivot_engine package shim.

The developer environment also exposes an older editable/namespace
``pivot_engine`` install from outside this workspace. Without a real package at
the repo root, Python resolves some submodules (notably ``tanstack_adapter``)
from that external copy and others from this repo, which leads to runtime/test
drift. Force local imports to resolve against the backend implementation shipped
with this repository.
"""

from pathlib import Path as _Path

_LOCAL_IMPL = (
    _Path(__file__).resolve().parent.parent
    / "dash_tanstack_pivot"
    / "pivot_engine"
    / "pivot_engine"
)

__path__ = [str(_LOCAL_IMPL)]

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
