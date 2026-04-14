"""Transport-agnostic runtime API for multi-instance pivot serving."""

from .dash_callbacks import (
    DashPivotInstanceConfig,
    register_dash_callbacks_for_instances,
    register_dash_drill_modal_callback,
    register_dash_pivot_transport_callback,
)
from .models import PivotRequestContext, PivotServiceResponse, PivotViewState, safe_int
from .resilience import CircuitBreaker, CircuitBreakerOpen, PivotRequestTimeout
from .service import PivotRuntimeService
from .session_gate import SessionRequestGate

__all__ = [
    "DashPivotInstanceConfig",
    "PivotRequestContext",
    "PivotServiceResponse",
    "PivotViewState",
    "PivotRuntimeService",
    "CircuitBreaker",
    "CircuitBreakerOpen",
    "PivotRequestTimeout",
    "SessionRequestGate",
    "register_dash_callbacks_for_instances",
    "register_dash_drill_modal_callback",
    "register_dash_pivot_transport_callback",
    "safe_int",
]
