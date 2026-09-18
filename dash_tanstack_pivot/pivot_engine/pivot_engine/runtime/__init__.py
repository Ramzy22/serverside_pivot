"""Transport-agnostic runtime API for multi-instance pivot serving."""

from .dash_callbacks import (
    DashPivotInstanceConfig,
    register_dash_callbacks_for_instances,
    register_dash_drill_modal_callback,
    register_dash_pivot_transport_callback,
)
from .models import PivotRequestContext, PivotServiceResponse, PivotViewState, safe_int
from .contracts import (
    RUNTIME_PROTOCOL_NAME,
    RUNTIME_SCHEMA_VERSION,
    RuntimeContractError,
    validate_runtime_request_v1,
    validate_runtime_response_v1,
)
from .resilience import CircuitBreaker, CircuitBreakerOpen, PivotRequestTimeout
from .request_coordinator import RuntimeRequestCoordinator
from .lifecycle import RuntimeRequestLifecycle
from .service import PivotRuntimeService
from .session_gate import SessionRequestGate
from .http_transport import RuntimeHttpGateway, register_runtime_http_transport
from .mutation_authorization import (
    MutationGrant,
    MutationTableAccess,
    RuntimeMutationAuthorizationError,
    RuntimeMutationAuthorizer,
)
from .dataset_authorization import (
    DatasetTableAccess,
    RowScopeAuthorizationError,
    RowScopeGrant,
    RowSecurityPolicy,
    RuntimeDatasetAuthorizationError,
    RuntimeDatasetAuthorizer,
)
from ..production_security import (
    InMemoryFixedWindowRateLimiter,
    ProductionSecuritySettings,
    RedisFixedWindowRateLimiter,
    install_asgi_security,
    install_flask_security,
)

__all__ = [
    "DashPivotInstanceConfig",
    "PivotRequestContext",
    "PivotServiceResponse",
    "PivotViewState",
    "PivotRuntimeService",
    "RuntimeRequestCoordinator",
    "RuntimeRequestLifecycle",
    "CircuitBreaker",
    "CircuitBreakerOpen",
    "PivotRequestTimeout",
    "SessionRequestGate",
    "register_dash_callbacks_for_instances",
    "register_dash_drill_modal_callback",
    "register_dash_pivot_transport_callback",
    "safe_int",
    "RUNTIME_SCHEMA_VERSION",
    "RUNTIME_PROTOCOL_NAME",
    "RuntimeContractError",
    "RuntimeHttpGateway",
    "validate_runtime_request_v1",
    "validate_runtime_response_v1",
    "register_runtime_http_transport",
    "MutationGrant",
    "MutationTableAccess",
    "RuntimeMutationAuthorizationError",
    "RuntimeMutationAuthorizer",
    "DatasetTableAccess",
    "RowScopeAuthorizationError",
    "RowScopeGrant",
    "RowSecurityPolicy",
    "RuntimeDatasetAuthorizationError",
    "RuntimeDatasetAuthorizer",
    "InMemoryFixedWindowRateLimiter",
    "ProductionSecuritySettings",
    "RedisFixedWindowRateLimiter",
    "install_asgi_security",
    "install_flask_security",
]
