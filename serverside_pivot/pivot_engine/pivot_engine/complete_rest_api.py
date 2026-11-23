"""
complete_rest_api.py - Complete REST API for the scalable pivot engine
Implements all missing endpoints to make the API complete
"""
import asyncio
import hashlib
import io
import json
import logging
import pyarrow as pa
import uuid
from dataclasses import dataclass, field
from typing import Dict, Any, List, Mapping, Optional, Set
from fastapi import FastAPI, HTTPException, Depends, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from pydantic import BaseModel, ConfigDict, Field
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.export_limits import ExportLimitError
from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.transport.core_bridge import CoreUnsupported
from pivot_engine.config import get_config
from pivot_engine.security import (
    authenticate_user_credentials,
    get_current_user,
    User,
    apply_rls_to_spec,
)
from pivot_engine.admin_api import admin_router, require_admin

from pivot_engine.tanstack_adapter import TanStackPivotAdapter, TanStackRequest, TanStackOperation
from pivot_engine.observability import setup_logging, setup_metrics
from pivot_engine.production_security import ProductionSecuritySettings, install_asgi_security
from pivot_engine.runtime import (
    PivotRuntimeService,
    RuntimeDatasetAuthorizationError,
    RuntimeDatasetAuthorizer,
    RuntimeMutationAuthorizer,
    register_runtime_http_transport,
)

# Initialize structured logging immediately
setup_logging()
_LOGGER = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class MeasureRequest(BaseModel):
    field: Optional[str]
    agg: str
    alias: str
    expression: Optional[str] = None
    percentile: Optional[float] = None
    separator: Optional[str] = None
    null_handling: Optional[str] = None
    filter_condition: Optional[str] = None
    ratio_numerator: Optional[str] = None
    ratio_denominator: Optional[str] = None
    ratio_format: str = "decimal"
    ratio_null_value: Optional[float] = None

class FilterRequest(BaseModel):
    field: str
    op: str = "="
    value: Any

class SortRequest(BaseModel):
    field: str
    order: str = "asc"
    nulls: Optional[str] = None

class PivotConfigRequest(BaseModel):
    enabled: bool = False
    top_n: Optional[int] = None
    order_by_measure: Optional[str] = None
    include_totals_column: bool = False
    null_column_label: str = "(null)"

class PivotSpecRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    table: str
    rows: List[str] = []
    columns: List[str] = []
    measures: List[MeasureRequest]
    filters: List[FilterRequest] = []
    sort: List[SortRequest] = []
    limit: Optional[int] = 100
    totals: bool = False
    cursor: Optional[Dict[str, Any]] = None
    pivot_config: Optional[PivotConfigRequest] = None
    measure_axis: Optional[Dict[str, Any]] = Field(default=None, alias="measureAxis")

class ExportRequest(BaseModel):
    spec: PivotSpecRequest
    format: str = "csv"

class TanStackRequestModel(BaseModel):
    """Pydantic model for TanStack request"""
    model_config = ConfigDict(populate_by_name=True)

    operation: str
    table: str
    columns: List[Dict[str, Any]]
    # TanStack filters are keyed by column id.  This matches the transport
    # contract consumed by TanStackPivotAdapter and the request mapper.
    filters: Dict[str, Any] = Field(default_factory=dict)
    sorting: List[Dict[str, Any]] = []
    grouping: List[str] = []
    aggregations: List[Dict[str, Any]] = []
    pagination: Optional[Dict[str, Any]] = None
    global_filter: Optional[str] = None
    measure_axis: Optional[Dict[str, Any]] = Field(default=None, alias="measureAxis")


class HierarchicalRequest(BaseModel):
    """Pydantic model for hierarchical pivot request"""
    spec: PivotSpecRequest
    expanded_paths: List[List[str]] = []
    user_preferences: Optional[Dict[str, Any]] = None


class VirtualScrollRequest(BaseModel):
    """Pydantic model for virtual scroll request"""
    spec: PivotSpecRequest
    start_row: int = 0
    end_row: int = 100
    expanded_paths: List[List[str]] = []


class ProgressiveLoadRequest(BaseModel):
    """Pydantic model for progressive loading request"""
    spec: PivotSpecRequest
    expanded_paths: List[List[str]] = []
    user_preferences: Optional[Dict[str, Any]] = None


class CDCSetupRequest(BaseModel):
    """Pydantic model for CDC setup request"""
    table_name: str


class APIResponse(BaseModel):
    """Base API response model"""
    status: str
    data: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class CompletePivotAPI:
    """Complete REST API implementation with all endpoints"""
    
    def __init__(
        self,
        controller: ScalablePivotController,
        *,
        mutation_authorizer: Optional[RuntimeMutationAuthorizer] = None,
        dataset_authorizer: Optional[RuntimeDatasetAuthorizer] = None,
    ):
        self.controller = controller
        self.tanstack_adapter = TanStackPivotAdapter(controller)
        self.dataset_authorizer = dataset_authorizer or RuntimeDatasetAuthorizer()
        # The host-neutral runtime route is the canonical UI API. The older
        # controller-oriented routes below remain available for direct engine
        # integrations, but React, Dash, and Streamlit share /v1/runtime.
        self.runtime_service = PivotRuntimeService(
            adapter_getter=lambda: self.tanstack_adapter,
        )
        # Add security dependency globally to all routes
        self.app = FastAPI(
            title="Complete Scalable Pivot Engine API",
            dependencies=[Depends(get_current_user)]
        )
        self.app.add_exception_handler(HTTPException, self._http_exception_handler)
        self.app.add_exception_handler(Exception, self._unexpected_exception_handler)
        self.production_security_settings = ProductionSecuritySettings.from_env()
        if self.production_security_settings.is_production:
            install_asgi_security(self.app, self.production_security_settings)
        setup_metrics(self.app)
        self.app.include_router(admin_router)
        self.runtime_http_gateway = register_runtime_http_transport(
            self.app,
            lambda: self.runtime_service,
            mutation_authorizer=mutation_authorizer,
            dataset_authorizer=self.dataset_authorizer,
        )
        self._setup_routes()

    @staticmethod
    def _correlation_id() -> str:
        return uuid.uuid4().hex

    def _internal_error_detail(self, request: Request, exc: Exception) -> dict[str, str]:
        correlation_id = self._correlation_id()
        endpoint = str(getattr(getattr(request, "url", None), "path", "unknown"))
        root_cause = exc.__cause__ if isinstance(exc.__cause__, Exception) else exc
        _LOGGER.error(
            "pivot_api_internal_error correlation_id=%s endpoint=%s exception_type=%s",
            correlation_id,
            endpoint,
            type(root_cause).__name__,
            exc_info=(type(root_cause), root_cause, root_cause.__traceback__),
        )
        return {"code": "internal_error", "correlationId": correlation_id}

    async def _http_exception_handler(self, request: Request, exc: HTTPException) -> JSONResponse:
        if exc.status_code < 500:
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
                headers=exc.headers,
            )
        return JSONResponse(
            status_code=500,
            content={"detail": self._internal_error_detail(request, exc)},
        )

    async def _unexpected_exception_handler(self, request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={"detail": self._internal_error_detail(request, exc)},
        )

    def _authorize_dataset(
        self, user: User, table: str, operation: str = "read", *, bind: bool = True
    ) -> Any:
        """Authorize and, for a data route, bind the grant to this request.

        ``bind=False`` only computes the grant (e.g. to store it with a
        websocket subscription); it must be used outside request handlers.
        """
        try:
            grant = self.dataset_authorizer.authorize(
                principal=user,
                table=table,
                operation=operation,
            )
        except RuntimeDatasetAuthorizationError as exc:
            raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from exc
        # Attach the grant to dataset access for the rest of this request
        # (REQ-SEC-001): every relation the route resolves is scoped, and
        # unscoped resolution of the protected dataset fails closed.
        if not bind:
            return grant
        from .core.authority.scope import UnscopedAccessError, bind_request_scope, connections_of

        try:
            bind_request_scope(grant, connections=connections_of(self.controller))
        except UnscopedAccessError as exc:
            raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from exc
        return grant

    @staticmethod
    def _apply_row_scope(pivot_spec: PivotSpec, user: User, row_scope: Any) -> PivotSpec:
        """Apply the route's already-authorized immutable row grant."""
        return apply_rls_to_spec(pivot_spec, user, row_scope=row_scope)

    @staticmethod
    def _cdc_change_matches_row_scope(row_scope: Any, change: Mapping[str, Any]) -> bool:
        """Require every supplied CDC row image to satisfy the writer's grant.

        CDC push is an externally reachable write boundary. A claims-scoped
        principal cannot assert an unscoped event, nor move a row across its
        own scope; doing either would let an authenticated admin mutate or
        invalidate another tenant's data.
        """
        field_values = getattr(row_scope, "field_values", None)
        if not isinstance(field_values, Mapping):
            return False
        if not field_values:
            return True
        images = [
            image
            for image in (change.get("old_row"), change.get("new_row"))
            if isinstance(image, Mapping)
        ]
        if not images:
            return False
        for image in images:
            for field_name, allowed_values in field_values.items():
                allowed = (
                    allowed_values
                    if isinstance(allowed_values, (list, tuple, set, frozenset))
                    else (allowed_values,)
                )
                if image.get(str(field_name)) not in allowed:
                    return False
        return True
    
    def _setup_routes(self):
        """Setup all API routes"""
        
        @self.app.get("/admin/dashboard", response_class=HTMLResponse)
        async def admin_dashboard(_user: User = Depends(require_admin)):
            """Serve the Admin UI Dashboard"""
            import os
            # Locate the static file relative to this file
            base_dir = os.path.dirname(os.path.abspath(__file__))
            static_path = os.path.join(base_dir, "static", "admin.html")
            with open(static_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())

        @self.app.get("/health")
        async def health_check():
            return {"status": "healthy", "service": "scalable-pivot-engine", "version": "1.0"}
        
        @self.app.post("/pivot")
        async def pivot_endpoint(request: PivotSpecRequest, request_obj: Request, user: User = Depends(get_current_user)):
            row_scope = self._authorize_dataset(user, request.table)
            try:
                # Check for Arrow IPC request
                accept_header = request_obj.headers.get("Accept", "")
                
                # Convert Pydantic model to dict for controller
                spec_dict = request.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                
                # Apply RLS
                pivot_spec = self._apply_row_scope(pivot_spec, user, row_scope)

                if "application/vnd.apache.arrow.stream" in accept_header or "application/octet-stream" in accept_header:
                     # Return Arrow IPC stream
                     table = await self.controller.run_pivot_arrow_async(pivot_spec)
                     
                     def iter_arrow_stream():
                         sink = io.BytesIO()
                         with pa.ipc.new_stream(sink, table.schema) as writer:
                             writer.write_table(table)
                         yield sink.getvalue()

                     return StreamingResponse(
                         iter_arrow_stream(),
                         media_type="application/vnd.apache.arrow.stream"
                     )
                
                # Default JSON response
                result = await self.controller.run_pivot_async(pivot_spec, return_format="dict")
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e

        @self.app.post("/pivot/export")
        async def export_endpoint(request: ExportRequest, user: User = Depends(get_current_user)):
            row_scope = self._authorize_dataset(user, request.spec.table, "export")
            export_format = str(request.format or "").strip().lower()
            if export_format not in {"csv", "parquet"}:
                raise HTTPException(
                    status_code=400,
                    detail="Unsupported export format. Supported formats are: csv, parquet.",
                )
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = self._apply_row_scope(pivot_spec, user, row_scope)
                export_executor = getattr(self.controller, "export_executor", None)
                export_limits = getattr(export_executor, "limits", None)
                if export_limits is not None:
                    export_limits.ensure_rows(int(pivot_spec.limit))
                
                # Async generators are already valid StreamingResponse bodies and
                # must not be awaited before Starlette iterates them.
                stream = self.controller.run_pivot_export(pivot_spec, format=export_format)
                
                media_type = "text/csv" if export_format == "csv" else "application/vnd.apache.parquet"
                filename = f"export.{export_format}"
                
                return StreamingResponse(
                    stream, 
                    media_type=media_type,
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
            except HTTPException:
                raise
            except ExportLimitError as exc:
                raise HTTPException(status_code=413, detail="Export exceeds the configured limit.") from exc
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e

        @self.app.post("/pivot/tanstack")
        async def tanstack_endpoint(request: TanStackRequestModel, user: User = Depends(get_current_user)):
            row_scope = self._authorize_dataset(user, request.table)
            try:
                # Convert Pydantic model to TanStackRequest dataclass
                ts_request = TanStackRequest(
                    operation=TanStackOperation(request.operation),
                    table=request.table,
                    columns=request.columns,
                    filters=request.filters,
                    sorting=request.sorting,
                    grouping=request.grouping,
                    aggregations=request.aggregations,
                    pagination=request.pagination,
                    global_filter=request.global_filter,
                    measure_axis=request.measure_axis
                )
                
                result = await self.tanstack_adapter.handle_request(
                    ts_request,
                    user=user,
                    row_scope=row_scope,
                )
                
                # Convert result object to dict/json compatible format
                return APIResponse(
                    status="success",
                    data=result.__dict__
                )
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e
        
        @self.app.post("/pivot/hierarchical")
        async def hierarchical_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            """The visible tree for the expanded paths (the core hierarchy view, REQ-TOT/REQ-PAGE)."""
            return await self._hierarchy_response(request.spec, user, request.expanded_paths, 0, None)

        @self.app.post("/pivot/virtual-scroll")
        async def virtual_scroll_endpoint(request: VirtualScrollRequest, user: User = Depends(get_current_user)):
            """An inclusive [start_row, end_row] window of the visible tree."""
            return await self._hierarchy_response(request.spec, user, request.expanded_paths, request.start_row, request.end_row)

        @self.app.post("/pivot/progressive-load")
        async def progressive_load_endpoint(request: ProgressiveLoadRequest, user: User = Depends(get_current_user)):
            """The visible tree (a loading strategy is no longer a separate engine: same answer as /pivot/hierarchical)."""
            return await self._hierarchy_response(request.spec, user, request.expanded_paths, 0, None)

        @self.app.post("/pivot/pruned-hierarchical")
        async def pruned_hierarchical_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            """The visible tree (same answer as /pivot/hierarchical)."""
            return await self._hierarchy_response(request.spec, user, request.expanded_paths, 0, None)

        @self.app.post("/pivot/intelligent-prefetch")
        async def intelligent_prefetch_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            """The visible tree (same answer as /pivot/hierarchical)."""
            return await self._hierarchy_response(request.spec, user, request.expanded_paths, 0, None)

        @self.app.post("/pivot/streaming-aggregation")
        async def streaming_aggregation_endpoint(request: PivotSpecRequest, user: User = Depends(get_current_user)):
            """The aggregate of the spec (same answer as /pivot)."""
            row_scope = self._authorize_dataset(user, request.table)
            try:
                pivot_spec = self._apply_row_scope(PivotSpec.from_dict(request.model_dump()), user, row_scope)
                result = await self.controller.run_pivot_async(pivot_spec, return_format="dict")
                return APIResponse(status="success", data=result)
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e

        @self.app.post("/pivot/cdc/setup")
        async def cdc_setup_endpoint(request: CDCSetupRequest, user: User = Depends(require_admin)):
            self._authorize_dataset(user, request.table_name, "cdc")
            try:
                # Create a mock change stream for the example
                async def mock_change_stream():
                    from pivot_engine.cdc.cdc_manager import Change
                    for i in range(3):  # Simulate a few changes
                        yield Change(
                            table=request.table_name,
                            type="INSERT",
                            new_row={"id": i, "data": f"record_{i}"}
                        )
                        await asyncio.sleep(0.01)
                
                cdc_manager = await self.controller.setup_cdc(request.table_name, mock_change_stream())
                
                # Bridge to WebSocket broadcast
                async def broadcast_change(change):
                    await self.broadcast_data_update(change.table, {
                        "type": "change",
                        "change": change.__dict__ if hasattr(change, '__dict__') else change
                    })
                
                cdc_manager.register_change_processor(broadcast_change)
                
                return APIResponse(
                    status="success",
                    data={"message": f"CDC setup complete for {request.table_name}"}
                )
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e

        @self.app.post("/pivot/cdc/push-setup")
        async def cdc_push_setup_endpoint(request: CDCSetupRequest, user: User = Depends(require_admin)):
            """Initialize CDC in Push mode for a table"""
            self._authorize_dataset(user, request.table_name, "cdc")
            try:
                cdc_manager = await self.controller.setup_push_cdc(request.table_name)
                
                # Bridge to WebSocket broadcast
                async def broadcast_change(change):
                    await self.broadcast_data_update(change.table, {
                        "type": "change",
                        "change": change.__dict__ if hasattr(change, '__dict__') else change
                    })
                
                cdc_manager.register_change_processor(broadcast_change)
                
                return APIResponse(
                    status="success",
                    data={"message": f"Push CDC setup complete for {request.table_name}"}
                )
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e

        @self.app.post("/pivot/cdc/push/{table_name}")
        async def cdc_push_endpoint(table_name: str, change: Dict[str, Any], user: User = Depends(require_admin)):
            """Push a change event to the CDC system"""
            row_scope = self._authorize_dataset(user, table_name, "cdc")
            if not self._cdc_change_matches_row_scope(row_scope, change):
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.")
            try:
                # Expects dict with type, new_row, old_row
                mode = await self.controller.push_change_event(table_name, change)
                return APIResponse(
                    status="success",
                    data={"message": "Change accepted", "mode": mode}
                )
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e
        
        @self.app.get("/pivot/schema/{table_name}")
        async def get_schema_endpoint(table_name: str, user: User = Depends(get_current_user)):
            self._authorize_dataset(user, table_name, "schema", bind=False)
            try:
                # This would typically query the database schema
                # For now, return mock schema based on a sample query
                spec = PivotSpec(
                    table=table_name,
                    rows=[],  # No grouping for schema query
                    measures=[],
                    filters=[]
                )
                
                try:
                    sample_result = await self.controller.run_pivot_async(spec, return_format="dict")
                    columns = sample_result.get('columns', []) if sample_result else []
                except:
                    columns = []  # Fallback to empty if table doesn't exist
                
                schema_info = {
                    "table": table_name,
                    "columns": [{"name": col, "type": "unknown"} for col in columns],
                    "total_rows": "unknown"  # Would query actual count in real implementation
                }
                
                return APIResponse(
                    status="success",
                    data=schema_info
                )
            except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
                raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
            except CoreUnsupported as e:  # a request the semantic core does not express
                raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
            except Exception as e:
                raise HTTPException(status_code=500, detail="Internal server error.") from e
        
        @self.app.post("/pivot/batch-load")
        async def batch_load_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            """The visible tree (same answer as /pivot/hierarchical)."""
            return await self._hierarchy_response(request.spec, user, request.expanded_paths, 0, None)

    async def _hierarchy_response(self, spec_model: Any, user: User, expanded_paths: Any,
                                  start_row: int, end_row: Optional[int]) -> "APIResponse":
        """A hierarchy endpoint's answer: the core hierarchy view of the scoped spec.

        ``rows`` is the inclusive [start_row, end_row] window of the visible
        tree (pre-order, grand total first when ``totals``), ``total_rows``
        its full length.
        """
        row_scope = self._authorize_dataset(user, spec_model.table)
        try:
            pivot_spec = self._apply_row_scope(PivotSpec.from_dict(spec_model.model_dump()), user, row_scope)
            view = await self.tanstack_adapter.hierarchy_view(
                pivot_spec, expanded_paths or [], start_row, end_row,
                include_grand_total=bool(pivot_spec.totals),
            )
            data = {"rows": view.get("rows", []), "total_rows": view.get("total_rows"),
                    "grand_total_row": view.get("grand_total_row")}
            metadata = None if end_row is None else {
                "start_row": start_row, "end_row": end_row, "range_size": end_row - start_row}
            return APIResponse(status="success", data=data, metadata=metadata)
        except PermissionError as e:  # scope / unproven-path denial (REQ-SEC-005)
            raise HTTPException(status_code=403, detail="Dataset access is not authorized.") from e
        except CoreUnsupported as e:  # a request the semantic core does not express
            raise HTTPException(status_code=422, detail={"code": e.code, "reasons": list(e.reasons)}) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal server error.") from e

    def get_app(self):
        """Get the FastAPI application instance"""
        return self.app


def create_complete_api(backend_uri: Optional[str] = None):
    """
    Create a complete API with configured controller.
    Uses centralized configuration.
    """
    config = get_config()
    
    # Allow overriding backend_uri if provided, otherwise use config
    uri = backend_uri or config.backend_uri
    
    controller = ScalablePivotController(
        backend_uri=uri,
        enable_streaming=config.enable_streaming,
        enable_incremental_views=config.enable_incremental_views,
        cache_ttl=config.default_cache_ttl,
        cdc_exact_state_store=config.cdc_exact_state_store,
    )
    
    return CompletePivotAPI(controller)


# WebSocket support for real-time updates


@dataclass
class _RealtimeConnection:
    """Per-connection server-owned state; messages never supply its principal."""

    websocket: Any
    principal: User
    subscriptions: Set[str] = field(default_factory=set)
    subscription_scopes: Dict[str, Any] = field(default_factory=dict)


class RealTimePivotAPI(CompletePivotAPI):
    """Authenticated, dataset-scoped real-time invalidation API."""

    def __init__(
        self,
        controller: ScalablePivotController,
        *,
        mutation_authorizer: Optional[RuntimeMutationAuthorizer] = None,
        dataset_authorizer: Optional[RuntimeDatasetAuthorizer] = None,
    ):
        super().__init__(
            controller,
            mutation_authorizer=mutation_authorizer,
            dataset_authorizer=dataset_authorizer,
        )
        self.active_connections: Dict[str, _RealtimeConnection] = {}
        self._setup_websocket_routes()

    def _register_connection(self, connection: _RealtimeConnection) -> str:
        """Allocate the registry key on the server, not from a client URL."""
        connection_id = uuid.uuid4().hex
        while connection_id in self.active_connections:  # pragma: no cover - UUID collision guard
            connection_id = uuid.uuid4().hex
        self.active_connections[connection_id] = connection
        return connection_id

    @staticmethod
    def _bearer_token(websocket: WebSocket) -> Optional[str]:
        authorization = str(websocket.headers.get("authorization") or "").strip()
        if authorization.lower().startswith("bearer "):
            token = authorization[7:].strip()
            return token or None
        return None

    @staticmethod
    def _row_matches_scope(row_scope: Any, row: Mapping[str, Any]) -> bool:
        """Apply the server-issued row grant to a CDC before/after image."""
        field_values = getattr(row_scope, "field_values", None)
        if not isinstance(field_values, Mapping):
            return False
        if not field_values:
            return True
        for field_name, allowed in field_values.items():
            values = allowed if isinstance(allowed, (list, tuple, set, frozenset)) else (allowed,)
            if row.get(str(field_name)) not in values:
                return False
        return True

    @classmethod
    def _scope_can_observe_change(cls, row_scope: Any, data: Any) -> bool:
        """Fail closed when a scoped CDC event has no row image to compare."""
        field_values = getattr(row_scope, "field_values", None)
        if not isinstance(field_values, Mapping):
            return False
        if not field_values:
            return True
        change = data.get("change") if isinstance(data, Mapping) else None
        if not isinstance(change, Mapping):
            return False
        rows = [
            row
            for row in (change.get("old_row"), change.get("new_row"))
            if isinstance(row, Mapping)
        ]
        return any(cls._row_matches_scope(row_scope, row) for row in rows)

    def _subscription_scope(self, principal: User, table_name: str) -> Optional[Any]:
        try:
            return self._authorize_dataset(principal, table_name, "subscribe", bind=False)
        except HTTPException:
            return None

    def _setup_websocket_routes(self):
        """Setup routes that authenticate before accepting a WebSocket."""

        @self.app.websocket("/ws/pivot/{connection_id}")
        async def websocket_endpoint(websocket: WebSocket, connection_id: str):
            try:
                principal = await authenticate_user_credentials(
                    api_key=websocket.headers.get("X-API-Key"),
                    token=self._bearer_token(websocket),
                )
            except HTTPException:
                # Handshake authentication is independent of FastAPI's HTTP
                # dependencies. Close before accept so a socket is never live
                # without a server-resolved principal.
                await websocket.close(code=4401)
                return

            await websocket.accept()
            connection = _RealtimeConnection(websocket=websocket, principal=principal)
            # ``connection_id`` is only a backwards-compatible client-side
            # correlation segment. It must never control server registry
            # ownership, otherwise a second client can replace an active one.
            registry_id = self._register_connection(connection)
            try:
                while True:
                    raw_message = await websocket.receive_text()
                    try:
                        message = json.loads(raw_message) if raw_message else {}
                    except json.JSONDecodeError:
                        await websocket.send_text(
                            json.dumps({"type": "subscription_error", "status": "invalid_message"})
                        )
                        continue
                    if isinstance(message, Mapping) and message.get("type") == "subscribe":
                        await self._handle_subscription(connection, dict(message))
                    else:
                        await websocket.send_text(
                            json.dumps({"type": "message_error", "status": "unsupported"})
                        )
            except WebSocketDisconnect:
                pass
            finally:
                if self.active_connections.get(registry_id) is connection:
                    del self.active_connections[registry_id]

    async def _handle_subscription(
        self,
        connection: _RealtimeConnection,
        message: Dict[str, Any],
    ) -> None:
        """Add a table subscription only after server-owned authorization."""
        table_name = str(message.get("table_name") or message.get("table") or "").strip()
        subscription_id = message.get("subscription_id", "")
        row_scope = self._subscription_scope(connection.principal, table_name)
        if not table_name or row_scope is None:
            await connection.websocket.send_text(
                json.dumps(
                    {
                        "type": "subscription_error",
                        "subscription_id": subscription_id,
                        "table": table_name,
                        "status": "denied",
                    }
                )
            )
            return

        connection.subscriptions.add(table_name)
        connection.subscription_scopes[table_name] = row_scope
        await connection.websocket.send_text(
            json.dumps(
                {
                    "type": "subscription_ack",
                    "subscription_id": subscription_id,
                    "table": table_name,
                    "status": "subscribed",
                }
            )
        )

    async def broadcast_data_update(self, table_name: str, data: Any):
        """Send tenant-scoped invalidations, never raw CDC rows, to subscribers."""
        normalized_table = str(table_name or "").strip()
        message = {
            "type": "data_update",
            "table": normalized_table,
            "data": {"type": "invalidated"},
            "timestamp": asyncio.get_running_loop().time(),
        }
        disconnected: List[str] = []
        for connection_id, connection in list(self.active_connections.items()):
            if normalized_table not in connection.subscriptions:
                continue
            row_scope = connection.subscription_scopes.get(normalized_table)
            if not self._scope_can_observe_change(row_scope, data):
                continue
            try:
                await connection.websocket.send_text(json.dumps(message))
            except Exception:
                disconnected.append(connection_id)

        for connection_id in disconnected:
            self.active_connections.pop(connection_id, None)


def create_realtime_api(backend_uri: Optional[str] = None):
    """Create real-time API with WebSocket support"""
    config = get_config()
    uri = backend_uri or config.backend_uri
    
    controller = ScalablePivotController(
        backend_uri=uri,
        enable_streaming=config.enable_streaming,
        enable_incremental_views=config.enable_incremental_views,
        cache_ttl=config.default_cache_ttl,
        cdc_exact_state_store=config.cdc_exact_state_store,
    )
    
    return RealTimePivotAPI(controller)


# Example usage
async def example_usage():
    """Example of using the complete API"""
    api = create_realtime_api()
    
    # The API is now ready to serve requests
    print("Complete REST API ready with endpoints:")
    for route in api.get_app().routes:
        print(f"- {getattr(route, 'path', route)}")
    
    return api


if __name__ == "__main__":
    asyncio.run(example_usage())
