"""
complete_rest_api.py - Complete REST API for the scalable pivot engine
Implements all missing endpoints to make the API complete
"""
import asyncio
import io
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.config import get_config
from pivot_engine.security import get_api_key, get_current_user, User, apply_rls_to_spec

from pivot_engine.tanstack_adapter import TanStackPivotAdapter, TanStackRequest, TanStackOperation

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

class ExportRequest(BaseModel):
    spec: PivotSpecRequest
    format: str = "csv"

class TanStackRequestModel(BaseModel):
    """Pydantic model for TanStack request"""
    operation: str
    table: str
    columns: List[Dict[str, Any]]
    filters: List[Dict[str, Any]] = []
    sorting: List[Dict[str, Any]] = []
    grouping: List[str] = []
    aggregations: List[Dict[str, Any]] = []
    pagination: Optional[Dict[str, Any]] = None
    global_filter: Optional[str] = None


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
    
    def __init__(self, controller: ScalablePivotController):
        self.controller = controller
        self.tanstack_adapter = TanStackPivotAdapter(controller)
        # Add security dependency globally to all routes
        self.app = FastAPI(
            title="Complete Scalable Pivot Engine API",
            dependencies=[Depends(get_api_key)]
        )
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup all API routes"""
        
        @self.app.get("/health")
        async def health_check():
            return {"status": "healthy", "service": "scalable-pivot-engine", "version": "1.0"}
        
        @self.app.post("/pivot")
        async def pivot_endpoint(request: PivotSpecRequest, user: User = Depends(get_current_user)):
            try:
                # Convert Pydantic model to dict for controller
                spec_dict = request.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                
                # Apply RLS
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                result = await self.controller.run_pivot_async(pivot_spec, return_format="dict")
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/pivot/export")
        async def export_endpoint(request: ExportRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                # Get the stream from controller
                stream = await self.controller.run_pivot_export(pivot_spec, format=request.format)
                
                media_type = "text/csv" if request.format.lower() == "csv" else "application/octet-stream"
                filename = f"export.{request.format.lower()}"
                
                return StreamingResponse(
                    stream, 
                    media_type=media_type,
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/pivot/tanstack")
        async def tanstack_endpoint(request: TanStackRequestModel, user: User = Depends(get_current_user)):
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
                    global_filter=request.global_filter
                )
                
                result = await self.tanstack_adapter.handle_request(ts_request, user=user)
                
                # Convert result object to dict/json compatible format
                return APIResponse(
                    status="success",
                    data=result.__dict__
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/hierarchical")
        async def hierarchical_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                # Update spec_dict with RLS changes
                spec_dict = pivot_spec.to_dict()
                
                # Use controller's hierarchical method
                result = self.controller.run_hierarchical_pivot(spec_dict)
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/virtual-scroll")
        async def virtual_scroll_endpoint(request: VirtualScrollRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                # Use controller's virtual scroll method
                result = self.controller.run_virtual_scroll_hierarchical(
                    pivot_spec, 
                    request.start_row, 
                    request.end_row, 
                    request.expanded_paths
                )
                
                return APIResponse(
                    status="success",
                    data=result,
                    metadata={
                        "start_row": request.start_row,
                        "end_row": request.end_row,
                        "range_size": request.end_row - request.start_row
                    }
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/progressive-load")
        async def progressive_load_endpoint(request: ProgressiveLoadRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                result = self.controller.run_progressive_hierarchical_load(
                    pivot_spec,
                    request.expanded_paths,
                    request.user_preferences
                )
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/materialized-hierarchy")
        async def materialized_hierarchy_endpoint(request: PivotSpecRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                # Await the async method
                result = await self.controller.run_materialized_hierarchy(pivot_spec)
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/pivot/jobs/{job_id}")
        async def job_status_endpoint(job_id: str):
            try:
                result = self.controller.get_materialization_status(job_id)
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/pruned-hierarchical")
        async def pruned_hierarchical_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                result = self.controller.run_pruned_hierarchical_pivot(
                    pivot_spec,
                    request.expanded_paths,
                    request.user_preferences or {}
                )
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/intelligent-prefetch")
        async def intelligent_prefetch_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                # Mock session for demo - in real app, this would come from auth
                user_session = {"user_id": user.id, "session_data": {}}
                
                result = await self.controller.run_intelligent_prefetch(
                    pivot_spec,
                    user_session,
                    request.expanded_paths
                )
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/streaming-aggregation")
        async def streaming_aggregation_endpoint(request: PivotSpecRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                result = await self.controller.run_streaming_aggregation(pivot_spec)
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/cdc/setup")
        async def cdc_setup_endpoint(request: CDCSetupRequest):
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
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/pivot/cdc/push-setup")
        async def cdc_push_setup_endpoint(request: CDCSetupRequest):
            """Initialize CDC in Push mode for a table"""
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
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/pivot/cdc/push/{table_name}")
        async def cdc_push_endpoint(table_name: str, change: Dict[str, Any]):
            """Push a change event to the CDC system"""
            try:
                # Expects dict with type, new_row, old_row
                await self.controller.push_change_event(table_name, change)
                return APIResponse(
                    status="success",
                    data={"message": "Change accepted"}
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/pivot/schema/{table_name}")
        async def get_schema_endpoint(table_name: str):
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
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/expansion-state")
        async def expansion_state_endpoint(spec_hash: str, path: List[str], expand: bool = True):
            try:
                result = self.controller.toggle_expansion(spec_hash, path)
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/pivot/batch-load")
        async def batch_load_endpoint(request: HierarchicalRequest, user: User = Depends(get_current_user)):
            try:
                spec_dict = request.spec.model_dump()
                pivot_spec = PivotSpec.from_dict(spec_dict)
                pivot_spec = apply_rls_to_spec(pivot_spec, user)
                
                # Convert back to dict
                spec_dict = pivot_spec.to_dict()
                
                result = self.controller.run_hierarchical_pivot_batch_load(
                    spec_dict,
                    request.expanded_paths,
                    max_levels=3
                )
                
                return APIResponse(
                    status="success",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
    
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
        tile_size=config.tile_size,
        cache_ttl=config.default_cache_ttl
    )
    
    return CompletePivotAPI(controller)


# WebSocket support for real-time updates
from fastapi import WebSocket
import json


class RealTimePivotAPI(CompletePivotAPI):
    """Extended API with WebSocket support for real-time updates"""
    
    def __init__(self, controller: ScalablePivotController):
        super().__init__(controller)
        self.active_connections: Dict[str, WebSocket] = {}
        self._setup_websocket_routes()
    
    def _setup_websocket_routes(self):
        """Setup WebSocket routes"""
        
        @self.app.websocket("/ws/pivot/{connection_id}")
        async def websocket_endpoint(websocket: WebSocket, connection_id: str):
            await websocket.accept()
            self.active_connections[connection_id] = websocket
            
            try:
                while True:
                    # This would handle client messages
                    data = await websocket.receive_text()
                    message = json.loads(data) if data else {}
                    
                    # Handle subscription messages
                    if message.get('type') == 'subscribe':
                        # Process subscription request
                        await self._handle_subscription(websocket, message)
                    
                    # For now, just echo back
                    await websocket.send_text(f"Echo: {data}")
            except Exception as e:
                # Connection closed or error
                if connection_id in self.active_connections:
                    del self.active_connections[connection_id]
    
    async def _handle_subscription(self, websocket: WebSocket, message: Dict[str, Any]):
        """Handle subscription requests from clients"""
        # This would set up data change subscriptions
        table_name = message.get('table_name', '')
        subscription_id = message.get('subscription_id', '')
        
        response = {
            'type': 'subscription_ack',
            'subscription_id': subscription_id,
            'table': table_name,
            'status': 'subscribed'
        }
        
        await websocket.send_text(json.dumps(response))
    
    async def broadcast_data_update(self, table_name: str, data: Any):
        """Broadcast data updates to subscribed clients"""
        message = {
            'type': 'data_update',
            'table': table_name,
            'data': data,
            'timestamp': asyncio.get_event_loop().time()
        }
        
        disconnected = []
        for conn_id, websocket in self.active_connections.items():
            try:
                await websocket.send_text(json.dumps(message))
            except:
                disconnected.append(conn_id)
        
        # Clean up disconnected connections
        for conn_id in disconnected:
            del self.active_connections[conn_id]


def create_realtime_api(backend_uri: Optional[str] = None):
    """Create real-time API with WebSocket support"""
    config = get_config()
    uri = backend_uri or config.backend_uri
    
    controller = ScalablePivotController(
        backend_uri=uri,
        enable_streaming=config.enable_streaming,
        enable_incremental_views=config.enable_incremental_views,
        tile_size=config.tile_size,
        cache_ttl=config.default_cache_ttl
    )
    
    return RealTimePivotAPI(controller)


# Example usage
async def example_usage():
    """Example of using the complete API"""
    api = create_realtime_api()
    
    # The API is now ready to serve requests
    print("Complete REST API ready with all endpoints:")
    print("- /health")
    print("- /pivot")
    print("- /pivot/export (New)")
    print("- /pivot/hierarchical") 
    print("- /pivot/virtual-scroll")
    print("- /pivot/progressive-load")
    print("- /pivot/materialized-hierarchy")
    print("- /pivot/pruned-hierarchical")
    print("- /pivot/intelligent-prefetch")
    print("- /pivot/streaming-aggregation")
    print("- /pivot/cdc/setup")
    print("- /pivot/schema/{table_name}")
    print("- /pivot/expansion-state")
    print("- /pivot/batch-load")
    print("- /ws/pivot/{connection_id} (WebSocket)")
    
    return api


if __name__ == "__main__":
    asyncio.run(example_usage())