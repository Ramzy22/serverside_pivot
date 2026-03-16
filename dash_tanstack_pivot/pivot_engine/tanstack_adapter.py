"""
tanstack_adapter.py - Direct TanStack Table/Query adapter for the scalable pivot engine
This bypasses the REST API and provides direct integration with TanStack components
"""
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import asyncio
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import PivotSpec, Measure
from pivot_engine.security import User, apply_rls_to_spec


class TanStackOperation(str, Enum):
    """TanStack operation types"""
    GET_DATA = "get_data"
    GET_ROWS = "get_rows"
    GET_COLUMNS = "get_columns"
    GET_PAGE_COUNT = "get_page_count"
    FILTER = "filter"
    SORT = "sort"
    GROUP = "group"


@dataclass
class TanStackRequest:
    """TanStack request structure"""
    operation: TanStackOperation
    table: str
    columns: List[Dict[str, Any]]
    filters: List[Dict[str, Any]]
    sorting: List[Dict[str, Any]]
    grouping: List[str]
    aggregations: List[Dict[str, Any]]
    pagination: Optional[Dict[str, Any]] = None
    global_filter: Optional[str] = None


@dataclass
class TanStackResponse:
    """TanStack response structure"""
    data: List[Dict[str, Any]]
    columns: List[Dict[str, Any]]
    pagination: Optional[Dict[str, Any]] = None
    total_rows: Optional[int] = None
    grouping: Optional[List[Dict[str, Any]]] = None


class TanStackPivotAdapter:
    """Direct TanStack adapter that bypasses REST API and connects to controller"""
    
    def __init__(self, controller: ScalablePivotController):
        self.controller = controller
        self.hierarchy_state = {}  # Store expansion state
    
    def convert_tanstack_request_to_pivot_spec(self, request: TanStackRequest) -> PivotSpec:
        """Convert TanStack request to PivotSpec format"""
        # Extract grouping columns as hierarchy
        hierarchy_cols = request.grouping or []
        
        # Extract measure columns
        measures = []
        value_cols = []
        
        for col in request.columns:
            if col.get('aggregationFn'):
                # This is an aggregation column
                measures.append(Measure(
                    field=col.get('aggregationField', col['id']),
                    agg=col.get('aggregationFn', 'sum'),
                    alias=col['id']
                ))
            elif col['id'] not in hierarchy_cols:
                # This is a value column
                value_cols.append(col['id'])
        
        # Convert TanStack filters to PivotSpec filters
        pivot_filters = []
        for tanstack_filter in request.filters:
            # TanStack filter format: {id: str, value: any, type?: str}
            field = tanstack_filter['id']
            value = tanstack_filter['value']
            operator = self._map_tanstack_operator(tanstack_filter.get('type', 'eq'))
            
            pivot_filters.append({
                'field': field,
                'op': operator,
                'value': value
            })
        
        # Convert TanStack sorting to PivotSpec sorting
        pivot_sort = []
        for sort_spec in request.sorting:
            pivot_sort.append({
                'field': sort_spec['id'],
                'order': 'asc' if sort_spec.get('desc', False) is False else 'desc'
            })
        
        # Handle pagination
        offset = 0
        limit = 1000  # Default
        
        if request.pagination:
            page_size = request.pagination.get('pageSize', 100)
            page = request.pagination.get('pageIndex', 0)
            offset = page * page_size
            limit = page_size
            
        # Parse column_cursor from global_filter
        column_cursor = None
        if request.global_filter and request.global_filter.startswith("column_cursor:"):
            column_cursor = request.global_filter.replace("column_cursor:", "", 1)
            
        from pivot_engine.types.pivot_spec import PivotConfig
        
        return PivotSpec(
            table=request.table,
            rows=hierarchy_cols,
            columns=value_cols,  # Map non-grouped dimensions to column pivots
            measures=measures,
            filters=pivot_filters,
            sort=pivot_sort,
            limit=limit,
            totals=True,  # Enable totals computation
            pivot_config=PivotConfig(enabled=True, column_cursor=column_cursor)
        )
    
    def _map_tanstack_operator(self, tanstack_op: str) -> str:
        """Map TanStack filter operators to pivot engine operators"""
        mapping = {
            'eq': '=',
            'ne': '!=',
            'lt': '<',
            'gt': '>',
            'lte': '<=',
            'gte': '>=',
            'contains': 'contains',
            'startsWith': 'starts_with',
            'endsWith': 'ends_with',
            'in': 'in',
            'notIn': 'not in'
        }
        return mapping.get(tanstack_op, '=')
    
    def convert_pivot_result_to_tanstack_format(self, pivot_result: Any, 
                                               tanstack_request: TanStackRequest) -> TanStackResponse:
        """Convert pivot engine result to TanStack format"""
        import pyarrow as pa
        
        # Convert from pivot format to TanStack row format
        rows = []
        
        if isinstance(pivot_result, pa.Table):
            # Optimized vectorised conversion using PyArrow
            rows = pivot_result.to_pylist()
        elif isinstance(pivot_result, dict) and 'rows' in pivot_result and 'columns' in pivot_result:
            # Dict format with columns and rows
            pivot_columns = pivot_result['columns']
            pivot_rows = pivot_result['rows']
            
            for pivot_row in pivot_rows:
                tanstack_row = {}
                for i, col_name in enumerate(pivot_columns):
                    tanstack_row[col_name] = pivot_row[i] if i < len(pivot_row) else None
                rows.append(tanstack_row)
        elif isinstance(pivot_result, list):
            # Already in row format
            rows = pivot_result
        
        # Calculate pagination info if needed
        pagination = None
        if tanstack_request.pagination:
            total_rows = len(rows)  # In real implementation, get actual total
            pagination = {
                'totalRows': total_rows,
                'pageSize': tanstack_request.pagination.get('pageSize', 100),
                'pageIndex': tanstack_request.pagination.get('pageIndex', 0),
                'pageCount': (total_rows + tanstack_request.pagination.get('pageSize', 100) - 1) // tanstack_request.pagination.get('pageSize', 100) if total_rows else 0
            }
        
        return TanStackResponse(
            data=rows,
            columns=tanstack_request.columns,
            pagination=pagination,
            total_rows=len(rows) if rows else 0
        )
    
    async def handle_request(self, request: TanStackRequest, user: Optional[User] = None) -> TanStackResponse:
        """Handle a TanStack request directly"""
        # Convert request to pivot spec
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)
        
        # Apply RLS if user is provided
        if user:
            pivot_spec = apply_rls_to_spec(pivot_spec, user)
        
        # Execute pivot operation asynchronously
        pivot_result = await self.controller.run_pivot_async(pivot_spec, return_format="dict")
        
        # Convert result to TanStack format
        tanstack_result = self.convert_pivot_result_to_tanstack_format(pivot_result, request)
        
        return tanstack_result
    
    async def handle_hierarchical_request(self, request: TanStackRequest, 
                                        expanded_paths: List[List[str]],
                                        user_preferences: Optional[Dict[str, Any]] = None) -> TanStackResponse:
        """Handle hierarchical TanStack request with expansion state"""
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)
        
        # Ensure the expansion state is properly communicated to the tree manager
        # We use the batch loading method which is more efficient for multiple levels
        # The controller/tree manager handles the expansion logic (filtering visible nodes)
        # effectively, so we don't need to re-filter in Python here.
        
        hierarchy_result = self.controller.run_hierarchical_pivot_batch_load(
             pivot_spec.to_dict(), expanded_paths, max_levels=10
        )
        
        # run_hierarchical_pivot_batch_load returns a dict of {path_key: [nodes]}
        # We need to flatten this into a list of rows for TanStack, respecting the tree structure order if possible.
        # However, TanStack often expects a flat list if using "manual" grouping or a tree structure.
        # If we assume TanStack Table's "expanded" state management, we often send a flat list of *visible* rows.
        
        # Reconstruct the flat list of visible rows from the batch result
        # This is faster than the previous approach of fetching all and filtering
        visible_rows = []
        
        # Helper to sort paths to ensure parents come before children
        # This is a simple topological sort based on path length and value
        sorted_paths = sorted(hierarchy_result.keys(), key=lambda k: (len(k.split('|')) if k else 0, k))
        
        for path_key in sorted_paths:
            nodes = hierarchy_result[path_key]
            for node in nodes:
                 visible_rows.append(node)

        # Convert to TanStack format
        # We skip _apply_expansion_state as we trusted the batch loader to only return relevant data
        tanstack_result = self.convert_pivot_result_to_tanstack_format(
            visible_rows, request
        )
        
        return tanstack_result
    
    # _apply_expansion_state removed as it is now handled by the controller/tree manager logic

    
    def get_schema_info(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for TanStack column configuration"""
        # Try to use backend's schema discovery first
        try:
            if hasattr(self.controller, 'backend') and hasattr(self.controller.backend, 'get_schema'):
                schema = self.controller.backend.get_schema(table_name)
                if schema:
                    columns_info = []
                    for col_name, col_type in schema.items():
                        columns_info.append({
                            'id': col_name,
                            'header': col_name.replace('_', ' ').title(),
                            'accessorKey': col_name,
                            'type': col_type,
                            'enableSorting': True,
                            'enableFiltering': True
                        })
                    
                    return {
                        'table': table_name,
                        'columns': columns_info,
                        'sample_data': [] # Fetching sample data could be separate if needed
                    }
        except Exception as e:
            print(f"Metadata schema retrieval failed: {e}")

        # Fallback: return mock schema based on the controller's data via sample query
        try:
            # Run a simple query to get column info
            spec = PivotSpec(
                table=table_name,
                rows=[],  # No grouping
                measures=[],
                filters=[]
            )
            sample_result = self.controller.run_pivot(spec, return_format="dict")
            
            if sample_result and sample_result.get('columns'):
                columns_info = []
                for col_name in sample_result['columns']:
                    # Determine column type (simplified)
                    col_type = "string"  # Default
                    # In a real implementation, this would analyze the data
                    columns_info.append({
                        'id': col_name,
                        'header': col_name.replace('_', ' ').title(),
                        'accessorKey': col_name,
                        'type': col_type,
                        'enableSorting': True,
                        'enableFiltering': True
                    })
                
                return {
                    'table': table_name,
                    'columns': columns_info,
                    'sample_data': sample_result.get('rows', [])[:5]  # First 5 rows as sample
                }
        except:
            pass
        
        # Return empty schema
        return {
            'table': table_name,
            'columns': [],
            'sample_data': []
        }
    
    async def get_grouped_data(self, request: TanStackRequest) -> TanStackResponse:
        """Handle grouped data request (for hierarchical tables)"""
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)
        
        # Use the controller to get grouped data
        pivot_result = await self.controller.run_pivot_async(pivot_spec, return_format="dict")
        
        # Format for TanStack grouping
        tanstack_result = self.convert_pivot_result_to_tanstack_format(pivot_result, request)
        
        # Add grouping information
        if request.grouping:
            tanstack_result.grouping = [{
                'id': group_col,
                'value': None  # Will be populated with actual grouped data
            } for group_col in request.grouping]
        
        return tanstack_result

    def get_invalidation_events(self, table_name: str, change_type: str) -> List[Dict[str, Any]]:
        """
        Generate invalidation events for TanStack Query.
        This allows the frontend to know which queries to refetch.
        """
        # In a real app, this would be more granular based on the change
        # For example, if we inserted a row with Region='North', we only invalidate queries filtering on 'North'
        
        events = [
            {
                "queryKey": ["pivot", table_name],
                "type": "invalidate",
                "reason": f"data_change_{change_type}"
            }
        ]
        return events


# Utility function for TanStack integration
def create_tanstack_adapter(backend_uri: str = ":memory:") -> TanStackPivotAdapter:
    """Create a TanStack adapter with a configured controller"""
    controller = ScalablePivotController(
        backend_uri=backend_uri,
        enable_streaming=True,
        enable_incremental_views=True,
        tile_size=100,
        cache_ttl=300
    )
    return TanStackPivotAdapter(controller)


# Example usage functions
async def example_usage():
    """Example of how to use the TanStack adapter"""
    adapter = create_tanstack_adapter()
    
    # Example TanStack request
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[
            {"id": "region", "header": "Region", "enableSorting": True},
            {"id": "product", "header": "Product", "enableSorting": True},
            {"id": "total_sales", "header": "Total Sales", "aggregationFn": "sum", "aggregationField": "sales"}
        ],
        filters=[],
        sorting=[{"id": "total_sales", "desc": True}],
        grouping=["region", "product"],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 100}
    )
    
    result = await adapter.handle_request(request)
    print(f"Received {len(result.data)} rows from TanStack adapter")
    
    return result


if __name__ == "__main__":
    asyncio.run(example_usage())