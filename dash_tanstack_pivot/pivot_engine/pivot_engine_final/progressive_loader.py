"""
ProgressiveDataLoader - Load data in chunks for progressive rendering
"""
import asyncio
from typing import Dict, Any, Optional, Callable, List
import pyarrow as pa
import ibis
from ibis import BaseBackend as IbisBaseBackend
from ibis.expr.api import Table as IbisTable, Expr
from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.common.ibis_expression_builder import IbisExpressionBuilder


class ProgressiveDataLoader:
    def __init__(self, backend: IbisBaseBackend, cache, event_bus=None):
        self.backend = backend
        self.cache = cache
        self.event_bus = event_bus
        self.default_chunk_size = 1000
        self.min_chunk_size = 100
        self.builder = IbisExpressionBuilder(backend)
        
    async def load_progressive_chunks(self, spec: PivotSpec, chunk_callback: Optional[Callable] = None):
        """Load data in chunks for progressive rendering"""
        # Determine chunk boundaries based on data size and complexity
        total_estimated_rows = await self._estimate_total_rows(spec) # Await the async method
        chunk_size = min(self.default_chunk_size, max(self.min_chunk_size, total_estimated_rows // 10))  # Adaptive chunk size
        
        offset = 0
        chunk_number = 0
        current_cursor = spec.cursor # Start with spec cursor if any
        use_keyset = bool(spec.sort)
        
        while True:
            # Fetch chunk
            if use_keyset:
                # Use keyset pagination if sorting is enabled
                chunk_ibis_expr = self._build_chunk_ibis_expression(spec, offset=None, chunk_size=chunk_size, cursor=current_cursor)
            else:
                # Fallback to OFFSET
                chunk_ibis_expr = self._build_chunk_ibis_expression(spec, offset=offset, chunk_size=chunk_size, cursor=None)
            
            chunk_data = await chunk_ibis_expr.to_pyarrow() # Execute Ibis expression
            
            if chunk_data.num_rows == 0:
                break
                
            # Notify UI about chunk availability
            chunk_info = {
                'data': chunk_data,
                'offset': offset,
                'total_estimated': total_estimated_rows,
                'progress': min(1.0, (offset + chunk_size) / total_estimated_rows),
                'chunk_number': chunk_number,
                'is_last_chunk': chunk_data.num_rows < chunk_size
            }
            
            if chunk_callback:
                await chunk_callback(chunk_info)
            
            offset += chunk_size # Keep tracking approximate offset for progress
            chunk_number += 1
            
            if chunk_data.num_rows < chunk_size:
                # Last chunk
                break
                
            # Update cursor for next chunk
            if use_keyset:
                current_cursor = self._extract_cursor(chunk_data, spec.sort)
        
        return {'total_chunks': chunk_number, 'total_rows': offset}

    def _extract_cursor(self, chunk_data: pa.Table, sort_spec: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Extract the cursor (last row values) for the sort fields"""
        if not chunk_data or chunk_data.num_rows == 0:
            return None
        
        # Get the last row
        last_row_idx = chunk_data.num_rows - 1
        cursor = {}
        
        sort_list = sort_spec if isinstance(sort_spec, list) else [sort_spec]
        
        for s in sort_list:
            field = s.get('field')
            if field in chunk_data.column_names:
                # Extract value safely
                col = chunk_data.column(field)
                val = col[last_row_idx].as_py()
                cursor[field] = val
                
        return cursor
    
    async def load_hierarchical_progressive(self, spec: PivotSpec, expanded_paths: List[List[str]], level_callback: Optional[Callable] = None):
        """Load hierarchical data progressively by levels"""
        result = {'levels': []}
        
        # Load root level first (top-level aggregations)
        root_ibis_expr = await self._create_level_ibis_expression(spec, [], spec.rows[0] if spec.rows else '')
        if root_ibis_expr:
            root_data = await root_ibis_expr.to_pyarrow() # Execute Ibis expression
            
            level_info = {
                'level': 0,
                'data': root_data,
                'is_root': True,
                'total_rows': root_data.num_rows if root_data else 0
            }
            
            result['levels'].append(level_info)
            
            if level_callback:
                await level_callback(level_info)
        
        # Load expanded paths progressively
        for path in expanded_paths:
            level = len(path)
            if level < len(spec.rows):
                level_ibis_expr = await self._create_level_ibis_expression(spec, path, spec.rows[level])
                if level_ibis_expr:
                    level_data = await level_ibis_expr.to_pyarrow() # Execute Ibis expression
                    
                    level_info = {
                        'level': level,
                        'data': level_data,
                        'parent_path': path,
                        'is_expanded': True,
                        'total_rows': level_data.num_rows if level_data else 0
                    }
                    
                    result['levels'].append(level_info)
                    
                    if level_callback:
                        await level_callback(level_info)
        
        return result
    
    async def _estimate_total_rows(self, spec: PivotSpec) -> int:
        """
        Estimate total number of rows for the query.
        Tries to use approximate counts or metadata first, falling back to exact count.
        """
        # If filters are present, we likely need exact count as metadata is usually table-wide
        # Exception: Partition filters if we can detect them (future optimization)
        
        ibis_table = self.backend.table(spec.table)
        
        # 1. If no filters, try fast metadata count
        if not spec.filters:
            try:
                # Some backends support fast count or metadata lookup
                # Ibis doesn't strictly expose 'approx_count' universally yet
                # We can try to peek at backend type
                backend_name = getattr(self.backend, 'name', 'unknown')
                
                if backend_name == 'duckdb':
                    # DuckDB specific: estimated_size in pragma? or strictly select count(*) is optimized?
                    # DuckDB's count(*) on parquet/arrow is usually O(1) or very fast (metadata scan)
                    pass 
                elif backend_name == 'postgres':
                    # Postgres: SELECT reltuples FROM pg_class ...
                    # This would require raw SQL execution capability
                    pass
                
                # For now, stick to count() but acknowledge it's usually fast without filters
                pass
            except:
                pass

        # Apply filters
        filtered_table = ibis_table
        if spec.filters:
            filter_expr = self.builder.build_filter_expression(ibis_table, spec.filters)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)
        
        # 2. Execute count query
        # Future: Use TABLESAMPLE for approximation if count is slow
        try:
            row_count = await filtered_table.count().execute()
            return row_count
        except Exception as e:
            print(f"Error estimating row count: {e}")
            return 0
    
    def _build_chunk_ibis_expression(self, spec: PivotSpec, offset: Optional[int], chunk_size: int, cursor: Optional[Dict[str, Any]] = None) -> IbisTable:
        """Build Ibis expression for a specific chunk."""
        base_table = self.backend.table(spec.table)

        # Apply filters
        filtered_table = base_table
        if spec.filters:
            filter_expr = self.builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)

        # Apply Cursor Filter (Keyset Pagination) if provided
        # We construct a synthetic spec with the current cursor to use the builder
        if cursor and spec.sort:
            # Shallow copy spec to inject cursor without modifying original
            # Note: PivotSpec is a class, we just need an object with 'cursor' and 'sort'
            # Or we can modify the builder method signature.
            # Ideally we pass a spec-like object.
            import copy
            cursor_spec = copy.copy(spec)
            cursor_spec.cursor = cursor
            
            cursor_filter = self.builder.build_cursor_filter_expression(filtered_table, cursor_spec)
            if cursor_filter is not None:
                filtered_table = filtered_table.filter(cursor_filter)

        # Define aggregations in Ibis
        aggregations = []
        for m in spec.measures:
            aggregations.append(self.builder.build_measure_aggregation(filtered_table, m))

        # Apply grouping
        grouped_table = filtered_table
        if spec.rows:
            grouped_table = filtered_table.group_by(spec.rows)
            
        # Apply aggregation
        agg_expr = grouped_table.aggregate(aggregations)

        # Apply ordering
        if spec.sort:
            ibis_sorts = self.builder.build_sort_expressions(agg_expr, spec.sort)
            if ibis_sorts:
                agg_expr = agg_expr.order_by(ibis_sorts)
        else:
             # Default order for stable pagination
             order_cols = spec.rows or [agg_expr.columns[0]]
             agg_expr = agg_expr.order_by([ibis.asc(col) for col in order_cols])

        # Apply LIMIT and OFFSET
        # If cursor is used, offset should be 0 (or None)
        if offset is not None and offset > 0:
            agg_expr = agg_expr.limit(chunk_size, offset=offset)
        else:
            agg_expr = agg_expr.limit(chunk_size)
        
        return agg_expr
    
    async def _create_level_ibis_expression(self, base_spec: PivotSpec, parent_path: List[str], current_dimension: str) -> Optional[IbisTable]:
        """Create an Ibis expression for a specific level of the hierarchy"""
        if not current_dimension:
            return None
            
        base_table = self.backend.table(base_spec.table)

        # Build filters based on parent path
        all_filters_dicts = base_spec.filters or []
        for i, value in enumerate(parent_path):
            if i < len(base_spec.rows):
                all_filters_dicts.append({
                    "field": base_spec.rows[i],
                    "op": "=",
                    "value": value
                })
        
        filtered_table = base_table
        if all_filters_dicts:
            filter_expr = self.builder.build_filter_expression(base_table, all_filters_dicts)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)

        # Define aggregations in Ibis
        aggregations = []
        for measure in base_spec.measures:
            aggregations.append(self.builder.build_measure_aggregation(filtered_table, measure))

        # Build the grouped and aggregated expression
        agg_expr = filtered_table.group_by(current_dimension).aggregate(aggregations)

        # Apply ordering
        agg_expr = agg_expr.order_by(ibis.asc(current_dimension))

        # Limit per level (optional, depends on use case)
        agg_expr = agg_expr.limit(1000)
        
        return agg_expr
    
    # _build_ibis_filter_expression removed as it is replaced by builder.build_filter_expression
