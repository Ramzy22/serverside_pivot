"""
HierarchicalVirtualScrollManager - Optimized virtual scrolling for hierarchical data
"""
from typing import Dict, Any, List, Optional, Union
import pyarrow as pa

from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.planner.ibis_planner import IbisPlanner


class HierarchicalVirtualScrollManager:
    def __init__(self, planner: IbisPlanner, cache, materialized_hierarchy_manager: MaterializedHierarchyManager):
        self.planner = planner
        self.backend = planner.con  # Use the Ibis connection as the backend
        self.cache = cache
        self.materialized_hierarchy_manager = materialized_hierarchy_manager
        self.cache_ttl = 300  # 5 minutes default

    def get_visible_rows_hierarchical(self, spec: PivotSpec, start_row: int, end_row: int, expanded_paths: List[List[str]]):
        """Get hierarchical rows for virtual scrolling with expansion state"""
        self.spec = spec
        limit = end_row - start_row
        offset = start_row

        cache_key = self._get_cache_key(spec, offset, limit, expanded_paths)
        cached_result = self.cache.get(cache_key)
        if cached_result:
            return self._format_for_ui(cached_result, expanded_paths)

        # Build the Ibis expression
        ibis_expr = self._build_hierarchical_ibis_expr(spec, expanded_paths, offset, limit)
        if ibis_expr is None:
            return []

        # Execute the expression
        result_table = ibis_expr.to_pyarrow()
        self.cache.set(cache_key, result_table, ttl=self.cache_ttl)

        return self._format_for_ui(result_table, expanded_paths)

    def _get_cache_key(self, spec: PivotSpec, offset: int, limit: int, expanded_paths: List[List[str]]):
        """Generate cache key for a hierarchical query."""
        import hashlib
        import json
        cache_data = {
            'spec_hash': hash(str(spec.to_dict())),
            'offset': offset,
            'limit': limit,
            'expanded_paths_hash': hash(str(sorted(map(str, expanded_paths)))) if expanded_paths else 0
        }
        cache_key_str = json.dumps(cache_data, sort_keys=True)
        return f"hier_scroll_ibis:{hashlib.sha256(cache_key_str.encode()).hexdigest()[:16]}"

    def get_total_visible_row_count(self, spec: PivotSpec, expanded_paths: List[List[str]]) -> int:
        """
        Calculate total number of visible rows for the scrollbar.
        Sum of:
        1. Top level rows.
        2. Children of each expanded path.
        """
        total_count = 0
        
        # 1. Count Level 1
        level_1_table_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, 1)
        if level_1_table_name and self.backend is not None:
             try:
                 # Fast metadata count if possible, else count()
                 level_1_table = self.planner.con.table(level_1_table_name)
                 total_count += level_1_table.count().execute()
             except Exception:
                 pass

        # 2. Count Children of Expanded Paths
        # We can optimize this by grouping expanded paths by level and doing batch counts
        # Or even simpler: count query with OR filters if feasible.
        # However, counts are usually fast on rollups.
        
        valid_expanded_paths = [p for p in expanded_paths if p]
        
        # To avoid N queries, we can try to batch count by level
        # For each level L > 1, we want count of rows where parent path IN (...)
        
        if valid_expanded_paths and self.backend is not None:
             paths_by_level = {}
             for path in valid_expanded_paths:
                 level = len(path) + 1
                 if level <= len(spec.rows):
                     if level not in paths_by_level:
                         paths_by_level[level] = []
                     paths_by_level[level].append(path)
            
             for level, paths in paths_by_level.items():
                 rollup_table_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, level)
                 if not rollup_table_name:
                     continue
                     
                 try:
                     rollup_table = self.planner.con.table(rollup_table_name)
                     
                     # Construct filter: (dim1=v1 AND dim2=v2) OR ...
                     # Similar to prefetch manager logic
                     or_expr = None
                     parent_dims = spec.rows[:level-1]
                     
                     for path in paths:
                         and_expr = None
                         for dim, val in zip(parent_dims, path):
                             clause = rollup_table[dim] == val
                             and_expr = clause if and_expr is None else (and_expr & clause)
                        
                         or_expr = and_expr if or_expr is None else (or_expr | and_expr)
                     
                     if or_expr is not None:
                         cnt = rollup_table.filter(or_expr).count().execute()
                         total_count += cnt
                         
                 except Exception as e:
                     print(f"Error calculating visible count for level {level}: {e}")

        return total_count

    def _build_hierarchical_ibis_expr(self, spec: PivotSpec, expanded_paths: List[List[str]], offset: int, limit: int):
        """Build a query for a tile of hierarchical data using Ibis expressions."""
        import ibis
        from ibis.expr.api import Table as IbisTable

        union_expressions = []
        con = self.planner.con

        all_dims = spec.rows
        all_measures_aliases = [m.alias for m in spec.measures]

        # 1. Base query for top-level items (level 1)
        level_1_table_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, 1)
        if not level_1_table_name:
            return None  # Hierarchy must be materialized

        level_1_table = con.table(level_1_table_name)

        # Project all dimension and measure columns to ensure UNION compatibility
        projection_l1 = []
        level_1_dims = spec.rows[:1]
        for dim in all_dims:
            if dim in level_1_dims:
                projection_l1.append(level_1_table[dim])
            else:
                projection_l1.append(ibis.literal(None, type='str').name(dim))

        for measure_alias in all_measures_aliases:
            if measure_alias in level_1_table.columns:
                projection_l1.append(level_1_table[measure_alias])
            else:
                # This case should ideally not happen if rollup tables are correct
                projection_l1.append(ibis.literal(0).name(measure_alias))

        union_expressions.append(level_1_table.select(projection_l1))

        # 2. Queries for children of expanded paths
        valid_expanded_paths = [p for p in expanded_paths if p]
        for path in valid_expanded_paths:
            level = len(path) + 1
            if level > len(all_dims):
                continue

            rollup_table_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, level)
            if not rollup_table_name:
                continue

            rollup_table = con.table(rollup_table_name)

            # Apply filters for the path
            path_filters = []
            for i, val in enumerate(path):
                dim_name = all_dims[i]
                path_filters.append({"field": dim_name, "op": "=", "value": val})
            
            # Use builder to create filter expression
            if hasattr(self.planner, 'builder'):
                filter_expr = self.planner.builder.build_filter_expression(rollup_table, path_filters)
            else:
                # Fallback manual construction if builder is missing (should not happen in updated code)
                filter_expr = None
                for f in path_filters:
                    condition = (rollup_table[f['field']] == f['value'])
                    if filter_expr is None:
                        filter_expr = condition
                    else:
                        filter_expr &= condition

            if filter_expr is not None:
                rollup_table = rollup_table.filter(filter_expr)

            # Project columns
            projection_level = []
            level_dims = spec.rows[:level]
            for dim in all_dims:
                if dim in level_dims:
                    projection_level.append(rollup_table[dim])
                else:
                    projection_level.append(ibis.literal(None, type='str').name(dim))

            for measure_alias in all_measures_aliases:
                    if measure_alias in rollup_table.columns:
                        projection_level.append(rollup_table[measure_alias])
                    else:
                        projection_level.append(ibis.literal(0).name(measure_alias))

            union_expressions.append(rollup_table.select(projection_level))

        if not union_expressions:
            return None

        # 3. Combine into a single expression
        final_expr = ibis.union(*union_expressions)

        # 4. Apply final ordering and pagination
        order_by_cols = [ibis.asc(dim, nulls_first=True) for dim in all_dims]
        final_expr = final_expr.order_by(order_by_cols)
        final_expr = final_expr.limit(limit, offset=offset)

        return final_expr
    
    def _format_for_ui(self, data, expanded_paths: List[List[str]]):
        """Format data for UI consumption"""
        if data is None:
            return []
        
        rows = data.to_pylist()
        formatted_rows = []
        
        for row in rows:
            formatted_row = {k: v for k, v in row.items() if v is not None}
            
            # Determine the path for the current row
            current_path = []
            for dim in self.spec.rows:
                if dim in formatted_row:
                    current_path.append(formatted_row[dim])
                else:
                    break
            
            path_tuple = tuple(current_path)

            # Check if the path is in the list of expanded paths
            is_expanded = list(path_tuple) in expanded_paths

            # A node has children if it's not at the maximum depth
            has_children = len(path_tuple) < len(self.spec.rows)

            formatted_row['_is_expanded'] = is_expanded
            formatted_row['_has_children'] = has_children
            formatted_row['_depth'] = len(path_tuple)
            
            formatted_rows.append(formatted_row)
        
        return formatted_rows