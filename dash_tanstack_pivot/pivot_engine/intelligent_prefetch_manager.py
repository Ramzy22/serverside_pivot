"""
IntelligentPrefetchManager and related components for optimizing data fetching.
"""
from typing import Dict, Any, List, Optional
import asyncio
import ibis
from ibis.expr.api import Table as IbisTable
from pivot_engine.types.pivot_spec import PivotSpec

class UserPatternAnalyzer:
    def __init__(self, cache):
        self.cache = cache

    async def analyze_patterns(self, user_session: Dict[str, Any]) -> List[List[str]]:
        """
        Analyzes user behavior patterns to predict future data access.
        Simple heuristic: Returns paths that have been drilled into frequently (> 2 times).
        """
        history = user_session.get('history', [])
        if not history:
            return []
            
        # Count frequency of paths in history
        # History is expected to be a list of lists of strings (paths)
        path_counts = {}
        for path in history:
            # Convert list path to tuple for hashing
            path_tuple = tuple(path)
            path_counts[path_tuple] = path_counts.get(path_tuple, 0) + 1
            
        # Return paths with frequency > 2
        frequent_paths = [list(p) for p, count in path_counts.items() if count > 2]
        return frequent_paths

class IntelligentPrefetchManager:
    def __init__(self, session_tracker: Any, pattern_analyzer: UserPatternAnalyzer, backend: Any, cache: Any):
        self.session_tracker = session_tracker
        self.pattern_analyzer = pattern_analyzer
        self.backend = backend # Ibis connection
        self.cache = cache

    async def determine_prefetch_strategy(self, user_session: Dict[str, Any], spec: PivotSpec, expanded_paths: List[List[str]]) -> List[List[str]]:
        """
        Determines which data paths to prefetch based on user patterns and current pivot state.
        Strategically combines:
        1. Paths predicted by user history analysis.
        2. Immediate children of currently expanded paths (next logical step).
        """
        # 1. Get predicted paths from analyzer
        predicted_paths = await self.pattern_analyzer.analyze_patterns(user_session)
        
        # 2. Add immediate children of currently expanded paths
        candidates = expanded_paths + predicted_paths
        
        # Deduplicate candidates
        unique_candidates = []
        seen = set()
        for c in candidates:
            t = tuple(c)
            if t not in seen:
                seen.add(t)
                unique_candidates.append(c)
        
        # Group candidates by depth (level) to batch queries
        candidates_by_depth = {}
        for path in unique_candidates:
            depth = len(path)
            if depth < len(spec.rows):
                if depth not in candidates_by_depth:
                    candidates_by_depth[depth] = []
                candidates_by_depth[depth].append(path)
        
        paths_to_fetch = []
        
        # Execute batch queries for each depth
        for depth, paths in candidates_by_depth.items():
            try:
                # Target dimension for this depth
                target_dim = spec.rows[depth]
                parent_dims = spec.rows[:depth]
                
                # Fetch children for all paths at this depth in one go
                # returns list of (parent_path_tuple, child_value)
                results = await self._fetch_children_batch(spec.table, parent_dims, paths, target_dim)
                
                for parent_path_tuple, child_val in results:
                    new_path = list(parent_path_tuple) + [child_val]
                    paths_to_fetch.append(new_path)
                    
            except Exception as e:
                print(f"Error batch prefetching for depth {depth}: {e}")

        return paths_to_fetch

    async def _fetch_children_batch(self, table_name: str, parent_dims: List[str], parent_paths: List[List[str]], target_dim: str, limit_per_parent: int = 5) -> List[Any]:
        """
        Queries the database to find the top values for the next dimension for multiple parent paths.
        Uses OR filters to batch the request.
        """
        if not parent_paths:
            return []
            
        try:
            t = self.backend.table(table_name)
            
            # Construct OR filter: (dim1=v1 AND dim2=v2) OR (...)
            # For Ibis, we can build this expression
            
            # Optimization: If only 1 parent dim, use ISIN
            if len(parent_dims) == 1:
                dim = parent_dims[0]
                values = [p[0] for p in parent_paths]
                filter_expr = t[dim].isin(values)
            elif len(parent_dims) == 0:
                # Root level, no filter
                filter_expr = None
            else:
                # Multiple dimensions: build OR chain
                or_expr = None
                for path in parent_paths:
                    and_expr = None
                    for dim, val in zip(parent_dims, path):
                        clause = t[dim] == val
                        and_expr = clause if and_expr is None else (and_expr & clause)
                    
                    or_expr = and_expr if or_expr is None else (or_expr | and_expr)
                filter_expr = or_expr
            
            query = t
            if filter_expr is not None:
                query = query.filter(filter_expr)
            
            # Precise Top-N per group using Window Functions
            # This avoids the over-fetching/under-fetching of global limits
            try:
                # 1. Project necessary columns first to simplify
                selection = parent_dims + [target_dim]
                # We need distinct values first to avoid counting duplicates (children) 
                # effectively distinct children per parent
                distinct_query = query.select(selection).distinct()
                
                # 2. Apply Window Function to rank children within each parent group
                # Order by target_dim (alphabetical/numeric) for deterministic results
                # In future, we could order by a metric (e.g. Sales) if passed
                w = ibis.window(group_by=parent_dims, order_by=ibis.asc(target_dim))
                
                # ibis.row_number() is standard
                ranked = distinct_query.mutate(rn=ibis.row_number().over(w))
                
                # 3. Filter top-N
                filtered_ranked = ranked.filter(ranked.rn <= limit_per_parent)
                
                # 4. Final selection
                final_query = filtered_ranked.select(selection)
                
                # Execute
                if hasattr(self.backend, 'execute'):
                     result = final_query.execute()
                else:
                     result = final_query.execute()
                
                # Process result to list of (parent_tuple, child_val)
                output = []
                if hasattr(result, 'to_dict'):
                     records = result.to_dict('records')
                     for row in records:
                         # Reconstruct parent path
                         p_path = tuple(row[d] for d in parent_dims)
                         child = row[target_dim]
                         output.append((p_path, child))
                
                return output

            except Exception as e:
                print(f"Window function prefetch failed (fallback to global limit): {e}")
                # Fallback to simple query with global limit if window functions fail (e.g. backend support)
                selection = parent_dims + [target_dim]
                fallback_query = query.select(selection).distinct().limit(limit_per_parent * len(parent_paths) * 2)
                
                if hasattr(self.backend, 'execute'):
                     result = fallback_query.execute()
                else:
                     result = fallback_query.execute()
                     
                output = []
                if hasattr(result, 'to_dict'):
                     records = result.to_dict('records')
                     for row in records:
                         p_path = tuple(row[d] for d in parent_dims)
                         child = row[target_dim]
                         output.append((p_path, child))
                return output

        except Exception as e:
            print(f"Batch prefetch query failed: {e}")
            return []

    async def _fetch_top_children(self, table_name: str, parent_dims: List[str], parent_values: List[str], target_dim: str, limit: int = 5) -> List[Any]:
        """
        Legacy method kept for reference or single-path fallback.
        """
        # Using Ibis to construct the query
        try:
            t = self.backend.table(table_name)
            
            # Filter by parent path
            query = t
            for dim, val in zip(parent_dims, parent_values):
                query = query.filter(t[dim] == val)
            
            # Select distinct target dimension values
            # Optimization: In a real scenario, we might order by a measure (e.g., top sales)
            # For now, just distinct values
            query = query.select(target_dim).distinct().limit(limit)
            
            # Execute
            # Note: This is synchronous in standard Ibis, but wrapped in async loop in controller if needed.
            # Here we assume we can call it. If backend supports async, use it.
            if hasattr(self.backend, 'execute'):
                 # Standard Ibis
                 result = query.execute()
                 return result[target_dim].tolist()
            else:
                 # Fallback
                 return []
        except Exception as e:
            # print(f"Prefetch query failed: {e}")
            return []
