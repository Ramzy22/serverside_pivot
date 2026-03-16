"""
HierarchyPruningManager - Prune less-relevant branches in large hierarchies
"""
import asyncio
from typing import Dict, Any, List, Optional, Union
import pyarrow as pa
import ibis
from ibis import BaseBackend as IbisBaseBackend
from ibis.expr.api import Table as IbisTable
from pivot_engine.types.pivot_spec import PivotSpec


class HierarchyPruningManager:
    def __init__(self, backend: IbisBaseBackend, config: Optional[Dict[str, Any]] = None):
        self.backend = backend
        self.config = config or {}
        
    def apply_pruning_strategy(self, hierarchy_data: Union[pa.Table, Dict], user_preferences: Dict[str, Any]):
        """Apply pruning to reduce hierarchy size"""
        strategy = user_preferences.get('pruning_strategy', 'top_n')

        if strategy == 'top_n':
            return self._prune_to_top_n(hierarchy_data, user_preferences)
        elif strategy == 'variance_threshold':
            return self._prune_by_variance(hierarchy_data, user_preferences)
        elif strategy == 'popularity_based':
            return self._prune_by_popularity(hierarchy_data, user_preferences)
        elif strategy == 'depth_based':
            return self._prune_by_depth(hierarchy_data, user_preferences)
        else:
            return hierarchy_data  # No pruning
    
    def _prune_to_top_n(self, hierarchy_data: Union[pa.Table, Dict], user_preferences: Dict[str, Any]):
        """Prune hierarchy to top N branches at each level"""
        top_n = user_preferences.get('top_n', 20)
        primary_measure = user_preferences.get('primary_measure', 'total_sales')

        if isinstance(hierarchy_data, pa.Table):
            # Sort by primary measure and take top N
            # This is a simplified approach - in real implementation, we'd need to group by hierarchy levels
            try:
                # Get the column index for the primary measure
                col_idx = None
                for i, name in enumerate(hierarchy_data.column_names):
                    if primary_measure in name:
                        col_idx = i
                        break

                if col_idx is not None:
                    # Sort the table by the primary measure
                    sorted_table = hierarchy_data.sort_by([(hierarchy_data.schema.field(col_idx).name, 'descending')])
                    # Take top N
                    return sorted_table.slice(0, top_n)
            except:
                # If sorting fails, return original data
                pass

        elif isinstance(hierarchy_data, dict):
            # If it's a dict with path -> data mapping, sort each path's data
            pruned_data = {}
            for path, data in hierarchy_data.items():
                if isinstance(data, pa.Table):
                    try:
                        # Sort by primary measure
                        col_idx = None
                        for i, name in enumerate(data.column_names):
                            if primary_measure in name:
                                col_idx = i
                                break

                        if col_idx is not None:
                            sorted_data = data.sort_by([(data.schema.field(col_idx).name, 'descending')])
                            pruned_data[path] = sorted_data.slice(0, top_n)
                        else:
                            # If primary measure not found, just take first N
                            pruned_data[path] = data.slice(0, top_n)
                    except:
                        pruned_data[path] = data.slice(0, top_n)
                else:
                    pruned_data[path] = data

            return pruned_data

        return hierarchy_data
    
    def _prune_by_variance(self, hierarchy_data: Union[pa.Table, Dict], user_preferences: Dict[str, Any]):
        """Prune branches with low variance in measures"""
        threshold = user_preferences.get('variance_threshold', 0.1)
        measure = user_preferences.get('variance_measure', 'total_sales')

        if isinstance(hierarchy_data, pa.Table):
            # Calculate variance across the measure column
            try:
                col_idx = None
                for i, name in enumerate(hierarchy_data.column_names):
                    if measure in name:
                        col_idx = i
                        break

                if col_idx is not None:
                    import pyarrow.compute as pc
                    measure_col = hierarchy_data.column(col_idx)
                    # Calculate variance (simplified - in real implementation would need more complex analysis)
                    mean_val = pc.mean(measure_col).as_py()
                    if mean_val and mean_val != 0:
                        # Create a mask for rows above threshold
                        # This is a simplified version
                        values = measure_col.to_pylist()
                        mask = [abs(v - mean_val) / mean_val > threshold if v and mean_val else False for v in values]
                        # This is a simplified approach - would need more sophisticated filtering
                        return hierarchy_data.slice(0, min(len(values), 50))  # Placeholder
            except:
                pass

        elif isinstance(hierarchy_data, dict):
            pruned_data = {}
            for path, data in hierarchy_data.items():
                if isinstance(data, pa.Table):
                    try:
                        col_idx = None
                        for i, name in enumerate(data.column_names):
                            if measure in name:
                                col_idx = i
                                break

                        if col_idx is not None:
                            measure_col = data.column(col_idx).to_pylist()
                            mean_val = sum(m for m in measure_col if m) / len([m for m in measure_col if m]) if any(measure_col) else 0

                            if mean_val and mean_val != 0:
                                # Filter rows with sufficient variance
                                filtered_indices = []
                                for i, val in enumerate(measure_col):
                                    if val and mean_val and abs(val - mean_val) / mean_val > threshold:
                                        filtered_indices.append(i)

                                if filtered_indices:
                                    # Get only rows with sufficient variance
                                    pruned_data[path] = data.take(pa.array(filtered_indices))
                                else:
                                    # If nothing passes threshold, take top half
                                    mid_point = len(measure_col) // 2
                                    pruned_data[path] = data.slice(0, mid_point)
                            else:
                                # If mean is zero, keep top half
                                mid_point = len(measure_col) // 2
                                pruned_data[path] = data.slice(0, mid_point)
                        else:
                            # If measure not found, return original
                            pruned_data[path] = data
                    except:
                        pruned_data[path] = data
                else:
                    pruned_data[path] = data

            return pruned_data

        return hierarchy_data
    
    def _prune_by_popularity(self, hierarchy_data: Union[pa.Table, Dict], user_preferences: Dict[str, Any]):
        """Prune based on popularity/access patterns"""
        min_access_count = user_preferences.get('min_access_count', 1)

        if isinstance(hierarchy_data, dict):
            # For dict format, we may have access count metadata
            pruned_data = {}
            for path, data in hierarchy_data.items():
                access_count = user_preferences.get('access_counts', {}).get(str(path), 0)
                if access_count >= min_access_count:
                    pruned_data[path] = data
            return pruned_data

        return hierarchy_data

    def _prune_by_depth(self, hierarchy_data: Union[pa.Table, Dict], user_preferences: Dict[str, Any]):
        """Prune based on maximum hierarchy depth"""
        max_depth = user_preferences.get('max_depth', 3)

        if isinstance(hierarchy_data, dict):
            # If it's path-based data, filter by path length
            pruned_data = {}
            for path, data in hierarchy_data.items():
                if len(path) <= max_depth:
                    pruned_data[path] = data
            return pruned_data

        return hierarchy_data
    
    def get_pruning_recommendations(self, hierarchy_data: Union[pa.Table, Dict], current_config: Dict[str, Any]):
        """Provide recommendations for optimal pruning configuration"""
        recommendations = {
            'suggested_top_n': 20,
            'suggested_variance_threshold': 0.05,
            'recommended_strategy': 'top_n'
        }
        
        if isinstance(hierarchy_data, (pa.Table, dict)):
            size_estimate = len(hierarchy_data) if hasattr(hierarchy_data, '__len__') else 1000
            if size_estimate > 10000:
                recommendations['suggested_top_n'] = 50
                recommendations['recommended_strategy'] = 'variance_threshold'
            elif size_estimate > 1000:
                recommendations['suggested_top_n'] = 30
            else:
                recommendations['recommended_strategy'] = 'none'  # No pruning needed
        
        return recommendations


class ProgressiveHierarchicalLoader:
    """Load hierarchy progressively, level by level"""
    
    def __init__(self, backend: IbisBaseBackend, cache, pruning_manager: Optional[HierarchyPruningManager] = None):
        self.backend = backend
        self.cache = cache
        self.pruning_manager = pruning_manager
        self.cache_ttl = 600
        
    def load_progressive_hierarchy(self, spec: PivotSpec, expanded_paths: List[List[str]],
                                       user_preferences: Optional[Dict[str, Any]] = None,
                                       progress_callback=None):
        """Load hierarchy progressively, level by level"""
        result = {'levels': [], 'metadata': {}}

        # Load top level first
        top_level = self._load_level(spec, [], 0)

        if self.pruning_manager and user_preferences:
            top_level = self.pruning_manager.apply_pruning_strategy(top_level, user_preferences)

        result['levels'].append({
            'level': 0,
            'data': top_level,
            'path': [],
            'parent_path': None
        })

        if progress_callback:
            # Don't await progress callback if it's async - just call it
            if progress_callback:
                progress_callback({
                    'level': 0,
                    'loaded': True,
                    'total_levels': len(spec.rows),
                    'progress': 1 / len(spec.rows),
                    'data_size': len(top_level) if hasattr(top_level, '__len__') else 0
                })

        # Load expanded paths progressively
        for level_idx in range(1, len(spec.rows)):
            level_data = []

            # Load expanded paths for this level
            for path in expanded_paths:
                if len(path) == level_idx:  # Current path corresponds to this level
                    level_result = self._load_level(spec, path[:-1], level_idx)

                    if self.pruning_manager and user_preferences:
                        level_result = self.pruning_manager.apply_pruning_strategy(level_result, user_preferences)

                    level_data.append({
                        'parent_path': path[:-1],
                        'data': level_result,
                        'level': level_idx,
                        'path': path
                    })

            result['levels'].extend(level_data)

            if progress_callback:
                progress_callback({
                    'level': level_idx,
                    'loaded': True,
                    'total_levels': len(spec.rows),
                    'progress': (level_idx + 1) / len(spec.rows),
                    'data_size': sum(len(ld['data']) if hasattr(ld['data'], 'num_rows') and ld['data'] is not None else 0 for ld in level_data)
                })

        # Add metadata about the load
        result['metadata'] = {
            'total_levels_loaded': len(result['levels']),
            'total_data_points': sum(
                ld['data'].num_rows if hasattr(ld['data'], 'num_rows') and ld['data'] is not None else 0
                for ld in result['levels']
            ),
            'pruning_applied': bool(self.pruning_manager)
        }

        return result

    def _load_level(self, spec: PivotSpec, parent_path: List[str], level_idx: int):
        """Load a single level of the hierarchy"""
        if level_idx >= len(spec.rows):
            return pa.table({})  # Empty table for out-of-bounds level

        dimension = spec.rows[level_idx]
        cache_key = f"hier_level:{hash(str(spec.to_dict()))}:{str(parent_path)}:{level_idx}"

        # Try cache first
        cached_data = self.cache.get(cache_key)
        if cached_data:
            return cached_data

        level_ibis_expr = self._create_level_ibis_expression(spec, parent_path, dimension)
        result = level_ibis_expr.to_pyarrow()

        # Cache the result
        self.cache.set(cache_key, result, ttl=self.cache_ttl)
        return result
    
    def _create_level_ibis_expression(self, spec: PivotSpec, parent_path: List[str], dimension: str) -> IbisTable:
        """Create an Ibis expression for a specific level with parent filters"""
        base_table = self.backend.table(spec.table)

        # Apply filters for parent path
        filtered_table = base_table
        if parent_path:
            filter_expr = None
            for i, value in enumerate(parent_path):
                field = spec.rows[i]
                condition = (base_table[field] == value)
                if filter_expr is None:
                    filter_expr = condition
                else:
                    filter_expr &= condition
            filtered_table = filtered_table.filter(filter_expr)

        # Define aggregations in Ibis
        aggregations = []
        for measure in spec.measures:
            agg_func = getattr(filtered_table[measure.field], measure.agg)
            aggregations.append(agg_func().name(measure.alias))

        # Build the grouped and aggregated expression
        agg_expr = filtered_table.group_by(dimension).aggregate(aggregations)

        # Apply ordering
        agg_expr = agg_expr.order_by(ibis.asc(dimension))

        return agg_expr