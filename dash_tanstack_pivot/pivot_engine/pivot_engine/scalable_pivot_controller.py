"""
ScalablePivotController - Main controller for high-scale pivot operations
"""
from typing import Optional, Any, Dict, List, Union, Callable, Generator
import threading
import time
import decimal
import re
import pyarrow as pa
import asyncio
import ibis
from ibis.expr.api import Table as IbisTable

from .controller import PivotController
from .lifecycle import get_task_manager
from .tree import TreeExpansionManager
from .planner.ibis_planner import IbisPlanner
from .diff.diff_engine import QueryDiffEngine
from .backends.ibis_backend import IbisBackend
from .backends.duckdb_backend import DuckDBBackend
from .cache.memory_cache import MemoryCache
from .cache.redis_cache import RedisCache
from .types.pivot_spec import PivotSpec
from pivot_engine.streaming.streaming_processor import StreamAggregationProcessor, IncrementalMaterializedViewManager
from pivot_engine.hierarchical_scroll_manager import HierarchicalVirtualScrollManager
from pivot_engine.progressive_loader import ProgressiveDataLoader
from pivot_engine.cdc.cdc_manager import PivotCDCManager
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.intelligent_prefetch_manager import IntelligentPrefetchManager, UserPatternAnalyzer
from pivot_engine.pruning_manager import HierarchyPruningManager, ProgressiveHierarchicalLoader


class QueryStatsTracker:
    """Tracks query performance to identify candidates for materialization"""
    def __init__(self, threshold_count: int = 3, threshold_duration: float = 0.5):
        self.stats = {}  # spec_hash -> {'count': int, 'total_time': float, 'last_run': float}
        self.threshold_count = threshold_count
        self.threshold_duration = threshold_duration
        self.materialized_specs = set()

    def record_query(self, spec: PivotSpec, duration: float) -> bool:
        """
        Record query execution statistics.
        Returns True if the query should be materialized.
        """
        import hashlib
        import json
        
        # Create a stable hash of the spec (ignoring limit/offset/cursor for aggregation pattern matching)
        # We want to catch the "shape" of the query (grouping + filters)
        spec_dict = spec.to_dict()
        key_parts = {
            'table': spec_dict.get('table'),
            'rows': spec_dict.get('rows'),
            'filters': str(sorted(spec_dict.get('filters', []), key=lambda x: str(x)))
        }
        spec_hash = hashlib.md5(json.dumps(key_parts, sort_keys=True).encode()).hexdigest()

        if spec_hash in self.materialized_specs:
            return False

        if spec_hash not in self.stats:
            self.stats[spec_hash] = {'count': 0, 'total_time': 0.0, 'last_run': 0}

        entry = self.stats[spec_hash]
        entry['count'] += 1
        entry['total_time'] += duration
        entry['last_run'] = time.time()

        avg_time = entry['total_time'] / entry['count']

        if entry['count'] >= self.threshold_count and avg_time >= self.threshold_duration:
            self.materialized_specs.add(spec_hash)
            return True
        
        return False


class ScalablePivotController(PivotController):
    """
    Scalable controller for pivot operations with advanced features for large datasets.
    Coordinates: Enhanced Planner -> Streaming Processor -> Incremental Views -> Diff Engine -> Cache -> Backend
    """
    
    def __init__(
        self,
        backend_uri: str = ":memory:",
        cache: Union[str, Any] = "memory",
        planner: Optional[Any] = None,
        planner_name: str = "ibis",
        enable_tiles: bool = True,
        enable_delta: bool = True,
        enable_streaming: bool = True,
        enable_incremental_views: bool = True,
        tile_size: int = 100,
        cache_ttl: int = 300,
        **cache_options: Any
    ):
        # Initialize base PivotController
        super().__init__(
            backend_uri=backend_uri,
            cache=cache,
            planner=planner,
            planner_name=planner_name,
            enable_tiles=enable_tiles,
            enable_delta=enable_delta,
            tile_size=tile_size,
            cache_ttl=cache_ttl,
            **cache_options
        )

        self.enable_streaming = enable_streaming
        self.enable_incremental_views = enable_incremental_views
        self.task_manager = get_task_manager()
        self.planning_lock = threading.Lock() # Lock for planner (Ibis/DuckDB metadata access is not thread-safe)
        self.execution_lock = threading.RLock()
        self.hierarchy_request_lock = threading.RLock()
        
        # Helper to get connection safely
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)

        # Keep DuckDB on the Ibis-backed execution path.
        # The raw pooled DuckDB swap can leave pending results in an invalid state
        # for totals, hierarchy counts, and virtual-scroll helper queries that still
        # execute through Ibis expressions on the shared connection.

        if enable_streaming:
            self.streaming_processor = StreamAggregationProcessor()

        if enable_incremental_views:
            self.incremental_view_manager = IncrementalMaterializedViewManager(con)

        # Advanced hierarchical managers
        self.materialized_hierarchy_manager = MaterializedHierarchyManager(con, self.cache)

        # Performance managers
        # HierarchicalVirtualScrollManager expects an IbisPlanner instance
        # Lock removed as manager is now stateless/thread-safe
        self.virtual_scroll_manager = HierarchicalVirtualScrollManager(
            self.planner, self.cache, self.materialized_hierarchy_manager
        )
        # ProgressiveDataLoader expects an Ibis connection
        self.progressive_loader = ProgressiveDataLoader(con, self.cache)


        # Initialize real pattern analyzer for intelligent prefetching
        self.intelligent_prefetch_manager = IntelligentPrefetchManager(
            session_tracker=None,
            pattern_analyzer=UserPatternAnalyzer(cache=self.cache),
            backend=con,
            cache=self.cache,
        )
        # PruningManager expects an Ibis connection
        self.pruning_manager = HierarchyPruningManager(con)
        # ProgressiveHierarchicalLoader expects an Ibis connection
        self.progressive_hierarchy_loader = ProgressiveHierarchicalLoader(
            con, self.cache, self.pruning_manager
        )

        # CDC for real-time updates
        self.cdc_manager = None  # Will be set via setup_cdc method
        
        self.stats_tracker = QueryStatsTracker()
        self._running_queries: Dict[str, asyncio.Task] = {} # Map request_key -> Task

    def _uses_duckdb_ibis(self) -> bool:
        backend_name = getattr(getattr(self.planner, "con", None), "name", "").lower()
        return backend_name == "duckdb"

    async def _execute_ibis_expr_async(self, expr) -> pa.Table:
        """Execute an Ibis expression safely for the current backend."""
        loop = asyncio.get_running_loop()

        if not self._uses_duckdb_ibis():
            return await loop.run_in_executor(None, expr.to_pyarrow)

        def execute_with_lock():
            with self.execution_lock:
                return expr.to_pyarrow()

        return await loop.run_in_executor(None, execute_with_lock)

    async def setup_cdc(self, table_name: str, change_stream):
        """Setup CDC for real-time tracking of data changes"""
        # PivotCDCManager expects an Ibis connection
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
        self.cdc_manager = PivotCDCManager(con, change_stream)

        await self.cdc_manager.setup_cdc(table_name)

        # Register materialized view manager to receive change notifications
        self.cdc_manager.register_materialized_view_manager(table_name, self.incremental_view_manager)

        # Start tracking changes in the background using TaskManager
        self.task_manager.create_task(
            self.cdc_manager.track_changes(table_name),
            name=f"cdc_track_changes_{table_name}"
        )

        return self.cdc_manager

    async def setup_push_cdc(self, table_name: str):
        """Setup Push-based CDC for real-time tracking via external events"""
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
        self.cdc_manager = PivotCDCManager(con)
        self.cdc_manager.use_push_provider()
        
        await self.cdc_manager.setup_cdc(table_name)
        
        self.cdc_manager.register_materialized_view_manager(table_name, self.incremental_view_manager)
        
        # Start processing changes (consuming the queue)
        self.task_manager.create_task(
            self.cdc_manager.track_changes(table_name),
            name=f"cdc_push_track_{table_name}"
        )
        
        return self.cdc_manager

    async def push_change_event(self, table_name: str, change_dict: Dict[str, Any]):
        """Push a change event to the CDC system"""
        if not self.cdc_manager:
            raise ValueError("CDC Manager not initialized. Call setup_push_cdc first.")
            
        from pivot_engine.cdc.models import Change
        
        # Convert dict to Change model
        change = Change(
            table=table_name,
            type=change_dict.get('type', 'INSERT'),
            new_row=change_dict.get('new_row'),
            old_row=change_dict.get('old_row')
        )
        
        await self.cdc_manager.push_change(table_name, change)

    async def run_streaming_aggregation(self, spec: PivotSpec):
        """Run streaming aggregation for real-time results"""
        if not self.enable_streaming:
            raise ValueError("Streaming aggregation is not enabled")
        
        job_id = await self.streaming_processor.create_real_time_aggregation_job(spec)
        return {"job_id": job_id, "status": "created"}

    async def create_incremental_view(self, spec: PivotSpec):
        """Create incremental materialized view"""
        if not self.enable_incremental_views:
            raise ValueError("Incremental views are not enabled")
        
        view_name = await self.incremental_view_manager.create_incremental_view(spec)
        return {"view_name": view_name, "status": "created"}

    def run_virtual_scroll_hierarchical(self, spec: PivotSpec, start_row: int, end_row: int, expanded_paths: List[List[str]]):
        """Run hierarchical pivot with virtual scrolling for large datasets"""
        result = self.virtual_scroll_manager.get_visible_rows_hierarchical(
            spec, start_row, end_row, expanded_paths
        )
        return result

    # Dimension-specific sort keys that must NOT propagate across levels.
    _DIM_SORT_KEYS = ("sortKeyField", "semanticType", "sortSemantic", "sortType")

    @staticmethod
    def _build_group_rows_sort(
        group_rows: List[str],
        base_sort: Any,
        column_sort_options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Ensure deterministic ordering for level queries.

        Adapts the sort to the deepest dimension in *group_rows*,
        preserving dimension-specific metadata from the base sort and
        applying per-column options from *column_sort_options* as overrides.
        """
        if not base_sort:
            # Even without an explicit sort, apply sortKeyField from column_sort_options
            # so the planner's ORDER BY uses the hidden key column rather than the raw
            # dimension string (which would produce lexicographic ordering).
            result = []
            for dim in group_rows:
                sort_item: Dict[str, Any] = {"field": dim, "order": "asc"}
                col_opts = (column_sort_options or {}).get(dim, {})
                for key in ScalablePivotController._DIM_SORT_KEYS:
                    if key in col_opts and col_opts[key] is not None:
                        sort_item[key] = col_opts[key]
                result.append(sort_item)
            return result

        # Ensure base_sort is a list
        sort_list = base_sort if isinstance(base_sort, list) else [base_sort]
        target_dim = group_rows[-1] if group_rows else None
        
        adapted = []
        for s in sort_list:
            if not isinstance(s, dict):
                continue
            
            # Start with original item to preserve all metadata (sortKeyField, etc.)
            item = s.copy()
            
            # If target_dim exists, we ensure we are sorting by the CURRENT dimension
            # in the hierarchy if the original field is part of the hierarchy.
            original_field = s.get("field")
            if target_dim and original_field in group_rows:
                item["field"] = target_dim
                
                # Apply per-column sort options for the target dimension as overrides
                col_opts = (column_sort_options or {}).get(target_dim, {})
                for key in ScalablePivotController._DIM_SORT_KEYS:
                    if key in col_opts and col_opts[key] is not None:
                        item[key] = col_opts[key]
            
            adapted.append(item)
            
        return adapted

    @staticmethod
    def _build_parent_batch_filters(
        parent_dims: List[str],
        parent_paths: List[List[str]],
    ) -> List[Dict[str, Any]]:
        """
        Build broad-but-safe batch filters for many parent paths.
        For 1 dim: exact IN list.
        For multi-dim: per-dim IN lists (superset), then strict parent-key filtering after query.
        """
        if not parent_dims or not parent_paths:
            return []

        if len(parent_dims) == 1:
            return [{
                "field": parent_dims[0],
                "op": "in",
                "value": [str(path[0]) for path in parent_paths if len(path) >= 1],
            }]

        filters = []
        for dim_idx, dim in enumerate(parent_dims):
            vals = sorted({str(path[dim_idx]) for path in parent_paths if len(path) > dim_idx})
            filters.append({"field": dim, "op": "in", "value": vals})
        return filters

    @staticmethod
    def _row_parent_key(row: Dict[str, Any], parent_dims: List[str]) -> str:
        """Build parent key 'd1|||d2' for dispatching children into hierarchy buckets."""
        if not parent_dims:
            return ""
        parts = []
        for dim in parent_dims:
            val = row.get(dim)
            parts.append("" if val is None else str(val))
        return "|||".join(parts)

    async def run_progressive_load(self, spec: PivotSpec, chunk_callback: Optional[Callable] = None):
        """Run progressive data loading for large datasets"""
        result = await self.progressive_loader.load_progressive_chunks(spec, chunk_callback)
        return result

    async def run_hierarchical_pivot_batch_load(self, spec_dict: Dict[str, Any], target_paths: List[List[str]], max_levels: int = 3) -> Dict[str, Any]:
        """
        Efficiently load only hierarchy levels that are actually needed.
        - Always fetch root level.
        - Fetch deeper levels only for expanded parent paths (or all levels for Expand All).
        
        Args:
            spec_dict: PivotSpec as dictionary
            target_paths: List of paths to expand. [['__ALL__']] means expand everything.
            max_levels: Maximum depth to load
            
        Returns:
            Dictionary mapping parent_path_key -> list of node dicts
        """
        spec = PivotSpec.from_dict(spec_dict)
        results = {}
        rows = spec.rows or []
        if not rows:
            return results

        max_group_len = min(len(rows), max_levels + 1)
        expand_all = any(path == ['__ALL__'] for path in (target_paths or []))

        # 1) Root level is always fetched.
        root_spec = spec.copy()
        root_spec.rows = rows[:1]
        root_spec.limit = 100000
        col_sort_opts = spec.column_sort_options
        root_spec.sort = self._build_group_rows_sort(root_spec.rows, spec.sort, col_sort_opts)
        root_spec.totals = bool(spec.totals)

        try:
            root_table = await self.run_pivot_async(root_spec, return_format="arrow")
            if isinstance(root_table, pa.Table):
                results[""] = root_table.to_pylist()
        except Exception as e:
            print(f"Error loading root level: {e}")
            results[""] = []

        # 2) Collapsed mode: no expanded paths, nothing deeper to load.
        if not expand_all and not target_paths:
            return results

        # 3) Determine which deeper group levels to fetch.
        # group_len = number of grouping dimensions in query.
        # root: group_len=1 (already fetched)
        levels_to_paths: Dict[int, Optional[List[List[str]]]] = {}

        if expand_all:
            for group_len in range(2, max_group_len + 1):
                levels_to_paths[group_len] = None
        else:
            for path in target_paths or []:
                if not isinstance(path, list) or not path or path == ['__ALL__']:
                    continue
                # Parent path length p -> fetch children at group_len = p + 1.
                group_len = len(path) + 1
                if 2 <= group_len <= max_group_len:
                    levels_to_paths.setdefault(group_len, []).append(path)

        # 4) Fetch each needed deeper level once (batched).
        for group_len in sorted(levels_to_paths.keys()):
            parent_dims = rows[:group_len - 1]
            group_rows = rows[:group_len]
            parent_paths = levels_to_paths[group_len]
            valid_parent_keys = (
                {"|||".join(str(v) for v in p) for p in (parent_paths or [])}
                if parent_paths
                else None
            )

            level_spec = spec.copy()
            level_spec.rows = group_rows
            level_spec.limit = 100000
            level_spec.sort = self._build_group_rows_sort(group_rows, spec.sort, col_sort_opts)
            # Only root level should carry totals. Deeper totals create duplicate subtotal
            # rows and inflate collapsed row counts.
            level_spec.totals = False
            if parent_paths:
                level_spec.filters = list(spec.filters or []) + self._build_parent_batch_filters(
                    parent_dims, parent_paths
                )

            try:
                level_table = await self.run_pivot_async(level_spec, return_format="arrow")
            except Exception as e:
                print(f"Error loading level group_len={group_len}: {e}")
                continue

            if not isinstance(level_table, pa.Table):
                continue

            for row in level_table.to_pylist():
                parent_key = self._row_parent_key(row, parent_dims)
                if valid_parent_keys is not None and parent_key not in valid_parent_keys:
                    continue
                results.setdefault(parent_key, []).append(row)

        return results

    def _flatten_hierarchy_rows(self, spec: PivotSpec, hierarchy_result: Dict[str, List[Dict[str, Any]]], expanded_paths: List[List[str]]) -> List[Dict[str, Any]]:
        """Build visible hierarchy rows in parent-before-children order from batch-loaded levels."""
        visible_rows = []
        grand_total_emitted = False
        expand_all = expanded_paths == [['__ALL__']] or any(path == ['__ALL__'] for path in (expanded_paths or []))
        expanded_path_set = {
            "|||".join(str(item) for item in path)
            for path in (expanded_paths or [])
            if isinstance(path, list) and path and path != ['__ALL__']
        }

        def traverse(parent_key: str):
            nonlocal grand_total_emitted
            nodes = hierarchy_result.get(parent_key, [])
            current_depth = len(parent_key.split('|||')) if parent_key else 0

            for node in nodes:
                if not isinstance(node, dict):
                    continue

                row = dict(node)
                first_dim = spec.rows[0] if spec.rows else None
                is_grand_total = (
                    current_depth == 0
                    and first_dim is not None
                    and row.get(first_dim) is None
                )
                if is_grand_total:
                    if grand_total_emitted:
                        continue
                    grand_total_emitted = True

                target_dim_idx = current_depth
                if target_dim_idx < len(spec.rows):
                    target_dim = spec.rows[target_dim_idx]
                    if current_depth > 0 and row.get(target_dim) is None:
                        continue
                    if current_depth == 0 and row.get(target_dim) is None:
                        row['_id'] = 'Grand Total'
                        row['_isTotal'] = True
                    elif target_dim in row:
                        row['_id'] = row[target_dim]

                row['depth'] = current_depth
                visible_rows.append(row)

                child_path_parts = parent_key.split('|||') if parent_key else []
                if target_dim_idx < len(spec.rows):
                    current_dim = spec.rows[target_dim_idx]
                    if current_dim in row and row[current_dim] is not None:
                        child_path_parts.append(str(row[current_dim]))
                        child_key = "|||".join(child_path_parts)
                        if child_key in hierarchy_result and (expand_all or child_key in expanded_path_set):
                            traverse(child_key)

        traverse("")
        return visible_rows

    def _compute_color_scale_stats(
        self,
        rows: list,
        row_fields: list,
        col_fields: list,
    ) -> dict:
        """Compute per-column and global min/max from all data rows.

        Excludes: grand total / subtotal rows, row-dimension fields, column-dimension
        fields, and internal meta keys.  Handles negative values correctly — the
        returned min/max span the actual data range so the frontend can detect a
        zero-crossing and colour negative values red / positive values green.
        """
        meta_keys = {
            '_id', '_path', '_isTotal', 'depth', '_depth', '_level', '_expanded',
            '_parentPath', '_has_children', '_is_expanded', 'subRows', 'uuid',
            '__virtualIndex',
        }
        for f in (row_fields or []):
            meta_keys.add(f)
        for f in (col_fields or []):
            meta_keys.add(f)

        by_col: dict = {}
        table_min = float('inf')
        table_max = float('-inf')

        for row in rows:
            if not isinstance(row, dict):
                continue
            # Skip total / grand-total rows
            if (row.get('_isTotal')
                    or row.get('_path') == '__grand_total__'
                    or row.get('_id') == 'Grand Total'):
                continue
            for key, value in row.items():
                if key in meta_keys:
                    continue
                if not isinstance(value, (int, float)):
                    continue
                if value != value:  # NaN guard
                    continue
                if key not in by_col:
                    by_col[key] = {'min': value, 'max': value}
                else:
                    if value < by_col[key]['min']:
                        by_col[key]['min'] = value
                    if value > by_col[key]['max']:
                        by_col[key]['max'] = value
                if value < table_min:
                    table_min = value
                if value > table_max:
                    table_max = value

        table_stats = None
        if table_min != float('inf') and table_max != float('-inf'):
            table_stats = {'min': table_min, 'max': table_max}

        return {'byCol': by_col, 'table': table_stats}

    async def run_hierarchy_view(
        self,
        spec: PivotSpec,
        expanded_paths: List[List[str]],
        start_row: Optional[int] = None,
        end_row: Optional[int] = None,
        include_grand_total_row: bool = False,
    ) -> Dict[str, Any]:
        """Single hierarchy pipeline for full hierarchical and virtual-window requests."""
        with self.hierarchy_request_lock:
            target_paths = expanded_paths or []
            hierarchy_result = await self.run_hierarchical_pivot_batch_load(
                spec.to_dict(), target_paths, max_levels=len(spec.rows)
            )
            visible_rows = self._flatten_hierarchy_rows(spec, hierarchy_result, target_paths)
            total_rows = len(visible_rows)

            def _is_grand_total(row: Dict[str, Any]) -> bool:
                if not isinstance(row, dict):
                    return False
                return bool(
                    row.get("_isTotal")
                    or row.get("_path") == "__grand_total__"
                    or row.get("_id") == "Grand Total"
                )

            grand_total_row = next((dict(row) for row in visible_rows if _is_grand_total(row)), None)

            # Compute color scale stats from ALL visible rows (excluding totals and row fields)
            color_scale_stats = self._compute_color_scale_stats(
                visible_rows, spec.rows, getattr(spec, 'columns', [])
            )

            if start_row is not None and end_row is not None:
                window_rows = visible_rows[start_row:end_row + 1]
            else:
                window_rows = visible_rows

            return {
                "rows": window_rows,
                "total_rows": total_rows,
                "grand_total_row": grand_total_row if include_grand_total_row else None,
                "color_scale_stats": color_scale_stats,
            }

    async def run_hierarchical_progressive(self, spec: PivotSpec, expanded_paths: List[List[str]], level_callback: Optional[Callable] = None):
        """Run hierarchical data loading progressively by levels"""
        result = await self.progressive_loader.load_hierarchical_progressive(spec, expanded_paths, level_callback)
        return result

    async def run_pivot_async(
        self,
        spec: Any,
        return_format: str = "arrow",
        force_refresh: bool = False
    ) -> Union[Dict[str, Any], pa.Table]:
        """Execute a pivot query asynchronously with all scalability features"""
        start_time = time.time()
        
        self._request_count += 1
        spec = self._normalize_spec(spec)
        
        # Request Cancellation Logic
        # Key by table for now (one pivot per table at a time per user session would be ideal, 
        # but here controller is shared? Controller is usually per-request or singleton.
        # Assuming singleton controller for app: locking by table might be too aggressive if multiple users.
        # But for this optimization "Cancel stale requests when user scrolls rapidly", we assume single user context or rely on a session ID.
        # The spec doesn't strictly have session ID here. 
        # We will use table + query type as a simple debounce key.
        request_key = f"pivot_{spec.table}"
        backend_name = getattr(getattr(self.planner, "con", None), "name", "").lower()
        supports_safe_cancellation = backend_name != "duckdb"
        
        if supports_safe_cancellation and request_key in self._running_queries:
            old_task = self._running_queries[request_key]
            if not old_task.done():
                old_task.cancel()
                if hasattr(self.backend, 'interrupt'):
                    self.backend.interrupt()
                # We don't await cancellation here to stay responsive, 
                # but backend should handle interruption.
                # print(f"Cancelled stale query for {request_key}")

        # Define the work as a coroutine
        async def _do_work():
            # Planning can be CPU-bound for complex queries, so we offload it
            loop = asyncio.get_running_loop()
            
            # Removed lock wrapper
            def plan_with_lock():
                with self.planning_lock:
                    return self.planner.plan(spec)

            plan_result = await loop.run_in_executor(None, plan_with_lock)
            
            metadata = plan_result.get("metadata", {})
            
            if metadata.get("needs_column_discovery"):
                result_table = await self._execute_topn_pivot_async(spec, plan_result, force_refresh)
            else:
                result_table = await self._execute_standard_pivot_async(spec, plan_result, force_refresh)

            # Smart Materialization Check
            duration = time.time() - start_time
            if self.stats_tracker.record_query(spec, duration):
                # DuckDB/Ibis on a shared connection is sensitive to concurrent background
                # materialization and can surface "closed pending query result" errors.
                # Keep smart materialization async only for backends that safely support it.
                if not self._uses_duckdb_ibis():
                    self.task_manager.create_task(
                        self._trigger_materialization(spec),
                        name=f"smart_materialization_{spec.table}"
                    )

            # Final conversion
            if return_format == "dict":
                # cpu bound conversion
                return self._convert_table_to_dict(result_table, spec)

            return result_table

        # Schedule the new task
        current_task = asyncio.create_task(_do_work())
        self._running_queries[request_key] = current_task
        
        try:
            return await current_task
        except asyncio.CancelledError:
            # Re-raise to let caller know
            raise
        finally:
            # Cleanup
            if request_key in self._running_queries and self._running_queries[request_key] == current_task:
                del self._running_queries[request_key]

    async def _trigger_materialization(self, spec: PivotSpec):
        """Helper to run materialization in background"""
        try:
            if self._uses_duckdb_ibis():
                # Disabled for DuckDB shared-connection mode (see caller guard).
                return
            loop = asyncio.get_running_loop()

            def _materialize_with_lock():
                with self.execution_lock:
                    self.materialized_hierarchy_manager.create_materialized_hierarchy(spec)

            await loop.run_in_executor(None, _materialize_with_lock)
            print(f"Smart materialization completed for table {spec.table}")
        except Exception as e:
            print(f"Smart materialization failed: {e}")

    async def _execute_standard_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute standard pivot asynchronously with parallel execution"""
        queries_to_run, strategy = self.diff_engine.plan(plan_result, spec, force_refresh=force_refresh)
        debug_context = {
            "table": spec.table,
            "rows": getattr(spec, "rows", []),
            "cols": getattr(spec, "columns", []),
            "query_count": len(queries_to_run),
            "force_refresh": force_refresh,
        }

        self._cache_hits += strategy.get("cache_hits", 0)
        self._cache_misses += len(queries_to_run)

        loop = asyncio.get_running_loop()
        tasks = []

        # Lock wrapper removed

        for query_expr in queries_to_run:
            if hasattr(query_expr, 'to_pyarrow'):
                tasks.append(self._execute_ibis_expr_async(query_expr))
            else:
                if self.backend and hasattr(self.backend, 'execute_async'):
                    tasks.append(self.backend.execute_async(query_expr))
                elif self.backend and hasattr(self.backend, 'execute'):
                    # Fallback to sync execute in executor
                    tasks.append(loop.run_in_executor(None, self.backend.execute, query_expr))
                else:
                     print(f"Error: Cannot execute legacy query format with current backend.")
                     tasks.append(asyncio.create_task(asyncio.sleep(0, result=pa.table({})))) # Dummy task

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle exceptions in results
            valid_results = []
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Query execution error [{idx}] {debug_context}: {r}")
                    valid_results.append(pa.table({}))
                else:
                    valid_results.append(r)
            results = valid_results
        else:
            results = []

        main_result = results[0] if results else pa.table({}) if pa is not None else None

        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and main_result is not None and main_result.num_rows > 0:
            main_result = self._append_totals_row(main_result, spec, metadata)

        final_table = self.diff_engine.merge_and_finalize([main_result] if main_result is not None else [], plan_result, spec, strategy)

        return final_table if final_table is not None else pa.table({}) if pa is not None else None

    async def _execute_topn_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute top-N pivot asynchronously"""
        queries = plan_result.get("queries", [])
        col_ibis_expr = queries[0]
        
        col_cache_key = self._cache_key_for_query(col_ibis_expr, spec)
        cached_cols_table = self.cache.get(col_cache_key) if not force_refresh else None
        
        loop = asyncio.get_running_loop()

        # Lock wrapper removed

        if cached_cols_table:
            column_values = cached_cols_table.column("_col_key").to_pylist()
            self._cache_hits += 1
        else:
            col_results_table = await self._execute_ibis_expr_async(col_ibis_expr)
                
            column_values = col_results_table.column("_col_key").to_pylist()
            self.cache.set(col_cache_key, col_results_table)
            self._cache_misses += 1

        pivot_ibis_expr = self.planner.build_pivot_query_from_columns(spec, column_values)
        
        pivot_cache_key = self._cache_key_for_query(pivot_ibis_expr, spec)
        cached_pivot_table = self.cache.get(pivot_cache_key) if not force_refresh else None

        if cached_pivot_table:
            result_table = cached_pivot_table
        else:
            pivot_results_table = await self._execute_ibis_expr_async(pivot_ibis_expr)
                
            self.cache.set(pivot_cache_key, pivot_results_table)
            result_table = pivot_results_table

        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and result_table is not None and result_table.num_rows > 0:
            result_table = self._append_totals_row(result_table, spec, metadata, column_values=column_values)

        return result_table

    async def run_materialized_hierarchy(self, spec: PivotSpec):
        """Run hierarchical pivot using materialized rollups (Async Job)"""
        job_id = await self.materialized_hierarchy_manager.create_materialized_hierarchy_async(spec)
        return {"status": "pending", "job_id": job_id, "message": "Materialization job started"}

    def get_materialization_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a materialization job"""
        return self.materialized_hierarchy_manager.get_job_status(job_id)

    async def run_intelligent_prefetch(self, spec: PivotSpec, user_session: Dict[str, Any], expanded_paths: List[List[str]]):
        """Run intelligent prefetching based on user behavior patterns"""
        # 1. Drill-down prefetch strategy
        prefetch_paths = await self.intelligent_prefetch_manager.determine_prefetch_strategy(
            user_session, spec, expanded_paths
        )
        
        # 2. Scroll prefetch (L2 Cache population)
        visible_range = user_session.get('visible_range')
        velocity = user_session.get('velocity', 0.0)
        
        if visible_range and isinstance(visible_range, dict):
            start_row = visible_range.get('start', 0)
            end_row = visible_range.get('end', 100)
            # Run prefetch in background (fire and forget task)
            self.task_manager.create_task(
                self.intelligent_prefetch_manager.prefetch_next_page(
                    spec, start_row, end_row, expanded_paths, velocity
                ),
                name=f"prefetch_scroll_{spec.table}"
            )

        return {"prefetch_paths": prefetch_paths, "status": "prefetching"}

    def run_progressive_hierarchical_load(self, spec: PivotSpec, expanded_paths: List[List[str]],
                                              user_preferences: Optional[Dict[str, Any]] = None,
                                              progress_callback: Optional[Callable] = None):
        """Run progressive hierarchical loading with pruning"""
        result = self.progressive_hierarchy_loader.load_progressive_hierarchy(
            spec, expanded_paths, user_preferences, progress_callback
        )
        return result

    def run_pruned_hierarchical_pivot(self, spec: PivotSpec, expanded_paths: List[List[str]],
                                      user_preferences: Optional[Dict[str, Any]] = None):
        """Run hierarchical pivot with pruning based on user preferences"""
        return self.progressive_hierarchy_loader.load_progressive_hierarchy(
            spec, expanded_paths, user_preferences
        )

    def register_delta_checkpoint(self, table: str, timestamp: float = None, max_id: Optional[int] = None, incremental_field: str = "updated_at"):
        """Register a delta checkpoint for incremental updates"""
        timestamp = timestamp or time.time()
        # The diff_engine.register_delta_checkpoint needs to be updated to work with Ibis as well
        self.diff_engine.register_delta_checkpoint(table, timestamp, max_id, incremental_field)
    
    def _serialize_cursor_value(self, val: Any) -> Any:
        """Helper to serialize values for cursor (timestamps, dates, etc)"""
        import datetime
        if isinstance(val, (datetime.date, datetime.datetime)):
            return val.isoformat()
        return val

    async def run_pivot_export(self, spec: Any, format: str = "csv") -> Generator[bytes, None, None]:
        """
        Execute a pivot query and yield result in chunks (CSV or Parquet) for memory efficiency.
        """
        spec = self._normalize_spec(spec)
        plan_result = await asyncio.get_running_loop().run_in_executor(None, self.planner.plan, spec)
        
        queries = plan_result.get("queries", [])
        if not queries:
            yield b""
            return

        main_query = queries[-1] # Main aggregation
        
        # Check if backend supports streaming
        if hasattr(self.backend, 'execute_streaming'):
            iterator = self.backend.execute_streaming(main_query, batch_size=5000)
            
            if format.lower() == "csv":
                import pyarrow.csv as csv
                import io
                
                header_written = False
                
                for batch_rows in iterator:
                    if not batch_rows:
                         continue
                         
                    batch_table = pa.Table.from_pylist(batch_rows)
                    sink = io.BytesIO()
                    
                    write_options = csv.WriteOptions(include_header=not header_written)
                    csv.write_csv(batch_table, sink, write_options=write_options)
                    header_written = True
                    
                    yield sink.getvalue()
                    await asyncio.sleep(0)
                    
            elif format.lower() == "parquet":
                import pyarrow.parquet as pq
                import tempfile
                import os
                
                # Use a temporary file for scalable Parquet writing
                # Parquet requires random access to write the footer, so streaming to stdout is hard.
                # We write to a temp file, then stream the file content.
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
                    tmp_path = tmp_file.name
                
                writer = None
                try:
                    for batch_rows in iterator:
                        if not batch_rows: continue
                        
                        batch_table = pa.Table.from_pylist(batch_rows)
                        
                        if writer is None:
                            writer = pq.ParquetWriter(tmp_path, batch_table.schema)
                        
                        writer.write_table(batch_table)
                        # Yield nothing while writing, or yield empty bytes to keep connection alive?
                        # Yielding empty bytes might be confusing if client expects data.
                        # We just await sleep to be nice.
                        await asyncio.sleep(0)
                    
                    if writer:
                        writer.close()
                    
                    # Stream the file back
                    with open(tmp_path, "rb") as f:
                        while chunk := f.read(64 * 1024): # 64KB chunks
                            yield chunk
                            await asyncio.sleep(0)
                            
                except Exception as e:
                    print(f"Error exporting parquet: {e}")
                    raise e
                finally:
                    if writer:
                        writer.close() # Ensure closed
                    if os.path.exists(tmp_path):
                        try:
                            os.unlink(tmp_path)
                        except:
                            pass
                    
        else:
            # Fallback to full load
            table = await self.run_pivot_async(spec, return_format="arrow")
            if format.lower() == "csv":
                import pyarrow.csv as csv
                import io
                sink = io.BytesIO()
                csv.write_csv(table, sink)
                yield sink.getvalue()
            else:
                 import pyarrow.parquet as pq
                 import io
                 sink = io.BytesIO()
                 pq.write_table(table, sink)
                 yield sink.getvalue()

    async def get_drill_through_data(
        self,
        spec: PivotSpec,
        filters: List[Dict[str, Any]],
        limit: int = 100,
        offset: int = 0,
        sort_col: Optional[str] = None,
        sort_dir: str = "asc",
        text_filter: str = "",
    ) -> Dict[str, Any]:
        """
        Fetch raw data for a specific set of filters (drill through).

        Returns a dict with keys:
          'rows'       - list of record dicts (paginated, filtered, sorted)
          'total_rows' - total matching row count before pagination
        """
        import ibis

        # We need a fresh query on the base table with filters
        table_expr = self.planner.con.table(spec.table)

        # Merge spec filters and drill filters
        all_filters = (spec.filters or []) + filters

        # Use builder to build filter expression
        if hasattr(self.planner, 'builder'):
            filter_expr = self.planner.builder.build_filter_expression(table_expr, all_filters)
        else:
            filter_expr = None

        if filter_expr is not None:
            table_expr = table_expr.filter(filter_expr)

        # Apply text filter — OR across all columns cast to string (case-insensitive)
        if text_filter:
            text_lower = text_filter.lower()
            conditions = []
            for col_name in table_expr.columns:
                col = table_expr[col_name]
                try:
                    conditions.append(col.cast('string').lower().contains(text_lower))
                except Exception:
                    pass  # skip columns that cannot be cast to string
            if conditions:
                combined = conditions[0]
                for c in conditions[1:]:
                    combined = combined | c
                table_expr = table_expr.filter(combined)

        # Apply sort (before limit/offset)
        if sort_col and sort_col in table_expr.columns:
            col_expr = table_expr[sort_col]
            table_expr = table_expr.order_by(
                ibis.desc(col_expr) if sort_dir == 'desc' else ibis.asc(col_expr)
            )

        # Execute in thread pool
        loop = asyncio.get_running_loop()

        # Compute total_rows before applying limit/offset
        total = await loop.run_in_executor(None, table_expr.count().execute)

        query = table_expr.limit(limit, offset=offset)
        result = await loop.run_in_executor(None, query.execute)

        return {"rows": result.to_dict('records'), "total_rows": int(total)}

    async def update_record(self, table_name: str, key_columns: Dict[str, Any], updates: Dict[str, Any]) -> bool:
        """
        Update records in the database based on composite keys.
        Uses parameterized ? binding for all values to prevent SQL injection.
        """
        if not table_name.isidentifier():
            raise ValueError("Invalid table name")

        set_parts = []
        where_parts = []
        params = []

        for col, val in updates.items():
            if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', col):
                continue
            set_parts.append(f"{col} = ?")
            params.append(val)

        for col, val in key_columns.items():
            if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', col):
                continue
            if val is None:
                where_parts.append(f"{col} IS NULL")
            else:
                where_parts.append(f"{col} = ?")
                params.append(val)

        if not set_parts or not where_parts:
            return False

        sql = f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"

        loop = asyncio.get_running_loop()
        con = self.planner.con

        def execute_update():
            if hasattr(con, 'raw_sql'):
                con.raw_sql(sql, [*params])
            elif hasattr(con, 'execute'):
                con.execute(sql, [*params])
            elif hasattr(con, 'con'):
                con.con.execute(sql, [*params])
            else:
                raise NotImplementedError("Backend does not support parameterized SQL updates")

        await loop.run_in_executor(None, execute_update)
        return True

    async def update_cell(self, table_name: str, row_id: Any, column: str, value: Any, id_column: str = "uuid"):
        """
        Update a single cell in the database.
        Assumes the backend supports SQL UPDATE and there is a unique ID column.
        """
        # Security Note: Parameterized queries should be used to prevent SQL injection.
        # Ibis doesn't natively support UPDATE statements easily yet for all backends.
        # We'll try to use the backend's raw SQL execution if available.
        
        con = self.planner.con

        # Basic sanitization — only identifiers, never values, go into the SQL template
        if not table_name.isidentifier() or not column.isidentifier() or not id_column.isidentifier():
            raise ValueError("Invalid identifier in update request")

        # Value params — use ? binding (NOT string interpolation)
        sql = f"UPDATE {table_name} SET {column} = ? WHERE {id_column} = ?"

        loop = asyncio.get_running_loop()

        def execute_update():
            if hasattr(con, 'raw_sql'):
                con.raw_sql(sql, [value, row_id])
            elif hasattr(con, 'execute'):
                con.execute(sql, [value, row_id])
            elif hasattr(con, 'con'):
                con.con.execute(sql, [value, row_id])
            else:
                raise NotImplementedError("Backend does not support parameterized SQL updates")

        await loop.run_in_executor(None, execute_update)
        self.cache.clear()
        return {"status": "success", "updated": 1}
