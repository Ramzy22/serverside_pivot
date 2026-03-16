"""
ScalablePivotController - Main controller for high-scale pivot operations
"""
from typing import Optional, Any, Dict, List, Union, Callable, Generator
import time
import decimal
import pyarrow as pa
import asyncio
import ibis
from ibis.expr.api import Table as IbisTable

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


def sanitize_column_name(value: str) -> str:
    """Sanitize column value for use in SQL identifier"""
    import re
    if not value:
        return "null"
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(value))
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized[:63]


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


class ScalablePivotController:
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
        self.enable_arrow = True
        if backend_uri == ":memory:":
            backend_uri = ":memory:shared_pivot_db"

        # Initialize Cache
        if isinstance(cache, str):
            if cache == "redis":
                self.cache = RedisCache(**cache_options)
            elif cache == "memory":
                self.cache = MemoryCache(ttl=cache_ttl)
            else:
                raise ValueError(f"Unknown cache type: {cache}")
        else:
            self.cache = cache or MemoryCache(ttl=cache_ttl)

        # Initialize Backend and Planner
        self.backend = None
        self.planner = planner

        if not self.planner:
            # Always default to IbisPlanner
            try:
                # Use IbisBackend which handles connection details
                self.backend = IbisBackend(connection_uri=backend_uri)
                # Create IbisPlanner with the connection from the backend
                self.planner = IbisPlanner(con=self.backend.con)
            except Exception as e:
                # If IbisBackend fails, we can try to fallback to a basic DuckDB connection via Ibis?
                # Or just raise error. The requirement is to be backend agnostic via Ibis.
                # Falling back to SQLPlanner is no longer an option.
                print(f"Could not connect to database backend via Ibis: {e}")
                # We can try to initialize IbisBackend with a default duckdb
                if backend_uri == ":memory:shared_pivot_db":
                     self.backend = IbisBackend(connection_uri="duckdb://:memory:")
                     self.planner = IbisPlanner(con=self.backend.con)
                else:
                     raise e
        else:
            # If planner is provided, try to infer backend or create a default one
            # This path assumes the caller manages the backend/planner relationship
            if not self.backend:
                # If planner has a connection, try to use it
                if hasattr(self.planner, 'con'):
                    self.backend = IbisBackend(connection=self.planner.con)
                else:
                    self.backend = DuckDBBackend(uri=backend_uri)

        # Enhanced components for scalability
        # Pass Ibis connection to DiffEngine for backend operations
        diff_engine_backend = self.backend.con if isinstance(self.backend, IbisBackend) else None
        
        self.diff_engine = QueryDiffEngine(
            cache=self.cache,
            default_ttl=cache_ttl,
            tile_size=tile_size,
            enable_tiles=enable_tiles,
            enable_delta_updates=enable_delta,
            backend=diff_engine_backend
        )
        
        self.tree_manager = TreeExpansionManager(self)
        
        # Scalability features (already set earlier)
        self.enable_streaming = enable_streaming
        if enable_streaming:
            # IncrementalMaterializedViewManager expects an Ibis connection
            # Use backend.con if available, or planner.con as fallback
            con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
            self.streaming_processor = StreamAggregationProcessor()

        self.enable_incremental_views = enable_incremental_views
        if enable_incremental_views:
            con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
            self.incremental_view_manager = IncrementalMaterializedViewManager(con)

        # Advanced hierarchical managers
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
        self.materialized_hierarchy_manager = MaterializedHierarchyManager(con, self.cache)

        # Performance managers
        # HierarchicalVirtualScrollManager expects an IbisPlanner instance
        self.virtual_scroll_manager = HierarchicalVirtualScrollManager(self.planner, self.cache, self.materialized_hierarchy_manager)
        # ProgressiveDataLoader expects an Ibis connection
        self.progressive_loader = ProgressiveDataLoader(con, self.cache)


        # Initialize real pattern analyzer for intelligent prefetching
        from pivot_engine.intelligent_prefetch_manager import UserPatternAnalyzer

        self.intelligent_prefetch_manager = IntelligentPrefetchManager(
            session_tracker=None,  # Would be injected
            pattern_analyzer=UserPatternAnalyzer(cache=self.cache),  # Real pattern analyzer
            backend=con, # Prefetch Manager expects an Ibis connection
            cache=self.cache
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
        
        self._request_count = 0
        self._cache_hits = 0
        self._cache_misses = 0

    async def setup_cdc(self, table_name: str, change_stream):
        """Setup CDC for real-time tracking of data changes"""
        # PivotCDCManager expects an Ibis connection
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
        self.cdc_manager = PivotCDCManager(con, change_stream)

        await self.cdc_manager.setup_cdc(table_name)

        # Register materialized view manager to receive change notifications
        self.cdc_manager.register_materialized_view_manager(table_name, self.incremental_view_manager)

        # Start tracking changes in the background
        asyncio.create_task(self.cdc_manager.track_changes(table_name))

        return self.cdc_manager

    async def setup_push_cdc(self, table_name: str):
        """Setup Push-based CDC for real-time tracking via external events"""
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
        self.cdc_manager = PivotCDCManager(con)
        self.cdc_manager.use_push_provider()
        
        await self.cdc_manager.setup_cdc(table_name)
        
        self.cdc_manager.register_materialized_view_manager(table_name, self.incremental_view_manager)
        
        # Start processing changes (consuming the queue)
        asyncio.create_task(self.cdc_manager.track_changes(table_name))
        
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

    def run_hierarchical_pivot(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Standard hierarchical pivot"""
        return self.tree_manager.run_hierarchical_pivot(spec)

    def toggle_expansion(self, spec_hash: str, path: List[str]) -> Dict[str, Any]:
        """Toggle expansion of a hierarchical path"""
        return self.tree_manager.toggle_expansion(spec_hash, path)

    def run_virtual_scroll_hierarchical(self, spec: PivotSpec, start_row: int, end_row: int, expanded_paths: List[List[str]]):
        """Run hierarchical pivot with virtual scrolling for large datasets"""
        result = self.virtual_scroll_manager.get_visible_rows_hierarchical(
            spec, start_row, end_row, expanded_paths
        )
        return result

    async def run_progressive_load(self, spec: PivotSpec, chunk_callback: Optional[Callable] = None):
        """Run progressive data loading for large datasets"""
        result = await self.progressive_loader.load_progressive_chunks(spec, chunk_callback)
        return result

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
        
        # Planning can be CPU-bound for complex queries, so we offload it
        loop = asyncio.get_running_loop()
        plan_result = await loop.run_in_executor(None, self.planner.plan, spec)
        
        metadata = plan_result.get("metadata", {})
        
        if metadata.get("needs_column_discovery"):
            result_table = await self._execute_topn_pivot_async(spec, plan_result, force_refresh)
        else:
            result_table = await self._execute_standard_pivot_async(spec, plan_result, force_refresh)

        # Smart Materialization Check
        duration = time.time() - start_time
        if self.stats_tracker.record_query(spec, duration):
            # Trigger materialization in background (fire and forget)
            asyncio.create_task(self._trigger_materialization(spec))

        # Final conversion
        if return_format == "dict":
            # cpu bound conversion
            return self._convert_table_to_dict(result_table, spec)

        return result_table

    async def _trigger_materialization(self, spec: PivotSpec):
        """Helper to run materialization in background"""
        try:
            loop = asyncio.get_running_loop()
            # Offload to thread pool as Ibis materialization calls are synchronous
            await loop.run_in_executor(None, self.materialized_hierarchy_manager.create_materialized_hierarchy, spec)
            print(f"Smart materialization completed for table {spec.table}")
        except Exception as e:
            print(f"Smart materialization failed: {e}")

    async def _execute_standard_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute standard pivot asynchronously with parallel execution"""
        queries_to_run, strategy = self.diff_engine.plan(plan_result, spec, force_refresh=force_refresh)

        self._cache_hits += strategy.get("cache_hits", 0)
        self._cache_misses += len(queries_to_run)

        loop = asyncio.get_running_loop()
        tasks = []

        for query_expr in queries_to_run:
            if hasattr(query_expr, 'to_pyarrow'):
                # Create a task for Ibis execution
                tasks.append(loop.run_in_executor(None, query_expr.to_pyarrow))
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
            for r in results:
                if isinstance(r, Exception):
                    print(f"Query execution error: {r}")
                    valid_results.append(pa.table({}))
                else:
                    valid_results.append(r)
            results = valid_results
        else:
            results = []

        main_result = results[0] if results else pa.table({}) if pa is not None else None

        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and main_result is not None and main_result.num_rows > 0:
            main_result = self._compute_totals_arrow(main_result, metadata)

        final_table = self.diff_engine.merge_and_finalize([main_result] if main_result is not None else [], plan_result, spec, strategy)

        return main_result if main_result is not None else pa.table({}) if pa is not None else None

    async def _execute_topn_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute top-N pivot asynchronously"""
        queries = plan_result.get("queries", [])
        col_ibis_expr = queries[0]
        
        col_cache_key = self._cache_key_for_query(col_ibis_expr, spec)
        cached_cols_table = self.cache.get(col_cache_key) if not force_refresh else None
        
        loop = asyncio.get_running_loop()

        if cached_cols_table:
            column_values = cached_cols_table.column("_col_key").to_pylist()
            self._cache_hits += 1
        else:
            col_results_table = await loop.run_in_executor(None, col_ibis_expr.to_pyarrow)
            column_values = col_results_table.column("_col_key").to_pylist()
            self.cache.set(col_cache_key, col_results_table)
            self._cache_misses += 1

        pivot_ibis_expr = self.planner.build_pivot_query_from_columns(spec, column_values)
        
        pivot_cache_key = self._cache_key_for_query(pivot_ibis_expr, spec)
        cached_pivot_table = self.cache.get(pivot_cache_key) if not force_refresh else None

        if cached_pivot_table:
            result_table = cached_pivot_table
        else:
            pivot_results_table = await loop.run_in_executor(None, pivot_ibis_expr.to_pyarrow)
            self.cache.set(pivot_cache_key, pivot_results_table)
            result_table = pivot_results_table

        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and result_table is not None and result_table.num_rows > 0:
            result_table = self._compute_totals_arrow(result_table, metadata)

        return result_table

    def run_pivot(
        self,
        spec: Any,
        return_format: str = "arrow",
        force_refresh: bool = False
    ) -> Union[Dict[str, Any], pa.Table]:
        """Execute a pivot query with all scalability features"""
        self._request_count += 1
        spec = self._normalize_spec(spec)
        plan_result = self.planner.plan(spec) # Returns {"queries": List[IbisExpr], "metadata": {...}}
        metadata = plan_result.get("metadata", {})
        queries_to_run = plan_result.get("queries", [])

        # Queries in plan_result.queries are now Ibis expressions
        # The logic here needs to adapt to handle Ibis expressions directly

        if metadata.get("needs_column_discovery"):
            result_table = self._execute_topn_pivot(spec, plan_result, force_refresh)
        else:
            result_table = self._execute_standard_pivot(spec, plan_result, force_refresh)

        # Final conversion based on requested format
        if return_format == "dict":
            return self._convert_table_to_dict(result_table, spec)

        return result_table

    def _execute_standard_pivot(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute standard pivot with all scalability optimizations"""
        # The diff_engine.plan needs to be updated to work with Ibis expressions
        # For now, we are passing the entire plan_result which contains IbisExpr
        queries_to_run, strategy = self.diff_engine.plan(plan_result, spec, force_refresh=force_refresh)

        self._cache_hits += strategy.get("cache_hits", 0)
        self._cache_misses += len(queries_to_run)

        results = []
        for query_expr in queries_to_run:
            if hasattr(query_expr, 'to_pyarrow'): # Check if it's an Ibis expression
                # Execute the Ibis expression directly
                try:
                    results.append(query_expr.to_pyarrow())
                except Exception as e:
                    print(f"Error executing Ibis expression: {e}")
                    results.append(pa.table({}))
            else:
                # Fallback for SQLPlanner legacy dicts
                # This should ideally be removed for 100% Ibis purity, but kept for safety with legacy planner
                if self.backend and hasattr(self.backend, 'execute'):
                    results.append(self.backend.execute(query_expr))
                else:
                     print(f"Error: Cannot execute legacy query format with current backend.")
                     results.append(pa.table({}))


        # Get the main aggregation result if available
        main_result = results[0] if results else pa.table({}) if pa is not None else None

        # Compute totals using Arrow operations if needed
        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and main_result is not None and main_result.num_rows > 0:
            # Calculate totals from the main result using Arrow compute
            main_result = self._compute_totals_arrow(main_result, metadata)

        # The diff_engine.merge_and_finalize also needs to be updated for Ibis expressions
        # For now, it will receive pyarrow tables
        final_table = self.diff_engine.merge_and_finalize([main_result] if main_result is not None else [], plan_result, spec, strategy)

        return main_result if main_result is not None else pa.table({}) if pa is not None else None

    def _execute_topn_pivot(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute top-N pivot"""
        # queries now contains Ibis expressions
        queries = plan_result.get("queries", [])
        col_ibis_expr = queries[0] # This should be an Ibis expression
        
        # The cache key needs to adapt to Ibis expressions
        col_cache_key = self._cache_key_for_query(col_ibis_expr, spec)
        cached_cols_table = self.cache.get(col_cache_key) if not force_refresh else None

        if cached_cols_table:
            column_values = cached_cols_table.column("_col_key").to_pylist()
            self._cache_hits += 1
        else:
            # Execute the Ibis expression directly
            col_results_table = col_ibis_expr.to_pyarrow()
            column_values = col_results_table.column("_col_key").to_pylist()
            self.cache.set(col_cache_key, col_results_table)
            self._cache_misses += 1

        # planner.build_pivot_query_from_columns now returns an Ibis expression
        pivot_ibis_expr = self.planner.build_pivot_query_from_columns(spec, column_values)
        
        pivot_cache_key = self._cache_key_for_query(pivot_ibis_expr, spec)
        cached_pivot_table = self.cache.get(pivot_cache_key) if not force_refresh else None

        if cached_pivot_table:
            result_table = cached_pivot_table
        else:
            # Execute the Ibis expression directly
            pivot_results_table = pivot_ibis_expr.to_pyarrow()
            self.cache.set(pivot_cache_key, pivot_results_table)
            result_table = pivot_results_table

        # Compute totals using Arrow operations if needed
        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and result_table is not None and result_table.num_rows > 0:
            result_table = self._compute_totals_arrow(result_table, metadata)

        return result_table

    def load_data_from_arrow(
        self,
        table_name: str,
        arrow_table: pa.Table,
        register_checkpoint: bool = True
    ):
        # Use the Ibis connection to create tables from Arrow, making them visible to the IbisPlanner
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
        if hasattr(con, 'create_table'):
            con.create_table(table_name, arrow_table, overwrite=True)
        
        if register_checkpoint:
            self.register_delta_checkpoint(table_name, timestamp=time.time())
        
        # If CDC is enabled, register the table for change tracking
        if self.cdc_manager:
            asyncio.create_task(self.cdc_manager.setup_cdc(table_name))

    def register_delta_checkpoint(self, table: str, timestamp: float = None, max_id: Optional[int] = None, incremental_field: str = "updated_at"):
        """Register a delta checkpoint for incremental updates"""
        timestamp = timestamp or time.time()
        # The diff_engine.register_delta_checkpoint needs to be updated to work with Ibis as well
        self.diff_engine.register_delta_checkpoint(table, timestamp, max_id, incremental_field)


    def _normalize_spec(self, spec: Any) -> Any:
        """Normalize the pivot spec"""
        if isinstance(spec, dict):
            return PivotSpec.from_dict(spec)
        return spec

    def _convert_table_to_dict(self, table: Optional[pa.Table], spec: PivotSpec) -> Dict[str, Any]:
        """Convert a PyArrow Table to the legacy dictionary format with optimization."""
        if table is None or table.num_rows == 0:
            return {"columns": [], "rows": [], "next_cursor": None}

        rows_as_lists = []
        try:
            # 1. Try Vectorized Pandas Conversion (Fastest)
            df = table.to_pandas()

            # Handle Decimal -> Float
            for col in df.columns:
                 # Check for object type which might hold Decimals
                 if df[col].dtype == 'object':
                     # Heuristic: check first non-null
                     valid_idx = df[col].first_valid_index()
                     if valid_idx is not None and isinstance(df[col][valid_idx], decimal.Decimal):
                         df[col] = df[col].astype(float)

            # Handle NaN -> None (standard JSON)
            # Efficiently replace NaN with None using where
            df = df.where(df.notnull(), None)

            rows_as_lists = df.values.tolist()

        except (ImportError, Exception):
            # 2. Fallback to vectorized PyArrow operations (instead of row-by-row)
            # This is much faster than the previous row-by-row approach
            import pyarrow.compute as pc

            # Convert each column to Python lists vectorized
            num_rows = table.num_rows
            num_cols = len(table.schema)

            # Pre-allocate the result lists
            rows_as_lists = [None] * num_rows
            for row_idx in range(num_rows):
                rows_as_lists[row_idx] = [None] * num_cols

            # Process each column vectorized
            for col_idx, col_name in enumerate(table.column_names):
                col_array = table.column(col_idx)

                # Handle different PyArrow types efficiently
                if pa.types.is_dictionary(col_array.type):
                    # Convert dictionary to string values
                    values = pc.cast(col_array, pa.string()).to_pylist()
                elif pa.types.is_decimal(col_array.type):
                    # Convert decimals to floats
                    values = [float(x) if x is not None else None for x in col_array.to_pylist()]
                elif pa.types.is_floating(col_array.type):
                    # Handle floats including NaN
                    values = col_array.to_pylist()
                    # Replace NaN with None
                    for i, val in enumerate(values):
                        if val is not None and isinstance(val, float) and val != val:  # NaN check
                            values[i] = None
                elif pa.types.is_temporal(col_array.type) or pa.types.is_timestamp(col_array.type):
                    # Convert temporal types to string representation
                    values = [str(x) if x is not None else None for x in col_array.to_pylist()]
                else:
                    # For other types, use direct conversion
                    values = col_array.to_pylist()

                # Put the values in the appropriate column of each row
                for row_idx in range(num_rows):
                    rows_as_lists[row_idx][col_idx] = values[row_idx]

        next_cursor = None
        if spec.limit and table.num_rows == spec.limit: # Check if result size matches limit
            next_cursor = self._generate_next_cursor(table, spec)

        return {
            "columns": table.column_names,
            "rows": rows_as_lists,
            "next_cursor": next_cursor
        }

    def _compute_totals_arrow(self, table: pa.Table, metadata: Dict[str, Any]) -> pa.Table:
        """
        Compute totals using Arrow compute operations for efficiency.
        This method computes grand totals from the main result table.
        """
        if table.num_rows == 0:
            return table

        import pyarrow.compute as pc
        import pyarrow as pa

        # Get the aggregation aliases and their original aggregation types from measures
        agg_aliases = metadata.get("agg_aliases", [])

        # Calculate totals for each aggregation column
        total_values = {}
        for col_name in table.column_names:
            if col_name in agg_aliases:
                col_array = table.column(col_name)

                # For aggregated values, use SUM to calculate grand totals
                try:
                    if pa.types.is_integer(col_array.type) or pa.types.is_floating(col_array.type):
                        total_val = pc.sum(col_array).as_py()
                    elif pa.types.is_decimal(col_array.type):
                        total_val = pc.sum(col_array).as_py()
                    else:
                        total_val = pc.sum(col_array).as_py()
                        if total_val is None:
                            total_val = col_array[0].as_py() if len(col_array) > 0 else None
                except Exception:
                    total_val = col_array[0].as_py() if len(col_array) > 0 else None

                total_values[col_name] = [total_val]
            else:
                # For non-aggregation (grouping) columns, set to None in totals row
                total_values[col_name] = [None]

        # Create a new row for the totals with proper schema
        total_row_arrays = []
        for col_name in table.column_names:
            if col_name in total_values:
                # Use the same data type as the original column
                value = total_values[col_name][0]
                if value is None:
                    # Create null array of the appropriate type
                    total_row_arrays.append(pa.array([None], type=table.schema.field(col_name).type))
                else:
                    # Create array of the same type as the original
                    original_type = table.schema.field(col_name).type
                    total_row_arrays.append(pa.array([value], type=original_type))
            else:
                # For any missing columns, use null
                total_row_arrays.append(pa.array([None], type=pa.string()))

        # Create totals table with the same schema as original
        try:
            totals_table = pa.table(total_row_arrays, schema=table.schema)
        except Exception:
            # Fallback: ensure the schema matches
            totals_table = pa.table(total_row_arrays, names=table.column_names)

        # Concatenate the original table with the totals row
        return pa.concat_tables([table, totals_table])

    def _generate_next_cursor(self, table: pa.Table, spec: PivotSpec) -> Optional[Dict[str, Any]]:
        """Generate the cursor for the next page based on the last row."""
        if not spec.sort or table.num_rows == 0:
            return None

        sort_keys = spec.sort if isinstance(spec.sort, list) else [spec.sort]
        last_row = table.to_pylist()[-1]

        cursor = {}
        for key in sort_keys:
            field = key.get("field")
            if field in last_row:
                cursor[field] = last_row[field]

        return cursor if cursor else None

    def _cache_key_for_query(self, ibis_expr: IbisTable, spec: Any) -> str:
        """Generate cache key for an Ibis expression."""
        import json
        import hashlib
        
        # Use Ibis expression hash or compiled string for the key
        # Compiling to SQL here might be expensive just for caching, but ensures uniqueness
        # Alternatively, use a structural hash of the Ibis expression object if Ibis provides one
        try:
            # Attempt to compile the expression to SQL for a unique string representation
            # This is a robust way to get a unique key for the query itself
            compiled_sql = str(self.planner.con.compile(ibis_expr))
            # Include the spec hash as well to ensure filter/sort context is captured
            spec_hash = hashlib.sha256(json.dumps(spec.to_dict(), sort_keys=True, default=str).encode()).hexdigest()[:16]
            key_str = f"{compiled_sql}-{spec_hash}"
            key_hash = hashlib.sha256(key_str.encode()).hexdigest()[:32]
            return f"pivot_ibis:query:{key_hash}"
        except Exception as e:
            # Fallback if compilation fails (e.g., incomplete expression)
            print(f"Warning: Could not compile Ibis expression for cache key: {e}")
            return f"pivot_ibis:query_fallback:{hashlib.sha256(str(ibis_expr).encode()).hexdigest()[:32]}"

    def run_hierarchical_pivot_batch_load(
        self,
        spec: Dict[str, Any],
        expanded_paths: List[List[str]],
        max_levels: int = 3
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Run batch loading of multiple levels of the hierarchy"""
        return self.tree_manager._load_multiple_levels_batch(spec, expanded_paths, max_levels)

    async def run_materialized_hierarchy(self, spec: PivotSpec):
        """Run hierarchical pivot using materialized rollups (Async Job)"""
        job_id = await self.materialized_hierarchy_manager.create_materialized_hierarchy_async(spec)
        return {"status": "pending", "job_id": job_id, "message": "Materialization job started"}

    def get_materialization_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a materialization job"""
        return self.materialized_hierarchy_manager.get_job_status(job_id)

    async def run_intelligent_prefetch(self, spec: PivotSpec, user_session: Dict[str, Any], expanded_paths: List[List[str]]):
        """Run intelligent prefetching based on user behavior patterns"""
        prefetch_paths = await self.intelligent_prefetch_manager.determine_prefetch_strategy(
            user_session, spec, expanded_paths
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

    def run_pivot_arrow(
        self,
        spec: Any,
    ) -> pa.Table:
        """
        Execute a pivot query and return the result as a PyArrow Table.
        This method is used by the Flight server for Arrow-native operations.
        """
        # Execute the pivot query and return the raw Arrow table
        result = self.run_pivot(spec, return_format="arrow")
        if isinstance(result, pa.Table):
            return result
        else:
            # If for some reason it's not an Arrow table, convert it
            # This would typically be the case if some error handling returns different format
            raise ValueError(f"Expected PyArrow Table but got {type(result)}")

    def clear_cache(self):
        """Clear all cached queries to force fresh data retrieval"""
        if hasattr(self, 'cache') and self.cache:
            self.cache.clear()

    def close(self):
        """Close any resources held by the controller"""
        if hasattr(self, 'backend') and hasattr(self.backend, 'close'):
            self.backend.close()

    def run_pivot_arrow(
        self,
        spec: Any,
    ) -> pa.Table:
        """
        Execute a pivot query and return the result as a PyArrow Table.
        This method is optimized for Arrow Flight operations and direct Arrow consumption.
        """
        # Execute the pivot query with Arrow format and return the raw Arrow table
        result = self.run_pivot(spec, return_format="arrow")
        if isinstance(result, pa.Table):
            return result
        else:
            # If for some reason it's not an Arrow table, convert it
            raise ValueError(f"Expected PyArrow Table but got {type(result)}")

    async def run_pivot_arrow_async(
        self,
        spec: Any,
    ) -> pa.Table:
        """
        Execute a pivot query asynchronously and return the result as a PyArrow Table.
        This method is optimized for Arrow Flight operations and direct Arrow consumption with async support.
        """
        # Execute the pivot query with Arrow format and return the raw Arrow table
        result = await self.run_pivot_async(spec, return_format="arrow")
        if isinstance(result, pa.Table):
            return result
        else:
            # If for some reason it's not an Arrow table, convert it
            raise ValueError(f"Expected PyArrow Table but got {type(result)}")

    async def run_pivot_export(self, spec: Any, format: str = "csv") -> Generator[bytes, None, None]:
        """
        Execute a pivot query and yield result in chunks (CSV or Parquet) for memory efficiency.
        """
        spec = self._normalize_spec(spec)
        plan_result = await asyncio.get_running_loop().run_in_executor(None, self.planner.plan, spec)
        
        # We assume standard pivot returns a single main query for export
        # If top-N columns are involved, we might need pre-execution, but export usually implies standard grid or raw data.
        # For simplicity, we take the last query which is usually the main result.
        queries = plan_result.get("queries", [])
        if not queries:
            yield b""
            return

        main_query = queries[-1] # Main aggregation
        
        # Check if backend supports streaming
        if hasattr(self.backend, 'execute_streaming'):
            # Use streaming execution
            # Note: execute_streaming is synchronous generator in backend, we should iterate it carefully
            # preventing blocking loop for too long, or run in executor? 
            # Ibis execution is CPU/IO bound.
            
            iterator = self.backend.execute_streaming(main_query, batch_size=5000)
            
            if format.lower() == "csv":
                import pyarrow.csv as csv
                import io
                
                first_batch = True
                header_written = False
                
                for batch_rows in iterator:
                    # batch_rows is list of dicts. Convert to Arrow Table/Batch for CSV writing efficiency
                    if not batch_rows:
                         continue
                         
                    batch_table = pa.Table.from_pylist(batch_rows)
                    sink = io.BytesIO()
                    
                    write_options = csv.WriteOptions(include_header=not header_written)
                    csv.write_csv(batch_table, sink, write_options=write_options)
                    header_written = True
                    
                    yield sink.getvalue()
                    
                    # Yield control to event loop
                    await asyncio.sleep(0)
                    
            elif format.lower() == "parquet":
                import pyarrow.parquet as pq
                import io
                
                # Parquet streaming requires a writer
                sink = io.BytesIO()
                writer = None
                
                for batch_rows in iterator:
                    if not batch_rows: continue
                    
                    batch_table = pa.Table.from_pylist(batch_rows)
                    
                    if writer is None:
                        writer = pq.ParquetWriter(sink, batch_table.schema)
                    
                    writer.write_table(batch_table)
                    
                    # Yield whatever is in buffer? ParquetWriter might buffer internally.
                    # We can't easily stream bytes of a single parquet file part by part without care.
                    # Usually Parquet is file-based. Streaming a single Parquet file over HTTP 
                    # works if we flush the writer?
                    # For strict valid Parquet, we need footer at the end.
                    # Simpler approach: Accumulate or use chunks? 
                    # If memory is constraint, we might just write row groups.
                    # But ParquetWriter writes to sink.
                    pass
                
                if writer:
                    writer.close()
                    yield sink.getvalue()
        else:
            # Fallback to full load if no streaming support
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

    def _generate_next_cursor(self, table: pa.Table, spec: PivotSpec) -> Optional[Dict[str, Any]]:
        """Generate the cursor for the next page based on the last row."""
        if not spec.sort or table.num_rows == 0:
            return None

        sort_keys = spec.sort if isinstance(spec.sort, list) else [spec.sort]
        last_row = table.to_pylist()[-1]

        cursor = {}
        for key in sort_keys:
            field = key.get("field")
            if field in last_row:
                val = last_row[field]
                cursor[field] = self._serialize_cursor_value(val)

        return cursor if cursor else None

    def _serialize_cursor_value(self, val: Any) -> Any:
        """Helper to serialize values for cursor (timestamps, dates, etc)"""
        import datetime
        if isinstance(val, (datetime.date, datetime.datetime)):
            return val.isoformat()
        return val