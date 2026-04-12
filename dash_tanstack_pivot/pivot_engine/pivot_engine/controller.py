"""
PivotController - Enhanced with support for advanced pivot features.
"""
from typing import Optional, Any, Dict, List, Union, Callable, Generator
import time
import decimal
import pyarrow as pa
import ibis
from ibis.expr.api import Table as IbisTable

from .tree import TreeExpansionManager
from .planner.ibis_planner import IbisPlanner
from .diff.diff_engine import QueryDiffEngine
from .backends.duckdb_backend import DuckDBBackend
from .backends.ibis_backend import IbisBackend
from .cache.memory_cache import MemoryCache
from .cache.redis_cache import RedisCache
from .types.pivot_spec import PivotSpec

def sanitize_column_name(value: str) -> str:
    """Sanitize column value for use in SQL identifier"""
    import re
    if not value:
        return "null"
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(value))
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized[:63]

class PivotController:
    """
    Enhanced controller for pivot operations with advanced features.
    Coordinates: Enhanced Planner -> Enhanced DiffEngine -> Cache -> Backend
    """
    def __init__(
        self,
        backend_uri: str = ":memory:",
        cache: Union[str, Any] = "memory",
        planner: Optional[Any] = None,
        planner_name: str = "ibis",
        enable_tiles: bool = True,
        enable_delta: bool = True,
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
                print(f"Could not connect to database backend via Ibis: {e}")
                # Fallback to default in-memory DuckDB via IbisBackend
                self.backend = IbisBackend(connection_uri="duckdb://:memory:")
                self.planner = IbisPlanner(con=self.backend.con)
        else:
            # If planner is provided, try to infer backend or create a default one
            # This path assumes the caller manages the backend/planner relationship
            if not self.backend:
                self.backend = DuckDBBackend(uri=backend_uri)

        # The diff_engine expects an Ibis connection for _compute_true_total
        # If we are using IbisBackend, we pass its connection
        # If using DuckDBBackend, we might need to handle it differently or DiffEngine supports it
        diff_engine_backend = (
            self.backend.con
            if isinstance(self.backend, IbisBackend)
            else getattr(self.planner, "con", None)
        )
        
        self.diff_engine = QueryDiffEngine(
            cache=self.cache,
            default_ttl=cache_ttl,
            tile_size=tile_size,
            enable_tiles=enable_tiles,
            enable_delta_updates=enable_delta,
            backend=diff_engine_backend
        )
        self.tree_manager = TreeExpansionManager(self)

        self._request_count = 0
        self._cache_hits = 0
        self._cache_misses = 0

    def run_hierarchical_pivot(self, spec: Dict[str, Any], flatten: bool = False, start_row: int = 0, end_row: int = None, expanded_paths: List[List[str]] = None) -> Dict[str, Any]:
        import asyncio
        # Since run_hierarchical_pivot is called synchronously but the underlying method is async,
        # we need to handle this appropriately. For now, we'll use asyncio.run if not in an event loop
        try:
            loop = asyncio.get_running_loop()
            # If we're in a loop, we can't use asyncio.run, so we return a coroutine
            # This is a limitation - callers from sync context will need to handle this differently
            return self.tree_manager.run_hierarchical_pivot(spec, flatten=flatten, start_row=start_row, end_row=end_row, expanded_paths=expanded_paths)
        except RuntimeError:
            # No event loop, safe to use asyncio.run
            return asyncio.run(self.tree_manager.run_hierarchical_pivot(spec, flatten=flatten, start_row=start_row, end_row=end_row, expanded_paths=expanded_paths))

    def toggle_expansion(self, spec_hash: str, path: List[str]) -> Dict[str, Any]:
        return self.tree_manager.toggle_expansion(spec_hash, path)

    def run_pivot(
        self,
        spec: Any,
        return_format: str = "arrow",
        force_refresh: bool = False
    ) -> Union[Dict[str, Any], pa.Table]:
        """Execute a pivot query synchronously."""
        self._request_count += 1
        spec = self._normalize_spec(spec)
        plan_result = self.planner.plan(spec)
        metadata = plan_result.get("metadata", {})

        if metadata.get("needs_column_discovery"):
            result_table = self._execute_topn_pivot(spec, plan_result, force_refresh)
        else:
            result_table = self._execute_standard_pivot(spec, plan_result, force_refresh)

        if return_format == "dict":
            return self._convert_table_to_dict(result_table, spec)
            
        return result_table

    def _execute_standard_pivot(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute standard pivot synchronously."""
        queries_to_run, strategy = self.diff_engine.plan(plan_result, spec, force_refresh=force_refresh)

        self._cache_hits += strategy.get("cache_hits", 0)
        self._cache_misses += len(queries_to_run)

        results = []
        for query_expr in queries_to_run:
            if hasattr(query_expr, 'to_pyarrow'):
                try:
                    results.append(query_expr.to_pyarrow())
                except Exception as e:
                    print(f"Error executing Ibis expression: {e}")
                    results.append(pa.table({}))
            else:
                results.append(pa.table({}))

        main_result = results[0] if results else pa.table({}) if pa is not None else None

        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and main_result is not None and main_result.num_rows > 0:
            main_result = self._append_totals_row(main_result, spec, metadata)

        final_table = self.diff_engine.merge_and_finalize([main_result] if main_result is not None else [], plan_result, spec, strategy)

        return final_table if final_table is not None else pa.table({}) if pa is not None else None

    def _execute_topn_pivot(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        """Execute top-N pivot synchronously."""
        queries = plan_result.get("queries", [])
        col_ibis_expr = queries[0]
        
        col_cache_key = self._cache_key_for_query(col_ibis_expr, spec)
        cached_cols_table = self.cache.get(col_cache_key) if not force_refresh else None

        if cached_cols_table:
            column_values = cached_cols_table.column("_col_key").to_pylist()
            self._cache_hits += 1
        else:
            col_results_table = col_ibis_expr.to_pyarrow()
            column_values = col_results_table.column("_col_key").to_pylist()
            self.cache.set(col_cache_key, col_results_table)
            self._cache_misses += 1

        pivot_ibis_expr = self.planner.build_pivot_query_from_columns(spec, column_values)
        
        pivot_cache_key = self._cache_key_for_query(pivot_ibis_expr, spec)
        cached_pivot_table = self.cache.get(pivot_cache_key) if not force_refresh else None

        if cached_pivot_table:
            result_table = cached_pivot_table
        else:
            result_table = pivot_ibis_expr.to_pyarrow()
            self.cache.set(pivot_cache_key, result_table)

        metadata = plan_result.get("metadata", {})
        if metadata.get("needs_totals", False) and result_table is not None and result_table.num_rows > 0:
            result_table = self._append_totals_row(result_table, spec, metadata, column_values=column_values)

        return result_table

    async def run_pivot_async(
        self,
        spec: Any,
        return_format: str = "arrow",
        force_refresh: bool = False
    ) -> Union[Dict[str, Any], pa.Table]:
        """Execute a pivot query asynchronously."""
        self._request_count += 1
        spec = self._normalize_spec(spec)
        
        # Planning is typically CPU-bound and fast, keep sync for now
        plan_result = self.planner.plan(spec)
        metadata = plan_result.get("metadata", {})
        
        if metadata.get("needs_column_discovery"):
            result_table = await self._execute_topn_pivot_async(spec, plan_result, force_refresh)
        else:
            result_table = await self._execute_standard_pivot_async(spec, plan_result, force_refresh)

        if return_format == "dict":
            return self._convert_table_to_dict(result_table, spec)
            
        return result_table

    async def _execute_standard_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        import asyncio
        queries_to_run, strategy = self.diff_engine.plan(plan_result, spec, force_refresh=force_refresh)

        self._cache_hits += strategy.get("cache_hits", 0)
        self._cache_misses += len(queries_to_run)
        
        loop = asyncio.get_running_loop()
        results = []
        
        for query_ibis_expr in queries_to_run:
            # Offload Ibis execution to thread pool to avoid blocking the event loop
            if hasattr(query_ibis_expr, 'to_pyarrow'):
                result = await loop.run_in_executor(None, query_ibis_expr.to_pyarrow)
                results.append(result)
            else:
                 # Fallback if somehow not an ibis expr
                results.append(pa.table({}))
            
        main_result = results[0] if results else pa.table({}) if pa is not None else None

        if metadata := plan_result.get("metadata", {}):
            if metadata.get("needs_totals", False) and main_result is not None and main_result.num_rows > 0:
                main_result = self._append_totals_row(main_result, spec, metadata)

        final_table = self.diff_engine.merge_and_finalize([main_result] if main_result is not None else [], plan_result, spec, strategy)

        return final_table if final_table is not None else pa.table({}) if pa is not None else None

    async def _execute_topn_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool) -> pa.Table:
        import asyncio
        loop = asyncio.get_running_loop()
        
        queries = plan_result.get("queries", [])
        col_ibis_expr = queries[0]
        
        col_cache_key = self._cache_key_for_query(col_ibis_expr, spec)
        cached_cols_table = self.cache.get(col_cache_key) if not force_refresh else None

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

        if metadata := plan_result.get("metadata", {}):
             if metadata.get("needs_totals", False) and result_table is not None and result_table.num_rows > 0:
                result_table = self._append_totals_row(result_table, spec, metadata, column_values=column_values)

        return result_table

    def _table_already_has_grand_total(self, table: "pa.Table", spec) -> bool:
        """Return True if table already contains a row where all row-dimension columns are None.

        This detects a ROLLUP/grouping_sets grand total row produced by the underlying
        Ibis query, preventing _append_totals_row from adding a second copy.
        """
        import pyarrow.compute as pc
        row_dims = getattr(spec, "rows", []) or []
        if not row_dims:
            return False
        first_dim = row_dims[0]
        if first_dim not in table.column_names:
            return False
        try:
            col = table.column(first_dim)
            null_mask = pc.is_null(col)
            return bool(pc.any(null_mask).as_py())
        except Exception:
            return False

    def _append_totals_row(
        self,
        table: pa.Table,
        spec: Any,
        metadata: Dict[str, Any],
        column_values: Optional[List[str]] = None,
    ) -> pa.Table:
        """
        Append a totals row computed from an ungrouped source query.

        This preserves aggregate semantics for avg/min/max instead of trying to
        derive a grand total from already-aggregated rows.
        """
        # Guard: if a grand total row already exists in the table (e.g. from ROLLUP),
        # do not append a second one. Per user decision (Phase 3.1): single-total enforcement.
        if self._table_already_has_grand_total(table, spec):
            return table
        totals_query = self._build_totals_query(spec, column_values=column_values)
        if totals_query is None:
            return self._compute_totals_arrow(table, metadata)

        try:
            totals_table = totals_query.to_pyarrow()
        except Exception as e:
            print(f"Warning: Could not execute totals query directly: {e}")
            return self._compute_totals_arrow(table, metadata)

        aligned_totals = self._align_total_row_to_result(totals_table, table)
        return pa.concat_tables([table, aligned_totals])

    def _build_totals_query(self, spec: Any, column_values: Optional[List[str]] = None):
        """Build a source-of-truth totals query with grouping removed."""
        total_spec = self._normalize_spec(spec)
        total_spec = total_spec.copy() if hasattr(total_spec, "copy") else total_spec

        total_spec.rows = []
        total_spec.full_rows = []
        total_spec.limit = 0
        total_spec.cursor = None
        total_spec.totals = False

        if total_spec.columns and column_values is not None:
            return self.planner.build_pivot_query_from_columns(total_spec, column_values)

        plan_result = self.planner.plan(total_spec, optimize=False)
        queries = plan_result.get("queries", [])
        return queries[0] if queries else None

    def _align_total_row_to_result(self, totals_table: pa.Table, result_table: pa.Table) -> pa.Table:
        """Cast or pad a one-row totals result so it matches the grouped result schema."""
        if totals_table is None or totals_table.num_rows == 0:
            return pa.table(
                [pa.array([None], type=field.type) for field in result_table.schema],
                schema=result_table.schema,
            )

        arrays = []
        total_column_names = set(totals_table.column_names)
        total_row = totals_table.slice(0, 1)

        for field in result_table.schema:
            if field.name in total_column_names:
                value = total_row.column(field.name)[0].as_py()
                arrays.append(pa.array([value], type=field.type))
            else:
                arrays.append(pa.array([None], type=field.type))

        return pa.table(arrays, schema=result_table.schema)

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
            columns = list(table.column_names)

            def normalize_cell(value: Any) -> Any:
                if isinstance(value, decimal.Decimal):
                    return float(value)
                if isinstance(value, float) and value != value:
                    return None
                if value is None:
                    return None
                isoformat = getattr(value, "isoformat", None)
                if callable(isoformat):
                    return isoformat()
                return value

            rows_as_lists = [
                [normalize_cell(row.get(column)) for column in columns]
                for row in table.to_pylist()
            ]

        next_cursor = None
        if spec.limit and table.num_rows == spec.limit:
            next_cursor = self._generate_next_cursor(table, spec)

        return {
            "columns": table.column_names,
            "rows": rows_as_lists,
            "next_cursor": next_cursor
        }

    def _normalize_spec(self, spec: Any) -> Any:
        """Normalize the pivot spec"""
        if isinstance(spec, dict):
            from .types.pivot_spec import PivotSpec
            return PivotSpec.from_dict(spec)
        return spec

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
        measure_aggs = metadata.get("measure_aggs", {})

        def resolve_agg_type(col_name: str) -> Optional[str]:
            if col_name in measure_aggs:
                return measure_aggs[col_name]

            for alias, agg_type in measure_aggs.items():
                if col_name == f"__RowTotal__{alias}" or col_name.endswith(f"_{alias}"):
                    return agg_type

            return None

        # Calculate totals for each aggregation column
        total_values = {}
        for col_name in table.column_names:
            # Check if column is in explicit aggregation aliases OR if it looks like a metric (numeric)
            # This handles dynamic pivot columns (e.g. 2024_sales_sum) which are not in agg_aliases
            is_metric = col_name in agg_aliases
            
            if not is_metric:
                # Heuristic: If column is numeric and not a grouping key (which are usually string/categorical in this context)
                # We assume it should be summed for the total row.
                field_type = table.schema.field(col_name).type
                if pa.types.is_integer(field_type) or pa.types.is_floating(field_type) or pa.types.is_decimal(field_type):
                    # Exclude typical ID/Key columns if they happen to be numeric (unlikely for pivot grouping keys which are cast to string usually)
                    is_metric = True

            if is_metric:
                col_array = table.column(col_name)
                agg_type = resolve_agg_type(col_name)

                try:
                    if agg_type == 'min':
                        total_val = pc.min(col_array).as_py()
                    elif agg_type == 'max':
                        total_val = pc.max(col_array).as_py()
                    elif pa.types.is_integer(col_array.type) or pa.types.is_floating(col_array.type):
                        total_val = pc.sum(col_array).as_py()
                    elif pa.types.is_decimal(col_array.type):
                        total_val = pc.sum(col_array).as_py()
                    else:
                        total_val = pc.sum(col_array).as_py()
                        if total_val is None:
                            total_val = col_array[0].as_py() if len(col_array) > 0 else None
                except Exception:
                    # If sum fails, return the original values or None
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
            spec_dict = spec.to_dict()
            spec_hash = hashlib.sha256(json.dumps(spec_dict, sort_keys=True, default=str).encode()).hexdigest()[:16]
            cache_epoch = "static"
            delta_info = getattr(getattr(self, "diff_engine", None), "_delta_info", {})
            table_name = spec_dict.get("table")
            if table_name in delta_info:
                cache_epoch = str(delta_info[table_name].last_timestamp)
            key_str = f"{compiled_sql}-{spec_hash}-{cache_epoch}"
            key_hash = hashlib.sha256(key_str.encode()).hexdigest()[:32]
            return f"pivot_ibis:query:{key_hash}"
        except Exception as e:
            # Fallback if compilation fails (e.g., incomplete expression)
            print(f"Warning: Could not compile Ibis expression for cache key: {e}")
            return f"pivot_ibis:query_fallback:{hashlib.sha256(str(ibis_expr).encode()).hexdigest()[:32]}"

    async def run_hierarchical_pivot(
        self,
        spec: Dict[str, Any],
        flatten: bool = False,
        start_row: int = 0,
        end_row: int = None,
        expanded_paths: List[List[str]] = None
    ) -> Dict[str, Any]:
        """Execute a hierarchical pivot query asynchronously."""
        return await self.tree_manager.run_hierarchical_pivot(spec, flatten=flatten, start_row=start_row, end_row=end_row, expanded_paths=expanded_paths)

    def run_hierarchical_pivot_progressive(
        self,
        spec: Dict[str, Any],
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:
        """Build and return tree progressively with intermediate results"""
        return self.tree_manager.run_hierarchical_pivot_progressive(spec, progress_callback)

    def run_hierarchical_pivot_streaming(
        self,
        spec: Dict[str, Any],
        path_cursor_map: Optional[Dict[str, Dict[str, Any]]] = None,
        chunk_size: int = 1000
    ) -> Generator[Dict[str, Any], None, None]:
        """Stream hierarchical pivot results in chunks"""
        # Use DuckDB's fetchmany() for chunked results
        spec_hash = self.tree_manager._hash_spec(spec)
        dimension_hierarchy = spec.get("rows", [])

        # Process and yield chunks instead of building full tree
        for chunk in self.tree_manager._build_tree_chunks(spec, path_cursor_map, chunk_size):
            yield chunk

    async def run_hierarchical_pivot_with_prefetch(
        self,
        spec: Dict[str, Any],
        path_cursor_map: Optional[Dict[str, Dict[str, Any]]] = None,
        prefetch_depth: int = 1
    ) -> Dict[str, Any]:
        """Run hierarchical pivot with optional prefetching of expanded nodes"""
        return await self.tree_manager.run_hierarchical_pivot_with_prefetch(spec, path_cursor_map, prefetch_depth)

    def clear_cache(self):
        """Clear all cached queries to force fresh data retrieval"""
        if hasattr(self, 'cache') and self.cache:
            self.cache.clear()

    def close(self):
        """Close any resources held by the controller"""
        if hasattr(self, 'backend') and hasattr(self.backend, 'close'):
            self.backend.close()

    async def run_hierarchical_pivot_batch_load(
        self,
        spec: Dict[str, Any],
        expanded_paths: List[List[str]],
        max_levels: int = 3
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Run batch loading of multiple levels of the hierarchy"""
        return await self.tree_manager._load_multiple_levels_batch(spec, expanded_paths, max_levels)

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

    def load_data_from_arrow(
        self,
        table_name: str,
        arrow_table: pa.Table,
        register_checkpoint: bool = True
    ):
        # Use the backend to create tables from Arrow
        if hasattr(self.backend, 'load_arrow_table'):
            self.backend.load_arrow_table(table_name, arrow_table)
        elif hasattr(self.planner, 'con') and hasattr(self.planner.con, 'create_table'):
            # If using IbisBackend, use the connection to create the table
            self.planner.con.create_table(table_name, arrow_table, overwrite=True)
        elif hasattr(self.backend, 'con') and hasattr(self.backend.con, 'create_table'):
            self.backend.con.create_table(table_name, arrow_table, overwrite=True)
        else:
            # Fallback: try DuckDB-specific approach
            if hasattr(self.backend, 'con'):
                self.backend.con.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.backend.con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table")

        if register_checkpoint:
            # Just call the diff_engine method if it exists, otherwise skip
            if hasattr(self.diff_engine, 'register_delta_checkpoint'):
                self.diff_engine.register_delta_checkpoint(table_name, timestamp=time.time())
        if hasattr(self, 'cache') and hasattr(self.cache, 'clear'):
            self.cache.clear()

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
