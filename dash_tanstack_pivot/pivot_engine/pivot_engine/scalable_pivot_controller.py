"""
ScalablePivotController - Main controller for high-scale pivot operations
"""
from typing import Optional, Any, Dict, List, Union, Callable, Generator
import threading
import time
import decimal
import datetime
import math
from contextlib import nullcontext
import re
import json
import hashlib
import pyarrow as pa
import asyncio
import ibis
from ibis.expr.api import Table as IbisTable
from collections import OrderedDict

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
        self._hierarchy_view_cache: OrderedDict[str, tuple[Dict[str, Any], float]] = OrderedDict()
        self._hierarchy_view_cache_ttl = 10.0
        self._hierarchy_view_cache_size = 32
        self._hierarchy_root_count_cache: OrderedDict[str, tuple[int, float]] = OrderedDict()
        self._hierarchy_root_count_cache_ttl = 30.0
        self._hierarchy_root_count_cache_size = 64
        self._hierarchy_root_page_cache: OrderedDict[str, tuple[List[Any], float]] = OrderedDict()
        self._hierarchy_root_page_cache_ttl = 30.0
        self._hierarchy_root_page_cache_size = 128
        self._hierarchy_grand_total_cache: OrderedDict[str, tuple[Dict[str, Any], float]] = OrderedDict()
        self._hierarchy_grand_total_cache_ttl = 30.0
        self._hierarchy_grand_total_cache_size = 64
        self._sparse_materialized_pivot_threshold = 8

    @staticmethod
    def _profile_ms(start: Optional[float], end: Optional[float]) -> Optional[float]:
        if start is None or end is None:
            return None
        return round((end - start) * 1000, 3)

    def _uses_duckdb_ibis(self) -> bool:
        backend_name = getattr(getattr(self.planner, "con", None), "name", "").lower()
        return backend_name == "duckdb"

    @staticmethod
    def _split_pivot_filters(spec: PivotSpec) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        measure_aliases = {m.alias for m in (spec.measures or []) if getattr(m, "alias", None)}
        pre_filters: List[Dict[str, Any]] = []
        post_filters: List[Dict[str, Any]] = []

        def is_measure_cond(cond: Dict[str, Any]) -> bool:
            field = cond.get("field")
            if not isinstance(field, str) or not field:
                return False
            if field in measure_aliases:
                return True
            return any(field.endswith(f"_{alias}") for alias in measure_aliases)

        for filter_spec in (spec.filters or []):
            if not isinstance(filter_spec, dict):
                continue
            if filter_spec.get("field"):
                target = pre_filters if not is_measure_cond(filter_spec) else post_filters
                target.append(filter_spec)
                continue
            conditions = filter_spec.get("conditions") or []
            has_measure = any(is_measure_cond(cond) for cond in conditions if isinstance(cond, dict))
            (post_filters if has_measure else pre_filters).append(filter_spec)

        return pre_filters, post_filters

    def _can_use_sparse_materialized_pivot(self, spec: PivotSpec) -> bool:
        pivot_config = getattr(spec, "pivot_config", None)
        materialized_column_values = (
            list(pivot_config.materialized_column_values)
            if pivot_config and pivot_config.materialized_column_values is not None
            else None
        )
        if not materialized_column_values or len(materialized_column_values) < self._sparse_materialized_pivot_threshold:
            return False
        if not getattr(spec, "columns", None):
            return False
        if not getattr(spec, "rows", None):
            return False
        if len(spec.rows) != 1:
            return False
        if getattr(spec, "having", None):
            return False
        if int(getattr(spec, "offset", 0) or 0) != 0:
            return False
        if int(getattr(spec, "limit", 0) or 0) not in (0,):
            return False
        if pivot_config and getattr(pivot_config, "include_totals_column", False):
            return False
        if any(getattr(measure, "ratio_numerator", None) or getattr(measure, "expression", None) for measure in (spec.measures or [])):
            return False
        _pre_filters, post_filters = self._split_pivot_filters(spec)
        if post_filters:
            return False

        first_row_dim = spec.rows[0]
        has_paged_row_filter = False
        for filter_spec in (spec.filters or []):
            if not isinstance(filter_spec, dict):
                continue
            if filter_spec.get("field") == first_row_dim and filter_spec.get("op") in {"in", "is null"}:
                has_paged_row_filter = True
                break
            for condition in (filter_spec.get("conditions") or []):
                if (
                    isinstance(condition, dict)
                    and condition.get("field") == first_row_dim
                    and condition.get("op") in {"in", "is null"}
                ):
                    has_paged_row_filter = True
                    break
            if has_paged_row_filter:
                break
        return has_paged_row_filter

    @staticmethod
    def _requested_first_row_values(spec: PivotSpec) -> Optional[List[Any]]:
        if not getattr(spec, "rows", None):
            return None
        first_row_dim = spec.rows[0]
        for filter_spec in (spec.filters or []):
            if not isinstance(filter_spec, dict):
                continue
            if filter_spec.get("field") == first_row_dim:
                if filter_spec.get("op") == "in" and isinstance(filter_spec.get("value"), list):
                    return list(filter_spec.get("value") or [])
                if filter_spec.get("op") == "is null":
                    return [None]
            for condition in (filter_spec.get("conditions") or []):
                if not isinstance(condition, dict) or condition.get("field") != first_row_dim:
                    continue
                if condition.get("op") == "in" and isinstance(condition.get("value"), list):
                    return list(condition.get("value") or [])
                if condition.get("op") == "is null":
                    return [None]
        return None

    def _sparse_materialized_pivot_cache_key(self, spec: PivotSpec, column_values: List[str]) -> str:
        payload = spec.to_dict()
        if payload.get("pivot_config") is None:
            payload["pivot_config"] = {}
        payload["pivot_config"]["materialized_column_values"] = list(column_values or [])
        key_hash = hashlib.sha256(self._stable_json(payload).encode()).hexdigest()[:32]
        return f"pivot_sparse:{key_hash}"

    def _build_sparse_materialized_column_filter(self, base_table: IbisTable, spec: PivotSpec, column_values: List[str]):
        column_dims = list(spec.columns or [])
        if not column_dims or not column_values:
            return None

        schema = self._get_table_schema(spec.table)
        schema_names = set(getattr(schema, "names", []) or []) if schema is not None else set()

        def coerce_value(dim: str, raw_value: Any) -> Any:
            dtype = schema[dim] if schema is not None and dim in schema_names else None
            return self._coerce_path_value_for_dtype(raw_value, dtype)

        if len(column_dims) == 1:
            dim = column_dims[0]
            coerced_values: List[Any] = []
            include_null = False
            for raw_value in column_values:
                try:
                    coerced = coerce_value(dim, raw_value)
                except (TypeError, ValueError, decimal.InvalidOperation):
                    continue
                if coerced is None:
                    include_null = True
                else:
                    coerced_values.append(coerced)
            coerced_values = list(dict.fromkeys(coerced_values))
            expr = None
            if coerced_values:
                expr = base_table[dim].isin(coerced_values)
            if include_null:
                null_expr = base_table[dim].isnull()
                expr = null_expr if expr is None else (expr | null_expr)
            return expr

        or_expr = None
        for raw_value in column_values:
            parts = str(raw_value).split("|")
            if len(parts) != len(column_dims):
                continue
            and_expr = None
            valid = True
            for dim, raw_part in zip(column_dims, parts):
                try:
                    coerced = coerce_value(dim, raw_part)
                except (TypeError, ValueError, decimal.InvalidOperation):
                    valid = False
                    break
                match_expr = base_table[dim].isnull() if coerced is None else (base_table[dim] == coerced)
                and_expr = match_expr if and_expr is None else (and_expr & match_expr)
            if not valid or and_expr is None:
                continue
            or_expr = and_expr if or_expr is None else (or_expr | and_expr)

        return or_expr

    def _build_sparse_materialized_source_query(self, spec: PivotSpec, column_values: List[str]):
        base_table = self.planner.con.table(spec.table)
        pre_filters, _post_filters = self._split_pivot_filters(spec)
        if pre_filters:
            filter_expr = self.planner.builder.build_filter_expression(base_table, pre_filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)

        column_filter_expr = self._build_sparse_materialized_column_filter(base_table, spec, column_values)
        if column_filter_expr is not None:
            base_table = base_table.filter(column_filter_expr)

        row_dims = list(spec.rows or [])
        column_dims = list(spec.columns or [])
        group_cols = list(dict.fromkeys(row_dims + column_dims))
        base_measures = [m for m in (spec.measures or []) if not getattr(m, "ratio_numerator", None)]

        aggs = [self.planner.builder.build_measure_aggregation(base_table, measure) for measure in base_measures]
        hidden_sort_keys: List[str] = []
        for sort_spec in (spec.sort or []):
            if not isinstance(sort_spec, dict):
                continue
            sort_key_field = sort_spec.get("sortKeyField")
            sort_field = sort_spec.get("field")
            sort_key_matches_group = (
                not sort_field
                or sort_field in row_dims
                or (isinstance(sort_key_field, str) and (
                    sort_key_field in row_dims or
                    (sort_key_field.startswith("__sortkey__") and sort_key_field[11:] in row_dims)
                ))
            )
            if (
                isinstance(sort_key_field, str)
                and sort_key_field
                and sort_key_field in base_table.columns
                and sort_key_matches_group
                and sort_key_field not in group_cols
            ):
                hidden_sort_keys.append(sort_key_field)
        hidden_sort_keys = list(dict.fromkeys(hidden_sort_keys))
        for key in hidden_sort_keys:
            aggs.append(base_table[key].min().name(key))

        if group_cols:
            result_expr = base_table.group_by(group_cols).aggregate(aggs)
        else:
            result_expr = base_table.aggregate(aggs)

        projection = [result_expr[col] for col in group_cols]
        for measure in base_measures:
            projection.append(result_expr[measure.alias])
        for key in hidden_sort_keys:
            projection.append(result_expr[key])
        result_expr = result_expr.select(projection)

        if row_dims:
            result_expr = self.planner._apply_stable_ordering(result_expr, spec.sort, row_dims)
        return result_expr, hidden_sort_keys

    @staticmethod
    def _materialized_column_key(values: List[Any]) -> str:
        if len(values) == 1:
            return str(values[0])
        return "|".join(str(value) for value in values)

    def _reshape_sparse_materialized_pivot(
        self,
        grouped_table: pa.Table,
        spec: PivotSpec,
        column_values: List[str],
        hidden_sort_keys: List[str],
    ) -> pa.Table:
        row_dims = list(spec.rows or [])
        column_dims = list(spec.columns or [])
        base_measures = [m for m in (spec.measures or []) if not getattr(m, "ratio_numerator", None)]
        ordered_rows: "OrderedDict[tuple, Dict[str, Any]]" = OrderedDict()
        allowed_column_values = set(column_values or [])

        requested_row_values = self._requested_first_row_values(spec)
        if len(row_dims) == 1 and requested_row_values:
            first_dim = row_dims[0]
            for value in requested_row_values:
                row_key = (value,)
                if row_key not in ordered_rows:
                    ordered_rows[row_key] = {first_dim: value}

        for grouped_row in (grouped_table.to_pylist() if isinstance(grouped_table, pa.Table) else []):
            row_key = tuple(grouped_row.get(dim) for dim in row_dims) if row_dims else ("__single__",)
            row_dict = ordered_rows.get(row_key)
            if row_dict is None:
                row_dict = {dim: grouped_row.get(dim) for dim in row_dims}
                for key in hidden_sort_keys:
                    row_dict[key] = grouped_row.get(key)
                ordered_rows[row_key] = row_dict

            column_key = self._materialized_column_key([grouped_row.get(dim) for dim in column_dims])
            if column_key not in allowed_column_values:
                continue
            for measure in base_measures:
                row_dict[f"{column_key}_{measure.alias}"] = grouped_row.get(measure.alias)

        if not ordered_rows:
            return pa.table({})

        ordered_row_list = list(ordered_rows.values())
        output_columns = list(row_dims) + list(hidden_sort_keys)
        for column_value in (column_values or []):
            for measure in base_measures:
                output_columns.append(f"{column_value}_{measure.alias}")

        column_data = {
            column_name: [row.get(column_name) for row in ordered_row_list]
            for column_name in output_columns
        }
        return pa.table(column_data)

    async def _execute_ibis_expr_async(self, expr) -> pa.Table:
        """Execute an Ibis expression safely for the current backend."""
        loop = asyncio.get_running_loop()

        if not self._uses_duckdb_ibis():
            return await loop.run_in_executor(None, expr.to_pyarrow)

        def execute_with_lock():
            with self.execution_lock:
                return expr.to_pyarrow()

        return await loop.run_in_executor(None, execute_with_lock)

    async def _execute_ibis_scalar_async(self, expr) -> Any:
        """Execute an Ibis scalar expression safely for the current backend."""
        loop = asyncio.get_running_loop()

        if not self._uses_duckdb_ibis():
            return await loop.run_in_executor(None, expr.execute)

        def execute_with_lock():
            with self.execution_lock:
                return expr.execute()

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
                "value": [path[0] for path in parent_paths if len(path) >= 1],
            }]

        filters = []
        for dim_idx, dim in enumerate(parent_dims):
            vals = []
            seen = set()
            for path in parent_paths:
                if len(path) <= dim_idx:
                    continue
                value = path[dim_idx]
                marker = (type(value), value)
                if marker in seen:
                    continue
                seen.add(marker)
                vals.append(value)
            filters.append({"field": dim, "op": "in", "value": vals})
        return filters

    @staticmethod
    def _path_key(parts: List[Any]) -> str:
        return "|||".join("" if value is None else str(value) for value in (parts or []))

    @staticmethod
    def _stable_json(value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))

    def _hierarchy_spec_fingerprint(self, spec: PivotSpec) -> str:
        spec_dict = spec.to_dict()
        for transient_key in ("limit", "offset", "cursor"):
            spec_dict.pop(transient_key, None)
        return hashlib.sha256(self._stable_json(spec_dict).encode()).hexdigest()[:24]

    @staticmethod
    def _normalize_expanded_paths(expanded_paths: List[List[str]]) -> tuple:
        if expanded_paths == [["__ALL__"]] or any(path == ["__ALL__"] for path in (expanded_paths or [])):
            return (("__ALL__",),)
        normalized = []
        for path in expanded_paths or []:
            if not isinstance(path, list) or not path:
                continue
            normalized.append(tuple("" if value is None else str(value) for value in path))
        return tuple(sorted(set(normalized)))

    @staticmethod
    def _clone_hierarchy_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [dict(row) for row in (rows or []) if isinstance(row, dict)]

    def _clone_hierarchy_result(self, hierarchy_result: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        return {
            str(parent_key): self._clone_hierarchy_rows(child_rows)
            for parent_key, child_rows in (hierarchy_result or {}).items()
        }

    def _hierarchy_view_cache_get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        cached_entry = self._hierarchy_view_cache.get(cache_key)
        if not cached_entry:
            return None
        payload, expires_at = cached_entry
        if time.time() > expires_at:
            self._hierarchy_view_cache.pop(cache_key, None)
            return None
        self._hierarchy_view_cache.move_to_end(cache_key)
        return {
            **payload,
            "expanded_set": set(payload.get("expanded_set") or set()),
            "hierarchy_result": self._clone_hierarchy_result(payload.get("hierarchy_result") or {}),
            "visible_rows": self._clone_hierarchy_rows(payload.get("visible_rows") or []),
            "grand_total_row": dict(payload["grand_total_row"]) if isinstance(payload.get("grand_total_row"), dict) else payload.get("grand_total_row"),
            "grand_total_formula_source_rows": self._clone_hierarchy_rows(payload.get("grand_total_formula_source_rows") or []),
        }

    def _hierarchy_view_cache_set(self, cache_key: str, payload: Dict[str, Any]) -> None:
        stored_payload = {
            **payload,
            "expanded_set": set(payload.get("expanded_set") or set()),
            "hierarchy_result": self._clone_hierarchy_result(payload.get("hierarchy_result") or {}),
            "visible_rows": self._clone_hierarchy_rows(payload.get("visible_rows") or []),
            "grand_total_row": dict(payload["grand_total_row"]) if isinstance(payload.get("grand_total_row"), dict) else payload.get("grand_total_row"),
            "grand_total_formula_source_rows": self._clone_hierarchy_rows(payload.get("grand_total_formula_source_rows") or []),
        }
        self._hierarchy_view_cache[cache_key] = (stored_payload, time.time() + self._hierarchy_view_cache_ttl)
        self._hierarchy_view_cache.move_to_end(cache_key)
        while len(self._hierarchy_view_cache) > self._hierarchy_view_cache_size:
            self._hierarchy_view_cache.popitem(last=False)

    def _find_reusable_hierarchy_cache_entry(self, spec_fingerprint: str, requested_expanded_set: set[tuple]) -> Optional[Dict[str, Any]]:
        if not requested_expanded_set or requested_expanded_set == {("__ALL__",)}:
            return None
        best_entry = None
        best_size = -1
        for cache_key, (payload, expires_at) in list(self._hierarchy_view_cache.items()):
            if time.time() > expires_at:
                self._hierarchy_view_cache.pop(cache_key, None)
                continue
            if payload.get("spec_fingerprint") != spec_fingerprint:
                continue
            cached_expanded_set = set(payload.get("expanded_set") or set())
            if cached_expanded_set == {("__ALL__",)}:
                continue
            if not cached_expanded_set.issubset(requested_expanded_set):
                continue
            if len(cached_expanded_set) > best_size:
                best_entry = self._hierarchy_view_cache_get(cache_key)
                best_size = len(cached_expanded_set)
        return best_entry

    def _hierarchy_root_count_cache_get(self, cache_key: str) -> Optional[int]:
        cached_entry = self._hierarchy_root_count_cache.get(cache_key)
        if not cached_entry:
            return None
        count, expires_at = cached_entry
        if time.time() > expires_at:
            self._hierarchy_root_count_cache.pop(cache_key, None)
            return None
        self._hierarchy_root_count_cache.move_to_end(cache_key)
        return int(count)

    def _hierarchy_root_count_cache_set(self, cache_key: str, count: int) -> None:
        self._hierarchy_root_count_cache[cache_key] = (int(count), time.time() + self._hierarchy_root_count_cache_ttl)
        self._hierarchy_root_count_cache.move_to_end(cache_key)
        while len(self._hierarchy_root_count_cache) > self._hierarchy_root_count_cache_size:
            self._hierarchy_root_count_cache.popitem(last=False)

    def _hierarchy_root_page_cache_get(self, cache_key: str) -> Optional[List[Any]]:
        cached_entry = self._hierarchy_root_page_cache.get(cache_key)
        if not cached_entry:
            return None
        values, expires_at = cached_entry
        if time.time() > expires_at:
            self._hierarchy_root_page_cache.pop(cache_key, None)
            return None
        self._hierarchy_root_page_cache.move_to_end(cache_key)
        return list(values or [])

    def _hierarchy_root_page_cache_set(self, cache_key: str, values: List[Any]) -> None:
        self._hierarchy_root_page_cache[cache_key] = (
            list(values or []),
            time.time() + self._hierarchy_root_page_cache_ttl,
        )
        self._hierarchy_root_page_cache.move_to_end(cache_key)
        while len(self._hierarchy_root_page_cache) > self._hierarchy_root_page_cache_size:
            self._hierarchy_root_page_cache.popitem(last=False)

    def _collapsed_root_count_cache_key(self, spec: PivotSpec) -> str:
        payload = {
            "table": spec.table,
            "rows": list(spec.rows[:1]),
            "filters": spec.filters or [],
        }
        return hashlib.sha256(self._stable_json(payload).encode()).hexdigest()[:24]

    def _collapsed_root_page_cache_key(
        self,
        spec: PivotSpec,
        root_sort: List[Dict[str, Any]],
        start_row: int,
        page_size: int,
    ) -> str:
        payload = {
            "count_key": self._collapsed_root_count_cache_key(spec),
            "sort": root_sort or [],
            "start_row": int(start_row or 0),
            "page_size": int(page_size or 0),
        }
        return hashlib.sha256(self._stable_json(payload).encode()).hexdigest()[:24]

    def _hierarchy_grand_total_cache_get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        cached_entry = self._hierarchy_grand_total_cache.get(cache_key)
        if not cached_entry:
            return None
        row, expires_at = cached_entry
        if expires_at < time.time():
            self._hierarchy_grand_total_cache.pop(cache_key, None)
            return None
        self._hierarchy_grand_total_cache.move_to_end(cache_key)
        return dict(row) if isinstance(row, dict) else None

    def _hierarchy_grand_total_cache_set(self, cache_key: str, row: Dict[str, Any]) -> None:
        if not isinstance(row, dict):
            return
        self._hierarchy_grand_total_cache[cache_key] = (
            dict(row),
            time.time() + self._hierarchy_grand_total_cache_ttl,
        )
        self._hierarchy_grand_total_cache.move_to_end(cache_key)
        while len(self._hierarchy_grand_total_cache) > self._hierarchy_grand_total_cache_size:
            self._hierarchy_grand_total_cache.popitem(last=False)

    @staticmethod
    def _filter_refs_measure_alias(filter_spec: Dict[str, Any], measure_aliases: set[str]) -> bool:
        def _is_measure_field(field: Any) -> bool:
            if not isinstance(field, str) or not field:
                return False
            if field in measure_aliases:
                return True
            return any(field.endswith(f"_{alias}") for alias in measure_aliases)

        if not isinstance(filter_spec, dict):
            return False
        if _is_measure_field(filter_spec.get("field")):
            return True
        for condition in filter_spec.get("conditions") or []:
            if isinstance(condition, dict) and _is_measure_field(condition.get("field")):
                return True
        return False

    def _can_use_paged_collapsed_root_view(
        self,
        spec: PivotSpec,
        expanded_paths: List[List[str]],
        start_row: Optional[int],
        end_row: Optional[int],
        include_grand_total_row: bool = False,
    ) -> bool:
        if start_row is None or end_row is None:
            return False
        if not spec.rows:
            return False
        if expanded_paths:
            return False
        if spec.having:
            return False
        if include_grand_total_row and any(
            getattr(measure, "agg", None) == "formula"
            for measure in (spec.measures or [])
        ):
            return False
        measure_aliases = {
            str(measure.alias)
            for measure in (spec.measures or [])
            if getattr(measure, "alias", None)
        }
        return not any(
            self._filter_refs_measure_alias(filter_spec, measure_aliases)
            for filter_spec in (spec.filters or [])
        )

    async def _count_collapsed_root_rows(self, spec: PivotSpec) -> int:
        cache_key = self._collapsed_root_count_cache_key(spec)
        cached_count = self._hierarchy_root_count_cache_get(cache_key)
        if cached_count is not None:
            return cached_count

        con = getattr(self.planner, "con", None)
        if con is None:
            return 0

        base_table = con.table(spec.table)
        if spec.filters:
            filter_expr = self.planner.builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)

        root_group_cols = list(spec.rows[:1])
        if root_group_cols:
            count_expr = base_table.select(root_group_cols).distinct().count()
        else:
            count_expr = base_table.count()

        total_rows = int(await self._execute_ibis_scalar_async(count_expr))
        self._hierarchy_root_count_cache_set(cache_key, total_rows)
        return total_rows

    @staticmethod
    def _can_page_collapsed_root_keys(root_field: str, root_sort: List[Dict[str, Any]]) -> bool:
        for sort_item in root_sort or []:
            if not isinstance(sort_item, dict):
                return False
            if sort_item.get("field") != root_field:
                return False
        return True

    @staticmethod
    def _build_collapsed_root_page_filters(
        existing_filters: Optional[List[Dict[str, Any]]],
        root_field: str,
        page_values: List[Any],
    ) -> List[Dict[str, Any]]:
        filters = [dict(item) for item in (existing_filters or []) if isinstance(item, dict)]
        non_null_values = [value for value in page_values if value is not None]
        include_null = any(value is None for value in page_values)

        if non_null_values and include_null:
            filters.append(
                {
                    "operator": "OR",
                    "conditions": [
                        {"field": root_field, "op": "in", "value": non_null_values},
                        {"field": root_field, "op": "is null"},
                    ],
                }
            )
            return filters

        if non_null_values:
            filters.append({"field": root_field, "op": "in", "value": non_null_values})
            return filters

        if include_null:
            filters.append({"field": root_field, "op": "is null"})

        return filters

    async def _fetch_paged_collapsed_root_values(
        self,
        spec: PivotSpec,
        root_sort: List[Dict[str, Any]],
        start_row: int,
        page_size: int,
    ) -> Optional[List[Any]]:
        cache_key = self._collapsed_root_page_cache_key(spec, root_sort, start_row, page_size)
        cached_values = self._hierarchy_root_page_cache_get(cache_key)
        if cached_values is not None:
            return cached_values

        root_rows = list(spec.rows[:1])
        if len(root_rows) != 1:
            return None

        root_field = root_rows[0]
        if not self._can_page_collapsed_root_keys(root_field, root_sort):
            return None

        con = getattr(self.planner, "con", None)
        if con is None:
            return None

        base_table = con.table(spec.table)
        if spec.filters:
            filter_expr = self.planner.builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)

        hidden_sort_keys: List[str] = []
        for sort_item in root_sort or []:
            sort_key_field = sort_item.get("sortKeyField")
            if (
                isinstance(sort_key_field, str)
                and sort_key_field
                and sort_key_field != root_field
                and sort_key_field in base_table.columns
            ):
                hidden_sort_keys.append(sort_key_field)
        hidden_sort_keys = list(dict.fromkeys(hidden_sort_keys))

        if hidden_sort_keys:
            key_query = base_table.group_by([root_field]).aggregate(
                [base_table[key].min().name(key) for key in hidden_sort_keys]
            )
        else:
            key_query = base_table.select([root_field]).distinct()

        key_query = self.planner._apply_stable_ordering(key_query, root_sort, [root_field])
        if page_size:
            key_query = key_query.limit(page_size, offset=max(int(start_row or 0), 0))

        key_table = await self._execute_ibis_expr_async(key_query)
        if not isinstance(key_table, pa.Table) or root_field not in key_table.column_names:
            return None

        values = key_table.column(root_field).to_pylist()
        self._hierarchy_root_page_cache_set(cache_key, values)
        return values

    async def _run_paged_collapsed_root_view(
        self,
        spec: PivotSpec,
        start_row: int,
        end_row: int,
        include_grand_total_row: bool = False,
        profiling: bool = False,
    ) -> Dict[str, Any]:
        view_started_at = time.perf_counter()
        safe_start = max(int(start_row or 0), 0)
        safe_end = max(int(end_row if end_row is not None else safe_start), safe_start)
        page_size = (safe_end - safe_start) + 1

        root_spec = spec.copy()
        root_spec.rows = list(spec.rows[:1])
        root_spec.totals = False
        root_spec.sort = self._build_group_rows_sort(root_spec.rows, spec.sort, spec.column_sort_options)

        page_key_fetch_started_at = time.perf_counter()
        paged_root_values = await self._fetch_paged_collapsed_root_values(
            spec,
            root_spec.sort,
            safe_start,
            page_size,
        )
        page_key_fetch_finished_at = time.perf_counter()
        if paged_root_values is None:
            root_spec.limit = page_size
            root_spec.offset = safe_start
            root_query_profile: Dict[str, Any] = {}
            root_query_started_at = time.perf_counter()
            root_table = await self.run_pivot_async(
                root_spec,
                return_format="arrow",
                force_refresh=True,
                profile_sink=root_query_profile if profiling else None,
            )
            root_query_finished_at = time.perf_counter()
            root_rows = root_table.to_pylist() if isinstance(root_table, pa.Table) else []
        elif not paged_root_values:
            root_query_profile = {}
            root_query_started_at = None
            root_query_finished_at = None
            root_rows = []
        else:
            root_spec.limit = None
            root_spec.offset = 0
            root_spec.filters = self._build_collapsed_root_page_filters(
                root_spec.filters,
                root_spec.rows[0],
                paged_root_values,
            )
            root_query_profile = {}
            root_query_started_at = time.perf_counter()
            root_table = await self.run_pivot_async(
                root_spec,
                return_format="arrow",
                force_refresh=True,
                profile_sink=root_query_profile if profiling else None,
            )
            root_query_finished_at = time.perf_counter()
            root_rows = root_table.to_pylist() if isinstance(root_table, pa.Table) else []
        total_count_started_at = time.perf_counter()
        total_rows = await self._count_collapsed_root_rows(spec)
        total_count_finished_at = time.perf_counter()
        grand_total_row = None
        grand_total_formula_source_rows = []

        if include_grand_total_row:
            total_cache_key = self._hierarchy_spec_fingerprint(spec)
            grand_total_row = self._hierarchy_grand_total_cache_get(total_cache_key)
            if grand_total_row is None:
                total_spec = spec.copy()
                total_spec.rows = []
                total_spec.limit = 1
                total_spec.offset = 0
                total_spec.totals = False
                total_spec.sort = []

                grand_total_query_profile: Dict[str, Any] = {}
                grand_total_started_at = time.perf_counter()
                total_table = await self.run_pivot_async(
                    total_spec,
                    return_format="arrow",
                    force_refresh=True,
                    profile_sink=grand_total_query_profile if profiling else None,
                )
                grand_total_finished_at = time.perf_counter()
                total_rows_list = total_table.to_pylist() if isinstance(total_table, pa.Table) else []
                if total_rows_list:
                    grand_total_row = dict(total_rows_list[0])
                    self._hierarchy_grand_total_cache_set(total_cache_key, grand_total_row)
            else:
                grand_total_query_profile = {"cached": True}
                grand_total_started_at = None
                grand_total_finished_at = None

            if grand_total_row is not None:
                grand_total_row = dict(grand_total_row)
                grand_total_row["_id"] = "Grand Total"
                grand_total_row["_isTotal"] = True
                grand_total_row["_path"] = "__grand_total__"
                grand_total_row["depth"] = 0
                grand_total_row["_depth"] = 0
                total_rows += 1
                grand_total_formula_source_rows = [dict(row) for row in root_rows if isinstance(row, dict)]
        else:
            grand_total_query_profile = None
            grand_total_started_at = None
            grand_total_finished_at = None

        color_scale_started_at = time.perf_counter()
        color_scale_stats = self._compute_color_scale_stats(root_rows, spec.rows, getattr(spec, "columns", []))
        color_scale_finished_at = time.perf_counter()
        spec_fingerprint = self._hierarchy_spec_fingerprint(spec)
        empty_expanded = self._normalize_expanded_paths([])
        hierarchy_cache_key = f"{spec_fingerprint}:{hashlib.sha256(self._stable_json(empty_expanded).encode()).hexdigest()[:16]}"
        self._hierarchy_view_cache_set(
            hierarchy_cache_key,
            {
                "spec_fingerprint": spec_fingerprint,
                "expanded_set": set(),
                "hierarchy_result": {"": self._clone_hierarchy_rows(root_rows)},
                "visible_rows": self._clone_hierarchy_rows(root_rows),
                "grand_total_row": dict(grand_total_row) if isinstance(grand_total_row, dict) else grand_total_row,
                "grand_total_formula_source_rows": self._clone_hierarchy_rows(grand_total_formula_source_rows),
            },
        )
        return {
            "rows": root_rows,
            "total_rows": total_rows,
            "grand_total_row": grand_total_row,
            "grand_total_formula_source_rows": grand_total_formula_source_rows,
            "color_scale_stats": color_scale_stats,
            "profile": (
                {
                    "controller": {
                        "operation": "hierarchy_view",
                        "path": "paged_collapsed_root",
                        "pageKeyFetchMs": self._profile_ms(page_key_fetch_started_at, page_key_fetch_finished_at),
                        "rootQueryMs": self._profile_ms(root_query_started_at, root_query_finished_at),
                        "rootCountMs": self._profile_ms(total_count_started_at, total_count_finished_at),
                        "grandTotalMs": self._profile_ms(grand_total_started_at, grand_total_finished_at),
                        "colorScaleMs": self._profile_ms(color_scale_started_at, color_scale_finished_at),
                        "totalMs": self._profile_ms(view_started_at, time.perf_counter()),
                        "requestedRows": page_size,
                        "returnedRows": len(root_rows),
                        "totalRows": total_rows,
                    },
                    "controllerPivot": {
                        "rootQuery": root_query_profile,
                        "grandTotalQuery": grand_total_query_profile,
                    },
                }
                if profiling
                else None
            ),
        }

    def _get_table_schema(self, table_name: str):
        try:
            con = getattr(self.planner, "con", None)
            if con is None:
                return None
            return con.table(table_name).schema()
        except Exception:
            return None

    @staticmethod
    def _coerce_path_value_for_dtype(raw_value: Any, dtype: Any) -> Any:
        if raw_value is None or dtype is None:
            return raw_value

        if callable(getattr(dtype, "is_string", None)) and dtype.is_string():
            return str(raw_value)

        if callable(getattr(dtype, "is_boolean", None)) and dtype.is_boolean():
            if isinstance(raw_value, bool):
                return raw_value
            lowered = str(raw_value).strip().lower()
            if lowered in {"true", "1", "yes", "y", "t"}:
                return True
            if lowered in {"false", "0", "no", "n", "f"}:
                return False
            return None

        if callable(getattr(dtype, "is_integer", None)) and dtype.is_integer():
            if isinstance(raw_value, bool):
                return None
            if isinstance(raw_value, int):
                return raw_value
            if isinstance(raw_value, float) and raw_value.is_integer():
                return int(raw_value)
            return int(str(raw_value).strip())

        if callable(getattr(dtype, "is_floating", None)) and dtype.is_floating():
            if isinstance(raw_value, bool):
                return None
            if isinstance(raw_value, (int, float)):
                return float(raw_value)
            return float(str(raw_value).strip())

        if callable(getattr(dtype, "is_decimal", None)) and dtype.is_decimal():
            if isinstance(raw_value, decimal.Decimal):
                return raw_value
            return decimal.Decimal(str(raw_value).strip())

        if callable(getattr(dtype, "is_date", None)) and dtype.is_date():
            if isinstance(raw_value, datetime.date) and not isinstance(raw_value, datetime.datetime):
                return raw_value
            return datetime.date.fromisoformat(str(raw_value).strip())

        if callable(getattr(dtype, "is_timestamp", None)) and dtype.is_timestamp():
            if isinstance(raw_value, datetime.datetime):
                return raw_value
            return datetime.datetime.fromisoformat(str(raw_value).strip().replace("Z", "+00:00"))

        return raw_value

    def _sanitize_parent_paths_for_level(
        self,
        table_name: str,
        parent_dims: List[str],
        parent_paths: List[List[Any]],
        valid_parent_keys: Optional[set[str]],
    ) -> List[List[Any]]:
        if not parent_dims or not parent_paths:
            return []

        schema = self._get_table_schema(table_name)
        schema_names = set(getattr(schema, "names", []) or []) if schema is not None else set()
        sanitized_paths: List[List[Any]] = []
        seen_keys: set[str] = set()

        for path in parent_paths:
            if not isinstance(path, list) or len(path) != len(parent_dims):
                continue

            coerced_path: List[Any] = []
            path_is_valid = True
            for dim_index, dim in enumerate(parent_dims):
                raw_value = path[dim_index]
                try:
                    coerced_value = self._coerce_path_value_for_dtype(
                        raw_value,
                        schema[dim] if schema is not None and dim in schema_names else None,
                    )
                except (TypeError, ValueError, decimal.InvalidOperation):
                    path_is_valid = False
                    break
                if coerced_value is None and raw_value is not None:
                    path_is_valid = False
                    break
                coerced_path.append(coerced_value)

            if not path_is_valid:
                continue

            parent_key = self._path_key(coerced_path)
            if valid_parent_keys is not None and parent_key not in valid_parent_keys:
                continue
            if parent_key in seen_keys:
                continue
            seen_keys.add(parent_key)
            sanitized_paths.append(coerced_path)

        return sanitized_paths

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
        valid_node_keys_by_depth: Dict[int, set[str]] = {}

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
                root_rows = root_table.to_pylist()
                results[""] = root_rows
                root_dim = rows[0]
                valid_node_keys_by_depth[1] = {
                    self._path_key([row.get(root_dim)])
                    for row in root_rows
                    if isinstance(row, dict) and row.get(root_dim) is not None
                }
        except Exception as e:
            print(f"Error loading root level: {e}")
            results[""] = []
            valid_node_keys_by_depth[1] = set()

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
                parent_paths = self._sanitize_parent_paths_for_level(
                    spec.table,
                    parent_dims,
                    parent_paths,
                    valid_node_keys_by_depth.get(group_len - 1),
                )
                if not parent_paths:
                    continue
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

            current_level_keys: set[str] = set()
            for row in level_table.to_pylist():
                parent_key = self._row_parent_key(row, parent_dims)
                if valid_parent_keys is not None and parent_key not in valid_parent_keys:
                    continue
                results.setdefault(parent_key, []).append(row)
                current_key = self._row_parent_key(row, group_rows)
                if current_key:
                    current_level_keys.add(current_key)

            if current_level_keys:
                valid_node_keys_by_depth[group_len] = current_level_keys

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
        profiling: bool = False,
    ) -> Dict[str, Any]:
        """Single hierarchy pipeline for full hierarchical and virtual-window requests."""
        with self.hierarchy_request_lock:
            view_started_at = time.perf_counter()
            target_paths = expanded_paths or []
            if self._can_use_paged_collapsed_root_view(
                spec,
                target_paths,
                start_row,
                end_row,
                include_grand_total_row=include_grand_total_row,
            ):
                return await self._run_paged_collapsed_root_view(
                    spec,
                    start_row,
                    end_row,
                    include_grand_total_row=include_grand_total_row,
                    profiling=profiling,
                )

            spec_fingerprint = self._hierarchy_spec_fingerprint(spec)
            normalized_expanded = self._normalize_expanded_paths(target_paths)
            cache_key = f"{spec_fingerprint}:{hashlib.sha256(self._stable_json(normalized_expanded).encode()).hexdigest()[:16]}"
            cache_lookup_started_at = time.perf_counter()
            cached_view = self._hierarchy_view_cache_get(cache_key)
            cache_lookup_finished_at = time.perf_counter()

            hierarchy_result: Dict[str, List[Dict[str, Any]]]
            visible_rows: List[Dict[str, Any]]
            hierarchy_load_started_at = None
            hierarchy_load_finished_at = None
            flatten_started_at = None
            flatten_finished_at = None
            reused_cache = False
            cache_hit = cached_view is not None
            cached_expanded_count = 0
            requested_expanded_set = set(normalized_expanded)

            if cached_view is not None:
                hierarchy_result = cached_view["hierarchy_result"]
                visible_rows = cached_view["visible_rows"]
            else:
                hierarchy_load_started_at = time.perf_counter()
                reusable_view = self._find_reusable_hierarchy_cache_entry(spec_fingerprint, requested_expanded_set)
                if reusable_view is not None:
                    reused_cache = True
                    hierarchy_result = reusable_view["hierarchy_result"]
                    cached_expanded_set = set(reusable_view.get("expanded_set") or set())
                    cached_expanded_count = len(cached_expanded_set)
                    missing_paths = [
                        list(path_tuple)
                        for path_tuple in sorted(requested_expanded_set - cached_expanded_set)
                    ]
                    if missing_paths:
                        delta_hierarchy = await self.run_hierarchical_pivot_batch_load(
                            spec.to_dict(), missing_paths, max_levels=len(spec.rows)
                        )
                        for parent_key, rows in (delta_hierarchy or {}).items():
                            hierarchy_result[str(parent_key)] = self._clone_hierarchy_rows(rows)
                    hierarchy_load_finished_at = time.perf_counter()
                    flatten_started_at = time.perf_counter()
                    visible_rows = self._flatten_hierarchy_rows(spec, hierarchy_result, target_paths)
                    flatten_finished_at = time.perf_counter()
                else:
                    hierarchy_result = await self.run_hierarchical_pivot_batch_load(
                        spec.to_dict(), target_paths, max_levels=len(spec.rows)
                    )
                    hierarchy_load_finished_at = time.perf_counter()
                    flatten_started_at = time.perf_counter()
                    visible_rows = self._flatten_hierarchy_rows(spec, hierarchy_result, target_paths)
                    flatten_finished_at = time.perf_counter()

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
            grand_total_formula_source_rows = [
                dict(row)
                for row in visible_rows
                if isinstance(row, dict) and not _is_grand_total(row) and row.get("depth") == 0
            ]

            # Compute color scale stats from ALL visible rows (excluding totals and row fields)
            color_scale_started_at = time.perf_counter()
            color_scale_stats = self._compute_color_scale_stats(
                visible_rows, spec.rows, getattr(spec, 'columns', [])
            )
            color_scale_finished_at = time.perf_counter()

            window_slice_started_at = time.perf_counter()
            if start_row is not None and end_row is not None:
                window_rows = visible_rows[start_row:end_row + 1]
            else:
                window_rows = visible_rows
            window_slice_finished_at = time.perf_counter()

            cache_store_started_at = time.perf_counter()
            self._hierarchy_view_cache_set(cache_key, {
                "spec_fingerprint": spec_fingerprint,
                "expanded_set": set(normalized_expanded),
                "hierarchy_result": hierarchy_result,
                "visible_rows": visible_rows,
                "grand_total_row": grand_total_row,
                "grand_total_formula_source_rows": grand_total_formula_source_rows,
            })
            cache_store_finished_at = time.perf_counter()

            return {
                "rows": self._clone_hierarchy_rows(window_rows),
                "total_rows": total_rows,
                "grand_total_row": (dict(grand_total_row) if isinstance(grand_total_row, dict) else grand_total_row) if include_grand_total_row else None,
                "grand_total_formula_source_rows": self._clone_hierarchy_rows(grand_total_formula_source_rows) if include_grand_total_row else None,
                "color_scale_stats": color_scale_stats,
                "profile": (
                    {
                        "controller": {
                            "operation": "hierarchy_view",
                            "path": "materialized_hierarchy",
                            "cacheHit": cache_hit,
                            "reusedCache": reused_cache,
                            "requestedExpandedCount": len(requested_expanded_set),
                            "cachedExpandedCount": cached_expanded_count,
                            "cacheLookupMs": self._profile_ms(cache_lookup_started_at, cache_lookup_finished_at),
                            "hierarchyLoadMs": self._profile_ms(hierarchy_load_started_at, hierarchy_load_finished_at),
                            "flattenMs": self._profile_ms(flatten_started_at, flatten_finished_at),
                            "colorScaleMs": self._profile_ms(color_scale_started_at, color_scale_finished_at),
                            "windowSliceMs": self._profile_ms(window_slice_started_at, window_slice_finished_at),
                            "cacheStoreMs": self._profile_ms(cache_store_started_at, cache_store_finished_at),
                            "totalMs": self._profile_ms(view_started_at, time.perf_counter()),
                            "returnedRows": len(window_rows),
                            "totalRows": total_rows,
                        }
                    }
                    if profiling
                    else None
                ),
            }

    async def run_hierarchical_progressive(self, spec: PivotSpec, expanded_paths: List[List[str]], level_callback: Optional[Callable] = None):
        """Run hierarchical data loading progressively by levels"""
        result = await self.progressive_loader.load_hierarchical_progressive(spec, expanded_paths, level_callback)
        return result

    async def run_pivot_async(
        self,
        spec: Any,
        return_format: str = "arrow",
        force_refresh: bool = False,
        profile_sink: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], pa.Table]:
        """Execute a pivot query asynchronously with all scalability features"""
        start_time = time.time()
        started_at = time.perf_counter()
        
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
            if self._can_use_sparse_materialized_pivot(spec):
                sparse_execution_profile: Dict[str, Any] = {}
                execute_started_at = time.perf_counter()
                result_table = await self._execute_sparse_materialized_pivot_async(
                    spec,
                    force_refresh,
                    profile_sink=sparse_execution_profile,
                )
                execute_finished_at = time.perf_counter()

                duration = time.time() - start_time
                if self.stats_tracker.record_query(spec, duration):
                    if not self._uses_duckdb_ibis():
                        self.task_manager.create_task(
                            self._trigger_materialization(spec),
                            name=f"smart_materialization_{spec.table}"
                        )

                if return_format == "dict":
                    conversion_started_at = time.perf_counter()
                    converted = self._convert_table_to_dict(result_table, spec)
                    conversion_finished_at = time.perf_counter()
                    if isinstance(profile_sink, dict):
                        profile_sink.clear()
                        profile_sink.update({
                            "planner": {
                                "planningMs": 0.0,
                                "executeMs": self._profile_ms(execute_started_at, execute_finished_at),
                                "convertMs": self._profile_ms(conversion_started_at, conversion_finished_at),
                                "totalMs": self._profile_ms(started_at, time.perf_counter()),
                                "needsColumnDiscovery": False,
                                "queryCount": 1,
                                "path": "sparse_materialized",
                            },
                            "plannerExecution": sparse_execution_profile,
                        })
                    return converted

                if isinstance(profile_sink, dict):
                    profile_sink.clear()
                    profile_sink.update({
                        "planner": {
                            "planningMs": 0.0,
                            "executeMs": self._profile_ms(execute_started_at, execute_finished_at),
                            "convertMs": 0.0,
                            "totalMs": self._profile_ms(started_at, time.perf_counter()),
                            "needsColumnDiscovery": False,
                            "queryCount": 1,
                            "path": "sparse_materialized",
                        },
                        "plannerExecution": sparse_execution_profile,
                    })
                return result_table

            # Planning can be CPU-bound for complex queries, so we offload it
            loop = asyncio.get_running_loop()
            
            # Removed lock wrapper
            def plan_with_lock():
                with self.planning_lock:
                    return self.planner.plan(spec)

            planning_started_at = time.perf_counter()
            plan_result = await loop.run_in_executor(None, plan_with_lock)
            planning_finished_at = time.perf_counter()
            
            metadata = plan_result.get("metadata", {})
            
            execute_started_at = time.perf_counter()
            if metadata.get("needs_column_discovery"):
                plan_execution_profile: Dict[str, Any] = {}
                result_table = await self._execute_topn_pivot_async(spec, plan_result, force_refresh, profile_sink=plan_execution_profile)
            else:
                plan_execution_profile = {}
                result_table = await self._execute_standard_pivot_async(spec, plan_result, force_refresh, profile_sink=plan_execution_profile)
            execute_finished_at = time.perf_counter()

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
                conversion_started_at = time.perf_counter()
                converted = self._convert_table_to_dict(result_table, spec)
                conversion_finished_at = time.perf_counter()
                if isinstance(profile_sink, dict):
                    profile_sink.clear()
                    profile_sink.update({
                        "planner": {
                            "planningMs": self._profile_ms(planning_started_at, planning_finished_at),
                            "executeMs": self._profile_ms(execute_started_at, execute_finished_at),
                            "convertMs": self._profile_ms(conversion_started_at, conversion_finished_at),
                            "totalMs": self._profile_ms(started_at, time.perf_counter()),
                            "needsColumnDiscovery": bool(metadata.get("needs_column_discovery")),
                            "queryCount": len(plan_result.get("queries", []) or []),
                        },
                        "plannerExecution": plan_execution_profile,
                    })
                return converted

            if isinstance(profile_sink, dict):
                profile_sink.clear()
                profile_sink.update({
                    "planner": {
                        "planningMs": self._profile_ms(planning_started_at, planning_finished_at),
                        "executeMs": self._profile_ms(execute_started_at, execute_finished_at),
                        "convertMs": 0.0,
                        "totalMs": self._profile_ms(started_at, time.perf_counter()),
                        "needsColumnDiscovery": bool(metadata.get("needs_column_discovery")),
                        "queryCount": len(plan_result.get("queries", []) or []),
                    },
                    "plannerExecution": plan_execution_profile,
                })
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

    async def _execute_standard_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool, profile_sink: Optional[Dict[str, Any]] = None) -> pa.Table:
        """Execute standard pivot asynchronously with parallel execution"""
        started_at = time.perf_counter()
        diff_plan_started_at = started_at
        queries_to_run, strategy = self.diff_engine.plan(plan_result, spec, force_refresh=force_refresh)
        diff_plan_finished_at = time.perf_counter()
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

        execute_queries_started_at = time.perf_counter()
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
        execute_queries_finished_at = time.perf_counter()

        main_result = results[0] if results else pa.table({}) if pa is not None else None

        metadata = plan_result.get("metadata", {})
        totals_started_at = None
        totals_finished_at = None
        if metadata.get("needs_totals", False) and main_result is not None and main_result.num_rows > 0:
            totals_started_at = time.perf_counter()
            main_result = self._append_totals_row(main_result, spec, metadata)
            totals_finished_at = time.perf_counter()

        finalize_started_at = time.perf_counter()
        final_table = self.diff_engine.merge_and_finalize([main_result] if main_result is not None else [], plan_result, spec, strategy)
        finalize_finished_at = time.perf_counter()
        if isinstance(profile_sink, dict):
            profile_sink.clear()
            profile_sink.update({
                "mode": "standard",
                "diffPlanMs": self._profile_ms(diff_plan_started_at, diff_plan_finished_at),
                "executeQueriesMs": self._profile_ms(execute_queries_started_at, execute_queries_finished_at),
                "totalsMs": self._profile_ms(totals_started_at, totals_finished_at),
                "finalizeMs": self._profile_ms(finalize_started_at, finalize_finished_at),
                "queryCount": len(queries_to_run),
                "cacheHits": strategy.get("cache_hits", 0),
                "cacheMisses": len(queries_to_run),
                "totalMs": self._profile_ms(started_at, time.perf_counter()),
            })

        return final_table if final_table is not None else pa.table({}) if pa is not None else None

    async def _execute_sparse_materialized_pivot_async(
        self,
        spec: PivotSpec,
        force_refresh: bool,
        profile_sink: Optional[Dict[str, Any]] = None,
    ) -> pa.Table:
        started_at = time.perf_counter()
        materialized_column_values = list(spec.pivot_config.materialized_column_values or [])
        cache_key = self._sparse_materialized_pivot_cache_key(spec, materialized_column_values)
        cached_table = self.cache.get(cache_key) if not force_refresh else None

        source_query_started_at = None
        source_query_finished_at = None
        reshape_started_at = None
        reshape_finished_at = None

        if cached_table is not None:
            result_table = cached_table
            pivot_cache_hit = True
            source_row_count = int(getattr(result_table, "num_rows", 0) or 0)
        else:
            pivot_cache_hit = False
            source_query_started_at = time.perf_counter()
            grouped_expr, hidden_sort_keys = self._build_sparse_materialized_source_query(spec, materialized_column_values)
            grouped_table = await self._execute_ibis_expr_async(grouped_expr)
            source_query_finished_at = time.perf_counter()
            source_row_count = int(getattr(grouped_table, "num_rows", 0) or 0)
            reshape_started_at = time.perf_counter()
            result_table = self._reshape_sparse_materialized_pivot(
                grouped_table,
                spec,
                materialized_column_values,
                hidden_sort_keys,
            )
            reshape_finished_at = time.perf_counter()
            self.cache.set(cache_key, result_table)

        totals_started_at = None
        totals_finished_at = None
        if getattr(spec, "totals", False) and result_table is not None and result_table.num_rows > 0:
            totals_started_at = time.perf_counter()
            metadata = {
                "measure_aggs": {
                    m.alias: (m.agg or "sum").lower()
                    for m in (spec.measures or [])
                    if not getattr(m, "ratio_numerator", None)
                }
            }
            result_table = self._append_totals_row(
                result_table,
                spec,
                metadata,
                column_values=materialized_column_values,
            )
            totals_finished_at = time.perf_counter()

        if isinstance(profile_sink, dict):
            profile_sink.clear()
            profile_sink.update({
                "mode": "sparse_materialized",
                "sourceQueryMs": self._profile_ms(source_query_started_at, source_query_finished_at),
                "reshapeMs": self._profile_ms(reshape_started_at, reshape_finished_at),
                "totalsMs": self._profile_ms(totals_started_at, totals_finished_at),
                "pivotCacheHit": pivot_cache_hit,
                "materializedColumns": len(materialized_column_values),
                "sourceRows": source_row_count,
                "resultRows": int(getattr(result_table, "num_rows", 0) or 0),
                "totalMs": self._profile_ms(started_at, time.perf_counter()),
            })

        return result_table if result_table is not None else pa.table({})

    async def _execute_topn_pivot_async(self, spec: Any, plan_result: Dict[str, Any], force_refresh: bool, profile_sink: Optional[Dict[str, Any]] = None) -> pa.Table:
        """Execute top-N pivot asynchronously"""
        started_at = time.perf_counter()
        queries = plan_result.get("queries", [])
        materialized_column_values = (
            spec.pivot_config.materialized_column_values
            if spec.pivot_config and spec.pivot_config.materialized_column_values is not None
            else None
        )

        column_discovery_started_at = time.perf_counter()
        if materialized_column_values is not None:
            column_values = list(materialized_column_values)
            column_cache_hit = True
            column_discovery_skipped = True
        else:
            col_ibis_expr = queries[0]
            col_cache_key = self._cache_key_for_query(col_ibis_expr, spec)
            cached_cols_table = self.cache.get(col_cache_key) if not force_refresh else None
            if cached_cols_table:
                column_values = cached_cols_table.column("_col_key").to_pylist()
                self._cache_hits += 1
                column_cache_hit = True
            else:
                col_results_table = await self._execute_ibis_expr_async(col_ibis_expr)
                column_values = col_results_table.column("_col_key").to_pylist()
                self.cache.set(col_cache_key, col_results_table)
                self._cache_misses += 1
                column_cache_hit = False
            materialized_column_values = column_values
            column_discovery_skipped = False
        column_discovery_finished_at = time.perf_counter()

        pivot_ibis_expr = self.planner.build_pivot_query_from_columns(spec, materialized_column_values)
        
        pivot_cache_key = self._cache_key_for_query(pivot_ibis_expr, spec)
        cached_pivot_table = self.cache.get(pivot_cache_key) if not force_refresh else None

        pivot_query_started_at = time.perf_counter()
        if cached_pivot_table:
            result_table = cached_pivot_table
            pivot_cache_hit = True
        else:
            pivot_results_table = await self._execute_ibis_expr_async(pivot_ibis_expr)
                
            self.cache.set(pivot_cache_key, pivot_results_table)
            result_table = pivot_results_table
            pivot_cache_hit = False
        pivot_query_finished_at = time.perf_counter()

        metadata = plan_result.get("metadata", {})
        totals_started_at = None
        totals_finished_at = None
        if metadata.get("needs_totals", False) and result_table is not None and result_table.num_rows > 0:
            totals_started_at = time.perf_counter()
            result_table = self._append_totals_row(result_table, spec, metadata, column_values=materialized_column_values)
            totals_finished_at = time.perf_counter()
        if isinstance(profile_sink, dict):
            profile_sink.clear()
            profile_sink.update({
                "mode": "topn",
                "columnDiscoveryMs": self._profile_ms(column_discovery_started_at, column_discovery_finished_at),
                "pivotQueryMs": self._profile_ms(pivot_query_started_at, pivot_query_finished_at),
                "totalsMs": self._profile_ms(totals_started_at, totals_finished_at),
                "columnCacheHit": column_cache_hit,
                "columnDiscoverySkipped": column_discovery_skipped,
                "pivotCacheHit": pivot_cache_hit,
                "materializedColumns": len(materialized_column_values or []),
                "totalMs": self._profile_ms(started_at, time.perf_counter()),
            })

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

    def _get_table_columns(self, table_name: str) -> List[str]:
        table_expr = self.planner.con.table(table_name)
        return list(getattr(table_expr, "columns", []) or [])

    @staticmethod
    def _sanitize_column_mapping(
        source: Optional[Dict[str, Any]],
        *,
        allowed_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if not isinstance(source, dict):
            return {}
        allowed = set(allowed_columns or [])
        excluded = set(exclude_columns or [])
        sanitized: Dict[str, Any] = {}
        for column, value in source.items():
            column_name = str(column or "")
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", column_name):
                continue
            if allowed and column_name not in allowed:
                continue
            if column_name in excluded:
                continue
            sanitized[column_name] = value
        return sanitized

    @staticmethod
    def _build_where_clause(key_columns: Dict[str, Any]) -> tuple[str, List[Any]]:
        where_parts = []
        params: List[Any] = []
        for col, val in key_columns.items():
            if val is None:
                where_parts.append(f"{col} IS NULL")
            else:
                where_parts.append(f"{col} = ?")
                params.append(val)
        return " AND ".join(where_parts), params

    def _count_matching_rows_sync(self, table_name: str, key_columns: Dict[str, Any]) -> int:
        table_expr = self.planner.con.table(table_name)
        filtered = table_expr
        for column, value in key_columns.items():
            if value is None:
                filtered = filtered.filter(filtered[column].isnull())
            else:
                filtered = filtered.filter(filtered[column] == value)
        return int(filtered.count().execute())

    def _fetch_matching_rows_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        table_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        table_expr = self.planner.con.table(table_name)
        filtered = table_expr
        for column, value in key_columns.items():
            if value is None:
                filtered = filtered.filter(filtered[column].isnull())
            else:
                filtered = filtered.filter(filtered[column] == value)
        result = filtered.execute()
        records = result.to_dict("records")
        if not table_columns:
            return records
        ordered_records: List[Dict[str, Any]] = []
        for row in records:
            if not isinstance(row, dict):
                continue
            ordered_records.append({
                column: row.get(column)
                for column in table_columns
                if column in row
            })
        return ordered_records

    def _fetch_matching_rows_with_rowid_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        table_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        selected_columns = [
            str(column)
            for column in (table_columns or self._get_table_columns(table_name) or [])
            if isinstance(column, str) and column and column != "__rowid__"
        ]
        select_list = ", ".join(["rowid AS __rowid__", *selected_columns]) if selected_columns else "rowid AS __rowid__"
        where_sql, where_params = self._build_where_clause(key_columns)
        rows = self._execute_parameterized_fetchall(
            f"SELECT {select_list} FROM {table_name} WHERE {where_sql}",
            where_params,
        )
        records: List[Dict[str, Any]] = []
        expected_columns = ["__rowid__", *selected_columns]
        for row in rows:
            if isinstance(row, dict):
                records.append({
                    key: row.get(key)
                    for key in expected_columns
                    if key in row
                })
                continue
            if isinstance(row, (list, tuple)):
                records.append({
                    expected_columns[index]: row[index]
                    for index in range(min(len(expected_columns), len(row)))
                })
        return records

    @staticmethod
    def _resolve_grouping_scope_paths_for_row(row: Dict[str, Any], grouping_fields: List[str]) -> List[str]:
        if not isinstance(row, dict) or not grouping_fields:
            return []
        parts: List[str] = []
        scope_paths: List[str] = []
        for field in grouping_fields:
            if field not in row or row.get(field) is None:
                break
            parts.append(str(row.get(field)))
            scope_paths.append("|||".join(parts))
        return scope_paths

    @staticmethod
    def _new_scope_value_accumulator() -> Dict[str, Any]:
        return {
            "before_sum": 0.0,
            "after_sum": 0.0,
            "before_count": 0,
            "after_count": 0,
            "before_weighted_sum": 0.0,
            "after_weighted_sum": 0.0,
            "before_weight_total": 0.0,
            "after_weight_total": 0.0,
            "before_min": None,
            "after_min": None,
            "before_max": None,
            "after_max": None,
        }

    def _accumulate_scope_value(
        self,
        accumulator: Dict[str, Any],
        row: Dict[str, Any],
        *,
        target_column: str,
        aggregation_fn: str,
        weight_field: Optional[str],
        side: str,
    ) -> None:
        numeric_value = self._coerce_numeric_value(row.get(target_column) if isinstance(row, dict) else None)
        if numeric_value is None:
            return
        if aggregation_fn == "sum":
            accumulator[f"{side}_sum"] += numeric_value
            return
        if aggregation_fn == "avg":
            accumulator[f"{side}_sum"] += numeric_value
            accumulator[f"{side}_count"] += 1
            return
        if aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"}:
            weight_value = self._coerce_numeric_value(row.get(weight_field) if isinstance(row, dict) else None)
            if weight_value is None:
                return
            accumulator[f"{side}_weighted_sum"] += numeric_value * weight_value
            accumulator[f"{side}_weight_total"] += weight_value
            return
        if aggregation_fn == "min":
            current_value = accumulator[f"{side}_min"]
            accumulator[f"{side}_min"] = numeric_value if current_value is None else min(current_value, numeric_value)
            return
        if aggregation_fn == "max":
            current_value = accumulator[f"{side}_max"]
            accumulator[f"{side}_max"] = numeric_value if current_value is None else max(current_value, numeric_value)

    @staticmethod
    def _finalize_scope_value_accumulator(accumulator: Dict[str, Any], aggregation_fn: str, side: str) -> Optional[float]:
        if aggregation_fn == "sum":
            return float(accumulator[f"{side}_sum"])
        if aggregation_fn == "avg":
            count = int(accumulator[f"{side}_count"] or 0)
            return (float(accumulator[f"{side}_sum"]) / count) if count > 0 else None
        if aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"}:
            weight_total = float(accumulator[f"{side}_weight_total"] or 0.0)
            if math.isclose(weight_total, 0.0, abs_tol=1e-12):
                return None
            return float(accumulator[f"{side}_weighted_sum"]) / weight_total
        if aggregation_fn == "min":
            return accumulator[f"{side}_min"]
        if aggregation_fn == "max":
            return accumulator[f"{side}_max"]
        return None

    def _build_scope_value_changes_from_row_pairs(
        self,
        row_pairs: List[Dict[str, Dict[str, Any]]],
        *,
        grouping_fields: List[str],
        direct_scope_id: Optional[str],
        measure_id: str,
        target_column: str,
        aggregation_fn: str,
        weight_field: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not row_pairs or not measure_id or not target_column or not aggregation_fn:
            return []
        scope_accumulators: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        normalized_direct_scope = str(direct_scope_id or "").strip()
        for row_pair in row_pairs:
            before_row = row_pair.get("before_row") if isinstance(row_pair, dict) else None
            after_row = row_pair.get("after_row") if isinstance(row_pair, dict) else None
            row_source = before_row if isinstance(before_row, dict) else after_row if isinstance(after_row, dict) else None
            if not isinstance(row_source, dict):
                continue
            scope_paths = self._resolve_grouping_scope_paths_for_row(row_source, grouping_fields)
            scoped_targets = ["__grand_total__", *scope_paths] if scope_paths else ([normalized_direct_scope] if normalized_direct_scope else [])
            for scope_id in list(dict.fromkeys([scope for scope in scoped_targets if scope])):
                accumulator = scope_accumulators.setdefault(scope_id, self._new_scope_value_accumulator())
                if isinstance(before_row, dict):
                    self._accumulate_scope_value(
                        accumulator,
                        before_row,
                        target_column=target_column,
                        aggregation_fn=aggregation_fn,
                        weight_field=weight_field,
                        side="before",
                    )
                if isinstance(after_row, dict):
                    self._accumulate_scope_value(
                        accumulator,
                        after_row,
                        target_column=target_column,
                        aggregation_fn=aggregation_fn,
                        weight_field=weight_field,
                        side="after",
                    )

        scope_value_changes: List[Dict[str, Any]] = []
        for scope_id, accumulator in scope_accumulators.items():
            before_value = self._finalize_scope_value_accumulator(accumulator, aggregation_fn, "before")
            after_value = self._finalize_scope_value_accumulator(accumulator, aggregation_fn, "after")
            if before_value is None or after_value is None:
                continue
            if math.isclose(float(before_value), float(after_value), rel_tol=1e-9, abs_tol=1e-9):
                continue
            scope_value_changes.append({
                "scopeId": scope_id,
                "measureId": measure_id,
                "beforeValue": before_value,
                "afterValue": after_value,
                "role": "direct" if scope_id == normalized_direct_scope else "propagated",
                "aggregationFn": aggregation_fn,
            })
        return scope_value_changes

    def _build_direct_scope_value_changes(
        self,
        before_rows: List[Dict[str, Any]],
        *,
        row_path: Optional[str],
        measure_id: str,
        target_column: str,
        next_value: Any,
    ) -> List[Dict[str, Any]]:
        if not before_rows or not row_path or not measure_id or not target_column:
            return []
        inverse_values = self._build_inverse_update_values(before_rows, [target_column]) or {}
        before_value = inverse_values.get(target_column)
        if before_value is None or before_value == next_value:
            return []
        return [{
            "scopeId": str(row_path),
            "measureId": str(measure_id),
            "beforeValue": before_value,
            "afterValue": next_value,
            "role": "direct",
            "aggregationFn": "direct",
        }]

    @staticmethod
    def _new_history_transaction() -> Dict[str, List[Dict[str, Any]]]:
        return {
            "add": [],
            "remove": [],
            "update": [],
            "upsert": [],
        }

    @staticmethod
    def _compact_history_transaction(transaction: Dict[str, List[Dict[str, Any]]]) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        compacted = {
            kind: list(entries or [])
            for kind, entries in (transaction or {}).items()
            if isinstance(entries, list) and entries
        }
        return compacted or None

    @staticmethod
    def _build_inverse_update_values(before_rows: List[Dict[str, Any]], update_columns: List[str]) -> Optional[Dict[str, Any]]:
        if not before_rows or not update_columns:
            return None
        inverse_values: Dict[str, Any] = {}
        for column in update_columns:
            sentinel = object()
            current_value = sentinel
            for row in before_rows:
                if not isinstance(row, dict):
                    return None
                row_value = row.get(column)
                if current_value is sentinel:
                    current_value = row_value
                    continue
                if row_value != current_value:
                    return None
            if current_value is not sentinel:
                inverse_values[column] = current_value
        return inverse_values or None

    @staticmethod
    def _normalize_aggregation_name(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _normalize_aggregate_propagation_strategy(value: Any) -> str:
        normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        if normalized in {"none", "skip", "parent_only"}:
            return "none"
        if normalized in {"", "equal", "even", "default", "delta", "uniform"}:
            return "equal"
        if normalized in {"proportional", "ratio", "scale", "scaled"}:
            return "proportional"
        return "equal"

    @staticmethod
    def _coerce_numeric_value(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, decimal.Decimal):
            return float(value)
        if isinstance(value, (int, float)):
            if isinstance(value, float) and math.isnan(value):
                return None
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                parsed = float(stripped)
            except ValueError:
                return None
            return None if math.isnan(parsed) else parsed
        return None

    @staticmethod
    def _coerce_number_for_storage(original_value: Any, numeric_value: float) -> Any:
        if isinstance(original_value, decimal.Decimal):
            return decimal.Decimal(str(numeric_value))
        if isinstance(original_value, int) and not isinstance(original_value, bool):
            rounded = round(numeric_value)
            if math.isclose(numeric_value, rounded, rel_tol=1e-9, abs_tol=1e-9):
                return int(rounded)
        return numeric_value

    def _get_table_column_dtype(self, table_name: str, column_name: str) -> Any:
        schema = self._get_table_schema(table_name)
        if schema is None:
            return None
        try:
            return schema[column_name]
        except Exception:
            return None

    @staticmethod
    def _is_integer_dtype(dtype: Any) -> bool:
        return bool(callable(getattr(dtype, "is_integer", None)) and dtype.is_integer())

    def _execute_parameterized_fetchone(self, sql: str, params: List[Any]) -> Any:
        con = self.planner.con
        if hasattr(con, "con"):
            result = con.con.execute(sql, [*params])
        elif hasattr(con, "execute"):
            result = con.execute(sql, [*params])
        else:
            raise NotImplementedError("Backend does not support parameterized SQL queries")

        if hasattr(result, "fetchone"):
            return result.fetchone()
        if hasattr(result, "fetchall"):
            rows = result.fetchall()
            return rows[0] if rows else None
        if isinstance(result, list):
            return result[0] if result else None
        return result

    def _execute_parameterized_fetchall(self, sql: str, params: List[Any]) -> List[Any]:
        con = self.planner.con
        if hasattr(con, "con"):
            result = con.con.execute(sql, [*params])
        elif hasattr(con, "execute"):
            result = con.execute(sql, [*params])
        else:
            raise NotImplementedError("Backend does not support parameterized SQL queries")

        if hasattr(result, "fetchall"):
            rows = result.fetchall()
        elif isinstance(result, list):
            rows = result
        else:
            rows = []

        description = getattr(result, "description", None) or []
        columns = [column[0] for column in description] if description else []
        if columns and rows and not isinstance(rows[0], dict):
            return [
                {
                    columns[index]: row[index]
                    for index in range(min(len(columns), len(row)))
                }
                for row in rows
                if isinstance(row, (list, tuple))
            ]
        return rows

    def _fetch_aggregate_edit_summary_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        aggregate_edit: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        target_column = str(aggregate_edit.get("column") or "").strip()
        aggregation_fn = self._normalize_aggregation_name(aggregate_edit.get("aggregationFn"))
        weight_field = str(aggregate_edit.get("weightField") or "").strip()

        if not target_column.isidentifier():
            return None
        if aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"} and not weight_field.isidentifier():
            return None

        where_sql, where_params = self._build_where_clause(key_columns)
        predicates = [predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate]
        predicate_sql = " AND ".join(predicates) if predicates else "TRUE"

        current_value_sql = ""
        if aggregation_fn == "sum":
            current_value_sql = f"SUM({target_column})"
        elif aggregation_fn == "avg":
            current_value_sql = f"AVG({target_column})"
        elif aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"}:
            current_value_sql = (
                f"SUM(CASE WHEN {weight_field} IS NOT NULL THEN {target_column} * {weight_field} ELSE NULL END)"
                f" / NULLIF(SUM(CASE WHEN {weight_field} IS NOT NULL THEN {weight_field} ELSE NULL END), 0)"
            )
        else:
            return None

        row = self._execute_parameterized_fetchone(
            (
                f"SELECT COUNT({target_column}) AS matched_row_count, "
                f"{current_value_sql} AS current_value "
                f"FROM {table_name} "
                f"WHERE {predicate_sql}"
            ),
            where_params,
        )
        if not row:
            return None

        matched_row_count = 0
        current_value = None
        if isinstance(row, dict):
            matched_row_count = int(row.get("matched_row_count") or 0)
            current_value = self._coerce_numeric_value(row.get("current_value"))
        elif isinstance(row, (list, tuple)):
            matched_row_count = int(row[0] or 0)
            current_value = self._coerce_numeric_value(row[1] if len(row) > 1 else None)

        return {
            "matchedRowCount": matched_row_count,
            "currentValue": current_value,
        }

    @staticmethod
    def _build_history_aggregate_update_entry(
        aggregate_edit: Dict[str, Any],
        *,
        value: Any,
        old_value: Any,
    ) -> Optional[Dict[str, Any]]:
        row_id = aggregate_edit.get("rowId")
        col_id = aggregate_edit.get("columnId")
        if row_id is None or col_id is None:
            return None
        return {
            "rowId": str(row_id),
            "colId": str(col_id),
            "value": value,
            "oldValue": old_value,
        }

    def _apply_integer_delta_distribution_sync(
        self,
        table_name: str,
        target_column: str,
        where_sql: str,
        where_params: List[Any],
        matched_row_count: int,
        total_delta: int,
    ) -> int:
        if matched_row_count <= 0 or total_delta == 0:
            return 0

        base_delta = int(total_delta / matched_row_count)
        remainder = int(total_delta - (base_delta * matched_row_count))
        remainder_count = abs(remainder)
        remainder_step = 1 if remainder > 0 else -1 if remainder < 0 else 0
        numeric_where = " AND ".join(
            predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate
        ) or f"{target_column} IS NOT NULL"

        if remainder_count == 0:
            self._execute_parameterized_mutation(
                f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",
                [base_delta, *where_params],
            )
            return matched_row_count if base_delta != 0 else 0

        self._execute_parameterized_mutation(
            (
                f"WITH target AS ("
                f"SELECT rowid, row_number() OVER (ORDER BY rowid) AS rn "
                f"FROM {table_name} "
                f"WHERE {numeric_where}"
                f") "
                f"UPDATE {table_name} "
                f"SET {target_column} = {target_column} + ? + CASE WHEN target.rn <= ? THEN ? ELSE 0 END "
                f"FROM target "
                f"WHERE {table_name}.rowid = target.rowid"
            ),
            [*where_params, base_delta, remainder_count, remainder_step],
        )
        return matched_row_count if base_delta != 0 else remainder_count

    def _execute_parameterized_fetchall(self, sql: str, params: List[Any]) -> list:
        con = self.planner.con
        if hasattr(con, "con"):
            result = con.con.execute(sql, [*params])
        elif hasattr(con, "execute"):
            result = con.execute(sql, [*params])
        else:
            raise NotImplementedError("Backend does not support parameterized SQL queries")
        if hasattr(result, "fetchall"):
            return result.fetchall()
        if isinstance(result, list):
            return result
        return []

    def _apply_integer_proportional_sync(
        self,
        table_name: str,
        target_column: str,
        where_sql: str,
        where_params: List[Any],
        matched_row_count: int,
        scale_factor: float,
        target_sum: float,
    ) -> int:
        """Scale integer rows proportionally with remainder correction."""
        if matched_row_count <= 0 or math.isclose(scale_factor, 1.0, rel_tol=1e-12):
            return 0
        numeric_where = " AND ".join(
            predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate
        ) or f"{target_column} IS NOT NULL"
        rows = self._execute_parameterized_fetchall(
            f"SELECT rowid, {target_column} AS val FROM {table_name} WHERE {numeric_where}",
            where_params,
        )
        if not rows:
            return 0
        scaled = []
        for row in rows:
            rid = row["rowid"] if isinstance(row, dict) else row[0]
            val = row["val"] if isinstance(row, dict) else row[1]
            if val is None:
                continue
            new_val = round(float(val) * scale_factor)
            scaled.append((rid, new_val))
        if not scaled:
            return 0
        rounded_sum = sum(v for _, v in scaled)
        remainder = int(round(target_sum)) - int(rounded_sum)
        if remainder != 0:
            step = 1 if remainder > 0 else -1
            for i in range(abs(remainder)):
                idx = i % len(scaled)
                rid, val = scaled[idx]
                scaled[idx] = (rid, val + step)
        updated = 0
        for rid, new_val in scaled:
            self._execute_parameterized_mutation(
                f"UPDATE {table_name} SET {target_column} = ? WHERE rowid = ?",
                [int(new_val), rid],
            )
            updated += 1
        return updated

    def _apply_set_based_aggregate_edit_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        aggregate_edit: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        target_column = str(aggregate_edit.get("column") or "").strip()
        aggregation_fn = self._normalize_aggregation_name(aggregate_edit.get("aggregationFn"))
        next_value = self._coerce_numeric_value(aggregate_edit.get("newValue"))

        if (
            not target_column.isidentifier()
            or not aggregation_fn
            or next_value is None
            or aggregate_edit.get("rowId") is None
            or not str(aggregate_edit.get("columnId") or "").strip()
        ):
            return None

        if aggregation_fn not in {"sum", "avg", "weighted_avg", "wavg", "weighted_mean"}:
            return None

        summary = self._fetch_aggregate_edit_summary_sync(table_name, key_columns, aggregate_edit)
        if not isinstance(summary, dict):
            return None

        matched_row_count = int(summary.get("matchedRowCount") or 0)
        current_value = self._coerce_numeric_value(summary.get("currentValue"))
        if matched_row_count <= 0:
            return {"warning": "Skipped aggregate edit because no editable source rows were found."}
        if current_value is None:
            return {"warning": "Skipped aggregate edit because the source rows were not numeric."}

        where_sql, where_params = self._build_where_clause(key_columns)
        numeric_where = " AND ".join(
            predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate
        ) or f"{target_column} IS NOT NULL"

        if math.isclose(next_value, current_value, rel_tol=1e-9, abs_tol=1e-9):
            return {
                "appliedCount": 0,
                "propagation": {
                    "mode": "aggregate",
                    "aggregationFn": aggregation_fn,
                    "targetColumn": target_column,
                    "weightField": aggregate_edit.get("weightField"),
                    "strategy": "no_op",
                    "execution": "set_based_sql",
                    "matchedRowCount": matched_row_count,
                    "updatedRowCount": 0,
                    "fromValue": current_value,
                    "toValue": next_value,
                    "columnId": aggregate_edit.get("columnId"),
                },
            }

        target_dtype = self._get_table_column_dtype(table_name, target_column)
        integer_storage = self._is_integer_dtype(target_dtype)
        updated_row_count = 0
        strategy = ""
        propagation_strategy = self._normalize_aggregate_propagation_strategy(
            aggregate_edit.get("propagationStrategy") or aggregate_edit.get("propagationFormula")
        )

        if propagation_strategy == "none":
            return {
                "warning": (
                    "Aggregate propagation policy 'none' is no longer supported for persisted edits."
                )
            }

        if aggregation_fn == "sum":
            if propagation_strategy == "proportional":
                if math.isclose(current_value, 0.0, rel_tol=1e-9, abs_tol=1e-9):
                    return {
                        "warning": (
                            "Skipped aggregate edit because proportional propagation requires a non-zero current value."
                        )
                    }
                scale_factor = next_value / current_value
                if integer_storage:
                    updated_row_count = self._apply_integer_proportional_sync(
                        table_name, target_column, where_sql, where_params,
                        matched_row_count, scale_factor, next_value,
                    )
                else:
                    self._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} * ? WHERE {numeric_where}",
                        [scale_factor, *where_params],
                    )
                    updated_row_count = matched_row_count
                strategy = "proportional"
            else:
                total_delta = next_value - current_value
                if integer_storage:
                    rounded_total_delta = round(total_delta)
                    if not math.isclose(total_delta, rounded_total_delta, rel_tol=1e-9, abs_tol=1e-9):
                        return {
                            "warning": (
                                "Skipped aggregate edit because integer source rows cannot exactly represent "
                                "the requested sum change."
                            )
                        }
                    updated_row_count = self._apply_integer_delta_distribution_sync(
                        table_name,
                        target_column,
                        where_sql,
                        where_params,
                        matched_row_count,
                        int(rounded_total_delta),
                    )
                    strategy = "balanced_delta"
                else:
                    delta_per_row = total_delta / matched_row_count
                    self._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",
                        [delta_per_row, *where_params],
                    )
                    updated_row_count = matched_row_count
                    strategy = "equal_delta"
        elif aggregation_fn == "avg":
            if propagation_strategy == "proportional":
                if math.isclose(current_value, 0.0, rel_tol=1e-9, abs_tol=1e-9):
                    return {
                        "warning": (
                            "Skipped aggregate edit because proportional propagation requires a non-zero current value."
                        )
                    }
                scale_factor = next_value / current_value
                if integer_storage:
                    # For avg, target_sum = next_value * matched_row_count
                    updated_row_count = self._apply_integer_proportional_sync(
                        table_name, target_column, where_sql, where_params,
                        matched_row_count, scale_factor, next_value * matched_row_count,
                    )
                else:
                    self._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} * ? WHERE {numeric_where}",
                        [scale_factor, *where_params],
                    )
                    updated_row_count = matched_row_count
                strategy = "proportional"
            else:
                if integer_storage:
                    total_delta = (next_value - current_value) * matched_row_count
                    rounded_total_delta = round(total_delta)
                    if not math.isclose(total_delta, rounded_total_delta, rel_tol=1e-9, abs_tol=1e-9):
                        return {
                            "warning": (
                                "Skipped aggregate edit because integer source rows cannot exactly represent "
                                "the requested average change."
                            )
                        }
                    updated_row_count = self._apply_integer_delta_distribution_sync(
                        table_name,
                        target_column,
                        where_sql,
                        where_params,
                        matched_row_count,
                        int(rounded_total_delta),
                    )
                    strategy = "balanced_shift"
                else:
                    delta_per_row = next_value - current_value
                    self._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",
                        [delta_per_row, *where_params],
                    )
                    updated_row_count = matched_row_count
                    strategy = "uniform_shift"
        else:
            if propagation_strategy == "proportional":
                if math.isclose(current_value, 0.0, rel_tol=1e-9, abs_tol=1e-9):
                    return {
                        "warning": (
                            "Skipped aggregate edit because proportional propagation requires a non-zero current value."
                        )
                    }
                scale_factor = next_value / current_value
                if integer_storage:
                    updated_row_count = self._apply_integer_proportional_sync(
                        table_name, target_column, where_sql, where_params,
                        matched_row_count, scale_factor, next_value * matched_row_count,
                    )
                else:
                    self._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} * ? WHERE {numeric_where}",
                        [scale_factor, *where_params],
                    )
                    updated_row_count = matched_row_count
                strategy = "proportional"
            else:
                delta_per_row = next_value - current_value
                if integer_storage:
                    rounded_delta = round(delta_per_row)
                    if not math.isclose(delta_per_row, rounded_delta, rel_tol=1e-9, abs_tol=1e-9):
                        return {
                            "warning": (
                                "Skipped aggregate edit because integer source rows cannot exactly represent "
                                "the requested weighted-average change."
                            )
                        }
                    delta_per_row = int(rounded_delta)
                self._execute_parameterized_mutation(
                    f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",
                    [delta_per_row, *where_params],
                )
                updated_row_count = matched_row_count
                strategy = "uniform_shift"

        return {
            "appliedCount": updated_row_count,
            "propagation": {
                "mode": "aggregate",
                "aggregationFn": aggregation_fn,
                "targetColumn": target_column,
                "weightField": aggregate_edit.get("weightField"),
                "strategy": strategy,
                "execution": "set_based_sql",
                "matchedRowCount": matched_row_count,
                "updatedRowCount": updated_row_count,
                "fromValue": current_value,
                "toValue": next_value,
                "columnId": aggregate_edit.get("columnId"),
            },
            "inverseUpdate": self._build_history_aggregate_update_entry(
                aggregate_edit,
                value=current_value,
                old_value=next_value,
            ),
            "redoUpdate": self._build_history_aggregate_update_entry(
                aggregate_edit,
                value=next_value,
                old_value=current_value,
            ),
        }

    def _compute_aggregate_edit_value(
        self,
        rows: List[Dict[str, Any]],
        column: str,
        aggregation_fn: str,
        weight_field: Optional[str] = None,
    ) -> Optional[float]:
        normalized_agg = self._normalize_aggregation_name(aggregation_fn)
        numeric_rows = []
        weighted_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            value = self._coerce_numeric_value(row.get(column))
            if value is None:
                continue
            numeric_rows.append(value)
            if weight_field:
                weight_value = self._coerce_numeric_value(row.get(weight_field))
                if weight_value is not None:
                    weighted_rows.append((value, weight_value))
        if normalized_agg == "sum":
            return float(sum(numeric_rows)) if numeric_rows else None
        if normalized_agg == "avg":
            return (float(sum(numeric_rows)) / len(numeric_rows)) if numeric_rows else None
        if normalized_agg in {"weighted_avg", "wavg", "weighted_mean"}:
            total_weight = sum(weight for _, weight in weighted_rows)
            if not weighted_rows or math.isclose(total_weight, 0.0, abs_tol=1e-12):
                return None
            return float(sum(value * weight for value, weight in weighted_rows) / total_weight)
        if normalized_agg == "min":
            return min(numeric_rows) if numeric_rows else None
        if normalized_agg == "max":
            return max(numeric_rows) if numeric_rows else None
        return None

    def _plan_aggregate_edit_rewrites(
        self,
        before_rows: List[Dict[str, Any]],
        aggregate_edit: Dict[str, Any],
    ) -> tuple[List[Dict[str, Dict[str, Any]]], Dict[str, Any]]:
        target_column = str(aggregate_edit.get("column") or "").strip()
        aggregation_fn = self._normalize_aggregation_name(aggregate_edit.get("aggregationFn"))
        weight_field = aggregate_edit.get("weightField")
        next_value = self._coerce_numeric_value(aggregate_edit.get("newValue"))
        if not target_column or not aggregation_fn:
            raise ValueError("Skipped aggregate edit without a valid target column or aggregation.")
        if next_value is None:
            raise ValueError("Skipped aggregate edit because the submitted value was not numeric.")

        current_value = self._compute_aggregate_edit_value(
            before_rows,
            target_column,
            aggregation_fn,
            weight_field=weight_field,
        )
        if current_value is None:
            raise ValueError("Skipped aggregate edit because the source rows were not numeric.")

        numeric_rows = []
        for row in before_rows:
            if not isinstance(row, dict):
                continue
            current_row_value = self._coerce_numeric_value(row.get(target_column))
            if current_row_value is None:
                continue
            numeric_rows.append(
                {
                    "before_row": dict(row),
                    "current_value": current_row_value,
                }
            )
        if not numeric_rows:
            raise ValueError("Skipped aggregate edit because no editable source rows were found.")

        changed_rows: List[Dict[str, Dict[str, Any]]] = []
        strategy = ""

        if aggregation_fn == "sum":
            strategy = "equal_delta"
            delta_per_row = (next_value - current_value) / len(numeric_rows)
            for entry in numeric_rows:
                updated_numeric = entry["current_value"] + delta_per_row
                after_row = dict(entry["before_row"])
                after_row[target_column] = self._coerce_number_for_storage(
                    entry["before_row"].get(target_column),
                    updated_numeric,
                )
                changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        elif aggregation_fn in {"avg", "weighted_avg", "wavg", "weighted_mean"}:
            strategy = "uniform_shift"
            delta_per_row = next_value - current_value
            for entry in numeric_rows:
                updated_numeric = entry["current_value"] + delta_per_row
                after_row = dict(entry["before_row"])
                after_row[target_column] = self._coerce_number_for_storage(
                    entry["before_row"].get(target_column),
                    updated_numeric,
                )
                changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        elif aggregation_fn == "min":
            if next_value >= current_value:
                strategy = "raise_floor"
                for entry in numeric_rows:
                    if entry["current_value"] < next_value:
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
            else:
                strategy = "lower_floor"
                for entry in numeric_rows:
                    if math.isclose(entry["current_value"], current_value, rel_tol=1e-9, abs_tol=1e-9):
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        elif aggregation_fn == "max":
            if next_value <= current_value:
                strategy = "lower_ceiling"
                for entry in numeric_rows:
                    if entry["current_value"] > next_value:
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
            else:
                strategy = "raise_ceiling"
                for entry in numeric_rows:
                    if math.isclose(entry["current_value"], current_value, rel_tol=1e-9, abs_tol=1e-9):
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        else:
            raise ValueError(f"Skipped aggregate edit because aggregation '{aggregation_fn}' is not supported.")

        filtered_changes = [
            row_change
            for row_change in changed_rows
            if row_change["before_row"].get(target_column) != row_change["after_row"].get(target_column)
        ]

        return filtered_changes, {
            "mode": "aggregate",
            "aggregationFn": aggregation_fn,
            "targetColumn": target_column,
            "weightField": weight_field,
            "strategy": strategy,
            "matchedRowCount": len(numeric_rows),
            "updatedRowCount": len(filtered_changes),
            "fromValue": current_value,
            "toValue": next_value,
            "columnId": aggregate_edit.get("columnId"),
        }

    def _build_insert_statement(self, table_name: str, row_data: Dict[str, Any], table_columns: List[str]) -> Optional[tuple[str, List[Any]]]:
        sanitized_row = self._sanitize_column_mapping(row_data, allowed_columns=table_columns)
        if not sanitized_row:
            return None
        insert_columns = [column for column in table_columns if column in sanitized_row]
        if not insert_columns:
            return None
        placeholders = ", ".join(["?"] * len(insert_columns))
        sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})"
        return sql, [sanitized_row[column] for column in insert_columns]

    async def apply_row_transaction(self, table_name: str, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Apply an atomic add/remove/update/upsert transaction and return a structured result."""
        if not table_name.isidentifier():
            raise ValueError("Invalid table name")

        table_columns = self._get_table_columns(table_name)
        add_operations = [
            operation
            for operation in (transaction.get("add") or [])
            if isinstance(operation, dict)
        ]
        remove_operations = [
            operation
            for operation in (transaction.get("remove") or [])
            if isinstance(operation, dict)
        ]
        update_operations = [
            operation
            for operation in (transaction.get("update") or [])
            if isinstance(operation, dict)
        ]
        upsert_operations = [
            operation
            for operation in (transaction.get("upsert") or [])
            if isinstance(operation, dict)
        ]
        requested = {
            "add": len(add_operations),
            "remove": len(remove_operations),
            "update": len(update_operations),
            "upsert": len(upsert_operations),
        }

        loop = asyncio.get_running_loop()

        def execute_transaction() -> Dict[str, Any]:
            applied = {
                "add": 0,
                "remove": 0,
                "update": 0,
                "upsertInserted": 0,
                "upsertUpdated": 0,
            }
            warnings: List[str] = []
            history_warnings: List[str] = []
            propagation_events: List[Dict[str, Any]] = []
            history_captureable = True
            inverse_transaction = self._new_history_transaction()
            redo_transaction = self._new_history_transaction()
            scope_value_changes: List[Dict[str, Any]] = []
            transaction_ctx = self.backend.transaction() if getattr(self, "backend", None) and hasattr(self.backend, "transaction") else nullcontext()

            with transaction_ctx:
                for operation in remove_operations:
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    if not key_columns:
                        warnings.append("Skipped remove operation without valid key columns.")
                        continue
                    matched_rows = self._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)
                    if not matched_rows:
                        continue
                    where_sql, where_params = self._build_where_clause(key_columns)
                    self._execute_parameterized_mutation(
                        f"DELETE FROM {table_name} WHERE {where_sql}",
                        where_params,
                    )
                    applied["remove"] += len(matched_rows)
                    redo_transaction["remove"].append({"keys": dict(key_columns)})
                    inverse_transaction["add"].extend([dict(row) for row in matched_rows if isinstance(row, dict)])

                for operation in update_operations:
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    aggregate_edit = operation.get("aggregate_edit") if isinstance(operation.get("aggregate_edit"), dict) else None
                    edit_meta = operation.get("edit_meta") if isinstance(operation.get("edit_meta"), dict) else {}
                    grouping_fields = [
                        str(field)
                        for field in (edit_meta.get("groupingFields") or [])
                        if isinstance(field, str) and field
                    ]
                    if aggregate_edit:
                        if not key_columns:
                            warnings.append("Skipped aggregate edit without valid row or pivot keys.")
                            continue
                        before_rows_with_rowid = self._fetch_matching_rows_with_rowid_sync(
                            table_name,
                            key_columns,
                            table_columns=table_columns,
                        )
                        set_based_result = self._apply_set_based_aggregate_edit_sync(
                            table_name,
                            key_columns,
                            aggregate_edit,
                        )
                        if isinstance(set_based_result, dict):
                            warning = set_based_result.get("warning")
                            if warning:
                                warnings.append(str(warning))
                                continue
                            applied_count = int(set_based_result.get("appliedCount") or 0)
                            propagation_summary = set_based_result.get("propagation")
                            if propagation_summary:
                                propagation_events.append(propagation_summary)
                            if applied_count > 0:
                                after_rows_with_rowid = self._fetch_matching_rows_with_rowid_sync(
                                    table_name,
                                    key_columns,
                                    table_columns=table_columns,
                                )
                                before_by_rowid = {
                                    row.get("__rowid__"): row
                                    for row in before_rows_with_rowid
                                    if isinstance(row, dict) and row.get("__rowid__") is not None
                                }
                                target_column = str(aggregate_edit.get("column") or "").strip()
                                row_pairs = [
                                    {
                                        "before_row": before_by_rowid.get(after_row.get("__rowid__")),
                                        "after_row": after_row,
                                    }
                                    for after_row in after_rows_with_rowid
                                    if isinstance(after_row, dict)
                                    and after_row.get("__rowid__") in before_by_rowid
                                    and before_by_rowid.get(after_row.get("__rowid__"), {}).get(target_column) != after_row.get(target_column)
                                ]
                                scope_value_changes.extend(
                                    self._build_scope_value_changes_from_row_pairs(
                                        row_pairs,
                                        grouping_fields=grouping_fields,
                                        direct_scope_id=aggregate_edit.get("rowPath") or aggregate_edit.get("rowId"),
                                        measure_id=str(aggregate_edit.get("columnId") or "").strip(),
                                        target_column=target_column,
                                        aggregation_fn=self._normalize_aggregation_name(aggregate_edit.get("aggregationFn")),
                                        weight_field=aggregate_edit.get("weightField"),
                                    )
                                )
                                applied["update"] += applied_count
                                inverse_update = set_based_result.get("inverseUpdate")
                                redo_update = set_based_result.get("redoUpdate")
                                if inverse_update:
                                    inverse_transaction["update"].append(inverse_update)
                                else:
                                    history_captureable = False
                                    history_warnings.append(
                                        "Skipped transaction-level undo capture for an aggregate edit because "
                                        "the aggregate cell identity could not be reconstructed."
                                    )
                                if redo_update:
                                    redo_transaction["update"].append(redo_update)
                                else:
                                    history_captureable = False
                                    history_warnings.append(
                                        "Skipped transaction-level redo capture for an aggregate edit because "
                                        "the aggregate cell identity could not be reconstructed."
                                    )
                            continue
                        before_rows = before_rows_with_rowid
                        if not before_rows:
                            continue
                        try:
                            row_rewrites, propagation_summary = self._plan_aggregate_edit_rewrites(before_rows, aggregate_edit)
                        except ValueError as exc:
                            warnings.append(str(exc))
                            continue
                        if not row_rewrites:
                            warnings.append(
                                f"Aggregate edit for '{aggregate_edit.get('column')}' produced no source-row changes."
                            )
                            continue
                        scope_value_changes.extend(
                            self._build_scope_value_changes_from_row_pairs(
                                row_rewrites,
                                grouping_fields=grouping_fields,
                                direct_scope_id=aggregate_edit.get("rowPath") or aggregate_edit.get("rowId"),
                                measure_id=str(aggregate_edit.get("columnId") or "").strip(),
                                target_column=str(aggregate_edit.get("column") or "").strip(),
                                aggregation_fn=self._normalize_aggregation_name(aggregate_edit.get("aggregationFn")),
                                weight_field=aggregate_edit.get("weightField"),
                            )
                        )

                        grouped_rewrites: OrderedDict[str, Dict[str, Any]] = OrderedDict()
                        unique_before_rows: OrderedDict[str, Dict[str, Any]] = OrderedDict()
                        unique_after_rows: OrderedDict[str, Dict[str, Any]] = OrderedDict()
                        for rewrite in row_rewrites:
                            before_row = self._sanitize_column_mapping(rewrite.get("before_row"), allowed_columns=table_columns)
                            after_row = self._sanitize_column_mapping(rewrite.get("after_row"), allowed_columns=table_columns)
                            if not before_row or not after_row:
                                continue
                            before_key = self._stable_json(before_row)
                            after_key = self._stable_json(after_row)
                            if before_key not in unique_before_rows:
                                unique_before_rows[before_key] = dict(before_row)
                            if after_key not in unique_after_rows:
                                unique_after_rows[after_key] = dict(after_row)
                            grouped_key = f"{before_key}->{after_key}"
                            if grouped_key not in grouped_rewrites:
                                grouped_rewrites[grouped_key] = {
                                    "before_row": dict(before_row),
                                    "after_row": dict(after_row),
                                }

                        if not grouped_rewrites:
                            warnings.append(
                                f"Aggregate edit for '{aggregate_edit.get('column')}' produced no executable row rewrites."
                            )
                            continue

                        for grouped_rewrite in grouped_rewrites.values():
                            before_row = grouped_rewrite["before_row"]
                            after_row = grouped_rewrite["after_row"]
                            updates = {
                                column: after_row[column]
                                for column in table_columns
                                if column in before_row and column in after_row and before_row.get(column) != after_row.get(column)
                            }
                            if not updates:
                                continue
                            where_sql, where_params = self._build_where_clause(before_row)
                            set_parts = [f"{column} = ?" for column in updates.keys()]
                            matched_count = self._count_matching_rows_sync(table_name, before_row)
                            if matched_count <= 0:
                                continue
                            self._execute_parameterized_mutation(
                                f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {where_sql}",
                                [*updates.values(), *where_params],
                            )

                        applied["update"] += len(row_rewrites)
                        propagation_events.append(propagation_summary)
                        redo_transaction["remove"].extend(
                            {"keys": dict(row), "exactKeys": True}
                            for row in unique_before_rows.values()
                        )
                        redo_transaction["add"].extend(
                            dict(rewrite["after_row"])
                            for rewrite in row_rewrites
                            if isinstance(rewrite.get("after_row"), dict)
                        )
                        inverse_transaction["remove"].extend(
                            {"keys": dict(row), "exactKeys": True}
                            for row in unique_after_rows.values()
                        )
                        inverse_transaction["add"].extend(
                            dict(rewrite["before_row"])
                            for rewrite in row_rewrites
                            if isinstance(rewrite.get("before_row"), dict)
                        )
                        continue

                    updates = self._sanitize_column_mapping(
                        operation.get("updates"),
                        allowed_columns=table_columns,
                        exclude_columns=list(key_columns.keys()),
                    )
                    if not key_columns or not updates:
                        warnings.append("Skipped update operation without valid key columns or updates.")
                        continue
                    before_rows = self._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)
                    if not before_rows:
                        continue
                    if edit_meta.get("rowPath") and edit_meta.get("colId") and len(updates) == 1:
                        target_column = next(iter(updates.keys()))
                        scope_value_changes.extend(
                            self._build_direct_scope_value_changes(
                                before_rows,
                                row_path=str(edit_meta.get("rowPath") or edit_meta.get("rowId") or ""),
                                measure_id=str(edit_meta.get("colId") or ""),
                                target_column=target_column,
                                next_value=updates.get(target_column),
                            )
                        )
                    set_parts = [f"{column} = ?" for column in updates.keys()]
                    where_sql, where_params = self._build_where_clause(key_columns)
                    self._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {where_sql}",
                        [*updates.values(), *where_params],
                    )
                    applied["update"] += len(before_rows)
                    redo_transaction["update"].append({
                        "keys": dict(key_columns),
                        "values": dict(updates),
                    })
                    inverse_updates = self._build_inverse_update_values(before_rows, list(updates.keys()))
                    if inverse_updates:
                        inverse_transaction["update"].append({
                            "keys": dict(key_columns),
                            "values": inverse_updates,
                        })
                    else:
                        inverse_transaction["remove"].append({"keys": dict(key_columns)})
                        inverse_transaction["add"].extend([dict(row) for row in before_rows if isinstance(row, dict)])
                        history_warnings.append(
                            "Captured structural inverse for a multi-row update because prior values were not uniform."
                        )

                for operation in upsert_operations:
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    row_data = self._sanitize_column_mapping(operation.get("row_data"), allowed_columns=table_columns)
                    merged_row = {**key_columns, **row_data}
                    if not key_columns or not merged_row:
                        warnings.append("Skipped upsert operation without valid keys or row data.")
                        continue
                    before_rows = self._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)
                    if before_rows:
                        updates = self._sanitize_column_mapping(
                            merged_row,
                            allowed_columns=table_columns,
                            exclude_columns=list(key_columns.keys()),
                        )
                        if updates:
                            set_parts = [f"{column} = ?" for column in updates.keys()]
                            where_sql, where_params = self._build_where_clause(key_columns)
                            self._execute_parameterized_mutation(
                                f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {where_sql}",
                                [*updates.values(), *where_params],
                            )
                            applied["upsertUpdated"] += len(before_rows)
                            redo_transaction["upsert"].append({
                                "keys": dict(key_columns),
                                "rowData": dict(merged_row),
                            })
                            inverse_updates = self._build_inverse_update_values(before_rows, list(updates.keys()))
                            if inverse_updates:
                                inverse_transaction["update"].append({
                                    "keys": dict(key_columns),
                                    "values": inverse_updates,
                                })
                            else:
                                inverse_transaction["remove"].append({"keys": dict(key_columns)})
                                inverse_transaction["add"].extend([dict(row) for row in before_rows if isinstance(row, dict)])
                                history_warnings.append(
                                    "Captured structural inverse for a multi-row upsert update because prior values were not uniform."
                                )
                        continue
                    insert_statement = self._build_insert_statement(table_name, merged_row, table_columns)
                    if insert_statement is None:
                        warnings.append("Skipped upsert insert because no valid row columns were provided.")
                        continue
                    sql, params = insert_statement
                    self._execute_parameterized_mutation(sql, params)
                    applied["upsertInserted"] += 1
                    redo_transaction["upsert"].append({
                        "keys": dict(key_columns),
                        "rowData": dict(merged_row),
                    })
                    if key_columns:
                        inverse_transaction["remove"].append({"keys": dict(key_columns)})
                    else:
                        history_captureable = False
                        history_warnings.append("Undo history skipped for an inserted upsert because no key fields were available.")

                for operation in add_operations:
                    row_source = operation.get("row_data") if isinstance(operation.get("row_data"), dict) else operation
                    row = self._sanitize_column_mapping(
                        row_source,
                        allowed_columns=table_columns,
                    )
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    insert_statement = self._build_insert_statement(table_name, row, table_columns)
                    if insert_statement is None:
                        warnings.append("Skipped add operation because no valid row columns were provided.")
                        continue
                    sql, params = insert_statement
                    self._execute_parameterized_mutation(sql, params)
                    applied["add"] += 1
                    redo_transaction["add"].append(dict(row))
                    if key_columns:
                        inverse_transaction["remove"].append({"keys": dict(key_columns)})
                    else:
                        history_captureable = False
                        history_warnings.append("Undo history skipped for an added row because no key fields were available.")

            undo_transaction = self._compact_history_transaction(inverse_transaction)
            replay_transaction = self._compact_history_transaction(redo_transaction)
            if not any(applied.values()):
                history_captureable = False
            if history_captureable and (undo_transaction is None or replay_transaction is None):
                history_captureable = False
                history_warnings.append("Undo history was not captured because the transaction produced no reversible operations.")

            return {
                "requested": requested,
                "applied": applied,
                "warnings": warnings,
                "rowCountDelta": (
                    applied["add"]
                    + applied["upsertInserted"]
                    - applied["remove"]
                ),
                "inverseTransaction": undo_transaction if history_captureable else None,
                "redoTransaction": replay_transaction if history_captureable else None,
                "propagation": propagation_events,
                "scopeValueChanges": scope_value_changes,
                "history": {
                    "captureable": bool(history_captureable),
                    "warnings": history_warnings,
                },
            }

        result = await loop.run_in_executor(None, execute_transaction)
        if any(result["applied"].values()):
            self._clear_mutation_caches()
        return result

    async def update_record(self, table_name: str, key_columns: Dict[str, Any], updates: Dict[str, Any]) -> bool:
        """
        Update records in the database based on composite keys.
        Uses parameterized ? binding for all values to prevent SQL injection.
        """
        updated_count = await self.update_records(
            table_name,
            [{"key_columns": key_columns, "updates": updates}],
        )
        return bool(updated_count)

    def _clear_mutation_caches(self) -> None:
        if hasattr(self.cache, "clear"):
            self.cache.clear()
        self._hierarchy_view_cache.clear()
        self._hierarchy_root_count_cache.clear()
        self._hierarchy_root_page_cache.clear()
        self._hierarchy_grand_total_cache.clear()

    def _execute_parameterized_mutation(self, sql: str, params: List[Any]) -> None:
        con = self.planner.con
        if hasattr(con, "con"):
            con.con.execute(sql, [*params])
        elif hasattr(con, "raw_sql"):
            con.raw_sql(sql)
        elif hasattr(con, "execute"):
            con.execute(sql, [*params])
        else:
            raise NotImplementedError("Backend does not support parameterized SQL updates")

    async def update_records(self, table_name: str, operations: List[Dict[str, Any]]) -> int:
        """Apply multiple row updates in one mutation transaction."""
        result = await self.apply_row_transaction(
            table_name,
            {"update": operations},
        )
        return int((result.get("applied") or {}).get("update") or 0)

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
            self._execute_parameterized_mutation(sql, [value, row_id])

        await loop.run_in_executor(None, execute_update)
        self._clear_mutation_caches()
        return {"status": "success", "updated": 1}
