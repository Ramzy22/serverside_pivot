"""
ScalablePivotController - Main controller for high-scale pivot operations
"""
from typing import Optional, Any, Dict, List, Mapping, Union, Callable, Generator
import threading
import time
import decimal
import datetime
import math
import os
from contextlib import nullcontext
from contextvars import ContextVar
import re
import json
import hashlib
import logging
from functools import cmp_to_key
import pyarrow as pa
import asyncio
import ibis
from ibis.expr.api import Table as IbisTable
from collections import OrderedDict

from .controller import PivotController


_raise_query_execution_errors = ContextVar("pivot_raise_query_execution_errors", default=False)
from .cache_coordinator import CacheCoordinator, CacheInvalidationEvent, CacheNamespaces
from .lifecycle import get_task_manager
from .tree import TreeExpansionManager
from .planner.ibis_planner import IbisPlanner
from .diff.diff_engine import QueryDiffEngine
from .backends.ibis_backend import IbisBackend
from .backends.duckdb_backend import DuckDBBackend
from .cache.memory_cache import MemoryCache
from .cache.redis_cache import RedisCache
from .cache.generation_guard import GenerationGuard, GenerationGuardedCache
from .resource_governor import ResourceGovernor, ResourceLimits, get_worker_resource_governor
from .types.pivot_spec import PivotSpec
from pivot_engine.streaming.streaming_processor import StreamAggregationProcessor, IncrementalMaterializedViewManager
from pivot_engine.hierarchical_scroll_manager import HierarchicalVirtualScrollManager
from pivot_engine.hierarchy_query_service import (
    HierarchyQuery,
    HierarchyQueryService,
    LegacyHierarchyAdapter,
)
from pivot_engine.editing.mutation_executor import DatabaseMutationExecutor
from pivot_engine.export_executor import PivotExportExecutor
from pivot_engine.execution_locks import TableExecutionLockRegistry
from pivot_engine.query_execution_service import PivotQueryExecutor
from pivot_engine.hierarchy_rows import (
    build_hierarchy_row_window,
    build_visible_hierarchy_rows,
    clone_hierarchy_rows,
    compute_hierarchy_color_scale_stats,
    find_hierarchy_grand_total_row,
    hierarchy_lookup_key,
    is_hierarchy_grand_total,
    is_unbalanced_missing_value,
    normalize_expanded_paths,
)
from pivot_engine.hierarchy_cache_registry import HierarchyCacheRegistry
from pivot_engine.progressive_loader import ProgressiveDataLoader
from pivot_engine.cdc.cdc_manager import PivotCDCManager
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.intelligent_prefetch_manager import IntelligentPrefetchManager, UserPatternAnalyzer
from pivot_engine.pruning_manager import HierarchyPruningManager, ProgressiveHierarchicalLoader


def _positive_env_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _non_negative_env_int(name: str, default: int) -> int:
    """Read an opt-out-friendly bounded-cache setting from the environment."""
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


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
        spec_hash = hashlib.sha256(json.dumps(key_parts, sort_keys=True).encode()).hexdigest()

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

    def close(self):
        if hasattr(self, "hierarchy_cache_registry"):
            for name in ("view", "root_count", "root_page", "root_result", "grand_total"):
                getattr(self.hierarchy_cache_registry, name).clear()
        super().close()
    
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
        max_hierarchy_rows: Optional[int] = None,
        distributed_runtime: Optional[Any] = None,
        cdc_exact_state_store: str = "memory",
        resource_limits: Optional[ResourceLimits] = None,
        resource_governor: Optional[ResourceGovernor] = None,
        **cache_options: Any
    ):
        if distributed_runtime is None:
            # Import lazily: runtime's public package also imports the adapter,
            # while this module is being imported by that adapter.
            from .runtime.distributed import distributed_runtime_from_env

            distributed_runtime = distributed_runtime_from_env()
        self.distributed_runtime = distributed_runtime
        resolved_governor = resource_governor or (
            ResourceGovernor(resource_limits) if resource_limits is not None
            else get_worker_resource_governor()
        )
        self.resource_limits = resource_limits or resolved_governor.limits
        if self.resource_limits != resolved_governor.limits:
            raise ValueError("Controller resource limits must match its worker governor")
        if self.distributed_runtime is not None and (
            cache == "redis" or isinstance(cache, RedisCache)
        ):
            raise ValueError(
                "Distributed runtime requires a worker-local memory cache; "
                "Redis runtime state is not a general query-cache backend."
            )
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
            resource_governor=resolved_governor,
            **cache_options
        )

        generation_guard = (
            GenerationGuard(self.distributed_runtime, lambda: None)
            if self.distributed_runtime is not None
            else None
        )
        self.cache_coordinator = CacheCoordinator(generation_guard=generation_guard)
        if generation_guard is not None:
            self.cache = GenerationGuardedCache(self.cache, generation_guard)
            # PivotController constructs the diff engine before the guarded
            # L1 proxy exists, so make every controller query path observe the
            # same shared generation.
            self.diff_engine.cache = self.cache
        self.cache_coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_QUERY,
            owner="controller",
            key_dimensions=("table", "compiled_sql", "pivot_spec", "delta_epoch"),
            max_entries=getattr(self.cache, "max_size", None),
            ttl_seconds=getattr(self.cache, "default_ttl", None),
            size_getter=self._query_cache_size_snapshot,
            invalidator=self._invalidate_controller_query_cache,
        )

        self.enable_streaming = enable_streaming
        self.enable_incremental_views = enable_incremental_views
        self.task_manager = get_task_manager()
        self.planning_lock = threading.Lock() # Lock for planner (Ibis/DuckDB metadata access is not thread-safe)
        self.execution_locks = TableExecutionLockRegistry()
        # Retain the historical default lock attribute for integrations that
        # accessed it directly; all controller-owned execution now uses the
        # registry so contention can be observed per table.
        self.execution_lock = self.execution_locks.lock_for(None)
        self.hierarchy_request_lock = threading.RLock()
        
        # Helper to get connection safely
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)

        # Keep DuckDB on the Ibis-backed execution path.
        # The raw pooled DuckDB swap can leave pending results in an invalid state
        # for totals, hierarchy counts, and virtual-scroll helper queries that still
        # execute through Ibis expressions on the shared connection.

        if cdc_exact_state_store not in {"memory", "redis"}:
            raise ValueError("cdc_exact_state_store must be 'memory' or 'redis'.")
        if enable_streaming:
            from pivot_engine.streaming.exact_state import CDCStateLimits, MemoryCDCStateStore

            cdc_state_limits = CDCStateLimits(
                max_groups=CDCStateLimits().max_groups,
                max_bytes=self.resource_limits.cdc_bytes,
                max_extrema_values=CDCStateLimits().max_extrema_values,
            )
            if cdc_exact_state_store == "redis":
                if self.distributed_runtime is None:
                    raise ValueError(
                        "Redis CDC state requires a validated distributed Redis runtime."
                    )
                from pivot_engine.streaming.exact_state import RedisCDCStateStore

                self.streaming_processor = StreamAggregationProcessor(
                    state_store=RedisCDCStateStore(
                        self.distributed_runtime, limits=cdc_state_limits,
                        governor=self.resource_governor,
                    ),
                    cdc_state_limits=cdc_state_limits,
                )
            else:
                self.streaming_processor = StreamAggregationProcessor(
                    state_store=MemoryCDCStateStore(
                        cdc_state_limits, governor=self.resource_governor,
                    ),
                    cdc_state_limits=cdc_state_limits
                )

        if enable_incremental_views and con is not None:
            self.incremental_view_manager = IncrementalMaterializedViewManager(con)
        else:
            self.incremental_view_manager = None

        # Advanced hierarchical managers
        self.materialized_hierarchy_manager = MaterializedHierarchyManager(con, self.cache) if con is not None else None

        # Performance managers
        # HierarchicalVirtualScrollManager expects an IbisPlanner instance
        # Lock removed as manager is now stateless/thread-safe
        self.virtual_scroll_manager = (
            HierarchicalVirtualScrollManager(self.planner, self.cache, self.materialized_hierarchy_manager)
            if con is not None and self.materialized_hierarchy_manager is not None
            else None
        )
        # ProgressiveDataLoader expects an Ibis connection
        self.progressive_loader = ProgressiveDataLoader(con, self.cache) if con is not None else None


        # Initialize real pattern analyzer for intelligent prefetching
        self.intelligent_prefetch_manager = IntelligentPrefetchManager(
            session_tracker=None,
            pattern_analyzer=UserPatternAnalyzer(cache=self.cache),
            backend=con,
            cache=self.cache,
        )
        # PruningManager expects an Ibis connection
        self.pruning_manager = HierarchyPruningManager(con) if con is not None else None
        # ProgressiveHierarchicalLoader expects an Ibis connection
        self.progressive_hierarchy_loader = (
            ProgressiveHierarchicalLoader(con, self.cache, self.pruning_manager)
            if con is not None and self.pruning_manager is not None
            else None
        )

        # CDC for real-time updates
        self.cdc_manager = None  # Will be set via setup_cdc method
        
        self.stats_tracker = QueryStatsTracker()
        hierarchy_cache_ttl = _positive_env_float("PIVOT_HIERARCHY_CACHE_TTL_SECONDS", 300.0)
        self.hierarchy_cache_registry = HierarchyCacheRegistry(
            self.cache_coordinator,
            ttl_seconds=hierarchy_cache_ttl,
            max_bytes=self.resource_limits.cache_bytes,
            governor=self.resource_governor,
        )
        # Compatibility aliases retain the established controller internals
        # while the registry owns creation and coordinator invalidation.
        self._hierarchy_view_cache = self.hierarchy_cache_registry.view
        self._hierarchy_view_cache_ttl = self.hierarchy_cache_registry.view_ttl
        self._hierarchy_view_cache_size = self.hierarchy_cache_registry.view_size
        self._hierarchy_root_count_cache = self.hierarchy_cache_registry.root_count
        self._hierarchy_root_count_cache_ttl = self.hierarchy_cache_registry.root_count_ttl
        self._hierarchy_root_count_cache_size = self.hierarchy_cache_registry.root_count_size
        self._hierarchy_root_page_cache = self.hierarchy_cache_registry.root_page
        self._hierarchy_root_page_cache_ttl = self.hierarchy_cache_registry.root_page_ttl
        self._hierarchy_root_page_cache_size = self.hierarchy_cache_registry.root_page_size
        self._hierarchy_root_result_cache = self.hierarchy_cache_registry.root_result
        self._hierarchy_root_result_cache_ttl = self.hierarchy_cache_registry.root_result_ttl
        self._hierarchy_root_result_cache_size = self.hierarchy_cache_registry.root_result_size
        # Materialising a modest collapsed root level avoids an expensive
        # ``IN (...)`` aggregate for every scroll. Both limits are hard safety
        # bounds; set either to 0 to retain page-at-a-time execution only.
        self._collapsed_root_materialization_max_rows = _non_negative_env_int(
            "PIVOT_COLLAPSED_ROOT_MATERIALIZATION_MAX_ROWS", 10_000
        )
        self._collapsed_root_materialization_max_bytes = _non_negative_env_int(
            "PIVOT_COLLAPSED_ROOT_MATERIALIZATION_MAX_BYTES", 32 * 1024 * 1024
        )
        self._hierarchy_grand_total_cache = self.hierarchy_cache_registry.grand_total
        self._hierarchy_grand_total_cache_ttl = self.hierarchy_cache_registry.grand_total_ttl
        self._hierarchy_grand_total_cache_size = self.hierarchy_cache_registry.grand_total_size
        self._sparse_materialized_pivot_threshold = 8
        try:
            configured_hierarchy_cap = int(max_hierarchy_rows) if max_hierarchy_rows is not None else 0
        except (TypeError, ValueError):
            configured_hierarchy_cap = 0
        self.max_hierarchy_rows = configured_hierarchy_cap if configured_hierarchy_cap > 0 else None
        self.hierarchy_query_service = HierarchyQueryService(LegacyHierarchyAdapter(self))
        # Public controller methods remain stable façades while streaming export
        # execution is owned by a focused collaborator.
        self.export_executor = PivotExportExecutor(self)
        self.query_executor = PivotQueryExecutor(self)
        self.mutation_executor = DatabaseMutationExecutor(self)

    @staticmethod
    def _profile_ms(start: Optional[float], end: Optional[float]) -> Optional[float]:
        if start is None or end is None:
            return None
        return round((end - start) * 1000, 3)

    def _sync_controller_cache_generation(self) -> int:
        return self.cache_coordinator.sync_generation()

    def _query_cache_size_snapshot(self) -> Dict[str, int]:
        if not hasattr(self.cache, "get_all_keys"):
            return {"entries": 0}
        count = 0
        for raw_key in list(self.cache.get_all_keys() or []):
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
            if (
                key.startswith("pivot_ibis:query:")
                or key.startswith("pivot_ibis:query_fallback:")
                or key.startswith("pivot_sparse:")
            ):
                count += 1
        return {"entries": count}

    def _invalidate_controller_query_cache(self, event: CacheInvalidationEvent) -> None:
        # A generation-only synchronization intentionally has no source table:
        # the worker missed the detailed Pub/Sub event, so preserving any query
        # entry would risk returning stale data.  Clear only this controller's
        # L1 query cache, never a shared Redis database.
        if event.table is None:
            if hasattr(self.cache, "clear"):
                self.cache.clear()
            return
        table_key = self._cache_table_key(event.table)
        table_scoped_prefixes = (
            f"pivot_ibis:query:{table_key}:",
            f"pivot_ibis:query_fallback:{table_key}:",
            f"pivot_sparse:{table_key}:",
        )
        legacy_query_prefixes = (
            "pivot_ibis:query:",
            "pivot_ibis:query_fallback:",
            "pivot_sparse:",
        )

        def should_delete_query_cache(key: str) -> bool:
            if key.startswith(table_scoped_prefixes):
                return True
            for legacy_prefix in legacy_query_prefixes:
                if key.startswith(legacy_prefix):
                    return ":" not in key[len(legacy_prefix):]
            return False

        if not self._delete_cache_keys(should_delete_query_cache) and hasattr(self.cache, "clear"):
            self.cache.clear()

    def register_ibis_table(
        self,
        table_name: str,
        table_expr: Any,
        *,
        backend: Optional[Any] = None,
        register_checkpoint: bool = True,
    ) -> Any:
        result = super().register_ibis_table(
            table_name,
            table_expr,
            backend=backend,
            register_checkpoint=register_checkpoint,
        )
        con = getattr(self.planner, "con", None)
        if con is not None:
            if getattr(self, "materialized_hierarchy_manager", None) is not None:
                self.materialized_hierarchy_manager.backend = con
                if getattr(self.materialized_hierarchy_manager, "builder", None) is not None:
                    self.materialized_hierarchy_manager.builder.backend = con
            if getattr(self, "progressive_loader", None) is not None:
                self.progressive_loader.backend = con
                if getattr(self.progressive_loader, "builder", None) is not None:
                    self.progressive_loader.builder.backend = con
            if getattr(self, "pruning_manager", None) is not None:
                self.pruning_manager.backend = con
            if getattr(self, "progressive_hierarchy_loader", None) is not None:
                self.progressive_hierarchy_loader.backend = con
        if hasattr(self, "cache_coordinator") and self.cache_coordinator is not None:
            self.cache_coordinator.invalidate(
                event="data_load",
                reason="register_ibis_table",
                table=table_name,
                structural=True,
            )
        return result

    def load_data_from_arrow(
        self,
        table_name: str,
        arrow_table: pa.Table,
        register_checkpoint: bool = True,
    ):
        super().load_data_from_arrow(table_name, arrow_table, register_checkpoint=register_checkpoint)
        self.cache_coordinator.invalidate(
            event="data_load",
            reason="load_data_from_arrow",
            table=table_name,
            structural=True,
        )

    def _uses_duckdb_ibis(self) -> bool:
        backend_name = getattr(getattr(self.planner, "con", None), "name", "").lower()
        return backend_name == "duckdb"

    def _execution_lock_for_table(self, table_name: Optional[str]) -> threading.RLock:
        return self.execution_locks.lock_for(table_name)

    def execution_lock_metrics_snapshot(
        self,
        table_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Expose per-table queue contention to runtime profiling consumers."""
        return self.execution_locks.snapshot(table_name)

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
        table_key = self._cache_table_key(getattr(spec, "table", None))
        return f"pivot_sparse:{table_key}:{key_hash}"

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
        spec = self.planner._prepare_measure_axis_spec(spec)
        base_table = self.planner._table_for_spec(spec)
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

    @staticmethod
    def _sort_sparse_materialized_rows(
        rows: List[Dict[str, Any]],
        sort_specs: Any,
        fallback_fields: List[str],
    ) -> List[Dict[str, Any]]:
        """Sort reshaped pivot rows, including dynamic pivot-value columns.

        SQL ordering runs before sparse columns such as ``Stress_pnl_sum`` exist,
        so those sort expressions are necessarily ignored by the query builder.
        Apply them after reshape and retain deterministic dimension tie-breakers.
        """
        normalized_sorts = [
            item for item in (sort_specs if isinstance(sort_specs, list) else [sort_specs])
            if isinstance(item, dict) and isinstance(item.get("field"), str)
        ]
        dynamic_sorts = [
            item for item in normalized_sorts
            if any(item.get("field") in row for row in rows)
        ]
        if not dynamic_sorts or not rows:
            return rows

        def is_missing(value: Any) -> bool:
            if value is None:
                return True
            if isinstance(value, float):
                return math.isnan(value)
            if isinstance(value, decimal.Decimal):
                return value.is_nan()
            return False

        def compare_values(left: Any, right: Any, spec: Dict[str, Any]) -> int:
            left_missing = is_missing(left)
            right_missing = is_missing(right)
            if left_missing or right_missing:
                if left_missing and right_missing:
                    return 0
                nulls_first = str(spec.get("nulls") or "").strip().lower() == "first"
                return (-1 if left_missing else 1) if nulls_first else (1 if left_missing else -1)

            sort_type = str(spec.get("sortType") or "").strip().lower()
            absolute_sort = bool(spec.get("absoluteSort")) or sort_type in {
                "absolute", "abs", "absolute_value", "absolute-value",
            }
            numeric_types = (int, float, decimal.Decimal)
            numeric = (
                not isinstance(left, bool)
                and not isinstance(right, bool)
                and isinstance(left, numeric_types)
                and isinstance(right, numeric_types)
                and (not isinstance(left, float) or math.isfinite(left))
                and (not isinstance(right, float) or math.isfinite(right))
                and (not isinstance(left, decimal.Decimal) or left.is_finite())
                and (not isinstance(right, decimal.Decimal) or right.is_finite())
            )

            if numeric:
                comparable_left = abs(left) if absolute_sort else left
                comparable_right = abs(right) if absolute_sort else right
                result = (comparable_left > comparable_right) - (comparable_left < comparable_right)
                if result == 0 and absolute_sort:
                    result = (left > right) - (left < right)
            else:
                left_text = str(left).casefold()
                right_text = str(right).casefold()
                result = (left_text > right_text) - (left_text < right_text)

            return -result if str(spec.get("order") or "asc").lower() == "desc" else result

        def compare_rows(left: Dict[str, Any], right: Dict[str, Any]) -> int:
            for spec in dynamic_sorts:
                result = compare_values(left.get(spec["field"]), right.get(spec["field"]), spec)
                if result:
                    return result
            for field in fallback_fields:
                result = compare_values(left.get(field), right.get(field), {"order": "asc"})
                if result:
                    return result
            return 0

        return sorted(rows, key=cmp_to_key(compare_rows))

    def _reshape_sparse_materialized_pivot(
        self,
        grouped_table: pa.Table,
        spec: PivotSpec,
        column_values: List[str],
        hidden_sort_keys: List[str],
    ) -> pa.Table:
        spec = self.planner._prepare_measure_axis_spec(spec)
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

        ordered_row_list = self._sort_sparse_materialized_rows(
            list(ordered_rows.values()),
            spec.sort,
            row_dims,
        )
        output_columns = list(row_dims) + list(hidden_sort_keys)
        for column_value in (column_values or []):
            for measure in base_measures:
                output_columns.append(f"{column_value}_{measure.alias}")

        column_data = {
            column_name: [row.get(column_name) for row in ordered_row_list]
            for column_name in output_columns
        }
        return pa.table(column_data)

    async def _execute_ibis_expr_async(self, expr, table_name: Optional[str] = None) -> pa.Table:
        """Execute through the base controller's cancellable Ibis facade."""
        return await super()._execute_ibis_expr_async(expr)

    async def _execute_ibis_scalar_async(self, expr, table_name: Optional[str] = None) -> Any:
        """Execute an Ibis scalar expression safely for the current backend."""
        loop = asyncio.get_running_loop()

        caps = getattr(self, "backend_capabilities", None)
        requires_lock = (
            bool(getattr(caps, "shared_connection_requires_lock", False))
            if caps is not None
            else self._uses_duckdb_ibis()
        )
        if not requires_lock:
            return await loop.run_in_executor(None, expr.execute)

        def execute_with_lock():
            return self.execution_locks.run(table_name, expr.execute)

        return await loop.run_in_executor(None, execute_with_lock)

    async def setup_cdc(self, table_name: str, change_stream):
        """Setup CDC for real-time tracking of data changes"""
        # PivotCDCManager expects an Ibis connection
        con = self.backend.con if isinstance(self.backend, IbisBackend) else getattr(self.planner, 'con', None)
        self.cdc_manager = PivotCDCManager(con, change_stream)

        await self.cdc_manager.setup_cdc(table_name)

        # Register materialized view manager to receive change notifications
        if self.incremental_view_manager is not None:
            self.cdc_manager.register_materialized_view_manager(table_name, self.incremental_view_manager)
        if getattr(self, 'streaming_processor', None) is not None:
            self.cdc_manager.register_change_consumer(table_name, self.streaming_processor)

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
        
        if self.incremental_view_manager is not None:
            self.cdc_manager.register_materialized_view_manager(table_name, self.incremental_view_manager)
        if getattr(self, 'streaming_processor', None) is not None:
            self.cdc_manager.register_change_consumer(table_name, self.streaming_processor)
        
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
            old_row=change_dict.get('old_row'),
            source=str(change_dict.get('source') or 'push'),
            event_id=change_dict.get('event_id'),
            partition=str(change_dict.get('partition') or '0'),
            sequence=change_dict.get('sequence'),
            row_identity=(
                str(change_dict['row_identity'])
                if change_dict.get('row_identity') is not None
                else str((change_dict.get('new_row') or change_dict.get('old_row') or {}).get('id'))
                if (change_dict.get('new_row') or change_dict.get('old_row') or {}).get('id') is not None
                else None
            ),
            row_images_complete=change_dict.get('row_images_complete'),
        )
        
        await self.cdc_manager.push_change(table_name, change)
        return 'accepted_exact' if change.is_exact() else 'accepted_invalidating'

    async def run_streaming_aggregation(
        self,
        spec: PivotSpec,
        *,
        source_rows: Optional[List[Mapping[str, Any]]] = None,
        source_checkpoints: Optional[Mapping[str, tuple[int, str]]] = None,
    ):
        """Create a streaming job and optionally install an exact source snapshot.

        ``source_rows`` and ``source_checkpoints`` must be captured atomically
        by the CDC source. The controller deliberately never scans the table
        after accepting a checkpoint: that later scan could silently omit an
        event between source watermark capture and query execution. Without
        the complete snapshot pair, the job remains unavailable rather than
        claiming an empty aggregate is current.
        """
        if not self.enable_streaming:
            raise ValueError("Streaming aggregation is not enabled")
        if (source_rows is None) != (source_checkpoints is None):
            raise ValueError(
                "Exact CDC requires source snapshot rows and source checkpoints "
                "captured atomically."
            )
        
        job_id = await self.streaming_processor.create_real_time_aggregation_job(spec)
        if source_rows is not None and source_checkpoints is not None:
            await self.streaming_processor.bootstrap_job(
                job_id,
                source_rows,
                checkpoints=source_checkpoints,
            )
        result = await self.streaming_processor.get_job_result(job_id)
        return {
            "job_id": job_id,
            "status": result.status if result is not None else "unavailable",
            "reason": result.reason if result is not None else "streaming_job_unavailable",
        }

    async def create_incremental_view(self, spec: PivotSpec):
        """Create incremental materialized view"""
        if not self.enable_incremental_views or self.incremental_view_manager is None:
            raise ValueError("Incremental views are not enabled")
        
        view_name = await self.incremental_view_manager.create_incremental_view(spec)
        return {"view_name": view_name, "status": "created"}

    def run_virtual_scroll_hierarchical(self, spec: PivotSpec, start_row: int, end_row: int, expanded_paths: List[List[str]]):
        """Run hierarchical pivot with virtual scrolling for large datasets"""
        if self.virtual_scroll_manager is None:
            raise ValueError("Virtual scroll manager requires an Ibis connection.")
        result = self.virtual_scroll_manager.get_visible_rows_hierarchical(
            spec, start_row, end_row, expanded_paths
        )
        return result

    # Dimension-specific sort keys that must NOT propagate across levels.
    _DIM_SORT_KEYS = ("sortKeyField", "semanticType", "sortSemantic", "sortType", "absoluteSort")

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
        seen_effective_sorts = set()
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

            effective_sort_key = (
                item.get("sortKeyField") or item.get("field"),
                item.get("order") or "asc",
                item.get("nulls") or "",
                bool(item.get("absoluteSort")),
            )
            if effective_sort_key in seen_effective_sorts:
                continue
            seen_effective_sorts.add(effective_sort_key)
            adapted.append(item)
            
        return adapted

    @staticmethod
    def _build_parent_batch_filters(
        parent_dims: List[str],
        parent_paths: List[List[str]],
    ) -> List[Dict[str, Any]]:
        """
        Build exact batch filters for many parent paths.
        For 1 dim: exact IN list.
        For multi-dim: OR of exact per-path AND groups, so the database
        filters to the requested hierarchy parents instead of fetching the
        Cartesian superset and relying on Python post-filtering.
        """
        if not parent_dims or not parent_paths:
            return []

        if len(parent_dims) == 1:
            return [{
                "field": parent_dims[0],
                "op": "in",
                "value": [path[0] for path in parent_paths if len(path) >= 1],
            }]

        path_groups: List[Dict[str, Any]] = []
        seen_paths: set[tuple] = set()
        for path in parent_paths:
            if not isinstance(path, list) or len(path) < len(parent_dims):
                continue
            path_tuple = tuple(path[:len(parent_dims)])
            if path_tuple in seen_paths:
                continue
            seen_paths.add(path_tuple)

            conditions = []
            for dim_idx, dim in enumerate(parent_dims):
                value = path[dim_idx]
                conditions.append(
                    {"field": dim, "op": "is null"}
                    if value is None
                    else {"field": dim, "op": "=", "value": value}
                )

            if len(conditions) == 1:
                path_groups.append(conditions[0])
            else:
                path_groups.append({"operator": "AND", "conditions": conditions})

        if not path_groups:
            return []
        if len(path_groups) == 1:
            return [path_groups[0]]
        return [{"operator": "OR", "conditions": path_groups}]

    @staticmethod
    def _path_key(parts: List[Any]) -> str:
        return hierarchy_lookup_key(parts)

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
        return normalize_expanded_paths(expanded_paths)

    @staticmethod
    def _clone_hierarchy_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return clone_hierarchy_rows(rows)

    def _clone_hierarchy_result(self, hierarchy_result: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        return {
            str(parent_key): self._clone_hierarchy_rows(child_rows)
            for parent_key, child_rows in (hierarchy_result or {}).items()
        }

    @staticmethod
    def _mark_measure_axis_exact_layout(spec: PivotSpec) -> PivotSpec:
        """Keep hierarchy level queries at their requested row/column prefix.

        The normal flat pivot path auto-adds the measure-axis label field to the
        configured axis. Hierarchy loading intentionally queries one prefix at a
        time, so the virtual measure-name dimension must appear only when that
        prefix has reached its level.
        """
        if getattr(spec, "measure_axis", None) is not None:
            setattr(spec, "_measure_axis_exact_layout", True)
        return spec

    def _hierarchy_view_cache_get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        self._sync_controller_cache_generation()
        cached_entry = self._hierarchy_view_cache.get(cache_key)
        if not cached_entry:
            self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_VIEW, hit=False)
            return None
        payload, expires_at = cached_entry
        if time.time() > expires_at:
            self._hierarchy_view_cache.pop(cache_key, None)
            self.cache_coordinator.record_lookup(
                CacheNamespaces.CONTROLLER_HIERARCHY_VIEW,
                hit=False,
                expired=True,
            )
            return None
        self._hierarchy_view_cache.move_to_end(cache_key)
        self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_VIEW, hit=True)
        return {
            **payload,
            "expanded_set": set(payload.get("expanded_set") or set()),
            "hierarchy_result": self._clone_hierarchy_result(payload.get("hierarchy_result") or {}),
            "visible_rows": self._clone_hierarchy_rows(payload.get("visible_rows") or []),
            "grand_total_row": dict(payload["grand_total_row"]) if isinstance(payload.get("grand_total_row"), dict) else payload.get("grand_total_row"),
            "grand_total_formula_source_rows": self._clone_hierarchy_rows(payload.get("grand_total_formula_source_rows") or []),
        }

    def _hierarchy_view_cache_set(self, cache_key: str, payload: Dict[str, Any]) -> None:
        self._sync_controller_cache_generation()
        stored_payload = {
            **payload,
            "expanded_set": set(payload.get("expanded_set") or set()),
            "hierarchy_result": self._clone_hierarchy_result(payload.get("hierarchy_result") or {}),
            "visible_rows": self._clone_hierarchy_rows(payload.get("visible_rows") or []),
            "grand_total_row": dict(payload["grand_total_row"]) if isinstance(payload.get("grand_total_row"), dict) else payload.get("grand_total_row"),
            "grand_total_formula_source_rows": self._clone_hierarchy_rows(payload.get("grand_total_formula_source_rows") or []),
        }
        self.hierarchy_cache_registry.store(
            "view", cache_key, stored_payload,
            ttl=self._hierarchy_view_cache_ttl,
            max_size=self._hierarchy_view_cache_size,
        )

    def _find_reusable_hierarchy_cache_entry(self, spec_fingerprint: str, requested_expanded_set: set[tuple]) -> Optional[Dict[str, Any]]:
        self._sync_controller_cache_generation()
        if not requested_expanded_set or requested_expanded_set == {("__ALL__",)}:
            return None
        best_entry = None
        best_rank: Optional[tuple[int, int]] = None
        for cache_key, (payload, expires_at) in list(self._hierarchy_view_cache.items()):
            if time.time() > expires_at:
                self._hierarchy_view_cache.pop(cache_key, None)
                self.cache_coordinator.record_expiration(
                    CacheNamespaces.CONTROLLER_HIERARCHY_VIEW,
                    entries=len(self._hierarchy_view_cache),
                )
                continue
            if payload.get("spec_fingerprint") != spec_fingerprint:
                continue
            # A collapsed virtual-scroll response only materializes the root
            # rows in its requested window.  It is useful for an identical
            # collapsed request, but it cannot be used as the base tree for an
            # expansion: doing so makes the flattened row indexes relative to
            # that one page and can omit children for parents near its end.
            if payload.get("is_complete_hierarchy_result") is False:
                continue
            cached_expanded_set = set(payload.get("expanded_set") or set())
            if cached_expanded_set == {("__ALL__",)}:
                continue

            # Expansion can reuse the largest cached subset and fetch only its
            # missing paths. Collapse is even cheaper: a cached superset already
            # contains every hierarchy branch required by the smaller expanded
            # set, so it only needs to be flattened with the new visibility.
            if requested_expanded_set.issubset(cached_expanded_set):
                rank = (2, -len(cached_expanded_set))  # smallest complete superset
            elif cached_expanded_set.issubset(requested_expanded_set):
                rank = (1, len(cached_expanded_set))  # largest useful subset
            else:
                continue
            if best_rank is None or rank > best_rank:
                best_entry = self._hierarchy_view_cache_get(cache_key)
                best_rank = rank
        return best_entry

    def _hierarchy_root_count_cache_get(self, cache_key: str) -> Optional[int]:
        self._sync_controller_cache_generation()
        cached_entry = self._hierarchy_root_count_cache.get(cache_key)
        if not cached_entry:
            self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_COUNT, hit=False)
            return None
        count, expires_at = cached_entry
        if time.time() > expires_at:
            self._hierarchy_root_count_cache.pop(cache_key, None)
            self.cache_coordinator.record_lookup(
                CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_COUNT,
                hit=False,
                expired=True,
            )
            return None
        self._hierarchy_root_count_cache.move_to_end(cache_key)
        self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_COUNT, hit=True)
        return int(count)

    def _hierarchy_root_count_cache_set(self, cache_key: str, count: int) -> None:
        self._sync_controller_cache_generation()
        self.hierarchy_cache_registry.store(
            "root_count", cache_key, int(count),
            ttl=self._hierarchy_root_count_cache_ttl,
            max_size=self._hierarchy_root_count_cache_size,
        )

    def _hierarchy_root_page_cache_get(self, cache_key: str) -> Optional[List[Any]]:
        self._sync_controller_cache_generation()
        cached_entry = self._hierarchy_root_page_cache.get(cache_key)
        if not cached_entry:
            self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_PAGE, hit=False)
            return None
        values, expires_at = cached_entry
        if time.time() > expires_at:
            self._hierarchy_root_page_cache.pop(cache_key, None)
            self.cache_coordinator.record_lookup(
                CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_PAGE,
                hit=False,
                expired=True,
            )
            return None
        self._hierarchy_root_page_cache.move_to_end(cache_key)
        self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_PAGE, hit=True)
        return list(values or [])

    def _hierarchy_root_page_cache_set(self, cache_key: str, values: List[Any]) -> None:
        self._sync_controller_cache_generation()
        self.hierarchy_cache_registry.store(
            "root_page", cache_key, list(values or []),
            ttl=self._hierarchy_root_page_cache_ttl,
            max_size=self._hierarchy_root_page_cache_size,
        )

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

    def _collapsed_root_result_cache_key(self, root_spec: PivotSpec) -> str:
        """Key a complete, collapsed root aggregate independently of a window."""
        return self._hierarchy_spec_fingerprint(root_spec)

    def _hierarchy_root_result_cache_get(self, cache_key: str) -> Optional[pa.Table]:
        self._sync_controller_cache_generation()
        cached_entry = self._hierarchy_root_result_cache.get(cache_key)
        if not cached_entry:
            self.cache_coordinator.record_lookup(
                CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_RESULT, hit=False
            )
            return None
        result, expires_at = cached_entry
        if expires_at < time.time() or not isinstance(result, pa.Table):
            self._hierarchy_root_result_cache.pop(cache_key, None)
            self.cache_coordinator.record_lookup(
                CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_RESULT,
                hit=False,
                expired=True,
            )
            return None
        self._hierarchy_root_result_cache.move_to_end(cache_key)
        self.cache_coordinator.record_lookup(
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_RESULT, hit=True
        )
        return result

    def _hierarchy_root_result_cache_set(self, cache_key: str, result: pa.Table) -> bool:
        self._sync_controller_cache_generation()
        if not isinstance(result, pa.Table):
            return False
        byte_count = max(int(getattr(result, "nbytes", 0) or 0), 0)
        if (
            self._collapsed_root_materialization_max_bytes <= 0
            or byte_count > self._collapsed_root_materialization_max_bytes
        ):
            return False
        return self.hierarchy_cache_registry.store(
            "root_result", cache_key, result,
            ttl=self._hierarchy_root_result_cache_ttl,
            max_size=self._hierarchy_root_result_cache_size,
        )

    def _hierarchy_grand_total_cache_get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        self._sync_controller_cache_generation()
        cached_entry = self._hierarchy_grand_total_cache.get(cache_key)
        if not cached_entry:
            self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_GRAND_TOTAL, hit=False)
            return None
        row, expires_at = cached_entry
        if expires_at < time.time():
            self._hierarchy_grand_total_cache.pop(cache_key, None)
            self.cache_coordinator.record_lookup(
                CacheNamespaces.CONTROLLER_HIERARCHY_GRAND_TOTAL,
                hit=False,
                expired=True,
            )
            return None
        self._hierarchy_grand_total_cache.move_to_end(cache_key)
        self.cache_coordinator.record_lookup(CacheNamespaces.CONTROLLER_HIERARCHY_GRAND_TOTAL, hit=True)
        return dict(row) if isinstance(row, dict) else None

    def _hierarchy_grand_total_cache_set(self, cache_key: str, row: Dict[str, Any]) -> None:
        self._sync_controller_cache_generation()
        if not isinstance(row, dict):
            return
        self.hierarchy_cache_registry.store(
            "grand_total", cache_key, dict(row),
            ttl=self._hierarchy_grand_total_cache_ttl,
            max_size=self._hierarchy_grand_total_cache_size,
        )

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
        if getattr(spec, "group_allow_unbalanced", False):
            return False
        if expanded_paths:
            return False
        axis_config = self.planner._measure_axis_config(spec)
        if (
            axis_config
            and axis_config.placement == "rows"
            and spec.rows[0] == axis_config.label_field
        ):
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

        base_table = self.planner.builder.apply_custom_dimensions(
            self.resolve_table(spec.table),
            getattr(spec, "custom_dimensions", []),
        )
        if spec.filters:
            filter_expr = self.planner.builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)

        root_group_cols = list(spec.rows[:1])
        if root_group_cols:
            count_expr = base_table.select(root_group_cols).distinct().count()
        else:
            count_expr = base_table.count()

        total_rows = int(await self._execute_ibis_scalar_async(count_expr, spec.table))
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

        base_table = self.planner.builder.apply_custom_dimensions(
            self.resolve_table(spec.table),
            getattr(spec, "custom_dimensions", []),
        )
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

        key_table = await self._execute_ibis_expr_async(key_query, spec.table)
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
        self._mark_measure_axis_exact_layout(root_spec)
        root_spec.totals = False
        root_spec.sort = self._build_group_rows_sort(root_spec.rows, spec.sort, spec.column_sort_options)

        total_count_started_at = time.perf_counter()
        root_total_rows = await self._count_collapsed_root_rows(spec)
        total_count_finished_at = time.perf_counter()
        materialized_root_table: Optional[pa.Table] = None
        materialized_root_cache_state = "bypassed"
        can_materialize_root_result = (
            self._collapsed_root_materialization_max_rows > 0
            and self._collapsed_root_materialization_max_bytes > 0
            and 0 < root_total_rows <= self._collapsed_root_materialization_max_rows
        )

        if can_materialize_root_result:
            page_key_fetch_started_at = None
            page_key_fetch_finished_at = None
            root_cache_key = self._collapsed_root_result_cache_key(root_spec)
            materialized_root_table = self._hierarchy_root_result_cache_get(root_cache_key)
            root_query_profile: Dict[str, Any] = {}
            if materialized_root_table is None:
                materialized_root_spec = root_spec.copy()
                materialized_root_spec.limit = 0
                materialized_root_spec.offset = 0
                root_query_started_at = time.perf_counter()
                root_table = await self.run_pivot_async(
                    materialized_root_spec,
                    return_format="arrow",
                    force_refresh=False,
                    profile_sink=root_query_profile if profiling else None,
                )
                root_query_finished_at = time.perf_counter()
                if isinstance(root_table, pa.Table):
                    materialized_root_table = root_table
                    materialized_root_cache_state = (
                        "stored"
                        if self._hierarchy_root_result_cache_set(root_cache_key, root_table)
                        else "not_stored"
                    )
            else:
                root_query_profile = {"cached": True}
                root_query_started_at = None
                root_query_finished_at = None
                materialized_root_cache_state = "hit"

            root_rows = (
                materialized_root_table.slice(safe_start, page_size).to_pylist()
                if isinstance(materialized_root_table, pa.Table)
                else []
            )
        else:
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
                root_query_profile = {}
                root_query_started_at = time.perf_counter()
                root_table = await self.run_pivot_async(
                    root_spec,
                    return_format="arrow",
                    force_refresh=False,
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
                    force_refresh=False,
                    profile_sink=root_query_profile if profiling else None,
                )
                root_query_finished_at = time.perf_counter()
                root_rows = root_table.to_pylist() if isinstance(root_table, pa.Table) else []
        grand_total_row = None
        grand_total_formula_source_rows = []
        grand_total_index = root_total_rows if spec.totals else None
        natural_grand_total_in_window = (
            grand_total_index is not None
            and safe_start <= grand_total_index <= safe_end
        )
        should_fetch_grand_total = bool(
            spec.totals
            and (include_grand_total_row or natural_grand_total_in_window)
        )

        if should_fetch_grand_total:
            total_cache_key = self._hierarchy_spec_fingerprint(spec)
            grand_total_row = self._hierarchy_grand_total_cache_get(total_cache_key)
            if grand_total_row is None:
                total_spec = spec.copy()
                total_spec.rows = []
                self._mark_measure_axis_exact_layout(total_spec)
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
                formula_source_root_rows = root_rows
                if len(root_rows) < max(root_total_rows, 0):
                    if isinstance(materialized_root_table, pa.Table):
                        formula_source_root_rows = materialized_root_table.to_pylist()
                    else:
                        formula_source_spec = spec.copy()
                        formula_source_spec.rows = list(spec.rows[:1])
                        self._mark_measure_axis_exact_layout(formula_source_spec)
                        formula_source_spec.limit = 0
                        formula_source_spec.offset = 0
                        formula_source_spec.totals = False
                        formula_source_table = await self.run_pivot_async(
                            formula_source_spec,
                            return_format="arrow",
                            force_refresh=True,
                        )
                        if isinstance(formula_source_table, pa.Table):
                            formula_source_root_rows = formula_source_table.to_pylist()
                grand_total_formula_source_rows = [
                    dict(row)
                    for row in formula_source_root_rows
                    if isinstance(row, dict)
                ]
                if natural_grand_total_in_window and not include_grand_total_row:
                    natural_total_row = dict(grand_total_row)
                    natural_total_row.pop("_depth", None)
                    root_rows = [*root_rows, natural_total_row]
        else:
            grand_total_query_profile = None
            grand_total_started_at = None
            grand_total_finished_at = None

        total_rows = root_total_rows + (1 if spec.totals else 0)

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
                "is_complete_hierarchy_result": False,
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
                        "rootMaterialization": materialized_root_cache_state,
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
            return self.resolve_table(table_name).schema()
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
        return hierarchy_lookup_key([row.get(dim) for dim in parent_dims])

    async def run_progressive_load(self, spec: PivotSpec, chunk_callback: Optional[Callable] = None):
        """Run progressive data loading for large datasets"""
        if self.progressive_loader is None:
            raise ValueError("Progressive data loading requires an Ibis connection.")
        result = await self.progressive_loader.load_progressive_chunks(spec, chunk_callback)
        return result

    @staticmethod
    def _sort_unbalanced_sibling_rows(
        sibling_rows: List[Dict[str, Any]],
        spec: PivotSpec,
    ) -> List[Dict[str, Any]]:
        sort_items = spec.sort if isinstance(spec.sort, list) else ([spec.sort] if isinstance(spec.sort, dict) else [])
        if not sort_items or len(sibling_rows) <= 1:
            return sibling_rows
        total_rows = [row for row in sibling_rows if is_hierarchy_grand_total(row)]
        regular_rows = [row for row in sibling_rows if not is_hierarchy_grand_total(row)]
        row_field_set = set(spec.rows or [])
        column_sort_options = spec.column_sort_options if isinstance(spec.column_sort_options, dict) else {}

        for sort_item in reversed(sort_items):
            if not isinstance(sort_item, dict):
                continue
            sort_field = sort_item.get("field")
            if not sort_field:
                continue
            desc = str(sort_item.get("order") or "asc").strip().lower() == "desc"
            static_opt = column_sort_options.get(sort_field) if isinstance(column_sort_options.get(sort_field), dict) else {}
            absolute_sort = bool(sort_item.get("absoluteSort", static_opt.get("absoluteSort", False)))
            sort_key_field = sort_item.get("sortKeyField") or static_opt.get("sortKeyField")

            present = []
            missing = []
            for row in regular_rows:
                if sort_field in ("hierarchy", "_id") or sort_field in row_field_set:
                    raw_val = row.get("_id")
                    if raw_val is None:
                        raw_val = row.get(sort_field)
                elif sort_key_field and row.get(sort_key_field) is not None:
                    raw_val = row.get(sort_key_field)
                else:
                    raw_val = row.get(sort_field)
                if raw_val is None or (isinstance(raw_val, float) and math.isnan(raw_val)):
                    missing.append(row)
                else:
                    present.append((raw_val, row))

            def _key_fn(item):
                val = item[0]
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    num = abs(float(val)) if absolute_sort else float(val)
                    return (0, num, "")
                return (1, 0.0, str(val).lower())

            present.sort(key=_key_fn, reverse=desc)
            regular_rows = [item[1] for item in present] + missing

        return [*total_rows, *regular_rows]

    async def _run_unbalanced_hierarchical_pivot_batch_load(
        self,
        spec: PivotSpec,
        target_paths: List[List[str]],
        *,
        include_root: bool = True,
        profile_sink: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        batch_started_at = time.perf_counter()
        rows = list(spec.rows or [])
        unbalanced_values = getattr(spec, "unbalanced_values", None)
        results: Dict[str, List[Dict[str, Any]]] = {"": []}
        level_profiles: Dict[str, Any] = {}

        level_specs: List[PivotSpec] = []
        for prefix_len in range(1, len(rows) + 1):
            level_spec = spec.copy()
            level_spec.rows = list(rows[:prefix_len])
            level_spec.totals = False
            level_spec.limit = max(int(getattr(spec, "limit", 1000) or 1000), 100000)
            level_spec.offset = 0
            self._mark_measure_axis_exact_layout(level_spec)
            level_specs.append(level_spec)

        async def _run_level(idx: int, level_spec: PivotSpec):
            prof: Dict[str, Any] = {}
            table = await self.run_pivot_async(
                level_spec,
                return_format="arrow",
                profile_sink=prof if profile_sink is not None else None,
            )
            if profile_sink is not None:
                level_profiles[f"level_{idx + 1}"] = prof
            return idx, table.to_pylist() if isinstance(table, pa.Table) else []

        level_coros = [_run_level(idx, level_spec) for idx, level_spec in enumerate(level_specs)]

        total_coro = None
        if include_root and spec.totals:
            total_spec = spec.copy()
            total_spec.rows = []
            total_spec.totals = False
            total_spec.limit = 1
            total_spec.offset = 0
            total_spec.sort = []
            self._mark_measure_axis_exact_layout(total_spec)
            total_coro = self.run_pivot_async(total_spec, return_format="arrow")

        if total_coro is not None:
            level_results, total_table = await asyncio.gather(
                asyncio.gather(*level_coros),
                total_coro,
            )
            total_rows_list = total_table.to_pylist() if isinstance(total_table, pa.Table) else []
            if total_rows_list:
                grand_total_row = dict(total_rows_list[0])
                grand_total_row["_id"] = "Grand Total"
                grand_total_row["_isTotal"] = True
                grand_total_row["_path"] = "__grand_total__"
                grand_total_row["depth"] = 0
                results[""].append(grand_total_row)
        else:
            level_results = await asyncio.gather(*level_coros)

        seen_entries_by_parent: Dict[str, Dict[str, Dict[str, Any]]] = {}
        dim_fields_set = set(rows) | set(spec.columns or [])
        measure_agg_by_alias = {
            str(getattr(m, "alias", "")): str(getattr(m, "agg", "sum") or "sum").lower()
            for m in (spec.measures or [])
            if getattr(m, "alias", None)
        }
        for level_idx, level_rows in level_results:
            target_field = rows[level_idx]
            prefix_fields = rows[:level_idx]
            for raw_row in level_rows:
                if not isinstance(raw_row, dict):
                    continue
                target_val = raw_row.get(target_field)
                if is_unbalanced_missing_value(target_val, unbalanced_values):
                    continue
                parent_parts = [
                    str(raw_row[field_name])
                    for field_name in prefix_fields
                    if not is_unbalanced_missing_value(raw_row.get(field_name), unbalanced_values)
                ]
                parent_key = hierarchy_lookup_key(parent_parts) if parent_parts else ""
                child_label_key = str(target_val)
                seen_children = seen_entries_by_parent.setdefault(parent_key, {})
                if child_label_key in seen_children:
                    existing_entry = seen_children[child_label_key]
                    for key, val in raw_row.items():
                        if key in dim_fields_set or str(key).startswith("_") or not isinstance(val, (int, float)) or isinstance(val, bool) or math.isnan(val):
                            continue
                        prev_val = existing_entry.get(key)
                        if not isinstance(prev_val, (int, float)) or isinstance(prev_val, bool) or math.isnan(prev_val):
                            existing_entry[key] = val
                            continue
                        agg_kind = measure_agg_by_alias.get(key, "sum")
                        if agg_kind == "min":
                            existing_entry[key] = min(prev_val, val)
                        elif agg_kind == "max":
                            existing_entry[key] = max(prev_val, val)
                        elif agg_kind in {"sum", "count"}:
                            existing_entry[key] = prev_val + val
                    continue
                row_entry = dict(raw_row)
                row_entry["_unbalanced_field"] = target_field
                row_entry["_id"] = target_val
                seen_children[child_label_key] = row_entry
                results.setdefault(parent_key, []).append(row_entry)

        for parent_key, sibling_rows in list(results.items()):
            results[parent_key] = self._sort_unbalanced_sibling_rows(sibling_rows, spec)

        if isinstance(profile_sink, dict):
            profile_sink.clear()
            profile_sink.update({
                "includeRoot": include_root,
                "unbalanced": True,
                "levels": level_profiles,
                "totalMs": self._profile_ms(batch_started_at, time.perf_counter()),
            })
        return results

    async def run_hierarchical_pivot_batch_load(
        self,
        spec_dict: Dict[str, Any],
        target_paths: List[List[str]],
        max_levels: int = 3,
        *,
        include_root: bool = True,
        profile_sink: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
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
        batch_started_at = time.perf_counter()
        spec = PivotSpec.from_dict(spec_dict)
        results = {}
        level_profiles: Dict[str, Any] = {}

        def _finish_batch_profile() -> None:
            if isinstance(profile_sink, dict):
                profile_sink.clear()
                profile_sink.update({
                    "includeRoot": include_root,
                    "levels": level_profiles,
                    "totalMs": self._profile_ms(batch_started_at, time.perf_counter()),
                })

        rows = spec.rows or []
        if not rows:
            _finish_batch_profile()
            return results

        if getattr(spec, "group_allow_unbalanced", False):
            return await self._run_unbalanced_hierarchical_pivot_batch_load(
                spec,
                target_paths,
                include_root=include_root,
                profile_sink=profile_sink,
            )

        max_group_len = min(len(rows), max_levels + 1)
        expand_all = any(path == ['__ALL__'] for path in (target_paths or []))
        valid_node_keys_by_depth: Dict[int, set[str]] = {}

        col_sort_opts = spec.column_sort_options
        # A delta expansion already owns cached root rows. Re-querying them made
        # every first expansion pay for both the root aggregation and its child
        # aggregation. Full loads still fetch the root as before.
        if include_root:
            root_spec = spec.copy()
            root_spec.rows = rows[:1]
            self._mark_measure_axis_exact_layout(root_spec)
            root_spec.limit = self.max_hierarchy_rows
            root_spec.sort = self._build_group_rows_sort(root_spec.rows, spec.sort, col_sort_opts)
            root_spec.totals = bool(spec.totals)

            root_profile: Dict[str, Any] = {}
            root_started_at = time.perf_counter()
            try:
                root_table = await self.run_pivot_async(
                    root_spec,
                    return_format="arrow",
                    profile_sink=root_profile if profile_sink is not None else None,
                )
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
            level_profiles["root"] = {
                "totalMs": self._profile_ms(root_started_at, time.perf_counter()),
                **root_profile,
            }

        # 2) Collapsed mode: no expanded paths, nothing deeper to load.
        if not expand_all and not target_paths:
            _finish_batch_profile()
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
                {self._path_key(p) for p in (parent_paths or [])}
                if parent_paths
                else None
            )

            level_spec = spec.copy()
            level_spec.rows = group_rows
            self._mark_measure_axis_exact_layout(level_spec)
            level_spec.limit = self.max_hierarchy_rows
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

            level_profile: Dict[str, Any] = {}
            level_started_at = time.perf_counter()
            try:
                level_table = await self.run_pivot_async(
                    level_spec,
                    return_format="arrow",
                    profile_sink=level_profile if profile_sink is not None else None,
                )
            except Exception as e:
                print(f"Error loading level group_len={group_len}: {e}")
                continue

            level_profiles[f"depth{group_len}"] = {
                "totalMs": self._profile_ms(level_started_at, time.perf_counter()),
                **level_profile,
            }

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

        _finish_batch_profile()
        return results

    def _flatten_hierarchy_rows(self, spec: PivotSpec, hierarchy_result: Dict[str, List[Dict[str, Any]]], expanded_paths: List[List[str]]) -> List[Dict[str, Any]]:
        """Build visible hierarchy rows in parent-before-children order from batch-loaded levels."""
        return build_visible_hierarchy_rows(spec, hierarchy_result, expanded_paths)

    def _flatten_hierarchy_rows_window(
        self,
        spec: PivotSpec,
        hierarchy_result: Dict[str, List[Dict[str, Any]]],
        expanded_paths: List[List[str]],
        start_row: Optional[int] = None,
        end_row: Optional[int] = None,
        collect_formula_source_rows: bool = False,
        show_subtotal_footers: bool = False,
        tabular_subtotals: bool = False,
    ) -> Dict[str, Any]:
        """Flatten hierarchy into only the requested window while counting the full visible size."""
        return build_hierarchy_row_window(
            spec,
            hierarchy_result,
            expanded_paths,
            start_row=start_row,
            end_row=end_row,
            collect_formula_source_rows=collect_formula_source_rows,
            show_subtotal_footers=show_subtotal_footers,
            tabular_subtotals=tabular_subtotals,
        )

    @staticmethod
    def _is_hierarchy_grand_total(row: Dict[str, Any]) -> bool:
        return is_hierarchy_grand_total(row)

    def _find_grand_total_row(self, rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return find_hierarchy_grand_total_row(rows)

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
        return compute_hierarchy_color_scale_stats(rows, row_fields, col_fields)

    async def run_hierarchy_view(
        self,
        spec: PivotSpec,
        expanded_paths: List[List[str]],
        start_row: Optional[int] = None,
        end_row: Optional[int] = None,
        include_grand_total_row: bool = False,
        profiling: bool = False,
        show_subtotal_footers: bool = False,
        tabular_subtotals: bool = False,
    ) -> Dict[str, Any]:
        """Run hierarchy query through the adapter service boundary."""
        return await self.hierarchy_query_service.execute(
            HierarchyQuery(
                spec=spec,
                expanded_paths=expanded_paths or [],
                start_row=start_row,
                end_row=end_row,
                include_grand_total_row=include_grand_total_row,
                profiling=profiling,
                show_subtotal_footers=show_subtotal_footers,
                tabular_subtotals=tabular_subtotals,
            )
        )

    async def _run_hierarchy_view_legacy(
        self,
        spec: PivotSpec,
        expanded_paths: List[List[str]],
        start_row: Optional[int] = None,
        end_row: Optional[int] = None,
        include_grand_total_row: bool = False,
        profiling: bool = False,
        show_subtotal_footers: bool = False,
        tabular_subtotals: bool = False,
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
            cache_key = f"{spec_fingerprint}:{hashlib.sha256(self._stable_json(normalized_expanded).encode()).hexdigest()[:16]}:sf{int(show_subtotal_footers)}:ts{int(tabular_subtotals)}"
            cache_lookup_started_at = time.perf_counter()
            cached_view = self._hierarchy_view_cache_get(cache_key)
            cache_lookup_finished_at = time.perf_counter()

            hierarchy_result: Dict[str, List[Dict[str, Any]]]
            cached_visible_rows: Optional[List[Dict[str, Any]]] = None
            hierarchy_load_started_at = None
            hierarchy_load_finished_at = None
            hierarchy_batch_profile: Dict[str, Any] = {}
            flatten_started_at = None
            flatten_finished_at = None
            window_slice_started_at = None
            window_slice_finished_at = None
            reused_cache = False
            reuse_direction = None
            cache_hit = cached_view is not None
            cached_expanded_count = 0
            requested_expanded_set = set(normalized_expanded)

            if cached_view is not None:
                hierarchy_result = cached_view["hierarchy_result"]
                cached_visible_rows = cached_view.get("visible_rows") or None
            else:
                hierarchy_load_started_at = time.perf_counter()
                reusable_view = self._find_reusable_hierarchy_cache_entry(spec_fingerprint, requested_expanded_set)
                if reusable_view is not None:
                    reused_cache = True
                    hierarchy_result = reusable_view["hierarchy_result"]
                    cached_expanded_set = set(reusable_view.get("expanded_set") or set())
                    cached_expanded_count = len(cached_expanded_set)
                    if requested_expanded_set < cached_expanded_set:
                        reuse_direction = "collapse"
                    elif cached_expanded_set < requested_expanded_set:
                        reuse_direction = "expand"
                    else:
                        reuse_direction = "equivalent"
                    missing_paths = [
                        list(path_tuple)
                        for path_tuple in sorted(requested_expanded_set - cached_expanded_set)
                    ]
                    if missing_paths:
                        delta_hierarchy = await self.run_hierarchical_pivot_batch_load(
                            spec.to_dict(),
                            missing_paths,
                            max_levels=len(spec.rows),
                            include_root=False,
                            profile_sink=hierarchy_batch_profile if profiling else None,
                        )
                        for parent_key, rows in (delta_hierarchy or {}).items():
                            hierarchy_result[str(parent_key)] = self._clone_hierarchy_rows(rows)
                    hierarchy_load_finished_at = time.perf_counter()
                else:
                    hierarchy_result = await self.run_hierarchical_pivot_batch_load(
                        spec.to_dict(),
                        target_paths,
                        max_levels=len(spec.rows),
                        profile_sink=hierarchy_batch_profile if profiling else None,
                    )
                    hierarchy_load_finished_at = time.perf_counter()

            color_scale_started_at = time.perf_counter()
            if cached_visible_rows is not None:
                total_rows = len(cached_visible_rows)
                grand_total_row = self._find_grand_total_row(cached_visible_rows)
                grand_total_formula_source_rows = [
                    dict(row)
                    for row in cached_visible_rows
                    if isinstance(row, dict) and not self._is_hierarchy_grand_total(row) and row.get("depth") == 0
                ] if include_grand_total_row else []
                color_scale_stats = self._compute_color_scale_stats(
                    cached_visible_rows, spec.rows, getattr(spec, 'columns', [])
                )
                color_scale_finished_at = time.perf_counter()
                window_slice_started_at = time.perf_counter()
                if start_row is not None and end_row is not None:
                    window_rows = cached_visible_rows[start_row:end_row + 1]
                else:
                    window_rows = cached_visible_rows
                window_slice_finished_at = time.perf_counter()
                visible_rows_to_cache = cached_visible_rows
            else:
                flatten_started_at = time.perf_counter()
                flattened = self._flatten_hierarchy_rows_window(
                    spec,
                    hierarchy_result,
                    target_paths,
                    start_row,
                    end_row,
                    collect_formula_source_rows=include_grand_total_row,
                    show_subtotal_footers=show_subtotal_footers,
                    tabular_subtotals=tabular_subtotals,
                )
                flatten_finished_at = time.perf_counter()
                window_rows = flattened["rows"]
                total_rows = flattened["total_rows"]
                grand_total_row = flattened["grand_total_row"]
                grand_total_formula_source_rows = flattened["grand_total_formula_source_rows"]
                color_scale_stats = flattened["color_scale_stats"]
                color_scale_finished_at = time.perf_counter()
                visible_rows_to_cache = flattened.get("full_rows") or []

            cache_store_started_at = time.perf_counter()
            self._hierarchy_view_cache_set(cache_key, {
                "spec_fingerprint": spec_fingerprint,
                "expanded_set": set(normalized_expanded),
                "is_complete_hierarchy_result": True,
                "hierarchy_result": hierarchy_result,
                "visible_rows": visible_rows_to_cache,
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
                            "reuseDirection": reuse_direction,
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
                        },
                        "controllerHierarchyBatch": hierarchy_batch_profile,
                    }
                    if profiling
                    else None
                ),
            }

    async def run_hierarchical_progressive(self, spec: PivotSpec, expanded_paths: List[List[str]], level_callback: Optional[Callable] = None):
        """Run hierarchical data loading progressively by levels"""
        if self.progressive_loader is None:
            raise ValueError("Hierarchical progressive loading requires an Ibis connection.")
        result = await self.progressive_loader.load_hierarchical_progressive(spec, expanded_paths, level_callback)
        return result

    async def run_pivot_async(
        self,
        spec: Any,
        return_format: str = "arrow",
        force_refresh: bool = False,
        profile_sink: Optional[Dict[str, Any]] = None,
        raise_execution_errors: bool = False,
    ) -> Union[Dict[str, Any], pa.Table]:
        """Delegate pivot execution to the dedicated query-execution service."""
        token = _raise_query_execution_errors.set(raise_execution_errors)
        try:
            return await self.query_executor.execute(
                spec,
                return_format=return_format,
                force_refresh=force_refresh,
                profile_sink=profile_sink,
            )
        finally:
            _raise_query_execution_errors.reset(token)

    async def _trigger_materialization(self, spec: PivotSpec):
        """Helper to run materialization in background"""
        try:
            if self._uses_duckdb_ibis():
                # Disabled for DuckDB shared-connection mode (see caller guard).
                return
            if self.materialized_hierarchy_manager is None:
                return
            loop = asyncio.get_running_loop()

            def _materialize_with_lock():
                with self._execution_lock_for_table(getattr(spec, "table", None)):
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
        cache_spec = self.diff_engine._normalize_spec_for_hash(spec)

        # Lock wrapper removed

        for query_expr in queries_to_run:
            if hasattr(query_expr, 'to_pyarrow'):
                tasks.append(self._execute_ibis_expr_async(query_expr, spec.table))
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
                    if _raise_query_execution_errors.get():
                        raise r
                    print(f"Query execution error [{idx}] {debug_context}: {r}")
                    valid_results.append(pa.table({}))
                else:
                    query_expr = queries_to_run[idx]
                    # Keep standard async execution on the same cache contract
                    # as diff planning. A later identical request can then
                    # resolve its Arrow result without another Ibis execution.
                    self.cache.set(
                        self.diff_engine._cache_key_for_query(query_expr, cache_spec),
                        r,
                    )
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
        final_table = self.diff_engine.merge_and_finalize(
            [main_result] if results and main_result is not None else [],
            plan_result,
            spec,
            strategy,
        )
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
            grouped_table = await self._execute_ibis_expr_async(grouped_expr, spec.table)
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
                col_results_table = await self._execute_ibis_expr_async(col_ibis_expr, spec.table)
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
            pivot_results_table = await self._execute_ibis_expr_async(pivot_ibis_expr, spec.table)
                
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

    async def run_materialized_hierarchy(
        self,
        spec: PivotSpec,
        *,
        job_owner: Optional[str] = None,
    ):
        """Run hierarchical pivot using materialized rollups (Async Job)"""
        job_id = await self.materialized_hierarchy_manager.create_materialized_hierarchy_async(
            spec,
            owner=job_owner,
        )
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
        """Stream a pivot export through the dedicated export executor."""
        async for chunk in self.export_executor.stream(spec, export_format=format):
            yield chunk

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
        table_expr = self.planner.builder.apply_custom_dimensions(
            self.resolve_table(spec.table),
            getattr(spec, "custom_dimensions", []),
        )

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
                except Exception:  # nosec B110
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
        if hasattr(self.planner, "get_table_schema_and_columns"):
            try:
                _, cols = self.planner.get_table_schema_and_columns(table_name)
                if cols:
                    return list(cols)
            except Exception as exc:
                logging.getLogger(__name__).debug("Planner schema lookup failed for %s: %s", table_name, exc)
        table_expr = self.resolve_table(table_name)
        return list(getattr(table_expr, "columns", []) or [])

    def _mutation_executor(self) -> DatabaseMutationExecutor:
        executor = getattr(self, "mutation_executor", None)
        if not isinstance(executor, DatabaseMutationExecutor):
            executor = DatabaseMutationExecutor(self)
            self.mutation_executor = executor
        return executor

    @staticmethod
    def _sanitize_column_mapping(
        source: Optional[Dict[str, Any]],
        *,
        allowed_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        return DatabaseMutationExecutor._sanitize_column_mapping(
            source,
            allowed_columns=allowed_columns,
            exclude_columns=exclude_columns,
        )

    @staticmethod
    def _build_where_clause(key_columns: Dict[str, Any]) -> tuple[str, List[Any]]:
        return DatabaseMutationExecutor._build_where_clause(key_columns)

    def _count_matching_rows_sync(self, table_name: str, key_columns: Dict[str, Any]) -> int:
        table_expr = self.resolve_table(table_name)
        filtered = table_expr
        for column, value in key_columns.items():
            if value is None:
                filtered = filtered.filter(filtered[column].isnull())
            elif isinstance(value, (list, tuple, set, frozenset)):
                filtered = filtered.filter(filtered[column].isin(list(value)))
            else:
                filtered = filtered.filter(filtered[column] == value)
        return int(filtered.count().execute())

    def _fetch_matching_rows_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        table_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        table_expr = self.resolve_table(table_name)
        filtered = table_expr
        for column, value in key_columns.items():
            if value is None:
                filtered = filtered.filter(filtered[column].isnull())
            elif isinstance(value, (list, tuple, set, frozenset)):
                filtered = filtered.filter(filtered[column].isin(list(value)))
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
        return self._mutation_executor()._fetch_matching_rows_with_rowid_sync(
            table_name,
            key_columns,
            table_columns=table_columns,
        )

    @staticmethod
    def _resolve_grouping_scope_paths_for_row(row: Dict[str, Any], grouping_fields: List[str]) -> List[str]:
        return DatabaseMutationExecutor._resolve_grouping_scope_paths_for_row(row, grouping_fields)

    @staticmethod
    def _new_scope_value_accumulator() -> Dict[str, Any]:
        return DatabaseMutationExecutor._new_scope_value_accumulator()

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
        self._mutation_executor()._accumulate_scope_value(
            accumulator,
            row,
            target_column=target_column,
            aggregation_fn=aggregation_fn,
            weight_field=weight_field,
            side=side,
        )

    @staticmethod
    def _finalize_scope_value_accumulator(accumulator: Dict[str, Any], aggregation_fn: str, side: str) -> Optional[float]:
        return DatabaseMutationExecutor._finalize_scope_value_accumulator(accumulator, aggregation_fn, side)

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
        return self._mutation_executor()._build_scope_value_changes_from_row_pairs(
            row_pairs,
            grouping_fields=grouping_fields,
            direct_scope_id=direct_scope_id,
            measure_id=measure_id,
            target_column=target_column,
            aggregation_fn=aggregation_fn,
            weight_field=weight_field,
        )

    def _build_direct_scope_value_changes(
        self,
        before_rows: List[Dict[str, Any]],
        *,
        row_path: Optional[str],
        measure_id: str,
        target_column: str,
        next_value: Any,
    ) -> List[Dict[str, Any]]:
        return self._mutation_executor()._build_direct_scope_value_changes(
            before_rows,
            row_path=row_path,
            measure_id=measure_id,
            target_column=target_column,
            next_value=next_value,
        )

    @staticmethod
    def _new_history_transaction() -> Dict[str, List[Dict[str, Any]]]:
        return DatabaseMutationExecutor._new_history_transaction()

    @staticmethod
    def _compact_history_transaction(transaction: Dict[str, List[Dict[str, Any]]]) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        return DatabaseMutationExecutor._compact_history_transaction(transaction)

    @staticmethod
    def _build_inverse_update_values(before_rows: List[Dict[str, Any]], update_columns: List[str]) -> Optional[Dict[str, Any]]:
        return DatabaseMutationExecutor._build_inverse_update_values(before_rows, update_columns)

    @staticmethod
    def _normalize_aggregation_name(value: Any) -> str:
        return DatabaseMutationExecutor._normalize_aggregation_name(value)

    @staticmethod
    def _normalize_aggregate_propagation_strategy(value: Any) -> str:
        return DatabaseMutationExecutor._normalize_aggregate_propagation_strategy(value)

    @staticmethod
    def _coerce_numeric_value(value: Any) -> Optional[float]:
        return DatabaseMutationExecutor._coerce_numeric_value(value)

    @staticmethod
    def _coerce_number_for_storage(original_value: Any, numeric_value: float) -> Any:
        return DatabaseMutationExecutor._coerce_number_for_storage(original_value, numeric_value)

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
        return DatabaseMutationExecutor._is_integer_dtype(dtype)

    def _execute_parameterized_fetchone(self, sql: str, params: List[Any]) -> Any:
        return self._mutation_executor().execute_parameterized_fetchone(sql, params)

    def _execute_parameterized_fetchall(self, sql: str, params: List[Any]) -> List[Any]:
        con = self.planner.con
        adapted_sql = self._mutation_executor()._adapt_sql_placeholders(sql)
        self._mutation_executor()._ensure_mutations_supported()
        if hasattr(con, "con"):
            result = con.con.execute(adapted_sql, [*params])
        elif hasattr(con, "execute"):
            result = con.execute(adapted_sql, [*params])
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
        return self._mutation_executor()._fetch_aggregate_edit_summary_sync(
            table_name,
            key_columns,
            aggregate_edit,
        )

    @staticmethod
    def _build_history_aggregate_update_entry(
        aggregate_edit: Dict[str, Any],
        *,
        value: Any,
        old_value: Any,
    ) -> Optional[Dict[str, Any]]:
        return DatabaseMutationExecutor._build_history_aggregate_update_entry(
            aggregate_edit,
            value=value,
            old_value=old_value,
        )

    def _apply_integer_delta_distribution_sync(
        self,
        table_name: str,
        target_column: str,
        where_sql: str,
        where_params: List[Any],
        matched_row_count: int,
        total_delta: int,
    ) -> int:
        return self._mutation_executor()._apply_integer_delta_distribution_sync(
            table_name,
            target_column,
            where_sql,
            where_params,
            matched_row_count,
            total_delta,
        )

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
        return self._mutation_executor()._apply_integer_proportional_sync(
            table_name,
            target_column,
            where_sql,
            where_params,
            matched_row_count,
            scale_factor,
            target_sum,
        )

    def _apply_set_based_aggregate_edit_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        aggregate_edit: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        return self._mutation_executor()._apply_set_based_aggregate_edit_sync(
            table_name,
            key_columns,
            aggregate_edit,
        )

    def _compute_aggregate_edit_value(
        self,
        rows: List[Dict[str, Any]],
        column: str,
        aggregation_fn: str,
        weight_field: Optional[str] = None,
    ) -> Optional[float]:
        return self._mutation_executor()._compute_aggregate_edit_value(
            rows,
            column,
            aggregation_fn,
            weight_field=weight_field,
        )

    def _plan_aggregate_edit_rewrites(
        self,
        before_rows: List[Dict[str, Any]],
        aggregate_edit: Dict[str, Any],
    ) -> tuple[List[Dict[str, Dict[str, Any]]], Dict[str, Any]]:
        return self._mutation_executor()._plan_aggregate_edit_rewrites(before_rows, aggregate_edit)

    def _build_insert_statement(self, table_name: str, row_data: Dict[str, Any], table_columns: List[str]) -> Optional[tuple[str, List[Any]]]:
        return self._mutation_executor()._build_insert_statement(table_name, row_data, table_columns)

    async def apply_row_transaction(self, table_name: str, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Apply an atomic add/remove/update/upsert transaction and return a structured result.
        Uses self._execute_parameterized_mutation(sql, [*updates.values(), *where_params]).
        """
        return await self._mutation_executor().apply_row_transaction(table_name, transaction)

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

    def _delete_cache_keys(self, predicate: Callable[[str], bool]) -> bool:
        if not hasattr(self.cache, "get_all_keys") or not hasattr(self.cache, "delete"):
            return False
        for raw_key in list(self.cache.get_all_keys() or []):
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
            if predicate(key):
                self.cache.delete(raw_key)
        return True

    def _clear_mutation_caches(self, table_name: Optional[str] = None, structural: bool = False) -> None:
        """Invalidate caches affected by row mutations without flushing unrelated cache namespaces."""
        namespaces = [
            CacheNamespaces.CONTROLLER_QUERY,
            CacheNamespaces.CONTROLLER_HIERARCHY_VIEW,
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_RESULT,
            CacheNamespaces.CONTROLLER_HIERARCHY_GRAND_TOTAL,
        ]
        if structural:
            namespaces.extend(
                [
                    CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_COUNT,
                    CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_PAGE,
                ]
            )
        coordinator = getattr(self, "cache_coordinator", None)
        if coordinator is None:
            event = CacheInvalidationEvent(
                sequence=0,
                event="mutation",
                namespaces=tuple(namespaces),
                reason="row_mutation",
                table=str(table_name) if table_name is not None else None,
                structural=structural,
            )
            self._invalidate_controller_query_cache(event)
            self._hierarchy_view_cache.clear()
            root_result_cache = getattr(self, "_hierarchy_root_result_cache", None)
            if root_result_cache is not None and hasattr(root_result_cache, "clear"):
                root_result_cache.clear()
            self._hierarchy_grand_total_cache.clear()
            if structural:
                self._hierarchy_root_count_cache.clear()
                self._hierarchy_root_page_cache.clear()
            return
        coordinator.invalidate(
            namespaces,
            event="mutation",
            reason="row_mutation",
            table=table_name,
            structural=structural,
        )

    def _execute_parameterized_mutation(self, sql: str, params: List[Any]) -> None:
        self._mutation_executor()._ensure_mutations_supported()
        sql = self._mutation_executor()._adapt_sql_placeholders(sql)
        con = self.planner.con
        if hasattr(con, "con") and hasattr(con.con, "execute"):
            con.con.execute(sql, [*params])
        elif hasattr(con, "execute"):
            con.execute(sql, [*params])
        elif hasattr(con, "raw_sql"):
            # Ibis raw_sql does not accept positional parameters; try to reach the
            # underlying native connection before falling back (which would drop params).
            underlying = getattr(con, "_con", None) or getattr(con, "con", None)
            if underlying is not None and hasattr(underlying, "execute"):
                underlying.execute(sql, [*params])
            else:
                raise NotImplementedError(
                    "raw_sql backend does not support parameterized queries; "
                    "use a backend that exposes .execute() or .con.execute()"
                )
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
        self._mutation_executor()._ensure_mutations_supported()
        if not table_name.isidentifier() or not column.isidentifier() or not id_column.isidentifier():
            raise ValueError("Invalid identifier in update request")

        sql = f"UPDATE {table_name} SET {column} = ? WHERE {id_column} = ?"  # nosec B608
        loop = asyncio.get_running_loop()

        def execute_update():
            self._execute_parameterized_mutation(sql, [value, row_id])

        await loop.run_in_executor(None, execute_update)
        self._clear_mutation_caches(table_name)
        return {"status": "success", "updated": 1}
