"""ScalablePivotController: the data host every edge reads and writes through.

It owns the backend connection, the scoped table catalog, data loading, the
per-table execution locks, the cache coordinator, CDC/streaming and row
editing.  Every read is answered by the semantic core (``pivot_engine.core``)
through ``transport.core_bridge``; the controller exposes the stable
``run_pivot*`` / export / drill-through façades over it.
"""
from typing import Optional, Any, Dict, List, Mapping, Tuple, Union, Callable, Generator
import asyncio
import copy
import decimal
import logging
import threading
import time

import pyarrow as pa

from .backends.capabilities import BackendCapabilities, resolve_backend_capabilities
from .backends.ibis_backend import IbisBackend
from .backends.table_catalog import IbisTableCatalog
from .cache.generation_guard import GenerationGuard, GenerationGuardedCache
from .cache.memory_cache import MemoryCache
from .cache.redis_cache import RedisCache
from .cache_coordinator import CacheCoordinator
from .cdc.cdc_manager import PivotCDCManager
from .core.authority.scope import run_in_executor_scoped
from .core.cache import CacheManager, key_for
from .editing.mutation_executor import DatabaseMutationExecutor
from .execution_locks import TableExecutionLockRegistry
from .export_executor import PivotExportExecutor
from .lifecycle import get_task_manager
from .query_execution_service import PivotQueryExecutor, spec_fingerprint
from .resource_governor import ResourceGovernor, ResourceLimits, get_worker_resource_governor
from .streaming.streaming_processor import IncrementalMaterializedViewManager, StreamAggregationProcessor
from .types.pivot_spec import PivotSpec
from .util.sql_builder import quote_ident

#: Core results depend on the data: every data load or mutation clears them through the cache coordinator.
CORE_RESULT_NAMESPACE = "core.result"


class ScalablePivotController:
    """The single pivot controller: data host, catalog, locks, caches, CDC and editing."""

    def __init__(
        self,
        backend_uri: str = ":memory:",
        cache: Union[str, Any] = "memory",
        enable_streaming: bool = True,
        enable_incremental_views: bool = True,
        cache_ttl: int = 300,
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
        self.resource_governor = resource_governor or (
            ResourceGovernor(resource_limits) if resource_limits is not None
            else get_worker_resource_governor()
        )
        self.resource_limits = resource_limits or self.resource_governor.limits
        if self.resource_limits != self.resource_governor.limits:
            raise ValueError("Controller resource limits must match its worker governor")
        if self.distributed_runtime is not None and (cache == "redis" or isinstance(cache, RedisCache)):
            raise ValueError(
                "Distributed runtime requires a worker-local memory cache; "
                "Redis runtime state is not a general query-cache backend."
            )
        if backend_uri == ":memory:":
            backend_uri = ":memory:shared_pivot_db"
        self.cache = self._make_cache(cache, cache_ttl, cache_options)

        allow_memory_fallback = bool(cache_options.pop("allow_memory_fallback", False))
        try:
            self.backend: Any = IbisBackend(connection_uri=backend_uri)
        except Exception as e:
            is_default_memory = str(backend_uri).startswith(":memory:") or str(backend_uri) == "duckdb://:memory:"
            if not is_default_memory and not allow_memory_fallback:
                raise
            logging.getLogger(__name__).warning("Could not connect to database backend via Ibis: %s", e)
            self.backend = IbisBackend(connection_uri="duckdb://:memory:")
        self.con: Any = self.backend.con
        self.catalog = IbisTableCatalog(self.con)
        self.backend_capabilities: BackendCapabilities = (
            getattr(self.backend, "capabilities", None)
            or resolve_backend_capabilities(self.con, connection_uri=backend_uri)
        )

        generation_guard = (
            GenerationGuard(self.distributed_runtime, lambda: None)
            if self.distributed_runtime is not None
            else None
        )
        self.cache_coordinator = CacheCoordinator(generation_guard=generation_guard)
        if generation_guard is not None:
            self.cache = GenerationGuardedCache(self.cache, generation_guard)
        # Every invalidation of a core tier (local or from another process) bumps the epoch under this
        # lock; a read stores what it computed only if the epoch it started under is still current, so
        # a read that overlaps a load or mutation can never put its stale result back.
        self._core_epoch = 0
        self._core_epoch_lock = threading.Lock()
        # The core result tier: pivot tables keyed by spec fingerprint under the active scope (REQ-SEC-004),
        # dropped for a table on its load or mutation and expired after ``cache_ttl``.
        self.core_cache = CacheManager(
            budgets={"result": self.resource_governor.limits.cache_bytes},
            sizeof=lambda entry: int(getattr(entry[1], "nbytes", 1024)) +
                (len(repr(entry[2]).encode()) * 2 if len(entry) > 2 and entry[2] is not None else 0),
        )
        self._core_result_ttl = float(cache_ttl)
        self.cache_coordinator.register_namespace(
            CORE_RESULT_NAMESPACE,
            owner="controller",
            key_dimensions=("dataset", "spec_fingerprint", "scope_fingerprint"),
            size_getter=lambda: {"bytes": self.core_cache.used_bytes("result")},
            invalidator=lambda event: self._invalidate_core(
                lambda: self.core_cache.invalidate(dataset=event.table, tiers=("result",))),
        )
        from .core.physical.capabilities import level_tables
        from .core.physical.level_tables import LevelTableStore
        from .core.catalog import CatalogStore

        self.catalog_store = CatalogStore(self.con, budget=self.resource_governor.limits.cache_bytes)
        object.__setattr__(self.con, "_pivot_catalog_store", self.catalog_store)
        self.cache_coordinator.register_namespace(
            "core.catalog", owner="controller",
            key_dimensions=("dataset", "revision", "scope_fingerprint", "filters", "columns", "order"),
            size_getter=lambda: {"bytes": self.catalog_store.cache.used_bytes("catalog")},
            invalidator=lambda event: self._invalidate_core(lambda: self.catalog_store.invalidate(event.table)),
        )

        self.level_store = None
        if level_tables(self.con):
            self.level_store = LevelTableStore(self.con, budget=self.resource_governor.limits.cache_bytes)
            object.__setattr__(self.con, "_pivot_level_store", self.level_store)
            self.cache_coordinator.register_namespace(
                "core.level",
                owner="controller",
                key_dimensions=("dataset", "level_meaning", "scope_fingerprint", "revision"),
                size_getter=lambda: {"bytes": self.level_store.cache.used_bytes("level")},
                invalidator=lambda event: self._invalidate_core(lambda: self.level_store.invalidate(event.table)),
            )

        self.enable_streaming = enable_streaming
        self.enable_incremental_views = enable_incremental_views
        self.task_manager = get_task_manager()
        self.execution_locks = TableExecutionLockRegistry()
        # Integrations that read the historical default lock attribute keep it;
        # controller-owned execution uses the per-table registry.
        self.execution_lock = self.execution_locks.lock_for(None)

        if cdc_exact_state_store not in {"memory", "redis"}:
            raise ValueError("cdc_exact_state_store must be 'memory' or 'redis'.")
        self.streaming_processor: Optional[StreamAggregationProcessor] = None
        if enable_streaming:
            self.streaming_processor = self._make_streaming_processor(cdc_exact_state_store)
        self.incremental_view_manager = (
            IncrementalMaterializedViewManager(self.con) if enable_incremental_views else None
        )
        self.cdc_manager: Optional[PivotCDCManager] = None  # set by setup_cdc / setup_push_cdc

        self.export_executor = PivotExportExecutor(self)
        self.query_executor = PivotQueryExecutor(self)
        self.mutation_executor = DatabaseMutationExecutor(self)

    # ------------------------------------------------------------------ setup

    def _make_cache(self, cache: Union[str, Any], cache_ttl: int, cache_options: Dict[str, Any]) -> Any:
        if isinstance(cache, str):
            if cache == "redis":
                return RedisCache(**{k: v for k, v in cache_options.items() if k != "allow_memory_fallback"})
            if cache != "memory":
                raise ValueError(f"Unknown cache type: {cache}")
            cache = None
        made = cache or MemoryCache(
            ttl=cache_ttl, max_bytes=self.resource_governor.limits.cache_bytes,
            governor=self.resource_governor,
        )
        if isinstance(made, MemoryCache) and made.governor is not self.resource_governor:
            raise ValueError("Controller memory cache must use its worker governor")
        return made

    def _make_streaming_processor(self, cdc_exact_state_store: str) -> StreamAggregationProcessor:
        from .streaming.exact_state import CDCStateLimits, MemoryCDCStateStore

        cdc_state_limits = CDCStateLimits(
            max_groups=CDCStateLimits().max_groups,
            max_bytes=self.resource_limits.cdc_bytes,
            max_extrema_values=CDCStateLimits().max_extrema_values,
        )
        if cdc_exact_state_store == "redis":
            if self.distributed_runtime is None:
                raise ValueError("Redis CDC state requires a validated distributed Redis runtime.")
            from .streaming.exact_state import RedisCDCStateStore

            store: Any = RedisCDCStateStore(
                self.distributed_runtime, limits=cdc_state_limits, governor=self.resource_governor,
            )
        else:
            store = MemoryCDCStateStore(cdc_state_limits, governor=self.resource_governor)
        return StreamAggregationProcessor(state_store=store, cdc_state_limits=cdc_state_limits)

    def clear_cache(self) -> None:
        """Clear every controller-owned cache (forces fresh reads)."""
        if self.cache is not None and hasattr(self.cache, "clear"):
            self.cache.clear()
        self._invalidate_core(self.core_cache.invalidate)
        self._invalidate_core(self.catalog_store.invalidate)
        if self.level_store is not None:
            self._invalidate_core(self.level_store.invalidate)

    def _invalidate_core(self, drop: Callable[[], Any]) -> None:
        with self._core_epoch_lock:
            self._core_epoch += 1
            drop()

    def core_epoch(self) -> int:
        """The current core cache epoch: capture it before a read, pass it when storing the result."""
        with self._core_epoch_lock:
            return self._core_epoch

    def cached_core_result(self, spec: Any) -> Optional[pa.Table]:
        """The core pivot table of ``spec`` from the result tier (under the active scope), if fresh."""
        entry = self.core_cache.get(key_for("result", str(spec.table), spec_fingerprint(spec)))
        if entry is None or time.monotonic() - entry[0] > self._core_result_ttl:
            return None
        return entry[1]

    def store_core_result(self, spec: Any, table: pa.Table, epoch: int) -> None:
        """Store a computed result unless a load or mutation invalidated the core since ``epoch``."""
        with self._core_epoch_lock:
            if epoch != self._core_epoch:
                return
            self.core_cache.put(key_for("result", str(spec.table), spec_fingerprint(spec)), (time.monotonic(), table, None))

    def core_result_dict(self, spec: Any, table: pa.Table) -> Dict[str, Any]:
        """Reuse the JSON-ready response next to its Arrow result, with isolated caller ownership."""
        key = key_for("result", str(spec.table), spec_fingerprint(spec))
        entry = self.core_cache.get(key)
        if entry is not None and entry[1] is table and len(entry) > 2 and entry[2] is not None:
            return copy.deepcopy(entry[2])
        result = self._convert_table_to_dict(table, spec)
        with self._core_epoch_lock:
            current = self.core_cache.get(key)
            if current is entry and entry is not None and entry[1] is table:
                self.core_cache.put(key, (entry[0], table, copy.deepcopy(result)))
        return result

    def close(self) -> None:
        """Release the controller's caches and backend connection."""
        self.clear_cache()
        if hasattr(self.backend, "close"):
            self.backend.close()

    # ------------------------------------------------------------- data host

    def resolve_table(self, table_name: str) -> Any:
        """The table expression for ``table_name``, scoped by the active grant (REQ-SEC-001)."""
        self.catalog.set_backend(self.con)
        return self.catalog.resolve_table(table_name)

    def _get_table_schema(self, table_name: str) -> Any:
        try:
            return self.resolve_table(table_name).schema()
        except Exception:
            return None

    def table_schema(self, table_name: str) -> Tuple[Any, List[str]]:
        """(schema, column names) of a table: catalog metadata only, no rows are read."""
        self.catalog.set_backend(self.con)
        return self.catalog.get_schema_and_columns(table_name)

    def _get_table_columns(self, table_name: str) -> List[str]:
        try:
            self.catalog.set_backend(self.con)
            _, cols = self.catalog.get_schema_and_columns(table_name)
            if cols:
                return list(cols)
        except Exception as exc:
            logging.getLogger(__name__).debug("Catalog schema lookup failed for %s: %s", table_name, exc)
        return list(getattr(self.resolve_table(table_name), "columns", []) or [])

    def _set_connection(self, con: Any, backend: Optional[Any] = None) -> None:
        """Adopt another Ibis connection (a table registered from a different backend)."""
        self.backend = backend if backend is not None else IbisBackend(connection=con)
        self.con = getattr(self.backend, "con", con)
        self.catalog.set_backend(self.con)
        self.backend_capabilities = getattr(self.backend, "capabilities", None) or resolve_backend_capabilities(self.con)
        if self.incremental_view_manager is not None:
            self.incremental_view_manager = IncrementalMaterializedViewManager(self.con)

    def register_ibis_table(
        self,
        table_name: str,
        table_expr: Any,
        *,
        backend: Optional[Any] = None,
        register_checkpoint: bool = True,
    ) -> Any:
        """Register a lazy Ibis table expression without materializing it to Arrow."""
        if backend is not None:
            self._set_connection(getattr(backend, "con", None), backend)
        else:
            try:
                expr_backend = table_expr._find_backend(use_default=False)
            except Exception:
                expr_backend = None
            if expr_backend is not None and expr_backend is not self.con:
                self._set_connection(expr_backend)
        self.catalog.set_backend(self.con)
        self.catalog.register_table(table_name, table_expr)
        self._data_changed(table_name, "register_ibis_table")
        return table_expr

    def load_data_from_arrow(self, table_name: str, arrow_table: pa.Table, register_checkpoint: bool = True) -> None:
        """Create (or replace) ``table_name`` from an Arrow table."""
        if hasattr(self.backend, "load_arrow_table"):
            self.backend.load_arrow_table(table_name, arrow_table)
        elif hasattr(self.con, "create_table"):
            self.con.create_table(table_name, arrow_table, overwrite=True)
        else:
            quoted_table_name = quote_ident(table_name)
            self.con.execute(f"DROP TABLE IF EXISTS {quoted_table_name}")
            self.con.execute(f"CREATE TABLE {quoted_table_name} AS SELECT * FROM arrow_table")  # nosec B608
        self.catalog.invalidate(table_name)
        self._data_changed(table_name, "load_data_from_arrow")

    def _data_changed(self, table_name: str, reason: str) -> None:
        if hasattr(self.cache, "clear"):
            self.cache.clear()
        self.cache_coordinator.invalidate(event="data_load", reason=reason, table=table_name, structural=True)

    def _uses_duckdb_ibis(self) -> bool:
        return str(getattr(self.con, "name", "")).lower() == "duckdb"

    def _execution_lock_for_table(self, table_name: Optional[str]) -> threading.RLock:
        return self.execution_locks.lock_for(table_name)

    def execution_lock_metrics_snapshot(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Expose per-table queue contention to runtime profiling consumers."""
        return self.execution_locks.snapshot(table_name)

    def _sync_controller_cache_generation(self) -> int:
        return self.cache_coordinator.sync_generation()

    # ----------------------------------------------------------------- reads

    @staticmethod
    def _profile_ms(start: Optional[float], end: Optional[float]) -> Optional[float]:
        if start is None or end is None:
            return None
        return round((end - start) * 1000, 3)

    @staticmethod
    def _normalize_spec(spec: Any) -> Any:
        return PivotSpec.from_dict(spec) if isinstance(spec, dict) else spec

    async def run_pivot_async(
        self,
        spec: Any,
        return_format: str = "arrow",
        force_refresh: bool = False,
        profile_sink: Optional[Dict[str, Any]] = None,
        raise_execution_errors: bool = False,
    ) -> Union[Dict[str, Any], pa.Table]:
        """One row per leaf group (the ``run_pivot`` contract), answered by the semantic core."""
        return await self.query_executor.execute(
            spec, return_format=return_format, force_refresh=force_refresh, profile_sink=profile_sink,
        )

    def run_pivot(self, spec: Any, return_format: str = "arrow", force_refresh: bool = False) -> Union[Dict[str, Any], pa.Table]:
        """Synchronous ``run_pivot_async`` (for callers without an event loop)."""
        from .transport.core_bridge import core_locked_call, core_pivot_table

        normalized = self._normalize_spec(spec)
        table = None if force_refresh else self.cached_core_result(normalized)
        if table is None:
            epoch = self.core_epoch()  # before the read: a write during it voids the store
            table = core_locked_call(self, normalized.table, lambda: core_pivot_table(self.resolve_table, normalized))
            self.store_core_result(normalized, table, epoch)
        return self.core_result_dict(normalized, table) if return_format == "dict" else table

    def run_pivot_arrow(self, spec: Any) -> pa.Table:
        """``run_pivot`` as an Arrow table (Arrow Flight)."""
        result = self.run_pivot(spec, return_format="arrow")
        if not isinstance(result, pa.Table):
            raise ValueError(f"Expected PyArrow Table but got {type(result)}")
        return result

    async def run_pivot_arrow_async(self, spec: Any) -> pa.Table:
        """``run_pivot_async`` as an Arrow table."""
        result = await self.run_pivot_async(spec, return_format="arrow")
        if not isinstance(result, pa.Table):
            raise ValueError(f"Expected PyArrow Table but got {type(result)}")
        return result

    async def run_pivot_export(self, spec: Any, format: str = "csv") -> Generator[bytes, None, None]:
        """Stream a pivot export (bounded core pages) through the export executor."""
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
        """Source records under ``spec.filters`` and ``filters`` (drill-through), from the core.

        Returns ``{"rows": [...], "total_rows": n}``: the page (sorted, offset,
        limited) and the count of every matching record.
        """
        from .transport.core_bridge import core_drill_through, run_core_read

        result = await run_core_read(self, spec.table, lambda resolve: core_drill_through(
            resolve, spec, list(filters or []), {}, limit=limit, offset=offset,
            sort_col=sort_col, sort_dir=sort_dir, text_filter=text_filter,
        ))
        return {"rows": result.get("rows", []), "total_rows": int(result.get("total_rows", 0))}

    def _convert_table_to_dict(self, table: Optional[pa.Table], spec: PivotSpec) -> Dict[str, Any]:
        """An Arrow result as ``{"columns", "rows", "next_cursor"}`` with JSON-safe cells."""
        if table is None or table.num_rows == 0:
            return {"columns": [], "rows": [], "next_cursor": None}
        columns = list(table.column_names)

        def normalize_cell(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, decimal.Decimal):
                return float(value)
            if isinstance(value, float) and value != value:
                return None
            isoformat = getattr(value, "isoformat", None)
            if callable(isoformat):
                return isoformat()
            return value

        column_values = [table.column(column).to_pylist() for column in columns]
        rows = [[normalize_cell(value) for value in row] for row in zip(*column_values)]
        next_cursor = None
        if spec.limit and table.num_rows == spec.limit:  # a full page: continue after it, in the same order
            done = int((spec.cursor or {}).get("offset", 0)) if isinstance(spec.cursor, dict) else 0
            next_cursor = {"offset": done + table.num_rows}
        return {"columns": columns, "rows": rows, "next_cursor": next_cursor}

    # ------------------------------------------------------------ CDC / live

    async def setup_cdc(self, table_name: str, change_stream):
        """Setup CDC for real-time tracking of data changes"""
        # PivotCDCManager expects an Ibis connection
        con = self.con
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
        con = self.con
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

    # --------------------------------------------------------------- editing

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
        con = self.con
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
        """Invalidate the caches a row mutation makes stale, without flushing unrelated namespaces."""
        self.cache_coordinator.invalidate(
            [CORE_RESULT_NAMESPACE],
            event="mutation",
            reason="row_mutation",
            table=table_name,
            structural=structural,
        )

    def _execute_parameterized_mutation(self, sql: str, params: List[Any]) -> None:
        self._mutation_executor()._ensure_mutations_supported()
        sql = self._mutation_executor()._adapt_sql_placeholders(sql)
        con = self.con
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

        await run_in_executor_scoped(loop, None, execute_update)
        self._clear_mutation_caches(table_name)
        return {"status": "success", "updated": 1}
