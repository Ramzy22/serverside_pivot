"""
StreamAggregationProcessor - Real-time stream processing for pivot aggregations
"""
import asyncio
from dataclasses import dataclass
import hashlib
import json
import threading
import time
from typing import Dict, Any, Optional, Callable, List, Mapping
import pyarrow as pa
import ibis
from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.cdc.models import Change
from pivot_engine.streaming.exact_state import (
    CDCJobSnapshot,
    CDCStateStore,
    CanonicalScalarCodec,
    ExactCDCStateError,
    ExactGroupState,
    MemoryCDCStateStore,
    StateVersionConflict,
)
from ibis.expr.api import Table as IbisTable


@dataclass(frozen=True)
class StreamingJobResult:
    """A freshness-bearing streaming result that remains mapping-friendly."""

    status: str
    values: Dict[str, Any]
    reason: Optional[str] = None

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


class StreamAggregationProcessor:
    def __init__(self, kafka_config: Optional[Dict[str, Any]] = None, state_store: Optional[Any] = None):
        self.kafka_config = kafka_config or {}
        self.aggregation_jobs = {}
        self.stream_clients = {}
        self.job_tasks = {}
        self.job_results = {}
        self.job_callbacks = {}
        
        self.cdc_state_store: CDCStateStore
        if isinstance(state_store, CDCStateStore):
            # Exact state is a separate, narrow persistence domain.  Retain an
            # in-memory legacy store only for compatibility callbacks that have
            # not moved to process_change yet.
            from pivot_engine.streaming.state_store import MemoryStateStore

            self.state_store = MemoryStateStore()
            self.cdc_state_store = state_store
        # Initialize legacy State Store
        elif state_store:
            self.state_store = state_store
            self.cdc_state_store = MemoryCDCStateStore()
        else:
            # Check if we can use Redis from config
            if self.kafka_config.get('redis_host'):
                from pivot_engine.cache.redis_cache import RedisCache
                from pivot_engine.streaming.state_store import RedisStateStore
                try:
                    redis_cache = RedisCache(
                        host=self.kafka_config.get('redis_host', 'localhost'),
                        port=self.kafka_config.get('redis_port', 6379)
                    )
                    self.state_store = RedisStateStore(redis_cache)
                except Exception:
                    from pivot_engine.streaming.state_store import MemoryStateStore
                    self.state_store = MemoryStateStore()
            else:
                from pivot_engine.streaming.state_store import MemoryStateStore
                self.state_store = MemoryStateStore()
            self.cdc_state_store = MemoryCDCStateStore()

    async def create_real_time_aggregation_job(self, pivot_spec: PivotSpec):
        """Create a stream processing job for real-time aggregations"""
        stable_spec = json.dumps(
            pivot_spec.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        job_id = f"agg_job_{pivot_spec.table}_{hashlib.sha256(stable_spec.encode('utf-8')).hexdigest()[:16]}"

        # Create the streaming job configuration
        job_config = {
            'job_id': job_id,
            'table': pivot_spec.table,
            'rows': pivot_spec.rows,
            'measures': [{'field': m.field, 'agg': m.agg, 'alias': m.alias} for m in pivot_spec.measures],
            'filters': pivot_spec.filters,
            'window_size': 60,  # 60 seconds default window
            'last_update': time.time(),
            # 'current_state': {}  REMOVED: State is now managed by state_store
        }

        # Initialize current state with empty aggregations
        initial_state = {}
        self._init_current_state_dict(initial_state, pivot_spec.measures)
        await self.state_store.save_state(job_id, initial_state)

        # Store job configuration
        self.aggregation_jobs[job_id] = job_config

        # Start background processing task
        task = asyncio.create_task(self._process_job_stream(job_id))
        self.job_tasks[job_id] = task

        return job_id

    def _init_current_state_dict(self, state_dict: Dict[str, Any], measures):
        """Initialize the current state dict with proper aggregation types"""
        for measure in measures:
            alias = measure.alias
            if measure.agg in ['sum', 'avg', 'count']:
                state_dict[alias] = 0
            elif measure.agg in ['min', 'max']:
                state_dict[alias] = None

    # kept for backward compatibility if needed, but redirects to new method
    def _init_current_state(self, job_config: Dict[str, Any], measures):
        # This was modifying job_config['current_state'] directly. 
        # Since we removed it, this is no-op or we create a temp dict
        return None

    async def _process_job_stream(self, job_id: str):
        """Background task to process stream updates"""
        while job_id in self.aggregation_jobs:
            await asyncio.sleep(0.1)  # Yield control to other tasks

    async def maintain_incremental_views(self, pivot_specs):
        """Maintain pre-computed views that update incrementally"""
        job_ids = []
        for spec in pivot_specs:
            job_id = await self.create_real_time_aggregation_job(spec)
            job_ids.append(job_id)
        return job_ids

    async def process_stream_update(self, table_name: str, record: Dict[str, Any], operation: str = 'INSERT'):
        """Compatibility entry point that never fabricates an exact event."""
        normalized_operation = str(operation or "INSERT").upper()
        change = Change(
            table=table_name,
            type=normalized_operation,
            new_row=record if normalized_operation in {"INSERT", "UPDATE"} else None,
            old_row=record if normalized_operation == "DELETE" else None,
            invalidation_reason="legacy_stream_record_without_cdc_metadata",
        )
        return await self.process_change(change)

    def _matches_filters(self, record: Dict[str, Any], filters: list) -> bool:
        """Check if a record matches the specified filters"""
        if not filters:
            return True

        for f in filters:
            field = f.get('field')
            op = f.get('op', '=')
            value = f.get('value')

            if field not in record:
                return False

            record_value = record[field]

            if op == '=' or op == '==':
                if record_value != value:
                    return False
            elif op == '!=':
                if record_value == value:
                    return False
            elif op == '>':
                if record_value <= value:
                    return False
            elif op == '>=':
                if record_value < value:
                    return False
            elif op == '<':
                if record_value >= value:
                    return False
            elif op == '<=':
                if record_value > value:
                    return False

        return True

    def _jobs_for_table(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            job for job in self.aggregation_jobs.values()
            if job.get("table") == table_name
        ]

    @staticmethod
    def _group_key(job: Mapping[str, Any], row: Mapping[str, Any]) -> str:
        values = [
            CanonicalScalarCodec.encode(row.get(str(field)))
            for field in list(job.get("rows") or [])
        ]
        return json.dumps(values, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _group_label(group_key: str) -> Any:
        values = [CanonicalScalarCodec.decode(value) for value in json.loads(group_key)]
        if len(values) == 1:
            return str(values[0])
        return tuple(values)

    def _apply_row(
        self,
        snapshot: CDCJobSnapshot,
        job: Mapping[str, Any],
        row: Mapping[str, Any],
        sign: int,
    ) -> None:
        if not self._matches_filters(dict(row), list(job.get("filters") or [])):
            return
        group_key = self._group_key(job, row)
        group = snapshot.groups.get(group_key)
        if group is None:
            if sign < 0:
                raise ExactCDCStateError("CDC group removal has no matching state.")
            group = ExactGroupState()
            snapshot.groups[group_key] = group
        group.apply(row, sign, list(job.get("measures") or []))
        if group.row_count == 0:
            snapshot.groups.pop(group_key, None)

    def _apply_exact_transition(
        self,
        snapshot: CDCJobSnapshot,
        job: Mapping[str, Any],
        change: Change,
    ) -> CDCJobSnapshot:
        next_snapshot = snapshot.clone()
        next_snapshot.status = "fresh"
        next_snapshot.invalidation_reason = None
        operation = str(change.type or "").upper()
        if operation == "INSERT":
            self._apply_row(next_snapshot, job, change.new_row or {}, 1)
        elif operation == "DELETE":
            self._apply_row(next_snapshot, job, change.old_row or {}, -1)
        elif operation == "UPDATE":
            self._apply_row(next_snapshot, job, change.old_row or {}, -1)
            self._apply_row(next_snapshot, job, change.new_row or {}, 1)
        else:
            raise ExactCDCStateError("CDC operation is not supported for exact state.")
        return next_snapshot.advance(change)

    async def _invalidate_job(self, job_id: str, reason: str) -> None:
        for _ in range(2):
            snapshot = await self.cdc_state_store.load(job_id)
            invalidated = snapshot.clone()
            invalidated.status = "invalidated"
            invalidated.invalidation_reason = reason
            try:
                await self.cdc_state_store.compare_and_swap(
                    job_id, snapshot.version, invalidated
                )
                return
            except StateVersionConflict:
                continue
        raise ExactCDCStateError("CDC state could not be invalidated safely.")

    async def invalidate_table(self, table_name: str, reason: str) -> int:
        jobs = self._jobs_for_table(table_name)
        for job in jobs:
            await self._invalidate_job(str(job["job_id"]), reason)
        return len(jobs)

    async def _notify_callback(self, job_id: str) -> None:
        callback = self.job_callbacks.get(job_id)
        if callback is None:
            return
        try:
            result = await self.get_job_result(job_id)
            outcome = callback(result)
            if asyncio.iscoroutine(outcome):
                await outcome
        except Exception:
            # Callbacks are notifications only; state has already been
            # persisted and a callback cannot make it stale.
            return

    async def process_change(self, change: Change) -> Dict[str, int]:
        """Apply one trusted event or invalidate jobs that cannot be updated."""
        if change.is_invalidating():
            return {
                "exact": 0,
                "invalidated": await self.invalidate_table(
                    change.table,
                    change.invalidation_reason or "incomplete_change",
                ),
                "ignored": 0,
            }

        exact = invalidated = ignored = 0
        for job in self._jobs_for_table(change.table):
            job_id = str(job["job_id"])
            for _ in range(2):
                snapshot = await self.cdc_state_store.load(job_id)
                decision = snapshot.accept(
                    change.partition, int(change.sequence), str(change.event_id)
                )
                if decision == "replay":
                    ignored += 1
                    break
                if decision == "out_of_order":
                    await self._invalidate_job(job_id, "out_of_order_source_event")
                    invalidated += 1
                    break
                if snapshot.status != "fresh":
                    # A prior incomplete event invalidated this job.  We must
                    # not resume from an unknown baseline using later deltas.
                    invalidated += 1
                    break
                try:
                    next_snapshot = self._apply_exact_transition(snapshot, job, change)
                    await self.cdc_state_store.compare_and_swap(
                        job_id, snapshot.version, next_snapshot
                )
                except StateVersionConflict:
                    continue
                except ExactCDCStateError:
                    await self._invalidate_job(job_id, "invalid_exact_transition")
                    invalidated += 1
                    break
                else:
                    job["last_update"] = time.time()
                    exact += 1
                    await self._notify_callback(job_id)
                    break
            else:
                await self._invalidate_job(job_id, "state_conflict")
                invalidated += 1
        return {"exact": exact, "invalidated": invalidated, "ignored": ignored}

    async def get_job_result(self, job_id: str) -> Optional[StreamingJobResult]:
        """Return a fresh exact result or an explicit invalidated status."""
        job = self.aggregation_jobs.get(job_id)
        if job is None:
            return None
        snapshot = await self.cdc_state_store.load(job_id)
        if snapshot.status != "fresh":
            return StreamingJobResult(
                status="invalidated",
                values={},
                reason=snapshot.invalidation_reason,
            )
        row_fields = list(job.get("rows") or [])
        if not row_fields:
            group = snapshot.groups.get("[]")
            return StreamingJobResult(
                status="fresh",
                values=group.result() if group is not None else {},
            )
        grouped_values = {
            self._group_label(group_key): group.result()
            for group_key, group in snapshot.groups.items()
        }
        return StreamingJobResult(status="fresh", values={"groups": grouped_values})

    def register_callback(self, job_id: str, callback: Callable):
        """Register a callback to be called when job results update"""
        self.job_callbacks[job_id] = callback


class IncrementalMaterializedViewManager:
    """Manages incremental materialized views that update in real-time"""

    def __init__(self, database: ibis.BaseBackend):
        self.database = database  # This should be an Ibis connection
        self.views = {}  # Store view metadata
        self.dependencies = {}  # Store table dependencies
        self.view_data = {}  # Cache for view data

    async def create_incremental_view(self, pivot_spec: PivotSpec):
        """Create an incremental materialized view in the database using Ibis."""
        view_name = f"mv_{pivot_spec.table}_{abs(hash(str(pivot_spec.to_dict()))):x}"

        # Build the Ibis expression for the materialized view
        base_table = self.database.table(pivot_spec.table)

        # Apply filters
        filtered_table = base_table
        if pivot_spec.filters:
            filter_expr = self._build_ibis_filter_expression(base_table, pivot_spec.filters)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)

        # Define aggregations in Ibis
        aggregations = []
        for m in pivot_spec.measures:
            agg_func = getattr(filtered_table[m.field], m.agg)
            aggregations.append(agg_func().name(m.alias))

        # Build the grouped and aggregated expression
        if pivot_spec.rows:
            agg_expr = filtered_table.group_by(pivot_spec.rows).aggregate(aggregations)
        else:
            # If no grouping columns, just aggregate the whole table
            agg_expr = filtered_table.aggregate(aggregations)

        # Create the table in the database
        self.database.create_table(view_name, agg_expr, overwrite=True)

        # Store view metadata
        self.views[view_name] = {
            'name': view_name,
            'spec': pivot_spec,
            'source_table': pivot_spec.table,
            'grouping_cols': pivot_spec.rows,
            'measures': pivot_spec.measures,
            'filters': pivot_spec.filters,
            'last_updated': time.time(),
            'dependencies': [pivot_spec.table],
            'refresh_interval': 300  # 5 minutes default
        }

        # Initialize view data cache (can be loaded from DB if needed)
        self.view_data[view_name] = []

        return view_name

    def _build_ibis_filter_expression(self, table: IbisTable, filters: List[Dict[str, Any]]) -> Optional[ibis.Expr]:
        """Builds an Ibis filter expression from a list of filter dictionaries."""
        ibis_filters = []
        for f in filters:
            field = f.get('field')
            op = f.get('op', '=')
            value = f.get('value')

            if field not in table.columns:
                print(f"Warning: Filter field '{field}' not found in table.")
                continue

            col = table[field]

            if op in ['=', '==']:
                ibis_filters.append(col == value)
            elif op == '!=':
                ibis_filters.append(col != value)
            elif op == '<':
                ibis_filters.append(col < value)
            elif op == '<=':
                ibis_filters.append(col <= value)
            elif op == '>':
                ibis_filters.append(col > value)
            elif op == '>=':
                ibis_filters.append(col >= value)
            elif op == 'in':
                if isinstance(value, list):
                    ibis_filters.append(col.isin(value))
            elif op == 'between':
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    ibis_filters.append(col.between(value[0], value[1]))
            # Add other operators as needed

        if not ibis_filters:
            return None
        
        # Combine all filters with AND
        combined_filter = ibis_filters[0]
        for f_expr in ibis_filters[1:]:
            combined_filter &= f_expr
        return combined_filter

    async def update_view_incrementally(self, view_name: str, changes: list):
        """Update materialized view with incremental changes based on table changes"""
        if view_name not in self.views:
            return

        view_info = self.views[view_name]
        source_table = view_info['source_table']
        grouping_cols = view_info['grouping_cols']
        measures = view_info['measures']

        # For each change in the list, update the materialized view
        for change in changes:
            change_type = change.get('type', 'INSERT').upper()
            row_data = change.get('new_row', {}) if change_type in ['INSERT', 'UPDATE'] else change.get('old_row', {})

            if change_type == 'INSERT':
                await self._handle_insert_incremental(view_name, view_info, row_data)
            elif change_type == 'UPDATE':
                old_row = change.get('old_row', {})
                new_row = change.get('new_row', {})
                await self._handle_update_incremental(view_name, view_info, old_row, new_row)
            elif change_type == 'DELETE':
                old_row = change.get('old_row', {})
                await self._handle_delete_incremental(view_name, view_info, old_row)

        # Update the last updated timestamp
        view_info['last_updated'] = time.time()

    async def _handle_insert_incremental(self, view_name: str, view_info: Dict[str, Any], new_row: Dict[str, Any]):
        """Handle incremental insert to materialized view by updating aggregations"""
        # Get current view data
        current_data = self.view_data.get(view_name, [])

        # Extract grouping values from the new row
        grouping_values = tuple(new_row.get(col) for col in view_info['grouping_cols'])

        # Find if this grouping combination already exists
        existing_row = None
        existing_idx = -1
        for i, row in enumerate(current_data):
            row_grouping = tuple(row.get(col) for col in view_info['grouping_cols'])
            if row_grouping == grouping_values:
                existing_row = row
                existing_idx = i
                break

        if existing_row:
            # Update existing aggregation values
            for measure in view_info['measures']:
                field = measure.field
                agg_type = measure.agg
                alias = measure.alias

                new_value = new_row.get(field, 0)

                if agg_type == 'sum':
                    current_data[existing_idx][alias] += new_value
                elif agg_type == 'count':
                    current_data[existing_idx][alias] += 1
                elif agg_type == 'min':
                    current_data[existing_idx][alias] = min(current_data[existing_idx][alias], new_value)
                elif agg_type == 'max':
                    current_data[existing_idx][alias] = max(current_data[existing_idx][alias], new_value)
                elif agg_type == 'avg':
                    # Update sum and count for average
                    sum_key = f"{alias}_sum"
                    count_key = f"{alias}_count"
                    current_data[existing_idx][sum_key] = current_data[existing_idx].get(sum_key, 0) + new_value
                    current_data[existing_idx][count_key] = current_data[existing_idx].get(count_key, 0) + 1
                    current_data[existing_idx][alias] = current_data[existing_idx][sum_key] / current_data[existing_idx][count_key]
        else:
            # Create a new aggregation row
            new_agg_row = {}
            # Set grouping columns
            for col in view_info['grouping_cols']:
                new_agg_row[col] = new_row.get(col)

            # Set initial aggregation values
            for measure in view_info['measures']:
                field = measure.field
                agg_type = measure.agg
                alias = measure.alias
                new_value = new_row.get(field, 0)

                if agg_type == 'sum':
                    new_agg_row[alias] = new_value
                elif agg_type == 'count':
                    new_agg_row[alias] = 1
                elif agg_type == 'min' or agg_type == 'max':
                    new_agg_row[alias] = new_value
                elif agg_type == 'avg':
                    new_agg_row[alias] = new_value  # Initially just the new value
                    new_agg_row[f"{alias}_sum"] = new_value
                    new_agg_row[f"{alias}_count"] = 1

            current_data.append(new_agg_row)

        # Update the cached data
        self.view_data[view_name] = current_data

    async def _handle_update_incremental(self, view_name: str, view_info: Dict[str, Any], old_row: Dict[str, Any], new_row: Dict[str, Any]):
        """Handle incremental update to materialized view"""
        # For an update, we effectively delete the old values and add the new values
        await self._handle_delete_incremental(view_name, view_info, old_row)
        await self._handle_insert_incremental(view_name, view_info, new_row)

    async def _handle_delete_incremental(self, view_name: str, view_info: Dict[str, Any], old_row: Dict[str, Any]):
        """Handle incremental delete from materialized view"""
        current_data = self.view_data.get(view_name, [])

        # Extract grouping values from the new row
        grouping_values = tuple(old_row.get(col) for col in view_info['grouping_cols'])

        # Find the matching row in current data
        existing_row = None
        existing_idx = -1
        for i, row in enumerate(current_data):
            row_grouping = tuple(row.get(col) for col in view_info['grouping_cols'])
            if row_grouping == grouping_values:
                existing_row = row
                existing_idx = i
                break

        if existing_row:
            # Update aggregation values by subtracting the old values
            for measure in view_info['measures']:
                field = measure.field
                agg_type = measure.agg
                alias = measure.alias

                old_value = old_row.get(field, 0)

                if agg_type == 'sum':
                    current_data[existing_idx][alias] -= old_value
                elif agg_type == 'count':
                    current_data[existing_idx][alias] -= 1
                    # If count goes to 0, remove the row
                    if current_data[existing_idx][alias] <= 0:
                        current_data.pop(existing_idx)
                        break
                elif agg_type == 'min' or agg_type == 'max':
                    # For min/max, we need to recalculate if the deleted value was the min/max
                    # This is complex in incremental updates - in a real system we'd maintain more state
                    # For now, this is a simplified handling
                    pass
                elif agg_type == 'avg':
                    sum_key = f"{alias}_sum"
                    count_key = f"{alias}_count"
                    current_data[existing_idx][sum_key] = current_data[existing_idx].get(sum_key, 0) - old_value
                    current_data[existing_idx][count_key] = max(0, current_data[existing_idx].get(count_key, 1) - 1)
                    if current_data[existing_idx][count_key] > 0:
                        current_data[existing_idx][alias] = current_data[existing_idx][sum_key] / current_data[existing_idx][count_key]
                    else:
                        current_data[existing_idx][alias] = 0
                        # Remove the row if no more items in this group
                        if current_data[existing_idx][alias] == 0 and current_data[existing_idx].get(count_key, 0) <= 0:
                            current_data.pop(existing_idx)

        # Update the cached data
        self.view_data[view_name] = current_data

    async def get_view_data(self, view_name: str) -> Optional[list]:
        """Get the current data from a materialized view"""
        if view_name in self.view_data:
            return self.view_data[view_name]
        # If not in cache, try to load from database
        if view_name in self.views:
            try:
                # Query the view using Ibis
                ibis_table_expr = self.database.table(view_name)
                query_result = ibis_table_expr.to_pyarrow()
                
                if query_result is not None:
                    self.view_data[view_name] = query_result.to_pylist()
                    return self.view_data[view_name]
            except Exception as e:
                print(f"Error loading view '{view_name}' from database: {e}")
                pass  # View may not exist in DB yet
        return None
