"""
StreamAggregationProcessor - Real-time stream processing for pivot aggregations
"""
import asyncio
import threading
import time
from typing import Dict, Any, Optional, Callable, List
import pyarrow as pa
import ibis
from pivot_engine.types.pivot_spec import PivotSpec
from ibis.expr.api import Table as IbisTable


class StreamAggregationProcessor:
    def __init__(self, kafka_config: Optional[Dict[str, Any]] = None, state_store: Optional[Any] = None):
        self.kafka_config = kafka_config or {}
        self.aggregation_jobs = {}
        self.stream_clients = {}
        self.job_tasks = {}
        self.job_results = {}
        self.job_callbacks = {}
        
        # Initialize State Store
        if state_store:
            self.state_store = state_store
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

    async def create_real_time_aggregation_job(self, pivot_spec: PivotSpec):
        """Create a stream processing job for real-time aggregations"""
        job_id = f"agg_job_{pivot_spec.table}_{abs(hash(str(pivot_spec.to_dict())))}"

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
        pass

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
        """Process a single record update from the stream"""
        # Find affected jobs
        affected_jobs = []
        for job_id, job in self.aggregation_jobs.items():
            if job.get('table') == table_name:
                # Check if the record matches the job's filters
                if self._matches_filters(record, job.get('filters', [])):
                    affected_jobs.append((job_id, job))

        # Update all affected jobs
        for job_id, job in affected_jobs:
            await self._update_job_state(job_id, job, record, operation)

    def _matches_filters(self, record: Dict[str, Any], filters: list) -> bool:
        """Check if a record matches the specified filters"""
        if not filters:
            return True

        for f in filters:
            field = f.get('field')
            op = f.get('op', '=')
            value = f.get('value')

            if field not in record:
                continue

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

    async def _update_job_state(self, job_id: str, job: Dict[str, Any], record: Dict[str, Any], operation: str):
        """Update job state based on the record and operation"""
        measures = job['measures']
        
        # Fetch current state from store
        current_state = await self.state_store.get_state(job_id)

        for measure in measures:
            field = measure['field']
            agg_type = measure['agg']
            alias = measure['alias']

            if field in record:
                field_value = record[field]

                if operation == 'INSERT':
                    self._apply_aggregation(current_state, alias, field_value, agg_type)
                elif operation == 'DELETE':
                    self._apply_aggregation_reverse(current_state, alias, field_value, agg_type)
                elif operation == 'UPDATE':
                    # For updates, we need old and new values
                    self._apply_aggregation_reverse(current_state, alias, record.get(f'old_{field}', field_value), agg_type)
                    self._apply_aggregation(current_state, alias, field_value, agg_type)

        # Save updated state back to store
        await self.state_store.save_state(job_id, current_state)
        
        # Update timestamp in memory
        job['last_update'] = time.time()
        
        # Trigger callbacks
        if job_id in self.job_callbacks:
             try:
                 # Call callback (potentially async handling needed?)
                 # For now assuming sync callback or fire-and-forget
                 self.job_callbacks[job_id](current_state)
             except Exception as e:
                 print(f"Error in streaming callback: {e}")

    def _apply_aggregation(self, current_state: Dict, alias: str, value: Any, agg_type: str):
        """Apply aggregation operation to update the current state"""
        # Ensure key exists
        if alias not in current_state:
            current_state[alias] = 0 if agg_type in ['sum', 'count', 'avg'] else None

        if agg_type == 'sum':
            current_state[alias] += value if isinstance(value, (int, float)) else 0
        elif agg_type == 'count':
            current_state[alias] += 1
        elif agg_type == 'min':
            if current_state[alias] is None:
                current_state[alias] = value
            else:
                current_state[alias] = min(current_state[alias], value)
        elif agg_type == 'max':
            if current_state[alias] is None:
                current_state[alias] = value
            else:
                current_state[alias] = max(current_state[alias], value)
        elif agg_type == 'avg':
            # For streaming average, we keep sum and count separately
            current_state[f'{alias}_sum'] = current_state.get(f'{alias}_sum', 0) + value
            current_state[f'{alias}_count'] = current_state.get(f'{alias}_count', 0) + 1
            current_state[alias] = current_state[f'{alias}_sum'] / current_state[f'{alias}_count']

    def _apply_aggregation_reverse(self, current_state: Dict, alias: str, value: Any, agg_type: str):
        """Reverse aggregation operation (for deletes)"""
        if alias not in current_state:
             return

        if agg_type == 'sum':
            current_state[alias] -= value if isinstance(value, (int, float)) else 0
        elif agg_type == 'count':
            current_state[alias] -= 1
        elif agg_type == 'min' or agg_type == 'max':
            # For min/max, we can't simply reverse - in a real system we'd need to maintain more state
            pass
        elif agg_type == 'avg':
            current_state[f'{alias}_sum'] = current_state.get(f'{alias}_sum', 0) - value
            current_state[f'{alias}_count'] = max(0, current_state.get(f'{alias}_count', 1) - 1)
            if current_state[f'{alias}_count'] > 0:
                current_state[alias] = current_state[f'{alias}_sum'] / current_state[f'{alias}_count']
            else:
                current_state[alias] = 0

    async def get_job_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get the current result for a streaming job"""
        if job_id in self.aggregation_jobs:
            job = self.aggregation_jobs[job_id]
            # Fetch state from store
            current_state = await self.state_store.get_state(job_id)
            result = current_state.copy()

            # Compute averages based on sum/count
            for k, v in list(result.items()):
                if k.endswith('_sum') and f'{k[:-4]}_count' in result:
                    count_key = f'{k[:-4]}_count'
                    avg_key = k[:-4]  # Remove '_sum'
                    if result[count_key] > 0:
                        result[avg_key] = result[k] / result[count_key]
                    else:
                        result[avg_key] = 0

            return result
        return None

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