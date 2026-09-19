import asyncio
import logging
import time
from typing import AsyncGenerator, Any, Dict, List, Optional, Union
import ibis
from pivot_engine.streaming.streaming_processor import IncrementalMaterializedViewManager
from pivot_engine.cdc.database_change_detector import TableSnapshot, PollingCDCProvider, PushCDCProvider, CDCProvider
from pivot_engine.cdc.models import Change


class CDCProcessingError(RuntimeError):
    """A registered CDC consumer failed after the event reached routing."""


class PivotCDCManager:
    def __init__(self, database, change_stream: Optional[AsyncGenerator[Change, None]] = None):
        """
        Initialize CDC Manager.
        
        Args:
            database: An Ibis connection object
            change_stream: Optional stream of changes for testing or external source
        """
        self.database = database
        self.change_stream = change_stream
        self.checkpoints = {}
        self.running = False
        self.change_processors = []
        self.materialized_view_managers = {}
        self.change_consumers = {}
        
        # Default to polling provider
        self.provider: CDCProvider = PollingCDCProvider(database)
        self.provider_type = "polling"

    def use_push_provider(self):
        """Switch to Push-based CDC provider"""
        self.provider = PushCDCProvider()
        self.provider_type = "push"
        print("Switched to Push-based CDC provider")

    async def push_change(self, table_name: str, change: Change):
        """Push a change into the system (only works if Push provider is active)"""
        if isinstance(self.provider, PushCDCProvider):
            await self.provider.push_change(table_name, change)
        else:
            print(f"Warning: Cannot push change, current provider is {self.provider_type}")

    async def setup_cdc(self, table_name: str, stream_override=None):
        """Set up CDC for a specific table"""
        # Create a changes tracking table if it doesn't exist
        changes_table_name = f"{table_name}_changes_tracking"
        
        # Define the schema for the tracking table using Ibis types
        schema = ibis.schema([
            ('id', 'int64'),
            ('operation', 'string'),
            ('timestamp', 'timestamp'),
            ('processed', 'boolean')
        ])
        
        try:
            if changes_table_name not in self.database.list_tables():
                self.database.create_table(changes_table_name, schema)
        except Exception as e:  # nosec B110
            # Some backends might raise if table exists or permission issues
            pass

        # Initialize checkpoint data
        self.checkpoints[table_name] = {
            'setup_time': asyncio.get_event_loop().time(),
            'last_processed': 0,
            'active': True,
            'tracking_table': changes_table_name
        }
        
        # Start provider for this table
        await self.provider.start_tracking_table(table_name)
        
        # If an override stream is provided (e.g. for testing), we can use it?
        # The original code returned self from this method, let's keep that API.
        return self

    def register_materialized_view_manager(self, table_name: str, manager: IncrementalMaterializedViewManager):
        """Register a materialized view manager to receive change notifications"""
        if table_name not in self.materialized_view_managers:
            self.materialized_view_managers[table_name] = []
        self.materialized_view_managers[table_name].append(manager)
        if (
            callable(getattr(type(manager), "process_change", None))
            and callable(getattr(type(manager), "invalidate_table", None))
        ):
            self.register_change_consumer(table_name, manager)

    def register_change_consumer(self, table_name: str, consumer) -> None:
        """Register a consumer with explicit exact/invalidation operations."""
        self.change_consumers.setdefault(table_name, []).append(consumer)

    def _record_consumer_failure(self, table_name: str, exc: Exception) -> None:
        checkpoint = self.checkpoints.setdefault(table_name, {})
        checkpoint["last_error"] = "cdc_consumer_failed"
        logging.getLogger(__name__).error(
            "CDC consumer failed table=%s code=cdc_consumer_failed type=%s",
            table_name,
            type(exc).__name__,
        )

    async def _route_change(self, change: Change) -> None:
        for consumer in tuple(self.change_consumers.get(change.table, ())):
            try:
                if change.is_invalidating():
                    await consumer.invalidate_table(
                        change.table,
                        change.invalidation_reason or "incomplete_change",
                    )
                else:
                    await consumer.process_change(change)
            except Exception as exc:
                self._record_consumer_failure(change.table, exc)
                raise CDCProcessingError("cdc_consumer_failed") from exc

    def register_change_processor(self, processor_func):
        """Register a function to process changes"""
        self.change_processors.append(processor_func)

    async def track_changes(self, table_name: str):
        """Track and process data changes in real-time"""
        if not self.running:
            self.running = True

        stream_source = self.change_stream if self.change_stream else self.provider.get_change_stream(table_name)

        async for change in stream_source:
            if not self.running:
                break
                
            if self.checkpoints.get(table_name, {}).get('active', False):
                await self._process_change(change)
                await self._update_affected_cache_keys(change)
                await self._update_materialized_views(change)
                for processor in self.change_processors:
                    try:
                        await processor(change)
                    except Exception as e:
                        print(f"Error in change processor: {e}")

    async def _process_change(self, change: Change):
        """Process individual changes and update pivot structures"""
        if self.change_consumers.get(change.table):
            await self._route_change(change)
            return
        if change.is_invalidating():
            return
        if change.type == 'INSERT':
            await self._incremental_insert(change.table, change.new_row)
        elif change.type == 'UPDATE':
            await self._incremental_update(change.table, change.old_row, change.new_row)
        elif change.type == 'DELETE':
            await self._incremental_delete(change.table, change.old_row)

    async def _incremental_insert(self, table_name: str, new_row: Dict):
        """Handle incremental insert by updating affected aggregations"""
        for manager in self.materialized_view_managers.get(table_name, []):
            try:
                await manager.process_incremental_change({
                    'table': table_name,
                    'operation': 'INSERT',
                    'old_row': None,
                    'new_row': new_row
                })
            except Exception as e:
                print(f"Error updating materialized view for insert: {e}")

    async def _incremental_update(self, table_name: str, old_row: Dict, new_row: Dict):
        """Handle incremental update by adjusting affected aggregations"""
        for manager in self.materialized_view_managers.get(table_name, []):
            try:
                await manager.process_incremental_change({
                    'table': table_name,
                    'operation': 'UPDATE',
                    'old_row': old_row,
                    'new_row': new_row
                })
            except Exception as e:
                print(f"Error updating materialized view for update: {e}")

    async def _incremental_delete(self, table_name: str, old_row: Dict):
        """Handle incremental delete by removing from aggregations"""
        for manager in self.materialized_view_managers.get(table_name, []):
            try:
                await manager.process_incremental_change({
                    'table': table_name,
                    'operation': 'DELETE',
                    'old_row': old_row,
                    'new_row': None
                })
            except Exception as e:
                print(f"Error updating materialized view for delete: {e}")

    async def _update_affected_cache_keys(self, change: Change):
        """Update affected cache keys by identifying and invalidating specific cached queries"""
        if hasattr(self.database, 'cache') and self.database.cache:
            cache = self.database.cache
            # The cache object must implement _find_and_invalidate_affected_cache_keys
            if hasattr(cache, '_find_and_invalidate_affected_cache_keys'):
                await cache._find_and_invalidate_affected_cache_keys(change.table)
            else:
                # Fallback to clearing table cache if granular invalidation is not supported
                if hasattr(cache, 'clear_table_cache'):
                    if asyncio.iscoroutinefunction(cache.clear_table_cache):
                        await cache.clear_table_cache(change.table)
                    else:
                        cache.clear_table_cache(change.table)

    async def _update_materialized_views(self, change: Change):
        """Deliver only compatibility updates not handled by typed CDC routing."""
        table_name = change.table
        routed_manager_ids = {
            id(consumer) for consumer in self.change_consumers.get(table_name, ())
        }
        for manager in self.materialized_view_managers.get(table_name, ()):
            if id(manager) in routed_manager_ids:
                continue
            update_view = getattr(manager, "update_view_incrementally", None)
            if not callable(update_view):
                continue
            try:
                views = getattr(manager, "views", None)
                view_names = (
                    [
                        name for name, info in views.items()
                        if info.get("source_table") == table_name
                    ]
                    if isinstance(views, dict)
                    else [f"mv_{table_name}"]
                )
                for view_name in view_names:
                    await update_view(view_name, [change])
            except Exception as e:
                print(f"Error updating materialized view for {table_name}: {e}")

    def stop_tracking(self):
        """Stop the CDC tracking"""
        self.running = False
        if self.provider:
            # We would need to stop all tracked tables on provider, but provider API is per-table
            # For now, just setting running=False stops the loop in track_changes
            pass

    def get_cdc_status(self, table_name: str) -> Dict[str, Any]:
        """Get the status of CDC for a specific table"""
        return self.checkpoints.get(table_name, {})
