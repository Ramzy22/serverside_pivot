"""
Database Change Detection System for Pivot Engine CDC

This module provides mechanisms to detect database changes and produce change streams.
It supports both polling-based detection (for standard databases) and push-based 
detection (for webhook/event-driven sources).
"""
import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, AsyncGenerator, Optional, List, Union
from dataclasses import dataclass
from pivot_engine.cdc.models import Change


@dataclass
class TableSnapshot:
    """Represents a snapshot of table data at a point in time"""
    table_name: str
    row_count: int
    checksum: str
    timestamp: float
    sample_data: List[Dict[str, Any]]
    max_id: Optional[int] = None
    max_updated_at: Optional[float] = None


class CDCProvider(ABC):
    """Abstract base class for CDC providers"""
    
    @abstractmethod
    async def start_tracking_table(self, table_name: str):
        """Start tracking changes for a specific table"""
        pass
    
    @abstractmethod
    async def stop_tracking_table(self, table_name: str):
        """Stop tracking changes for a specific table"""
        pass
        
    @abstractmethod
    async def get_change_stream(self, table_name: str, **kwargs) -> AsyncGenerator[Change, None]:
        """Generate a stream of changes for a table"""
        pass


class PushCDCProvider(CDCProvider):
    """
    CDC Provider that accepts externally pushed changes.
    This enables true push-based CDC via webhooks or external event consumers.
    """
    
    def __init__(self):
        self.queues: Dict[str, asyncio.Queue] = {}
        self.active_tables = set()
        
    async def start_tracking_table(self, table_name: str):
        """Start tracking changes for a specific table"""
        if table_name not in self.queues:
            self.queues[table_name] = asyncio.Queue()
        self.active_tables.add(table_name)
        print(f"Push CDC: Started listening for {table_name}")
        
    async def stop_tracking_table(self, table_name: str):
        """Stop tracking changes for a specific table"""
        if table_name in self.queues:
            # Signal end of stream? Or just remove
            # self.queues[table_name].put_nowait(None) 
            del self.queues[table_name]
        self.active_tables.discard(table_name)
        
    async def push_change(self, table_name: str, change: Change):
        """Push a change from an external source"""
        if table_name in self.queues:
            await self.queues[table_name].put(change)
        else:
            # Optional: warn or auto-create queue
            pass
            
    async def get_change_stream(self, table_name: str, **kwargs) -> AsyncGenerator[Change, None]:
        """Generate a stream of changes for a table"""
        if table_name not in self.queues:
            await self.start_tracking_table(table_name)
            
        queue = self.queues[table_name]
        
        while table_name in self.active_tables:
            # Wait for next change
            try:
                change = await queue.get()
                yield change
                queue.task_done()
            except asyncio.CancelledError:
                break


class PollingCDCProvider(CDCProvider):
    """
    Detects changes in database tables using polling.
    Optimized to minimize query load using snapshots and incremental keys.
    """
    
    def __init__(self, backend):
        self.backend = backend  # This is an Ibis connection object
        self.table_snapshots: Dict[str, TableSnapshot] = {}
        self.running = False
        
    async def start_tracking_table(self, table_name: str):
        """Start tracking changes for a specific table"""
        # Take initial snapshot
        initial_snapshot = await self._take_snapshot(table_name)
        self.table_snapshots[table_name] = initial_snapshot
        print(f"Polling CDC: Started tracking {table_name}")
    
    async def stop_tracking_table(self, table_name: str):
        """Stop tracking changes for a specific table"""
        if table_name in self.table_snapshots:
            del self.table_snapshots[table_name]
    
    async def _take_snapshot(self, table_name: str) -> TableSnapshot:
        """Take a snapshot of the table data using Ibis expressions."""
        ibis_table = self.backend.table(table_name)
        
        # Get row count
        row_count = ibis_table.count().execute()
        
        # Get a simple checksum using row count for backend agnosticism.
        checksum = str(row_count)
        
        # Get sample data
        sample_data_pyarrow = ibis_table.limit(5).to_pyarrow()
        sample_data = sample_data_pyarrow.to_pylist() if sample_data_pyarrow.num_rows > 0 else []
        
        # Capture incremental markers if available
        max_id = None
        max_updated_at = None
        
        cols = ibis_table.columns
        if 'id' in cols:
            try:
                max_id = ibis_table['id'].max().execute()
            except: pass
            
        if 'updated_at' in cols:
            try:
                max_updated_at = ibis_table['updated_at'].max().execute()
            except: pass
        
        return TableSnapshot(
            table_name=table_name,
            row_count=row_count,
            checksum=checksum,
            timestamp=time.time(),
            sample_data=sample_data,
            max_id=max_id,
            max_updated_at=max_updated_at
        )
    
    async def detect_changes(self, table_name: str) -> List[Change]:
        """Detect changes for a tracked table by comparing with previous snapshot"""
        if table_name not in self.table_snapshots:
            await self.start_tracking_table(table_name)
        
        current_snapshot = await self._take_snapshot(table_name)
        previous_snapshot = self.table_snapshots[table_name]
        
        changes = []
        
        # Compare snapshots to detect changes
        if current_snapshot.checksum != previous_snapshot.checksum:
            # Data has changed - determine what changed
            if current_snapshot.row_count > previous_snapshot.row_count:
                # Likely INSERT operations
                changes.extend(await self._detect_inserts(table_name, previous_snapshot, current_snapshot))
            elif current_snapshot.row_count < previous_snapshot.row_count:
                # Likely DELETE operations
                changes.extend(await self._detect_deletions(table_name, previous_snapshot, current_snapshot))
            else:
                # Row count is same but data changed - likely UPDATE operations
                changes.append(Change(
                    table=table_name,
                    type='UPDATE',
                    old_row={"_change_type": "detected_update", "_timestamp": time.time()},
                    new_row={"_change_type": "detected_update", "_timestamp": time.time()}
                ))
        
        # Update the stored snapshot
        self.table_snapshots[table_name] = current_snapshot
        
        return changes
    
    async def _detect_inserts(self, table_name: str, old_snapshot: TableSnapshot, new_snapshot: TableSnapshot) -> List[Change]:
        """Detect INSERT operations"""
        changes = []
        ibis_table = self.backend.table(table_name)
        
        # Try to fetch actual inserted rows using incremental keys
        new_rows = []
        
        if old_snapshot.max_id is not None and new_snapshot.max_id is not None:
            if new_snapshot.max_id > old_snapshot.max_id:
                try:
                    new_rows_table = ibis_table.filter(ibis_table['id'] > old_snapshot.max_id).to_pyarrow()
                    new_rows = new_rows_table.to_pylist()
                except Exception as e:
                    print(f"Error fetching incremental inserts by ID: {e}")
                    
        elif old_snapshot.max_updated_at is not None and new_snapshot.max_updated_at is not None:
            if new_snapshot.max_updated_at > old_snapshot.max_updated_at:
                try:
                    new_rows_table = ibis_table.filter(ibis_table['updated_at'] > old_snapshot.max_updated_at).to_pyarrow()
                    new_rows = new_rows_table.to_pylist()
                except Exception as e:
                    print(f"Error fetching incremental inserts by updated_at: {e}")

        if new_rows:
            for row in new_rows:
                changes.append(Change(
                    table=table_name,
                    type='INSERT',
                    new_row=row
                ))
        else:
            num_inserts = new_snapshot.row_count - old_snapshot.row_count
            if num_inserts > 0:
                for _ in range(num_inserts):
                    changes.append(Change(
                        table=table_name,
                        type='INSERT',
                        new_row={"_change_type": "detected_insert_placeholder", "_timestamp": time.time()}
                    ))
        
        return changes
    
    async def _detect_deletions(self, table_name: str, old_snapshot: TableSnapshot, new_snapshot: TableSnapshot) -> List[Change]:
        """Detect DELETE operations"""
        changes = []
        num_deletes = old_snapshot.row_count - new_snapshot.row_count
        
        for _ in range(num_deletes):
            changes.append(Change(
                table=table_name,
                type='DELETE',
                old_row={"_change_type": "detected_delete", "_timestamp": time.time()}
            ))
        
        return changes
    
    async def get_change_stream(self, table_name: str, poll_interval: float = 1.0) -> AsyncGenerator[Change, None]:
        """Generate a stream of changes for a table using polling"""
        if table_name not in self.table_snapshots:
            await self.start_tracking_table(table_name)
        
        while True:
            changes = await self.detect_changes(table_name)
            for change in changes:
                yield change
            await asyncio.sleep(poll_interval)


# Alias for backward compatibility if needed, though manager should be updated
DatabaseChangeDetector = PollingCDCProvider
