import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from pivot_engine.cdc.cdc_manager import PivotCDCManager
from pivot_engine.cdc.models import Change
from pivot_engine.scalable_pivot_controller import ScalablePivotController
import pyarrow as pa
import time


@pytest.fixture
def mock_backend():
    """Create a mock backend for testing"""
    backend = MagicMock()
    # Mock cache
    backend.cache = MagicMock()
    backend.cache.get = MagicMock(return_value=None)
    backend.cache.set = MagicMock()
    backend.cache._find_and_invalidate_affected_cache_keys = AsyncMock()
    
    # Mock Ibis connection methods
    backend.list_tables = MagicMock(return_value=[])
    backend.create_table = MagicMock()
    
    # Mock table object for change detector
    mock_table = MagicMock()
    mock_table.count.return_value.execute.return_value = 100
    mock_table.limit.return_value.to_pyarrow.return_value = pa.Table.from_pydict({'id': [1, 2, 3]})
    backend.table.return_value = mock_table
    
    return backend


@pytest.fixture
def cdc_manager(mock_backend):
    """Create a CDC manager with mocked backend"""
    return PivotCDCManager(mock_backend)


@pytest.mark.asyncio
async def test_cdc_initialization(cdc_manager):
    """Test proper initialization of CDC manager"""
    assert cdc_manager.database is not None
    assert cdc_manager.checkpoints == {}
    assert not cdc_manager.running


@pytest.mark.asyncio
async def test_setup_cdc(cdc_manager):
    """Test CDC setup for a table"""
    # Setup CDC for a table
    await cdc_manager.setup_cdc("test_table")
    
    # Assertions about database.create_table calls would verify the setup
    # The create_table method should have been called to create tracking table
    assert cdc_manager.database.create_table.called
    args, _ = cdc_manager.database.create_table.call_args
    assert args[0] == "test_table_changes_tracking"
    
    # Verify checkpoint initialized
    assert "test_table" in cdc_manager.checkpoints
    assert cdc_manager.checkpoints["test_table"]["active"] is True


@pytest.mark.asyncio
async def test_process_change_insert(cdc_manager):
    """Test processing an INSERT change"""
    change = Change(table="test_table", type="INSERT", new_row={"id": 1, "val": "new"})
    
    # Mock materialized view manager
    mock_mv_manager = AsyncMock()
    cdc_manager.register_materialized_view_manager("test_table", mock_mv_manager)
    
    # Process change
    await cdc_manager._process_change(change)
    
    # Verify manager was notified
    assert mock_mv_manager.process_incremental_change.called
    call_args = mock_mv_manager.process_incremental_change.call_args[0][0]
    assert call_args['operation'] == 'INSERT'
    assert call_args['new_row'] == {"id": 1, "val": "new"}


@pytest.mark.asyncio
async def test_process_change_update(cdc_manager):
    """Test processing an UPDATE change"""
    change = Change(
        table="test_table", 
        type="UPDATE", 
        old_row={"id": 1, "val": "old"}, 
        new_row={"id": 1, "val": "new"}
    )
    
    # Mock materialized view manager
    mock_mv_manager = AsyncMock()
    cdc_manager.register_materialized_view_manager("test_table", mock_mv_manager)
    
    # Process change
    await cdc_manager._process_change(change)
    
    # Verify manager was notified
    assert mock_mv_manager.process_incremental_change.called
    call_args = mock_mv_manager.process_incremental_change.call_args[0][0]
    assert call_args['operation'] == 'UPDATE'
    assert call_args['old_row'] == {"id": 1, "val": "old"}
    assert call_args['new_row'] == {"id": 1, "val": "new"}


@pytest.mark.asyncio
async def test_track_changes_stream(cdc_manager):
    """Test tracking changes from a stream"""
    # Setup a mock stream
    async def mock_stream():
        yield Change(table="test_table", type="INSERT", new_row={"id": 1})
        yield Change(table="test_table", type="UPDATE", old_row={"id": 1}, new_row={"id": 1, "v": 2})
    
    cdc_manager.change_stream = mock_stream()
    
    # Set table as active
    cdc_manager.checkpoints["test_table"] = {"active": True}
    
    # Register a processor
    processed_changes = []
    async def processor(change):
        processed_changes.append(change)
    
    cdc_manager.register_change_processor(processor)
    
    # Run tracking (will stop when stream is exhausted)
    await cdc_manager.track_changes("test_table")
    
    assert len(processed_changes) == 2
    assert processed_changes[0].type == "INSERT"
    assert processed_changes[1].type == "UPDATE"


@pytest.mark.asyncio
async def test_controller_cdc_integration():
    """Test CDC integration with the main controller"""
    controller = ScalablePivotController(backend_uri=":memory:")
    
    # Pre-create the table in Ibis backend so setup_cdc doesn't fail
    dummy_data = pa.Table.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    controller.load_data_from_arrow("test_table", dummy_data)
    
    # This test would set up CDC with a mock stream
    async def mock_stream():
        yield Change(table="test_table", type="INSERT", new_row={"id": 4, "val": "d"})
        return  # Stop after one change for testing
    
    # Setup CDC for a table
    cdc_manager = await controller.setup_cdc("test_table", mock_stream())
    
    assert cdc_manager is not None
    assert "test_table" in cdc_manager.checkpoints
    
    # Cleanup
    if cdc_manager:
        cdc_manager.stop_tracking()
    controller.close()


@pytest.mark.asyncio
async def test_update_affected_cache_keys(cdc_manager):
    """Test updating affected cache keys after changes"""
    change = Change(table="test_table", type="INSERT", new_row={"id": 1})
    
    # This should not raise an exception
    await cdc_manager._update_affected_cache_keys(change)
    
    # Verify cache invalidation was called
    assert cdc_manager.database.cache._find_and_invalidate_affected_cache_keys.called
    cdc_manager.database.cache._find_and_invalidate_affected_cache_keys.assert_called_with("test_table")
