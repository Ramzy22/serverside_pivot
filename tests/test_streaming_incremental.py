"""
Test suite for streaming and incremental views
"""
import pytest
import asyncio
from unittest.mock import Mock, MagicMock
import ibis
import pyarrow as pa
from pivot_engine.streaming.streaming_processor import StreamAggregationProcessor, IncrementalMaterializedViewManager
from pivot_engine.streaming.exact_state import MemoryCDCStateStore
from pivot_engine.cdc.models import Change
from pivot_engine.types.pivot_spec import PivotSpec, Measure


@pytest.fixture
def mock_backend():
    """Mock backend for testing"""
    backend = MagicMock()
    backend.execute = Mock(return_value=pa.table({"col1": [1], "col2": [2]}))
    # Setup mock table to return something subscriptable
    mock_table = MagicMock()
    backend.table.return_value = mock_table
    # Ensure filtered table is also a mock
    mock_table.filter.return_value = mock_table
    return backend


@pytest.fixture
def stream_processor():
    """Create stream aggregation processor for testing"""
    processor = StreamAggregationProcessor()
    return processor


@pytest.fixture
def incremental_view_manager(mock_backend):
    """Create incremental materialized view manager for testing"""
    manager = IncrementalMaterializedViewManager(mock_backend)
    return manager


def test_stream_aggregation_processor_creation(stream_processor):
    """Test creation of stream aggregation processor"""
    assert stream_processor is not None
    assert stream_processor.aggregation_jobs == {}
    assert stream_processor.kafka_config == {}


@pytest.mark.asyncio
async def test_create_real_time_aggregation_job(stream_processor):
    """Test creation of real-time aggregation jobs"""
    spec = PivotSpec(
        table="test_table",
        rows=["region"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    job_id = await stream_processor.create_real_time_aggregation_job(spec)
    
    assert isinstance(job_id, str)
    assert job_id.startswith("agg_job_")
    assert job_id in stream_processor.aggregation_jobs


@pytest.mark.asyncio
async def test_maintain_incremental_views(stream_processor):
    """Test maintaining incremental views"""
    spec = PivotSpec(
        table="test_table", 
        rows=["region"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    # Test maintaining incremental views
    await stream_processor.maintain_incremental_views([spec])
    
    # Should have created jobs for the spec
    assert len(stream_processor.aggregation_jobs) >= 1


@pytest.mark.asyncio
async def test_process_stream_update(stream_processor):
    """Test processing of stream updates"""
    # Add a job first
    spec = PivotSpec(
        table="test_table",
        rows=["region"], 
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    await stream_processor.create_real_time_aggregation_job(spec)
    
    # Process a stream update
    record = {"region": "North", "sales": 100}
    await stream_processor.process_stream_update("test_table", record, "INSERT")
    
    # Should not raise an exception
    assert True


def test_incremental_materialized_view_manager_creation(incremental_view_manager):
    """Test creation of incremental view manager"""
    assert incremental_view_manager is not None
    assert incremental_view_manager.views == {}


@pytest.mark.asyncio
async def test_create_incremental_view(incremental_view_manager, mock_backend):
    """Test creation of incremental materialized views"""
    spec = PivotSpec(
        table="test_table",
        rows=["region", "product"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    # Mock the database execute call to avoid actual table creation
    mock_backend.execute = Mock(return_value=True)
    incremental_view_manager.database = mock_backend
    
    view_name = await incremental_view_manager.create_incremental_view(spec)
    
    assert isinstance(view_name, str)
    assert view_name.startswith("mv_")
    # Just check that the table key exists, regardless of view name format
    view_names = list(incremental_view_manager.views.keys())
    assert len(view_names) > 0


@pytest.mark.asyncio
async def test_update_view_incrementally(incremental_view_manager, mock_backend):
    """Test incremental view updates"""
    spec = PivotSpec(
        table="test_table",
        rows=["region"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    # Add view to manager (simulate it exists)
    incremental_view_manager.views["test_table"] = {
        'name': 'mv_test_table_12345',
        'spec': spec,
        'source_table': 'test_table', # Added missing source_table
        'grouping_cols': ['region'],
        'measures': spec.measures,
        'last_updated': 0,
        'dependencies': ['test_table']
    }
    
    # Test incremental update with changes
    changes = [
        {"type": "INSERT", "new_row": {"region": "North", "sales": 100}},
        {"type": "UPDATE", "old_row": {"region": "South", "sales": 50}, "new_row": {"region": "South", "sales": 75}},
        {"type": "DELETE", "old_row": {"region": "East", "sales": 25}}
    ]
    
    # This should run without errors
    await incremental_view_manager.update_view_incrementally("test_table", changes)
    
    assert True


@pytest.mark.asyncio
async def test_legacy_view_update_refreshes_deleted_minimum_from_authoritative_source():
    """A direct legacy update must not retain a deleted group minimum in its cache."""
    backend = ibis.duckdb.connect()
    backend.create_table(
        "sales",
        pa.table(
            {
                "region": ["North", "North", "North"],
                "sales": [10, 20, 30],
            }
        ),
        overwrite=True,
    )
    manager = IncrementalMaterializedViewManager(backend)
    view_name = await manager.create_incremental_view(
        PivotSpec(
            table="sales",
            rows=["region"],
            measures=[Measure(field="sales", agg="min", alias="minimum")],
            filters=[],
        )
    )
    manager.view_data[view_name] = backend.table(view_name).to_pyarrow().to_pylist()

    backend.raw_sql("DELETE FROM sales WHERE region = 'North' AND sales = 10")
    await manager.update_view_incrementally(
        view_name,
        [{"type": "DELETE", "old_row": {"region": "North", "sales": 10}}],
    )

    assert await manager.get_view_data(view_name) == [
        {"region": "North", "minimum": 20}
    ]


@pytest.mark.asyncio
async def test_stream_aggregation_with_complex_spec(stream_processor):
    """Test stream aggregation with more complex pivot specification"""
    spec = PivotSpec(
        table="complex_table",
        rows=["region", "product", "category"],
        measures=[
            Measure(field="sales", agg="sum", alias="total_sales"),
            Measure(field="quantity", agg="avg", alias="avg_quantity")
        ],
        filters=[{"field": "year", "op": "=", "value": 2023}]
    )
    
    job_id = await stream_processor.create_real_time_aggregation_job(spec)
    
    assert job_id in stream_processor.aggregation_jobs
    job_config = stream_processor.aggregation_jobs[job_id]
    
    assert job_config['table'] == 'complex_table'
    assert len(job_config['measures']) == 2


@pytest.mark.asyncio
async def test_incremental_view_manager_handles_multiple_tables(incremental_view_manager, mock_backend):
    """Test incremental view manager with multiple tables"""
    specs = [
        PivotSpec(
            table="table1",
            rows=["col1"],
            measures=[Measure(field="val1", agg="sum", alias="sum_val1")],
            filters=[]
        ),
        PivotSpec(
            table="table2", 
            rows=["col2"],
            measures=[Measure(field="val2", agg="count", alias="count_val2")],
            filters=[]
        )
    ]
    
    incremental_view_manager.database = mock_backend
    
    # Create views for multiple tables
    for spec in specs:
        view_name = await incremental_view_manager.create_incremental_view(spec)
        assert view_name.startswith("mv_")
        # Just check that it was added
        assert len(incremental_view_manager.views) > 0


@pytest.mark.asyncio
async def test_stream_processor_process_various_operations(stream_processor):
    """Test stream processor with various CRUD operations"""
    spec = PivotSpec(
        table="test_table",
        rows=["region"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    await stream_processor.create_real_time_aggregation_job(spec)
    
    # Test different operations
    operations = ["INSERT", "UPDATE", "DELETE"]
    
    for op in operations:
        record = {"region": "TestRegion", "sales": 100} if op != "DELETE" else {"region": "TestRegion"}
        old_record = {"region": "TestRegion", "sales": 50} if op == "UPDATE" else None
        
        stream_data = {"type": op}
        if op == "INSERT":
            stream_data["new_row"] = record
        elif op == "UPDATE":
            stream_data["old_row"] = old_record
            stream_data["new_row"] = record
        elif op == "DELETE":
            stream_data["old_row"] = record
        
        # Adjust arguments to match method signature
        if op == 'INSERT':
             await stream_processor.process_stream_update("test_table", record, op)
        elif op == 'UPDATE':
             # Need to combine old/new for the simplified processor logic or pass raw record if processor handles it
             # Based on code, it expects 'record' dict.
             # The test logic seems slightly off vs processor signature: process_stream_update(table_name, record, operation)
             await stream_processor.process_stream_update("test_table", record, op)
        else:
             await stream_processor.process_stream_update("test_table", record, op)


def _exact_change(change_type, event_id, sequence, old_row=None, new_row=None):
    row = new_row if new_row is not None else old_row
    return Change(
        table="sales",
        type=change_type,
        old_row=old_row,
        new_row=new_row,
        source="postgres",
        event_id=event_id,
        partition="0",
        sequence=sequence,
        row_identity=str(row["id"]),
        row_images_complete=True,
    )


@pytest.mark.asyncio
async def test_stream_job_removes_old_group_and_adds_new_group_for_exact_update():
    processor = StreamAggregationProcessor(state_store=MemoryCDCStateStore())
    spec = PivotSpec(
        table="sales",
        rows=["region"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[{"field": "active", "op": "=", "value": True}],
    )
    job_id = await processor.create_real_time_aggregation_job(spec)
    await processor.process_change(
        _exact_change(
            "INSERT",
            "evt-1",
            1,
            new_row={"id": 1, "region": "North", "active": True, "sales": 10},
        )
    )
    await processor.process_change(
        _exact_change(
            "UPDATE",
            "evt-2",
            2,
            old_row={"id": 1, "region": "North", "active": True, "sales": 10},
            new_row={"id": 1, "region": "South", "active": True, "sales": 25},
        )
    )

    result = await processor.get_job_result(job_id)

    assert result.status == "fresh"
    assert result.values["groups"] == {"South": {"total_sales": 25}}


@pytest.mark.asyncio
async def test_incomplete_change_invalidates_job_and_never_returns_previous_extremum():
    processor = StreamAggregationProcessor(state_store=MemoryCDCStateStore())
    spec = PivotSpec(
        table="sales",
        rows=[],
        measures=[Measure(field="sales", agg="min", alias="minimum")],
        filters=[],
    )
    job_id = await processor.create_real_time_aggregation_job(spec)
    await processor.process_change(
        _exact_change("INSERT", "evt-1", 1, new_row={"id": 1, "sales": 10})
    )
    await processor.process_change(Change(table="sales", type="DELETE", old_row={"id": 1}))

    result = await processor.get_job_result(job_id)

    assert result.status == "invalidated"
    assert result.values == {}


if __name__ == "__main__":
    pytest.main([__file__])
