"""
Test suite for the scalable pivot engine
"""
import pytest
import asyncio
import pyarrow as pa
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import PivotSpec, Measure


@pytest.fixture
def sample_data():
    """Create sample data for testing"""
    return pa.table({
        "region": ["North", "South", "East", "West"] * 100,  # 400 rows
        "product": ["A", "B", "C", "D"] * 100,
        "sales": [100, 200, 150, 300] * 100,
        "quarter": ["Q1", "Q2", "Q3", "Q4"] * 100,
        "year": [2023] * 200 + [2024] * 200
    })


@pytest.fixture
def controller():
    """Create a controller instance for testing"""
    controller = ScalablePivotController(
        backend_uri=":memory:",
        enable_streaming=True,
        enable_incremental_views=True,
        tile_size=50,
        cache_ttl=600
    )
    return controller


@pytest.mark.asyncio
async def test_scalable_pivot_controller_basic(controller, sample_data):
    """Test basic functionality of the scalable pivot controller"""
    # Load sample data
    controller.load_data_from_arrow("sales", sample_data)
    
    # Define pivot specification
    spec = PivotSpec(
        table="sales",
        rows=["region", "product"],
        measures=[
            Measure(field="sales", agg="sum", alias="total_sales"),
            Measure(field="sales", agg="avg", alias="avg_sales")
        ],
        filters=[]
    )
    
    # Basic pivot should work
    result = controller.run_pivot(spec, return_format="dict")
    assert "columns" in result
    assert "rows" in result
    assert len(result["rows"]) > 0


@pytest.mark.asyncio
async def test_materialized_hierarchy_creation(controller, sample_data):
    """Test materialized hierarchy creation and usage"""
    # Load sample data
    controller.load_data_from_arrow("sales", sample_data)
    
    spec = PivotSpec(
        table="sales",
        rows=["region", "product", "quarter"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    # Create materialized hierarchies
    result = controller.run_materialized_hierarchy(spec)
    assert result["status"] == "materialized"
    assert "hierarchy_name" in result


@pytest.mark.asyncio
async def test_virtual_scroll_hierarchical(controller, sample_data):
    """Test virtual scrolling functionality for hierarchical data"""
    # Load sample data
    controller.load_data_from_arrow("sales", sample_data)
    
    spec = PivotSpec(
        table="sales",
        rows=["region", "product"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    expanded_paths = [["North"], ["South"]]
    
    # Test virtual scrolling
    result = controller.run_virtual_scroll_hierarchical(
        spec, start_row=0, end_row=10, expanded_paths=expanded_paths
    )
    assert result is not None


@pytest.mark.asyncio
async def test_progressive_hierarchical_load(controller, sample_data):
    """Test progressive hierarchical loading"""
    # Load sample data
    controller.load_data_from_arrow("sales", sample_data)
    
    spec = PivotSpec(
        table="sales",
        rows=["region", "product"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    expanded_paths = [["North"], ["South"]]
    user_preferences = {
        "pruning_strategy": "top_n",
        "top_n": 5
    }
    
    def mock_progress_callback(progress_info):
        # Mock progress callback for testing
        pass
    
    # Test progressive loading
    result = controller.run_progressive_hierarchical_load(
        spec, expanded_paths, user_preferences, mock_progress_callback
    )
    assert "levels" in result
    assert "metadata" in result


@pytest.mark.asyncio
async def test_hierarchical_pivot_batch_load(controller, sample_data):
    """Test batch loading of multiple hierarchy levels"""
    # Load sample data
    controller.load_data_from_arrow("sales", sample_data)
    
    spec = PivotSpec(
        table="sales",
        rows=["region", "product"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    expanded_paths = [["North"], ["South"]]
    
    # Test batch loading
    result = controller.run_hierarchical_pivot_batch_load(spec.to_dict(), expanded_paths, max_levels=2)
    assert result is not None


if __name__ == "__main__":
    pytest.main([__file__])