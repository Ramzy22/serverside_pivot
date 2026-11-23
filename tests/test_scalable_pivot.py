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


if __name__ == "__main__":
    pytest.main([__file__])