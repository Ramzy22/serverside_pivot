"""
Test suite for hierarchical managers, using a real Ibis backend.
"""
import pytest
import pyarrow as pa
import ibis
from pivot_engine.hierarchical_scroll_manager import HierarchicalVirtualScrollManager
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.types.pivot_spec import PivotSpec, Measure
from pivot_engine.planner.ibis_planner import IbisPlanner
from pivot_engine.cache.memory_cache import MemoryCache

@pytest.fixture(scope="module")
def ibis_con():
    """Create a real in-memory DuckDB Ibis connection for testing."""
    con = ibis.duckdb.connect()
    
    # Create sample data
    sales_data = pa.table({
        "region": ["NA", "NA", "NA", "EU", "EU", "AS"],
        "country": ["US", "US", "CA", "DE", "FR", "JP"],
        "city": ["NY", "SF", "TO", "BE", "PA", "TY"],
        "sales": [100, 50, 70, 80, 40, 60],
    })
    con.create_table("sales", sales_data, overwrite=True)
    
    return con

@pytest.fixture
def mock_cache():
    """Mock cache for testing."""
    return MemoryCache()

@pytest.fixture
def ibis_planner(ibis_con):
    """Fixture for a real IbisPlanner."""
    return IbisPlanner(con=ibis_con)

@pytest.fixture
def materialized_hierarchy_manager(ibis_con, mock_cache):
    """Fixture for a real MaterializedHierarchyManager."""
    # This manager needs a backend that can execute CREATE TABLE queries.
    # The ibis_con can serve this purpose.
    return MaterializedHierarchyManager(ibis_con, mock_cache)


def test_ibis_based_hierarchical_scroll(ibis_planner, mock_cache, materialized_hierarchy_manager):
    """
    Integration-style test to verify the Ibis-based hierarchical scrolling
    produces the correct results.
    """
    # 1. Setup
    scroll_manager = HierarchicalVirtualScrollManager(ibis_planner, mock_cache, materialized_hierarchy_manager)

    spec = PivotSpec(
        table="sales",
        rows=["region", "country"],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    scroll_manager.spec = spec # Manually set spec for _format_for_ui

    # 2. Create the physical rollup tables in the in-memory database
    materialized_hierarchy_manager.create_materialized_hierarchy(spec)

    # 3. Call the method under test with an expansion
    # We expect to see the top-level items, plus the children of "NA"
    results = scroll_manager.get_visible_rows_hierarchical(
        spec=spec,
        start_row=0,
        end_row=10, # Request enough to get all results
        expanded_paths=[["NA"]]
    )

    # 4. Assert the results
    assert len(results) == 5 # 3 top-level regions + 2 countries under NA

    # Convert to a more easily verifiable format
    # Expected order: AS, EU, NA, NA-CA, NA-US
    result_paths = [tuple(row[d] for d in spec.rows if d in row) for row in results]
    
    # Top-level regions
    assert ("AS",) in result_paths
    assert ("EU",) in result_paths
    assert ("NA",) in result_paths

    # Expanded children of NA
    assert ("NA", "CA") in result_paths
    assert ("NA", "US") in result_paths

    # Check aggregation values
    for row in results:
        path = tuple(row[d] for d in spec.rows if d in row)
        if path == ("NA",):
            assert row["total_sales"] == 220 # 100 + 50 + 70
        elif path == ("EU",):
            assert row["total_sales"] == 120 # 80 + 40
        elif path == ("AS",):
            assert row["total_sales"] == 60
        elif path == ("NA", "US"):
            assert row["total_sales"] == 150 # 100 + 50
        elif path == ("NA", "CA"):
            assert row["total_sales"] == 70

    # 5. Test pagination (offset)
    paginated_results = scroll_manager.get_visible_rows_hierarchical(
        spec=spec,
        start_row=2, # Skip AS, EU
        end_row=4,   # Get NA, NA-CA
        expanded_paths=[["NA"]]
    )

    assert len(paginated_results) == 2
    paginated_paths = [tuple(row[d] for d in spec.rows if d in row) for row in paginated_results]
    assert paginated_paths[0] == ("NA",)
    assert paginated_paths[1] == ("NA", "CA")


if __name__ == "__main__":
    pytest.main([__file__])