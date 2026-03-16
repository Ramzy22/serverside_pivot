import pytest
import pyarrow as pa
from pivot_engine.controller import PivotController
from pivot_engine.types.pivot_spec import PivotSpec, Measure

@pytest.fixture
def controller() -> PivotController:
    """Fixture to create a PivotController with Ibis planner."""
    # Use in-memory DuckDB for tests
    return PivotController(planner_name="ibis", backend_uri=":memory:")

@pytest.fixture
def sample_data() -> pa.Table:
    """Fixture to create sample sales data."""
    return pa.table({
        "region": ["East", "West", "East", "West", "East", "West"],
        "product": ["A", "A", "B", "B", "A", "A"],
        "sales": [100, 200, 150, 250, 50, 300],
        "year": [2024, 2024, 2024, 2024, 2025, 2025]
    })

def test_ibis_planner_simple_pivot(controller: PivotController, sample_data: pa.Table):
    """
    Test a simple pivot operation with the Ibis planner.
    Group by region, sum sales.
    """
    # Load data
    controller.load_data_from_arrow("sales", sample_data)

    # Define pivot spec
    spec = {
        "table": "sales",
        "rows": ["region"],
        "columns": [],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "filters": [],
    }

    # Run pivot, requesting dict format for easy assertion
    result = controller.run_pivot(spec, return_format="dict")

    # Assertions
    assert "columns" in result
    assert "rows" in result
    assert result["columns"] == ["region", "total_sales"]
    
    # Sort rows for consistent comparison
    sorted_rows = sorted(result["rows"], key=lambda x: x[0])
    
    assert len(sorted_rows) == 2
    assert sorted_rows[0] == ["East", 300]
    assert sorted_rows[1] == ["West", 750]


def test_ibis_planner_with_filter(controller: PivotController, sample_data: pa.Table):
    """
    Test a pivot with a filter.
    """
    controller.load_data_from_arrow("sales", sample_data)

    spec = {
        "table": "sales",
        "rows": ["product"],
        "columns": [],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "filters": [{"field": "year", "op": "=", "value": 2024}],
    }

    result = controller.run_pivot(spec, return_format="dict")
    
    sorted_rows = sorted(result["rows"], key=lambda x: x[0])

    assert result["columns"] == ["product", "total_sales"]
    assert len(sorted_rows) == 2
    assert sorted_rows[0] == ["A", 300]
    assert sorted_rows[1] == ["B", 400]

def test_ibis_planner_with_totals(controller: PivotController, sample_data: pa.Table):
    """
    Test a pivot with totals.
    """
    controller.load_data_from_arrow("sales", sample_data)

    spec = {
        "table": "sales",
        "rows": ["region"],
        "columns": [],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "filters": [],
        "totals": True,
    }

    result = controller.run_pivot(spec, return_format="dict")

    # With the new Arrow-native implementation, totals should be in the main result
    # The result should contain 3 rows: 2 for regions + 1 for total
    assert len(result["rows"]) == 3

    # Find the total row (should have None for region column, and 1050 for sales)
    total_row = None
    for row in result["rows"]:
        if row[0] is None:  # region is None in totals row
            total_row = row
            break

    assert total_row is not None
    assert total_row[1] == 1050  # total sales should be 1050

    # Verify the individual rows are still there
    region_rows = [row for row in result["rows"] if row[0] is not None]
    assert len(region_rows) == 2
    assert ["East", 300] in region_rows
    assert ["West", 750] in region_rows


def test_ibis_planner_with_sort(controller: PivotController, sample_data: pa.Table):
    """
    Test a pivot with sorting.
    """
    controller.load_data_from_arrow("sales", sample_data)

    spec = {
        "table": "sales",
        "rows": ["region"],
        "columns": [],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "filters": [],
        "sort": [{"field": "total_sales", "order": "desc"}],
    }

    result = controller.run_pivot(spec, return_format="dict")

    assert result["rows"][0] == ["West", 750]
    assert result["rows"][1] == ["East", 300]

# Conditional import for fakeredis
try:
    import fakeredis
    FAKEREDIS_AVAILABLE = True
except ImportError:
    FAKEREDIS_AVAILABLE = False

@pytest.mark.skipif(not FAKEREDIS_AVAILABLE, reason="fakeredis is not installed")
def test_controller_with_redis_cache(sample_data: pa.Table):
    """
    Test that the PivotController works correctly with the RedisCache.
    """
    import redis
    fake_redis_server = fakeredis.FakeServer()
    fake_client = redis.StrictRedis(server=fake_redis_server, decode_responses=False)

    controller = PivotController(
        planner_name="ibis",
        backend_uri=":memory:",
        cache=fake_client
    )

    controller.load_data_from_arrow("sales", sample_data)

    spec = {
        "table": "sales",
        "rows": ["region"],
        "columns": [],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "filters": [],
    }

    result = controller.run_pivot(spec, return_format="dict")

    assert "columns" in result
    assert "rows" in result
    assert result["columns"] == ["region", "total_sales"]
    
    sorted_rows = sorted(result["rows"], key=lambda x: x[0])
    
    assert len(sorted_rows) == 2
    assert sorted_rows[0] == ["East", 300]
    assert sorted_rows[1] == ["West", 750]

    assert len(fake_redis_server.keys()) > 0


def test_cursor_pagination(controller: PivotController):
    """
    Test cursor-based pagination.
    """
    # Create a larger dataset for pagination
    data = pa.table({
        "city": [f"City {i}" for i in range(5)],
        "sales": [10, 20, 30, 40, 50],
    })
    controller.load_data_from_arrow("sales_pagination", data)

    # --- First Page ---
    spec1 = {
        "table": "sales_pagination",
        "rows": ["city"],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "sort": [{"field": "city", "order": "asc"}],
        "limit": 2,
    }
    
    result1 = controller.run_pivot(spec1, return_format="dict")
    
    assert len(result1["rows"]) == 2
    assert result1["rows"][0][0] == "City 0"
    assert result1["rows"][1][0] == "City 1"
    assert result1["next_cursor"] is not None
    assert result1["next_cursor"] == {"city": "City 1"}

    # --- Second Page ---
    spec2 = {
        "table": "sales_pagination",
        "rows": ["city"],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "sort": [{"field": "city", "order": "asc"}],
        "limit": 2,
        "cursor": result1["next_cursor"], # Use the cursor from the previous result
    }

    result2 = controller.run_pivot(spec2, return_format="dict")

    assert len(result2["rows"]) == 2
    assert result2["rows"][0][0] == "City 2"
    assert result2["rows"][1][0] == "City 3"
    assert result2["next_cursor"] is not None
    assert result2["next_cursor"] == {"city": "City 3"}

    # --- Third and Final Page ---
    spec3 = {
        "table": "sales_pagination",
        "rows": ["city"],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "sort": [{"field": "city", "order": "asc"}],
        "limit": 2,
        "cursor": result2["next_cursor"],
    }

    result3 = controller.run_pivot(spec3, return_format="dict")
    
    assert len(result3["rows"]) == 1
    assert result3["rows"][0][0] == "City 4"
    assert result3["next_cursor"] is None # Should be the last page

    