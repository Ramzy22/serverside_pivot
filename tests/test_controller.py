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


def test_measure_axis_rows_virtualizes_wide_physical_measures(controller: PivotController):
    data = pa.table({
        "category": ["Equity", "Equity"],
        "portfolio": ["Book", "Hedge"],
        "Delta Explain": [100, -80],
        "Gamma Explain": [25, -10],
    })
    controller.load_data_from_arrow("greeks_measure_axis_rows", data)

    spec = {
        "table": "greeks_measure_axis_rows",
        "rows": ["category"],
        "measures": [
            {"field": "Delta Explain", "agg": "sum", "alias": "delta_sum"},
            {"field": "Gamma Explain", "agg": "sum", "alias": "gamma_sum"},
        ],
        "measureAxis": {
            "placement": "rows",
            "labelField": "Explain Line",
            "valueField": "Amount",
        },
        "sort": [{"field": "Explain Line", "order": "asc", "sortKeyField": "__sortkey__Explain Line"}],
    }

    result = controller.run_pivot(spec, return_format="dict")

    assert result["columns"] == ["category", "Explain Line", "__sortkey__Explain Line", "Amount"]
    rows = sorted(result["rows"], key=lambda row: row[2])
    assert rows == [
        ["Equity", "Delta Explain", 0, 20],
        ["Equity", "Gamma Explain", 1, 15],
    ]


def test_measure_axis_rows_can_still_pivot_normal_column_dimensions(controller: PivotController):
    data = pa.table({
        "category": ["Equity", "Equity"],
        "portfolio": ["Book", "Hedge"],
        "Delta Explain": [100, -80],
        "Gamma Explain": [25, -10],
    })
    controller.load_data_from_arrow("greeks_measure_axis_columns", data)

    spec = {
        "table": "greeks_measure_axis_columns",
        "rows": ["category"],
        "columns": ["portfolio"],
        "measures": [
            {"field": "Delta Explain", "agg": "sum", "alias": "delta_sum"},
            {"field": "Gamma Explain", "agg": "sum", "alias": "gamma_sum"},
        ],
        "pivot_config": {"enabled": True},
        "measureAxis": {
            "placement": "rows",
            "labelField": "Explain Line",
            "valueField": "Amount",
            "members": [
                {"sourceField": "Delta Explain", "agg": "sum", "label": "Delta Explain", "order": 10},
                {"sourceField": "Gamma Explain", "agg": "sum", "label": "Gamma Explain", "order": 20},
            ],
        },
    }

    result = controller.run_pivot(spec, return_format="dict")

    assert result["columns"] == [
        "category",
        "Explain Line",
        "__sortkey__Explain Line",
        "Book_Amount",
        "Hedge_Amount",
    ]
    rows = sorted(result["rows"], key=lambda row: row[2])
    assert rows == [
        ["Equity", "Delta Explain", 10, 100, -80],
        ["Equity", "Gamma Explain", 20, 25, -10],
    ]


def test_measure_axis_aggregate_first_supports_mixed_and_weighted_measures(controller: PivotController):
    data = pa.table({
        "category": ["A", "A", "B"],
        "amount": [10, 20, 5],
        "price": [2, 10, 100],
        "quantity": [1, 3, 2],
    })
    controller.load_data_from_arrow("mixed_measure_axis", data)

    spec = {
        "table": "mixed_measure_axis",
        "rows": ["category"],
        "measures": [
            {"field": "amount", "agg": "sum", "alias": "amount_sum"},
            {"field": "amount", "agg": "avg", "alias": "amount_avg"},
            {"field": "price", "agg": "weighted_avg", "weighted_field": "quantity", "alias": "weighted_price"},
        ],
        "measureAxis": {
            "placement": "rows",
            "labelField": "Metric",
            "valueField": "Value",
            "members": [
                {"measureAlias": "amount_sum", "label": "Amount Sum", "order": 0},
                {"measureAlias": "amount_avg", "label": "Amount Avg", "order": 1},
                {"measureAlias": "weighted_price", "label": "Weighted Price", "order": 2},
            ],
        },
    }

    result = controller.run_pivot(spec, return_format="dict")

    assert result["columns"] == ["category", "Metric", "__sortkey__Metric", "Value"]
    rows = sorted(result["rows"], key=lambda row: (row[0], row[2]))
    assert rows == [
        ["A", "Amount Sum", 0, 30.0],
        ["A", "Amount Avg", 1, 15.0],
        ["A", "Weighted Price", 2, 8.0],
        ["B", "Amount Sum", 0, 5.0],
        ["B", "Amount Avg", 1, 5.0],
        ["B", "Weighted Price", 2, 100.0],
    ]


def test_measure_axis_aggregate_first_supports_formula_ratio_and_window_measures(controller: PivotController):
    data = pa.table({
        "category": ["A", "A", "B"],
        "amount": [10, 20, 5],
        "cost": [4, 8, 2],
    })
    controller.load_data_from_arrow("derived_measure_axis", data)

    spec = {
        "table": "derived_measure_axis",
        "rows": ["category"],
        "measures": [
            {"field": "amount", "agg": "sum", "alias": "sales"},
            {"field": "cost", "agg": "sum", "alias": "cost"},
            {"field": None, "agg": "formula", "alias": "margin", "expression": "sales - cost"},
            {
                "field": "amount",
                "agg": "sum",
                "alias": "sales_pct_total",
                "window_func": "percent_of_total",
            },
            {
                "field": None,
                "agg": "ratio",
                "alias": "cost_ratio",
                "ratio_numerator": "cost",
                "ratio_denominator": "sales",
            },
        ],
        "measureAxis": {
            "placement": "rows",
            "labelField": "Metric",
            "valueField": "Value",
            "members": [
                {"measureAlias": "margin", "label": "Margin", "order": 0},
                {"measureAlias": "cost_ratio", "label": "Cost Ratio", "order": 1},
                {"measureAlias": "sales_pct_total", "label": "Sales % Total", "order": 2},
            ],
        },
    }

    result = controller.run_pivot(spec, return_format="dict")
    rows = {
        (row[0], row[1]): row[3]
        for row in result["rows"]
    }

    assert rows[("A", "Margin")] == 18.0
    assert rows[("B", "Margin")] == 3.0
    assert rows[("A", "Cost Ratio")] == pytest.approx(0.4)
    assert rows[("B", "Cost Ratio")] == pytest.approx(0.4)
    assert rows[("A", "Sales % Total")] == pytest.approx(30 / 35)
    assert rows[("B", "Sales % Total")] == pytest.approx(5 / 35)


def test_measure_axis_aggregate_first_can_pivot_mixed_measures(controller: PivotController):
    data = pa.table({
        "category": ["Equity", "Equity"],
        "portfolio": ["Book", "Hedge"],
        "delta": [100, -80],
        "gamma": [25, -10],
    })
    controller.load_data_from_arrow("mixed_measure_axis_columns", data)

    spec = {
        "table": "mixed_measure_axis_columns",
        "rows": ["category"],
        "columns": ["portfolio"],
        "measures": [
            {"field": "delta", "agg": "sum", "alias": "delta_sum"},
            {"field": "gamma", "agg": "avg", "alias": "gamma_avg"},
        ],
        "pivot_config": {"enabled": True},
        "measureAxis": {
            "placement": "rows",
            "labelField": "Metric",
            "valueField": "Value",
            "members": [
                {"measureAlias": "delta_sum", "label": "Delta Sum", "order": 0},
                {"measureAlias": "gamma_avg", "label": "Gamma Avg", "order": 1},
            ],
        },
    }

    result = controller.run_pivot(spec, return_format="dict")

    assert result["columns"] == [
        "category",
        "Metric",
        "__sortkey__Metric",
        "Book_Value",
        "Hedge_Value",
    ]
    rows = sorted(result["rows"], key=lambda row: row[2])
    assert rows == [
        ["Equity", "Delta Sum", 0, 100.0, -80.0],
        ["Equity", "Gamma Avg", 1, 25.0, -10.0],
    ]

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

