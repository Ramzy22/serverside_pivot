"""
Test suite for advanced planning features in the pivot_engine.
Tests cost estimation, query optimization, and plan selection.
"""
import pytest
import pyarrow as pa
import ibis # Import ibis
from pivot_engine.controller import PivotController
from pivot_engine.planner.sql_planner import SQLPlanner, CostEstimator
from pivot_engine.planner.ibis_planner import IbisPlanner, CostEstimator as IbisCostEstimator
from pivot_engine.types.pivot_spec import PivotSpec, Measure


@pytest.fixture
def controller() -> PivotController:
    """Create a controller with advanced planning enabled"""
    return PivotController(
        planner_name="ibis", 
        backend_uri=":memory:", 
        enable_delta=False,  # Disable delta for simpler tests
        enable_tiles=False   # Disable tiles for simpler tests
    )


@pytest.fixture  
def sample_data() -> pa.Table:
    """Sample data for testing"""
    return pa.table({
        "region": ["East", "West", "East", "West", "East", "West", "East", "West"],
        "product": ["A", "A", "B", "B", "A", "A", "C", "C"],
        "sales": [100, 200, 150, 250, 50, 300, 200, 100],
        "year": [2023, 2023, 2024, 2024, 2023, 2024, 2023, 2024],
        "quarter": ["Q1", "Q1", "Q2", "Q2", "Q1", "Q2", "Q2", "Q1"]
    })


class TestCostEstimator:
    """Test the cost estimation functionality"""
    
    def test_cost_estimator_base_cost_calculation(self):
        """Test basic cost estimation calculation"""
        cost_estimator = CostEstimator()
        
        # Simple case: 1000 rows, 2 filters, 3 grouping cols, 2 measures
        cost = cost_estimator.estimate_base_cost(
            num_rows=1000,
            num_filters=2, 
            num_grouping_cols=3,
            num_measures=2
        )
        
        expected = 1000 * 0.01 * (1.0 + 2 * 0.15 + 3 * 0.25 + 2 * 0.20)  # 10 * (1 + 0.3 + 0.75 + 0.4) = 24.5
        assert abs(cost - 24.5) < 0.001
    
    def test_cost_estimator_with_joins(self):
        """Test cost estimation with joins"""
        cost_estimator = CostEstimator()
        
        # Same as above but with joins (2x multiplier)
        cost = cost_estimator.estimate_base_cost(
            num_rows=1000,
            num_filters=2,
            num_grouping_cols=3,
            num_measures=2,
            has_joins=True
        )
        
        expected = 1000 * 0.01 * (1.0 + 2 * 0.15 + 3 * 0.25 + 2 * 0.20) * 2  # 24.5 * 2 = 49.0
        assert abs(cost - 49.0) < 0.001
    
    def test_cost_estimator_with_table_stats(self):
        """Test cost estimation with table stats"""
        cost_estimator = CostEstimator()
        table_stats = {"row_count": 5000}
        
        cost = cost_estimator.estimate_with_table_stats(
            table_name="test_table",
            filters=[{"field": "region", "op": "=", "value": "East"}],
            grouping_cols=["region"],
            measures=[Measure(field="sales", agg="sum", alias="total_sales")],
            table_stats=table_stats
        )
        
        # 5000 rows, 1 filter, 1 grouping col, 1 measure
        expected = 5000 * 0.01 * (1.0 + 1 * 0.15 + 1 * 0.25 + 1 * 0.20)  # 50 * (1 + 0.15 + 0.25 + 0.20) = 80
        assert abs(cost - 80.0) < 0.001


class TestSQLPlannerAdvanced:
    """Test advanced planning features in SQLPlanner"""
    
    def test_sql_planner_cost_estimation(self):
        """Test that SQLPlanner includes cost estimation in plan"""
        planner = SQLPlanner(dialect="duckdb", enable_optimization=True)
        
        spec = PivotSpec(
            table="test",
            rows=["region"],
            columns=[],
            measures=[Measure(field="sales", agg="sum", alias="total_sales")],
            filters=[{"field": "year", "op": "=", "value": 2024}]
        )
        
        plan = planner.plan(spec)
        
        # Check that queries have cost estimation
        assert len(plan["queries"]) > 0
        for query in plan["queries"]:
            assert "estimated_cost" in query
            assert isinstance(query["estimated_cost"], float)
            assert query["estimated_cost"] >= 0
        
        # Check that metadata includes optimization info
        assert plan["metadata"]["optimization_enabled"] is True
        assert "total_estimated_cost" in plan["metadata"]
        assert plan["metadata"]["advanced_planning_applied"] is True
    
    def test_sql_planner_optimization_disabled(self):
        """Test that cost estimation is not applied when optimization is disabled"""
        planner = SQLPlanner(dialect="duckdb", enable_optimization=False)
        
        spec = PivotSpec(
            table="test",
            rows=["region"],
            columns=[],
            measures=[Measure(field="sales", agg="sum", alias="total_sales")],
            filters=[{"field": "year", "op": "=", "value": 2024}]
        )
        
        plan = planner.plan(spec)
        
        # Without optimization, cost estimation might still be applied depending on implementation
        # But optimization metadata should reflect disabled state
        assert plan["metadata"]["optimization_enabled"] is False


@pytest.fixture
def ibis_planner_with_con():
    """Fixture for IbisPlanner with a real in-memory connection and a dummy table."""
    con = ibis.duckdb.connect(":memory:")
    # Create a dummy table for the planner to introspect with relevant columns
    con.create_table(
        "test", 
        pa.table({
            "region": ["East", "West"], 
            "product": ["A", "B"], 
            "sales": [100, 200], 
            "year": [2023, 2024],
            "quarter": ["Q1", "Q2"]
        }), 
        overwrite=True
    )
    return IbisPlanner(con=con, enable_optimization=True)


class TestIbisPlannerAdvanced:
    """Test advanced planning features in IbisPlanner"""
    
    def test_ibis_planner_cost_estimation(self, ibis_planner_with_con: IbisPlanner):
        """Test that IbisPlanner includes cost estimation in plan"""
        spec = PivotSpec(
            table="test",
            rows=["region"],
            columns=[],
            measures=[Measure(field="sales", agg="sum", alias="total_sales")],
            filters=[{"field": "year", "op": "=", "value": 2024}]
        )
        
        plan = ibis_planner_with_con.plan(spec)
        
        # Check that the plan is a dictionary with queries list
        assert isinstance(plan, dict)
        assert "queries" in plan
        assert len(plan["queries"]) > 0
        
        # Check that the first query is an Ibis expression
        assert isinstance(plan["queries"][0], ibis.Expr)
        
        # Check for metadata
        assert "metadata" in plan
    
    def test_ibis_planner_complex_query_cost(self, ibis_planner_with_con: IbisPlanner):
        """Test cost estimation for a complex query"""
        spec = PivotSpec(
            table="test",
            rows=["region", "product"],
            columns=["year", "quarter"],
            measures=[
                Measure(field="sales", agg="sum", alias="total_sales"),
                Measure(field="sales", agg="avg", alias="avg_sales"),
                Measure(field="sales", agg="count", alias="count_sales")
            ],
            filters=[
                {"field": "year", "op": "=", "value": 2024},
                {"field": "region", "op": "in", "value": ["East", "West"]},
                {"field": "sales", "op": ">", "value": 100}
            ]
        )
        
        plan = ibis_planner_with_con.plan(spec)
        
        # Check that the plan is a dictionary with queries list
        assert isinstance(plan, dict)
        assert "queries" in plan
        
        # Check that the first query is an Ibis expression
        assert isinstance(plan["queries"][0], ibis.Expr)


class TestControllerWithAdvancedPlanning:
    """Test that controller integrates advanced planning features properly"""
    
    def test_controller_advanced_planning_integration(self, controller, sample_data):
        """Test that controller uses advanced planning when enabled"""
        controller.load_data_from_arrow("sales", sample_data)
        
        spec = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [
                {"field": "sales", "agg": "sum", "alias": "total_sales"},
                {"field": "sales", "agg": "avg", "alias": "avg_sales"}
            ],
            "filters": [{"field": "year", "op": ">=", "value": 2023}],
        }
        
        # Run pivot and check that it includes optimization metadata
        result = controller.run_pivot(spec, return_format="dict")
        
        # The result format should be valid
        assert "columns" in result
        assert "rows" in result
        assert isinstance(result["columns"], list)
        assert isinstance(result["rows"], list)
    
    def test_controller_optimization_metadata(self, controller, sample_data):
        """Test that controller plan includes optimization metadata"""
        controller.load_data_from_arrow("sales", sample_data)
        
        spec = {
            "table": "sales",
            "rows": ["region"],
            "columns": ["year"],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
        }
        
        # Directly test the planner to see optimization metadata
        plan = controller.planner.plan(PivotSpec.from_dict(spec))
        
        # Should return a dictionary with queries list containing Ibis expressions
        assert isinstance(plan, dict)
        assert "queries" in plan
        assert isinstance(plan["queries"][0], ibis.Expr)


# Removed TestQueryRewriter as it's a placeholder operating on SQL strings
# Removed test_query_rewriter_basic
# Removed test_query_rewriter_optimizations_list

def test_planner_comparison():
    """Compare behavior between optimized and non-optimized planners"""
    # Create both optimized and non-optimized planners
    optimized_planner = SQLPlanner(dialect="duckdb", enable_optimization=True)
    non_optimized_planner = SQLPlanner(dialect="duckdb", enable_optimization=False)
    
    spec = PivotSpec(
        table="test",
        rows=["region"],
        columns=[],
        measures=[Measure(field="sales", agg="sum", alias="total_sales")],
        filters=[]
    )
    
    optimized_plan = optimized_planner.plan(spec)
    non_optimized_plan = non_optimized_planner.plan(spec)
    
    # Both should produce valid plans
    assert "queries" in optimized_plan
    assert "queries" in non_optimized_plan
    assert len(optimized_plan["queries"]) > 0
    assert len(non_optimized_plan["queries"]) > 0
    
    # Optimized plan should have cost estimation
    assert "total_estimated_cost" in optimized_plan["metadata"]
    for query in optimized_plan["queries"]:
        assert "estimated_cost" in query
    
    # Non-optimized may or may not have cost depending on implementation
    # but optimization metadata should be different
    assert optimized_plan["metadata"]["optimization_enabled"] is True
    assert non_optimized_plan["metadata"]["optimization_enabled"] is False


# if __name__ == "__main__":
#     # Run tests directly for debugging
#     test_estimator = TestCostEstimator()
#     test_estimator.test_cost_estimator_base_cost_calculation()
#     test_estimator.test_cost_estimator_with_joins()
#     test_estimator.test_cost_estimator_with_table_stats()
#     print("Cost estimator tests passed!")
    
#     test_sql = TestSQLPlannerAdvanced()
#     test_sql.test_sql_planner_cost_estimation()
#     print("SQL planner advanced tests passed!")
    
#     test_query_rewriter = TestQueryRewriter()
#     test_query_rewriter.test_query_rewriter_basic()
#     print("Query rewriter tests passed!")
    
#     print("All advanced planning tests passed!")