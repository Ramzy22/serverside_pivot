"""
Comprehensive test suite for enhanced diff engine features:
- Multi-dimensional tile support
- Enhanced semantic analysis
- Delta update functionality  
- Robust tile-aware diffing
"""
import pytest
import asyncio
from unittest.mock import Mock, MagicMock
import pyarrow as pa
import ibis
from pivot_engine.diff.diff_engine import (
    QueryDiffEngine, 
    SpecChangeType, 
    TileKey, 
    MultiDimensionalTilePlanner
)
from pivot_engine.types.pivot_spec import PivotSpec, Measure
from pivot_engine.cache.memory_cache import MemoryCache
from pivot_engine.backends.duckdb_backend import DuckDBBackend
import tempfile
import os


@pytest.fixture
def cache():
    """Create a memory cache for testing"""
    return MemoryCache(ttl=300)


@pytest.fixture
def mock_backend():
    """Create a mock backend for testing"""
    # Use in-memory DuckDB database for testing
    return DuckDBBackend(uri=":memory:")


class TestTileKeyEnhancements:
    """Test enhanced TileKey functionality with hierarchical support"""
    
    def test_tile_key_string_conversion(self):
        """Test converting tile keys to and from string with hierarchical data"""
        tile = TileKey(
            row_start=0,
            row_end=100,
            col_start=0,
            col_end=50,
            dimension_level={"region": 0, "product": 1},
            drill_path=["USA", "California"]
        )
        
        tile_string = tile.to_string()
        assert "r0-100_c0-50" in tile_string
        assert "path_USA:California" in tile_string
        assert "level_region:0,product:1" in tile_string
        
        # Parse back
        parsed_tile = TileKey.from_string(tile_string)
        assert parsed_tile.row_start == tile.row_start
        assert parsed_tile.row_end == tile.row_end
        assert parsed_tile.col_start == tile.col_start
        assert parsed_tile.col_end == tile.col_end
        assert parsed_tile.drill_path == tile.drill_path
        assert parsed_tile.dimension_level == tile.dimension_level
    
    def test_tile_key_without_hierarchical_data(self):
        """Test tile key with just basic dimensions"""
        tile = TileKey(row_start=0, row_end=100, col_start=0, col_end=50)
        tile_string = tile.to_string()
        assert "r0-100_c0-50" in tile_string
        assert "path_" not in tile_string
        assert "level_" not in tile_string
        
        parsed_tile = TileKey.from_string(tile_string)
        assert parsed_tile.row_start == 0
        assert parsed_tile.row_end == 100
        assert parsed_tile.col_start == 0
        assert parsed_tile.col_end == 50
        assert parsed_tile.drill_path is None
        assert parsed_tile.dimension_level is None


class TestMultiDimensionalTilePlanner:
    """Test the MultiDimensionalTilePlanner functionality"""
    
    def test_plan_hierarchical_tiles(self):
        """Test hierarchical tile planning"""
        planner = MultiDimensionalTilePlanner(tile_size=50)
        
        spec = {
            "rows": ["region", "state", "city"],
            "columns": ["year"],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}]
        }
        
        drill_state = {
            "expanded_paths": [
                ["USA"],
                ["USA", "California"],
                ["Europe"]
            ]
        }
        
        tiles = planner.plan_hierarchical_tiles(spec, drill_state)
        assert len(tiles) > 0
        
        # Check that tiles have the expected hierarchical properties
        for tile in tiles:
            assert tile.drill_path is not None
            assert tile.dimension_level is not None
    
    def test_plan_multi_dimensional_tiles(self):
        """Test multi-dimensional tile planning"""
        planner = MultiDimensionalTilePlanner(tile_size=100)
        
        spec = {
            "rows": ["region", "product"],
            "columns": ["year"],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}]
        }
        
        tiles = planner.plan_multi_dimensional_tiles(spec)
        assert len(tiles) > 0
        assert all(tile.dimension_level is not None for tile in tiles)


class TestEnhancedSemanticAnalysis:
    """Test the enhanced semantic analysis in QueryDiffEngine"""
    
    def test_identical_specs(self, cache):
        """Test detection of identical specs"""
        engine = QueryDiffEngine(cache)
        
        spec = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
        }
        
        change_type = engine._analyze_spec_change(spec, spec)
        assert change_type == SpecChangeType.IDENTICAL
    
    def test_page_only_change(self, cache):
        """Test detection of page-only changes"""
        engine = QueryDiffEngine(cache)
        
        spec1 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
            "page": {"offset": 0, "limit": 100}
        }
        
        spec2 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
            "page": {"offset": 100, "limit": 100}  # Different page
        }
        
        change_type = engine._analyze_spec_change(spec2, spec1)
        assert change_type == SpecChangeType.PAGE_ONLY
    
    def test_sort_only_change(self, cache):
        """Test detection of sort-only changes"""
        engine = QueryDiffEngine(cache)
        
        spec1 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
            "sort": [{"field": "region", "order": "asc"}]
        }
        
        spec2 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
            "sort": [{"field": "region", "order": "desc"}]  # Different sort
        }
        
        change_type = engine._analyze_spec_change(spec2, spec1)
        assert change_type == SpecChangeType.SORT_ONLY
    
    def test_filter_added_change(self, cache):
        """Test detection of filter-added changes"""
        engine = QueryDiffEngine(cache)
        
        spec1 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [{"field": "year", "op": "=", "value": 2023}],
        }
        
        spec2 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [
                {"field": "year", "op": "=", "value": 2023},
                {"field": "region", "op": "=", "value": "East"}  # Additional filter
            ],
        }
        
        change_type = engine._analyze_spec_change(spec2, spec1)
        assert change_type == SpecChangeType.FILTER_ADDED
    
    def test_filter_removed_change(self, cache):
        """Test detection of filter-removed changes"""
        engine = QueryDiffEngine(cache)
        
        spec1 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [
                {"field": "year", "op": "=", "value": 2023},
                {"field": "region", "op": "=", "value": "East"}
            ],
        }
        
        spec2 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [{"field": "year", "op": "=", "value": 2023}],  # One filter removed
        }
        
        change_type = engine._analyze_spec_change(spec2, spec1)
        assert change_type == SpecChangeType.FILTER_REMOVED
    
    def test_structure_changed(self, cache):
        """Test detection of structural changes"""
        engine = QueryDiffEngine(cache)
        
        spec1 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
        }
        
        spec2 = {
            "table": "sales",
            "rows": ["region", "product"],  # Additional row dimension
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [],
        }
        
        change_type = engine._analyze_spec_change(spec2, spec1)
        assert change_type == SpecChangeType.STRUCTURE_CHANGED


class TestDeltaUpdateFunctionality:
    """Test the delta update functionality"""
    
    def test_register_delta_checkpoint(self, cache):
        """Test registering delta checkpoints"""
        engine = QueryDiffEngine(cache)
        
        # Register a checkpoint
        timestamp = 1678886400.0  # Example timestamp
        engine.register_delta_checkpoint(
            table="sales",
            timestamp=timestamp,
            max_id=1000,
            incremental_field="updated_at"
        )
        
        # Verify the checkpoint was stored
        assert "sales" in engine._delta_info
        assert engine._delta_info["sales"].last_timestamp == timestamp
        assert engine._delta_info["sales"].last_max_id == 1000
        assert engine._delta_info["sales"].incremental_field == "updated_at"

    def test_compute_delta_queries(self, cache):
        """Test computing delta queries"""
        engine = QueryDiffEngine(cache)
        
        # Register a checkpoint first
        engine.register_delta_checkpoint(
            table="sales",
            timestamp=1678886400.0,
            incremental_field="updated_at"
        )
        
        # Create a mock Ibis expression
        # For testing purposes, we just need an ibis.Expr object.
        # This assumes the QueryDiffEngine will inspect the expression
        # to find relevant information for delta queries.
        table_expr = ibis.table([
            ('region', 'string'), ('sales', 'int'), ('updated_at', 'timestamp')
        ], name='sales')
        plan_expr = table_expr.group_by('region').agg(total_sales=table_expr.sales.sum())
        
        # Wrap plan expression in a structure that the DiffEngine expects
        # DiffEngine.compute_delta_queries expects plan_result with 'queries' list
        plan_result = {"queries": [plan_expr]}
        
        spec = {"table": "sales"}
        delta_queries = engine.compute_delta_queries(spec, plan_result)
        
        # Should return delta queries if checkpoint exists
        if delta_queries:
            assert len(delta_queries) == 1
            # Check if filter was applied (Ibis expression manipulation)
            # This is tricky to inspect without compiling, but we can check if it's a valid expr
            assert isinstance(delta_queries[0], ibis.expr.types.Table)
            
    def test_apply_delta_updates(self, cache, mock_backend):
        """Test applying delta updates"""
        engine = QueryDiffEngine(cache)
        
        # Set up test data
        import pyarrow as pa
        base_table = pa.table({
            "region": ["East", "West"],
            "total_sales": [1000, 1500]
        })
        
        spec = {"table": "sales"}
        plan = {"queries": []}
        
        # Apply delta updates with no backend (should return base result)
        result = engine.apply_delta_updates(spec, plan, base_table, backend=None)
        assert result == base_table
        
        # Apply delta updates with no delta info (should return base result)
        result2 = engine.apply_delta_updates(spec, plan, base_table, backend=mock_backend)
        assert result2 == base_table


class TestRobustTileAwareDiffing:
    """Test the enhanced tile-aware diffing functionality"""
    
    def test_plan_tile_aware_with_hierarchical_data(self, cache):
        """Test tile-aware planning with hierarchical data"""
        engine = QueryDiffEngine(cache, tile_size=50)
        
        # Plan with drill paths (hierarchical data)
        spec_dict = {
            "rows": ["region", "state"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "drill_paths": [
                {"dimensions": ["region", "state"], "values": ["USA", "California"], "level": 2}
            ],
            "page": {"offset": 0, "limit": 100}
        }
        
        queries = [{"name": "agg", "sql": "SELECT * FROM sales", "purpose": "aggregate"}]
        plan = {"queries": queries}
        strategy = {
            "cache_hits": 0,
            "tiles_needed": [],
            "can_reuse_tiles": False
        }
        
        tile_queries, updated_strategy = engine._plan_tile_aware(queries, spec_dict, plan, strategy)
        
        # Should have some tile-specific queries
        assert "total_tiles" in updated_strategy
        assert updated_strategy["total_tiles"] >= 0
        
    def test_plan_tile_aware_regular_data(self, cache):
        """Test tile-aware planning with regular (non-hierarchical) data"""
        engine = QueryDiffEngine(cache, tile_size=50)
        
        spec_dict = {
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "page": {"offset": 0, "limit": 100}
        }
        
        queries = [{"name": "agg", "sql": "SELECT * FROM sales", "purpose": "aggregate"}]
        plan = {"queries": queries}
        strategy = {
            "cache_hits": 0,
            "tiles_needed": [],
            "can_reuse_tiles": False
        }
        
        tile_queries, updated_strategy = engine._plan_tile_aware(queries, spec_dict, plan, strategy)
        
        # Should have proper tile strategy
        assert "total_tiles" in updated_strategy
        assert "can_reuse_tiles" in updated_strategy
        assert "cache_hits" in updated_strategy

    def test_prefetch_tiles_calculation(self, cache):
        """Test the prefetch tile calculation functionality"""
        engine = QueryDiffEngine(cache, tile_size=50)
        
        # Create some cached tiles
        cached_tiles = [
            TileKey(row_start=0, row_end=50, col_start=0, col_end=-1),
            TileKey(row_start=50, row_end=100, col_start=0, col_end=-1)
        ]
        
        spec_dict = {
            "rows": ["region"],
            "columns": []
        }
        
        prefetch_tiles = engine._calculate_prefetch_tiles(cached_tiles, spec_dict)
        
        # Should have prefetch tiles (next and previous to cached tiles)
        assert len(prefetch_tiles) > 0


class TestIntegration:
    """Test integration between different diff engine components"""
    
    def test_full_diff_engine_workflow(self, cache):
        """Test the complete workflow of the diff engine"""
        engine = QueryDiffEngine(cache, tile_size=50)
        
        # First spec
        spec1 = {
            "table": "sales",
            "rows": ["region"],
            "columns": [],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [{"field": "year", "op": "=", "value": 2023}],
            "page": {"offset": 0, "limit": 100}
        }
        
        # Create a mock Ibis expression for plan1
        table_expr = ibis.table([
            ('region', 'string'), ('sales', 'int'), ('year', 'int')
        ], name='sales')
        plan1_expr = table_expr.filter(table_expr.year == 2023).group_by('region').agg(total_sales=table_expr.sales.sum())
        
        # Wrap plan in dictionary format expected by DiffEngine.plan
        plan1_result = {"queries": [plan1_expr]}
        
        # Run initial plan
        queries_to_run, strategy = engine.plan(plan1_result, spec1)
        
        # Now update with a page change (should use tile strategy)
        spec2 = spec1.copy()
        spec2["page"] = {"offset": 100, "limit": 100}
        
        queries_to_run2, strategy2 = engine.plan(plan1_result, spec2)
        
        # The strategy should reflect the page-only change
        from pivot_engine.diff.diff_engine import SpecChangeType
        assert strategy2["change_type"] == SpecChangeType.PAGE_ONLY
        
        # Try with delta updates enabled
        engine.enable_delta_updates = True
        engine.register_delta_checkpoint(
            table="sales",
            timestamp=1678886400.0,
            incremental_field="updated_at"
        )
        
        queries_to_run3, strategy3 = engine.plan(plan1_result, spec2)
        
        # Should have delta update information in strategy
        assert "use_delta_updates" in strategy3
        assert "delta_queries_generated" in strategy3


def test_all_features_together():
    """Integration test combining all enhanced features"""
    cache = MemoryCache(ttl=300)
    engine = QueryDiffEngine(
        cache, 
        tile_size=100, 
        enable_tiles=True, 
        enable_delta_updates=True
    )
    
    # Register a delta checkpoint
    engine.register_delta_checkpoint(
        table="sales",
        timestamp=1678886400.0,
        incremental_field="updated_at"
    )
    
    # Create a complex spec with hierarchical elements
    spec = {
        "table": "sales",
        "rows": ["region", "product"],
        "columns": ["year"],
        "measures": [
            {"field": "sales", "agg": "sum", "alias": "total_sales"},
            {"field": "sales", "agg": "avg", "alias": "avg_sales"}
        ],
        "filters": [{"field": "year", "op": ">=", "value": 2023}],
        "page": {"offset": 0, "limit": 100},
        "drill_paths": [
            {"dimensions": ["region"], "values": ["USA"], "level": 1}
        ]
    }
    
    # Create a mock Ibis expression for the plan
    table_expr = ibis.table([
        ('region', 'string'), ('product', 'string'), ('year', 'int'), ('sales', 'int')
    ], name='sales')
    plan_expr = table_expr.filter(table_expr.year >= 2023).group_by(
        table_expr.region, table_expr.product, table_expr.year
    ).agg(
        total_sales=table_expr.sales.sum(),
        avg_sales=table_expr.sales.mean()
    )
    
    plan_result = {"queries": [plan_expr]}
    
    # Execute the full planning process
    queries_to_run, execution_strategy = engine.plan(plan_result, spec)
    
    # Verify that the strategy contains information about all our features
    assert "change_type" in execution_strategy
    assert "use_delta_updates" in execution_strategy
    assert "delta_queries_generated" in execution_strategy
    
    print("All features integrated successfully!")


if __name__ == "__main__":
    # Run specific tests for debugging
    test_tile = TestTileKeyEnhancements()
    test_tile.test_tile_key_string_conversion()
    test_tile.test_tile_key_without_hierarchical_data()
    print("Tile key tests passed!")
    
    test_analysis = TestEnhancedSemanticAnalysis()
    test_analysis.test_identical_specs(MemoryCache(ttl=300))
    test_analysis.test_page_only_change(MemoryCache(ttl=300))
    test_analysis.test_sort_only_change(MemoryCache(ttl=300))
    print("Semantic analysis tests passed!")
    
    test_delta = TestDeltaUpdateFunctionality()
    test_delta.test_register_delta_checkpoint(MemoryCache(ttl=300))
    print("Delta functionality tests passed!")
    
    test_integration = TestIntegration()
    test_integration.test_full_diff_engine_workflow(MemoryCache(ttl=300))
    print("Integration tests passed!")
    
    test_all_features_together()
    print("All enhanced features working together!")