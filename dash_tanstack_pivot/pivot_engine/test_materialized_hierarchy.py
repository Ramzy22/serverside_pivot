from pivot_engine.types.pivot_spec import PivotSpec, Measure
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.backends.duckdb_backend import DuckDBBackend
from pivot_engine.cache.memory_cache import MemoryCache

# Create the components for testing
backend = DuckDBBackend(':memory:')
cache = MemoryCache(ttl=300)

# Create test data
import pyarrow as pa
test_data = pa.table({
    'region': ['NA', 'SA', 'EU'],
    'country': ['US', 'BR', 'DE'], 
    'sales': [100, 200, 150]
})
backend.create_table_from_arrow('sales', test_data)

# Create manager 
manager = MaterializedHierarchyManager(backend, cache)

# Test with the same spec as the failing test
spec = PivotSpec(
    table='sales',
    rows=['region', 'country'],
    measures=[Measure(field='sales', agg='sum', alias='total_sales')],
    filters=[]  # Empty filters as in the test
)

print("Creating materialized hierarchy...")
# Call the method that was failing
try:
    manager.create_materialized_hierarchy(spec)
    print('SUCCESS: create_materialized_hierarchy worked!')
    print(f"Rollup tables created: {list(manager.rollup_tables.keys())}")
except Exception as e:
    print(f'ERROR: {e}')
    import traceback
    traceback.print_exc()