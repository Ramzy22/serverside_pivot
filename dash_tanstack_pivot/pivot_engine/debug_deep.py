from pivot_engine.controller import PivotController
from pivot_engine.planner.sql_planner import SQLPlanner
from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.diff.diff_engine import QueryDiffEngine
from pivot_engine.cache.memory_cache import MemoryCache
import pyarrow as pa

print("=== Deep debugging the issue ===")

# Create data
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})

print("Creating controller with debug settings...")
controller = PivotController(backend_uri=':memory:', planner_name='sql', cache=MemoryCache(ttl=600))
controller.load_data_from_arrow('sales_pagination', data)

print("\n--- First page request ---")
spec1 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
}

# Let's manually walk through the controller steps
print("Step 1: Normalizing spec")
spec1_obj = controller._normalize_spec(spec1)

print("Step 2: Planning")
plan1 = controller.planner.plan(spec1_obj)

print("Step 3: Getting diff plan")
queries_to_run1, strategy1 = controller.diff_engine.plan(plan1, spec1_obj, force_refresh=False, backend=controller.backend)

print("Step 4: Query execution")
results1 = [controller.backend.execute(query) for query in queries_to_run1]
print(f"  Number of queries to run: {len(queries_to_run1)}")
if results1:
    print(f"  First result has {results1[0].num_rows} rows")

print("Step 5: Converting to dict format")
result1 = controller._convert_table_to_dict(results1[0] if results1 else None, spec1_obj)
print(f"  First page result: {len(result1['rows'])} rows")
print(f"  First page cursor: {result1['next_cursor']}")

print("\n--- Second page request ---")
spec2 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
    'cursor': result1['next_cursor'],
}

print("Step 1: Normalizing spec")
spec2_obj = controller._normalize_spec(spec2)

print("Step 2: Planning")
plan2 = controller.planner.plan(spec2_obj)
print(f"  Generated SQL: {plan2['queries'][0]['sql']}")
print(f"  Parameters: {plan2['queries'][0]['params']}")

print("Step 3: Getting diff plan")
queries_to_run2, strategy2 = controller.diff_engine.plan(plan2, spec2_obj, force_refresh=False, backend=controller.backend)
print(f"  Strategy: {strategy2}")
print(f"  Number of queries to run: {len(queries_to_run2)}")

print("Step 4: Query execution")
results2 = [controller.backend.execute(query) for query in queries_to_run2]
print(f"  Results count: {len(results2)}")
if results2:
    print(f"  First result has {results2[0].num_rows} rows")
    print(f"  Result rows: {results2[0].to_pydict()}")

print("Step 5: Converting to dict format")
result2 = controller._convert_table_to_dict(results2[0] if results2 else None, spec2_obj)
print(f"  Second page result: {len(result2['rows'])} rows")
for i, row in enumerate(result2['rows']):
    print(f"    Row {i}: {row[0]}, {row[1]}")

print(f"\nFinal result: {len(result2['rows'])} rows (expected: 2)")