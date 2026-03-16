from pivot_engine.controller import PivotController
import pyarrow as pa

print("=== Reproducing the exact issue step-by-step ===")

controller = PivotController(backend_uri=':memory:', planner_name='sql')

# Create a larger dataset for pagination
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})
controller.load_data_from_arrow('sales_pagination', data)

print("\n--- First page ---")
spec1 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
}

result1 = controller.run_pivot(spec1, return_format='dict')
print(f"Result: {result1}")

print("\n--- Second page ---")
spec2 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
    'cursor': result1['next_cursor'], # Use the cursor from the previous result
}

# Now let's manually execute what the controller does
from pivot_engine.types.pivot_spec import PivotSpec
spec2_obj = controller._normalize_spec(spec2)
plan2 = controller.planner.plan(spec2_obj)

print(f"Planned SQL: {plan2['queries'][0]['sql']}")
print(f"Planned params: {plan2['queries'][0]['params']}")

# Execute diff engine plan
queries_to_run2, strategy2 = controller.diff_engine.plan(plan2, spec2_obj, force_refresh=False)
print(f"Queries to run count: {len(queries_to_run2)}")
print(f"Strategy: {strategy2}")

for i, query in enumerate(queries_to_run2):
    print(f"Query {i} to execute: {query['sql']} with params {query['params']}")
    
    # Execute the query directly
    result = controller.backend.execute(query)
    print(f"  Result: {result.num_rows} rows - {result.to_pydict()}")

# Now run the full controller method
print(f"\n--- Full controller method ---")
result2 = controller.run_pivot(spec2, return_format='dict')
print(f"Final result: {len(result2['rows'])} rows")
for i, row in enumerate(result2['rows']):
    print(f"  Row {i}: {row}")