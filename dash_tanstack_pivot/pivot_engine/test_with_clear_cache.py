from pivot_engine.controller import PivotController
import pyarrow as pa

print("Testing with cache clearing...")
controller = PivotController(backend_uri=':memory:', planner_name='sql')

# Create a larger dataset for pagination
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})
controller.load_data_from_arrow('sales_pagination', data)

# --- First Page ---
spec1 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
}

result1 = controller.run_pivot(spec1, return_format='dict')
print(f"First page result: {len(result1['rows'])} rows")
print(f"First page cursor: {result1['next_cursor']}")

# Clear cache to prevent any interference 
controller.clear_cache()
print("Cache cleared")

# --- Second Page ---
spec2 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
    'cursor': result1['next_cursor'],  # Use the cursor from the previous result
}

result2 = controller.run_pivot(spec2, return_format='dict')
print(f"Second page result: {len(result2['rows'])} rows")
for i, row in enumerate(result2['rows']):
    print(f"  Row {i}: {row[0]}, {row[1]}")

print(f"Test result: Second page has {len(result2['rows'])} rows, expected 2")