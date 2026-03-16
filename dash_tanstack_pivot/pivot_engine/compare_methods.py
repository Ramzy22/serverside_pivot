from pivot_engine.controller import PivotController
from pivot_engine.planner.sql_planner import SQLPlanner
from pivot_engine.types.pivot_spec import PivotSpec, Measure
import pyarrow as pa
import duckdb

print("=== Testing the difference between direct DuckDB and Controller ===")

# Method 1: Direct DuckDB
print("Method 1: Direct DuckDB")
con = duckdb.connect()
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})
con.register('sales_pagination', data)

# Use the same planner and execute the query
planner = SQLPlanner(dialect='duckdb')
spec_dict = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
    'cursor': {'city': 'City 1'}  # Assume cursor from first page
}
spec_obj = PivotSpec.from_dict(spec_dict)
plan = planner.plan(spec_obj)

queries = plan.get("queries", [])
query = queries[0]
print(f"SQL: {query.get('sql')}")
print(f"Params: {query.get('params')}")

result = con.execute(query.get('sql'), query.get('params')).fetch_arrow_table()
print(f"Direct execution result: {result.num_rows} rows")
for i in range(result.num_rows):
    print(f"  Row {i}: {result.to_pydict()['city'][i]}, {result.to_pydict()['total_sales'][i]}")

print("\n" + "="*60)

# Method 2: Through Controller
print("Method 2: Through Controller")
controller = PivotController(backend_uri=':memory:', planner_name='sql')
controller.load_data_from_arrow('sales_pagination', data)

result_dict = controller.run_pivot(spec_dict, return_format='dict')
print(f"Controller result: {len(result_dict['rows'])} rows")
for i, row in enumerate(result_dict['rows']):
    print(f"  Row {i}: {row[0]}, {row[1]}")

print("\n" + "="*60)

# Method 3: Full test with controller like in pytest
print("Method 3: Full test sequence with controller")
controller2 = PivotController(backend_uri=':memory:', planner_name='sql')
controller2.load_data_from_arrow('sales_pagination', data)

# First page
spec1 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
}
result1 = controller2.run_pivot(spec1, return_format='dict')
print(f"First page result: {len(result1['rows'])} rows")
for i, row in enumerate(result1['rows']):
    print(f"  Row {i}: {row[0]}, {row[1]}")

print(f"First page cursor: {result1['next_cursor']}")

# Second page with cursor
spec2 = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
    'cursor': result1['next_cursor'],
}
result2 = controller2.run_pivot(spec2, return_format='dict')
print(f"Second page result: {len(result2['rows'])} rows")
for i, row in enumerate(result2['rows']):
    print(f"  Row {i}: {row[0]}, {row[1]}")