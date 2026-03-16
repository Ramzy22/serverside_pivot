from pivot_engine.controller import PivotController
from pivot_engine.planner.sql_planner import SQLPlanner
from pivot_engine.types.pivot_spec import PivotSpec, Measure
import pyarrow as pa
import duckdb

print("=== Testing the SQL query directly vs through controller ===")

# Create a DuckDB connection directly
con = duckdb.connect()
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})
con.register('sales_pagination', data)
print("Data registered in DuckDB")

# Create the exact same spec as in the test
spec_dict = {
    'table': 'sales_pagination',
    'rows': ['city'],
    'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
    'sort': [{'field': 'city', 'order': 'asc'}],
    'limit': 2,
    'cursor': {'city': 'City 1'}
}

# Generate the plan using SQLPlanner directly
planner = SQLPlanner(dialect='duckdb')
spec_obj = PivotSpec.from_dict(spec_dict)
plan = planner.plan(spec_obj)

print("Generated plan:")
print(plan)

# Execute the SQL directly
queries = plan.get("queries", [])
query = queries[0]
print(f"\nDirect SQL execution:")
print(f"SQL: {query.get('sql')}")
print(f"Params: {query.get('params')}")

result = con.execute(query.get('sql'), query.get('params')).fetch_arrow_table()
print(f"Direct execution result: {result.num_rows} rows")
for i in range(result.num_rows):
    print(f"  Row {i}: {result.to_pydict()['city'][i]}, {result.to_pydict()['total_sales'][i]}")

print("\n" + "="*50)

# Now test through the controller
print("Testing through controller...")
controller = PivotController(backend_uri=':memory:', planner_name='sql')
controller.load_data_from_arrow('sales_pagination', data)

result_dict = controller.run_pivot(spec_dict, return_format='dict')
print(f"Controller result: {len(result_dict['rows'])} rows")
for i, row in enumerate(result_dict['rows']):
    print(f"  Row {i}: {row[0]}, {row[1]}")