from pivot_engine.controller import PivotController
from pivot_engine.backends.duckdb_backend import DuckDBBackend
import pyarrow as pa

print("=== Testing if connection reuse causes issues ===")

# Create backend directly
backend = DuckDBBackend(uri=":memory:")

# Create test data
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})
backend.create_table_from_arrow('sales_pagination', data)

# Execute first query (without cursor)
sql1 = "SELECT city, SUM(sales) AS total_sales FROM sales_pagination GROUP BY city ORDER BY city ASC LIMIT 2"
params1 = []

query_dict1 = {'sql': sql1, 'params': params1}
print("Executing first query")
result1 = backend.execute(query_dict1)
print(f"First query result: {result1.num_rows} rows - {result1.to_pydict()}")

# Execute second query (with cursor, should return 2 rows with LIMIT 2)
sql2 = "SELECT city, SUM(sales) AS total_sales FROM sales_pagination WHERE (city > ?) GROUP BY city ORDER BY city ASC LIMIT 2"
params2 = ['City 1']

query_dict2 = {'sql': sql2, 'params': params2}
print(f"\nExecuting second query: {sql2} with params {params2}")
result2 = backend.execute(query_dict2)
print(f"Second query result: {result2.num_rows} rows - {result2.to_pydict()}")

# Let's also test the exact same SQL directly with DuckDB
print(f"\nTesting same SQL directly with DuckDB:")
import duckdb
con = duckdb.connect()
con.register('sales_pagination', data)
direct_result = con.execute(sql2, params2).fetch_arrow_table()
print(f"Direct DuckDB result: {direct_result.num_rows} rows - {direct_result.to_pydict()}")

# Now let's try to see if there's a state issue in the controller by 
# creating a new backend but sharing the same underlying connection
print(f"\n=== Testing with a fresh controller ===")
controller = PivotController(backend_uri=':memory:', planner_name='sql')
controller.load_data_from_arrow('sales_pagination', data)

# Execute the same query that was problematic
query_dict2 = {'sql': sql2, 'params': params2}
result3 = backend.execute(query_dict2)
print(f"Same query on fresh backend: {result3.num_rows} rows - {result3.to_pydict()}")