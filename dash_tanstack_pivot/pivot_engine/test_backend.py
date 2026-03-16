from pivot_engine.backends.duckdb_backend import DuckDBBackend
import pyarrow as pa

# Create backend and test
backend = DuckDBBackend(uri=":memory:")

# Create test data
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})
backend.create_table_from_arrow('sales_pagination', data)

# Test the exact SQL that's causing issues
sql = "SELECT city, SUM(sales) AS total_sales FROM sales_pagination WHERE (city > ?) GROUP BY city ORDER BY city ASC LIMIT 2"
params = ['City 1']

query_dict = {
    'sql': sql,
    'params': params
}

print("Executing SQL through DuckDBBackend...")
result = backend.execute(query_dict)
print(f"Result size: {result.num_rows} rows")
print(f"Result: {result.to_pydict()}")

# Test the same query directly with DuckDB to double-check
import duckdb
con = duckdb.connect()
con.register('sales_pagination', data)
direct_result = con.execute(sql, params).fetch_arrow_table()
print(f"\nDirect DuckDB result size: {direct_result.num_rows} rows")
print(f"Direct DuckDB result: {direct_result.to_pydict()}")