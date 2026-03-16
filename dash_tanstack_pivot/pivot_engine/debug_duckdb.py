import duckdb
import pyarrow as pa

# Connect to DuckDB
con = duckdb.connect()

# Create test data
data = pa.table({
    'city': [f'City {i}' for i in range(5)],
    'sales': [10, 20, 30, 40, 50],
})
con.register('sales_pagination', data)

# Execute the query that should limit to 2 rows after 'City 1'
sql = 'SELECT city, SUM(sales) AS total_sales FROM sales_pagination WHERE (city > ?) GROUP BY city ORDER BY city ASC LIMIT 2'
params = ['City 1']

result = con.execute(sql, params).fetch_arrow_table()
print("Result:")
print(result)
print("Number of rows:", result.num_rows)

# Let's also see what the original data looks like
all_data = con.execute('SELECT city, sales FROM sales_pagination ORDER BY city ASC').fetch_arrow_table()
print("\nAll data:")
print(all_data)