
import duckdb
import pyarrow as pa
import os
import random
import numpy as np

def create_large_sales_database(num_rows=1_000_000):
    """
    Creates a DuckDB database file with a large amount of sample sales data.
    """
    db_path = os.path.join(os.path.dirname(__file__), "sales_large.duckdb")
    
    # Delete existing database file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        
    con = duckdb.connect(db_path)

    print(f"Generating {num_rows} rows of sample data...")

    # Create sample data
    regions = ["East", "West", "North", "South", "Central"]
    products = [f"Product_{i}" for i in range(100)]
    
    region_data = [random.choice(regions) for _ in range(num_rows)]
    product_data = [random.choice(products) for _ in range(num_rows)]
    sales_data = np.random.uniform(10, 1000, size=num_rows)
    year_data = np.random.randint(2020, 2026, size=num_rows)

    large_sample_data = pa.table({
        "region": pa.array(region_data),
        "product": pa.array(product_data),
        "sales": pa.array(sales_data),
        "year": pa.array(year_data)
    })

    # Create a table from the sample data
    con.execute("CREATE TABLE sales AS SELECT * FROM large_sample_data")

    print(f"Database 'sales_large.duckdb' created successfully in the 'examples' directory.")
    
    # Verify by counting rows
    row_count = con.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    print(f"\nTotal rows in the 'sales' table: {row_count}")

    con.close()

if __name__ == "__main__":
    create_large_sales_database()
