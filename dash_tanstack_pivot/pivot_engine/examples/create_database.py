
import duckdb
import pyarrow as pa
import os

def create_sales_database():
    """
    Creates a DuckDB database file with sample sales data.
    """
    db_path = os.path.join(os.path.dirname(__file__), "sales.duckdb")
    
    # Delete existing database file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        
    con = duckdb.connect(db_path)

    # Create sample data
    sample_data = pa.table({
        "region": ["East", "West", "East", "West", "East", "West"],
        "product": ["A", "A", "B", "B", "A", "A"],
        "sales": [100, 200, 150, 250, 50, 300],
        "year": [2024, 2024, 2024, 2024, 2025, 2025]
    })

    # Create a table from the sample data
    con.execute("CREATE TABLE sales AS SELECT * FROM sample_data")

    print(f"Database 'sales.duckdb' created successfully in the 'examples' directory.")
    
    # Verify by listing tables
    print("\nTables in the database:")
    print(con.execute("SHOW TABLES").fetchall())
    
    # Verify content
    print("\nContent of the 'sales' table:")
    print(con.execute("SELECT * FROM sales LIMIT 5").fetch_arrow_table())

    con.close()

if __name__ == "__main__":
    create_sales_database()
