"""
Example usage of the pivot_engine with a file-based DuckDB database.
"""
import os
from pivot_engine.controller import PivotController
import json

def main():
    # Construct the path to the database file
    db_path = os.path.join(os.path.dirname(__file__), "sales.duckdb")
    
    print(f"Connecting to database: {db_path}")

    # Initialize the controller with the Ibis planner
    # You can change planner_name to "sql" to use the SQL planner
    controller = PivotController(backend_uri=db_path, planner_name="ibis")

    # Define the pivot specification
    spec = {
        "table": "sales",
        "rows": ["region"],
        "columns": [],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "filters": [{"field": "year", "op": "=", "value": 2024}],
    }

    print("\nRunning pivot with spec:")
    print(json.dumps(spec, indent=2))

    # Run the pivot query
    result = controller.run_pivot(spec, return_format="dict")

    print("\nPivot result:")
    # Pretty print the result
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()

