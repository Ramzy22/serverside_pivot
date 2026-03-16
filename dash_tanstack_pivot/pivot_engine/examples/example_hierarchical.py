"""
Example usage of the pivot_engine with hierarchical data.
"""
import os
from pivot_engine.controller import PivotController
import json

def main():
    # Construct the path to the database file
    db_path = os.path.join(os.path.dirname(__file__), "sales.duckdb")
    
    print(f"Connecting to database: {db_path}")

    # Initialize the controller with the Ibis planner
    controller = PivotController(backend_uri=db_path, planner_name="ibis")

    # =================================================================
    # 1. Run an initial hierarchical pivot (only the top level is loaded)
    # =================================================================
    spec = {
        "table": "sales",
        "rows": ["region", "product"],  # Defines the hierarchy using available columns
        "measures": [
            {"field": "sales", "agg": "sum", "alias": "total_sales"}
        ],
        "filters": [],
    }

    print("\nRunning initial hierarchical pivot with spec:")
    print(json.dumps(spec, indent=2))

    # Run the hierarchical pivot query
    result = controller.run_hierarchical_pivot(spec)

    print("\nHierarchical pivot result (top level only):")
    print(json.dumps(result["rows"], indent=2))
    
    # Keep the spec_hash for subsequent operations
    spec_hash = result["spec_hash"]
    print(f"\nSpec hash for this query: {spec_hash}")

    # =================================================================
    # 2. Toggle a node to expand it
    # =================================================================

    # Let's expand the first region (e.g., 'East' or 'West')
    # We'll take the first region from the results
    if result["rows"]:
        first_region = result["rows"][0].get("region")
        if first_region:
            path_to_expand = [first_region]
            print(f"\nToggling expansion for path: {path_to_expand}")

            toggle_result = controller.toggle_expansion(spec_hash, path_to_expand)
            print("Toggle result:", toggle_result)

            # =================================================================
            # 3. Re-run the hierarchical pivot to get the newly expanded data
            # =================================================================

            print("\nRe-running pivot to get data for expanded nodes:")

            # In a real application, you would re-run the *same* spec.
            # The controller and its tree manager will use the cached expansion state.
            result_after_expand = controller.run_hierarchical_pivot(spec)

            print(f"\nHierarchical pivot result (with '{first_region}' expanded):")
            print(json.dumps(result_after_expand["rows"], indent=2))

            # =================================================================
            # 4. Toggle the same node to collapse it
            # =================================================================

            print(f"\nToggling expansion again for path: {path_to_expand} to collapse it.")

            controller.toggle_expansion(spec_hash, path_to_expand)

            print("\nRe-running pivot after collapsing node:")
            result_after_collapse = controller.run_hierarchical_pivot(spec)

            print(f"\nHierarchical pivot result (with '{first_region}' collapsed again):")
            print(json.dumps(result_after_collapse["rows"], indent=2))
        else:
            print("No region found to expand")
    else:
        print("No results to expand")


if __name__ == "__main__":
    main()
