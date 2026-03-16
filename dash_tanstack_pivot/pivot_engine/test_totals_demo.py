"""
Test script to demonstrate the Arrow-native totals feature
"""
from pivot_engine.controller import PivotController
import pyarrow as pa

def test_totals_feature():
    print("Testing Arrow-native totals feature...")
    
    # Create controller with Ibis planner
    controller = PivotController(planner_name="ibis", backend_uri=":memory:")
    
    # Create sample data
    sample_data = pa.table({
        "region": ["East", "West", "East", "West", "East", "West"],
        "product": ["A", "A", "B", "B", "A", "A"],
        "sales": [100, 200, 150, 250, 50, 300],
        "year": [2024, 2024, 2024, 2024, 2025, 2025]
    })
    
    # Load data
    controller.load_data_from_arrow("sales", sample_data)
    
    print("Sample data loaded:")
    print(f"  Region: {sample_data.column('region').to_pylist()}")
    print(f"  Sales: {sample_data.column('sales').to_pylist()}")
    print(f"  Total sales: {sum(sample_data.column('sales').to_pylist())}")
    print()
    
    # Define pivot spec with totals
    spec = {
        "table": "sales",
        "rows": ["region"],
        "columns": [],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
        "filters": [],
        "totals": True,  # Enable totals
    }
    
    print("Running pivot with totals...")
    result = controller.run_pivot(spec, return_format="dict")
    
    print(f"Result columns: {result['columns']}")
    print("Result rows:")
    for i, row in enumerate(result['rows']):
        region, sales = row
        if region is None:
            print(f"  [{i}] TOTAL ROW: region={region}, total_sales={sales}")
        else:
            print(f"  [{i}] Data row: region={region}, total_sales={sales}")
    
    # Verify that the total is correct
    data_rows = [row for row in result['rows'] if row[0] is not None]
    total_row = [row for row in result['rows'] if row[0] is None]
    
    if total_row:
        calculated_total = sum(row[1] for row in data_rows)  # Sum of individual region totals
        total_from_pivot = total_row[0][1]  # Value from totals row
        
        print(f"\nVerification:")
        print(f"  Sum of individual rows: {calculated_total}")
        print(f"  Value from totals row: {total_from_pivot}")
        print(f"  Match: {calculated_total == total_from_pivot}")
        
        if calculated_total == total_from_pivot:
            print("  SUCCESS: Arrow-native totals computation is working correctly!")
        else:
            print("  ERROR: Error in totals computation!")
    else:
        print("  ERROR: No totals row found!")

if __name__ == "__main__":
    test_totals_feature()