
import ibis
import pandas as pd
from pivot_engine.planner.ibis_planner import IbisPlanner
from pivot_engine.types.pivot_spec import PivotSpec, Measure

def test_mixed_composite_filter():
    # 1. Setup: Create test data
    df = pd.DataFrame({
        "region": ["North", "South", "North", "South"],
        "sales": [100, 200, 300, 400]
    })
    
    con = ibis.connect("duckdb://:memory:")
    con.create_table('sales_data', df, overwrite=True)

    # 2. Spec: Composite filter with mixed fields
    # We want rows where region == 'North' AND total_sales > 150
    spec = PivotSpec(
        table='sales_data',
        rows=['region'],
        measures=[Measure(field='sales', agg='sum', alias='total_sales')],
        filters=[
            {
                'op': 'AND', 
                'conditions': [
                    {'field': 'region', 'op': 'eq', 'value': 'North'},
                    {'field': 'total_sales', 'op': 'gt', 'value': 150}
                ]
            }
        ]
    )

    # 3. Plan & Execute
    planner = IbisPlanner(con=con)
    plan_result = planner.plan(spec)
    query = plan_result['queries'][0]

    print("\n--- Generated SQL ---")
    print(con.compile(query))
    
    result = query.execute()
    print("\n--- Result ---")
    print(result)
    
    # Expected: Only 'North' because North sum is 400 (> 150)
    # If the bug exists, 'total_sales > 150' might be ignored or fail.
    # If it's ignored, we'd get North and South (if South sum > 150 too, which it is 600).
    # Wait, South sum is 600. So both would match if the region filter was ignored?
    # No, if the whole AND is treated as pre_filter, and total_sales is ignored, it becomes just region == 'North'.
    # Result should be only North.
    # If correctly handled, it should be North.
    
    # Let's try to make it fail by putting total_sales first.
    spec2 = PivotSpec(
        table='sales_data',
        rows=['region'],
        measures=[Measure(field='sales', agg='sum', alias='total_sales')],
        filters=[
            {
                'op': 'AND', 
                'conditions': [
                    {'field': 'total_sales', 'op': 'gt', 'value': 150},
                    {'field': 'region', 'op': 'eq', 'value': 'North'}
                ]
            }
        ]
    )
    
    plan_result2 = planner.plan(spec2)
    query2 = plan_result2['queries'][0]
    print("\n--- Generated SQL 2 ---")
    print(con.compile(query2))
    result2 = query2.execute()
    print("\n--- Result 2 ---")
    print(result2)

if __name__ == "__main__":
    import os
    import sys
    # Add pivot_engine to sys.path
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "dash_tanstack_pivot", "pivot_engine")))
    test_mixed_composite_filter()
