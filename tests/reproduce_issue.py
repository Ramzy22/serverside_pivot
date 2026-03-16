
import ibis
import pandas as pd
from pivot_engine.planner.ibis_planner import IbisPlanner
from pivot_engine.types.pivot_spec import PivotSpec, Measure

def reproduce_issue():
    # 1. Setup: Create test data matching app.py
    rows = 2000000 
    
    data_source = {
        "region": (["North", "South", "East", "West"] * (rows // 4)),
        "country": (["USA", "Canada", "Brazil", "UK", "China", "Japan", "Germany", "France"] * (rows // 8)),
        "product": (["Laptop", "Phone", "Tablet", "Monitor", "Headphones"] * (rows // 5)),
        "sales": [x % 1000 for x in range(rows)],
        "cost": [x % 800 for x in range(rows)],
    }
    df = pd.DataFrame(data_source)
    
    # Calculate expected sums for verification
    # 'West' contains 'France'.
    # France rows are indices where index % 8 == 7
    # Cost is x % 800.
    
    # Total Cost for France:
    france_df = df[df['country'] == 'France']
    expected_france_cost = france_df['cost'].sum()
    print(f"Expected Cost for France: {expected_france_cost}")
    
    con = ibis.connect("duckdb://:memory:")
    con.create_table('sales_data', df, overwrite=True)

    # 2. Spec: Exact filter failing in app.py
    # {'cost_sum': {'operator': 'AND', 'conditions': [{'type': 'eq', 'value': '199500000', 'caseSensitive': False}]}}
    
    spec = PivotSpec(
        table='sales_data',
        rows=['region', 'country'],
        measures=[Measure(field='cost', agg='sum', alias='cost_sum')],
        filters=[
            {
                'op': 'AND', 
                'conditions': [
                    {'field': 'cost_sum', 'op': 'eq', 'value': str(expected_france_cost), 'caseSensitive': False}
                ]
            }
        ]
    )

    # 3. Plan & Execute
    planner = IbisPlanner(con=con)
    plan_result = planner.plan(spec)
    query = plan_result['queries'][0]

    print("\n--- Generated SQL ---")
    try:
        print(con.compile(query))
    except:
        print(query)
    print("---------------------")

    result = query.execute()
    print("\n--- Result ---")
    print(result)
    print(f"Row count: {len(result)}")

if __name__ == "__main__":
    reproduce_issue()
