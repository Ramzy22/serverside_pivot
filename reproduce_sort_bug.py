
import ibis
import duckdb
import pandas as pd
import asyncio
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.tanstack_adapter import TanStackPivotAdapter, TanStackRequest, TanStackOperation
from pivot_engine.types.pivot_spec import Measure

async def debug_sorting():
    # 1. Create dummy data with a hidden sort key
    # 'Tenor' column has strings, '__sortkey__Tenor' has numeric days
    data = {
        'Tenor': ['1M', '2W', '1W', '1D', '1Y', '6M'],
        '__sortkey__Tenor': [30, 14, 7, 1, 365, 180],
        'Region': ['North', 'North', 'South', 'South', 'East', 'East'],
        'Sales': [100, 200, 150, 50, 300, 400]
    }
    df = pd.DataFrame(data)
    
    con = ibis.duckdb.connect()
    con.create_table('sales', df)
    
    controller = ScalablePivotController(backend_uri=":memory:")
    # Register the table in the controller's backend
    controller.backend.con.create_table('sales', df)
    
    adapter = TanStackPivotAdapter(controller)
    
    print("\n--- TEST 1: Sorting by Tenor (Custom Sort Key) ---")
    # Request sorting by Tenor using the hidden sort key
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[
            {"id": "Tenor", "header": "Tenor"},
            {"id": "Sales", "aggregationFn": "sum"}
        ],
        filters={},
        sorting=[{"id": "Tenor", "desc": False, "sortKeyField": "__sortkey__Tenor"}],
        grouping=["Tenor"],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 10}
    )
    
    # 2. Check PivotSpec conversion
    spec = adapter.convert_tanstack_request_to_pivot_spec(request)
    print(f"Converted PivotSpec Sort: {spec.sort}")
    
    # 3. Check Planner and SQL
    planner = controller.planner
    plan = planner.plan(spec)
    query = plan['queries'][0]
    sql = str(con.compile(query))
    print("\nGenerated SQL:")
    print(sql)
    
    # 4. Execute and check results
    result = await adapter.handle_request(request)
    print("\nResult Data (Should be sorted by days: 1D, 1W, 2W, 1M, 6M, 1Y):")
    for row in result.data:
        print(f"Tenor: {row.get('Tenor')}, Sales: {row.get('Sales')}")

    # 5. Check semantic tenor sorting (heuristic)
    print("\n--- TEST 2: Semantic Tenor Sorting (Heuristic) ---")
    request_semantic = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[
            {"id": "Tenor", "header": "Tenor"},
            {"id": "Sales", "aggregationFn": "sum"}
        ],
        filters={},
        sorting=[{"id": "Tenor", "desc": False}], # No sortKeyField, should trigger tenor heuristic
        grouping=["Tenor"],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 10}
    )
    
    result_semantic = await adapter.handle_request(request_semantic)
    print("\nResult Data (Semantic):")
    for row in result_semantic.data:
        print(f"Tenor: {row.get('Tenor')}, Sales: {row.get('Sales')}")

if __name__ == "__main__":
    asyncio.run(debug_sorting())
