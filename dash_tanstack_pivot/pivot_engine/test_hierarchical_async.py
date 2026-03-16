#!/usr/bin/env python3

import asyncio
import pyarrow as pa
from pivot_engine.controller import PivotController

async def test_async_hierarchical_functionality():
    print("Testing async hierarchical pivot functionality...")
    
    controller = PivotController()
    
    # Create sample hierarchical data
    data = pa.table({
        'region': ['North', 'North', 'South', 'South'],
        'city': ['A', 'B', 'C', 'D'],
        'sales': [100, 150, 200, 120]
    })
    
    controller.load_data_from_arrow('hier_test_table', data)
    
    # Test the async hierarchical pivot with prefetch method
    spec = {
        'table': 'hier_test_table',
        'rows': ['region', 'city'],
        'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'sales'}]
    }
    
    # This should now work asynchronously
    result = await controller.run_hierarchical_pivot_with_prefetch(spec, prefetch_depth=2)
    print(f"Async hierarchical result keys: {list(result.keys())}")
    print(f"Number of rows returned: {len(result.get('rows', []))}")
    
    print("Async hierarchical functionality test passed!")
    
if __name__ == "__main__":
    asyncio.run(test_async_hierarchical_functionality())