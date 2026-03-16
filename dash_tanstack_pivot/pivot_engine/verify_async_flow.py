
import asyncio
from typing import Any, Dict
from pivot_engine.tanstack_adapter import TanStackPivotAdapter, TanStackRequest, TanStackOperation

# Mock controller that supports run_pivot_async
class MockAsyncController:
    async def run_pivot_async(self, spec, return_format="dict", force_refresh=False):
        # Simulate async work
        await asyncio.sleep(0.01)
        return {
            "rows": [
                ["A", 10],
                ["B", 20]
            ],
            "columns": ["dim", "val"]
        }
        
    def run_hierarchical_pivot(self, spec):
        return {}

async def test_async_flow():
    controller = MockAsyncController()
    adapter = TanStackPivotAdapter(controller)
    
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[{"id": "dim"}, {"id": "val"}],
        filters=[],
        sorting=[],
        grouping=[],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 10}
    )
    
    print("Calling handle_request...")
    result = await adapter.handle_request(request)
    print(f"Result data: {result.data}")
    
    # Verify we got data back
    assert len(result.data) == 2
    print("Async flow verified successfully.")

if __name__ == "__main__":
    asyncio.run(test_async_flow())
