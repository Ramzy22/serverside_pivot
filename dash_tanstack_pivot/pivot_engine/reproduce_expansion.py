
import asyncio
from pivot_engine.tanstack_adapter import TanStackPivotAdapter, TanStackRequest, TanStackOperation
from pivot_engine.scalable_pivot_controller import ScalablePivotController

# Mock controller that returns a full hierarchy
class MockController:
    def run_hierarchical_pivot(self, spec):
        # Returns a simple hierarchy:
        # A
        # ├── A1
        # │   └── A1x
        # └── A2
        # B
        return {
            "rows": [
                ["A", "A1", "A1x", 10],
                ["A", "A2", None, 20],
                ["B", None, None, 30]
            ],
            "columns": ["l1", "l2", "l3", "val"]
        }

async def test_expansion():
    adapter = TanStackPivotAdapter(MockController())
    
    # We only want to see expanded paths.
    # If we expand ["A"], we should see A's children.
    # If we don't expand ["A", "A1"], we shouldn't see A1's children (A1x).
    
    expanded_paths = [["A"]] # A is expanded, A1 is NOT expanded.
    
    # We need to simulate how the adapter should filter this.
    # Currently _apply_expansion_state just returns everything.
    
    result = adapter._apply_expansion_state(
        adapter.controller.run_hierarchical_pivot({}),
        expanded_paths
    )
    
    print("Rows after expansion filtering:")
    for row in result['rows']:
        print(row)

if __name__ == "__main__":
    asyncio.run(test_expansion())
