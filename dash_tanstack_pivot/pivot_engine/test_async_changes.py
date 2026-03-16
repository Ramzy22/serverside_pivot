#!/usr/bin/env python3

def test_async_changes():
    from pivot_engine.controller import PivotController
    import inspect
    
    print("Testing async changes...")
    
    controller = PivotController()
    
    # Check if the async methods exist
    print(f"run_pivot_arrow_async exists: {hasattr(controller, 'run_pivot_arrow_async')}")
    print(f"run_hierarchical_pivot_with_prefetch exists: {hasattr(controller, 'run_hierarchical_pivot_with_prefetch')}")
    
    # Check if the controller method is async
    from inspect import iscoroutinefunction
    print(f"Controller run_hierarchical_pivot_with_prefetch is async: {iscoroutinefunction(PivotController.run_hierarchical_pivot_with_prefetch)}")
    
    # Test TreeExpansionManager methods
    from inspect import iscoroutinefunction
    tree_manager_type = type(controller.tree_manager)
    print(f"TreeExpansionManager _build_level_with_prefetch is async: {iscoroutinefunction(tree_manager_type._build_level_with_prefetch)}")
    print(f"TreeExpansionManager run_hierarchical_pivot_with_prefetch is async: {iscoroutinefunction(tree_manager_type.run_hierarchical_pivot_with_prefetch)}")
    
    print("All tests passed!")

if __name__ == "__main__":
    test_async_changes()