"""
example_scalable_pivot.py - Example usage of the scalable pivot engine
"""
import asyncio
from pivot_engine import ScalablePivotController
from pivot_engine.types.pivot_spec import PivotSpec, Measure
import pyarrow as pa


async def example_scalable_features():
    """Demonstrate the scalable features of the pivot engine"""
    print("=== Scalable Pivot Engine Example ===")
    
    # Create controller with scalability features enabled
    controller = ScalablePivotController(
        backend_uri=":memory:",
        enable_streaming=True,
        enable_incremental_views=True,
        tile_size=50,
        cache_ttl=600
    )
    
    # Create sample data
    sample_data = pa.table({
        "region": ["North", "South", "East", "West"] * 2500,  # 10K rows to demonstrate scalability
        "product": ["A", "B", "C", "D", "E", "F", "G", "H"] * 1250,
        "sales": [100, 200, 150, 300, 120, 180, 90, 250] * 1250,
        "quarter": ["Q1", "Q2", "Q3", "Q4"] * 2500,
        "year": [2023, 2023, 2024, 2024] * 2500
    })
    
    # Load data into the database
    controller.load_data_from_arrow("sales", sample_data)
    print("SUCCESS: Sample data loaded")
    
    # Define pivot specification
    spec = PivotSpec(
        table="sales",
        rows=["region", "product", "quarter"],  # 3-level hierarchy
        measures=[
            Measure(field="sales", agg="sum", alias="total_sales"),
            Measure(field="sales", agg="avg", alias="avg_sales"),
            Measure(field="sales", agg="count", alias="transaction_count")
        ],
        filters=[],
        sort=[{"field": "region", "order": "asc"}]
    )
    
    print(f"SUCCESS: Pivot specification created with {len(spec.rows)}-level hierarchy")
    
    # 1. Run materialized hierarchy for performance
    print("\n1. Creating materialized hierarchies...")
    materialized_result = controller.run_materialized_hierarchy(spec)
    print(f"SUCCESS: {materialized_result}")

    # 2. Use intelligent prefetching
    print("\n2. Setting up intelligent prefetching...")
    user_session = {"user_id": "demo_user", "preferences": {"region": "North"}}
    expanded_paths = [["North"], ["South"]]
    prefetch_result = await controller.run_intelligent_prefetch(spec, user_session, expanded_paths)
    print(f"SUCCESS: Prefetch setup: {prefetch_result}")

    # 3. Run pruned hierarchical pivot
    print("\n3. Running pruned hierarchical pivot...")
    pruning_preferences = {
        "pruning_strategy": "top_n",
        "top_n": 5,
        "primary_measure": "total_sales"
    }
    pruned_result = controller.run_pruned_hierarchical_pivot(spec, expanded_paths, pruning_preferences)
    print(f"SUCCESS: Pruned result has {len(pruned_result.get('data', {}))} paths")

    # 4. Virtual scrolling for large datasets
    print("\n4. Demonstrating virtual scrolling...")
    virtual_result = controller.run_virtual_scroll_hierarchical(
        spec, start_row=0, end_row=100, expanded_paths=expanded_paths
    )
    print(f"SUCCESS: Virtual scroll returned {len(virtual_result) if isinstance(virtual_result, list) else 'data'} rows")

    # 5. Progressive hierarchical loading
    print("\n5. Running progressive hierarchical load...")
    def progress_callback(progress_info):
        print(f"   Progress: {progress_info['level']}/{progress_info['total_levels']} "
              f"({progress_info['progress']:.1%})")

    progressive_result = controller.run_progressive_hierarchical_load(
        spec, expanded_paths, pruning_preferences, progress_callback
    )
    print(f"SUCCESS: Progressive load completed with {progressive_result['metadata']['total_levels_loaded']} levels")

    # 6. Streaming aggregation (if enabled)
    if controller.enable_streaming:
        print("\n6. Setting up streaming aggregation...")
        streaming_result = await controller.run_streaming_aggregation(spec)
        print(f"SUCCESS: Streaming job created: {streaming_result}")

    # 7. CDC setup for real-time updates
    print("\n7. Setting up CDC for real-time updates...")
    from pivot_engine.cdc.database_change_detector import DatabaseChangeProducer

    # Create a real change stream producer
    change_producer = DatabaseChangeProducer(controller.backend)

    # Register the table for CDC tracking (this creates a background task)
    tracker_task = await change_producer.register_table_for_cdc("sales")

    # For demo purposes, we'll create a limited change stream
    async def demo_change_stream():
        from pivot_engine.cdc.cdc_manager import Change
        for i in range(3):
            yield Change(
                table="sales",
                type="INSERT",
                new_row={"region": "North", "product": f"Product_{i}", "sales": 100 + i*50}
            )
            await asyncio.sleep(0.1)

    cdc_manager = await controller.setup_cdc("sales", demo_change_stream())
    print("SUCCESS: CDC manager setup complete")

    # 8. Batch loading for multiple levels
    print("\n8. Running batch load for multiple levels...")
    batch_result = controller.run_hierarchical_pivot_batch_load(spec.to_dict(), expanded_paths, max_levels=2)
    print(f"SUCCESS: Batch load completed")

    print("\n=== All Scalable Features Demonstrated ===")
    return {
        "materialized": materialized_result,
        "prefetch": prefetch_result,
        "pruned": pruned_result,
        "virtual_scroll": virtual_result,
        "progressive": progressive_result,
        "streaming": streaming_result if controller.enable_streaming else None
    }


async def example_microservice_usage():
    """Example of using the microservice architecture"""
    print("\n=== Microservice Architecture Example ===")

    from pivot_engine.main import ScalablePivotApplication, FASTAPI_AVAILABLE
    from pivot_engine.config import ScalablePivotConfig
    
    # Create configuration for microservices
    config = ScalablePivotConfig(
        enable_microservices=True,
        max_concurrent_queries=20,
        query_timeout=60,
        virtual_scroll_threshold=500
    )
    
    # Create the application
    app = ScalablePivotApplication(config)

    if not FASTAPI_AVAILABLE:
        print("INFO: FastAPI not available, microservice features disabled")
        print("SUCCESS: Scalable Pivot Application created (basic features only)")
        return

    print("SUCCESS: Scalable Pivot Application created with microservices")

    # Create a pivot spec
    spec = {
        "table": "sales",
        "rows": ["region", "product"],
        "measures": [
            {"field": "sales", "agg": "sum", "alias": "total_sales"}
        ]
    }

    # Use the application to handle requests
    result = await app.handle_pivot_request(spec)
    print(f"SUCCESS: Pivot request handled: {result['status']}")

    # Hierarchical request through the application
    hier_result = await app.handle_hierarchical_request(
        spec,
        [["North"], ["South"]],
        {"enable_pruning": True, "pruning_strategy": "top_n", "top_n": 10}
    )
    print(f"SUCCESS: Hierarchical request handled: {hier_result['status']}")

    print("SUCCESS: Microservice architecture example completed")


async def main():
    """Main example function"""
    print("Scalable Pivot Engine - Comprehensive Example")
    print("=" * 50)
    
    # Run scalable features example
    results = await example_scalable_features()
    
    # Run microservice example
    await example_microservice_usage()
    
    print("\n" + "=" * 50)
    print("All examples completed successfully!")
    print("\nKey Scalable Features Demonstrated:")
    print("- Materialized Hierarchies for fast access")
    print("- Intelligent Prefetching based on user patterns") 
    print("- Hierarchical Data Pruning to reduce complexity")
    print("- Virtual Scrolling for large datasets")
    print("- Progressive Loading for better UX")
    print("- Streaming Aggregations for real-time updates")
    print("- CDC for real-time change tracking")
    print("- Microservice Architecture for horizontal scaling")
    print("- Distributed Caching with multiple levels")


if __name__ == "__main__":
    asyncio.run(main())