
import time
import statistics
import json
import math
from pivot_engine.controller import PivotController
from pivot_engine.types.pivot_spec import PivotSpec

# A realistic PivotSpec for benchmarking
BENCHMARK_SPEC = {
    "table": "sales",
    "rows": ["region", "product"],
    "columns": ["year"],
    "measures": [
        {"field": "sales", "agg": "sum", "alias": "total_sales"},
        {"field": "sales", "agg": "avg", "alias": "avg_sales"},
    ],
    "filters": [{"field": "year", "op": ">=", "value": 2022}],
    "sort": [ # Added for deterministic order
        {"field": "region", "order": "asc"},
        {"field": "product", "order": "asc"},
        {"field": "year", "order": "asc"},
    ],
    "page": {"offset": 0, "limit": 500}
}

sql_results = None
ibis_results = None

def run_benchmark(planner_name: str, num_runs: int = 10):
    """
    Runs a benchmark for a given planner.

    Args:
        planner_name: "sql" or "ibis"
        num_runs: The number of times to run the benchmark.
    """
    print(f"--- Benchmarking {planner_name.upper()} Planner ---")

    controller = PivotController(
        backend_uri="examples/sales_large.duckdb",
        planner_name=planner_name
    )

    spec = PivotSpec.from_dict(BENCHMARK_SPEC)

    # --- Benchmark planner.plan() ---
    plan_times = []
    print(f"Running planner.plan() {num_runs} times...")
    # Warm-up
    controller.planner.plan(spec)

    for _ in range(num_runs):
        start_time = time.perf_counter()
        controller.planner.plan(spec)
        end_time = time.perf_counter()
        plan_times.append((end_time - start_time) * 1000)

    print_stats("planner.plan()", plan_times)

    # --- Benchmark controller.run_pivot() ---
    run_pivot_times = []
    print(f"\nRunning controller.run_pivot() {num_runs} times...")
    # Warm-up and clear cache
    controller.clear_cache()
    
    # Store the result for comparison
    global sql_results, ibis_results
    if planner_name == "sql":
        sql_results = controller.run_pivot(spec, return_format="dict", force_refresh=True)
    else:
        ibis_results = controller.run_pivot(spec, return_format="dict", force_refresh=True)

    controller.clear_cache()


    for i in range(num_runs -1): # Subtract 1 because we already ran it once to store results
        start_time = time.perf_counter()
        # Force refresh to bypass diff engine caching and measure full execution
        controller.run_pivot(spec, force_refresh=True)
        end_time = time.perf_counter()
        run_pivot_times.append((end_time - start_time) * 1000)
        # Clear cache for the next run to be independent
        controller.clear_cache()


    print_stats("controller.run_pivot()", run_pivot_times)
    print("-" * (23 + len(planner_name)))
    controller.close()


def print_stats(name: str, times: list[float]):
    """Prints performance statistics."""
    print(f"  Results for {name}:")
    print(f"    Avg:    {statistics.mean(times):.2f} ms")
    print(f"    Median: {statistics.median(times):.2f} ms")
    print(f"    Min:    {min(times):.2f} ms")
    print(f"    Max:    {max(times):.2f} ms")
    print(f"    Stdev:  {statistics.stdev(times):.2f} ms")

def compare_rows(row1, row2, rel_tol=1e-9, abs_tol=0.0):
    if len(row1) != len(row2):
        return False
    for i in range(len(row1)):
        val1 = row1[i]
        val2 = row2[i]
        if isinstance(val1, float) and isinstance(val2, float):
            if not math.isclose(val1, val2, rel_tol=rel_tol, abs_tol=abs_tol):
                return False
        elif val1 != val2:
            return False
    return True


if __name__ == "__main__":
    run_benchmark("sql")
    print("\n")
    run_benchmark("ibis")
    
    print("\n--- Comparing Results ---")
    if sql_results and ibis_results:
        sql_rows = sql_results.get("rows", [])
        ibis_rows = ibis_results.get("rows", [])
        sql_cols = sql_results.get("columns", [])
        ibis_cols = ibis_results.get("columns", [])

        is_rows_identical = True
        if len(sql_rows) != len(ibis_rows):
            is_rows_identical = False
            print(f"Row count mismatch: SQL has {len(sql_rows)}, Ibis has {len(ibis_rows)}")
        else:
            for i, (sql_row, ibis_row) in enumerate(zip(sql_rows, ibis_rows)):
                if not compare_rows(sql_row, ibis_row):
                    print(f"Row {i} differs (ignoring float precision up to 1e-9):")
                    print(f"  SQL: {sql_row}")
                    print(f"  Ibis: {ibis_row}")
                    is_rows_identical = False
                    break 
        
        is_cols_identical = True
        if sql_cols != ibis_cols:
            is_cols_identical = False
            print("\nColumn names differ:")
            print(f"  SQL Columns: {sql_cols}")
            print(f"  Ibis Columns: {ibis_cols}")

        if is_rows_identical and is_cols_identical:
            print("Results for 'rows' and 'columns' are identical between SQL and Ibis planners (accounting for float precision).")
        else:
            print("Results for 'rows' and/or 'columns' DIFFER between SQL and Ibis planners.")

    else:
        print("Could not compare results: one or both planners did not produce results.")
