#!/usr/bin/env python3
"""Repeatable end-to-end benchmark for the in-process pivot runtime.

This is an observation tool, not a fixed CI gate: database and CPU speed vary
between developer machines.  It reports data-load time, cold and warm pivot
latency, repeated-request latency, a paged viewport request, row throughput,
and observed cache activity. Supply
``--max-*-ms`` flags when a reproducible environment needs a pass/fail budget.

Examples:

    python scripts/benchmark_pivot_runtime.py
    python scripts/benchmark_pivot_runtime.py --rows 500000 --repeat-runs 7
    # npm 11 requires the extra option separator when forwarding flags.
    npm run benchmark:runtime -- -- --rows 100000 --json
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, TypeVar


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = REPOSITORY_ROOT / "dash_tanstack_pivot" / "pivot_engine"
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

import pyarrow as pa

from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import Measure, PivotSpec


T = TypeVar("T")


def _timed(operation: Callable[[], T]) -> Tuple[float, T]:
    started_at = time.perf_counter()
    result = operation()
    return (time.perf_counter() - started_at) * 1000, result


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * percentile) - 1)
    return ordered[index]


def build_sales_table(row_count: int) -> pa.Table:
    """Create deterministic, moderately-cardinal data without external I/O."""
    if row_count < 1:
        raise ValueError("row_count must be at least 1")

    regions = [f"Region {index % 24:02d}" for index in range(row_count)]
    products = [f"Product {(index // 24) % 120:03d}" for index in range(row_count)]
    quarters = [f"Q{(index // (24 * 120)) % 4 + 1}" for index in range(row_count)]
    desks = [f"Desk {(index // (24 * 120 * 4)) % 8:02d}" for index in range(row_count)]
    sales = [float((index * 37) % 10_000) / 10 for index in range(row_count)]
    units = [index % 19 + 1 for index in range(row_count)]

    return pa.table(
        {
            "region": regions,
            "product": products,
            "quarter": quarters,
            "desk": desks,
            "sales": sales,
            "units": units,
        }
    )


def build_pivot_spec() -> PivotSpec:
    return PivotSpec(
        table="benchmark_sales",
        rows=["region", "product"],
        columns=["quarter"],
        measures=[
            Measure(field="sales", agg="sum", alias="sales_sum"),
            Measure(field="units", agg="sum", alias="units_sum"),
        ],
        sort=[{"field": "region", "order": "asc"}, {"field": "product", "order": "asc"}],
        limit=5_000,
    )


def run_benchmark(row_count: int, repeat_runs: int) -> Dict[str, Any]:
    if repeat_runs < 1:
        raise ValueError("repeat_runs must be at least 1")

    data = build_sales_table(row_count)
    controller = ScalablePivotController(
        backend_uri=":memory:",
        enable_streaming=False,
        enable_incremental_views=False,
        tile_size=200,
        cache_ttl=600,
    )
    spec = build_pivot_spec()

    try:
        load_ms, _ = _timed(lambda: controller.load_data_from_arrow("benchmark_sales", data))
        cold_ms, cold_result = _timed(
            lambda: controller.run_pivot(spec, return_format="dict", force_refresh=True)
        )

        repeat_ms = [
            _timed(lambda: controller.run_pivot(spec, return_format="dict"))[0]
            for _ in range(repeat_runs)
        ]

        viewport_spec = spec.copy()
        viewport_spec.columns = []
        viewport_spec.limit = 200
        viewport_spec.offset = 1_000
        viewport_ms, viewport_result = _timed(
            lambda: controller.run_pivot(viewport_spec, return_format="dict")
        )

        output_rows = len(cold_result.get("rows", []))
        return {
            "inputRows": row_count,
            "outputRows": output_rows,
            "outputColumns": len(cold_result.get("columns", [])),
            "timingsMs": {
                "load": round(load_ms, 3),
                "coldPivot": round(cold_ms, 3),
                "repeatPivotP50": round(statistics.median(repeat_ms), 3),
                "repeatPivotP95": round(_percentile(repeat_ms, 0.95), 3),
                "viewportPage": round(viewport_ms, 3),
            },
            "throughput": {
                "coldInputRowsPerSecond": round(row_count / max(cold_ms / 1000, 0.000001), 1),
                "repeatInputRowsPerSecond": round(
                    row_count / max(statistics.median(repeat_ms) / 1000, 0.000001), 1
                ),
            },
            "cache": {
                "hits": int(getattr(controller, "_cache_hits", 0)),
                "misses": int(getattr(controller, "_cache_misses", 0)),
            },
            "viewportRows": len(viewport_result.get("rows", [])),
            "repeatRuns": repeat_runs,
        }
    finally:
        controller.close()


def _print_report(report: Dict[str, Any]) -> None:
    timings = report["timingsMs"]
    throughput = report["throughput"]
    cache = report["cache"]
    print(f"Input rows:       {report['inputRows']:,}")
    print(f"Pivot result:     {report['outputRows']:,} rows × {report['outputColumns']} columns")
    print(f"Data load:        {timings['load']:,.1f} ms")
    print(f"Cold pivot:       {timings['coldPivot']:,.1f} ms ({throughput['coldInputRowsPerSecond']:,.0f} rows/s)")
    print(f"Repeat pivot p50: {timings['repeatPivotP50']:,.1f} ms ({throughput['repeatInputRowsPerSecond']:,.0f} rows/s)")
    print(f"Repeat pivot p95: {timings['repeatPivotP95']:,.1f} ms over {report['repeatRuns']} runs")
    print(f"Viewport page:    {timings['viewportPage']:,.1f} ms ({report['viewportRows']:,} rows)")
    print(f"Cache activity:   {cache['hits']} hits / {cache['misses']} misses")


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark pivot runtime latency and throughput.")
    parser.add_argument("--rows", type=int, default=100_000, help="Synthetic input rows (default: 100000).")
    parser.add_argument("--repeat-runs", type=int, default=5, help="Repeated pivot runs after the cold request (default: 5).")
    parser.add_argument("--max-cold-ms", type=float, default=None, help="Optional cold-pivot budget.")
    parser.add_argument("--max-repeat-p95-ms", type=float, default=None, help="Optional repeated-request p95 budget.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON only.")
    args = parser.parse_args()

    report = run_benchmark(args.rows, args.repeat_runs)
    if args.json:
        print(json.dumps(report, sort_keys=True))
    else:
        _print_report(report)

    failures = []
    if args.max_cold_ms is not None and report["timingsMs"]["coldPivot"] > args.max_cold_ms:
        failures.append(f"cold pivot {report['timingsMs']['coldPivot']} ms exceeds {args.max_cold_ms} ms")
    if args.max_repeat_p95_ms is not None and report["timingsMs"]["repeatPivotP95"] > args.max_repeat_p95_ms:
        failures.append(f"repeat p95 {report['timingsMs']['repeatPivotP95']} ms exceeds {args.max_repeat_p95_ms} ms")
    if failures:
        print("Benchmark budget failure: " + "; ".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
