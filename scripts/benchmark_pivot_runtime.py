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
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os
import statistics
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, TypeVar


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = REPOSITORY_ROOT / "serverside_pivot" / "pivot_engine"
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

import pyarrow as pa
import psutil

from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.resource_governor import (
    ResourceExhaustedError,
    ResourceGovernor,
    ResourceKind,
    ResourceLimits,
    WorkloadAccount,
    current_workload_account,
)
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


def run_memory_benchmark(
    rows: int = 500_000,
    concurrent_requests: int = 4,
    max_rss_bytes: int | None = None,
) -> Dict[str, Any]:
    """Run deterministic pressure work and report native RSS evidence.

    This mode is intentionally opt-in: ordinary benchmark and unit-test paths
    do not allocate the 500k/1M-row fixture. Admission is made deliberately
    contended (one query slot for several workers) so the report proves that
    overload is rejected without terminating the process.
    """
    if rows < 1:
        raise ValueError("rows must be at least 1")
    if concurrent_requests < 1:
        raise ValueError("concurrent_requests must be at least 1")
    if max_rss_bytes is not None and max_rss_bytes <= 0:
        raise ValueError("max_rss_bytes must be positive")

    process = psutil.Process(os.getpid())
    rss_samples: list[int] = []
    stop_sampling = threading.Event()

    def sample_rss() -> None:
        while not stop_sampling.is_set():
            try:
                rss_samples.append(int(process.memory_info().rss))
            except Exception:
                pass
            stop_sampling.wait(0.01)

    sampler = threading.Thread(target=sample_rss, name="pivot-memory-sampler", daemon=True)
    sampler.start()
    controller = None
    latencies: list[float] = []
    rejected_work = 0
    completed_work = 0
    started_at = time.perf_counter()
    try:
        data = build_sales_table(rows)
        # Preserve the configured DuckDB and worker envelope while reducing
        # query slots for deterministic overload evidence in this run.
        configured_limits = ResourceLimits.from_environment()
        limits = replace(
            configured_limits,
            max_queries=max(1, min(configured_limits.max_queries, concurrent_requests - 1 or 1)),
            tenant_max_queries=1,
        )
        governor = ResourceGovernor(limits)
        controller = ScalablePivotController(
            backend_uri=":memory:",
            enable_streaming=False,
            enable_incremental_views=False,
            tile_size=200,
            cache_ttl=600,
            resource_limits=limits,
            resource_governor=governor,
        )
        controller.load_data_from_arrow("benchmark_sales", data)
        spec = build_pivot_spec()
        viewport_spec = spec.copy()
        viewport_spec.columns = []
        viewport_spec.limit = 200
        viewport_spec.offset = 1_000

        def run_one(index: int) -> tuple[float, bool]:
            operation = index % 5
            started = time.perf_counter()
            account = WorkloadAccount.tenant("benchmark-a" if index % 2 else "benchmark-b")
            account_token = current_workload_account.set(account)
            try:
                # All pressure lanes consume a query slot for this evidence
                # run. The short hold creates a real concurrent admission race.
                with governor.reserve(
                    ResourceKind.QUERY,
                    governor.limits.query_bytes // max(1, governor.limits.max_queries),
                    account=account,
                ):
                    time.sleep(0.005)
                    if operation == 0:  # viewport
                        controller.run_pivot(viewport_spec, return_format="dict")
                    elif operation == 1:  # bounded export
                        list(controller.backend.execute_record_batches(
                            {"sql": "SELECT * FROM benchmark_sales LIMIT 1000"},
                            max_batch_rows=256,
                        ))
                    elif operation == 2:  # payload serialization
                        json.dumps({"rows": data.slice(index % max(1, rows - 32), 32).to_pylist()})
                    elif operation == 3:  # cache pressure
                        controller.run_pivot(spec, return_format="dict", force_refresh=True)
                    else:  # deterministic CDC-cardinality pressure
                        with governor.reserve(ResourceKind.CDC, min(governor.limits.cdc_bytes, 1024)):
                            {f"group-{(index + item) % 256}" for item in range(128)}
                return (time.perf_counter() - started) * 1000, True
            except ResourceExhaustedError:
                return (time.perf_counter() - started) * 1000, False
            finally:
                current_workload_account.reset(account_token)

        total_requests = max(concurrent_requests * 2, 8)
        with ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
            futures = [executor.submit(run_one, index) for index in range(total_requests)]
            for future in as_completed(futures):
                elapsed_ms, admitted = future.result()
                latencies.append(elapsed_ms)
                if admitted:
                    completed_work += 1
                else:
                    rejected_work += 1
    finally:
        if controller is not None:
            controller.close()
        stop_sampling.set()
        sampler.join(timeout=1)

    try:
        rss_samples.append(int(process.memory_info().rss))
    except Exception:
        pass
    snapshot = governor.snapshot() if "governor" in locals() else {}
    rejected_total = sum(
        count
        for reasons in snapshot.get("rejected", {}).values()
        for count in reasons.values()
    )
    admitted_total = sum(snapshot.get("admitted", {}).values())
    peak_rss = max(rss_samples or [0])
    report = {
        "mode": "memory",
        "inputRows": rows,
        "repeatRuns": 0,
        "timingsMs": {
            "workloadP50": round(_percentile(latencies, 0.50), 3),
            "workloadP95": round(_percentile(latencies, 0.95), 3),
        },
        "throughput": {
            "requestsPerSecond": round(completed_work / max(time.perf_counter() - started_at, 0.000001), 3),
            "admittedRequests": completed_work,
            "attemptedRequests": len(latencies),
        },
        "memory": {
            "currentRssBytes": rss_samples[-1] if rss_samples else 0,
            "peakRssBytes": peak_rss,
            "rssCeilingBytes": max_rss_bytes,
            "rssCeilingExceeded": bool(max_rss_bytes is not None and peak_rss > max_rss_bytes),
        },
        "resourceGovernor": {
            "admitted": admitted_total,
            "rejections": rejected_total,
            "tenantRejections": sum(
                count for reasons in snapshot.get("rejected", {}).values()
                for reason, count in reasons.items() if reason.startswith("tenant_")
            ),
            "maxTenantReservedBytes": snapshot.get("high_water_tenant_reserved_bytes", 0),
            "rejectedByKind": {
                kind: sum(reasons.values())
                for kind, reasons in snapshot.get("rejected", {}).items()
                if reasons
            },
        },
        "concurrentRequests": concurrent_requests,
    }
    return report


def _print_report(report: Dict[str, Any]) -> None:
    if report.get("mode") == "memory":
        print(f"Memory workload rows: {report['inputRows']:,}")
        print(f"Peak RSS:            {report['memory']['peakRssBytes']:,} bytes")
        print(f"Workload p95:        {report['timingsMs']['workloadP95']:,.1f} ms")
        print(f"Throughput:          {report['throughput']['requestsPerSecond']:,.1f} requests/s")
        print(f"Admission:           {report['resourceGovernor']['admitted']} admitted / {report['resourceGovernor']['rejections']} rejected")
        return
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
    parser.add_argument("--memory-workload", action="store_true", help="Run the opt-in native-memory pressure workload.")
    parser.add_argument("--concurrent-requests", type=int, default=4, help="Concurrent memory-workload requests (default: 4).")
    parser.add_argument("--max-rss-bytes", type=int, default=None, help="Optional memory-workload RSS ceiling.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON only.")
    args = parser.parse_args()

    if args.memory_workload:
        report = run_memory_benchmark(
            rows=args.rows if args.rows != 100_000 else 500_000,
            concurrent_requests=args.concurrent_requests,
            max_rss_bytes=args.max_rss_bytes,
        )
    else:
        report = run_benchmark(args.rows, args.repeat_runs)
    if args.json:
        print(json.dumps(report, sort_keys=True))
    else:
        _print_report(report)

    failures = []
    if not args.memory_workload and args.max_cold_ms is not None and report["timingsMs"]["coldPivot"] > args.max_cold_ms:
        failures.append(f"cold pivot {report['timingsMs']['coldPivot']} ms exceeds {args.max_cold_ms} ms")
    if not args.memory_workload and args.max_repeat_p95_ms is not None and report["timingsMs"]["repeatPivotP95"] > args.max_repeat_p95_ms:
        failures.append(f"repeat p95 {report['timingsMs']['repeatPivotP95']} ms exceeds {args.max_repeat_p95_ms} ms")
    if args.memory_workload:
        if report["resourceGovernor"]["rejections"] <= 0:
            failures.append("memory workload produced no admission rejection")
        if report["memory"]["rssCeilingExceeded"]:
            failures.append(
                f"peak RSS {report['memory']['peakRssBytes']} exceeds {args.max_rss_bytes}"
            )
    if failures:
        print("Benchmark budget failure: " + "; ".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
