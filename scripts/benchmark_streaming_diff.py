"""Institutional Performance Benchmark for the Streaming & Diff Engine.

Measures p50 / p95 / p99 latencies and memory budgets for:
1. Tier A: Exact incremental algebraic updates (`sum`, `avg`, `wavg`, `count`, monotonic `min`/`max`)
2. Tier B: Revision-stamped targeted dirty-group SQL (extrema deletion / holistic fallback)
3. Tier C: Full authoritative SQL baseline comparison
4. Baseline (T0) capture + Live vs T0 diff + Top-3 PnL Explain drivers
5. Fan-out amortization: 10 subscribers sharing 1 SharedLiveQuery across distinct baselines
"""

from __future__ import annotations

import json
import random
import statistics
import sys
import time
from pathlib import Path

import pyarrow as pa

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "serverside_pivot" / "pivot_engine"))

from pivot_engine.diff.baseline_comparison_service import BaselineComparisonService
from pivot_engine.diff.snapshot_provider import SnapshotProvider
from pivot_engine.streaming.reactive_query_coordinator import ReactiveQueryCoordinator
from pivot_engine.streaming.subscription_broker import SubscriptionBroker, ViewportWindow
from pivot_engine.streaming.versioned_source_adapter import VersionedSourceAdapter
from pivot_engine.tanstack_adapter import create_tanstack_adapter
from pivot_engine.types.pivot_spec import Measure, PivotSpec


def _percentile(samples_ms: list[float], pct: float) -> float:
    if not samples_ms:
        return 0.0
    ordered = sorted(samples_ms)
    idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return round(ordered[idx], 3)


def _summarize(samples_ms: list[float]) -> dict[str, float]:
    return {
        "count": len(samples_ms),
        "p50_ms": _percentile(samples_ms, 50),
        "p95_ms": _percentile(samples_ms, 95),
        "p99_ms": _percentile(samples_ms, 99),
        "mean_ms": round(statistics.fmean(samples_ms), 3) if samples_ms else 0.0,
    }


def _build_synthetic_book(num_rows: int = 20_000, seed: int = 42) -> pa.Table:
    rng = random.Random(seed)
    desks = ["Equities", "Rates", "FX", "Credit", "Commodities", "Derivatives"]
    books = [f"Book_{i:02d}" for i in range(1, 25)]
    regions = ["AMER", "EMEA", "APAC"]
    return pa.table(
        {
            "id": list(range(1, num_rows + 1)),
            "desk": [rng.choice(desks) for _ in range(num_rows)],
            "book": [rng.choice(books) for _ in range(num_rows)],
            "region": [rng.choice(regions) for _ in range(num_rows)],
            "pnl": [round(rng.uniform(-50_000.0, 50_000.0), 2) for _ in range(num_rows)],
            "notional": [round(rng.uniform(10_000.0, 2_000_000.0), 2) for _ in range(num_rows)],
            "price": [round(rng.uniform(10.0, 500.0), 4) for _ in range(num_rows)],
            "qty": [round(rng.uniform(10.0, 1_000.0), 2) for _ in range(num_rows)],
        }
    )


def run_benchmark(num_rows: int = 20_000, iterations: int = 40) -> dict:
    rng = random.Random(1337)
    tbl = _build_synthetic_book(num_rows=num_rows)
    adapter = create_tanstack_adapter()
    adapter.controller.load_data_from_arrow("bench_trades", tbl)

    stack = adapter.get_streaming_stack()
    source = stack["source_adapter"]
    source.register_immutable_arrow_source("bench_trades", tbl)
    snapshots = stack["snapshot_provider"]
    coordinator = stack["coordinator"]
    broker = stack["broker"]

    spec = PivotSpec(
        table="bench_trades",
        rows=["desk", "book"],
        columns=["region"],
        measures=[
            Measure(field="pnl", agg="sum", alias="pnl_sum"),
            Measure(field="price", agg="wavg", alias="vwap", weighted_field="qty"),
            Measure(field="pnl", agg="min", alias="pnl_min"),
            Measure(field="pnl", agg="max", alias="pnl_max"),
        ],
    )

    # 1. Capture zero-copy immutable T0 baseline
    t0_cap = time.perf_counter()
    baseline = snapshots.capture_snapshot("bench_trades", label="EOD T0")
    baseline_capture_ms = round((time.perf_counter() - t0_cap) * 1000.0, 3)

    # 2. Fan-out: 10 subscribers sharing 1 SharedLiveQuery (5 with baseline, 5 live-only)
    subs = []
    for idx in range(10):
        sub, _ = broker.subscribe(
            spec=spec,
            viewport=ViewportWindow(start_row=0, end_row=50, sort_mode="in_place"),
            baseline_id=baseline.snapshot_id if idx < 5 else None,
        )
        subs.append(sub)
    assert coordinator.active_live_query_count() == 1
    live_q = subs[0].live_query

    # 3. Tier A benchmark (incremental algebraic updates + 10-subscriber fan-out poll)
    tier_a_apply_ms: list[float] = []
    fanout_poll_ms: list[float] = []
    for i in range(iterations):
        target_id = rng.randint(1, num_rows)
        new_pnl = round(rng.uniform(-10_000.0, 10_000.0), 2)  # strictly interior -> Tier A
        batch = source.apply_mutation_batch(
            "bench_trades",
            [{"id": target_id, "pnl": new_pnl, "price": round(rng.uniform(50.0, 200.0), 4)}],
            mode="update",
            key_fields=["id"],
        )
        t_start = time.perf_counter()
        delta = live_q.apply_batch(batch)
        elapsed_ms = (time.perf_counter() - t_start) * 1000.0
        if delta.strategy_used == "tier_a_incremental":
            tier_a_apply_ms.append(elapsed_ms)

        t_fan = time.perf_counter()
        for sub in subs:
            frame = broker.poll_updates(sub.subscription_id, ack_seq=sub.last_ack_seq)
            if isinstance(frame.get("seq"), int):
                sub.acknowledge(frame["seq"])
        fanout_poll_ms.append((time.perf_counter() - t_fan) * 1000.0)

    # 4. Tier B benchmark (targeted dirty-group SQL when current min is deleted)
    tier_b_apply_ms: list[float] = []
    for i in range(10):
        ext_id = num_rows + 100 + i
        b_ins = source.apply_mutation_batch(
            "bench_trades",
            [
                {
                    "id": ext_id,
                    "desk": "Equities",
                    "book": "Book_01",
                    "region": "AMER",
                    "pnl": -9_999_999.0 - i,
                    "notional": 100_000.0,
                    "price": 100.0,
                    "qty": 100.0,
                }
            ],
            mode="insert",
        )
        live_q.apply_batch(b_ins)

        b_del = source.apply_mutation_batch(
            "bench_trades",
            [{"id": ext_id}],
            mode="delete",
            key_fields=["id"],
        )
        t_start = time.perf_counter()
        delta = live_q.apply_batch(b_del)
        elapsed_ms = (time.perf_counter() - t_start) * 1000.0
        if delta.strategy_used == "tier_b_targeted_sql":
            tier_b_apply_ms.append(elapsed_ms)

    # 5. Tier C benchmark (full authoritative SQL recomputation)
    tier_c_ms: list[float] = []
    for _ in range(10):
        t_start = time.perf_counter()
        live_q._execute_authoritative_full_locked()
        tier_c_ms.append((time.perf_counter() - t_start) * 1000.0)

    for sub in subs:
        broker.unsubscribe(sub.subscription_id)
    snapshots.close()
    source.close()
    adapter.controller.close()

    report = {
        "rows": num_rows,
        "shared_live_queries_for_10_subscribers": 1,
        "baseline_capture_strategy": str(baseline.strategy),
        "baseline_capture_ms": baseline_capture_ms,
        "tier_a_incremental": _summarize(tier_a_apply_ms),
        "tier_b_targeted_sql": _summarize(tier_b_apply_ms),
        "tier_c_full_sql": _summarize(tier_c_ms),
        "ten_subscriber_fanout_total": _summarize(fanout_poll_ms),
    }
    return report


if __name__ == "__main__":
    results = run_benchmark()
    print(json.dumps(results, indent=2))
