#!/usr/bin/env python3
"""
Benchmark: DashTanstackPivot vs AG Grid Server-Side Row Model

AG Grid's Enterprise SSRM sends { startRow, endRow, rowGroupCols, valueCols,
pivotCols, pivotMode, groupKeys, filterModel, sortModel } to a server datasource.
We simulate that datasource with pandas — the canonical approach used in AG Grid
Python integrations — then run the identical logical operation through our
DuckDB-backed engine and compare latencies.

Run from the repo root:
    python benchmark_vs_aggrid.py
    python benchmark_vs_aggrid.py --rows 1000000   # bump scale
    python benchmark_vs_aggrid.py --runs 10        # more measurement runs
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import os
import sys
import time
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# ── path shim (mirrors conftest.py) ───────────────────────────────────────────
sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "dash_tanstack_pivot", "pivot_engine"),
)

from pivot_engine import (
    PivotRequestContext,
    PivotRuntimeService,
    PivotViewState,
    SessionRequestGate,
    create_tanstack_adapter,
)


# ── Dataset factory ────────────────────────────────────────────────────────────

def make_dataset(n: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    regions  = ["North", "South", "East", "West", "Central"]
    segments = ["Enterprise", "SMB", "Consumer", "Government", "Education"]
    channels = ["Direct", "Online", "Reseller", "Partner", "Retail"]
    products = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta"]
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    years    = ["2021", "2022", "2023", "2024"]
    reps     = [f"Rep_{i:03d}" for i in range(1, 51)]
    return pd.DataFrame({
        "region":   rng.choice(regions,   n),
        "segment":  rng.choice(segments,  n),
        "channel":  rng.choice(channels,  n),
        "product":  rng.choice(products,  n),
        "rep":      rng.choice(reps,      n),
        "quarter":  rng.choice(quarters,  n),
        "year":     rng.choice(years,     n),
        "sales":    rng.integers(1_000,   50_000,  n).astype(float),
        "revenue":  rng.integers(5_000,  200_000,  n).astype(float),
        "cost":     rng.integers(500,    30_000,   n).astype(float),
        "profit":   rng.integers(100,    50_000,   n).astype(float),
        "quantity": rng.integers(1,      200,      n).astype(float),
    })


# ── AG Grid pandas backend ─────────────────────────────────────────────────────

class AGGridPandasBackend:
    """
    Simulates an AG Grid SSRM server datasource implemented in pandas.

    This is the typical setup for a Python AG Grid backend:
      POST /getRows  → { rowData, rowCount }

    No server-side caching is applied — AG Grid manages row-block caching
    on the client and the server is expected to be stateless.
    """

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def get_rows(
        self,
        *,
        start_row: int = 0,
        end_row: int = 100,
        row_group_cols: List[str] = (),
        value_cols: List[Dict] = (),
        pivot_cols: List[str] = (),
        pivot_mode: bool = False,
        group_keys: List[str] = (),
        filter_model: Optional[Dict[str, Any]] = None,
        sort_model: List[Dict] = (),
    ) -> Dict[str, Any]:
        df = self._df

        # 1. Filter
        if filter_model:
            df = df.copy()
            for col, flt in filter_model.items():
                if col not in df.columns:
                    continue
                ftype = flt.get("filterType", "text")
                op    = flt.get("type", "equals")
                val   = flt.get("filter")
                if ftype == "number":
                    if op == "equals":               df = df[df[col] == val]
                    elif op == "greaterThan":         df = df[df[col] > val]
                    elif op == "lessThan":            df = df[df[col] < val]
                    elif op == "greaterThanOrEqual":  df = df[df[col] >= val]
                    elif op == "lessThanOrEqual":     df = df[df[col] <= val]
                elif ftype == "text":
                    sv = str(val) if val is not None else ""
                    if op == "equals":    df = df[df[col].astype(str) == sv]
                    elif op == "contains": df = df[df[col].astype(str).str.contains(sv, na=False)]

        # 2. Drill into group path
        group_cols = list(row_group_cols)
        for i, key in enumerate(group_keys):
            if i < len(group_cols):
                df = df[df[group_cols[i]].astype(str) == str(key)]
        remaining = group_cols[len(group_keys):]

        # 3. Group + aggregate
        _AGG = {"sum": "sum", "avg": "mean", "count": "count", "min": "min", "max": "max"}
        if remaining:
            agg_spec: Dict[str, str] = {}
            for vc in value_cols:
                fn = _AGG.get(vc.get("aggFunc", "sum"), "sum")
                if vc["field"] in df.columns:
                    agg_spec[vc["field"]] = fn
            if agg_spec:
                df = df.groupby(remaining, as_index=False, sort=False).agg(agg_spec)
            else:
                df = df[remaining].drop_duplicates()

        # 4. Pivot (cross-tabulate on pivot_cols if pivot_mode)
        if pivot_mode and pivot_cols and remaining and value_cols:
            vf = value_cols[0]["field"]
            if vf in df.columns and pivot_cols[0] in df.columns:
                try:
                    df = df.pivot_table(
                        index=remaining,
                        columns=pivot_cols[0],
                        values=vf,
                        aggfunc="sum",
                        fill_value=0,
                    ).reset_index()
                    df.columns = [
                        str(c[1]) if isinstance(c, tuple) and c[0] == vf else str(c)
                        for c in df.columns
                    ]
                except Exception:
                    pass

        # 5. Sort
        if sort_model:
            sc = [s["colId"] for s in sort_model if s.get("colId") in df.columns]
            asc = [s.get("sort", "asc") == "asc" for s in sort_model if s.get("colId") in df.columns]
            if sc:
                df = df.sort_values(sc, ascending=asc)

        total = len(df)
        page  = df.iloc[start_row:end_row]
        return {"rowData": page.to_dict("records"), "rowCount": total}


# ── AG Grid pandas backend + LRU cache ────────────────────────────────────────

import hashlib
import json
from collections import OrderedDict


class AGGridCachedBackend:
    """
    AG Grid SSRM datasource with a server-side LRU cache layered on top.

    This represents a production-hardened AG Grid Python backend where the
    team has added an in-memory cache (or Redis) to avoid re-running pandas
    groupby on repeated getRows calls.  Cache key = hash of the full request
    parameters.  Cache is NOT invalidated between scenarios so it captures
    the best-case "warm server cache" scenario.

    Max cache size: 256 entries (configurable).
    """

    def __init__(self, df: pd.DataFrame, max_size: int = 256):
        self._backend = AGGridPandasBackend(df)
        self._cache: OrderedDict = OrderedDict()
        self._max_size = max_size
        self.hits = 0
        self.misses = 0

    def _cache_key(self, **kwargs) -> str:
        # Serialize all request params to a stable string
        payload = json.dumps(kwargs, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    def get_rows(self, **kwargs) -> Dict[str, Any]:
        key = self._cache_key(**kwargs)
        if key in self._cache:
            self._cache.move_to_end(key)
            self.hits += 1
            return self._cache[key]

        result = self._backend.get_rows(**kwargs)
        self._cache[key] = result
        if len(self._cache) > self._max_size:
            self._cache.popitem(last=False)
        self.misses += 1
        return result


# ── Our engine wrapper ─────────────────────────────────────────────────────────

def _make_service(df: pd.DataFrame):
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.load_data(df, "sales")
    service = PivotRuntimeService(
        adapter_getter=lambda: adapter,
        session_gate=SessionRequestGate(),
    )
    return service, adapter


def _viewport_meta(
    *,
    start: int = 0,
    end: int = 99,
    intent: str = "viewport",
    seq: int = 1,
    epoch: int = 1,
    needs_col_schema: bool = True,
) -> Dict[str, Any]:
    return {
        "start": start,
        "end": end,
        "intent": intent,
        "window_seq": seq,
        "state_epoch": epoch,
        "abort_generation": 0,
        "session_id": "bench",
        "client_instance": "bench",
        "needs_col_schema": needs_col_schema,
        "include_grand_total": False,
    }


async def _our_request(
    service: PivotRuntimeService,
    *,
    table: str = "sales",
    row_fields: List[str],
    col_fields: List[str] = (),
    val_configs: List[Dict[str, Any]],
    filters: Dict[str, Any] = None,
    sorting: List[Dict[str, Any]] = (),
    start: int = 0,
    end: int = 99,
    epoch: int = 1,
    seq: int = 1,
) -> Any:
    state = PivotViewState(
        row_fields=list(row_fields),
        col_fields=list(col_fields),
        val_configs=list(val_configs),
        filters=filters or {},
        sorting=list(sorting),
    )
    context = PivotRequestContext.from_frontend(
        table=table,
        trigger_prop="comp.viewport",
        viewport=_viewport_meta(start=start, end=end, seq=seq, epoch=epoch),
    )
    return await service.process_async(state, context)


# ── Timing helpers ─────────────────────────────────────────────────────────────

@dataclass
class Stats:
    times_ms: List[float] = field(default_factory=list)

    @property
    def mean(self): return statistics.mean(self.times_ms)
    @property
    def median(self): return statistics.median(self.times_ms)
    @property
    def p95(self):
        s = sorted(self.times_ms)
        return s[max(0, int(len(s) * 0.95) - 1)]
    @property
    def minimum(self): return min(self.times_ms)


def _time_sync(fn, runs: int, warmup: int = 1):
    for _ in range(warmup):
        fn()
    gc.collect()
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return Stats(times_ms=times)


def _time_async(coro_fn, runs: int, warmup: int = 1):
    """coro_fn(run_index) must return a coroutine."""
    async def _run():
        for i in range(warmup):
            await coro_fn(-(i + 1))
        gc.collect()
        times = []
        for i in range(runs):
            t0 = time.perf_counter()
            await coro_fn(i)
            times.append((time.perf_counter() - t0) * 1000)
        return Stats(times_ms=times)
    return asyncio.run(_run())


# ── Scenarios ──────────────────────────────────────────────────────────────────

_VALS = [
    {"field": "sales",   "agg": "sum",   "alias": "sum_sales"},
    {"field": "revenue", "agg": "sum",   "alias": "sum_revenue"},
    {"field": "profit",  "agg": "avg",   "alias": "avg_profit"},
]

_AG_VALS = [
    {"field": "sales",   "aggFunc": "sum"},
    {"field": "revenue", "aggFunc": "sum"},
    {"field": "profit",  "aggFunc": "avg"},
]


def _scenarios(
    ag: AGGridPandasBackend,
    ag_cached: AGGridCachedBackend,
    service: PivotRuntimeService,
    n_rows: int,
    runs: int,
):
    """
    Each scenario uses an independent epoch so requests are not short-circuited
    as stale by the session gate. Each run within a scenario uses a unique seq
    (window_seq) so the request coordinator doesn't cancel repeated calls.

    Returns list of (name, stats_ag_nocache, stats_ag_cached, stats_ours).
    """
    results = []
    # Global seq counter — incremented across ALL runs so every request is distinct.
    _SEQ = [1000]

    def _seq():
        _SEQ[0] += 1
        return _SEQ[0]

    def _run_scenario(
        name: str,
        ag_kwargs: Dict[str, Any],
        cached_kwargs: Dict[str, Any],
        our_coro_fn,
    ):
        s_ag     = _time_sync(lambda: ag.get_rows(**ag_kwargs), runs)
        s_cached = _time_sync(lambda: ag_cached.get_rows(**cached_kwargs), runs)
        s_our    = _time_async(our_coro_fn, runs)
        results.append((name, s_ag, s_cached, s_our))

    # -- cold load: first time this pivot spec is seen ─────────────────────────
    _epoch_cold = [10]
    async def _our_cold(i):
        _epoch_cold[0] += 1
        return await _our_request(
            service, row_fields=["region", "segment"], col_fields=["year"],
            val_configs=_VALS, start=0, end=99, epoch=_epoch_cold[0], seq=_seq(),
        )
    _run_scenario(
        "Initial pivot (cold)",
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True),
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True),
        _our_cold,
    )

    # -- warm cache ────────────────────────────────────────────────────────────
    _epoch_warm = _epoch_cold[0]
    async def _our_warm(i):
        return await _our_request(
            service, row_fields=["region", "segment"], col_fields=["year"],
            val_configs=_VALS, start=0, end=99, epoch=_epoch_warm, seq=_seq(),
        )
    _run_scenario(
        "Warm cache (same request)",
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True),
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True),
        _our_warm,
    )

    # -- page scroll ───────────────────────────────────────────────────────────
    _epoch_scroll = [100]
    async def _our_scroll(i):
        _epoch_scroll[0] += 1
        return await _our_request(
            service, row_fields=["region", "segment"], col_fields=["year"],
            val_configs=_VALS, start=100, end=199, epoch=_epoch_scroll[0], seq=_seq(),
        )
    _run_scenario(
        "Page scroll (rows 100-199)",
        dict(start_row=100, end_row=200, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True),
        dict(start_row=100, end_row=200, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True),
        _our_scroll,
    )

    # -- sort change ───────────────────────────────────────────────────────────
    _epoch_sort = [200]
    async def _our_sort(i):
        _epoch_sort[0] += 1
        return await _our_request(
            service, row_fields=["region", "segment"], col_fields=["year"],
            val_configs=_VALS, sorting=[{"id": "sum_revenue", "desc": True}],
            start=0, end=99, epoch=_epoch_sort[0], seq=_seq(),
        )
    _run_scenario(
        "Sort by revenue desc",
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True,
             sort_model=[{"colId": "revenue", "sort": "desc"}]),
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True,
             sort_model=[{"colId": "revenue", "sort": "desc"}]),
        _our_sort,
    )

    # -- filter apply ──────────────────────────────────────────────────────────
    _epoch_filter = [300]
    async def _our_filter(i):
        _epoch_filter[0] += 1
        return await _our_request(
            service, row_fields=["region", "segment"], col_fields=["year"],
            val_configs=_VALS, filters={"region": {"type": "eq", "value": "North"}},
            start=0, end=99, epoch=_epoch_filter[0], seq=_seq(),
        )
    _run_scenario(
        "Filter: region = North",
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True,
             filter_model={"region": {"filterType": "text", "type": "equals", "filter": "North"}}),
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True,
             filter_model={"region": {"filterType": "text", "type": "equals", "filter": "North"}}),
        _our_filter,
    )

    # -- hierarchy drill ───────────────────────────────────────────────────────
    _epoch_drill = [400]
    async def _our_drill(i):
        _epoch_drill[0] += 1
        return await _our_request(
            service, row_fields=["segment"], col_fields=["year"],
            val_configs=_VALS, filters={"region": {"type": "eq", "value": "North"}},
            start=0, end=99, epoch=_epoch_drill[0], seq=_seq(),
        )
    _run_scenario(
        "Hierarchy expand: North",
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True,
             group_keys=["North"]),
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment"],
             value_cols=_AG_VALS, pivot_cols=["year"], pivot_mode=True,
             group_keys=["North"]),
        _our_drill,
    )

    # -- flat aggregation ──────────────────────────────────────────────────────
    _epoch_flat = [500]
    async def _our_flat(i):
        _epoch_flat[0] += 1
        return await _our_request(
            service, row_fields=["region", "segment", "channel"], col_fields=[],
            val_configs=_VALS, start=0, end=99, epoch=_epoch_flat[0], seq=_seq(),
        )
    _run_scenario(
        "Flat groupby (3 dims, no pivot)",
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment", "channel"],
             value_cols=_AG_VALS),
        dict(start_row=0, end_row=100, row_group_cols=["region", "segment", "channel"],
             value_cols=_AG_VALS),
        _our_flat,
    )

    # -- large page ────────────────────────────────────────────────────────────
    _epoch_page = [600]
    async def _our_page(i):
        _epoch_page[0] += 1
        return await _our_request(
            service, row_fields=["region"], col_fields=[],
            val_configs=_VALS, start=0, end=499, epoch=_epoch_page[0], seq=_seq(),
        )
    _run_scenario(
        "Large page (500 rows, 1 dim)",
        dict(start_row=0, end_row=500, row_group_cols=["region"], value_cols=_AG_VALS),
        dict(start_row=0, end_row=500, row_group_cols=["region"], value_cols=_AG_VALS),
        _our_page,
    )

    return results


# ── Output ─────────────────────────────────────────────────────────────────────

def _bar(value: float, max_value: float, width: int = 20) -> str:
    filled = int(round(value / max_value * width)) if max_value else 0
    filled = max(1, min(filled, width))
    return "#" * filled + "." * (width - filled)


def _print_table(scenario_results, n_rows: int, runs: int):
    W = 110
    print(f"\n{'='*W}")
    print(f"  Benchmark: DashTanstackPivot vs AG Grid SSRM")
    print(f"  Dataset: {n_rows:,} rows  |  Runs: {runs}  |  All times in ms (median / p95)")
    print(f"{'='*W}")
    print(
        f"  {'Scenario':<32} "
        f"{'AG Grid (no cache)':<22} "
        f"{'AG Grid (+ LRU cache)':<22} "
        f"{'DashTanstackPivot':<22} "
        f"{'vs no-cache':>10}  {'vs cached':>9}"
    )
    print(f"  {'-'*32} {'-'*22} {'-'*22} {'-'*22} {'-'*10}  {'-'*9}")

    for name, s_ag, s_cached, s_our in scenario_results:
        vs_ag     = s_ag.median     / s_our.median if s_our.median > 0 else 9999
        vs_cached = s_cached.median / s_our.median if s_our.median > 0 else 9999
        w_ag      = ">>>" if s_our.median < s_ag.median     else "<<<"
        w_ca      = ">>>" if s_our.median < s_cached.median else "<<<"
        print(
            f"  {name:<32} "
            f"{s_ag.median:>7.1f} / {s_ag.p95:>7.1f}   "
            f"{s_cached.median:>7.1f} / {s_cached.p95:>7.1f}   "
            f"{s_our.median:>7.1f} / {s_our.p95:>7.1f}   "
            f"{w_ag} {vs_ag:>5.1f}x   "
            f"{w_ca} {vs_cached:>4.1f}x"
        )

    print(f"\n{'='*W}")
    print("  >>> = DashTanstackPivot faster   <<< = comparison backend faster")
    print(f"{'='*W}\n")

    # Bar chart
    print("  Median latency (shorter = faster):\n")
    all_m = (
        [s.median for _, s, _, _ in scenario_results]
        + [s.median for _, _, s, _ in scenario_results]
        + [s.median for _, _, _, s in scenario_results]
    )
    chart_max = max(all_m) if all_m else 1
    for name, s_ag, s_cached, s_our in scenario_results:
        label = name[:28].ljust(28)
        print(f"    {label}  no-cache [{_bar(s_ag.median,     chart_max, 24)}]  {s_ag.median:6.0f}ms")
        print(f"    {'':28}  + cache  [{_bar(s_cached.median, chart_max, 24)}]  {s_cached.median:6.0f}ms")
        print(f"    {'':28}  ours     [{_bar(s_our.median,    chart_max, 24)}]  {s_our.median:6.0f}ms")
        print()

    # Totals
    t_ag     = sum(s.median for _, s, _, _ in scenario_results)
    t_cached = sum(s.median for _, _, s, _ in scenario_results)
    t_our    = sum(s.median for _, _, _, s in scenario_results)
    print(
        f"  Cumulative: AG no-cache={t_ag:.0f}ms  "
        f"AG+cache={t_cached:.0f}ms  "
        f"DashTanstackPivot={t_our:.0f}ms  "
        f"(speedup vs no-cache: {t_ag/t_our:.0f}x  vs cached: {t_cached/t_our:.0f}x)"
    )
    print()


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Benchmark DashTanstackPivot vs AG Grid SSRM")
    parser.add_argument("--rows",  type=int, default=100_000, help="Dataset size (default: 100000)")
    parser.add_argument("--runs",  type=int, default=5,       help="Measurement runs per scenario (default: 5)")
    parser.add_argument("--sizes", type=str, default="",      help="Comma-separated row counts to sweep, e.g. 10000,100000,500000")
    args = parser.parse_args()

    sizes = [int(s) for s in args.sizes.split(",") if s.strip()] if args.sizes else [args.rows]

    for n in sizes:
        print(f"\n[+] Generating {n:,}-row dataset ...", end=" ", flush=True)
        df = make_dataset(n)
        print("done")

        print("[+] Building AG Grid backends (no-cache + LRU cache) ...", end=" ", flush=True)
        ag        = AGGridPandasBackend(df)
        ag_cached = AGGridCachedBackend(df)
        print("done")

        print("[+] Building DashTanstackPivot engine ...", end=" ", flush=True)
        service, _ = _make_service(df)
        print("done")

        print(f"[+] Running 8 scenarios x {args.runs} runs (3 backends each) ...\n")
        results = _scenarios(ag, ag_cached, service, n, args.runs)
        _print_table(results, n, args.runs)
        print(f"  AG Grid LRU cache: {ag_cached.hits} hits / {ag_cached.misses} misses across all runs\n")


if __name__ == "__main__":
    main()
