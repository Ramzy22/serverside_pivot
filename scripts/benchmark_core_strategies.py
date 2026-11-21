"""Benchmark the core's physical strategies on DuckDB (M1 gate input).

Usage: python scripts/benchmark_core_strategies.py --rows 10000000 [--rows 100000000] [--repeat 3]

For each depth (2, 4, 6 row dimensions) and expansion (all expanded; top level
only) it times a 200-row viewport per strategy and prints a JSON summary, so
``core.physical.choose`` defaults are set from measurements, not guesses.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "serverside_pivot" / "pivot_engine"))

import ibis  # noqa: E402

from pivot_engine.core.exec import execute  # noqa: E402
from pivot_engine.core.ir import MeasureRef, PivotQuery, Window  # noqa: E402

CARD = [5, 12, 30, 80, 200, 1000]
MEASURES = (
    MeasureRef("amount_sum", "sum", "amount"),
    MeasureRef("amount_avg", "avg", "amount"),
    MeasureRef("qty_max", "max", "qty"),
    MeasureRef("n", "count", None),
)


def build(con, rows: int) -> None:
    dims = ", ".join(f"'d{i}_' || CAST(hash(range * {i + 7}) % {c} AS VARCHAR) AS d{i}" for i, c in enumerate(CARD))
    con.raw_sql(
        f"CREATE OR REPLACE TABLE t AS SELECT {dims}, "
        "CAST(hash(range) % 100000 AS DOUBLE) / 100 AS amount, CAST(hash(range * 3) % 50 AS BIGINT) AS qty "
        f"FROM range({rows})"
    )


def time_query(con, query, strategy, repeat):
    samples = []
    for _ in range(repeat):
        start = time.perf_counter()
        frame = execute(query, con.table, strategy=strategy)
        samples.append(time.perf_counter() - start)
    return statistics.median(samples), frame.table.num_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, action="append")
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    results = []
    for rows in args.rows or [10_000_000]:
        con = ibis.duckdb.connect()
        build(con, rows)
        for depth in (2, 4, 6):
            dims = tuple(f"d{i}" for i in range(depth))
            top = con.table("t").select("d0").distinct().to_pyarrow().column(0).to_pylist()
            for expansion, expanded in (("all", None), ("top", frozenset((v,) for v in top))):
                q = PivotQuery(dataset="t", rows=dims, measures=MEASURES, expanded=expanded, window=Window(0, 200))
                entry = {"rows": rows, "depth": depth, "expansion": expansion}
                for strategy in ("union_all", "leaf_partials"):
                    seconds, n = time_query(con, q, strategy, args.repeat)
                    entry[strategy] = round(seconds, 3)
                    entry["returned"] = n
                entry["speedup"] = round(entry["union_all"] / max(entry["leaf_partials"], 1e-9), 2)
                print(json.dumps(entry), flush=True)
                results.append(entry)
    out = ROOT / "artifacts" / "benchmarks" / "core_strategies.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
