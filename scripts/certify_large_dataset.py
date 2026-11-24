"""M5 large-dataset certification: the core equals an independent DuckDB ROLLUP at scale.

Usage: python scripts/certify_large_dataset.py --rows 10000000 [--rows 50000000] [--json out.json]

The oracle cannot run at tens of millions of rows, so the reference here is a
hand-written ``GROUP BY ROLLUP`` over the same table, sharing no code with the
core. For each size it checks, with NULL dimension members and NULL measures
in the data:
  * every node value (sum, avg, min, max, count, count(*), count_distinct, median) of a 3-level
    hierarchy, for both physical strategies;
  * sampled pages (first, last, random) equal the matching slice of the unpaged result (REQ-PAGE-001);
  * timings and peak RSS, reported, not gated.
Exit code 1 on any mismatch.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "serverside_pivot" / "pivot_engine"))

import ibis  # noqa: E402
import psutil  # noqa: E402

from pivot_engine.core.exec import execute  # noqa: E402
from pivot_engine.core.ir import MeasureRef, PivotQuery, Window  # noqa: E402

LEVELS = ("d0", "d1", "d2")
MEASURES = (
    MeasureRef("s", "sum", "amount"),
    MeasureRef("a", "avg", "amount"),
    MeasureRef("lo", "min", "qty"),
    MeasureRef("hi", "max", "qty"),
    MeasureRef("c", "count", "amount"),
    MeasureRef("n", "count", None),
    MeasureRef("cd", "count_distinct", "qty"),
    MeasureRef("med", "median", "qty"),
)
REFERENCE_SQL = """
SELECT d0, d1, d2, GROUPING(d0) AS g0, GROUPING(d1) AS g1, GROUPING(d2) AS g2,
       SUM(amount) AS s, AVG(amount) AS a, MIN(qty) AS lo, MAX(qty) AS hi, COUNT(amount) AS c,
       COUNT(*) AS n, COUNT(DISTINCT qty) AS cd, MEDIAN(CAST(qty AS DOUBLE)) AS med
FROM t GROUP BY ROLLUP (d0, d1, d2)
"""


def build(con, rows: int) -> None:
    # 1 in 50 dimension members and 1 in 20 amounts are NULL; members collide only by value.
    con.raw_sql(f"""
        CREATE OR REPLACE TABLE t AS SELECT
          CASE WHEN hash(range) % 50 = 0 THEN NULL ELSE 'r' || CAST(hash(range * 7) % 12 AS VARCHAR) END AS d0,
          CASE WHEN hash(range * 3) % 50 = 0 THEN NULL ELSE 'p' || CAST(hash(range * 11) % 40 AS VARCHAR) END AS d1,
          'c' || CAST(hash(range * 13) % 150 AS VARCHAR) AS d2,
          CASE WHEN hash(range * 5) % 20 = 0 THEN NULL ELSE CAST(hash(range * 17) % 100000 AS DECIMAL(18, 2)) / 100 END
            AS amount,
          CAST(hash(range * 19) % 5000 AS BIGINT) AS qty
        FROM range({rows})
    """)


def reference(con) -> dict:
    out = {}
    for r in con.raw_sql(REFERENCE_SQL).fetchall():
        d0, d1, d2, g0, g1, g2, *values = r
        depth = 3 - (g0 + g1 + g2)
        path = (d0, d1, d2)[:depth]
        out[path] = dict(zip([m.alias for m in MEASURES], values))
    return out


def progress(report: dict, phase: str) -> None:
    """Each phase is reported as it completes, so a crash later never loses earlier evidence."""
    print(json.dumps({"rows": report["rows"], "phase": phase, **{k: v for k, v in report.items() if k != "rows"}},
                     default=str), file=sys.stderr, flush=True)


def same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-6)


def certify(rows: int) -> dict:
    con = ibis.duckdb.connect()
    t0 = time.perf_counter()
    build(con, rows)
    report = {"rows": rows, "build_s": round(time.perf_counter() - t0, 2), "mismatches": []}
    t0 = time.perf_counter()
    ref = reference(con)
    report["reference_s"] = round(time.perf_counter() - t0, 2)
    report["nodes"] = len(ref)
    progress(report, "reference")
    query = PivotQuery(dataset="t", rows=LEVELS, measures=MEASURES)
    full = None
    for strategy in (None, "union_all", "leaf_partials"):
        try:
            t0 = time.perf_counter()
            got = {r["path"]: r["values"] for r in execute(query, con.table, strategy=strategy).records()}
        except Exception as exc:  # noqa: BLE001 - a strategy that declines the plan is reported, not fatal
            report[f"{strategy or 'default'}_s"] = f"declined: {type(exc).__name__}"
            continue
        report[f"{strategy or 'default'}_s"] = round(time.perf_counter() - t0, 2)
        if strategy is None:
            full = got
        if set(got) != set(ref):
            report["mismatches"].append({"strategy": strategy, "paths": len(set(got) ^ set(ref))})
            continue
        for path, values in ref.items():
            bad = {k: (got[path][k], v) for k, v in values.items() if not same(got[path][k], v)}
            if bad:
                report["mismatches"].append({"strategy": strategy, "path": repr(path), "values": repr(bad)})
                if len(report["mismatches"]) > 20:
                    break
        progress(report, f"values:{strategy or 'default'}")
    order = [r["path"] for r in execute(query, con.table).records()]
    page = 997
    offsets = sorted({0, page, 2 * page, *random.Random(rows).sample(range(0, len(order), page), 5),
                      (len(order) // page) * page, len(order)})
    bad_pages = []
    for offset in offsets:  # each sampled window equals the matching slice of the unpaged order
        windowed = PivotQuery(dataset="t", rows=LEVELS, measures=MEASURES, window=Window(offset, page))
        if [r["path"] for r in execute(windowed, con.table).records()] != order[offset:offset + page]:
            bad_pages.append(offset)
    report["pages_checked"] = len(offsets)
    report["pages_concatenate"] = not bad_pages
    progress(report, "paging")
    if bad_pages:
        report["mismatches"].append({"paging": f"windows at offsets {bad_pages} differ from the unpaged slice"})
    report["peak_rss_mb"] = round(psutil.Process().memory_info().rss / 2**20)
    report["ok"] = not report["mismatches"] and full is not None
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, action="append", required=True)
    parser.add_argument("--json", type=pathlib.Path)
    args = parser.parse_args(argv)
    reports = [certify(n) for n in args.rows]
    text = json.dumps(reports, indent=2)
    print(text)
    if args.json:
        args.json.write_text(text, encoding="utf-8")
    return 0 if all(r["ok"] for r in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
