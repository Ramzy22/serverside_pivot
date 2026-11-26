#!/usr/bin/env python3
"""Paired server-side benchmark: native AG Grid SSRM datasource vs the pivot runtime (M6 gate).

AG Grid side: ``scripts/performance/million_queries.query_ag_grid``, the native AG Grid
Enterprise SSRM DuckDB datasource of the million-row browser comparison, plus its
grand-total request, issued exactly as that comparison's server issues them.

Product side: ``PivotRuntimeService`` with the comparison's workspace (region > account >
product, period pivot, sum of ``sales_cents`` and ``units``, row and column totals).

Both read the same deterministic Arrow facts (``million_dataset.build_dataset``) in the
same process. Every timed scenario is checked cell by cell: each AG Grid page row, and the
grand total, must equal the product row with the same path. A mismatch fails the run.

Columns: ``ag``/``product`` uncached (no cache on either side), ``first`` the product's
first-ever request of that shape (compile cache cleared too), ``hit`` an identical repeat.

    python scripts/benchmark_core_vs_aggrid.py --sizes 200000,1000000 --runs 5
    python scripts/benchmark_core_vs_aggrid.py --sizes 10000000 --json-out m6.json --max-ratio 1.0

10M rows: both engines and the Arrow source share one process; raise the worker memory
budget, e.g. ``PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES=13000000000``.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
for entry in (ROOT, ROOT / "serverside_pivot" / "pivot_engine"):
    if str(entry) not in sys.path:
        sys.path.insert(0, str(entry))

import duckdb  # noqa: E402

from pivot_engine import (  # noqa: E402
    PivotRequestContext,
    PivotRuntimeService,
    PivotViewState,
    SessionRequestGate,
    create_tanstack_adapter,
)
from pivot_engine.core import exec as core_exec  # noqa: E402
from scripts.performance.million_dataset import GROUP_FIELDS, MEASURES, PIVOT_SEPARATOR, build_dataset  # noqa: E402
from scripts.performance.million_queries import query_ag_grid  # noqa: E402

TABLE = "benchmark_facts"
COLUMN_WINDOW = 48  # columns per verification read (the client reads a column window too)
SHAPES = ("hierarchy", "wide")


# --------------------------------------------------------------------------- AG Grid


class AGGridServer:
    """The native SSRM datasource on its own DuckDB connection over the same facts."""

    def __init__(self, facts: Any, shape: str) -> None:
        self.shape = shape
        self.con = duckdb.connect()
        self.con.register("facts_source", facts)
        self.con.execute(f"CREATE TABLE {TABLE} AS SELECT * FROM facts_source")
        self.con.unregister("facts_source")
        self._window_cache: Dict[str, Dict[str, Any]] = {}

    def get_rows(self, request: Dict[str, Any], *, cached: bool = False) -> Dict[str, Any]:
        key = json.dumps(request, sort_keys=True)
        if cached and key in self._window_cache:
            return self._window_cache[key]
        page = query_ag_grid(self.con, request, shape=self.shape)
        totals = request | {"rowGroupCols": [], "groupKeys": [], "startRow": 0, "endRow": 1, "sortModel": []}
        result = page | {"grandTotalData": query_ag_grid(self.con, totals, shape=self.shape)["rowData"][0]}
        self._window_cache[key] = result
        return result

    def close(self) -> None:
        self.con.close()


def ag_request(*, start: int = 0, end: int = 200, keys: Tuple[str, ...] = (), region: Optional[str] = None,
               sort: Optional[str] = None) -> Dict[str, Any]:
    return {
        "startRow": start, "endRow": end,
        "rowGroupCols": [{"id": f, "field": f} for f in GROUP_FIELDS], "groupKeys": list(keys),
        "pivotMode": True, "pivotCols": [{"id": "period", "field": "period"}],
        "valueCols": [{"id": m, "field": m, "aggFunc": "sum"} for m in MEASURES],
        "filterModel": {"region": {"filterType": "text", "type": "equals", "filter": region}} if region else {},
        "sortModel": [{"colId": sort, "sort": "desc"}] if sort else [],
    }


# --------------------------------------------------------------------------- product


class ProductServer:
    def __init__(self, facts: Any) -> None:
        self.adapter = create_tanstack_adapter(backend_uri=":memory:")
        self.adapter.load_data(facts, TABLE)
        self.service = PivotRuntimeService(adapter_getter=lambda: self.adapter, session_gate=SessionRequestGate())
        self._seq = 0
        self._epoch = 0
        self._loop = asyncio.new_event_loop()

    def clear(self, *, compile_cache: bool = False) -> None:
        """Drop every result/viewport cache (and optionally the plan/compile cache)."""
        self.adapter.controller.clear_cache()
        self.adapter._viewport_cache.invalidate_all()
        if compile_cache:
            core_exec.clear_compile_cache()

    def get_rows(self, *, start: int = 0, end: int = 199, expanded: Optional[Dict[str, bool]] = None,
                 region: Optional[str] = None, sort: Optional[str] = None, same_epoch: bool = False,
                 columns: Optional[Tuple[int, int]] = None) -> Any:
        """One viewport request: the client's default column window, or the ``columns`` window (inclusive)."""
        self._seq += 1
        if not same_epoch:
            self._epoch += 1  # a new view state; a repeat keeps the current one
        state = PivotViewState(
            row_fields=list(GROUP_FIELDS), col_fields=["period"],
            val_configs=[{"field": m, "agg": "sum", "alias": f"{m}_sum"} for m in MEASURES],
            filters=({"region": {"operator": "AND", "conditions": [{"type": "eq", "value": region,
                                                                     "caseSensitive": True}]}} if region else {}),
            sorting=[{"id": sort, "desc": True}] if sort else [],
            expanded=dict(expanded or {}), show_row_totals=True, show_col_totals=True, show_subtotals=True,
            sort_options={"columnOptions": {"period": {"pivotSortMode": "label", "pivotDirection": "asc"}}},
        )
        context = PivotRequestContext.from_frontend(
            table=TABLE, trigger_prop="comp.viewport",
            viewport={"start": start, "end": end, "intent": "viewport", "window_seq": self._seq,
                      "state_epoch": self._epoch, "abort_generation": 0,
                      "session_id": "benchmark", "client_instance": "benchmark", "needs_col_schema": True,
                      "include_grand_total": True, "col_start": columns[0] if columns else 0,
                      "col_end": columns[1] if columns else None},
        )
        response = self._loop.run_until_complete(self.service.process_async(state, context))
        if response.status != "data":
            raise RuntimeError(f"product request failed: {response.status} {getattr(response, 'message', '')}")
        return response

    def all_cells(self, **kwargs: Any) -> Dict[str, Dict[str, Any]]:
        """Every row of the view by path, with every column, read window by window as the client scrolls."""
        first = self.get_rows(**kwargs)
        total = int((first.col_schema or {}).get("total_center_cols") or 0)
        rows = {str(r.get("_path")): dict(r) for r in first.data or []}
        for start in range(0, total, COLUMN_WINDOW):
            page = self.get_rows(columns=(start, start + COLUMN_WINDOW - 1), **kwargs)
            for r in page.data or []:
                rows.setdefault(str(r.get("_path")), {}).update(r)
        return rows

    def close(self) -> None:
        self._loop.close()


# --------------------------------------------------------------------------- scenarios


Scenario = Tuple[str, Dict[str, Any], Dict[str, Any]]


def scenarios(shape: str) -> List[Scenario]:
    first, second = "Region0000", "Region0001"
    out: List[Scenario] = [
        ("initial (collapsed)", {}, {}),
        ("expand first region", {"keys": (first,)}, {"expanded": {first: True}}),
        ("filter region", {"region": second}, {"region": second}),
        ("sort P000|units desc", {"sort": f"P000{PIVOT_SEPARATOR}units"}, {"sort": "P000_units_sum"}),
    ]
    if shape == "hierarchy":
        out.insert(1, ("vertical scroll rows 2000-2199", {"start": 2000, "end": 2200}, {"start": 2000, "end": 2199}))
    return out


def _product_field(ag_field: str) -> str:
    if PIVOT_SEPARATOR in ag_field:
        period, measure = ag_field.split(PIVOT_SEPARATOR)
        return f"{period}_{measure}_sum"
    return f"__RowTotal__{ag_field}_sum"


def verify_cells(ag_page: Dict[str, Any], ag_kwargs: Dict[str, Any], by_path: Dict[str, Dict[str, Any]]) -> int:
    """Every AG Grid page row and the grand total equal the product row with the same path. Returns cells checked."""
    keys = list(ag_kwargs.get("keys", ()))
    field = GROUP_FIELDS[len(keys)]
    fields = list(ag_page["pivotResultFields"]) + list(MEASURES)
    checked = 0
    pairs = [("|||".join([*keys, str(row[field])]), row) for row in ag_page["rowData"]]
    pairs.append(("__grand_total__", ag_page["grandTotalData"]))
    for path, ag_row in pairs:
        mine = by_path.get(path)
        if mine is None:
            if path == "__grand_total__":
                raise AssertionError("product response has no grand total row")
            continue  # outside the product's row window
        for name in fields:
            expected, actual = ag_row.get(name), mine.get(_product_field(name))
            if expected != actual:
                raise AssertionError(f"cell mismatch at {path!r} {name}: AG Grid {expected!r}, product {actual!r}")
            checked += 1
    if checked == 0:
        raise AssertionError("no AG Grid row found in the product window")
    return checked


def _median_ms(fn: Callable[[], Any], runs: int, before: Optional[Callable[[], None]] = None) -> Tuple[float, Any]:
    samples, result = [], None
    for _ in range(runs):
        if before is not None:
            before()
        gc.collect()
        started = time.perf_counter()
        result = fn()
        samples.append((time.perf_counter() - started) * 1000.0)
    return statistics.median(samples), result


def run_case(rows: int, shape: str, runs: int, log: Callable[[str], None]) -> Dict[str, Any]:
    facts = build_dataset(rows, shape)
    started = time.perf_counter()
    ag = AGGridServer(facts, shape)
    ag_load = (time.perf_counter() - started) * 1000.0
    started = time.perf_counter()
    product = ProductServer(facts)
    product_load = (time.perf_counter() - started) * 1000.0
    del facts
    log(f"\n== {shape} @ {rows:,} rows (load: AG Grid {ag_load:.0f} ms, product {product_load:.0f} ms)")
    report: Dict[str, Any] = {"rows": rows, "shape": shape,
                              "loadMs": {"agGrid": round(ag_load, 1), "product": round(product_load, 1)},
                              "scenarios": []}
    try:
        for name, ag_kwargs, product_kwargs in scenarios(shape):
            first, _ = _median_ms(lambda: product.get_rows(**product_kwargs), 2,
                                  before=lambda: product.clear(compile_cache=True))
            ag.get_rows(ag_request(**ag_kwargs))  # warm the AG connection the same way
            ag_ms, ag_page = _median_ms(lambda: ag.get_rows(ag_request(**ag_kwargs)), runs)
            product_ms, _ = _median_ms(lambda: product.get_rows(**product_kwargs), runs, before=product.clear)
            # Timed requests use the client's column window; the check reads every column, untimed.
            cells = verify_cells(ag_page, ag_kwargs, product.all_cells(**product_kwargs))
            product.get_rows(same_epoch=True, **product_kwargs)
            ag_hit, _ = _median_ms(lambda: ag.get_rows(ag_request(**ag_kwargs), cached=True), runs)
            product_hit, _ = _median_ms(lambda: product.get_rows(same_epoch=True, **product_kwargs), runs)
            record = {"name": name, "agMs": round(ag_ms, 2), "productMs": round(product_ms, 2),
                      "ratio": round(product_ms / ag_ms, 3), "productFirstMs": round(first, 2),
                      "agHitMs": round(ag_hit, 3), "productHitMs": round(product_hit, 3), "cellsChecked": cells}
            report["scenarios"].append(record)
            log(f"  {name:<32} AG {ag_ms:9.1f}  product {product_ms:9.1f}  ratio {record['ratio']:6.2f}x"
                f"  | first {first:9.1f} | hit AG {ag_hit:.2f} product {product_hit:.2f} | {cells} cells ok")
        if "expand first region" in {s[0] for s in scenarios(shape)}:
            def warm_then_expand() -> None:
                product.clear()
                product.get_rows()

            expand_ms, _ = _median_ms(lambda: product.get_rows(expanded={"Region0000": True}), runs,
                                             before=warm_then_expand)
            ag_ms = next(s["agMs"] for s in report["scenarios"] if s["name"] == "expand first region")
            report["expandAfterView"] = {"agMs": ag_ms, "productMs": round(expand_ms, 2),
                                         "ratio": round(expand_ms / ag_ms, 3)}
            log(f"  {'expand after a cached view':<32} AG {ag_ms:9.1f}  product {expand_ms:9.1f}  "
                f"ratio {expand_ms / ag_ms:6.2f}x")
    finally:
        product.close()
        ag.close()
        del product, ag
        gc.collect()
    return report


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", default="200000,1000000", help="comma-separated fact row counts")
    parser.add_argument("--shapes", default=",".join(SHAPES), help="hierarchy, wide or both")
    parser.add_argument("--runs", type=int, default=5, help="timed runs per scenario and side (median)")
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--max-ratio", type=float, default=None, help="fail when any uncached ratio exceeds this")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    sizes = [int(s) for s in args.sizes.split(",") if s.strip()]
    shapes = [s.strip() for s in args.shapes.split(",") if s.strip()]
    if any(s not in SHAPES for s in shapes) or args.runs < 1 or any(n < 1 for n in sizes):
        parser.error("invalid sizes, shapes or runs")
    log = (lambda _msg: None) if args.quiet else (lambda msg: print(msg, flush=True))
    reports = []
    for rows in sizes:
        for shape in shapes:
            reports.append(run_case(rows, shape, args.runs, log))
            if args.json_out is not None:
                args.json_out.write_text(json.dumps(reports, indent=1), encoding="utf-8")
    if args.quiet:
        print(json.dumps(reports))
    if args.max_ratio is not None:
        over = [(r["shape"], r["rows"], s["name"], s["ratio"]) for r in reports for s in r["scenarios"]
                if s["ratio"] > args.max_ratio]
        over += [(r["shape"], r["rows"], "expand after a cached view", r["expandAfterView"]["ratio"])
                 for r in reports if "expandAfterView" in r and r["expandAfterView"]["ratio"] > args.max_ratio]
        if over:
            for item in over:
                print(f"over budget: {item}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
