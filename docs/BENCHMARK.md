# Pivot runtime vs AG Grid SSRM — server benchmark

**Script:** `scripts/benchmark_core_vs_aggrid.py` (the M6 gate harness; see
`docs/architecture/M6_PERFORMANCE_PLAN.md`).

```
python scripts/benchmark_core_vs_aggrid.py --sizes 200000,1000000 --runs 5
PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES=13000000000 \
  python scripts/benchmark_core_vs_aggrid.py --sizes 10000000 --runs 5 --json-out m6-10m.json
```

## What it measures

- **AG Grid:** the native AG Grid Enterprise SSRM DuckDB datasource of the million-row browser comparison
  (`scripts/performance/million_queries.query_ag_grid`), plus its grand-total request. Each `getRows`
  aggregates exactly the next group level, pages it, counts it, and pivots the page in long form.
- **Product:** `PivotRuntimeService` with the same workspace (region > account > product, period pivot,
  sum of `sales_cents` and `units`, row and column totals) and the client's default column window.
- **Data:** the comparison's deterministic Arrow facts (`scripts/performance/million_dataset.py`):
  - `hierarchy`: 5,000 regions × 5 accounts × 5 products, 4 periods;
  - `wide`: 8 × 16 × 8, 256 periods.
- **Scenarios:**
  - initial collapsed view;
  - vertical scroll (hierarchy);
  - expanding the first region;
  - a region filter;
  - a sort by the `P000|units` pivot cell;
  - expanding after the collapsed view is cached.
- **Timing:** in-process, server side only, with no HTTP and no browser. The median of `--runs`.
  - "uncached": no cache on either side; every product result and viewport cache is cleared before each run.
  - "first": the product's first request of a shape, with the compile cache cleared too.
  - "hit": an identical repeat.
- **Correctness:** every timed scenario is checked cell by cell. Each AG Grid page row and the grand total must
  equal the product row with the same path, read in an untimed all-columns request. A mismatch fails the run.

## Baseline (2026-11-25, before M6)

i7-11800H, DuckDB 1.5. Product ÷ AG Grid, uncached; lower is better.

| Scenario | 200k | 1M | 10M |
|---|---|---|---|
| hierarchy initial | 7.3× | 7.8× | 5.0× |
| hierarchy vertical scroll | 8.7× | 8.4× | 6.5× |
| hierarchy expand | 15.6× | 22.2× | 36.2× |
| hierarchy filter | 4.3× | 4.8× | 14.1× |
| hierarchy sort by pivot cell | 10.5× | 10.5× | 22.0× |
| wide initial | 3.1× | 3.6× | 4.3× |
| wide expand | 6.3× | 5.0× | 7.8× |
| wide filter | 6.5× | 7.4× | 14.0× |
| wide sort | 6.0× | 6.0× | 10.7× |

Cache hits: AG Grid ~0.1 ms (dict lookup), product ~2 ms.

The M6 gate requires ≤ 1.0× in every row at every size; results are recorded in `docs/gates/M6.md`.

## Retired

`scripts/benchmark_vs_aggrid.py` and the results it produced (the "20–967× faster" tables) are withdrawn.
Its warmup filled the product cache, so every product figure it reported was a cache hit, compared against
an uncached pandas simulation of a datasource that is not AG Grid's.
