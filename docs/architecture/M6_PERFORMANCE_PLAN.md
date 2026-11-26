# M6 — Read path beyond AG Grid (plan, 2026-11-25; revised 2026-11-26)

Status: **in progress** (M6.0–M6.3 landed in 0.0.97; the partial cube was measured and removed, §3.1). Branch: `feat/golden-core` (no side branch, no parallel engine, no
feature flag).

The core stays the one read path: IR → logical plan → physical strategy → executor → result frame → bridge.
Every change lands in the layer that owns the cost and is the end state for that layer. `union_all` stays the
reference strategy. The public request/response contract (`tests/core/consumer_contract.json`) and the TS
client do not change.

**Goal: be much faster than AG Grid, not a copy of it.** AG Grid's SSRM datasource re-reads the raw facts on
every interaction, with about five statements per request (count, page keys, long-form cells, grand total and
its cells). This design answers a view with one statement (window, total and grand total together), and
child levels read only their visible parents' records. A cached partial cube to skip the facts entirely was
measured and removed (§3.1).

## 1. Evidence

Baseline, 2026-11-25 (i7-11800H, DuckDB 1.5, in process, server only, median of 3, both sides uncached). The
AG Grid side is the native SSRM DuckDB datasource (`scripts/performance/million_queries.query_ag_grid`) plus
its grand-total request. The data is the deterministic `hierarchy` set (5000 × 5 × 5, 4 periods) and the
`wide` set (8 × 16 × 8, 256 periods). Every scenario's cells matched AG Grid.

Product ÷ AG Grid (lower is better):

| Scenario | 200k | 1M | 10M | 1M after M6.1–M6.3 |
|---|---|---|---|---|
| hierarchy initial | 7.3× | 7.8× | 5.0× | 1.68× |
| hierarchy vertical scroll | 8.7× | 8.4× | 6.5× | 1.75× |
| hierarchy expand | 15.6× | 22.2× | 36.2× | 3.15× |
| hierarchy filter | 4.3× | 4.8× | 14.1× | 2.04× |
| hierarchy sort by pivot cell | 10.5× | 10.5× | 22.0× | 2.06× |
| wide initial | 3.1× | 3.6× | 4.3× | 2.09× |
| wide expand | 6.3× | 5.0× | 7.8× | 2.14× |
| wide filter | 6.5× | 7.4× | 14.0× | 2.77× |
| wide sort | 6.0× | 6.0× | 10.7× | 2.01× |

The remaining cost at 1M hierarchy initial: about 107 ms of SQL and about 50 ms of Python.
- The SQL is two full scans (the grand total and the region level), each evaluating every pivot cell as
  `SUM(CASE WHEN period IS NOT DISTINCT FROM …)` for every record.
- The Python is the runtime, the bridge and row assembly.

A long-form `GROUP BY region, period` over the same 1M rows takes 43 ms.

## 2. Root causes, by owning layer

| # | Cost | Layer | Status |
|---|---|---|---|
| R1 | ~60% of a 1M request: dead formula-source rows | bridge | **fixed (M6.1)**, producers and consumer deleted |
| R2 | expand 6.8 s of 7.9 s at 10M: a child level aggregated the whole table, then semi-joined the parents | logical → physical | **fixed (M6.2)**, `Level.parent_restriction()`, applied to the records before aggregating, in every strategy and in the sort-key lookups |
| R4 | 0.8 s at 10M: a separate count statement re-ran the whole tree | executor | **fixed (M6.3)**, one statement returns the window, `ResultFrame.total` and `ResultFrame.grand_total`; backends without window functions (Polars) read count and grand total from the same compiled tree |
| R6 | the column-member `DISTINCT` ran twice per request | bridge/adapter | **fixed (M6.1)**, members resolved once and passed to the view |
| R8 | per-row JSON canonical keys | frame | **fixed (M6.1)**, columnar keys, byte-identical to the codec (property test) |
| R3 | every level re-scans the facts and evaluates every cell per record | physical | `leaf_partials` where measured faster (≥2 visible levels, no pivot columns); the cube was slower (§3.1) |
| R5 | a cell or measure sort re-aggregates the source per level | physical | open, §3.2 (cube merging was slower) |
| R7 | expand, scroll and sort recompute from the facts | cache | the result tier serves repeats; the cube tier was slower than recomputing (§3.1) |
| R9 | about 50 ms of Python per request at 1M | runtime/bridge | §3.4 |

## 3. Target architecture

### 3.1 The partial cube: measured and removed (2026-11-26)

The cube (M6.4/M6.5) merged every level, pivot cell, sort and the grand total from one long-form scan of
mergeable partials, cached per meaning so later interactions skipped reading the facts. It was exact
(equivalence properties on DuckDB, SQLite and Polars) and slower everywhere. 1M rows, product ÷ AG Grid, same
harness, same run:

| Scenario | cube (cached tier) | union_all |
|---|---|---|
| hierarchy initial / scroll | 2.25× / 2.36× | 1.34× / 1.07× |
| hierarchy expand / expand after a view | 5.86× / 4.62× | 3.05× / 2.86× |
| hierarchy filter / cell sort | 4.67× / 3.34× | 1.36× / 1.43× |
| wide initial / expand / filter / sort | 2.37× / 2.81× / 4.34× / 3.75× | 1.64× / 1.75× / 2.68× / 1.76× |

Why: in process, DuckDB scans 1M facts in ~40 ms, while a cached cube must be re-registered (a memtable per
request) and its merge expression rebuilt and compiled, which costs more than the scan it saves. Without the
cache, merging pivot cells from partials is 1.2–1.5× slower than per-level cells (1M, 48 members, every view).
The cube, its cache tier (`core/cube_cache.py`, `physical/arrow_relation.py`, the controller's cube namespace)
and its tests were deleted.

What stays is the measured part: `leaf_partials` (one scan at the visible grain) is chosen with two or more
visible levels and no pivot columns, where it wins (10M fully expanded: 304 vs 468 ms); `union_all` serves
everything else (`core/physical/__init__.py`).

### 3.2 Next levers (re-planned from the measurement)

The remaining cost is not the scan: it is per-request expression building and compilation, and the Python
around the read (§3.4). The plan tier already caches the tree per meaning; the next step is measuring the
compile and frame-assembly share per scenario at 200k/1M/10M and removing it where it dominates.

### 3.4 Runtime and bridge fixed cost

Profile the per-request work outside the core read: admission, the session gate, layout, the column window,
response building and serialization. Remove what the response does not need, and assemble rows column by
column from the frame. Target: the Python overhead is below the SQL time at 200k.

### 3.5 Unchanged

The IR, the spec JSON, the oracle, the conformance vectors, the TS DuckDB-Wasm emitter, the V1 contract, the
frontend contract, authority and scope.

## 4. Deletion list

Done:
- formula-source rows;
- `_visible_node_count` and the count statement;
- the controller's visible-count cache and namespace;
- the post-aggregation `visible_parents` semi join;
- per-row JSON keys;
- `scripts/benchmark_vs_aggrid.py`.

Also removed (2026-11-26): the partial cube and its cache tier (`core/cube_cache.py`,
`physical/arrow_relation.py`, the controller's `core.cube` namespace and the `cube` cache tier).

Dropped from the earlier draft: the `page_first` strategy (a keys pass plus a long-form cells pass per
request). It reproduced AG Grid's per-request rescans.

## 5. Gate

The server harness is `scripts/benchmark_core_vs_aggrid.py` (sizes 200k / 1M / 10M, both shapes, every page
cell checked against AG Grid). Its wiring test is `tests/test_benchmark_core_vs_aggrid.py`. The 10M run sets
`PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES=13000000000`.

The browser end-to-end comparison (native AG Grid Enterprise SSRM against this branch's production React
build, same DuckDB server, pinned Chrome) is ported in M6.7 onto `PivotRuntimeService`.

| Criterion | Target |
|---|---|
| First view (initial, filter), uncached, every size | product ≤ 1.0× AG Grid |
| Interactions after a view (scroll, sort, column scroll, collapse, re-expand), every size | product ≤ 0.1× AG Grid |
| Expand a new node after a view, every size | product ≤ 0.5× AG Grid |
| Browser end to end, every size and workflow | product ≤ 1.0× AG Grid on first view, ≤ 0.5× on interactions; RSS ≤ 1.05× |
| Correctness | oracle, conformance vectors, equivalence properties at full budget on DuckDB/SQLite/Polars; every page cell equal to AG Grid |
| Scale | `scripts/certify_large_dataset.py --rows 10000000` passes, peak RSS reported |
| Suites | `tests/core` (ruff, mypy `--strict`, import contracts); full suite core-on and core-off, one after the other, `pytest -n auto` |

## 6. Milestones

Each step ships its final form with its tests; no step adds code a later step deletes.

| Step | Scope | Status |
|---|---|---|
| M6.0 Gate harness | §5 server harness and wiring test | done |
| M6.1 One read, no dead work | R1, R6, R8 | done |
| M6.2 Parent restriction | R2 | done |
| M6.3 Frame totals | R4 | done |
| M6.4 The cube strategy and tier | §3.1 | measured, removed |
| M6.7 Runtime fixed cost and browser comparison | §3.4 and the browser port | |
| M6.8 Certification | `docs/gates/M6.md`, all §5 rows green | |

## 7. Decided (2026-11-25)

1. Beat AG Grid by design, with no AG Grid query-shape copy (`page_first` dropped). The partial cube was the
   first design; measured slower, it was removed (§3.1). A design stays only if the harness shows the gain.
2. Exactness is never traded: holistic measures stay exact on `union_all`, with the parent restriction.
3. The browser end-to-end comparison is part of M6.
