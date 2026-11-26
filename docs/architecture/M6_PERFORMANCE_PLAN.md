# M6 — Read path beyond AG Grid: the partial cube (plan, 2026-11-25)

Status: **in progress** (M6.0–M6.3 landed). Branch: `feat/golden-core` (no side branch, no parallel engine, no
feature flag).

The core stays the one read path: IR → logical plan → physical strategy → executor → result frame → bridge.
Every change lands in the layer that owns the cost and is the end state for that layer. `union_all` stays the
reference strategy. The public request/response contract (`tests/core/consumer_contract.json`) and the TS
client do not change.

**Goal: be much faster than AG Grid, not a copy of it.** AG Grid's SSRM datasource re-reads the raw facts on
every interaction, with about five statements per request (count, page keys, long-form cells, grand total and
its cells). This design makes reading the facts rare. A first view is one scan, which already does less work
than AG Grid's five. Every later interaction is answered from a cached **partial cube**, or from a scan of only
the records it adds.

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
| R3 | every level re-scans the facts and evaluates every cell per record | physical | §3.1, the cube: one scan in long form, cells merged from partials |
| R5 | a cell or measure sort re-aggregates the source per level | physical | §3.1, sort values are merged from the same cube |
| R7 | expand, scroll and sort recompute from the facts | cache | §3.2, the cube is a cache tier; interactions read it |
| R9 | about 50 ms of Python per request at 1M | runtime/bridge | §3.4 |

## 3. Target architecture

### 3.1 Physical: the partial cube (`cube` strategy, evolved from `leaf_partials`)

**The cube.** It is the relation of mergeable sufficient statistics per measure, grouped in long form. It has
one row per (leaf node, column member):
- the grain is the deepest level that can show a node, plus the column dimensions;
- the statistics are `sum`, `count`, `min` and `max`, and `avg` stored as sum and count.

It is computed in **one scan** of E (or E0, per the totals mode) with the parent restriction of §2 R2 applied.

**Everything is merged from the cube, not from the facts:**
- every level's row totals;
- every pivot cell (a cell merges only its member's partials, null-safe and typed);
- the grand total;
- measure-sort values, and cell-sort values (the member's partials of the ancestor);
- the measure-filter eligibility, when its measures are mergeable.

Merging is algebraic and exact for the mergeable aggregations. Count over no rows is 0. Avg is NULL when its
count is 0.

**Who uses it.** `choose()` picks the cube whenever every measure is mergeable, the grain is bounded by the
measured `MAX_PARTIAL_GRAIN` rule, and the query is not ragged:
- ragged hierarchies stay on `union_all` until the cube handles ended paths; that is a measured follow-up, not
  a decline;
- holistic aggregations (median, percentile, exact distinct, first/last, array_agg) stay on the exact
  `union_all` path, which keeps the parent restriction. Their correctness is never approximated.

`union_all` remains the reference. `tests/core/test_strategy_equivalence.py` and `test_parent_restriction.py`
prove the cube equals it and the oracle (generated data × queries × windows × expansion × sorts × measure
filters × pivot columns) on DuckDB, SQLite and Polars.

### 3.2 Cache manager: the `cube` tier

`core/cache.py` `TIERS` gains `cube`. An entry is an Arrow table of partials, keyed by:
- the query meaning without window, sort and expansion (dataset, filters, categories, measures, columns,
  totals mode);
- the grain depth and the parent-restriction set it covers;
- the scope fingerprint and the policy version (REQ-SEC-004).

It follows the same rules as the result tier: the epoch rule (a read that overlaps a load or mutation never
stores), byte accounting under the governor's cache budget, scope binding, and `invalidate(dataset)`.

| Interaction | Reads the facts? | Work |
|---|---|---|
| first view | one scan | build the cube for the visible levels |
| vertical scroll | no | merge, order and window the cached cube |
| sort (measure or pivot cell) | no | reorder from the cached cube |
| column scroll / column window | no | merge other members' partials |
| collapse | no | the cube for the remaining levels is a subset |
| expand a node | only that node's records | scan under the new parent (parent restriction), add the child grain to the cube |
| filter change | one scan | new meaning, new cube |
| data load / mutation | — | the tier is invalidated (M6.6 updates it incrementally instead) |

The merge-and-order step runs on the cube in the same backend: an in-memory DuckDB relation over the cached
Arrow table, with no Python per row.

### 3.3 Incremental maintenance (M6.6)

On a row mutation the controller's change feed applies the delta to the cached cube (sum and count add;
min/max add, or invalidate the affected partial when a current extreme is removed), instead of invalidating
the tier. It is exact by construction and proven against a rebuild by a property test.

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

Next:
- the per-level source re-aggregation in `_with_cell_sorts`, `_with_member_keys` and `_with_ancestor_measures`
  when the cube serves the query;
- `leaf_partials` as a separate name, once it becomes the cube.

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
| M6.4 The cube strategy | §3.1: `leaf_partials` → cube with pivot columns, cell/measure sorts and eligibility merged from it; chooser rule re-measured | next |
| M6.5 The cube tier | §3.2: cached cube, interactions served without reading the facts; expand scans only the new parent's records | |
| M6.6 Incremental cube | §3.3 | |
| M6.7 Runtime fixed cost and browser comparison | §3.4 and the browser port | |
| M6.8 Certification | `docs/gates/M6.md`, all §5 rows green | |

## 7. Decided (2026-11-25)

1. Beat AG Grid by design: scan the facts rarely and serve interactions from the partial cube. No AG Grid
   query-shape copy (`page_first` dropped).
2. Exactness is never traded: holistic measures stay exact on `union_all`, with the parent restriction.
3. The browser end-to-end comparison is part of M6.
