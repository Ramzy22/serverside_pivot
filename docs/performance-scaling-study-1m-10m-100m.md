# Performance Scaling Study: Beating AG Grid at 1M, 10M, and 100M Rows

> **Scope & Verification Note**
> This study synthesizes paired server benchmarks (`1M`, `10M`, `100M`), direct Python `cProfile` / phase-timing runs (`1M`), DuckDB SQL query-shape ablations (`1M`, `10M`), and Playwright/CDP browser E2E telemetry (`1M`, Run 14). All cited source locations reflect files inspected directly in the main workspace (`c:/Users/ramzy/Downloads/serverside_pivot`) and the `4cc1` benchmark worktree (`C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot`). Projected speedups in the roadmap are clearly separated from directly measured baselines.

---

## 1. Executive Summary

Across scaling tiers, **`serverside_pivot` already overtakes AG Grid on large server workloads (10M–100M rows)** where DuckDB query execution dominates, **loses at 1M on the server** due to fixed Python/planning/caching overhead, and **lags 1.4x–28.8x in browser E2E latency** due to frontend React rendering, debounced viewport/column state synchronization, and network round-trips on horizontal scrolling.

| Dimension | 1M (Server) | 10M (Server) | 100M (Server) | 1M Browser (Run 14 E2E) |
| :--- | :--- | :--- | :--- | :--- |
| **Non-cache-hit wins** | **2 / 11** (`1.67s` vs `0.71s`) | **8 / 11** (`2.15s` vs `2.84s`; `9/14` total) | **11 / 11** (`30.89s` vs `63.27s`) | **0 / 8** reported scenarios faster |
| **Memory footprint (RSS)** | `179.8 MB` | — | `191.9 MB` | `396.1 MB` server / `556.5 MB` browser peak (cold hierarchy) |
| **Primary bottleneck** | Fixed Python overhead (`~70–150 ms` warm, `~960–2000 ms` cold) | Remaining dual/multi-scan SQL + serialization/planning overhead | Multi-scan DuckDB SQL & large expanded cache materialization (`cacheStoreMs`) | React cell wrapper overhead, 67-dep `useColumnDefs` rebuilds, `96 ms` horizontal fetch + JSON round-trips |

### Key Strategic Takeaways

1. **Why 100M wins (`11/11`, ~2.05x faster) while 1M loses (`2/11`)**: At `100M` rows, DuckDB query execution time (`~0.7s–4.4s` product vs `~0.8s–9.3s` AG Grid) dwarfs Python orchestration cost (`~70–150 ms`), so our engine wins every non-cache-hit case. At `1M` rows, DuckDB itself takes `<100 ms`, meaning `85–140 ms` of Python planning, multi-compile cache hashing, envelope validation, and extra count/total queries decide the race.
2. **Naive Single-Scan `GROUPING SETS` is a trap — 2-Pass Window + `IN` Pivot wins cleanly**: Replacing multi-scan queries with a single `GROUPING SETS` containing 8 wide `FILTER (WHERE period = ...)` aggregates actually **regressed** `1M` SQL time (`214.87 ms` vs `122.24 ms` AG Grid) because wide hash payloads had to be maintained across all 5,000 groups. Conversely, a **2-Pass Window + `IN` Pivot** (computing `COUNT(*) OVER()` + `SUM(SUM(...)) OVER()` in a narrow pass, then filtering the top 100 groups via `IN (...)` before pivoting) beat AG Grid at both **1M (`73.02 ms` vs `98.85 ms`, `-26%`)** and **10M (`348.58 ms` vs `611.12 ms`, `-43%`)**. A **1-Pass Narrow Long `GROUPING SETS`** shape (`76.19 ms` at 1M; `492.63 ms` at 10M) covers non-additive measures.
3. **Browser E2E performance requires zero-network horizontal pan & slim React cells**: In Run 14 (`1M`), horizontal scrolling took **`1262.3 ms` vs `43.8 ms` (`28.82x` slower)** because column scrolling debounces metrics (`96 ms`), transfers `26,357` bytes vs `0` bytes, and triggers `+1.14s` of JavaScript execution (`17.2 MB -> 41.9 MB` JS heap). Meanwhile, cached vertical scrolling (`205.2 ms` vs `65.2 ms`, `3.15x` slower) rendered `10,509` DOM nodes vs AG Grid's leaner viewport and incurred heavy React reconciliation inside per-cell `<CellErrorBoundary>` + `<SafeCellContent>` wrappers.

---

## 2. Measured Baselines & Ablations

### 2.1 Server Paired Benchmarks (1M vs 10M vs 100M)

Benchmark harnesses and result files:
- 1M script & result: [benchmark_1m.py](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/benchmark_1m.py), [comparison_1m.json](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/comparison_1m.json) (imports main workspace `serverside_pivot/pivot_engine`)
- 10M script & result: [paired_comparison_10m.py](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/paired_comparison_10m.py), [comparison_10m.json](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/comparison_10m.json) (imports `4cc1` worktree `dash_tanstack_pivot/pivot_engine`)
- 100M script & result: [benchmark_100m.py](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/benchmark_100m.py), [comparison_100m.json](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/comparison_100m.json) (imports main workspace `serverside_pivot/pivot_engine`)

*Note on cache-hit rows (`0.0 ms`)*: In [benchmark_1m.py](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/benchmark_1m.py), `AGGridRunner` checks an in-process `MemoryCache` via raw JSON key before planning SQL (`query_ag_grid` page + separate `totals_req`), yielding `0.0 ms` on exact hits. `ProductRunner` routes through `controller.run_pivot(spec, return_format="dict", force_refresh=...)`, executing planning/diffing/key hashing before returning cached results.

| Case | 1M (`Product vs AG`, ms) | 10M (`Product vs AG`, ms) | 100M (`Product vs AG`, ms) |
| :--- | :---: | :---: | :---: |
| `hierarchy / initial / cold` | `962.9` vs `61.1` | `369.7` vs `321.3` | **`5692.0` vs `6046.2`** |
| `hierarchy / initial / warm` *(cache hit)* | `34.8` vs `0.0` | `45.4` vs `0.0` | `92.1` vs `0.0` |
| `hierarchy / expand-collapse / warm` | `50.7` vs `31.3` | **`130.2` vs `158.0`** | **`1956.8` vs `4442.0`** |
| `hierarchy / filter / warm` | `51.2` vs `30.7` | **`100.9` vs `105.3`** | **`1393.8` vs `3648.1`** |
| `hierarchy / sort / warm` | `69.2` vs `66.4` | **`377.9` vs `398.7`** | **`4176.8` vs `7892.4`** |
| `hierarchy / vertical-uncached / warm` | `62.5` vs `54.0` | `395.7` vs `355.2` | **`3820.7` vs `6360.4`** |
| `hierarchy / vertical-cached / warm` *(cache hit)* | `64.2` vs `0.0` | `361.4` vs `0.0` | `4638.8` vs `0.0` |
| `hierarchy / combination / warm` | `59.7` vs `39.4` | **`122.7` vs `143.3`** | **`1531.2` vs `4554.1`** |
| `wide / initial / cold` | **`107.6` vs `149.5`** | **`209.8` vs `420.9`** | **`4433.8` vs `8848.2`** |
| `wide / initial / warm` *(cache hit at 1M/100M)* | `53.1` vs `0.0` | **`174.7` vs `430.7`** | `43.4` vs `0.0` |
| `wide / filter / warm` | `99.3` vs `64.8` | **`115.3` vs `204.9`** | **`1737.1` vs `5629.7`** |
| `wide / sort / warm` | **`76.1` vs `127.8`** | **`169.7` vs `477.5`** | **`3816.8` vs `9283.3`** |
| `wide / horizontal / warm` | `67.9` vs `24.4` | `73.3` vs `41.5` | **`721.6` vs `769.8`** |
| `wide / combination / warm` | `63.7` vs `56.1` | **`89.4` vs `214.5`** | **`1612.1` vs `5798.3`** |
| **Non-cache-hit Wins & Totals** | **2/11** (`1.67s` vs `0.71s`, RSS `179.8 MB`) | **8/11** (`2.15s` vs `2.84s`; `9/14` total) | **11/11** (`30.89s` vs `63.27s`, RSS `191.9 MB`) |

*Notice*: Prior log [task-318.log](file:///C:/Users/ramzy/.gemini/antigravity/brain/4b2a4f84-16be-4fc1-bd33-f3c0c4613b21/.system_generated/tasks/task-318.log#L1-L28) from an earlier investigation produced no `cProfile` data because all 12 `/v1/runtime` calls failed HTTP `422` (`Additional properties are not allowed ('layoutMode' was unexpected)`). All profiling data below was freshly collected in this study.

---

### 2.2 Direct Python `cProfile`, `run_hierarchy_view`, and Unprofiled Timing Breakdown (1M)

#### `cProfile` Breakdown (`task-1337`, 1M rows)
- **Case 1 (Process-Cold)**: **`2005.75 ms`** total
  - `ibis\backends\duckdb\__init__.py:1393(execute)`: `1.769s` cumtime -> `_to_duckdb_relation`: `1.753s` -> `pandas\__init__.py`: `1.678s` (lazy Pandas import triggered on first execution path).
  - `ibis\backends\duckdb\__init__.py:1377(to_pyarrow)`: `0.124s`.
- **Case 2 (Warm Cache Hit)**: **`136.87 ms`** total *(even on a full cache hit!)*
  - `ibis\backends\sql\__init__.py:93(compile)`: `ncalls=3`, `0.091s`
  - `diff_engine.py:238(plan)`: `0.063s`
  - `diff_engine.py:810(_cache_key_for_query)`: `ncalls=2`, `0.058s`
  - `ibis_planner.py:239(plan)`: `0.039s`
  - `duckdb\__init__.py:286(get_schema)`: `ncalls=1`, `0.012s`
- **Case 3 (Warm-Process Cold)**: **`230.35 ms`**
- **Case 6 (`offset=2400`)**: **`181.28 ms`**
- **Case 7 (`offset=0` after `2400`, `force_refresh=False`)**: **`161.90 ms`** (vertical scroll miss caused by `offset` omitted in diff analysis)

#### `run_hierarchy_view(profiling=True)` Breakdown (`task-1337`, 1M rows)
- **Collapsed Cold**: **`1452.50 ms`**
  - `path`: `'paged_collapsed_root'`, `totalRows`: `5001`, `totalMs`: `1449.521`
  - `rootQueryMs`: `240.766`, `rootCountMs`: `76.173`, `grandTotalMs`: `92.16`, `colorScaleMs`: `0.657`, `rootMaterialization`: `'stored'`
  - Remaining **~1,040 ms** spent outside SQL in Python dict/Arrow conversion (`to_pylist()` over 5,000 root rows), `force_refresh=True` sub-calls, and cache serialization.
- **Collapsed Warm**: **`835.84 ms`** (fails to short-circuit cleanly at entry of `_run_paged_collapsed_root_view`, re-running uncached/heavy work).
- **Expand 1 Row**: **`1934.89 ms`**
  - `path`: `'materialized_hierarchy'`, `cacheHit`: `False`, `reusedCache`: `False`, `totalRows`: `20081`, `totalMs`: `1928.899`
  - `hierarchyLoadMs`: `546.37` (`root`: `424.932 ms`, `depth2`: `114.365 ms`)
  - `flattenMs`: `396.151 ms`
  - `colorScaleMs`: `396.156 ms`
  - **`cacheStoreMs`: `971.032 ms`** (~50% of total expand time spent sizing & writing 20,081 rows into the bounded hierarchy cache!)

#### Unprofiled Phase Timing (`task-1357`, 1M rows)
Measuring each isolated stage without `cProfile` call-hook overhead:

| Stage (ms) | 1M Hierarchy | 1M Wide |
| :--- | :---: | :---: |
| `normalize` | `0.01` | `0.01` |
| `planner.plan` | `20.89` | `16.49` |
| `diff.plan` | `18.02` | `16.40` |
| `col_disc` / `build_piv` | `0.00` / `0.00` | `0.00` / `0.00` |
| `compile` | `17.32` | `13.07` |
| `ibis.to_pyarrow` | `133.63` | `71.15` |
| `direct_duckdb_sql` | `94.59` | `49.45` |
| `to_dict` | `10.53` | `3.67` |
| **`warm_hit`** | **`84.80`** | **`63.24`** |

Notice the Python tax at 1M: `direct_duckdb_sql` is `94.59 ms` (hierarchy) / `49.45 ms` (wide), yet an **already-cached `warm_hit`** still costs **`84.80 ms` / `63.24 ms`** (almost as much as executing raw SQL!). And routing through `planner.plan` + `diff.plan` + `compile` + `ibis.to_pyarrow` adds `~77 ms` on top of raw DuckDB SQL on hierarchy (`20.89 + 18.02 + 17.32 + (133.63 - 94.59)`) and `~68 ms` on wide.

---

### 2.3 SQL Strategy Ablation (1M & 10M DuckDB)

We directly tested candidate SQL execution shapes in DuckDB against `AG Grid (multi-scan)`:

| SQL Strategy | 1M (`min` of 3, runs ms) | 10M (`min` of 3, runs ms) | Behavior & Engine Notes |
| :--- | :--- | :--- | :--- |
| **Naive Single-Scan `GROUPING SETS`** (8 `FILTER (WHERE period = ...)`) | `214.87 ms` (`rows=101`) | — *(lost in 1M check)* | **Failed**: Wide hash-table aggregate states across all 5,000 groups inflate hash payload (~80B per group) × N input rows. Slower than AG Grid (`122.24 ms` in same `task-1337` batch). |
| **`AG Grid (multi-scan)`** *(Baseline)* | **`98.85 ms`** (`[98.8, 100.8, 120.4]`) | **`611.12 ms`** (`[611.1, 691.2, 626.3]`) | Runs separate viewport query + count query + grand-total query. |
| **`1-Pass Narrow GROUPING`** *(Long format)* | **`76.19 ms`** (`[83.1, 76.2, 88.4]`) | **`492.63 ms`** (`[512.2, 492.6, 508.9]`) | Narrow long `GROUPING SETS` avoids wide `FILTER` accumulators; strong fallback for non-additive measures (`DISTINCT COUNT`, median/quantiles). `-23%` at 1M, `-19%` at 10M. |
| **`2-Pass Window + IN Pivot`** *(Best overall)* | **`73.02 ms`** (`[88.7, 77.8, 73.0]`) | **`348.58 ms`** (`[348.6, 368.6, 403.4]`) | **Pass 1**: Narrow `GROUP BY region` with `COUNT(*) OVER()` + `SUM(SUM(...)) OVER()` returns total row count + grand total alongside top 100 root keys in **one scan** (eliminates AG Grid's `count_sql` and `totals_req`). **Pass 2**: `WHERE region IN (...100...) GROUP BY region, period` prunes 98% of groups before pivoting. **`-26%` at 1M, `-43%` at 10M**. |

---

### 2.4 Browser E2E Paired Metrics (`1M`, Run 14)

Source artifacts: [report.json](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/artifacts/million-paired-run-14-20261113-2050/report.json) and case `browser-heap.json` telemetry inside [million-paired-run-14-20261113-2050](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/artifacts/million-paired-run-14-20261113-2050).

| Scenario (1M Browser) | `Product` vs `AG Grid` (ms) | Slowdown | Bytes Transferred | CDP DOM / Layout / Script / Memory Telemetry |
| :--- | :---: | :---: | :---: | :--- |
| `hierarchy / initial / cold` | `1180.9` vs `688.5` | `1.72x` | `79,956` vs `50,388` | `serverPeakRss`: `396.1 MB` vs `386.7 MB`; `browserPeakRss`: `556.5 MB` vs `526.6 MB` |
| `hierarchy / initial / warm` | `418.4` vs — | — | `79,955` | — |
| `hierarchy / expand-collapse / warm` | `2112.7` vs `1037.2` | **`2.04x`** | — | Product `Layout`: `17 -> 41` (`0.1792s -> 0.4071s`), `Script`: `0.5629s -> 2.0021s` vs AG `Layout`: `39 -> 55` (`0.0617s -> 0.0722s`), `Script`: `0.1464s -> 0.8107s` |
| `hierarchy / vertical-cached / warm` | `205.2` vs `65.2` | **`3.15x`** | — | Product `Nodes`: `5828 -> 10509`, `LO`: `7287 -> 13408`, `Layout`: `32 -> 36` (`0.2808s -> 0.3240s`), `Script`: `1.1784s -> 1.3973s`, `Heap`: `23.6 -> 35.6 MB` vs AG `LO`: `4486 -> 5818`, `Layout`: `47 -> 48` (`0.0858s -> 0.0890s`), `Script`: `0.4482s -> 0.4931s`, `Heap`: `12.1 -> 15.9 MB` |
| `wide / initial / warm` | `490.2` vs `347.9` | `1.41x` | `31,249` vs `129,403` | Product sends fewer bytes on wide initial load but still renders `142 ms` slower |
| `wide / horizontal / warm` | `1262.3` vs `43.8` | **`28.82x`** | `26,357` vs `0` | Product `Script`: `0.6324s -> 1.7692s` (`+1.14s`), `Heap`: `17.2 -> 41.9 MB` vs AG `Script`: `0.1680s -> 0.2083s` (`+0.04s`), `Heap`: `8.3 -> 11.1 MB` |
| `wide / filter / warm` | `1260.4` vs `429.1` | `2.94x` | `8,372` vs `33,001` | Small payload (`8.4 KB`), yet `831 ms` slower — dominated by server + client execution |
| `wide / combination / warm` | `2674.8` vs `904.8` | **`2.96x`** | — | Compounds filter/sort/layout recomputation across backend + React |

---

## 3. Verified Root-Cause Inventory

### 3.1 Python Backend & Planner Bottlenecks

*(Files verified identical between `main` and `4cc1`: [diff_engine.py](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/diff/diff_engine.py), [hierarchy_cache_registry.py](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/hierarchy_cache_registry.py), and [duckdb_backend.py](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py).)*

1. **Process-Cold Lazy `pandas` Import (`~1.68s` one-time tax)**
   - [ibis_backend.py#L318-L349](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py#L318-L349): Line 340 performs a lazy `import pandas as pd` fallback inside execution, causing `ibis.execute()` -> `_to_duckdb_relation()` to load Pandas (`1.678s` cumtime) on the first query (`Case 1: 2005.75 ms`).
   - Also eliminates Arrow-native execution fast path wherever `execute()` is hit instead of native `to_pyarrow` or direct DuckDB Arrow consumption.

2. **Full Planning & Multi-Compile Before Cache Lookup (`~85 ms` warm hit)**
   - [controller.py#L147-L243](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/controller.py#L147-L243): Synchronous `run_pivot` runs `planner.plan(spec)` (`~16–21 ms`) before checking any response cache.
   - [diff_engine.py#L810-L843](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/diff/diff_engine.py#L810-L843): `_cache_key_for_query` and `_digest_plan` trigger multiple `backend.compile` calls (`ncalls=3`, `91 ms` profiled / `17.32 ms` unprofiled) and DuckDB `get_schema` (`12 ms` profiled) just to compute a cache key.
   - [diff_engine.py#L687-L745](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/diff/diff_engine.py#L687-L745): `_analyze_spec_change` omits `offset` when analyzing delta vs full-replan changes, causing avoidable cache misses on vertical scroll (`Case 7: 161.90 ms` when scrolling back to `offset=0`).
   - Compare with [benchmark_1m.py](file:///C:/Users/ramzy/.gemini/antigravity/brain/bf321db5-5db4-4306-862c-356a3a814aaf/scratch/benchmark_1m.py), where `AGGridRunner` checks a normalized request JSON key against `MemoryCache` before touching SQL/planners (`0.0 ms`).

3. **Paged Collapsed Root View Skips Entry Cache, Forces Refresh, and Double-Scans**
   - [scalable_pivot_controller.py#L1580-L1822](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py#L1580-L1822) (`_run_paged_collapsed_root_view`) and `#L1407-L1474` (`_can_use_paged_collapsed_root_view`, `_count_collapsed_root_rows`):
     - Does **not** check the view cache (`_hierarchy_view_cache_get`) at function entry, explaining why `COLLAPSED WARM` still took **`835.84 ms`**.
     - Issues three separate DuckDB round-trips on collapsed cold loads (`rootQueryMs: 240.766`, `rootCountMs: 76.173`, `grandTotalMs: 92.16` — `totalMs: 1449.521`), and passes `force_refresh=True` on `total_spec` (`#L1720`, `#L1754`).
     - **`main` vs `4cc1` drift**: On `main` (`#L1740-L1762`), `_run_paged_collapsed_root_view` unconditionally converts/clones 5,000 root rows via `to_pylist()`. In `4cc1` ([scalable_pivot_controller.py#L1695-L1746](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py#L1695-L1746)), `collect_formula_source_rows` skips cloning all 5,000 rows when no formula measures exist, **but still calls `_hierarchy_view_cache_set` on every call**.

4. **Hierarchy Expansion Cache Serialization Explosion (`971 ms` for `20,081` rows)**
   - [hierarchy_cache_registry.py#L18-L47](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/hierarchy_cache_registry.py#L18-L47) (`estimate_container_bytes`) and `#L68-L98` (`BoundedHierarchyCache._sync`, `set_entry`, `__getitem__`):
     - Every cache read/write rebuilds the `OrderedDict` via `_sync()`.
     - Storing a cache entry recursively traverses nested containers via `estimate_container_bytes` across `20,081` expanded rows, burning **`971.032 ms` (`cacheStoreMs`)** out of `1928.899 ms` during `EXPAND 1 ROW`, alongside `flattenMs: 396.151 ms` and `colorScaleMs: 396.156 ms`.
   - Meanwhile, [scalable_pivot_controller.py#L2584-L2601](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py#L2584-L2601) and [query_execution_service.py#L22-L136](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/query_execution_service.py#L22-L136) disable async background materialization when `shared_connection_requires_lock` or `_uses_duckdb_ibis()` is true, and [materialized_hierarchy_manager.py#L1-L100,#L200-L232](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/materialized_hierarchy_manager.py#L1-L100) only matches exact-level hashes (`find_best_rollup`) and runs inline synchronously on DuckDB.

5. **Planner UNION Simulation, Tight Sparse Caps, Connection Probes, Hashing & Envelope Copies**
   - [ibis_planner.py#L1518-L1592](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/planner/ibis_planner.py#L1518-L1592): `_plan_grouping_sets` still uses `"simulated_via_union": True` rather than emitting native DuckDB multi-pass / narrow long `GROUPING SETS`.
   - [bounded_sparse_pivot.py#L13-L79](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/pivot_engine/pivot_engine/bounded_sparse_pivot.py#L13-L79): Hard caps (`MAX_ROOTS = 32`, `MAX_COLUMNS = 512`, `MAX_CELLS = 8192`) restrict when `_can_use_sparse_materialized_pivot` ([scalable_pivot_controller.py#L505-L552](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py#L505-L552)) applies.
   - [duckdb_backend.py#L207-L280](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py#L207-L280): `ConnectionPool._is_healthy` executes `SELECT 1` on **both** `_borrow_connection` (checkout) and `checkin`.
   - [column_layout.py#L53-L107](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/column_layout.py#L53-L107), [tanstack_adapter.py#L1051-L1110](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/tanstack_adapter.py#L1051-L1110), [adapter_viewport_cache.py#L1-L175](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/adapter_viewport_cache.py#L1-L175): Heavy SHA-256 JSON hashing across window, row-block, grand-total, catalog, and layout-revision caches.
   - [contracts.py#L100-L195](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/runtime/contracts.py#L100-L195) & [http_transport.py#L370-L440](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/runtime/http_transport.py#L370-L440): `jsonschema` validation + `copy.deepcopy` on runtime request/response envelopes.

---

### 3.2 Frontend & Transport Bottlenecks (`4cc1` Run 14 Browser Trace)

1. **Horizontal Pan Forces Debounced Server Round-Trip + React State Rebuild (`1262.3 ms` vs `43.8 ms`, `28.82x`)**
   - [useColumnVirtualizer.js#L11](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useColumnVirtualizer.js#L11) & [`#L236-L288`](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useColumnVirtualizer.js#L236-L288): Every horizontal scroll invokes `scheduleHorizontalMetrics(..., { debounceMs: SERVER_SIDE_HORIZONTAL_METRICS_DEBOUNCE_MS })` (`96 ms`).
   - [useServerSideRowModel.js#L109-L110](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useServerSideRowModel.js#L109-L110) & [`#L1371-L1417`](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useServerSideRowModel.js#L1371-L1417): `coalesceColumnJumpRequest` evaluates `columnRangeUrgent || columnJumpDistance >= 12` and sets `immediate: shouldDispatchImmediately && !coalesceJumpRequest`, meaning column jumps wait `VIEWPORT_FAST_COLUMN_DEBOUNCE_MS` (`96 ms`) and large vertical jumps wait `VIEWPORT_FAST_SCROLL_DEBOUNCE_MS` (`140 ms`).
   - Consequence in Run 14 (`wide/horizontal`): `26,357` bytes transferred over the network vs `0` bytes for AG Grid (which retains loaded wide columns cleanly on the client), driving `+1.14s` of main-thread `Script` time and `+24.7 MB` JS heap growth (`17.2 -> 41.9 MB`).

2. **Per-Cell ErrorBoundary & Render-Closure Allocation (`3.15x` slower on cached vertical scroll)**
   - [useRenderHelpers.js#L353-L357](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useRenderHelpers.js#L353-L357) & [safeCellContent.js#L18-L52](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/presentation/safeCellContent.js#L18-L52): Every visible cell wraps `flexRender(col.columnDef.cell, displayCellLike.getContext())` in `<CellErrorBoundary resetKey={...}><SafeCellContent render={() => ...} /></CellErrorBoundary>`. Each cell allocates a React class-component boundary instance (`constructor`, `componentDidUpdate`), an extra wrapper component, and a fresh `render={() => ...}` closure.
   - Combined with [PivotTableBody.js#L865-L955](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/components/Table/PivotTableBody.js#L865-L955) per-row lookups and overscan (`rowOverscan: 8` in [million_server.py](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/scripts/performance/million_server.py#L151-L350), `cacheBlockSize: 200, maxBlocksInCache: 10`), `hierarchy/vertical-cached` grew to **`10,509` DOM nodes** (`LO=13,408`) vs AG Grid's `LO=5,818`, taking `205.2 ms` vs `65.2 ms`.

3. **67-Dependency `useColumnDefs` Memo & Deep `JSON.stringify` State Diffs**
   - [useColumnDefs.js#L1798-L1896](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useColumnDefs.js#L1798-L1896): The column-definition `useMemo` tracks **67 dependencies**, including frequently changing UI/request states (`sorting`, `pendingRowTransitions`, `selectedRowKeys`, `renderedOffset`, `cachedColSchema`, `responseColumns`, `filteredData`). Any bump rebuilds the entire recursive column tree (`buildColumns([...hierarchyCols, ...dataCols])`).
   - [useRuntimeStructuralSync.js#L83-L147](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/runtime/useRuntimeStructuralSync.js#L83-L147): Serializes all 23+ state keys with `JSON.stringify(...)` to detect changed keys on commit. *(Also check `main` drift: [main PivotWorkspace.tsx#L147](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/src/lib/components/PivotWorkspace.tsx#L147) still runs `JSON.stringify(initialProps)`.)*

4. **Synchronous DOM Cloning & Reflow in Column Measurement**
   - [useRuntimeColumnMeasurement.js#L16-L85](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/runtime/useRuntimeColumnMeasurement.js#L16-L85) & [useRuntimeColumnAutoSizing.js#L27-L72](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/runtime/useRuntimeColumnAutoSizing.js#L27-L72): `autoSizeColumn` allocates a fresh `<canvas>` per column **and** still invokes `getRenderedHeaderWidthForColumn` (which calls `cloneNode(true)`, appends into `headerElement`, forces synchronous reflow via `.scrollWidth`, and removes the clone) and `getRenderedCellWidthForColumn` (`querySelectorAll` + `window.getComputedStyle` + `.scrollWidth`), plus `queueHeaderFitValidation`.

---

## 4. Prioritized Optimization Roadmap

> Estimates in **Projected Impact** are engineering projections derived from the measured timings in Section 2 and have not yet been benchmarked end-to-end after implementation.

### Phase 1: Quick Wins — Eliminate Pure Waste (Low Risk)

1. **O(1) Pre-Plan Response Cache Gate & `offset` Diff Fix**
   - **Targets**: [controller.py#L147-L243](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/controller.py#L147-L243), [scalable_pivot_controller.py#L1580-L1822](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py#L1580-L1822), [diff_engine.py#L687-L745](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/diff/diff_engine.py#L687-L745)
   - **Action**:
     - Check a fast canonical tuple / pre-hashed spec key (`table`, `rows`, `columns`, `values`, `filters`, `sort`, `expanded`, `offset`, `limit`) at the **entry** of `run_pivot()` and `_run_paged_collapsed_root_view()` before calling `planner.plan(spec)` or `backend.compile()`.
     - Include `offset` in `_analyze_spec_change` (`Case 7` fix) and remove unconditional `force_refresh=True` on `total_spec` when spec filters/rows haven't changed.
     - Sync `4cc1`'s `collect_formula_source_rows` optimization back to `main` so collapsed non-formula loads never convert/clone 5,000 root rows.
   - **Projected Impact**: Warm cache hits drop from **`63–85 ms` (`835.8 ms` collapsed warm)** to **`< 2 ms`**; eliminates Case 7 scroll-back misses (`161.9 ms -> < 2 ms`).

2. **Eliminate Lazy `pandas` Import, Dual `SELECT 1` Pool Probes & `OrderedDict` Rebuild Tax**
   - **Targets**: [ibis_backend.py#L318-L349](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py#L318-L349), [duckdb_backend.py#L207-L280](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py#L207-L280), [hierarchy_cache_registry.py#L18-L98](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/hierarchy_cache_registry.py#L18-L98)
   - **Action**:
     - Remove or pre-warm/bypass the lazy `import pandas as pd` fallback in `ibis_backend.py` by registering/binding PyArrow/DuckDB relations natively (`~1.68s` cold-start elimination).
     - Replace `ConnectionPool._is_healthy` `SELECT 1` on every checkout + checkin with idle-TTL / exception-triggered validation (zero SQL round-trips per borrow).
     - In `BoundedHierarchyCache`, stop rebuilding `OrderedDict` in `_sync()` on every `__getitem__` / `set_entry`, and compute `estimate_container_bytes` via bounded header/sample heuristics (`len(rows) * sampled_row_bytes`) rather than recursively walking `20,081` dict entries.
   - **Projected Impact**: Cuts `Case 1 Process-Cold` from **`2005.8 ms` -> `~250–350 ms`**, and cuts `EXPAND 1 ROW` `cacheStoreMs` from **`971 ms` -> `< 15 ms`**.

3. **Zero-Copy Runtime Contracts & Structural Cache Fast Keys**
   - **Targets**: [contracts.py#L100-L195](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/runtime/contracts.py#L100-L195), [http_transport.py#L370-L440](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/runtime/http_transport.py#L370-L440), [column_layout.py#L53-L107](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/column_layout.py#L53-L107), [adapter_viewport_cache.py#L1-L175](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/adapter_viewport_cache.py#L1-L175)
   - **Action**: Validate incoming requests with lightweight structural checks (or opt-in strict `jsonschema` in debug/test mode), drop `deepcopy` of outgoing read-only response envelopes, and key layout/viewport caches with structural tuple/version stamps instead of repeated SHA-256 JSON dumps.

---

### Phase 2: SQL & Arrow Engine Breakthrough (Beat AG Grid on All 1M & 10M Server Cases)

1. **Deploy the Measured 2-Pass Window + `IN` Pivot SQL Executor on DuckDB**
   - **Targets**: [ibis_planner.py#L1518-L1592](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/planner/ibis_planner.py#L1518-L1592), [scalable_pivot_controller.py#L1580-L1822](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py#L1580-L1822)
   - **Action**:
     - For additive measures (`SUM`, `COUNT`, `MIN`, `MAX`), emit direct DuckDB SQL using **Pass 1** (`GROUP BY root_key` + `COUNT(*) OVER()` + `SUM(SUM(...)) OVER()`) to obtain paged root sort order, exact total row count, and grand total in a **single scan** (replacing separate `rootQueryMs` + `rootCountMs` + `grandTotalMs` queries: `240.8 + 76.2 + 92.2 = 409 ms` uncached on 1M collapsed).
     - Execute **Pass 2** (`WHERE root_key IN (...) GROUP BY root_key, col_dims`) across only the visible page's root keys.
     - For non-additive measures (`DISTINCT`, quantiles), route to **1-Pass Narrow Long `GROUPING SETS`** instead of `"simulated_via_union": True`.
   - **Directly Measured SQL Ablation Gain**: `73.02 ms` vs `98.85 ms` (`-26%`) at 1M; `348.58 ms` vs `611.12 ms` (`-43%`) at 10M.

2. **Bypass Ibis Re-Compile + Per-Row Python Dict Materialization**
   - **Targets**: [diff_engine.py#L810-L843](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/diff/diff_engine.py#L810-L843), [scalable_pivot_controller.py#L2378-L2550](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py#L2378-L2550), [bounded_sparse_pivot.py#L13-L79](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/pivot_engine/pivot_engine/bounded_sparse_pivot.py#L13-L79)
   - **Action**:
     - Compile/parameterize directly against DuckDB (`execute(sql).arrow()`) for standard pivot/hierarchy queries instead of `planner.plan` + 3× `ibis.compile` + `ibis.to_pyarrow` (`133.63 ms` vs `94.59 ms` direct SQL on 1M hierarchy; `71.15 ms` vs `49.45 ms` on 1M wide).
     - Keep viewport blocks in columnar PyArrow / NumPy structures up to the JSON boundary, computing color-scale min/max via vectorized PyArrow compute on only visible slices instead of `colorScaleMs: 396.156 ms` + `flattenMs: 396.151 ms` over `20,081` Python dicts.
     - Re-evaluate sparse pivot thresholds (`MAX_ROOTS = 32`, `MAX_COLUMNS = 512`, `MAX_CELLS = 8192`) once vectorized slicing replaces Python loops.

---

### Phase 3: Frontend & Transport Velocity (Eliminate the 1.4x–28.8x Browser Gap)

1. **Zero-Network Horizontal Scroll for Wide Tables (Target: `1262 ms -> ~40 ms`)**
   - **Targets**: [useColumnVirtualizer.js#L236-L288](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useColumnVirtualizer.js#L236-L288), [useServerSideRowModel.js#L1371-L1417](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useServerSideRowModel.js#L1371-L1417)
   - **Action**:
     - When total column count is within a practical threshold (e.g., `<= 256` leaf columns — matching typical wide benchmarks), return all columns cleanly in each already-fetched row block (or cache full column schema on initial load) and stop firing `scheduleHorizontalMetrics` / `VIEWPORT_FAST_COLUMN_DEBOUNCE_MS = 96` network requests on horizontal pan.
     - Only enable horizontal window paging when leaf columns exceed the wide-page threshold, and dispatch urgent jumps immediately rather than forcing `96 ms` / `140 ms` debounce delays via `!coalesceJumpRequest`.
   - **Projected Impact**: Drops `wide/horizontal` transferred bytes from **`26,357` -> `0`**, eliminates the `96 ms` debounce + `+1.14s` JS re-commit (`1262.3 ms` -> parity with `~40–60 ms` pure DOM virtualization).

2. **Replace Per-Cell Error Boundaries & Closures with Grid-Level Handling (Target: `-50%+` Script & DOM Nodes)**
   - **Targets**: [useRenderHelpers.js#L353-L357](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useRenderHelpers.js#L353-L357), [safeCellContent.js#L18-L52](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/presentation/safeCellContent.js#L18-L52)
   - **Action**: Move `<CellErrorBoundary>` up to the row or table body boundary (only attaching cell-level safety or JSON stringification for custom/object formatters), and inline primitive cell formatting without creating per-cell closures on every render pass.
   - **Projected Impact**: Cuts per-scroll React fiber allocations and brings DOM node / LayoutObject counts and `Script` time on `hierarchy/vertical-cached` (`205.2 ms` vs `65.2 ms`) closer to AG Grid (`~60–90 ms`).

3. **Decouple `useColumnDefs`, Eliminate Reference-Check `JSON.stringify`, & Remove Forced DOM Reflows**
   - **Targets**: [useColumnDefs.js#L1798-L1896](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/hooks/useColumnDefs.js#L1798-L1896), [useRuntimeStructuralSync.js#L83-L147](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/runtime/useRuntimeStructuralSync.js#L83-L147), [useRuntimeColumnMeasurement.js#L16-L85](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/runtime/useRuntimeColumnMeasurement.js#L16-L85), [useRuntimeColumnAutoSizing.js#L27-L72](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/runtime/useRuntimeColumnAutoSizing.js#L27-L72), [main PivotWorkspace.tsx#L147](file:///C:/Users/ramzy/Downloads/serverside_pivot/serverside_pivot/src/lib/components/PivotWorkspace.tsx#L147)
   - **Action**:
     - Split volatile state (`pendingRowTransitions`, `selectedRowKeys`, `renderedOffset`, handler callbacks via refs) out of `useColumnDefs`'s 67-item dependency array so column definitions only rebuild when schema/hierarchy/format rules structurally change.
     - Replace `JSON.stringify` equality checks in `useRuntimeStructuralSync.js` (and `main` `PivotWorkspace.tsx#L147`) with shallow/reference equality on normalized immutable config objects.
     - Reuse one cached offscreen `<canvas>` context in `useRuntimeColumnAutoSizing` and skip DOM cloning/reflow (`getRenderedHeaderWidthForColumn` / `getRenderedCellWidthForColumn`) during normal viewport renders.

4. **Sweep Block Sizing & Overscan (`cacheBlockSize` / `rowOverscan`)**
   - **Targets**: [million_server.py#L151-L350](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/scripts/performance/million_server.py#L151-L350), [PivotTableBody.js#L865-L955](file:///C:/Users/ramzy/.codex/worktrees/4cc1/serverside_pivot/dash_tanstack_pivot/src/lib/components/Table/PivotTableBody.js#L865-L955)
   - **Action**: Benchmark smaller `cacheBlockSize` (e.g., `100` vs `200`) and tighter `rowOverscan` (`3–4` vs `8`) on 1M browser workloads to trade off fewer offscreen DOM rows (`Nodes: 5828 -> 10509`) against block-fetch frequency.

---

## 5. Verification Plan

1. **Python & SQL Verification**
   - Re-run paired 1M, 10M, and 100M suites (`benchmark_1m.py`, `paired_comparison_10m.py`, `benchmark_100m.py`) after Phase 1 and Phase 2 changes, asserting:
     - Warm hits (`< 5 ms`), process-cold first query (`< 350 ms` at 1M), and expand (`< 300 ms` total at 1M).
     - `1M` non-cache-hit server total beats AG Grid (`< 0.71s`), maintaining `10M` and `100M` leads (`8/11` -> `11/11`, `11/11` preserved).
2. **Browser E2E Verification**
   - Re-run the paired 1M Playwright/CDP harness after Phase 3 and confirm parity or wins in `report.json` + `browser-heap.json`, targeting:
     - `wide / horizontal / warm`: `0` bytes transferred, `< 60 ms`.
     - `hierarchy / vertical-cached / warm`: `Nodes < 6,000`, `< 70 ms`.
     - `hierarchy / expand-collapse / warm`: `< 900 ms`.
3. **Correctness Parity**
   - Run existing `pytest` backend suite + frontend unit/E2E tests verifying zero behavioral regressions in sorting, filtering, expand/collapse, formula measures, grand totals, and subtotals.
