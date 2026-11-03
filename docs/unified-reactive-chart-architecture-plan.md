# Unified Reactive Chart & Streaming Architecture Plan

## Status & Sequencing (Post-207/207 Milestone)

```text
    YOU ARE HERE: 207/207 Mapped
                 │
                 ▼
    STEP 1 — PATH 1: RELEASE (Certification & Merge)
    • Run `certify_flagship.py`
    • Fast-forward to master & stamp formal 1.0 Candidate
    • Clean up obsolete root docs / worktrees
                 │
                 ▼
    STEP 2 — PATH 2: UI/UX + UNIFIED ECHARTS 6 ENGINE
    • Modern Drag-and-Drop 4-Box & Fluid Context Menus
    • Commercial-grade theme presets
    • Unified "1-Click Range-to-ECharts" & Scoped Hypercube View:
      1. Consolidate 100% of 2D/3D charts onto ECharts 6 Canvas/WebGL (`replaceMerge` + `lazyUpdate`)
      2. Unify `selection` & `pivot` chart binding into one Scoped Hypercube View
         (instant L1 cache preview during drag + server `intent: 'chart'` subscription once settled)
                 │
                 ▼
    STEP 3 — PATH 3: ADVANCED SUPERPOWERS ENGINE
    • WebSocket Delta Tick Engine:
      `exact_state.py` CDC ticks push dirty slices to both Grid and active Chart Panes (`chartPaneDataById`)
    • DuckDB-WASM In-Browser (Zero-Server Mode):
      In-browser Mosaic Coordinator speaking the exact same V1 Runtime Contract
    • Proportional Write-Back (EPM)
```

---

## 1. Architectural Synthesis: Mosaic + Perspective + ECharts 6

Rather than copying AG Grid's DOM-coupled range charts (which break across unloaded SSRM blocks and throttle under streaming) or maintaining two separate chart data models (`selection` vs. `pivot`) and two separate renderers (2,800+ lines of custom React SVG vs. ECharts), `serverside_pivot` will unify onto a single pipeline combining three industry-proven patterns:

1. **UW IDL Mosaic Coordinator Pattern (Backend Query & Selection Sharing):**
   * Cell selections, chart category clicks, and brush ranges are represented as **semantic coordinate predicates** (`{ rowPaths, columnIds }` / dimension filters) against the shared DuckDB / [`materialized_hierarchy_manager.py`](../dash_tanstack_pivot/pivot_engine/pivot_engine/materialized_hierarchy_manager.py) rollup state.
   * Structural and CDC updates can piggyback dirty `chart_data` payloads for active `pane_id` subscriptions alongside grid viewport responses.
2. **FINOS Perspective Viewport Decoupling (Zero-Jank Streaming):**
   * Active chart panes (`chartPaneDataById`) are first-class viewport projections of the pivot engine, completely decoupled from the grid's vertical/horizontal scroll offset (`effectiveCenterRows`).
   * Reversible CDC state in [`exact_state.py`](../dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/exact_state.py) and [`streaming_processor.py`](../dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py) updates aggregations out-of-thread and emits dirty slices to both the grid and subscribed charts.
3. **Apache ECharts 6 Unified Canvas/WebGL Engine:**
   * Standardize all 2D and 3D charts onto the existing `echarts ^6.1.0` + `echarts-gl ^2.1.0` packages in [`package.json`](../dash_tanstack_pivot/package.json).
   * Replace `notMerge={true}` in [`ECharts2dRenderer.js`](../dash_tanstack_pivot/src/lib/components/Charts/ECharts2dRenderer.js) with incremental updates (`lazyUpdate: true`, `replaceMerge: ['series', 'xAxis', 'yAxis', 'visualMap']`, `universalTransition: true`).

---

## 2. Current Codebase Audit (What Exists vs. What Changes in Path 2 & 3)

| Subsystem | Current Implementation | Target Unified Implementation (Path 2 & Path 3) |
| :--- | :--- | :--- |
| **Chart Transport** | [`useRuntimeChartRequestDispatch.js`](../dash_tanstack_pivot/src/lib/runtime/useRuntimeChartRequestDispatch.js) & [`router.py`](../dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py) (`trigger_kind == "chart"`) | Add `scope: { rowPaths, columnIds }` predicate support for selection charts; co-push dirty `chart_data` on CDC ticks in Path 3. |
| **Chart Data Models** | Split between `buildSelectionChartModel` (DOM-bound) and `buildPivotChartModel` in [`useRuntimeChartModels.js`](../dash_tanstack_pivot/src/lib/runtime/useRuntimeChartModels.js) | Single **Scoped Hypercube Model**: resolves synchronously from client L1 cache (`loadedRows`) while dragging selection, then binds to server `intent: 'chart'` once settled. |
| **2D/3D Renderers** | ~2,800 lines of custom SVG in [`ChartRenderers.js`](../dash_tanstack_pivot/src/lib/components/Charts/ChartRenderers.js) + ECharts for 6 chart types (`range`, `boxplot`, `nightingale`, `bar3d`, `line3d`, `scatter3d`) | Route all chart types (`bar`, `line`, `area`, `combo`, `pie`, `donut`, `sunburst`, `sankey`, `treemap`/`icicle`, `heatmap`, `boxplot`, `range`, `nightingale`, 3D) through lazy-loaded ECharts 6 Canvas/WebGL. |
| **Streaming Update Mode** | `<ReactEChartsCore notMerge={true} />` in [`ECharts2dRenderer.js`](../dash_tanstack_pivot/src/lib/components/Charts/ECharts2dRenderer.js) | Direct `echarts.init()` hook using `setOption(opt, { lazyUpdate: true, replaceMerge: ['series', 'xAxis', 'yAxis'] })` with `animationDurationUpdate: 150`. |

---

## 3. Execution Checklist by Path

### Path 1: Release Certification & Merge (Immediate)
- [ ] Run `python scripts/certify_flagship.py` and verify 207/207 green certification.
- [ ] Fast-forward / merge clean baseline to `master` and tag formal 1.0 Release Candidate.
- [ ] Clean up obsolete root markdown ledger files and temporary `.worktrees`.

### Path 2: UI/UX Polish & Unified ECharts 6 Engine
- [ ] Upgrade [`ECharts2dRenderer.js`](../dash_tanstack_pivot/src/lib/components/Charts/ECharts2dRenderer.js) to register `LineChart`, `PieChart`, `SunburstChart`, `SankeyChart`, `TreemapChart`, `HeatmapChart`, and `CustomChart` alongside `BarChart` and `BoxplotChart`.
- [ ] Switch ECharts update lifecycle from `notMerge={true}` to `{ lazyUpdate: true, replaceMerge: ['series', 'xAxis', 'yAxis', 'visualMap'] }` with `universalTransition: true`.
- [ ] Unify `buildSelectionChartModel` into `buildPivotChartModel` in [`useRuntimeChartModels.js`](../dash_tanstack_pivot/src/lib/runtime/useRuntimeChartModels.js) using semantic `{ rowPaths, columnIds }` coordinates (0ms L1 cache preview on drag + server `intent: 'chart'` subscription on mouse-up).
- [ ] Retire legacy custom SVG layout builders in [`ChartRenderers.js`](../dash_tanstack_pivot/src/lib/components/Charts/ChartRenderers.js) and [`chartModelBuilders.js`](../dash_tanstack_pivot/src/lib/utils/chartModelBuilders.js).

### Path 3: Superpowers Engine (WebSocket Delta Tick + DuckDB-WASM)
- [ ] Connect [`exact_state.py`](../dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/exact_state.py) / [`streaming_processor.py`](../dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py) CDC tick batches to active session `chart` subscriptions so both grid viewports and `chartPaneDataById` receive live delta pushes.
- [ ] Implement DuckDB-WASM in-browser coordinator implementing the V1 runtime contract for zero-server mode.
- [ ] Implement Proportional Write-Back (EPM).
