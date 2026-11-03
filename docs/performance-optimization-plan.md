# Performance Optimization Plan: Dash TanStack Pivot

This plan covers the three broader performance items that were confirmed or partially confirmed in the audit:

- PERF-1: too many fresh inline style objects, especially in render hot paths.
- PERF-2: `PivotConfigContext` broadcasts unrelated updates to all consumers.
- PERF-4: header leaf mapping still does repeated column-tree work and object allocation.

The goal is not a cosmetic cleanup. The goal is to reduce filter typing latency, scroll-frame work, and avoidable React prop churn without changing Dash prop synchronization, sticky behavior, row spanning, multi-sort, or server-side viewport scheduling.

## Architecture Corrections

The previous draft was directionally right, but these points must be corrected before implementation:

1. Do not add `why-did-you-render` as a default dependency. Use local dev-only counters and the existing profiler path first. WDYR can be an optional local experiment only if it is already available or explicitly approved.
2. Do not design the header work around binary search by default. `PivotTableBody` already receives `virtualCenterCols` and a visible index window. The source problem is repeated `getLeafColumns()` tree walks plus per-header object allocation, not viewport discovery.
3. Do not inject CSS variables from `PivotConfigProvider`. Config state and theme/root styling should stay separate. Put CSS variables on the pivot root element or in the theme layer.
4. Do not promise `React.memo` wrappers around `PivotRow`, `PivotCell`, and `PivotHeader` until those boundaries actually exist. First extract stable components or memoize existing render helpers where props are stable.
5. Do not target every `style={{...}}` at once. There are about 844 inline style literals in `src/lib`; only the hot-path render styles should be handled first.

## Phase 0: Baseline and Guardrails

Target files:

- `dash_tanstack_pivot/src/lib/utils/pivotProfiler.js`
- `dash_tanstack_pivot/src/lib/components/Table/PivotTableBody.js`
- `dash_tanstack_pivot/src/lib/components/PivotAppBar.js`
- `dash_tanstack_pivot/src/lib/components/Sidebar/SidebarPanel.js`

Tasks:

1. Add dev-only render counters without dependencies.
   - Count renders for `PivotTableBody`, `PivotAppBar`, `SidebarPanel`, and chart panel components.
   - Keep counters behind the existing profiling/debug flag.
   - Do not emit console noise in production builds.

2. Add profiling marks for the specific bottlenecks.
   - Filter edit start -> committed filter update.
   - Committed filter update -> table body render complete.
   - Header leaf map build duration.
   - Center header render plan build duration.
   - Vertical and horizontal scroll frame scheduling.

3. Add source-level regression checks.
   - Track hot-path `style={{` occurrences separately from cold UI files.
   - Track context consumer migrations so new code does not keep using broad `usePivotConfig()` where a focused hook exists.

Acceptance:

- Profiling can be enabled without adding a dependency.
- Baseline numbers are captured for 1k x 50, 10k+ server-side, and 500+ pivot-column scenarios.
- No production behavior changes.

## Phase 1: Header Leaf Map Cache

Target:

- `dash_tanstack_pivot/src/lib/components/Table/PivotTableBody.js`

Current issue:

`headerLeafPairsMap` is memoized, but inside that memo it still calls `header.column.getLeafColumns()` per header. With large nested column trees, that repeats tree walks and creates many short-lived `{ idx }` objects.

Plan:

1. Extract `useHeaderLeafIndexMap`.
   - Inputs: `centerHeaderGroups`, `centerColIndexMap`, `centerCols`.
   - Output: a stable map keyed by `header.id`.

2. Build a leaf cache once per column structure.
   - Walk headers/columns once and cache leaf IDs per column ID.
   - Use the existing `centerColIndexMap` to convert leaf IDs into center indices.
   - Avoid calling `getLeafColumns()` repeatedly for the same column.

3. Compact the render metadata.
   - Replace `pairs: [{ idx }]` with either `indices: number[]` or a typed-like compact array.
   - Store `minIdx` and `maxIdx` for fast visible-range rejection.

4. Keep viewport logic index-based.
   - Use `visibleCenterRange.start/end` from `virtualCenterCols`.
   - Only consider prefix sums or binary search later if header width calculation becomes the next measured hotspot.

Acceptance:

- Header map construction is O(headers + leaves) for a given column structure.
- Center header render plan still supports partially visible group headers.
- No behavior change for pinned columns, group headers, column totals, or row-spanning.

## Phase 2: Context Partitioning

Target:

- `dash_tanstack_pivot/src/lib/contexts/PivotConfigContext.js`
- Consumers: `PivotTableBody`, `PivotAppBar`, `SidebarPanel`

Current issue:

`PivotConfigContext` provides one memoized object containing filters, report config, display options, mode state, and setters. When `filters` changes, all consumers of `usePivotConfig()` re-render even if they do not use filters.

Plan:

1. Split context into focused providers.
   - `PivotFiltersContext`: `filters`, `setFilters`.
   - `PivotReportContext`: `reportDef`, `setReportDef`, `savedReports`, `setSavedReports`, `activeReportId`, `setActiveReportId`.
   - `PivotDisplayOptionsContext`: `showFloatingFilters`, `setShowFloatingFilters`, `stickyHeaders`, `setStickyHeaders`, `showColTotals`, `setShowColTotals`, `showRowTotals`, `setShowRowTotals`, `showRowNumbers`, `setShowRowNumbers`, `numberGroupSeparator`, `setNumberGroupSeparator`.
   - `PivotModeContext`: `pivotMode`, `setPivotMode`, `viewMode`.

2. Keep backward compatibility during migration.
   - `PivotConfigProvider` should compose the new providers.
   - `usePivotConfig()` can remain temporarily, but new code should use focused hooks.
   - Remove or shrink `usePivotConfig()` only after consumers are migrated.

3. Migrate consumers in dependency order.
   - `PivotTableBody` should use only filter and display/mode hooks needed by table rendering.
   - `SidebarPanel` should use filter/report/mode hooks.
   - `PivotAppBar` can be migrated last because it legitimately consumes many controls.

4. Add a draft-state pattern for filter inputs.
   - Sidebar filter typing updates local draft state.
   - Commit to `PivotFiltersContext` on Enter, blur, explicit apply, or a measured debounce.
   - Floating header filters in `PivotTableBody` need a separate decision: either preserve immediate filtering or introduce the same draft/commit behavior. Do not silently change UX.

Acceptance:

- A sidebar filter keystroke does not re-render consumers that do not subscribe to `PivotFiltersContext`.
- Dash-facing `filters` prop synchronization stays unchanged after commit.
- Existing report, totals, filter, and view-mode behavior is unchanged.

## Phase 3: Hot-Path Style Strategy

Primary targets:

- `dash_tanstack_pivot/src/lib/hooks/useRenderHelpers.js`
- `dash_tanstack_pivot/src/lib/hooks/useColumnDefs.js`
- `dash_tanstack_pivot/src/lib/components/Table/PivotTableBody.js`

Current issue:

Inline style objects are not all equally bad. The expensive ones are cell, header, row, and section styles created during scroll/render. Cold UI inline styles in sidebar/charts are lower priority.

Plan:

1. Define root CSS variables in the root/theme layer.
   - Put theme variables on the pivot root element near the existing root style and loading CSS variables.
   - Keep `PivotConfigProvider` free of DOM/style responsibility.

2. Create stable class names for stable visual states.
   - Examples: selected, fill selected, edited, total row, grand total, hierarchy cell, row-spanned cell, loading skeleton, sticky boundary.
   - Add a tiny local `cx()` helper if the repo does not already have one.

3. Keep only truly dynamic inline styles.
   - Column widths.
   - Virtual row transform/top/height.
   - Sticky left/right offsets and z-index where they depend on layout.
   - Row-span height.
   - Per-cell conditional format values only when they cannot be represented with CSS variables.

4. Convert hot-path object literals first.
   - Start with `renderCell` and `renderHeaderCell` in `useRenderHelpers`.
   - Then handle `useColumnDefs` cell wrappers.
   - Then handle row/section wrappers in `PivotTableBody`.

5. Use CSS variables for dynamic colors when practical.
   - Prefer `className` plus `style={{ '--pvt-cell-bg': value }}` only when the variable value is truly dynamic.
   - Avoid replacing one large inline style object with another large CSS-variable object.

Acceptance:

- Hot-path style object count is reduced materially; the exact target should be measured against the Phase 0 baseline.
- Sticky columns, row spanning, selected cells, edited cells, conditional formatting, and loading skeletons remain visually identical.
- No broad sidebar/chart style cleanup is included in this phase.

## Phase 4: Memoization Boundaries

Targets:

- `PivotTableBody` render helpers and extracted table subcomponents.

Plan:

1. Extract real component boundaries before memoizing.
   - Candidate boundaries: center header row, row section, pinned section, virtual row, skeleton row section.
   - Do not memoize anonymous render functions without stable props.

2. Stabilize props first.
   - Event handlers should be `useCallback` only when they are passed to memoized children.
   - Style/class props should come from Phase 3 stable classes or memoized objects.
   - Avoid passing broad context-derived objects into memoized children.

3. Add targeted `React.memo`.
   - Memoize only after profiler evidence shows a repeated unnecessary render.
   - Add custom comparison only for small, obvious prop sets.

Acceptance:

- Memoized components skip renders during scroll and filter typing when their inputs do not change.
- No custom comparator hides legitimate visual updates.

## Phase 5: Cold UI Cleanup

Secondary targets by current inline-style count:

- `SidebarPanel.js`: about 180 inline style literals.
- `PivotCharts.js`: about 175 inline style literals.
- `PivotAppBar.js`: about 139 inline style literals.

Plan:

1. Move repeated layout styles into local classes.
2. Keep one-off cold UI styles when converting them would add noise without measurable performance benefit.
3. Avoid changing chart rendering behavior or sidebar layout semantics during the hot-path performance work.

Acceptance:

- No regressions in sidebar filter editing, sparkline configuration, chart pane behavior, or app bar controls.
- Cold UI cleanup does not block the Phase 1-4 performance work.

## Verification

Run after each phase:

```powershell
python -m pytest tests/test_frontend_contract.py -q
python -m pytest tests/test_runtime_service.py tests/test_dash_runtime_callbacks.py -q
npm run build:js
npm run build:py
git diff --check
```

Performance checks:

1. Filter typing:
   - Sidebar draft typing should not commit global filters until the chosen commit point.
   - After commit, only filter subscribers should re-render because of filter state.

2. Header rendering:
   - Header leaf map build time should drop on 500+ and 1,000+ column cases.
   - Center header render plan should not rebuild when only unrelated context slices change.

3. Scroll rendering:
   - Vertical and horizontal scroll should show fewer hot-path allocations.
   - Row spanning and sticky columns must remain correct during scroll.

4. Bundle/build:
   - `npm run build:js` can keep existing bundle-size warnings, but should add no new build errors.
   - `npm run build:py` can keep existing Dash/react-docgen warnings, but should exit 0.

## Explicit Non-Goals

- No scheduler rewrite in this performance pass.
- No new runtime dependencies.
- No full visual redesign.
- No removal of necessary dynamic inline styles for layout-critical values.
- No context API break until all internal consumers have migrated.
