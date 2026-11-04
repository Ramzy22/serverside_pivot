# serverside-pivot: Architecture & Technical Reference

This document provides a comprehensive technical reference for the `serverside-pivot` package across its tri-host presentation targets (**Plotly Dash**, **Standalone React**, and **Streamlit**).

## 1. Core Analytical Engine (The Pivot Architecture)
The component operates as a high-performance, server-side data processing engine powered by DuckDB and Ibis.

*   **Pivoting & Hierarchy:** Supports multidimensional pivoting with unlimited Row and Column fields with level-based $\mathcal{O}(D)$ query batching.
*   **Three First-Class View Modes & Report Layout:**
    *   `pivot`: Standard multidimensional analysis with grouping and column pivoting.
    *   `tree`: Optimized hierarchical record view for balanced or unbalanced tree structures.
    *   `table`: Flat tabular record mode.
    *   *Report Mode*: Configurable report layout state (`layoutMode: 'report'`) with specialized totals and layout constraints.
*   **Virtualization & Windowing:**
    *   **Server-Side Cached Row Model:** Block-based caching system with deep byte-bounded LRU eviction and request epoch freshness gates.
    *   **Column Virtualization & Sparse Windows:** Active column virtualizer (`useColumnVirtualizer.ts`) delivering sparse horizontal windows (`col_start`/`col_end`) for wide pivots exceeding hundreds of columns.
    *   **Concurrent Execution:** DuckDB connection pooling allows for non-blocking concurrent queries without global locks.
    *   **Intelligent Prefetching:** Velocity-based scroll analysis predicts user movement and pre-loads data blocks.

## 2. Advanced Visualizations
Integrated visualization capabilities decoupled from DOM cell selections.

*   **Pivot Charts (ECharts 6 Powered):**
    *   **Standard Types:** Bar (Stacked/Grouped), Line, Area, Pie, Donut, Scatter, Heatmap, Bubble, Radar.
    *   **Specialized Types:** Waterfall, Combo Charts (Multi-layer), Range Charts (Toolbar & context selection driven).
    *   **3D Visualizations:** 3D Bar, 3D Line, 3D Scatter (via `echarts-gl`).
    *   **Hierarchy Charts:** Sankey, Sunburst, Icicle, Treemap (integrated with the active pivot tree).
    *   **Trend Analysis:** Built-in compute engines for Moving Averages and Linear Trendlines.
    *   **Docked / Floating Panes:** Flexible chart positioning (`Left`, `Right`, `Top`, `Bottom`, or floating overlay).
*   **In-Cell Sparklines:** 
    *   Rendered directly in grid cells using `array_agg` data buffers.
    *   Supports multiple types (Line, Bar, Area) and display modes (Trend vs. Value).

## 3. Data Interaction & Editing
Enterprise-grade interaction patterns for operational and analytical workflows.

*   **Full CRUD Editing:**
    *   Server-side cell updates with comprehensive transaction management and reversible CDC checkpoints.
    *   **Validation:** Native support for type-checking and numeric range rules.
    *   **Edit Side Panel:** Dedicated UI for complex multi-field edits and property updates.
*   **Advanced Filtering UI:**
    *   **DateRangeFilter:** Native date-time range selection.
    *   **NumericRangeFilter:** Slider and input-based range filtering.
    *   **MultiSelectFilter:** Set-based filtering with search capabilities.
*   **Detail Contexts:**
    *   **Drill-Through:** Efficient retrieval of underlying raw records via `DrillThroughModal`.
    *   **Contextual UI:** `DetailSidePanel`, `DetailDrawer`, and `InlineDetailPanel` for inspecting cell-level metadata.

## 4. Calculated Logic (Formula Engine)
Server-side calculation engine using Excel-like syntax.

*   **Formula Columns:** Create new metrics using the `[Column Name]` syntax.
*   **Pivot Window Functions:**
    *   `percent_of_row`, `percent_of_col`, `percent_of_grand_total`.
*   **Visual Totals:** Optional recalculation of parent groups based on post-filter child values, ensuring data integrity during analysis.

## 5. UI/UX Interaction Notes & Design Scope
*   **Column Pinning:** Pinning can be performed via the header context menu, column options popover, or the field sidebar panel.
*   **Header Sync:** Pinned and unpinned columns maintain synchronized scroll and header row heights across all three host adapters.
*   **Natural Sorting:** Supports natural and financial tenor sorting (e.g. 1M, 3M, 1Y) when mapped in `sortOptions`.

## 6. Real-time & Connectivity
*   **Tri-Host Support:** Identical `PivotWorkspace` core runs in Dash (`DashTanstackPivot`), React (`PivotWorkspace` with `HttpPivotTransport`), and Streamlit (`streamlit_pivot` zero-rerun iframe component).
*   **CDC Integration:** Native `PivotCDCManager` for incremental data updates and reversible state.
*   **Streaming Processor:** Support for live-updating aggregations via `StreamAggregationProcessor`.
*   **Export Governance:** High-speed, cancellable XLSX and CSV exports streaming from backend/worker with SheetJS client fallback.
