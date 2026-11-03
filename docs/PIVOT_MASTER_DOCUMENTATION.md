# DashTanstackPivot: Advanced Capabilities & Technical Reference

This document provides a comprehensive and verified technical reference for the `DashTanstackPivot` component. Unlike earlier design documents, this reflects the **actual implemented state** of the codebase as of April 2026.

## 1. Core Analytical Engine (The "Pivot" Mode)
The component operates as a high-performance, server-side data processing engine powered by DuckDB and Ibis.

*   **Pivoting & Hierarchy:** Supports multidimensional pivoting with unlimited Row and Column fields.
*   **Four Distinct View Modes:**
    *   `pivot`: Standard multidimensional analysis with grouping and column pivoting.
    *   `tree`: Optimized hierarchical view for balanced or unbalanced tree data.
    *   `table`: Flat tabular mode (equivalent to "Tabular" or "Outline" modes in other tools).
    *   `report`: High-fidelity reporting mode with specific layout constraints.
*   **Virtualization & Performance:**
    *   **Server-Side Cached Row Model:** Implementation of a robust block-based caching system with LRU eviction.
    *   **Concurrent Execution:** DuckDB connection pooling allows for non-blocking concurrent queries.
    *   **Intelligent Prefetching:** Velocity-based scroll analysis predicts user movement and pre-loads data blocks.

## 2. Advanced Visualizations
Integrated visualization capabilities beyond standard grid display.

*   **Pivot Charts (ECharts Powered):**
    *   **Standard Types:** Bar (Stacked/Grouped), Line, Area, Pie, Donut, Scatter, Heatmap, Bubble, Radar.
    *   **Specialized Types:** Waterfall, Combo Charts (Multi-layer), Range Charts.
    *   **3D Visualizations:** 3D Bar, 3D Line, 3D Scatter.
    *   **Hierarchy Charts:** Sankey, Sunburst, Icicle, Treemap (fully integrated with the pivot hierarchy).
    *   **Trend Analysis:** Built-in compute engines for Moving Averages and Linear Trendlines.
*   **In-Cell Sparklines:** 
    *   Rendered directly in the grid using `array_agg` data.
    *   Supports multiple types (Line, Bar, Area) and display modes (Trend vs. Value).

## 3. Data Interaction & Editing
Enterprise-grade interaction patterns for operational and analytical workflows.

*   **Full CRUD Editing:**
    *   Server-side cell updates with comprehensive transaction management.
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
Powerful server-side calculation engine using Excel-like syntax.

*   **Formula Columns:** Create new metrics using the `[Column Name]` syntax.
*   **Pivot Window Functions:**
    *   `percent_of_row`, `percent_of_col`, `percent_of_grand_total`.
*   **Visual Totals:** Optional recalculation of parent groups based on post-filter child values, ensuring data integrity during analysis.

## 5. Known Implementation Quirks & Bugs
*   **Column Virtualization:** Currently disabled due to layout synchronization issues. Performance may degrade with >200 visible columns.
*   **Drag-to-Pin UI:** UI handles for pinning columns via drag-and-drop are missing; pinning must be performed via the context menu or sidebar.
*   **Header Sync:** Minimal misalignment (1-2px) can occur in pinned regions during rapid horizontal scrolling or extreme resizing.
*   **Natural Sort Edge Case:** Default sorting on financial tenors (e.g., 1M, 3M, 1Y) may occasionally revert to lexicographical order if the `sortKeyField` is not explicitly mapped in the `sortOptions`.

## 6. Real-time & Connectivity
*   **CDC Integration:** Native `PivotCDCManager` for incremental data updates.
*   **Streaming Processor:** Support for live-updating aggregations via `StreamAggregationProcessor`.
*   **Export:** High-speed XLSX and CSV export using SheetJS, preserving basic data types and hierarchical structure.
