# DashTanstackPivot Capabilities Documentation

This document provides a comprehensive overview of the features and capabilities of the `DashTanstackPivot` component and its underlying `ScalablePivotController` engine.

## 1. Core Grid & Pivot Engine
The heart of the component is a high-performance pivot engine powered by DuckDB and Ibis.

*   **Multidimensional Pivoting:** Supports full pivoting with an unlimited number of Row fields and Column fields.
*   **Hierarchical Tree Data:** Automatically handles path-based hierarchies. Rows can be expanded/collapsed at any level.
*   **Flexible Aggregations:** 
    *   Standard: `sum`, `avg`, `min`, `max`, `count`.
    *   Advanced: `weighted_avg` (wavg), `array_agg` (for lists/sparklines), `percentile`.
*   **Multi-Column Sorting:** Support for sorting by multiple columns simultaneously, including server-side sort logic.
*   **Advanced Filtering:**
    *   **Multi-condition UI:** Support for complex AND/OR filter logic.
    *   **Measure Filtering:** Filtering based on aggregated values (HAVING clause simulation).
    *   **Visual Totals:** When filters are applied, parent totals can be automatically recalculated based only on the visible/filtered leaf nodes.

## 2. Layout & UI Features
The frontend is built on TanStack Table v8, providing a modern and responsive experience.

*   **Zone-based Sidebar:** Intuitive drag-and-drop interface for managing Row, Column, and Value (Measure) zones.
*   **Column Management:**
    *   **Pinning:** Support for pinning columns to the Left or Right.
    *   **Resizing:** Interactive drag-to-resize handles.
    *   **Visibility:** Toggle visibility of individual columns.
*   **Expansion States:** Persistent row expansion states, even after data refreshes or filter changes.
*   **Grand Totals:** Configurable grand total positions (Top or Bottom) for both rows and columns.
*   **Detail Modes:** Multiple ways to view underlying data for a cell:
    *   `inline`: Expand details directly within the table.
    *   `sidepanel`: Open a detail panel on the right.
    *   `drawer`: Slide up a bottom drawer.
*   **Conditional Formatting:** Rules-based cell styling (colors, icons, etc.) based on data values.

## 3. Advanced Data & Calculations
Beyond simple aggregation, the engine supports complex analytical workflows.

*   **Formula Columns (Calculated Fields):**
    *   Support for Excel-like formulas using `[Field Name]` syntax.
    *   Executed server-side for performance on large datasets.
*   **Window Functions:**
    *   `percent_of_row`, `percent_of_col`, `percent_of_grand_total`.
*   **Sparklines:** In-cell trend visualizations using `array_agg` data.
*   **Drill-Through:** Efficient retrieval of the raw underlying records for any aggregated cell.
*   **Cell Editing:** 
    *   Support for server-side cell updates with transaction management.
    *   Validation rules for numeric inputs and ranges.

## 4. Performance & Scalability
Designed to handle millions of rows without locking the UI.

*   **Server-Side Cached Row Model:** 
    *   Block-based caching with LRU (Least Recently Used) eviction.
    *   Velocity-based scroll prefetching to minimize loading flickers.
*   **DuckDB Backend:** Uses vectorized execution for lightning-fast aggregations.
*   **Concurrent Execution:** Connection pooling and multi-threaded query execution (no global lock).
*   **Intelligent Prefetching:** Analyzes user scroll patterns to pre-load data before it's needed.
*   **Progressive Loading:** Large hierarchies are loaded incrementally to maintain responsiveness.

## 5. Real-time & Enterprise Features
*   **CDC (Change Data Capture):** Support for tracking and reflecting data changes incrementally.
*   **Streaming Support:** Integration with streaming data sources for live-updating pivot tables.
*   **Export:** High-performance export to CSV and Excel (XLSX).
*   **Persistence:** Save and restore pivot configurations (view states) using Dash persistence.
*   **Theme Support:** Material-inspired design with support for custom styling via CSS variables.
