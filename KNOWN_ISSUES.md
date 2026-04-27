# Known Issues and Feature Gaps

This document tracks identified bugs, limitations, and missing features in the `DashTanstackPivot` project.

## 1. High Priority / Critical Bugs
*   **Column Virtualization Disabled:** Column virtualization is currently disabled in the frontend due to layout and synchronization bugs. This may cause performance degradation when dealing with a very high number of columns (e.g., hundreds of pivot columns).
*   **Header-Cell Alignment (Pinned Columns):** There are known issues where headers and cells might become misaligned when columns are pinned, especially after resizing.
*   **Sorting Logic Gaps:** 
    *   Default sorting sometimes falls back to lexicographical order instead of honoring custom `sortKeyField` (e.g., in Financial Tenors like '1M', '10M').
    *   Resetting sort (clearing sort state) occasionally fails to return the grid to the original data order.

## 2. Feature Gaps (vs. Enterprise Standards)
*   **Layout Modes:** Missing "Tabular" (Flat) and "Outline" (Excel-style) modes. Currently, only "Hierarchy" mode is fully supported.
*   **UI Drag-and-Drop Limitations:** 
    *   Cannot drag column headers directly to move or pin them; must use the sidebar or column menu.
    *   No "Drag-to-Pin" functionality in the header area.
*   **Tree Data (Self-Referencing):** Native support for `ID` / `ParentID` structures is missing; the engine currently expects a path-based hierarchy or distinct row fields.
*   **Clipboard:** "Paste" functionality for bulk cell updates is not implemented.
*   **Auto-Size Columns:** Missing the ability to auto-size a column by double-clicking the resize handle.
*   **Theme Switcher:** No built-in component for switching between different visual themes (e.g., Dark, Light, Bloomberg).

## 3. Performance & Stability Gaps
*   **Master/Detail Grids:** Not yet implemented.
*   **Advanced XLSX Styling:** Export to Excel supports basic styles and types but lacks advanced formatting parity with AG Grid Enterprise.
*   **Aggregation Summary in Status Bar:** The status bar currently only shows row/column counts and does not provide automatic aggregations (Sum, Avg) for selected cell ranges.

## 4. Documentation Gaps
*   **API Reference:** Detailed documentation for all 400+ lines of Python props is still in progress.
*   **Custom Backend Guide:** Instructions on how to implement custom Ibis/SQL backends are missing.
