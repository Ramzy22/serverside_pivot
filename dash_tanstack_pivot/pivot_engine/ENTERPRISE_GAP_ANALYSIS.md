# Enterprise Gap Analysis: DashTanstackPivot vs AG Grid Enterprise

This document tracks the feature parity gap between the current `DashTanstackPivot` implementation and AG Grid Enterprise, serving as a roadmap for development.

## 1. Core Features (Grid & Pivot)

| Feature | AG Grid Enterprise | DashTanstackPivot (Current) | Status | Gap / Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Pivoting** | Full support (Client & Server) | Full support (Client & Server) | ✅ | Parity achieved. |
| **Row Grouping** | Tree Data, Row Grouping | Tree Data (Hierarchy) | ⚠️ | Need "Flat" grouping mode (Tabular view). |
| **Aggregations** | Sum, Min, Max, Avg, Count, Custom | Sum, Min, Max, Avg, Count, Ratio, Window | ✅ | Advanced window functions supported. |
| **Filtering** | Text, Number, Date, Set, Multi-condition | Multi-condition (AND/OR), Text, Number | ✅ | Date & Set filters (checkbox list) missing. |
| **Sorting** | Multi-column, Custom Comparator | Multi-column, Server-side | ✅ | - |
| **Column Pinning** | Left, Right (Drag & Menu) | Left, Right (Menu) | ⚠️ | Drag-to-pin not implemented. |
| **Virtualization** | Row & Column (High Perf) | Row Only (Column disabled) | ⚠️ | Column virtualization disabled due to layout bugs. |
| **Selection** | Row, Range, Cell, Multi-region | Cell, Range, Multi-region | ✅ | - |
| **Clipboard** | Copy, Copy with Headers, Paste | Copy, Copy with Headers | ⚠️ | Paste (editing) not implemented. |

## 2. Layout & UX

| Feature | AG Grid Enterprise | DashTanstackPivot (Current) | Status | Gap / Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Themes** | Material, Alpine, Balham, Custom | Custom Material-ish | ⚠️ | No built-in theme switcher. |
| **Sidebar** | Columns, Filters, config | Columns, Filters, config | ✅ | - |
| **Status Bar** | Aggregations, Count, etc. | Row/Col Count | ⚠️ | Selection aggregation summary missing. |
| **Context Menu** | Fully customizable | Customizable (Code) | ✅ | - |
| **Column Resizing**| Drag handle, Auto-size | Drag handle | ⚠️ | Auto-size on double click missing. |
| **Column Moving** | Drag & Drop headers | Sidebar Drag & Drop only | ⚠️ | Dragging headers directly not implemented. |
| **View Modes** | Pivot, Table, Tree | Hierarchy Only | ❌ | **Next Priority:** Add Tabular/Outline modes. |

## 3. Advanced Enterprise Features

| Feature | AG Grid Enterprise | DashTanstackPivot (Current) | Status | Gap / Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Master/Detail** | Nested grids | - | ❌ | Not started. |
| **Tree Data** | Self-referencing hierarchy | Path-based hierarchy | ⚠️ | Self-referencing (ID/ParentID) not native. |
| **Sparklines** | In-cell charts | - | ❌ | Not started. |
| **Cell Editing** | Text, Select, Popup editors | - | ❌ | Read-only currently. |
| **Excel Export** | Native XLSX (Styles, types) | XLSX (Basic styles) | ⚠️ | Advanced styling/types partial. |
| **Charts** | Integrated Charting | - | ❌ | Not started. |
| **Server-Side Row Model** | Infinite Scroll, Partial Store | Virtual Pagination | ⚠️ | True "Infinite" scroll partial. |

## 4. Immediate Roadmap (User Requested)

1.  **Layout Modes:** Implement "Tabular" (Flat) and "Outline" (Excel-style) switch.
2.  **Group Pinning:** Pinning a header group should pin all children.
3.  **Expansion Fix:** Ensure row expansion click works reliably.
4.  **Header Sync:** Ensure headers align with cells when pinned.