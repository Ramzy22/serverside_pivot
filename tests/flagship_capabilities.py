"""The release contract for the three published flagship applications.

The matrix names user-visible workflows, not internal modules.  A capability
is either supported on every publishing host or intentionally unavailable;
there is no implicit "best effort" state for a flagship claim.
"""

from __future__ import annotations

from dataclasses import dataclass


HOSTS = ("dash", "react", "streamlit")


@dataclass(frozen=True)
class FlagshipCapability:
    id: str
    group: str
    workflow: str
    verification: str
    supported: bool = True
    disabled_reason: str | None = None


CAPABILITIES = (
    FlagshipCapability("initial-workspace", "Startup", "The configured pivot renders populated with the initial hierarchy, measures, and totals.", "browser"),
    FlagshipCapability("runtime-error", "Startup", "A failed runtime request is visible and the next valid request recovers the workspace.", "browser"),
    FlagshipCapability("hierarchy-expand", "Navigation", "A hierarchy row expands and collapses without losing populated measure cells.", "browser"),
    FlagshipCapability("hierarchy-expand-all", "Navigation", "Expand all and collapse all update the visible hierarchy.", "browser"),
    FlagshipCapability("vertical-virtual-scroll", "Navigation", "Vertical scroll requests and renders later row windows.", "browser"),
    FlagshipCapability("horizontal-virtual-scroll", "Navigation", "Horizontal scroll requests and renders later column windows.", "browser"),
    FlagshipCapability("saved-scroll", "Navigation", "A saved view restores the table scroll position.", "browser"),
    FlagshipCapability("layout-field-zones", "Model", "Fields can move among Rows, Columns, Filters, and Values.", "browser"),
    FlagshipCapability("layout-values", "Model", "A value can be added, removed, and assigned an aggregation.", "browser"),
    FlagshipCapability("layout-swap", "Model", "Row and column axes can be transposed.", "browser"),
    FlagshipCapability("global-filter", "Filtering", "Global search filters the displayed pivot and clears cleanly.", "browser"),
    FlagshipCapability("header-filter", "Filtering", "A context/header filter changes the active data request and can be cleared.", "browser"),
    FlagshipCapability("sorting", "Filtering", "Sorting changes the displayed order and reports its direction.", "browser"),
    FlagshipCapability("filter-options", "Filtering", "Server filter options load and are usable in the filter UI.", "browser"),
    FlagshipCapability("totals", "Table", "Row totals, column totals, subtotals, and grand totals render according to controls.", "browser"),
    FlagshipCapability("row-numbers", "Table", "Row numbers can be toggled.", "browser"),
    FlagshipCapability("layout-modes", "Table", "Hierarchy, outline, and tabular layouts can be selected.", "browser"),
    FlagshipCapability("density", "Table", "Row density cycles without corrupting rows.", "browser"),
    FlagshipCapability("column-sizing", "Table", "Visible columns can be resized and auto-sized.", "browser"),
    FlagshipCapability("column-pinning", "Table", "Columns can be pinned and unpinned.", "browser"),
    FlagshipCapability("cell-selection", "Selection", "A cell range can be selected and cleared.", "browser"),
    FlagshipCapability("copy-selection", "Selection", "Copy Selection and Copy Selection with Headers write the expected tabular data.", "browser"),
    FlagshipCapability("copy-current-table", "Selection", "Copy Current Table preserves the active expansion, sorting, filters, layout, and visible table shape.", "browser"),
    FlagshipCapability("copy-all-records", "Selection", "Copy All Records includes matching rows that are currently collapsed in the table.", "browser"),
    FlagshipCapability("keyboard-copy", "Selection", "Ctrl/Cmd+C copies the active selection without stealing editable input shortcuts.", "browser"),
    FlagshipCapability("export-csv", "Delivery", "Export CSV downloads the filtered pivot with its visible column order, Unicode text, and totals.", "browser"),
    FlagshipCapability("export-xlsx", "Delivery", "Export XLSX downloads a non-empty, valid workbook containing the rendered pivot.", "browser"),
    FlagshipCapability("large-delivery", "Delivery", "Large copy/export presents progress, supports cancel, and delivers a selected part.", "browser"),
    FlagshipCapability("number-format", "Presentation", "Number-format and decimal controls update selected or default values.", "browser"),
    FlagshipCapability("cell-format", "Presentation", "The cell-format dialog applies and closes cleanly.", "browser"),
    FlagshipCapability("color-scales", "Presentation", "Color scales and data bars update selected columns.", "browser"),
    FlagshipCapability("themes", "Presentation", "Theme selection and theme overrides update the workspace.", "browser"),
    FlagshipCapability("zoom-immersive", "Presentation", "Zoom and immersive mode preserve an interactive table.", "browser"),
    FlagshipCapability("formula-columns", "Advanced sidebar", "Formula creation/removal changes the available/displayed measures.", "browser"),
    FlagshipCapability("custom-dimensions", "Advanced sidebar", "A supported custom dimension/category can be created and removed.", "browser"),
    FlagshipCapability("report-mode", "Saved work", "Report mode, report definition, and saved-report selection work together.", "browser"),
    FlagshipCapability("persistence", "Saved work", "A saved local view survives reload without emitting host transport errors.", "browser"),
    FlagshipCapability("undo-redo", "Saved work", "Supported layout changes round-trip through undo and redo.", "browser"),
    FlagshipCapability("range-chart", "Analysis", "A selected range can open a chart.", "browser"),
    FlagshipCapability("chart-pane", "Analysis", "A chart pane opens, resizes, docks/floats, and closes while the grid remains usable.", "browser"),
    FlagshipCapability("chart-export", "Analysis", "Chart export produces its selected artifact.", "browser"),
    FlagshipCapability("streamlit-no-rerun", "Host integration", "Streamlit scrolling and chart-pane resizing stay inside the component and do not rerun the script.", "browser"),
    FlagshipCapability("react-direct-transport", "Host integration", "React uses V1 HTTP envelopes and direct payload fetches without Dash callbacks.", "browser"),
    FlagshipCapability("dash-callback-transport", "Host integration", "Dash callbacks receive and apply the same runtime semantics.", "browser"),
    FlagshipCapability("editing", "Explicitly unavailable", "Cell and row editing are not a public flagship feature.", "static", supported=False, disabled_reason="Editing remains outside the public component contract."),
)


def capability_ids() -> set[str]:
    return {capability.id for capability in CAPABILITIES}


def markdown_matrix() -> str:
    rows = [
        "# Flagship functionality certification",
        "",
        "Every supported workflow must pass in Dash, React, and Streamlit.",
        "",
        "| Group | Capability | Verification | Dash | React | Streamlit |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for capability in CAPABILITIES:
        status = "required" if capability.supported else "intentionally unavailable"
        rows.append(
            f"| {capability.group} | {capability.workflow} | {capability.verification} | {status} | {status} | {status} |"
        )
    return "\n".join(rows) + "\n"
