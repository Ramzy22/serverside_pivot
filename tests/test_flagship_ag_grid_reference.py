"""Browser observations of the pinned AG Grid Enterprise SSRM reference."""

from __future__ import annotations

import pytest
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

from tests.flagship_reference_support import reference_case


pytestmark = pytest.mark.flagship


def test_reference_startup_exposes_live_request_and_visible_groups():
    """Missing fixture groups, pivot values, or grand totals must fail startup."""
    with reference_case("initial-workspace.populated") as case:
        driver = case.driver
        cells = WebDriverWait(driver, 10).until(lambda driver: (
            visible if all(any(name in cell["text"] for cell in visible) for name in ("Éclair", "München")) else False
        ) if (visible := driver.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        trace = driver.execute_script("return window.referenceEvidence.trace()")
        scroll = driver.execute_script("""
          const viewport = document.querySelector('.ag-body-horizontal-scroll-viewport');
          viewport.scrollLeft = viewport.scrollWidth;
          return viewport.scrollLeft;
        """)
        right_cells = WebDriverWait(driver, 5).until(lambda browser: (
            visible if any(item["colId"] == "Total|ventes_€" and item["text"] == "200.00" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        case.observed = f"Initial pivot rows and grand row visible; total columns after horizontal scroll {scroll} px"
        case.parsed_output = {"initial_cells": cells, "right_cells": right_cells, "request_count": len(trace), "scroll_px": scroll}
        assert trace[-1]["request"]["filterModel"]["scope"]["values"] == ["Included"]
        assert any("Éclair" in cell["text"] for cell in cells)
        assert any("München" in cell["text"] for cell in cells)
        assert any(cell["text"] == "150.00" for cell in cells), "Q1 grand sales 150.00 is absent from displayed cells"
        assert any(cell["text"] == "50.00" for cell in cells), "Q2 grand sales 50.00 is absent from displayed cells"
        assert scroll > 0
        assert any(cell["colId"] == "Total|ventes_€" and cell["text"] == "200.00" for cell in right_cells)
        assert any(cell["colId"] == "Total|unités" and cell["text"] == "10.00" for cell in right_cells)
        assert any(cell["colId"] == "Total|écart" and cell["text"] == "-6.75" for cell in right_cells)


def test_reference_column_resize_changes_visible_geometry_and_retains_values():
    """A handle drag that changes no width or corrupts rows must fail resizing."""
    with reference_case("column-sizing.resize") as case:
        driver = case.driver
        driver.set_window_size(1000, 800)
        before = driver.execute_script("return window.referenceEvidence.headerGeometry()")
        before_order = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
        sales = next(item for item in before if item["colId"] == "Q1|ventes_€")
        handle = driver.find_element(By.CSS_SELECTOR, '.ag-header-cell[col-id="Q1|ventes_€"] .ag-header-cell-resize')
        ActionChains(driver).click_and_hold(handle).move_by_offset(40, 0).release().perform()
        after = driver.execute_script("return window.referenceEvidence.headerGeometry()")
        after_order = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
        resized = next(item for item in after if item["colId"] == sales["colId"])
        cells = driver.execute_script("return window.referenceEvidence.visibleCells()")
        scroll = driver.execute_script("""
          const viewport = document.querySelector('.ag-body-horizontal-scroll-viewport');
          viewport.scrollLeft = viewport.scrollWidth;
          return {left: viewport.scrollLeft, max: viewport.scrollWidth - viewport.clientWidth};
        """)
        right_cells = WebDriverWait(driver, 5).until(lambda browser: (
            visible if any(item["colId"] == "Total|ventes_€" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = 0")
        restored = WebDriverWait(driver, 5).until(lambda browser: (
            visible if any(item["colId"] == "Q1|ventes_€" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        case.observed = f"Dragged Q1 sales by 40 px; measured {sales['width']} -> {resized['width']} px; horizontal scroll {scroll}"
        case.parsed_output = {"before": before, "after": after, "before_order": before_order, "after_order": after_order,
                              "scroll": scroll, "right_cells": right_cells, "restored_cells": restored}
        assert 38 <= resized["width"] - sales["width"] <= 42
        assert before_order == after_order
        assert scroll["left"] > 0
        assert any(cell["colId"] == "Q1|ventes_€" and cell["text"] == "100.25" for cell in restored)
        assert any(cell["colId"] == "Total|ventes_€" and cell["text"] == "120.25" for cell in right_cells)


def test_reference_hierarchy_expansion_requests_child_and_collapses_cleanly():
    """An expand icon without a rendered server child must fail hierarchy parity."""
    with reference_case("hierarchy-expand.round-trip") as case:
        driver = case.driver
        parent = WebDriverWait(driver, 10).until(lambda browser: browser.find_element(
            By.CSS_SELECTOR, '.ag-row[row-index="0"] .ag-group-contracted'
        ))
        parent.click()
        expanded = WebDriverWait(driver, 10).until(lambda browser: (
            visible if any(item["text"] == "東京" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        expanded_trace = driver.execute_script("return window.referenceEvidence.trace()")
        driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = document.querySelector('.ag-body-horizontal-scroll-viewport').scrollWidth")
        totals = WebDriverWait(driver, 5).until(lambda browser: (
            visible if any(item["colId"] == "Total|ventes_€" and item["text"] == "120.25" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        driver.find_element(By.CSS_SELECTOR, '.ag-row[row-index="0"] .ag-group-expanded').click()
        collapsed = WebDriverWait(driver, 10).until(lambda browser: (
            visible if not any(item["text"] == "東京" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        case.observed = "Éclair expanded to 東京 via SSRM child request and collapsed; sales 120.25 and grand 200 retained"
        case.parsed_output = {"expanded_cells": expanded, "total_cells": totals, "collapsed_cells": collapsed}
        assert any(entry["request"]["groupKeys"] == ["Éclair"] for entry in expanded_trace)
        assert any(cell["colId"] == "Total|ventes_€" and cell["text"] == "120.25" for cell in totals)
        assert any(cell["colId"] == "Total|ventes_€" and cell["text"] == "200.00" for cell in totals)
        assert not any(cell["text"] == "東京" for cell in collapsed)
