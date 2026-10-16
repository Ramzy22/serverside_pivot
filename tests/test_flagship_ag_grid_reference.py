"""Browser observations of the pinned AG Grid Enterprise SSRM reference."""

from __future__ import annotations

import os
import csv
import io
from pathlib import Path

import pytest
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from tests.flagship_reference_support import DEFAULT_EVIDENCE, reference_case, start_reference_run


pytestmark = pytest.mark.flagship


def _clipboard(driver, *, sentinel=None):
    result = driver.execute_async_script("""
      const done = arguments[arguments.length - 1], sentinel = arguments[0];
      (async () => {
        if (!navigator.clipboard) throw new Error('Clipboard API unavailable');
        for (const name of ['clipboard-read', 'clipboard-write']) {
          const permission = await navigator.permissions.query({name});
          if (permission.state !== 'granted') throw new Error(name + ': ' + permission.state);
        }
        if (sentinel !== null) await navigator.clipboard.writeText(sentinel);
        return await navigator.clipboard.readText();
      })().then(text => done({text}), error => done({error: String(error)}));
    """, sentinel)
    if "error" in result:
        raise RuntimeError("Clipboard infrastructure unavailable: " + result["error"])
    return result["text"]


def _select_rectangle(driver, first, last, rows=(0, 1), columns=None):
    start = driver.find_element(By.CSS_SELECTOR, f'.ag-row[row-index="{rows[0]}"] [col-id="{first}"]')
    end = driver.find_element(By.CSS_SELECTOR, f'.ag-row[row-index="{rows[1]}"] [col-id="{last}"]')
    start.click()
    ActionChains(driver).key_down(Keys.SHIFT).click(end).key_up(Keys.SHIFT).perform()
    selected = driver.execute_script("""
      return [...document.querySelectorAll('[role=gridcell].ag-cell-range-selected')]
        .map(e => ({rowIndex: e.closest('[row-index]').getAttribute('row-index'), colId: e.getAttribute('col-id'), text: e.innerText, rawText: e.textContent}));
    """)
    assert {(cell['rowIndex'], cell['colId']) for cell in selected} == {
        (str(row), column) for row in range(rows[0], rows[1] + 1) for column in (columns or (first, last))}
    return selected


def _copy_selection(driver, *, headers=False):
    sentinel = "task-3d-clipboard-must-change"
    assert _clipboard(driver, sentinel=sentinel) == sentinel
    # Both commands are actual UI actions; the context-menu command is AG Grid's
    # built-in Copy with Headers rather than an API-only substitute.
    if headers:
        cell = driver.find_element(By.CSS_SELECTOR, '[role=gridcell].ag-cell-range-selected')
        ActionChains(driver).context_click(cell).perform()
        menu = WebDriverWait(driver, 5).until(lambda browser: browser.find_element(By.CSS_SELECTOR, '.ag-menu.ag-popup-child'))
        options = menu.find_elements(By.CSS_SELECTOR, '.ag-menu-option')
        choices = [option for option in options if option.text.strip() == 'Copy with Headers']
        if len(choices) != 1:
            raise RuntimeError('Copy with Headers UI unavailable: ' + menu.text)
        choices[0].click()
    else:
        driver.find_element(By.ID, 'copy').click()
    text = WebDriverWait(driver, 5).until(lambda browser: value if (value := _clipboard(browser)) != sentinel else False)
    return {"text": text, "rows": list(csv.reader(io.StringIO(text, newline=""), delimiter="\t"))}


def test_reference_copy_formatted_selection_exact_rectangle(reference_run):
    """Copying unrelated cells or bypassing the value formatter fails."""
    with reference_case('copy-selection.formatted-values', reference_run) as case:
        case.driver.execute_cdp_cmd('Browser.resetPermissions', {})
        with pytest.raises(RuntimeError, match='Clipboard infrastructure unavailable:.*clipboard-read'):
            _clipboard(case.driver)
        case.driver.execute_cdp_cmd('Browser.grantPermissions', {
            'origin': case.driver.execute_script('return location.origin'),
            'permissions': ['clipboardReadWrite', 'clipboardSanitizedWrite'],
        })
        selected = _select_rectangle(case.driver, 'Q1|ventes_€', 'Q1|unités', rows=(0, 0))
        copied = _copy_selection(case.driver)
        case.parsed_output = {"selected": selected, "clipboard": copied}
        assert copied['rows'] == [['100.25', '2.00']]
        _assert_cells(selected, {('0', 'Q1|ventes_€'): '100.25', ('0', 'Q1|unités'): '2.00'})
        case.observed = 'Mouse-selected Q1 sales/units for Éclair; Copy selection delivered exact formatted 1x2 clipboard body'


def test_reference_copy_headers_opt_in_exact_selected_order(reference_run):
    """Missing headers, extra pivot groups, or unselected columns fails."""
    with reference_case('copy-selection.headers', reference_run) as case:
        selected = _select_rectangle(case.driver, 'Q1|ventes_€', 'Q1|unités')
        body = _copy_selection(case.driver)
        headers = _copy_selection(case.driver, headers=True)
        case.parsed_output = {"selected": selected, "body": body, "with_headers": headers}
        assert body['rows'] == [['100.25', '2.00'], ['49.75', '3.00']]
        assert headers['rows'] == [['Ventes_€', 'Unités'], ['100.25', '2.00'], ['49.75', '3.00']]
        case.observed = 'Same mouse-selected 2x2 rectangle copied through body button and real Copy with Headers context menu; exact parsed rows and header order agree'


def test_reference_copy_pinned_boundary_without_duplicate_cells(reference_run):
    """Pinned rendering duplicated in clipboard or source-order copy fails."""
    with reference_case('copy-selection.pinned-columns', reference_run) as case:
        driver = case.driver
        driver.execute_script("window.referenceGrid.setColumnsPinned(['Q1|unités'], 'left')")
        driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = 40")
        WebDriverWait(driver, 5).until(lambda browser: browser.execute_script("return document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft > 0"))
        order = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
        assert order[:3] == ['ag-Grid-AutoColumn', 'Q1|unités', 'Q1|ventes_€']
        WebDriverWait(driver, 5).until(lambda browser: browser.find_element(By.CSS_SELECTOR, '.ag-grid-pinned-left-cells [role="gridcell"][col-id="Q1|unités"]'))
        selected = _select_rectangle(driver, 'Q1|unités', 'Q1|ventes_€')
        copied = _copy_selection(driver)
        case.parsed_output = {"order": order, "selected": selected, "clipboard": copied}
        assert copied['rows'] == [['2.00', '100.25'], ['3.00', '49.75']]
        _assert_cells(selected, {('0', 'Q1|unités'): '2.00', ('1', 'Q1|unités'): '3.00',
                                 ('0', 'Q1|ventes_€'): '100.25', ('1', 'Q1|ventes_€'): '49.75'})
        case.observed = 'Pinned Q1 units via column API; scrolled 40px; mouse-selected across pinned/center containers; exact 2x2 clipboard contains each logical cell once in display order'


def test_reference_copy_display_order_after_sort_and_reorder(reference_run):
    """Clipboard reverting to fixture row order or measure order fails."""
    with reference_case('copy-selection.display-order', reference_run) as case:
        driver = case.driver
        count = len(driver.execute_script('return window.referenceEvidence.trace()'))
        driver.execute_script("""
          window.referenceGrid.moveColumns(['Q1|unités'], 1);
          window.referenceGrid.applyColumnState({state:[{colId:'region',sort:'desc'}], defaultState:{sort:null}});
        """)
        request = _wait_request(driver, count)
        labels = _wait_column(driver, 'ag-Grid-AutoColumn', ['München', 'Éclair', 'Total'])
        order = driver.execute_script('return window.referenceGrid.getAllDisplayedColumns().map(c=>c.getColId())')
        assert order[:3] == ['ag-Grid-AutoColumn', 'Q1|unités', 'Q1|ventes_€']
        assert request['request']['sortModel'][0]['sort'] == 'desc'
        selected = _select_rectangle(driver, 'Q1|unités', 'Q1|ventes_€')
        copied = _copy_selection(driver)
        case.parsed_output = {"order": order, "labels": labels, "request": request, "selected": selected, "clipboard": copied}
        assert copied['rows'] == [['3.00', '49.75'], ['2.00', '100.25']]
        _assert_cells(selected, {('0', 'Q1|unités'): '3.00', ('0', 'Q1|ventes_€'): '49.75',
                                 ('1', 'Q1|unités'): '2.00', ('1', 'Q1|ventes_€'): '100.25'})
        case.observed = 'Column API reordered units before sales and requested descending region order; real selection/copy preserved exact visible row and field order'


@pytest.mark.parametrize('requirement,fields,expected', [
    ('tabs', ['copyTab', 'rowId'], [['left\tright', 'q1-eclair'], ['', 'q2-eclair'], ['', 'q1-munchen'], ['', 'q2-munchen']]),
    ('newlines', ['copyLF', 'copyCRLF'], [['', ''], ['first\nsecond', ''], ['', 'first\r\nsecond'], ['', '']]),
    ('unicode', ['copyUnicode', 'rowId'], [['Éclair', 'q1-eclair'], ['東京', 'q2-eclair'], ['القاهرة', 'q1-munchen'], ['Éclair München 東京 القاهرة 🚀', 'q2-munchen']]),
    ('empty-values', ['rowId', 'copyNull', 'copyEmpty'], [['q1-eclair', 'value', 'value'], ['q2-eclair', 'value', 'value'], ['q1-munchen', 'value', 'value'], ['q2-munchen', '', '']]),
])
def test_reference_copy_special_text_from_same_ssrm_fixture(reference_run, requirement, fields, expected):
    """Quoted TSV corruption, lost trailing empties, or Unicode changes fails."""
    with reference_case('copy-selection.' + requirement, reference_run) as case:
        driver = case.driver
        driver.execute_script("""
          window.referenceGrid.setGridOption('pivotMode', false);
          window.referenceGrid.setRowGroupColumns([]);
          window.referenceGrid.applyColumnState({
            state: arguments[0].map(colId=>({colId, hide:false, pinned:null})),
            defaultState:{hide:true}, applyOrder:true});
        """, fields)
        visible = _wait_column(driver, 'rowId' if 'rowId' in fields else 'copyLF',
                               ['q1-eclair', 'q2-eclair', 'q1-munchen', 'q2-munchen', ''] if 'rowId' in fields else ['', 'first second', '', '', ''])
        order = driver.execute_script('return window.referenceGrid.getAllDisplayedColumns().map(c=>c.getColId())')
        assert order == fields
        selected = _select_rectangle(driver, fields[0], fields[-1], rows=(0, 3), columns=fields)
        copied = _copy_selection(driver)
        case.parsed_output = {"setup": "pivot off, row grouping removed, selected fixture fields shown via column API", "order": order,
                              "visible": visible, "selected": selected, "clipboard": copied}
        actual_dom = {(cell['rowIndex'], cell['colId']): cell['rawText'] for cell in selected}
        assert actual_dom == {(str(row), field): expected[row][column]
                              for row in range(4) for column, field in enumerate(fields)}
        case.observed = 'Same SSRM fixture flattened via column APIs; visible logical cells and actual clipboard TSV captured against literal special-text oracle'
        assert copied['rows'] == expected
        # DOM innerText normalizes CRLF and can trim whitespace; clipboard must
        # preserve the literal source strings independently of that rendering.
        assert len(selected) == 4 * len(fields)
        trace = driver.execute_script('return window.referenceEvidence.trace()')
        assert trace[-1]['request']['pivotMode'] is False
        assert trace[-1]['request']['rowGroupCols'] == []
        assert trace[-1]['response']['rowCount'] == 4
        case.observed = 'Same SSRM fixture flattened via column APIs; exact visible field order and mouse-selected logical cells verified; actual clipboard TSV parsed and compared with literal special-text oracle'


def _assert_cells(cells, expected):
    actual = {(cell["rowIndex"], cell["colId"]): cell["text"] for cell in cells}
    assert {key: actual.get(key) for key in expected} == expected


@pytest.fixture(scope="module")
def reference_run():
    root = Path(os.environ.get("AG_GRID_REFERENCE_EVIDENCE", DEFAULT_EVIDENCE)).resolve()
    return start_reference_run(root, os.environ.get("AG_GRID_REFERENCE_RUN_ID"))


def _displayed_rows(driver):
    assert driver.execute_script("return typeof window.referenceEvidence.displayedRows === 'function'"), (
        "Reference must expose displayed SSRM row ancestry and totals for observation"
    )
    return driver.execute_script("return window.referenceEvidence.displayedRows()")


def _wait_column(driver, column, values):
    """Wait for unique logical positions, ignoring DOM insertion order during animation."""
    expected = [(str(index), value) for index, value in enumerate(values)]
    return WebDriverWait(driver, 10).until(lambda browser: (
        cells if sorted((cell["rowIndex"], cell["text"]) for cell in cells if cell["colId"] == column) == expected else False
    ) if (cells := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)


def _column_model(driver):
    assert driver.execute_script("return typeof window.referenceEvidence.columnModel === 'function'"), (
        "Reference must expose active filter, pivot and aggregate column model for observation"
    )
    return driver.execute_script("return window.referenceEvidence.columnModel()")


def _wait_request(driver, count):
    return WebDriverWait(driver, 10).until(lambda browser: (
        trace[-1] if len(trace) > count and "response" in trace[-1] else False
    ) if (trace := browser.execute_script("return window.referenceEvidence.trace()")) else False)


def _q1_header_slots(driver):
    slots = driver.execute_script("""
      return ['Q1|ventes_€', 'Q1|unités', 'Q1|écart'].map(id => {
        const header = [...document.querySelectorAll('.ag-header-cell')].find(e => e.getAttribute('col-id') === id);
        const cell = [...document.querySelectorAll('.ag-row[row-index="0"] [role="gridcell"]')].find(e => e.getAttribute('col-id') === id);
        if (!header || !cell) return null;
        const h = header.getBoundingClientRect(), c = cell.getBoundingClientRect();
        return {colId: header.getAttribute('col-id'), text: header.querySelector('.ag-header-cell-text')?.innerText,
          slot: header.getAttribute('aria-colindex'), left: h.left, width: h.width, top: h.top,
          cellLeft: c.left, cellWidth: c.width};
      });
    """)
    assert all(slots), "Q1 sales and neighboring headers/cells must be rendered"
    assert [(item["colId"], item["text"], item["slot"]) for item in slots] == [
        ("Q1|ventes_€", "Ventes_€", "2"), ("Q1|unités", "Unités", "3"), ("Q1|écart", "Écart", "4")]
    for item in slots:
        assert abs(item["left"] - item["cellLeft"]) <= 1
        assert abs(item["width"] - item["cellWidth"]) <= 1
    assert slots[0]["left"] < slots[1]["left"] < slots[2]["left"]
    return slots


def test_reference_value_columns_remove_and_add_units_with_exact_pivot_values(reference_run):
    """Missing valueCols updates or stale unit headers/cells after removal fails."""
    with reference_case("layout-values.add-remove", reference_run) as case:
        driver = case.driver
        _column_model(driver)
        observations = []
        for fields in (["ventes_€"], ["ventes_€", "unités"], ["ventes_€"]):
            count = len(driver.execute_script("return window.referenceEvidence.trace()"))
            driver.execute_script("window.referenceGrid.setValueColumns(arguments[0])", fields)
            request = _wait_request(driver, count)
            model = _column_model(driver)
            sales = _wait_column(driver, "Q1|ventes_€", ["100.25", "49.75", "150.00"])
            columns = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
            assert [value["field"] for value in request["request"]["valueCols"]] == fields
            assert model["values"] == [{"field": field, "aggFunc": "sum"} for field in fields]
            assert model["pivots"] == ["period"]
            assert request["request"]["filterModel"] == {"scope": {"filterType": "set", "values": ["Included"]}}
            labels = _wait_column(driver, "ag-Grid-AutoColumn", ["Éclair", "München", "Total"])
            if "unités" in fields:
                units = _wait_column(driver, "Q1|unités", ["2.00", "3.00", "5.00"])
                q2_units = _wait_column(driver, "Q2|unités", ["1.00", "4.00", "5.00"])
                assert "Total|unités" in columns
            else:
                units = []
                q2_units = []
                assert not any("unités" in column for column in columns)
                assert not any("unités" in cell["colId"] for cell in sales)
                assert not driver.find_elements(By.CSS_SELECTOR, '.ag-header-cell[col-id$="|unités"]')
            driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = 99999")
            totals = _wait_column(driver, "Total|ventes_€", ["120.25", "79.75", "200.00"])
            if "unités" in fields:
                unit_totals = _wait_column(driver, "Total|unités", ["3.00", "7.00", "10.00"])
            else:
                unit_totals = []
            observations.append({"request": request, "model": model, "columns": columns, "labels": labels,
                                 "sales": sales, "units": units, "q2_units": q2_units, "unit_totals": unit_totals, "totals": totals})
            case.parsed_output = observations
            driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = 0")
        case.observed = "AG Grid value-column API reduced to sales, added sum(units), then removed units; SSRM models, exact pivot cells and sales grand 200 agree"


def test_reference_aggregate_average_round_trip_updates_request_and_cells(reference_run):
    """A changed aggregate label without datasource avg or exact values fails."""
    with reference_case("layout-values.aggregation", reference_run) as case:
        driver = case.driver
        _column_model(driver)
        baseline_headers = _q1_header_slots(driver)
        observations = []
        for aggregate, values in (("avg", ["100.25", "49.75", "75.00"]), ("sum", ["100.25", "49.75", "150.00"])):
            count = len(driver.execute_script("return window.referenceEvidence.trace()"))
            before = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
            driver.execute_script("window.referenceGrid.setColumnAggFunc('ventes_€', arguments[0])", aggregate)
            request = _wait_request(driver, count)
            cells = _wait_column(driver, "Q1|ventes_€", values)
            headers = _q1_header_slots(driver)
            assert headers == baseline_headers
            labels = _wait_column(driver, "ag-Grid-AutoColumn", ["Éclair", "München", "Total"])
            model = _column_model(driver)
            after = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
            observations.append({"request": request, "model": model, "cells": cells, "labels": labels, "columns": after,
                                 "baseline_headers": baseline_headers, "headers": headers})
            case.parsed_output = observations
            assert before == after
            assert model["pivots"] == ["period"]
            assert next(value for value in request["request"]["valueCols"] if value["field"] == "ventes_€")["aggFunc"] == aggregate
            assert next(value for value in model["values"] if value["field"] == "ventes_€")["aggFunc"] == aggregate
            assert request["request"]["filterModel"] == {"scope": {"filterType": "set", "values": ["Included"]}}
        case.observed = "Changed sales aggregate via AG Grid API sum to avg to sum; SSRM requests matched and Q1 grand cell changed 150 to 75 to 150 in unchanged column order"


def test_reference_region_filter_apply_clear_preserves_scope_and_exact_sales(reference_run):
    """Header popup selections must issue a matching filter and recalculate cells."""
    with reference_case("header-filter.apply-clear", reference_run) as case:
        driver = case.driver
        assert driver.execute_script("return window.referenceConfig.options.autoGroupColumnDef.filter") == "agGroupColumnFilter"
        assert "GroupFilterModule" in driver.execute_script("return window.referenceConfig.modules")
        observations = []
        for region, labels, sales in (("Éclair", ["Éclair", "Total"], ["120.25", "120.25"]), (None, ["Éclair", "München", "Total"], ["120.25", "79.75", "200.00"])):
            buttons = driver.find_elements(By.CSS_SELECTOR, '.ag-header-cell[col-id="ag-Grid-AutoColumn"] .ag-header-cell-filter-button')
            assert len(buttons) == 1, "Actual grouped-region header filter button must exist; filter API calls cannot substitute"
            buttons[0].click()
            menu = WebDriverWait(driver, 5).until(lambda browser: browser.find_element(By.CSS_SELECTOR, '.ag-filter-menu'))
            assert menu.get_attribute("aria-label") == "Column Filter"
            assert menu.find_element(By.CSS_SELECTOR, '.ag-group-filter-field-select-wrapper .ag-picker-field-display').text == "Region"
            items = WebDriverWait(driver, 5).until(lambda browser: (
                items if len(items) == 3 else False
            ) if (items := menu.find_elements(By.CSS_SELECTOR, '.ag-set-filter-item')) else False)
            assert [item.text for item in items] == ["(Select All)", "München", "Éclair"]
            popup_before = menu.text
            # Default Set Filter applies immediately: unchecking München selects
            # Éclair alone; Select All clears the region filter on the next open.
            target = "München" if region else "(Select All)"
            count = len(driver.execute_script("return window.referenceEvidence.trace()"))
            next(item for item in items if item.text == target).find_element(By.CSS_SELECTOR, 'input[type="checkbox"]').click()
            request = _wait_request(driver, count)
            popup_after = menu.text
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            labels_cells = _wait_column(driver, "ag-Grid-AutoColumn", labels)
            driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = 99999")
            cells = _wait_column(driver, "Total|ventes_€", sales)
            model = _column_model(driver)
            observations.append({"request": request, "model": model, "labels": labels_cells, "sales": cells,
                                 "popup_before": popup_before, "popup_after": popup_after, "clicked_option": target})
            case.parsed_output = observations
            expected = {"scope": {"filterType": "set", "values": ["Included"]}}
            if region:
                expected["region"] = {"filterType": "set", "values": [region]}
            assert request["request"]["filterModel"] == expected == model["filterModel"]
            assert [row["label"] for row in _displayed_rows(driver)] == labels
            driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = 0")
        case.observed = "Clicked actual Region/account header filter popup, selected Éclair by unchecking München, then cleared with Select All; immediate SSRM apply changed visible total 200 to 120.25 to 200 while scope=Included remained"


def test_reference_sort_total_sales_updates_request_header_and_displayed_order(reference_run):
    """Wrong server sort direction, header indicator or grand-row placement fails."""
    with reference_case("sorting.direction-order", reference_run) as case:
        driver = case.driver
        _displayed_rows(driver)
        driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = 99999")
        observations = []
        for direction, labels, amounts in (
            ("ascending", ["München", "Éclair", "Total"], [79.75, 120.25, 200]),
            ("descending", ["Éclair", "München", "Total"], [120.25, 79.75, 200]),
        ):
            count = len(driver.execute_script("return window.referenceEvidence.trace()"))
            WebDriverWait(driver, 5).until(lambda browser: browser.find_element(
                By.CSS_SELECTOR, '.ag-header-cell[col-id="Total|ventes_€"] .ag-header-cell-label')).click()
            rows = WebDriverWait(driver, 10).until(lambda browser: (
                rows if [row["label"] for row in rows] == labels else False
            ) if (rows := _displayed_rows(browser)) else False)
            trace = WebDriverWait(driver, 10).until(lambda browser: (
                trace if len(trace) > count and "response" in trace[-1] else False
            ) if (trace := browser.execute_script("return window.referenceEvidence.trace()")) else False)
            indicator = driver.find_element(By.CSS_SELECTOR, '.ag-header-cell[col-id="Total|ventes_€"]').get_attribute("aria-sort")
            cells = _wait_column(driver, "Total|ventes_€", [f"{amount:.2f}" for amount in amounts])
            label_cells = _wait_column(driver, "ag-Grid-AutoColumn", labels)
            _assert_cells(label_cells, {(str(index), "ag-Grid-AutoColumn"): label for index, label in enumerate(labels)})
            observations.append({"direction": direction, "rows": rows, "cells": cells, "label_cells": label_cells, "request": trace[-1], "indicator": indicator})
            case.parsed_output = observations
            assert indicator == direction
            assert [row["data"]["Total|ventes_€"] for row in rows] == amounts
            assert rows[-1]["footer"] is True
            assert trace[-1]["request"]["sortModel"] == [{"colId": "Total|ventes_€", "sort": "asc" if direction == "ascending" else "desc", "type": "default"}]
            assert trace[-1]["request"]["filterModel"]["scope"]["values"] == ["Included"]
            _assert_cells(cells, {(str(index), "Total|ventes_€"): f"{amount:.2f}" for index, amount in enumerate(amounts)})
        case.observed = "Header clicks sorted total sales ascending/descending with matching SSRM sortModel, visible values and bottom grand total"


def test_reference_expand_all_preserves_parentage_totals_and_collapses_without_duplicates(reference_run):
    """Missing second branch or duplicated/misparented account rows fails expansion."""
    with reference_case("hierarchy-expand-all.round-trip", reference_run) as case:
        driver = case.driver
        _displayed_rows(driver)
        driver.execute_script("window.referenceGrid.expandAll()")
        expanded = WebDriverWait(driver, 10).until(lambda browser: (
            rows if [row["label"] for row in rows] == ["Éclair", "東京", "München", "القاهرة", "Total"] else False
        ) if (rows := _displayed_rows(browser)) else False)
        cells = _wait_column(driver, "ag-Grid-AutoColumn", ["Éclair", "東京", "München", "القاهرة", "Total"])
        trace = driver.execute_script("return window.referenceEvidence.trace()")
        driver.execute_script("window.referenceGrid.collapseAll()")
        collapsed = WebDriverWait(driver, 10).until(lambda browser: (
            rows if [row["label"] for row in rows] == ["Éclair", "München", "Total"] else False
        ) if (rows := _displayed_rows(browser)) else False)
        collapsed_cells = _wait_column(driver, "ag-Grid-AutoColumn", ["Éclair", "München", "Total"])
        case.parsed_output = {"expanded": expanded, "expanded_cells": cells, "collapsed": collapsed, "collapsed_cells": collapsed_cells}
        assert [(row["label"], row["parentKey"], row["level"]) for row in expanded[:-1]] == [
            ("Éclair", None, 0), ("東京", "Éclair", 1), ("München", None, 0), ("القاهرة", "München", 1)]
        assert [row["data"]["Total|ventes_€"] for row in expanded] == [120.25, 120.25, 79.75, 79.75, 200]
        assert [row["data"]["Total|ventes_€"] for row in collapsed] == [120.25, 79.75, 200]
        assert {tuple(entry["request"]["groupKeys"]) for entry in trace} >= {(), ("Éclair",), ("München",)}
        _assert_cells(cells, {("1", "ag-Grid-AutoColumn"): "東京", ("3", "ag-Grid-AutoColumn"): "القاهرة", ("4", "ag-Grid-AutoColumn"): "Total"})
        _assert_cells(collapsed_cells, {("0", "Q1|ventes_€"): "100.25", ("1", "Q1|ventes_€"): "49.75", ("2", "Q1|ventes_€"): "150.00"})
        case.observed = "expandAll loaded both SSRM account branches under correct parents; collapseAll retained exactly two region rows and grand sales 200"


def test_reference_horizontal_scroll_renders_last_measure_and_restores_first(reference_run):
    """A shifted header/value pairing or detached row label fails virtual navigation."""
    with reference_case("horizontal-virtual-scroll.window", reference_run) as case:
        driver = case.driver
        driver.set_window_size(1000, 800)
        _displayed_rows(driver)
        snapshots = []
        initial_trace = driver.execute_script("return window.referenceEvidence.trace()")
        assert len(initial_trace) == 1 and "response" in initial_trace[0]
        assert "Total|extrema" in initial_trace[0]["response"]["pivotResultFields"]
        assert {"RowApiModule", "ServerSideRowModelApiModule"} <= set(driver.execute_script("return window.referenceConfig.modules"))
        columns = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
        assert columns[-1] == "Total|extrema"
        for offset, column, values in ((99999, "Total|extrema", ["100", "-100", "-100"]), (0, "Q1|ventes_€", ["100.25", "49.75", "150.00"])):
            driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = arguments[0]", offset)
            cells = _wait_column(driver, column, values)
            geometry = driver.execute_script("return window.referenceEvidence.headerGeometry()")
            scroll = driver.execute_script("const v=document.querySelector('.ag-body-horizontal-scroll-viewport'); return {left:v.scrollLeft,max:v.scrollWidth-v.clientWidth}")
            snapshots.append({"column": column, "cells": cells, "headers": geometry, "scroll": scroll})
            case.parsed_output = snapshots
            _assert_cells(cells, {(str(index), column): value for index, value in enumerate(values)})
            _assert_cells(cells, {("0", "ag-Grid-AutoColumn"): "Éclair", ("1", "ag-Grid-AutoColumn"): "München", ("2", "ag-Grid-AutoColumn"): "Total"})
            assert any(header["colId"] == column for header in geometry)
            assert driver.find_element(By.CSS_SELECTOR, f'.ag-header-cell[col-id="{column}"] .ag-header-cell-text').text == ("Extrema" if offset else "Ventes_€")
            assert driver.execute_script("return window.referenceEvidence.trace()") == initial_trace
        assert snapshots[0]["scroll"]["left"] == snapshots[0]["scroll"]["max"] > 0
        assert snapshots[1]["scroll"]["left"] == 0
        case.observed = "Last pivot measure Total/extrema and first Q1/sales rendered with correct values and pinned labels; SSRM supplies all pivot columns without a column-window request"


def test_reference_startup_exposes_live_request_and_visible_groups(reference_run):
    """Missing fixture groups, pivot values, or grand totals must fail startup."""
    with reference_case("initial-workspace.populated", reference_run) as case:
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
        _assert_cells(cells, {
            ("0", "ag-Grid-AutoColumn"): "Éclair",
            ("0", "Q1|ventes_€"): "100.25", ("0", "Q1|unités"): "2.00", ("0", "Q1|écart"): "-12.50",
            ("0", "Q2|ventes_€"): "20.00", ("0", "Q2|unités"): "1.00", ("0", "Q2|écart"): "2.50",
            ("1", "ag-Grid-AutoColumn"): "München",
            ("1", "Q1|ventes_€"): "49.75", ("1", "Q1|unités"): "3.00", ("1", "Q1|écart"): "4.25",
            ("1", "Q2|ventes_€"): "30.00", ("1", "Q2|unités"): "4.00", ("1", "Q2|écart"): "-1.00",
            ("2", "ag-Grid-AutoColumn"): "Total",
            ("2", "Q1|ventes_€"): "150.00", ("2", "Q1|unités"): "5.00", ("2", "Q1|écart"): "-8.25",
            ("2", "Q2|ventes_€"): "50.00", ("2", "Q2|unités"): "5.00", ("2", "Q2|écart"): "1.50",
        })
        assert scroll > 0
        _assert_cells(right_cells, {
            ("0", "Total|ventes_€"): "120.25", ("0", "Total|unités"): "3.00", ("0", "Total|écart"): "-10.00",
            ("1", "Total|ventes_€"): "79.75", ("1", "Total|unités"): "7.00", ("1", "Total|écart"): "3.25",
            ("2", "Total|ventes_€"): "200.00", ("2", "Total|unités"): "10.00", ("2", "Total|écart"): "-6.75",
        })


def test_reference_column_resize_changes_visible_geometry_and_retains_values(reference_run):
    """A handle drag that changes no width or corrupts rows must fail resizing."""
    with reference_case("column-sizing.resize", reference_run) as case:
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


def test_reference_hierarchy_expansion_requests_child_and_collapses_cleanly(reference_run):
    """An expand icon without a rendered server child must fail hierarchy parity."""
    with reference_case("hierarchy-expand.round-trip", reference_run) as case:
        driver = case.driver
        parent = WebDriverWait(driver, 10).until(lambda browser: browser.find_element(
            By.CSS_SELECTOR, '.ag-row[row-index="0"] .ag-group-contracted'
        ))
        parent.click()
        expanded = WebDriverWait(driver, 10).until(lambda browser: (
            visible if any(item["text"] == "東京" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        expanded_trace = driver.execute_script("return window.referenceEvidence.trace()")
        before_columns = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
        driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft = document.querySelector('.ag-body-horizontal-scroll-viewport').scrollWidth")
        totals = WebDriverWait(driver, 5).until(lambda browser: (
            visible if any(item["colId"] == "Total|ventes_€" and item["text"] == "120.25" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        driver.find_element(By.CSS_SELECTOR, '.ag-row[row-index="0"] .ag-group-expanded').click()
        collapsed = WebDriverWait(driver, 10).until(lambda browser: (
            visible if not any(item["text"] == "東京" for item in visible) else False
        ) if (visible := browser.execute_script("return window.referenceEvidence.visibleCells()")) else False)
        after_columns = driver.execute_script("return window.referenceGrid.getAllDisplayedColumns().map(c => c.getColId())")
        case.observed = "Éclair expanded to 東京 via SSRM child request and collapsed; sales 120.25 and grand 200 retained"
        case.parsed_output = {"expanded_cells": expanded, "total_cells": totals, "collapsed_cells": collapsed,
                              "before_columns": before_columns, "after_columns": after_columns}
        child_requests = [entry for entry in expanded_trace if entry["request"]["groupKeys"] == ["Éclair"]]
        assert child_requests
        assert child_requests[-1]["response"]["rowData"][0]["account"] == "東京"
        assert child_requests[-1]["response"]["rowData"][0]["Total|ventes_€"] == 120.25
        _assert_cells(expanded, {
            ("0", "ag-Grid-AutoColumn"): "Éclair", ("1", "ag-Grid-AutoColumn"): "東京",
            ("1", "Q1|ventes_€"): "100.25", ("1", "Q2|ventes_€"): "20.00",
        })
        _assert_cells(totals, {
            ("0", "Total|ventes_€"): "120.25", ("1", "Total|ventes_€"): "120.25",
            ("3", "Total|ventes_€"): "200.00",
        })
        _assert_cells(collapsed, {
            ("0", "ag-Grid-AutoColumn"): "Éclair", ("1", "ag-Grid-AutoColumn"): "München",
            ("0", "Total|ventes_€"): "120.25", ("2", "Total|ventes_€"): "200.00",
        })
        assert before_columns == after_columns
