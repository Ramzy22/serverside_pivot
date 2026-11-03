"""Native clipboard and analysis observations, collected by the shared reference module.

There is deliberately no fixture here: reference_run belongs to the collecting
module, and importing its helpers at execution time avoids a collection cycle.
"""
from __future__ import annotations

import pytest
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import ElementClickInterceptedException, StaleElementReferenceException

from tests.flagship_reference_support import reference_case


def _select_all_native(driver):
    driver.find_element(By.CSS_SELECTOR, '.ag-row[row-index="0"] [role="gridcell"]').click()
    ActionChains(driver).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()
    ranges = driver.execute_script("""
      return window.referenceGrid.getCellRanges().map(r=>({
        start:r.startRow.rowIndex,end:r.endRow.rowIndex,
        columns:r.columns.map(c=>c.getColId())}));
    """)
    assert len(ranges) == 1
    return ranges[0]


def _clipboard_formats(driver):
    result = driver.execute_async_script("""
      const done=arguments[arguments.length-1];
      navigator.clipboard.read().then(async items=>done(await Promise.all(items.map(async item=>{
        const result={}; for(const type of item.types) result[type]=await (await item.getType(type)).text();
        return result;
      }))),error=>done({error:String(error)}));
    """)
    assert isinstance(result, list), result
    return result


def test_reference_clipboard_default_pivot_headers_across_groups(reference_run):
    from tests.test_flagship_ag_grid_reference import _select_rectangle, _copy_selection

    with reference_case('copy-selection.merged-headers', reference_run) as case:
        driver = case.driver
        columns = ['Q1|ventes_€', 'Q1|unités', 'Q1|écart', 'Q1|extrema', 'Q2|ventes_€']
        selected = _select_rectangle(driver, columns[0], columns[-1], rows=(0, 0), columns=columns)
        copied = _copy_selection(driver, headers=True)
        formats = _clipboard_formats(driver)
        case.parsed_output = {'selected': selected, 'clipboard': copied, 'nativeFormats': formats,
                              'scope': 'Default built-in Copy with Headers across Q1/Q2, no clipboard customization'}
        assert copied['rows'] == [['Ventes_€', 'Unités', 'Écart', 'Extrema', 'Ventes_€'],
                                  ['100.25', '2.00', '-12.50', '100', '20.00']]
        assert any(item.get('text/plain') == copied['text'] for item in formats)
        # Default AG Grid copies a plain TSV measure-header row, not HTML spans.
        assert all('text/html' not in item for item in formats)
        case.observed = ('Pinned configured SSRM Copy with Headers across Q1/Q2 delivered exact five measure labels '
                         'and values as native text/plain; clipboard advertised no text/html or merged pivot spans. '
                         'Observed default-clipboard difference, not product merged-header equivalence.')


@pytest.mark.parametrize('requirement', ['copy-current-table.visible-scope', 'copy-all-records.complete-scope'])
def test_reference_clipboard_collapsed_table_scope(reference_run, requirement):
    from tests.test_flagship_ag_grid_reference import _copy_selection, _displayed_rows, _EXPORT_FORMATTED, _EXPORT_MEASURES

    with reference_case(requirement, reference_run) as case:
        driver = case.driver
        rows = _displayed_rows(driver)
        assert [row['label'] for row in rows] == ['Éclair', 'München', 'Total']
        before = driver.execute_script('return window.referenceEvidence.trace()')
        selected = _select_all_native(driver)
        copied = _copy_selection(driver, headers=True)
        after = driver.execute_script('return window.referenceEvidence.trace()')
        case.parsed_output = {'rows': rows, 'range': selected, 'clipboard': copied,
                              'beforeTrace': before, 'afterTrace': after,
                              'scope': 'Native Ctrl+A followed by built-in Copy with Headers; selected displayed table'}
        assert copied['rows'] == [['Region / account', *_EXPORT_MEASURES],
                                  ['Éclair', *_EXPORT_FORMATTED[0]],
                                  ['München', *_EXPORT_FORMATTED[2]]]
        assert after == before
        assert {tuple(item['request']['groupKeys']) for item in after} == {()}
        assert selected['columns'] == driver.execute_script('return window.referenceGrid.getAllDisplayedColumns().map(c=>c.getColId())')
        assert selected['start'] == 0 and selected['end'] == 2
        case.observed = ('Native select-all and Copy with Headers copied the exact collapsed displayed table, '
                         'two included region aggregates in displayed column order. The selection included '
                         'the visible grand-total footer, but native copy omitted that footer. No child '
                         'request occurred; Tokyo/Cairo account rows were absent. This selected-table route '
                         'is not a separate current-table command or a complete-hierarchy clipboard operation '
                         'in this configured reference.')


def test_reference_clipboard_selection_crosses_unloaded_ssrm_blocks(reference_run):
    from tests.test_flagship_ag_grid_reference import _open_scroll_reference, _copy_selection

    with reference_case('copy-selection.unloaded-rows', reference_run) as case:
        driver = case.driver
        # Keep the reference's 500px minimum grid entirely inside the viewport,
        # while rendering fewer than one 50-row block at the bottom.
        driver.set_window_size(1680, 850)
        _open_scroll_reference(driver)
        # Start and endpoint are actual rendered native cells. Jumping to the last
        # block leaves the middle 50..99 block genuinely unrequested.
        start = driver.find_element(By.CSS_SELECTOR, '.ag-row[row-index="0"] [col-id="P23|ventes_€"]')
        start.click()
        driver.execute_script("document.querySelector('.ag-body-vertical-scroll-viewport').scrollTop=99999")
        end = WebDriverWait(driver, 10).until(lambda browser: browser.find_element(
            By.CSS_SELECTOR, '.ag-row[row-index="119"] [col-id="P23|ventes_€"]'))
        ActionChains(driver).move_to_element(end).perform()
        # The newly loaded rows animate into position. Confirm the actual native
        # hit target has settled so Shift+click cannot land on the adjacent footer.
        previous = None
        def stable_endpoint(browser):
            nonlocal previous
            cell = browser.find_element(By.CSS_SELECTOR, '.ag-row[row-index="119"] [col-id="P23|ventes_€"]')
            current = cell.rect
            correct_hit = browser.execute_script("const r=arguments[0].getBoundingClientRect();return document.elementFromPoint(r.x+r.width/2,r.y+r.height/2)?.closest('.ag-row')?.getAttribute('row-index')==='119'", cell)
            ready = correct_hit and previous == current
            previous = current
            return cell if ready else False
        end = WebDriverWait(driver, 10).until(stable_endpoint)
        ActionChains(driver).key_down(Keys.SHIFT).click(end).key_up(Keys.SHIFT).perform()
        before = driver.execute_script('return window.referenceEvidence.trace()')
        blocks = {(entry['request']['startRow'], entry['request']['endRow']) for entry in before if 'response' in entry}
        assert (0, 50) in blocks and (100, 150) in blocks and (50, 100) not in blocks
        ranges = driver.execute_script('return window.referenceGrid.getCellRanges().map(r=>({start:r.startRow.rowIndex,end:r.endRow.rowIndex,columns:r.columns.map(c=>c.getColId())}))')
        assert ranges == [{'start': 0, 'end': 119, 'columns': ['P23|ventes_€']}]
        copied = _copy_selection(driver)
        after = driver.execute_script('return window.referenceEvidence.trace()')
        expected_loaded = [[f'{region * 100 + 23.25:.2f}'] for region in [*range(50), *range(100, 120)]]
        case.parsed_output = {'range': ranges, 'beforeTrace': before, 'afterTrace': after,
                              'clipboard': copied, 'expectedLoadedOnly': expected_loaded}
        # This is an observation of AG Grid's documented loaded-row limitation,
        # not the product contract's accepted full-load/report-unavailable branch.
        assert copied['rows'] == expected_loaded
        assert after == before
        case.observed = ('Native shift selection covered rows 0..119 across SSRM blocks 0 and 100; '
                         'block 50 remained unrequested. Actual copy returned exactly the 70 loaded sales '
                         'values and omitted the 50 unloaded rows without fetching them. This is a '
                         'loaded-row/truncation difference from the product required behavior.')


def test_reference_selection_journey_filter_pin_scroll_copy(reference_run):
    from tests.test_flagship_ag_grid_reference import _wait_column, _pin_menu, _select_rectangle, _copy_selection

    with reference_case('journey-selection.filter-pin-scroll-copy', reference_run) as case:
        driver = case.driver
        search = driver.find_element(By.ID, 'global-search')
        search.send_keys('München')
        search.submit()
        _wait_column(driver, 'Q1|ventes_€', ['49.75', '49.75'])
        _pin_menu(driver, 'Q1|unités', 'Pin Left')
        WebDriverWait(driver, 5).until(lambda browser: browser.execute_script("return window.referenceGrid.getColumn('Q1|unités').getPinned()==='left'"))
        driver.execute_script("document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft=40")
        scroll = driver.execute_script("return document.querySelector('.ag-body-horizontal-scroll-viewport').scrollLeft")
        assert scroll > 0
        selected = _select_rectangle(driver, 'Q1|unités', 'Q1|ventes_€', rows=(0, 0))
        copied = _copy_selection(driver, headers=True)
        trace = driver.execute_script('return window.referenceEvidence.trace()')
        case.parsed_output = {'selected': selected, 'clipboard': copied, 'scroll': scroll, 'trace': trace}
        assert copied['rows'] == [['Unités', 'Ventes_€'], ['3.00', '49.75']]
        assert trace[-1]['request']['globalSearch'] == 'München'
        assert trace[-1]['request']['filterModel']['scope']['values'] == ['Included']
        assert trace[-1]['response']['rowCount'] == 1
        case.observed = ('Submitted native search for München, used native header menu Pin Left on Q1 units, '
                         'scrolled and shift-selected the cross-container row range; Copy with Headers '
                         'returned exactly units/sales 3.00/49.75 once. Pinned configured reference retains '
                         'two-decimal formatting; product journey disables fixed formatting.')


def test_reference_analysis_journey_average_expand_swap_export(reference_run):
    from openpyxl import load_workbook
    from tests.test_flagship_ag_grid_reference import _wait_column, _displayed_rows, _download_after_click

    with reference_case('journey-analysis.aggregate-expand-swap-export', reference_run) as case:
        driver = case.driver
        # APIs seed the analysis model; expansion and export use native controls.
        driver.execute_script("window.referenceGrid.setColumnAggFunc('ventes_€','avg')")
        average = _wait_column(driver, 'Q1|ventes_€', ['100.25', '49.75', '75.00'])
        for index in (0, 2):
            def click_group(browser):
                try:
                    row = browser.find_element(By.CSS_SELECTOR, f'.ag-row[row-index="{index}"]')
                    first = browser.find_element(By.CSS_SELECTOR, '.ag-row[row-index="0"]')
                    if abs(row.rect['y'] - (first.rect['y'] + index * first.rect['height'])) > 1:
                        return False
                    toggle = row.find_element(By.CSS_SELECTOR, '.ag-group-contracted')
                    if not toggle.is_displayed() or not toggle.is_enabled():
                        return False
                    toggle.click()
                    return True
                except (ElementClickInterceptedException, StaleElementReferenceException):
                    return False
            WebDriverWait(driver, 10).until(click_group)
            expected_count = 4 if index == 0 else 5
            WebDriverWait(driver, 10).until(lambda browser: len(_displayed_rows(browser)) == expected_count)
        expanded = _displayed_rows(driver)
        assert [row['label'] for row in expanded] == ['Éclair', '東京', 'München', 'القاهرة', 'Total']
        assert driver.execute_script("""return window.referenceGrid.applyColumnState({state:[
          {colId:'period',rowGroup:true,rowGroupIndex:0,pivot:false,pivotIndex:null},
          {colId:'region',rowGroup:false,rowGroupIndex:null,pivot:true,pivotIndex:0},
          {colId:'account',rowGroup:false,rowGroupIndex:null,pivot:true,pivotIndex:1}]});""")
        WebDriverWait(driver, 10).until(lambda browser: browser.execute_script("const c=window.referenceGrid.getColumn(arguments[0]),v=document.querySelector('.ag-body-horizontal-scroll-viewport');if(!c||c.getLeft()==null)return false;v.scrollLeft=c.getLeft();return true", 'Éclair|東京|ventes_€'))
        _wait_column(driver, 'Éclair|東京|ventes_€', ['100.25', '20.00', '60.13'])
        rows = _displayed_rows(driver)
        assert [row['label'] for row in rows] == ['Q1', 'Q2', 'Total']
        sales = ['Éclair|東京|ventes_€', 'München|القاهرة|ventes_€', 'Total|ventes_€']
        expected = [[100.25, 49.75, 75], [20, 30, 25], [60.125, 39.875, 50]]
        assert [[row['data'][column] for column in sales] for row in rows] == expected
        # Excel export defaults to raw numeric cells, unlike formatted clipboard.
        file = _download_after_click(case, 'excel', '.xlsx')
        workbook = load_workbook(file, data_only=False)
        sheet = workbook.active
        exported = [[cell.value for cell in row] for row in sheet.iter_rows()]
        columns = driver.execute_script('return window.referenceGrid.getAllDisplayedColumns().map(c=>c.getColId())')
        data_rows = exported[-3:]
        case.parsed_output = {'averageBeforeSwap': average, 'expanded': expanded, 'columns': columns,
                              'rows': rows, 'exported': exported, 'filename': file.name,
                              'types': [[cell.data_type for cell in row] for row in sheet.iter_rows()]}
        assert [row[0] for row in data_rows] == [' -> Q1', ' -> Q2', 'Total ']
        assert [[row[columns.index(column)] for column in sales] for row in data_rows] == expected
        expected_columns = ['ag-Grid-AutoColumn'] + [
            f'{pivot}|{field}' for pivot in ('Total', 'München|القاهرة', 'Éclair|東京')
            for field in ('ventes_€', 'unités', 'écart', 'extrema')]
        expected_numeric = [
            [75, 5, -8.25, -100, 49.75, 3, 4.25, -100, 100.25, 2, -12.5, 100],
            [25, 5, 1.5, -100, 30, 4, -1, -100, 20, 1, 2.5, 100],
            [50, 10, -6.75, -100, 39.875, 7, 3.25, -100, 60.125, 3, -10, 100],
        ]
        assert columns == expected_columns
        assert [row[1:] for row in data_rows] == expected_numeric
        assert [[row['data'][column] for column in columns[1:]] for row in rows] == expected_numeric
        for row in sheet.iter_rows(min_row=sheet.max_row - 2):
            assert all(cell.data_type == 'n' for cell in row[1:])
        assert all('São Paulo' not in str(row) for row in exported)
        trace = driver.execute_script('return window.referenceEvidence.trace()')
        last = trace[-1]['request']
        assert [column['field'] for column in last['rowGroupCols']] == ['period']
        assert [column['field'] for column in last['pivotCols']] == ['region', 'account']
        assert next(column for column in last['valueCols'] if column['field'] == 'ventes_€')['aggFunc'] == 'avg'
        case.observed = ('Public model APIs changed sales to average and swapped axes after native expansion '
                         'loaded both accounts; native Excel download preserved period rows and region/account '
                         'pivot order, numeric Q1 100.25/49.75/grand 75 and Q2 20/30/grand 25, grand average 50. '
                         'Axis and aggregate configuration is API seeded, not a product-equivalent swap button.')
