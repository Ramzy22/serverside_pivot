"""Native chart and loaded-export observations for the pinned SSRM reference.

Imported by the fixture-owning reference module; never reset its evidence run.
"""
from __future__ import annotations

import csv
import io
import hashlib
import base64
import struct
import pytest
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait

from tests.flagship_reference_support import reference_case as live_reference_case

SALES = 'Q1|ventes_\u20ac'
UNITS = 'Q1|unit\u00e9s'
CATEGORY = 'ag-Grid-AutoColumn'
SOURCE = [['\u00c9clair', 100.25, 2], ['M\u00fcnchen', 49.75, 3]]


def _menu(driver, label, *, hover=False):
    item = WebDriverWait(driver, 8).until(lambda browser: next((
        e for e in browser.find_elements(By.CSS_SELECTOR, '.ag-menu-option')
        if e.is_displayed() and e.text.strip() == label), False))
    if hover:
        ActionChains(driver).move_to_element(item).perform()
    else:
        item.click()


def _select(driver, rows=(0, 1)):
    from tests.test_flagship_ag_grid_reference import _select_rectangle
    return _select_rectangle(driver, CATEGORY, UNITS, rows=rows,
                             columns=(CATEGORY, SALES, UNITS))


def _chart_snapshot(driver):
    return driver.execute_script("""
      const rawModels=window.referenceGrid.getChartModels();
      const models=rawModels.map(m=>({chartId:m.chartId,chartType:m.chartType,
        unlinkChart:m.unlinkChart,cellRange:m.cellRange}));
      if(models.length!==1) return {models};
      const model=models[0], ref=window.referenceGrid.getChartRef(model.chartId);
      const options=ref.chart.getOptions();
      const series=options.series.map(s=>({type:s.type,xKey:s.xKey,yKey:s.yKey,yName:s.yName}));
      const rect=ref.chartElement.getBoundingClientRect();
      const keys=[...new Set(series.flatMap(s=>[s.xKey,s.yKey]).filter(Boolean))];
      const data=(options.data??[]).map(row=>Object.fromEntries(keys.map(key=>{
        const value=row[key];
        return [key, value && typeof value==='object' && Array.isArray(value.value)
          ? {labels:[...value.value]} : value && typeof value==='object' && Array.isArray(value.labels)
          ? {labels:[...value.labels]} : value];
      })));
      return {models,series,data,geometry:{width:rect.width,height:rect.height},
        canvas:[...ref.chartElement.querySelectorAll('canvas')].map(c=>({width:c.width,height:c.height})),
        ranges:(window.referenceGrid.getCellRanges()??[]).map(r=>({start:r.startRow.rowIndex,
          end:r.endRow.rowIndex,columns:r.columns.map(c=>c.getColId())})),
        dialogs:document.querySelectorAll('.ag-dialog').length};
    """)


def _source_rows(snapshot):
    """Read declared series keys rather than assuming AG Charts' data field names."""
    series = snapshot['series']
    assert len(series) == 2, series
    assert [s['yKey'] for s in series] == [SALES, UNITS], series
    assert all(s['type'] == 'bar' for s in series), series
    key = series[0]['xKey']
    assert key and all(s['xKey'] == key for s in series)
    def label(value):
        # Group chart categories carry their original hierarchy labels.
        return value['labels'][-1] if isinstance(value, dict) and 'labels' in value else value
    return [[label(row[key]), row[SALES], row[UNITS]] for row in snapshot['data']]


def _create_chart(case):
    from tests.test_flagship_ag_grid_reference import _wait_column
    driver = case.driver
    _wait_column(driver, SALES, ['100.25', '49.75', '150.00'])
    selected = _select(driver)
    cell = driver.find_element(By.CSS_SELECTOR, '.ag-row[row-index="0"] [col-id="'+SALES+'"]')
    ActionChains(driver).context_click(cell).perform()
    _menu(driver, 'Chart Range', hover=True)
    _menu(driver, 'Bar', hover=True)
    _menu(driver, 'Grouped')
    snapshot = WebDriverWait(driver, 15).until(lambda browser: (
        state if state.get('data') and len(state.get('series', [])) == 2 else False
    ) if (state := _chart_snapshot(browser)) else False)
    case.parsed_output = {'selected': selected, 'initial': snapshot}
    assert _source_rows(snapshot) == SOURCE, _source_rows(snapshot)
    assert snapshot['models'][0]['unlinkChart'] is False
    assert snapshot['dialogs'] == 1
    return snapshot


def _toolbar(driver, title):
    button = WebDriverWait(driver, 8).until(lambda browser: next((
        e for e in browser.find_elements(By.CSS_SELECTOR, '.ag-chart-menu-toolbar-button')
        if e.is_displayed() and e.get_attribute('title') == title), False))
    button.click()


def _chart_menu_action(driver, label):
    _toolbar(driver, 'Menu')
    _menu(driver, label)


def test_reference_range_chart_native_source(reference_run):
    with live_reference_case('range-chart.source', reference_run) as case:
        _create_chart(case)
        case.observed = 'Native selected category/Q1 sales/units range and Chart Range > Bar > Grouped created one linked chart; parsed chart options contain exactly Eclair/Munchen sales then units values and no footer or excluded record.'


@pytest.mark.parametrize('requirement,unlink', [
    ('range-chart.locked-source', True), ('range-chart.live-source', False)])
def test_reference_chart_native_link_and_reselection(reference_run, requirement, unlink):
    from tests.test_flagship_ag_grid_reference import _wait_column, _wait_request
    with live_reference_case(requirement, reference_run) as case:
        initial = _create_chart(case)
        driver = case.driver
        if unlink:
            _chart_menu_action(driver, 'Unlink from Grid')
            WebDriverWait(driver, 8).until(lambda _: _chart_snapshot(driver)['models'][0]['unlinkChart'])
        # Native charts link to their original range, not an application selection
        # listener. Observe the pinned behavior; do not install a custom handler.
        selected = _select(driver, (1, 1))
        driver.execute_async_script('const done=arguments[arguments.length-1]; requestAnimationFrame(()=>requestAnimationFrame(done))')
        after = _chart_snapshot(driver)
        case.parsed_output.update({'reselected': selected, 'after': after})
        assert after['models'][0]['unlinkChart'] is unlink
        assert _source_rows(after) == SOURCE
        assert after['models'][0]['cellRange'] == initial['models'][0]['cellRange']
        assert len(after['models']) == 1
        # Change the server-side view through the application's native search,
        # then compare the same chart model with the returned aggregate rows.
        count = len(driver.execute_script('return window.referenceEvidence.trace()'))
        driver.find_element(By.ID, 'global-search').send_keys('M\u00fcnchen')
        driver.find_element(By.ID, 'search-apply').click()
        request = _wait_request(driver, count)
        assert request['request']['globalSearch'] == 'M\u00fcnchen'
        assert request['response']['rowCount'] == 1
        rows = request['response']['rowData']
        assert len(rows) == 1
        assert rows[0]['region'] == 'M\u00fcnchen'
        assert [rows[0][SALES], rows[0][UNITS]] == [49.75, 3]
        _wait_column(driver, SALES, ['49.75', '49.75'])
        expected = SOURCE if unlink else [['M\u00fcnchen', 49.75, 3]]
        filtered = WebDriverWait(driver, 15).until(lambda browser: (
            state if len(state.get('models', [])) == 1 and state.get('data')
            and _source_rows(state) == expected else False
        ) if (state := _chart_snapshot(browser)) else False)
        assert filtered['models'][0]['chartId'] == initial['models'][0]['chartId']
        assert filtered['models'][0]['unlinkChart'] is unlink
        assert _source_rows(filtered) == expected
        case.parsed_output.update({'filteredRequest': request, 'filtered': filtered,
            'filteredDisplayedRows': driver.execute_script('return window.referenceEvidence.displayedRows()')})
        case.observed = ('Native unlink retained the exact original two-row chart snapshot after mouse reselection and a server-side search returned only Munich.' if unlink else
            'Native linked chart retained its original range on mouse reselection, then updated to the exact Munich aggregate returned by the server-side search; the chart model identity remained unchanged.')


def test_reference_chart_native_dialog_lifecycle(reference_run):
    with live_reference_case('chart-pane.lifecycle', reference_run) as case:
        before = _create_chart(case)
        driver = case.driver
        dialog = driver.find_element(By.CSS_SELECTOR, '.ag-dialog')
        initial_rect = dialog.rect
        # AG Grid's built-in floating dialog resize border and title bar.
        resize = dialog.find_element(By.CSS_SELECTOR, '.ag-resizer-bottomRight')
        ActionChains(driver).move_to_element(resize).click_and_hold().move_by_offset(65, 45).release().perform()
        WebDriverWait(driver, 8).until(lambda _: dialog.rect['width'] > initial_rect['width'] + 40)
        resized = dialog.rect
        title = dialog.find_element(By.CSS_SELECTOR, '.ag-panel-title-bar')
        ActionChains(driver).move_to_element(title).click_and_hold().move_by_offset(35, 25).release().perform()
        moved = dialog.rect
        assert abs(moved['x'] - resized['x']) >= 20
        after = _chart_snapshot(driver)
        assert _source_rows(after) == SOURCE
        assert after['models'][0]['chartId'] == before['models'][0]['chartId']
        dialog.find_element(By.CSS_SELECTOR, '.ag-button.ag-panel-title-bar-button').click()
        WebDriverWait(driver, 8).until(lambda _: not driver.find_elements(By.CSS_SELECTOR, '.ag-dialog'))
        assert driver.execute_script('return window.referenceGrid.getChartModels()') == []
        case.parsed_output.update({'dialogBefore': initial_rect, 'resized': resized, 'moved': moved, 'beforeClose': after, 'afterCloseModels': []})
        case.observed = 'Native floating chart dialog resized and moved with mouse while retaining exact chart source, then close removed chart model and dialog. Product pane docking has no equivalent in this configured reference floating-dialog lifecycle.'


def test_reference_chart_native_image_download(reference_run):
    with live_reference_case('chart-export.artifact', reference_run) as case:
        before = _create_chart(case)
        driver = case.driver
        rows = driver.execute_script('return window.referenceEvidence.displayedRows()')
        files = set(case.download_dir.iterdir())
        _chart_menu_action(driver, 'Download Chart')
        path = WebDriverWait(driver, 20).until(lambda _: next((p for p in case.download_dir.iterdir()
            if p not in files and p.suffix.lower() == '.png' and p.stat().st_size > 0), False))
        content = path.read_bytes()
        assert content[:8] == b'\x89PNG\r\n\x1a\n' and content[12:16] == b'IHDR'
        dimensions = struct.unpack('>II', content[16:24])
        assert dimensions[0] > 100 and dimensions[1] > 100
        decoded = driver.execute_async_script("""
          const done=arguments[arguments.length-1], image=new Image();
          image.onerror=()=>done({error:'Downloaded PNG failed to decode'});
          image.onload=()=>{
            const canvas=document.createElement('canvas');
            canvas.width=image.naturalWidth;canvas.height=image.naturalHeight;
            const ctx=canvas.getContext('2d');ctx.drawImage(image,0,0);
            const pixels=ctx.getImageData(0,0,canvas.width,canvas.height).data,colors=new Set();
            for(let i=0;i<pixels.length;i+=4) {
              colors.add([pixels[i],pixels[i+1],pixels[i+2],pixels[i+3]].join(','));
              if(colors.size>20) break;
            }
            done({width:image.naturalWidth,height:image.naturalHeight,colors:colors.size});
          };
          image.src=arguments[0];
        """, 'data:image/png;base64,' + base64.b64encode(content).decode('ascii'))
        assert decoded == {'width': dimensions[0], 'height': dimensions[1], 'colors': 21}, decoded
        after = _chart_snapshot(driver)
        assert _source_rows(after) == SOURCE
        assert after['models'] == before['models']
        assert after['dialogs'] == 1
        assert driver.execute_script('return window.referenceEvidence.displayedRows()') == rows
        assert any(abs(c['width']-dimensions[0]) <= 2 and abs(c['height']-dimensions[1]) <= 2 for c in after['canvas'])
        case.parsed_output.update({'download': {'filename': path.name, 'dimensions': dimensions,
            'sha256': hashlib.sha256(content).hexdigest(), 'decoded': decoded}, 'after': after})
        case.observed = 'Clicked native Download Chart toolbar; decoded nonblank PNG matches a chart canvas dimension, exact selected labels/series source remains intact, no grid mutation or extra chart pane.'


def _loaded_export(case):
    from tests.test_flagship_ag_grid_reference import _download_after_click
    driver = case.driver
    # Browser download policy may overwrite the default name. Give each actual
    # export a distinct public filename so repeated downloads remain observable.
    driver.execute_script("window.referenceGrid.setGridOption('defaultCsvExportParams',{fileName:arguments[0]})",
                          'reference-snapshot-' + str(len(list(case.download_dir.glob('*.csv')))) + '.csv')
    before = driver.execute_script('return window.referenceEvidence.trace()')
    resources_before = driver.execute_script('return performance.getEntriesByType("resource").map(e=>e.name)')
    path = _download_after_click(case, 'csv', '.csv')
    rows = list(csv.reader(io.StringIO(path.read_text(encoding='utf-8-sig'), newline='')))
    assert len(rows) >= 3 and rows[0][1] == 'Q1'
    assert rows[1][0] == 'Region / account'
    after = driver.execute_script('return window.referenceEvidence.trace()')
    assert after == before, 'Loaded-row export must not silently fetch more SSRM data'
    resources_after = driver.execute_script('return performance.getEntriesByType("resource").map(e=>e.name)')
    assert resources_after == resources_before, 'Native loaded CSV export unexpectedly requested server work'
    return {'rows': rows, 'filename': path.name, 'bytes': path.stat().st_size,
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'trace': after,
        'resourcesBefore': resources_before, 'resourcesAfter': resources_after,
        'controls': driver.execute_script('return [...document.querySelectorAll("nav button")].map(e=>({id:e.id,text:e.innerText}))')}


@pytest.mark.parametrize('requirement', ['large-delivery.progress', 'large-delivery.cancel-retry',
    'large-delivery.selected-part', 'journey-recovery.cancel-new-work'])
def test_reference_delivery_loaded_export_observation(reference_run, requirement):
    from tests.test_flagship_ag_grid_reference import _prepare_full_export, _wait_column, _wait_request
    with live_reference_case(requirement, reference_run) as case:
        driver = case.driver
        _wait_column(driver, SALES, ['100.25', '49.75', '150.00'])
        collapsed = _loaded_export(case)
        assert [r[0] for r in collapsed['rows'][2:]] == [' -> \u00c9clair', ' -> M\u00fcnchen', 'Total ']
        assert [r[1:3] for r in collapsed['rows'][2:]] == [['100.25','2.00'], ['49.75','3.00'], ['150.00','5.00']]
        _prepare_full_export(case)
        expanded = _loaded_export(case)
        assert [r[0] for r in expanded['rows'][2:]] == [' -> \u00c9clair', ' -> \u00c9clair -> \u6771\u4eac',
            ' -> M\u00fcnchen', ' -> M\u00fcnchen -> \u0627\u0644\u0642\u0627\u0647\u0631\u0629', 'Total ']
        assert expanded['rows'][-1][1:] == ['150.00','5.00','-8.25','-100','50.00','5.00','1.50','-100','200.00','10.00','-6.75','-100']
        count = len(driver.execute_script('return window.referenceEvidence.trace()'))
        search = driver.find_element(By.ID, 'global-search')
        search.send_keys('M\u00fcnchen')
        driver.find_element(By.ID, 'search-apply').click()
        request = _wait_request(driver, count)
        _wait_column(driver, SALES, ['49.75', '49.75'])
        fresh = _loaded_export(case)
        assert request['request']['globalSearch'] == 'M\u00fcnchen'
        assert [r[0] for r in fresh['rows'][2:]] == [' -> M\u00fcnchen', 'Total ']
        assert fresh['rows'][-1][1:3] == ['49.75', '3.00']
        case.parsed_output = {'collapsed': collapsed, 'expanded': expanded, 'freshFiltered': fresh,
            'freshRequest': request, 'difference': 'Configured native CSV export serializes loaded SSRM rows locally; no server job identity, measured progress, cancellation acknowledgement or selected-part manifest is produced.'}
        case.observed = ('Real CSV downloads parsed before/after account loading and fresh native search; exact hierarchy scope, Unicode and totals verified, with unchanged SSRM/resource traces across export. '
            'This pinned reference exports loaded rows locally; product server-job progress/cancel/retry/part and cancel-then-new-work semantics have no direct analogue. Fresh filtered export succeeds independently; this is an observed limitation, not cancellation or partition equivalence.')
