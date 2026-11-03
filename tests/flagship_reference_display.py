"""Native observations of the pinned reference's display and layout surface.

Collected only by imports in test_flagship_ag_grid_reference: no run fixture here.
An observed configured limitation is deliberately distinct from feature parity.
"""
from __future__ import annotations

import pytest
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait

SALES = 'ventes_\u20ac'
BASE_LABELS = ['\u00c9clair', 'M\u00fcnchen', 'Total']


def _snapshot(driver):
    return driver.execute_script("""
      const api=window.referenceGrid;
      return {rows:window.referenceEvidence.displayedRows(),
        cells:window.referenceEvidence.visibleCells(),state:api.getState(),
        columns:api.getAllDisplayedColumns().map(c=>c.getColId()),
        model:{rows:api.getRowGroupColumns().map(c=>c.getColId()),
          pivots:api.getPivotColumns().map(c=>c.getColId()),
          values:api.getValueColumns().map(c=>c.getColId())},
        trace:window.referenceEvidence.trace()};
    """)


def _assert_base(snapshot):
    assert [row['label'] for row in snapshot['rows']] == ['\u00c9clair', 'M\u00fcnchen', 'Total']
    assert [row['data']['Total|' + SALES] for row in snapshot['rows']] == [120.25, 79.75, 200]
    total = snapshot['rows'][-1]['data']
    assert [total['Q1|' + SALES], total['Q2|' + SALES]] == [150, 50]
    assert snapshot['trace'] and any(entry.get('response') for entry in snapshot['trace'])


def _select_sales(driver):
    cell = driver.find_element(By.CSS_SELECTOR, f'#grid [row-index="0"] [col-id="Q1|{SALES}"]')
    cell.click()
    return cell


def _surface(driver):
    """Open actual reference tool panel and cell menu, recording what they offer."""
    tab = driver.find_element(By.CSS_SELECTOR, '.ag-side-button button')
    tab.click()
    panel = WebDriverWait(driver, 10).until(lambda d: next(
        (e for e in d.find_elements(By.CSS_SELECTOR, '.ag-tool-panel-wrapper') if e.is_displayed()), False))
    panel_text = panel.text
    cell = _select_sales(driver)
    ActionChains(driver).context_click(cell).perform()
    menu = WebDriverWait(driver, 10).until(lambda d: next(
        (e for e in d.find_elements(By.CSS_SELECTOR, '.ag-menu') if e.is_displayed()), False))
    menu_text = menu.text
    assert 'Copy' in menu_text
    driver.switch_to.active_element.send_keys(Keys.ESCAPE)
    cell = _select_sales(driver)
    selected = driver.execute_script("return window.referenceGrid.getCellRanges().map(r=>({start:r.startRow.rowIndex,end:r.endRow.rowIndex,columns:r.columns.map(c=>c.getColId())}))")
    assert selected and selected[-1]['columns'] == ['Q1|' + SALES]
    return {'panel': panel_text, 'menu': menu_text, 'selection': selected}


def test_reference_display_density_configuration(reference_run):
    from tests.flagship_reference_support import reference_case
    with reference_case('density.cycle', reference_run) as case:
        driver = case.driver
        before = _snapshot(driver)
        _assert_base(before)
        observations = []
        for height in (44, 56, 36, 44):
            driver.execute_script("window.referenceGrid.setGridOption('rowHeight',arguments[0]);window.referenceGrid.resetRowHeights()", height)
            heights = WebDriverWait(driver, 10).until(lambda d: (
                values if values and all(abs(value-height) <= 1 for value in values) else False
            ) if (values := d.execute_script("return [...document.querySelectorAll('#grid .ag-row')].map(e=>e.getBoundingClientRect().height)")) else False)
            current = _snapshot(driver)
            _assert_base(current)
            assert [r['id'] for r in current['rows']] == [r['id'] for r in before['rows']]
            observations.append({'target': height, 'heights': heights, 'rows': current['rows']})
        case.parsed_output = {'configuration': 'Public rowHeight option, no density toolbar in pinned app', 'sequence': observations}
        case.observed = 'Public rowHeight configuration rendered 44,56,36,44 CSS px within 1px; stable row IDs and sales totals 150/50/200. This is configuration support, not a built-in density-cycle UI.'


def test_reference_display_report_restore(reference_run):
    from tests.flagship_reference_support import reference_case
    with reference_case('report-mode.restore-definition', reference_run) as case:
        driver = case.driver
        _assert_base(_snapshot(driver))
        # The existing native controls are application-owned Grid State storage.
        driver.find_element(By.ID, 'save').click()
        saved = driver.execute_script("return JSON.parse(localStorage.getItem('ag-grid-reference-state'))")
        driver.execute_script("window.referenceGrid.applyColumnState({state:[{colId:'period',pivot:false,pivotIndex:null,rowGroup:true,rowGroupIndex:0},{colId:'region',rowGroup:false,rowGroupIndex:null,pivot:true,pivotIndex:0},{colId:'account',rowGroup:false,rowGroupIndex:null}]})")
        changed = WebDriverWait(driver, 10).until(lambda d: (s if s['model']['rows'] == ['period'] and [r['label'] for r in s['rows']] == ['Q1', 'Q2', 'Total'] else False) if (s := _snapshot(d)) else False)
        driver.find_element(By.ID, 'restore').click()
        restored = WebDriverWait(driver, 10).until(lambda d: (s if s['model']['rows'] == ['region', 'account'] and [r['label'] for r in s['rows']] == ['\u00c9clair', 'M\u00fcnchen', 'Total'] else False) if (s := _snapshot(d)) else False)
        _assert_base(restored)
        assert restored['model']['pivots'] == ['period']
        _select_sales(driver)
        case.parsed_output = {'storageKey': 'ag-grid-reference-state', 'saved': saved, 'changed': changed, 'restored': restored}
        case.observed = 'Native Save grid state / Restore grid state controls recovered period pivots, region/account groups, exact sales 150/50/200 and a selectable grid after an alternate layout. Storage is application-owned localStorage; the pinned app has no named report catalog or report mode.'


def _scroll_snapshot(driver):
    return driver.execute_script("""
      const h=document.querySelector('.ag-body-horizontal-scroll-viewport'),
        v=document.querySelector('.ag-body-vertical-scroll-viewport'),
        body=document.querySelector('.ag-grid-viewport').getBoundingClientRect();
      return {left:h.scrollLeft,top:v.scrollTop,state:window.referenceGrid.getState(),
        cells:window.referenceEvidence.visibleCells(),
        visibleRows:[...document.querySelectorAll('#grid .ag-row')]
          .filter(e=>{const r=e.getBoundingClientRect();return r.bottom>body.top&&r.top<body.bottom})
          .map(e=>({id:e.getAttribute('row-id'),index:e.getAttribute('row-index')}))};
    """)


def test_reference_display_saved_scroll_ssrm(reference_run):
    from tests.flagship_reference_support import reference_case
    from tests.test_flagship_ag_grid_reference import _open_scroll_reference
    with reference_case('saved-scroll.restore', reference_run) as case:
        driver = case.driver
        _open_scroll_reference(driver)
        driver.set_window_size(1000, 800)
        # Native wheel scroll on the grid; horizontal scroll via Shift-wheel is
        # platform-dependent, so target the actual native scrollbar using keys.
        from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
        viewport = driver.find_element(By.CSS_SELECTOR, '.ag-grid-viewport')
        ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(viewport), 650, 850).perform()
        before = WebDriverWait(driver, 10).until(lambda d: (s if s['top'] > 0 else False) if (s := _scroll_snapshot(d)) else False)
        assert before['left'] > 0
        driver.find_element(By.ID, 'save').click()
        saved = driver.execute_script("return JSON.parse(localStorage.getItem('ag-grid-reference-state'))")
        origin = driver.execute_script('return performance.timeOrigin')
        driver.refresh()
        WebDriverWait(driver, 20).until(lambda d: d.execute_script('return window.referenceEvidence?.ready===true'))
        assert driver.execute_script('return performance.timeOrigin') > origin
        driver.find_element(By.ID, 'restore').click()
        after = _scroll_snapshot(driver)
        # Grid State scroll is client-side-only. Record actual SSRM offsets and
        # row IDs without manufacturing product-equivalent restoration.
        assert saved.get('scroll') is None
        assert after['state'].get('scroll') is None
        assert after['visibleRows'] and before['visibleRows']
        assert after['top'] == 0
        assert after['visibleRows'][0]['id'] != before['visibleRows'][0]['id']
        case.parsed_output = {'before': before, 'serialized': saved, 'afterReloadRestore': after, 'trace': _snapshot(driver)['trace']}
        case.observed = 'Native wheel scrolling changed both offsets; native Save, actual reload and Restore under SSRM omitted scroll from Grid State and returned to the first row. Saved SSRM scroll restoration is a configured reference difference; offsets and logical visible row IDs are recorded.'


def test_reference_display_field_zone_native_move(reference_run):
    from tests.flagship_reference_support import reference_case
    with reference_case('layout-field-zones.move', reference_run) as case:
        driver = case.driver
        # Eligibility is starting configuration; the actual layout move below
        # is a native drag of the existing row-group chip into the pivot zone.
        driver.execute_script("const defs=window.referenceGrid.getColumnDefs();defs.find(c=>c.field==='account').enablePivot=true;window.referenceGrid.setGridOption('columnDefs',defs)")
        before = _snapshot(driver)
        chips = driver.find_elements(By.CSS_SELECTOR, '.ag-column-drop-rowgroup .ag-column-drop-cell')
        chip = next(c for c in chips if 'Account' in c.text)
        target = driver.find_element(By.CSS_SELECTOR, '.ag-column-drop-horizontal-pivot .ag-column-drop-list')
        handle = chip.find_element(By.CSS_SELECTOR, '.ag-column-drop-cell-drag-handle')
        ActionChains(driver).move_to_element(handle).click_and_hold().move_by_offset(12, 0).move_to_element(target).pause(0.5).release().perform()
        # Dropping at the existing Period chip inserts Account before Period.
        # Assert the native insertion position and its matching server axes.
        moved = WebDriverWait(driver, 10).until(lambda d: (s if s['model']['rows'] == ['region'] and s['model']['pivots'] == ['account', 'period'] and any(e.get('response') and [c['id'] for c in e['request']['pivotCols']] == ['account', 'period'] for e in s['trace']) else False) if (s := _snapshot(d)) else False)
        rows = moved['rows']
        assert [r['label'] for r in rows] == ['\u00c9clair', 'M\u00fcnchen', 'Total']
        # Sparse account pivots come from the committed fixture, never invented.
        total = rows[-1]['data']
        sales = {k: v for k, v in total.items() if k.endswith('|' + SALES)}
        assert sales['Total|' + SALES] == 200
        assert sum(v for k, v in sales.items() if k != 'Total|' + SALES) == 200
        assert moved['state']['filter']['filterModel']['scope']['values'] == ['Included']
        assert before['model']['rows'] == ['region', 'account']
        case.parsed_output = {'before': before, 'after': moved, 'grandTotalSalesByPivot': sales}
        case.observed = 'Native Account row-group-chip drag inserted account once before period in pivot columns; live SSRM request and sparse pivot sales totaling 200 verified. Scope remains the existing Included Set Filter; AG Grid has filter models rather than the product Filters layout zone.'


def _flat_fixture(driver, mode, scope='default', decimals=2):
    """Configure real callbacks against existing server fixture fields only."""
    driver.execute_script("""
      const api=window.referenceGrid,mode=arguments[0],scope=arguments[1],decimals=arguments[2];
      const fmt=n=>new Intl.NumberFormat('en-GB',{style:'decimal',useGrouping:false,
        minimumFractionDigits:n,maximumFractionDigits:n,roundingMode:'halfExpand'});
      const defs=api.getColumnDefs().filter(d=>d.field==='scope').map(d=>({...d,hide:true}));
      defs.push({field:'rowId',headerName:'Row ID'});
      for(const field of ['ventes_\u20ac','format_edge','scale_value','scale_control']) {
        const digits=scope==='default'||field==='ventes_\u20ac'?decimals:2;
        const d={field,width:180,valueFormatter:p=>typeof p.value==='number'?fmt(digits).format(p.value):''};
        if(mode==='cell'&&field==='ventes_\u20ac') d.cellStyle=p=>
          ['q1-eclair','q2-eclair'].includes(p.data?.rowId)?{color:'#123456',backgroundColor:'#f0e0d0'}:null;
        if(mode==='scale'&&field==='scale_value') d.cellStyle=p=>({backgroundColor:
          p.value<=0?'rgba(248,105,107,0.82)':p.value===100?'rgba(248,105,107,0.06)':'rgba(99,190,123,0.82)'});
        if(mode==='bars'&&field==='scale_value') d.cellRenderer=p=>{
          const host=document.createElement('div');host.style.cssText='position:relative;width:100%;height:100%';
          const bar=document.createElement('span');bar.dataset.referenceBar='true';
          bar.style.cssText='position:absolute;left:0;top:25%;height:50%;background:rgba(99,190,123,.5);width:'+Math.min(100,Math.max(0,p.value/2))+'%';
          const text=document.createElement('span');text.style.position='relative';text.textContent=String(p.value);
          host.append(bar,text);return host;
        };
        defs.push(d);
      }
      api.setGridOption('grandTotalRow',undefined);api.setGridOption('pivotMode',false);
      api.setGridOption('columnDefs',defs);api.refreshServerSide({purge:true});
    """, mode, scope, decimals)
    return WebDriverWait(driver, 10).until(lambda d: (
        s if len(s['rows']) == 4 and [r['data'].get('rowId') for r in s['rows']] ==
        ['q1-eclair', 'q2-eclair', 'q1-munchen', 'q2-munchen'] else False
    ) if (s := _snapshot(d)) and all(r.get('data') for r in s['rows']) else False)


def _flat_presentation(driver):
    return driver.execute_script("""
      return [...document.querySelectorAll('#grid [role=gridcell]')].map(e=>{
        const s=getComputedStyle(e),bar=e.querySelector('[data-reference-bar]');return {
          row:e.closest('[row-index]').getAttribute('row-index'),column:e.getAttribute('col-id'),text:e.innerText,
          color:s.color,background:s.backgroundColor,
          bar:bar?{width:bar.getBoundingClientRect().width,usable:bar.parentElement.getBoundingClientRect().width}:null};});
    """)


@pytest.mark.parametrize('requirement,mode', [
    ('number-format.decimals','number'), ('cell-format.apply-close','cell'),
    ('color-scales.scale','scale'), ('color-scales.data-bars','bars'),
])
def test_reference_display_application_callbacks(reference_run, requirement, mode):
    from tests.flagship_reference_support import reference_case
    with reference_case(requirement, reference_run) as case:
        driver = case.driver
        surface = _surface(driver)
        stages = []
        configurations = [('default', 2), ('default', 1), ('selected-columns', 1)] if mode == 'number' else [('default', 2)]
        for scope, decimals in configurations:
            snapshot = _flat_fixture(driver, mode, scope, decimals)
            assert sum(r['data'][SALES] for r in snapshot['rows']) == 200
            expected_sales = ['100.25', '20.00', '49.75', '30.00'] if decimals == 2 else ['100.3', '20.0', '49.8', '30.0']
            def ready(d):
                cells = _flat_presentation(d)
                return cells if [c['text'] for c in cells if c['column'] == SALES] == expected_sales else False
            cells = WebDriverWait(driver, 10).until(ready)
            by_column = {field: [c for c in cells if c['column'] == field]
                         for field in (SALES, 'format_edge', 'scale_value', 'scale_control')}
            assert len(by_column['scale_control']) == 4
            if mode == 'number':
                assert by_column['format_edge'][0]['text'] == ('-100.3' if decimals == 1 and scope == 'default' else '-100.25')
                assert by_column['scale_control'][0]['text'] == ('10.0' if decimals == 1 and scope == 'default' else '10.00')
            elif mode == 'cell':
                assert [c['background'] for c in by_column[SALES]][:2] == ['rgb(240, 224, 208)'] * 2
                assert [c['color'] for c in by_column[SALES]][:2] == ['rgb(18, 52, 86)'] * 2
                assert all(c['background'] != 'rgb(240, 224, 208)' for c in by_column[SALES][2:] + by_column['scale_control'])
            elif mode == 'scale':
                assert [c['background'] for c in by_column['scale_value']] == [
                    'rgba(248, 105, 107, 0.82)', 'rgba(248, 105, 107, 0.06)',
                    'rgba(99, 190, 123, 0.82)', 'rgba(99, 190, 123, 0.82)']
                assert [c['text'] for c in by_column['scale_value']] == ['0.00', '100.00', '200.00', '300.00']
                assert all(c['background'] not in {v['background'] for v in by_column['scale_value']} for c in by_column['scale_control'])
            elif mode == 'bars':
                for cell, percent in zip(by_column['scale_value'], (0, 50, 100, 100)):
                    assert cell['bar']['usable'] > 0
                    assert abs(100 * cell['bar']['width'] / cell['bar']['usable'] - percent) <= 1
                assert all(c['bar'] is None for c in by_column['scale_control'])
            # Selection remains a real native operation against rendered cells.
            driver.find_element(By.CSS_SELECTOR, f'#grid [row-index="0"] [col-id="{SALES}"]').click()
            assert driver.execute_script('return window.referenceGrid.getCellRanges().length') == 1
            stages.append({'scope': scope, 'decimals': decimals, 'snapshot': snapshot, 'cells': cells})
        case.parsed_output = {'nativeSurface': surface, 'configuration': 'Application-provided public ColDef callbacks against committed SSRM fixture, no custom datasource or authoring UI', 'stages': stages}
        case.observed = ('Public ColDef ' + mode + ' callbacks rendered the real four Included fixture rows; raw sales remained 200, selected numeric controls and computed output checked. '
                         'Native menu/panel and native cell selection observed. This demonstrates application configuration, not built-in formatting dialogs, conditional-format authoring or dialog-close/copy parity. Bars clamp to 0..100%; scale clamps beyond 200.')


@pytest.mark.parametrize('requirement', [
    'totals.controls', 'row-numbers.toggle', 'themes.select-override',
    'zoom-immersive.round-trip', 'formula-columns.add-remove', 'custom-dimensions.add-remove',
])
def test_reference_display_configured_limitations(reference_run, requirement):
    """Observe each configuration against actual rendered data in the pinned app."""
    from tests.flagship_reference_support import reference_case
    with reference_case(requirement, reference_run) as case:
        driver = case.driver
        before = _snapshot(driver)
        _assert_base(before)
        controls = driver.execute_script("return [...document.querySelectorAll('nav button')].filter(e=>e.offsetParent).map(e=>e.textContent.trim())")
        assert controls == ['Copy selection', 'Export CSV', 'Export Excel', 'Save grid state', 'Restore grid state', 'Search', 'Clear search']
        if requirement == 'totals.controls':
            driver.execute_script("window.referenceGrid.setGridOption('grandTotalRow',undefined)")
            without = WebDriverWait(driver, 10).until(lambda d: (s if not any(r['footer'] for r in s['rows']) else False) if (s := _snapshot(d)) else False)
            assert [r['data']['Total|' + SALES] for r in without['rows']] == [120.25, 79.75]
            driver.execute_script("window.referenceGrid.setGridOption('grandTotalRow','bottom')")
            restored = WebDriverWait(driver, 10).until(lambda d: (s if len(s['rows']) == 3 and s['rows'][-1]['footer'] else False) if (s := _snapshot(d)) else False)
            _assert_base(restored)
            evidence = {'withoutGrandTotal': without, 'restored': restored}
            observed = 'Public grandTotalRow configuration removed and restored the footer; pivot totals and underlying SSRM data remained visible. The application toolbar has no independent totals controls.'
        elif requirement == 'row-numbers.toggle':
            details = driver.execute_script("""return {modules:window.referenceConfig.modules,
              option:window.referenceGrid.getGridOption('rowNumbers') ?? null,
              columns:window.referenceGrid.getAllDisplayedColumns().map(c=>c.getColId()),
              cells:window.referenceEvidence.visibleCells()};""")
            assert 'RowNumbersModule' not in details['modules'] and details['option'] is None
            assert not any('rowNumber' in c for c in details['columns'])
            assert [r['label'] for r in before['rows']] == BASE_LABELS
            evidence = details
            observed = 'The pinned configuration omits RowNumbersModule and rowNumbers, and its actual displayed columns and cells contain group labels rather than a row-number column. The application toolbar has no row-number toggle.'
        elif requirement in ('formula-columns.add-remove', 'custom-dimensions.add-remove'):
            flat = _flat_fixture(driver, 'number')
            field = 'double_sales' if requirement.startswith('formula') else 'IncludedGroup'
            driver.execute_script("""const api=window.referenceGrid,field=arguments[0],salesField=arguments[1];
              const defs=api.getColumnDefs();
              defs.push(field==='double_sales' ? {colId:field,headerName:field,valueGetter:p=>p.data?.[salesField]*2} :
                {colId:field,headerName:field,valueGetter:p=>p.data?.scope==='Included'?'IncludedGroup':'Other'});
              api.setGridOption('columnDefs',defs); api.ensureColumnVisible(field);""", field, SALES)
            def derived(d):
                return d.execute_script("return window.referenceEvidence.visibleCells().filter(c=>c.colId===arguments[0])", field)
            cells = WebDriverWait(driver, 10).until(lambda d: (v if len(v) == 4 else False) if (v := derived(d)) else False)
            expected = ['200.5', '40', '99.5', '60'] if field == 'double_sales' else ['IncludedGroup'] * 4
            assert [c['text'] for c in cells] == expected
            assert sum(r['data'][SALES] for r in flat['rows']) == 200
            driver.execute_script("const api=window.referenceGrid;api.setGridOption('columnDefs',api.getColumnDefs().filter(d=>d.colId!==arguments[0]))", field)
            assert field not in _snapshot(driver)['columns']
            evidence = {'source': flat, 'derivedCells': cells, 'columnsAfterRemoval': _snapshot(driver)['columns']}
            observed = 'An application-supplied public ColDef valueGetter added and removed ' + field + ' on the real four SSRM rows; rendered derived values and raw sales were checked. The pinned application toolbar has no authoring control.'
        else:
            def appearance():
                return driver.execute_script("""const g=document.querySelector('#grid'),c=g.querySelector('[role=gridcell]'),r=g.getBoundingClientRect(),s=getComputedStyle(c);return {width:r.width,height:r.height,cellHeight:c.getBoundingClientRect().height,zoom:getComputedStyle(g).zoom,color:s.color,background:s.backgroundColor};""")
            initial = appearance()
            if requirement == 'themes.select-override':
                driver.execute_script("document.querySelector('#grid').style.setProperty('--ag-foreground-color','#123456')")
                changed = WebDriverWait(driver, 10).until(lambda d: (a if a['color'] == 'rgb(18, 52, 86)' else False) if (a := appearance()) else False)
                driver.execute_script("document.querySelector('#grid').style.removeProperty('--ag-foreground-color')")
                restored = appearance()
                assert restored['color'] == initial['color']
                observed = 'Application CSS theme parameter override changed the rendered cell foreground and removal restored it; the application toolbar has no theme selector.'
            else:
                driver.execute_script("document.querySelector('#grid').style.zoom='1.2'")
                changed = appearance()
                assert float(changed['zoom']) == pytest.approx(1.2)
                assert changed['cellHeight'] > initial['cellHeight'] * 1.1
                driver.execute_script("document.querySelector('#grid').style.zoom=''")
                restored = appearance()
                assert abs(restored['cellHeight'] - initial['cellHeight']) < 1
                observed = 'Application CSS zoom changed the grid geometry and clearing it restored the original geometry; the application toolbar has no zoom or immersive control.'
            after = _snapshot(driver)
            _assert_base(after)
            assert after['rows'] == before['rows']
            evidence = {'initial': initial, 'changed': changed, 'restored': restored, 'after': after}
        case.parsed_output = {'scope': 'Pinned configured reference only', 'toolbar': controls, 'before': before, 'evidence': evidence}
        case.observed = observed


def test_reference_display_fixture_identifiers():
    """The server field and labels retain the fixture's Unicode characters."""
    assert SALES.encode('unicode_escape') == b'ventes_\\u20ac'
    assert [label.encode('unicode_escape') for label in BASE_LABELS] == [
        b'\\xc9clair', b'M\\xfcnchen', b'Total']
