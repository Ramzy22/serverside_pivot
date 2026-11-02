"""Observable installed-host assertions for clipboard, persistence and combined journeys.

All interaction mutations below use WebDriver native input. JavaScript only observes
DOM/network/storage or seeds an OS clipboard sentinel, never product state.
"""
from __future__ import annotations

import csv
import hashlib
from html.parser import HTMLParser
import io
import json
import os
from pathlib import Path
import time
import traceback

REQUIREMENTS = (
    'saved-scroll.restore', 'copy-selection.merged-headers',
    'copy-selection.unloaded-rows', 'large-delivery.selected-part',
    'journey-selection.filter-pin-scroll-copy',
    'journey-analysis.aggregate-expand-swap-export',
)

ACCEPTANCE_CHECKS = {
    'saved-scroll.restore': 'savedScrollRestored',
    'copy-selection.merged-headers': 'mergedSelectionHeadersVerified',
    'copy-selection.unloaded-rows': 'unloadedSelectionRejectedWithoutWrite',
    'large-delivery.selected-part': 'selectedPartRowsVerified',
    'journey-selection.filter-pin-scroll-copy': 'combinedSelectionClipboardVerified',
    'journey-analysis.aggregate-expand-swap-export': 'analysisDisplayAndExportVerified',
}

# Retain complete public V1 envelopes rather than a projection which might hide
# a wrong axis, aggregation, filter or an omitted boundary row.
PROBE = r"""
if (!window.__pivotJourneyTrace) {
 const trace=window.__pivotJourneyTrace=[];
 const visit=(raw,phase,url)=>{
  let v=raw;if(typeof v==='string'){try{v=JSON.parse(v)}catch(_){return}}
  if(!v||typeof v!=='object')return;
  if(phase==='response'&&new URL(url,location.href).pathname.includes('/payload/')&&Array.isArray(v.data)){
   trace.push({phase:'payload',at:performance.now(),url:new URL(url,location.href).href,payload:v});return;
  }
  if(v.schemaVersion===1&&v.requestId&&v.kind){trace.push({phase,at:performance.now(),url,envelope:v});return}
  Object.values(v).forEach(child=>visit(child,phase,url));
 };
 const fetch=window.fetch.bind(window);
 window.fetch=(...args)=>{const url=String(args[0]?.url||args[0]);visit(args[1]?.body,'request',url);
  return fetch(...args).then(r=>{r.clone().text().then(t=>visit(t,'response',r.url||url)).catch(()=>{});return r})};
 const open=XMLHttpRequest.prototype.open,send=XMLHttpRequest.prototype.send;
 XMLHttpRequest.prototype.open=function(method,url,...rest){this.__journeyURL=String(url);return open.call(this,method,url,...rest)};
 XMLHttpRequest.prototype.send=function(body){visit(body,'request',this.__journeyURL);
  this.addEventListener('load',()=>visit(this.responseText,'response',this.__journeyURL));return send.call(this,body)};
}
"""


class ClipboardTable(HTMLParser):
    """Parse actual clipboard cells and retain the selected header span widths."""
    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.rows = []
        self.cell = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        if tag == 'tr':
            self.rows.append([])
        if tag in ('td', 'th'):
            attrs = dict(attrs)
            self.cell = {'tag': tag, 'text': '', 'colspan': int(attrs.get('colspan', 1)),
                         'rowspan': int(attrs.get('rowspan', 1))}
            self.rows[-1].append(self.cell)

    def handle_data(self, data):
        if self.cell is not None:
            self.cell['text'] += data

    def handle_endtag(self, tag):
        if tag in ('td', 'th'):
            self.cell = None


def assert_merged_selection(html, body):
    rows = ClipboardTable(html).rows
    assert len(rows) == 2 + len(body)
    headers = rows[:2]
    assert [[(c['text'], c['colspan'], c['rowspan']) for c in row] for row in headers] == [
        [('Q1', 2, 1), ('Q2', 1, 1)],
        [('Ventes € (Sum)', 1, 1), ('Écart (Sum)', 1, 1), ('Unités (Sum)', 1, 1)],
    ]
    assert all(sum(c['colspan'] for c in row) == 3 for row in headers)
    assert [[c['text'] for c in row] for row in rows[2:]] == body
    assert all(len(row) == 3 and all(c['tag'] == 'td' for c in row) for row in rows[2:])
    return rows


def assert_scroll_restored(saved, restored):
    assert saved['top'] > 0 and saved['left'] > 0
    assert saved['firstRow'] and saved['firstColumn']
    assert restored['firstRow'] == saved['firstRow']
    assert restored['firstColumn'] == saved['firstColumn']
    assert abs(restored['top'] - saved['top']) <= 2
    assert abs(restored['left'] - saved['left']) <= 2
    assert restored['viewport'] == saved['viewport']
    assert restored['font'] == saved['font'] and restored['zoom'] == saved['zoom']


def assert_part_rows(rows, request):
    payload, state = request['payload'], request['state']
    assert (payload['partId'], payload['rowStart'], payload['rowEnd']) == ('r1_c0', 5000, 10000)
    assert payload['includeHeaders'] is True and payload['allRows'] is False
    assert payload['colIds'] == ['hierarchy', 'sales_sum']
    assert state['rowFields'] == ['region'] and state['colFields'] == []
    manifest = json.loads(Path(__file__).with_name('data').joinpath('flagship_parts_manifest.json').read_text(encoding='utf-8'))
    assert state['filters'] == manifest['filters']
    assert rows[0] == ['Region', 'Sales (Sum)']
    expected = [[f'R{i:05d}', str(i + .25)] for i in range(5000, 10000)]
    assert rows[1:] == expected
    assert len({row[0] for row in rows[1:]}) == 5000
    assert 'R04999' not in [row[0] for row in rows]
    assert 'R10000' not in [row[0] for row in rows]


def assert_part_manifest_binding(sha256):
    digest = hashlib.sha256(Path(__file__).with_name('data').joinpath('flagship_parts_manifest.json').read_bytes()).hexdigest()
    assert sha256.get('parts_fixture') == digest, 'Part oracle differs from frozen installed fixture'
    return digest


def _trace(driver):
    return driver.execute_script('return window.__pivotJourneyTrace || []')


def _responses(driver, checkpoint=-1, kind='data'):
    return correlated_responses(_trace(driver), checkpoint, kind)


def correlated_responses(trace, checkpoint=-1, kind='data'):
    requests = {e['envelope']['requestId']: e for e in trace
                if e['phase'] == 'request' and e['at'] > checkpoint and e['envelope']['kind'] == kind}
    pairs = []
    for event in trace:
        if event['phase'] != 'response':
            continue
        reply = event['envelope']
        if reply['requestId'] not in requests or reply.get('status') != ('data' if kind == 'data' else 'export'):
            continue
        request = requests[reply['requestId']]['envelope']
        ref = reply.get('payload', {}).get('payloadRef')
        if kind == 'data' and ref:
            from urllib.parse import urljoin
            url = urljoin(event['url'], ref['url'])
            remote = next((e['payload'] for e in trace if e['phase'] == 'payload'
                           and e['url'] == url and e['at'] >= event['at']), None)
            if remote is None:
                continue
            reply = {**reply, 'payload': {**reply['payload'], **remote}}
        pairs.append((request, reply))
    return pairs


def _scroll_snapshot(driver):
    from tests.test_flagship_certification import _wait_for_visible_workspace_element
    scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
    return driver.execute_script(r"""
      const s=arguments[0],b=s.getBoundingClientRect(),style=getComputedStyle(s);
      const headerBottom=Math.max(b.top,...[...s.querySelectorAll('[role=columnheader]')].map(h=>h.getBoundingClientRect().bottom));
      const cells=[...s.querySelectorAll('[role=gridcell][data-rowid][data-colid]')];
      const visible=cells.map(c=>({c,b:c.getBoundingClientRect()})).filter(x=>
        x.b.bottom>headerBottom&&x.b.top<b.bottom&&x.b.right>b.left&&x.b.left<b.right);
      const data=visible.filter(x=>x.c.dataset.rowid!=='__grand_total__');
      const rows=data.filter(x=>x.c.dataset.colid==='hierarchy').sort((a,b)=>a.b.top-b.b.top);
      const leftBoundary=Math.max(b.left,...rows.map(x=>x.b.right));
      const cols=data.filter(x=>x.c.dataset.colid!=='hierarchy'&&getComputedStyle(x.c).position!=='sticky'
        && x.b.right>leftBoundary).sort((a,b)=>a.b.left-b.b.left);
      return {top:s.scrollTop,left:s.scrollLeft,firstRow:rows[0]?.c.dataset.rowid,
        firstColumn:cols[0]?.c.dataset.colid,viewport:[s.clientWidth,s.clientHeight,innerWidth,innerHeight,devicePixelRatio],
        font:[style.fontFamily,style.fontSize],zoom:style.zoom};
    """, scroller)


def _stable_scroll(driver, wait):
    previous = None
    def settled(current):
        nonlocal previous
        value = _scroll_snapshot(current)
        if value == previous and value.get('firstRow') and value.get('firstColumn'):
            return value
        previous = value
        return False
    return wait.until(settled)


def _wheel(driver, dx, dy):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
    from tests.test_flagship_certification import _wait_for_visible_workspace_element
    scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
    ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), dx, dy).perform()


def scroll_window_response(trace, row_id, checkpoint=-1):
    """Correlate an aligned nonzero window with its observed logical row."""
    if not row_id.startswith('R') or not row_id[1:].isdigit():
        return False
    index = int(row_id[1:])
    pairs = correlated_responses(trace)
    initial_sizes = [req['payload']['end'] + 1 for req, _ in pairs
                     if req['payload'].get('start') == 0 and req['payload'].get('end', -1) >= 0]
    if not initial_sizes:
        return False
    block = min(initial_sizes)
    recent = {e['envelope']['requestId'] for e in trace if e['phase'] == 'request' and e['at'] > checkpoint}
    for request, response in reversed(pairs):
        payload = request['payload']
        start, end = payload.get('start', 0), payload.get('end', -1)
        if request['requestId'] not in recent or start <= 0 or start % block or not start <= index <= end:
            continue
        if (end-start+1) % block or payload.get('count', end-start+1) != end-start+1:
            continue
        ids = [r.get('_id') for r in response['payload'].get('data', []) if r.get('_id') != 'Grand Total']
        offset = index-start
        if ids.count(row_id) == 1 and offset < len(ids) and ids[offset] == row_id:
            return {'request': request, 'response': response, 'blockSize': block}
    return False


def unloaded_range_evidence(loaded_before, loaded_after, endpoint):
    """Prove a real missing interior, with both endpoints loaded and under cap."""
    assert endpoint.startswith('R') and len(endpoint) == 6 and endpoint[1:].isdigit(), 'Parts fixture row ID required'
    end = int(endpoint[1:])
    assert 0 < end < 10000, 'Selection must fit the 10000-row copy cap'
    assert 'R00000' in loaded_before and 'R00000' in loaded_after
    assert endpoint not in loaded_before and endpoint in loaded_after
    missing = [f'R{i:05d}' for i in range(1, end) if f'R{i:05d}' not in loaded_after]
    assert missing, 'Fixture selection did not cross a genuinely unloaded block'
    return {'endpointIds': ['R00000', endpoint], 'logicalRowCount': end+1, 'unloadedRowIds': missing}


def assert_journey_fixture_binding(requirement, sha256):
    if requirement in ('large-delivery.selected-part', 'copy-selection.unloaded-rows'):
        return assert_part_manifest_binding(sha256)
    return None


def _save_scroll(driver, host, wait, observation, downloads):
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import _verify_release_asset_batch
    assert os.environ.get('PIVOT_FLAGSHIP_SCROLL_DATA') == '1', 'Wide fixture required'
    checkpoint = driver.execute_script('return performance.now()')
    _wheel(driver, 850, 2500)
    wait.until(lambda d: _scroll_snapshot(d)['top'] > 0 and _scroll_snapshot(d)['left'] > 0)
    saved = _stable_scroll(driver, wait)
    assert saved['firstRow'] != 'R000' and saved['firstColumn'] != 'P23_unités_sum'
    observation['savedWindow'] = wait.until(lambda d: scroll_window_response(_trace(d), saved['firstRow'], checkpoint))
    if s._totals_control_button(driver, 'Save View') is None:
        s._click_totals_control(driver, 'Theme')
    s._click_totals_control(driver, 'Save View')
    key = f'flagship-{host}-pivot-savedView'
    snapshot = wait.until(lambda d: d.execute_script('return JSON.parse(localStorage.getItem(arguments[0]))', key))
    assert abs(snapshot['state']['scroll']['top'] - saved['top']) <= 2
    assert abs(snapshot['state']['scroll']['left'] - saved['left']) <= 2
    observation.update(saved=saved, stored=snapshot)
    driver.save_screenshot(str(downloads / 'saved-scroll.png'))
    _verify_release_asset_batch(driver, require_assets=True)
    origin = driver.execute_script('return performance.timeOrigin')
    driver.get_log('performance')
    driver.refresh()
    def restored(d):
        try:
            current = _scroll_snapshot(d)
            assert_scroll_restored(saved, current)
            return current
        except (AssertionError, KeyError):
            return False
    observation['restored'] = wait.until(restored)
    assert driver.execute_script('return performance.timeOrigin') > origin
    assert driver.execute_script('return JSON.parse(localStorage.getItem(arguments[0]))', key) == snapshot
    assert_scroll_restored(saved, _stable_scroll(driver, wait))


def _copy_html(driver, cell, menu, wait):
    from selenium.webdriver.common.action_chains import ActionChains
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import _clipboard_html, _clipboard_text
    sentinel = 'journey clipboard checkpoint'
    assert driver.execute_async_script("const done=arguments[1];navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false))", sentinel)
    ActionChains(driver).context_click(cell).perform()
    s._selection_menu(driver, menu).click()
    text = wait.until(lambda d: (v if (v := _clipboard_text(d)) != sentinel else False))
    html = wait.until(lambda d: (v if (v := _clipboard_html(d)).startswith('<') else False))
    return text, html


def _merged(driver, host, wait, observation, downloads):
    from tests import flagship_browser_support as s
    rows = wait.until(lambda d: s._selection_rows(d) if len(s._selection_rows(d)) == 2 else False)
    columns = ['Q1_ventes_€_sum', 'Q1_écart_sum', 'Q2_unités_sum']
    order = s._selection_column_order(driver)
    assert order[order.index(columns[0]):order.index(columns[-1])+1] == columns
    ids = [row['id'] for row in rows]
    observation['selectedIds'] = s._select_rectangle(driver, ids, columns)
    text, html = _copy_html(driver, s._selection_cell(driver, ids[0], columns[0]), 'Copy Selection with Merged Headers (Raw Values)', wait)
    expected = {'Éclair': ['100.25', '-12.5', '1'], 'München': ['49.75', '4.25', '4']}
    body = [expected[row['label']] for row in rows]
    observation.update(clipboardText=text, clipboardHtml=html, parsedHtml=assert_merged_selection(html, body))
    assert list(csv.reader(io.StringIO(text), delimiter='\t'))[-2:] == body


def _selection_journey(driver, host, wait, observation, downloads):
    from selenium.webdriver.common.action_chains import ActionChains
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import _wait_for_visible_workspace_element as element
    checkpoint = driver.execute_script('return performance.now()')
    element(driver, '[data-testid="pivot-global-search"]').send_keys('München')
    wait.until(lambda d: [r['label'] for r in s._selection_rows(d)] == ['München']
               and any(any(r.get('_id') == 'München' and r.get('Q1_ventes_€_sum') == 49.75 for r in reply['payload'].get('data', []))
                       for req, reply in _responses(d, checkpoint)))
    units, sales = 'Q1_unités_sum', 'Q1_ventes_€_sum'
    header = element(driver, f'[role="columnheader"][data-header-column-id="{units}"]')
    ActionChains(driver).context_click(header).perform()
    ActionChains(driver).move_to_element(s._selection_menu(driver, 'Pin')).perform()
    s._selection_menu(driver, 'Pin Left').click()
    wait.until(lambda d: element(d, f'[role="columnheader"][data-header-column-id="{units}"]').get_attribute('data-pinned') == 'left')
    before = _scroll_snapshot(driver)
    _wheel(driver, 50, 0)
    wait.until(lambda d: _scroll_snapshot(d)['left'] > before['left'])
    order = s._selection_column_order(driver)
    assert order.index(units) < order.index(sales)
    rows = s._selection_rows(driver)
    assert len(rows) == 1 and rows[0]['label'] == 'München'
    observation['selectedIds'] = s._select_rectangle(driver, [rows[0]['id']], [units, sales])
    text, html = _copy_html(driver, s._selection_cell(driver, rows[0]['id'], units), 'Copy Selection with Headers (Raw Values)', wait)
    assert list(csv.reader(io.StringIO(text), delimiter='\t')) == [s.selected_headers_oracle([units, sales]), ['3', '49.75']]
    parsed = ClipboardTable(html).rows
    assert [[c['text'] for c in row] for row in parsed] == [s.selected_headers_oracle([units, sales]), ['3', '49.75']]
    assert all(len(row) == 2 for row in parsed)
    assert 'Éclair' not in text and 'São Paulo' not in text
    observation.update(clipboardText=text, clipboardHtml=html, parsedHtml=parsed, scroll=_scroll_snapshot(driver))


def _unloaded(driver, host, wait, observation, downloads):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import _clipboard_text, _workspace_elements
    assert os.environ.get('PIVOT_FLAGSHIP_PARTS_DATA') == '1', 'Dedicated 12001-row profile required'
    column = 'sales_sum'
    first = s._selection_cell(driver, 'R00000', column)
    first.click()
    assert s._selection_ids(driver) == [('R00000', column)]
    before = _trace(driver)
    loaded_before = {r['_id'] for _, reply in _responses(driver) for r in reply['payload'].get('data', []) if '_id' in r}
    assert 'R00000' in loaded_before
    checkpoint = driver.execute_script('return performance.now()')
    _wheel(driver, 0, 40000)
    def far_endpoint(d):
        ids = [c['rowid'] for c in _analysis_cells(d) if c['colid'] == column
               and c['rowid'].startswith('R') and len(c['rowid']) == 6 and c['rowid'][1:].isdigit()
               and 0 < int(c['rowid'][1:]) < 10000 and c['rowid'] not in loaded_before]
        if not ids:
            return False
        endpoint = max(ids, key=lambda row: int(row[1:]))
        window = scroll_window_response(_trace(d), endpoint, checkpoint)
        return {'id': endpoint, 'window': window} if window else False
    endpoint = wait.until(far_endpoint)
    last = s._selection_cell(driver, endpoint['id'], column)
    ActionChains(driver).key_down(Keys.SHIFT).click(last).key_up(Keys.SHIFT).perform()
    selected = s._selection_ids(driver)
    assert (endpoint['id'], column) in selected
    loaded = {r['_id'] for _, reply in _responses(driver) for r in reply['payload'].get('data', []) if '_id' in r}
    gap = unloaded_range_evidence(loaded_before, loaded, endpoint['id'])
    observation.update(loadedBefore=sorted(loaded_before), loadedAfter=sorted(loaded), **gap,
                       farWindow=endpoint['window'], visibleSelection=selected, initialTrace=before)
    sentinel = 'unavailable selection must preserve this OS clipboard'
    assert driver.execute_async_script("const done=arguments[1];navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false))", sentinel)
    ActionChains(driver).context_click(last).perform()
    s._selection_menu(driver, 'Copy Selection').click()
    notice = wait.until(lambda d: next((e.text for e in _workspace_elements(d, '*') if e.is_displayed()
        and e.text.strip() == 'Selected range unavailable: some rows are not loaded. Load the complete range before copying.'), False))
    s.assert_clipboard_stable(driver, sentinel, 1.5)
    assert _clipboard_text(driver) == sentinel
    observation.update(acceptedBranch='explicit unavailable range, clipboard preserved', difference='Copy requires all logical rows loaded',
                       notification=notice)


def _part(driver, host, wait, observation, downloads):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import _workspace_elements, _wait_for_visible_workspace_element as element
    assert os.environ.get('PIVOT_FLAGSHIP_PARTS_DATA') == '1', 'Dedicated 12001-row profile required'
    cell = wait.until(lambda d: next((c for c in _workspace_elements(d, '[role="gridcell"][data-colid="sales_sum"]') if c.is_displayed()), False))
    ActionChains(driver).context_click(cell).perform()
    s._geometry_copy_current_table(driver)
    number = wait.until(lambda d: next((e for e in _workspace_elements(d, 'input[type="number"][min="100"]') if e.is_displayed()), False))
    number.send_keys(Keys.CONTROL, 'a')
    number.send_keys('5000', Keys.TAB)
    parts = wait.until(lambda d: [b for b in _workspace_elements(d, 'button') if b.is_displayed() and b.text.strip().startswith('Part ')] or False)
    assert len(parts) == 3
    labels = [b.text for b in parts]
    assert '5,001' in labels[1] and '10,000' in labels[1]
    checkpoint = driver.execute_script('return performance.now()')
    text, html = None, None
    from tests.test_flagship_certification import _clipboard_text, _clipboard_html
    sentinel = 'part two pending'
    assert driver.execute_async_script("const done=arguments[1];navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false))", sentinel)
    parts[1].click()
    text = wait.until(lambda d: v if (v := _clipboard_text(d)) != sentinel else False)
    html = _clipboard_html(driver)
    requests = [e['envelope'] for e in _trace(driver) if e['phase'] == 'request' and e['at'] > checkpoint
                and e['envelope']['kind'] == 'export' and e['envelope']['payload'].get('partId')]
    assert len(requests) == 1
    rows = list(csv.reader(io.StringIO(text), delimiter='\t'))
    assert_part_rows(rows, requests[0])
    assert [[c['text'] for c in row] for row in ClipboardTable(html).rows] == rows
    observation.update(partLabels=labels, manifest=json.loads(Path(__file__).with_name('data').joinpath('flagship_parts_manifest.json').read_text()),
                       request=requests[0], firstRows=rows[:3], lastRows=rows[-3:], rowCount=len(rows)-1,
                       clipboardSha256=hashlib.sha256(text.encode()).hexdigest())
    artifact = downloads / 'part-2-clipboard.tsv'
    artifact.write_text(text, encoding='utf-8')
    (downloads / 'part-2-clipboard.html').write_text(html, encoding='utf-8')


def analysis_cells_match(cells, expected):
    """Compare settled displayed text at exact logical coordinates, uniquely."""
    return all([cell['text'] for cell in cells
                if (cell['rowid'], cell['colid']) == key] == [value]
               for key, value in expected.items())


def _analysis_format(driver):
    from tests.test_flagship_certification import _workspace_elements
    return ' | '.join(e.text for e in _workspace_elements(driver, '[title="Open number format options"]') if e.is_displayed())


def _analysis_two_decimal_display(driver, wait):
    from tests.test_flagship_certification import _workspace_elements, _wait_for_visible_workspace_element as element
    if not any(e.is_displayed() for e in _workspace_elements(driver, '[title="Increase decimal places"]')):
        element(driver, '[data-testid="pivot-toolbar-format"]').click()
    initial = _analysis_format(driver)
    assert '0 dp' in initial, ('Fresh analysis fixture must start at zero decimals', initial)
    # This fresh fixture starts at zero decimals and has no selected cells.
    # The global title also guards against accidentally formatting a selection.
    for _ in range(2):
        element(driver, '[title="Increase decimal places"]').click()
    display = wait.until(lambda d: (v if '2 dp' in (v := _analysis_format(d)) else False))
    return {'control': 'Format > Increase decimal places', 'clicks': 2,
            'decimalPlaces': 2, 'selectionScoped': False, 'initialDisplayFormat': initial, 'displayFormat': display}


def _analysis_cells(driver):
    """Read rendered cells whose centers are inside the actual grid viewport."""
    from tests.test_flagship_certification import _wait_for_visible_workspace_element as element
    scroller = element(driver, '[data-pivot-scroll-container]')
    return driver.execute_script(r"""
      const s=arguments[0],b=s.getBoundingClientRect();
      const top=Math.max(b.top,...[...s.querySelectorAll('[role=columnheader]')].map(e=>e.getBoundingClientRect().bottom));
      const hierarchy=[...s.querySelectorAll('[role=gridcell][data-colid="hierarchy"]')];
      const left=Math.max(b.left,...hierarchy.map(e=>e.getBoundingClientRect().right));
      return [...s.querySelectorAll('[role=gridcell][data-rowid][data-colid]')].filter(c=>{
        const r=c.getBoundingClientRect(),x=(r.left+r.right)/2,y=(r.top+r.bottom)/2;
        return r.width>0&&r.height>0&&y>Math.max(0,top)&&y<Math.min(innerHeight,b.bottom)
          &&x>Math.max(0,c.dataset.colid==='hierarchy'?b.left:left)&&x<Math.min(innerWidth,b.right);
      }).map(c=>({rowid:c.dataset.rowid,colid:c.dataset.colid,text:c.innerText.trim(),
        bounds:{x:c.getBoundingClientRect().x,y:c.getBoundingClientRect().y,width:c.getBoundingClientRect().width,height:c.getBoundingClientRect().height}}));
    """, scroller)


def _analysis_scroll(driver):
    from tests.test_flagship_certification import _wait_for_visible_workspace_element as element
    return driver.execute_script('const s=arguments[0];return {left:s.scrollLeft,maxLeft:s.scrollWidth-s.clientWidth}',
                                 element(driver, '[data-pivot-scroll-container]'))


def _analysis_column_step(driver, direction):
    """Use the grid's real keyboard navigation from an on-screen data cell."""
    from selenium.webdriver.common.keys import Keys
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import _wait_for_visible_workspace_element as element
    cells = [c for c in _analysis_cells(driver) if c['colid'] != 'hierarchy' and c['rowid'] != '__grand_total__']
    assert cells, 'No visible data cell available for native column navigation'
    anchor = (max if direction > 0 else min)(cells, key=lambda c: (c['bounds']['x'], c['bounds']['y']))
    order = [c for c in s._selection_column_order(driver) if c != 'hierarchy']
    if direction > 0:
        index = order.index(anchor['colid'])
        assert index+1 < len(order), 'No adjacent rendered column for native navigation'
        expected_column = order[index+1]
    before = _analysis_scroll(driver)
    s._selection_cell(driver, anchor['rowid'], anchor['colid']).click()
    grid = element(driver, '[data-pivot-scroll-container]')
    if direction > 0:
        grid.send_keys(Keys.ARROW_RIGHT)
    else:
        grid.send_keys(Keys.CONTROL, Keys.ARROW_LEFT)
        expected_column = next(c for c in s._selection_column_order(driver) if c != 'hierarchy')
    selected = s._selection_ids(driver)
    assert selected == [(anchor['rowid'], expected_column)], 'Native navigation stalled or skipped a logical column'
    return {'direction': direction, 'anchor': anchor, 'selected': selected,
            'before': before, 'scroll': _analysis_scroll(driver)}


def _analysis_display_columns(driver, wait, expected, observation):
    """Assert each logical column through bounded native keyboard navigation."""
    observation['navigation'] = []
    if _analysis_scroll(driver)['left'] > 0:
        observation['navigation'].append(_analysis_column_step(driver, -1))
    wait.until(lambda d: _analysis_scroll(d)['left'] == 0)
    observation['columnViews'] = []
    for column in dict.fromkeys(col for row, col in expected):
        wanted = {key: value for key, value in expected.items() if key[1] == column}
        def visible(d):
            cells = _analysis_cells(d)
            if analysis_cells_match(cells, wanted):
                return {'column': column, 'cells': cells, 'scroll': _analysis_scroll(d)}
            # Wrong text/row coordinates in a rendered column must time out;
            # scrolling cannot turn stale or rounded values into a pass.
            scroll = _analysis_scroll(d)
            if not any(c['colid'] == column for c in cells) and scroll['left'] < scroll['maxLeft']:
                step = _analysis_column_step(d, 1)
                observation['navigation'].append(step)
                assert step['scroll']['left'] > step['before']['left'], 'Native column navigation did not advance the viewport'
            return False
        observation['columnViews'].append(wait.until(visible))


def _analysis(driver, host, wait, observation, downloads):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import Select
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import _workspace_elements, _wait_for_visible_workspace_element as element, _click_export, _wait_for_download
    def agg_select():
        return next((select for chip in _workspace_elements(driver, '[data-sidebar-field-chip="zone"]')
                     if chip.is_displayed() and 'ventes' in chip.text.lower()
                     for select in chip.find_elements('css selector', 'select[title="Aggregation"]') if select.is_displayed()), None)
    observation['displayPrecondition'] = _analysis_two_decimal_display(driver, wait)
    if agg_select() is None:
        element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
    checkpoint = driver.execute_script('return performance.now()')
    Select(wait.until(lambda _: agg_select())).select_by_value('avg')
    wait.until(lambda d: any(any(r.get('_id') == 'Grand Total' and r.get('Q1_ventes_€_avg') == 75 for r in reply['payload'].get('data', []))
        for req, reply in _responses(d, checkpoint)))
    wait.until(lambda d: analysis_cells_match(_analysis_cells(d), {
        ('__grand_total__', 'Q1_ventes_€_avg'): '75.00'}))
    observation['averageCells'] = _analysis_cells(driver)
    header = element(driver, '[role="columnheader"][data-header-column-id="hierarchy"]')
    checkpoint = driver.execute_script('return performance.now()')
    ActionChains(driver).context_click(header).perform()
    s._selection_menu(driver, 'Expand All Rows').click()
    wait.until(lambda d: {'東京', 'القاهرة'}.issubset({label for _, label in s._navigation_rows(d)}))
    def expanded(d):
        for req, reply in reversed(_responses(d, checkpoint)):
            rows = {r.get('_path'): r for r in reply['payload'].get('data', []) if '_id' in r}
            if all(rows.get(path, {}).get('Q1_ventes_€_avg') == value for path, value in
                   [('Éclair|||東京', 100.25), ('München|||القاهرة', 49.75)]):
                assert req['state']['filters']['scope']['conditions'][0]['value'] == 'Included'
                return {'request': req, 'response': reply}
        return False
    observation['expandedResponse'] = wait.until(expanded)
    _analysis_display_columns(driver, wait, {
        ('Éclair|||東京', 'Q1_ventes_€_avg'): '100.25',
        ('München|||القاهرة', 'Q1_ventes_€_avg'): '49.75',
        ('__grand_total__', 'Q1_ventes_€_avg'): '75.00',
        ('Éclair|||東京', 'Q2_ventes_€_avg'): '20.00',
        ('München|||القاهرة', 'Q2_ventes_€_avg'): '30.00',
        ('__grand_total__', '__RowTotal__ventes_€_avg'): '50.00'},
        observation.setdefault('expandedDisplay', {}))
    observation['expandedCells'] = _analysis_cells(driver)
    observation['expandedRows'] = s._navigation_rows(driver)
    if any(e.is_displayed() for e in _workspace_elements(driver, '[data-sidebar-drop-zone="rows"]')):
        element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
    if s._totals_control_button(driver, 'Transpose') is None:
        s._click_totals_control(driver, 'View')
    checkpoint = driver.execute_script('return performance.now()')
    s._click_totals_control(driver, 'Transpose')
    def transposed(d):
        for req, reply in reversed(_responses(d, checkpoint)):
            if req['state'].get('rowFields') != ['period'] or req['state'].get('colFields') != ['region', 'account']:
                continue
            rows = {r.get('_id'): r for r in reply['payload'].get('data', []) if '_id' in r}
            if set(rows) != {'Q1', 'Q2', 'Grand Total'}:
                continue
            q1 = rows['Q1']
            if [q1.get(c) for c in ['Éclair|東京_ventes_€_avg', 'München|القاهرة_ventes_€_avg', '__RowTotal__ventes_€_avg']] == [100.25, 49.75, 75]:
                return {'request': req, 'response': reply}
        return False
    observation['transposed'] = wait.until(transposed)
    _analysis_display_columns(driver, wait, {
        ('Q1', 'Éclair|東京_ventes_€_avg'): '100.25',
        ('Q2', 'Éclair|東京_ventes_€_avg'): '20.00',
        ('Q1', 'München|القاهرة_ventes_€_avg'): '49.75',
        ('Q2', 'München|القاهرة_ventes_€_avg'): '30.00',
        ('Q1', '__RowTotal__ventes_€_avg'): '75.00',
        ('__grand_total__', '__RowTotal__ventes_€_avg'): '50.00'}, observation['transposed'])
    checkpoint = driver.execute_script('return performance.now()')
    started = time.time()
    _click_export(driver, 'csv')
    downloaded = _wait_for_download(downloads, 'csv', timeout=90, started_at=started)
    s.assert_single_exact_download(downloads, downloaded)
    parsed = list(csv.reader(io.StringIO(downloaded.read_text(encoding='utf-8-sig'))))
    requests = [e['envelope'] for e in _trace(driver) if e['phase'] == 'request' and e['at'] > checkpoint
                and e['envelope']['kind'] == 'export' and e['envelope']['payload'].get('format') == 'csv']
    assert len(requests) == 1
    assert_analysis_csv(parsed, requests[0])
    observation.update(parsedCsv=parsed, exportRequest=requests[0], download={'file': downloaded.name,
        'sha256': hashlib.sha256(downloaded.read_bytes()).hexdigest(), 'bytes': downloaded.stat().st_size})
    wait.until(lambda d: d.execute_script('return window.__pivotExactExportPending === 0'))
    observation['jobId'] = s.assert_exact_export_delivery(driver.execute_script('return window.__pivotExactExports || []'), 'csv', checkpoint,
        observation['download']['sha256'], downloaded.name, downloaded.stat().st_size)


def assert_analysis_csv(rows, request):
    from tests.export_qualification_fixture import FLAGSHIP_QUALIFICATION_SOURCE_ROWS
    state, payload = request['state'], request['payload']
    assert state['rowFields'] == ['period'] and state['colFields'] == ['region', 'account']
    measures = [('ventes_€', 'avg'), ('unités', 'sum'), ('écart', 'sum')]
    assert [(c['field'], c['agg']) for c in state['valConfigs']] == measures
    assert state['filters']['scope']['conditions'][0]['value'] == 'Included'
    ids = payload['colIds']
    # Transpose creates new pivot groups; the existing total group retains its
    # saved leaf order across the sum-to-average change (stable measure identity).
    assert ids == ['hierarchy'] + [f'{prefix}_{field}_{agg}'
        for prefix in ('Éclair|東京', 'München|القاهرة') for field, agg in measures] + [
            '__RowTotal__unités_sum', '__RowTotal__ventes_€_avg', '__RowTotal__écart_sum']
    assert len(rows) == 4 and [r[0] for r in rows[1:]] == ['Q1', 'Q2', 'Grand Total']
    assert rows[0] == [payload['style']['headerLabels'].get(c, c) for c in ids]
    source = [r for r in FLAGSHIP_QUALIFICATION_SOURCE_ROWS if r['scope'] == 'Included']
    for period, actual in zip(('Q1', 'Q2', None), rows[1:]):
        assert len(actual) == len(ids)
        for col, cell in zip(ids[1:], actual[1:]):
            field, agg = next((field, agg) for field, agg in (('unités', 'sum'), ('ventes_€', 'avg'), ('écart', 'sum')) if col.endswith('_'+field+'_'+agg))
            subset = [r for r in source if period is None or r['period'] == period]
            if not col.startswith('__RowTotal__'):
                prefix = col[:-(len(field)+len(agg)+2)]
                region, account = prefix.split('|')
                subset = [r for r in subset if r['region'] == region and r['account'] == account]
            expected = sum(r[field] for r in subset) / (len(subset) if agg == 'avg' else 1)
            assert float(cell) == expected, (period, col, cell, expected)


HANDLERS = dict(zip(REQUIREMENTS, (_save_scroll, _merged, _unloaded, _part, _selection_journey, _analysis)))


def qualify_journeys(host, requirement):
    import pytest
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests import flagship_browser_support as s
    from tests.test_flagship_certification import (_qualification_chrome_options, _switch_to_workspace,
        _verify_release_asset_batch, _host_url, _origin, _workspace_elements)
    if os.environ.get('PIVOT_RUN_FLAGSHIP_BROWSER_TESTS') != '1':
        pytest.skip('Enable installed flagship browser qualification explicitly')
    assert requirement in HANDLERS
    binding = load_artifact_binding(Path(os.environ['PIVOT_FLAGSHIP_ARTIFACT_BINDING']))
    part_manifest_digest = assert_journey_fixture_binding(requirement, binding.sha256)
    directory = Path(os.environ['PIVOT_FLAGSHIP_EVIDENCE_ROOT']) / requirement / host
    assert ('runs', os.environ['PIVOT_FLAGSHIP_RUN_ID']) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / 'downloads'
    downloads.mkdir(exist_ok=True)
    result = {'result': 'blocked', 'browser': 'unavailable', 'infrastructureUnavailable': True}
    observation, driver, paths = {'requirement': requirement, 'partManifestSha256': part_manifest_digest}, None, []
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        result.update(result='failed', browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}", infrastructureUnavailable=False)
        driver.execute_cdp_cmd('Network.enable', {'maxTotalBufferSize': 50000000, 'maxResourceBufferSize': 50000000})
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': PROBE + '\n' + s._EXACT_EXPORT_PROBE_SCRIPT})
        driver.execute_cdp_cmd('Browser.grantPermissions', {'origin': _origin(_host_url(host)), 'permissions': ['clipboardReadWrite', 'clipboardSanitizedWrite']})
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 45)
        wait.until(lambda d: any(c.is_displayed() for c in _workspace_elements(d, '[role="gridcell"]')) and _responses(d))
        HANDLERS[requirement](driver, host, wait, observation, downloads)
        assert not any(e.is_displayed() for e in _workspace_elements(driver, '[role="alert"]'))
        result.update(result='passed', actionCompleted=True, v1RequestsCorrelated=True)
        result[ACCEPTANCE_CHECKS[requirement]] = True
        observation['diagnostic'] = ''
    except Exception:
        observation['diagnostic'] = traceback.format_exc()
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result['loadedAssetSha256'] = dict(driver._pivot_loaded_assets)
                observation['trace'] = _trace(driver)
                if requirement == 'journey-analysis.aggregate-expand-swap-export':
                    try:
                        observation['finalCells'] = _analysis_cells(driver)
                        observation['finalDisplayFormat'] = _analysis_format(driver)
                        observation['finalScroll'] = _analysis_scroll(driver)
                    except Exception:
                        observation['analysisSnapshotDiagnostic'] = traceback.format_exc()
                observation['exportDeliveryTrace'] = driver.execute_script('return window.__pivotExactExports || []')
                screenshot = directory / 'final.png'
                driver.save_screenshot(str(screenshot)); paths.append(str(screenshot))
            except Exception:
                result['result'] = 'blocked'
                observation['evidenceDiagnostic'] = traceback.format_exc()
            finally:
                driver.quit()
        observation.update(result=result.copy(), loadedAssetSha256=result.get('loadedAssetSha256', {}))
        path = directory / 'observation.json'
        path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding='utf-8')
        result['evidence_paths'] = paths + [str(path)] + [str(p) for p in downloads.iterdir() if p.is_file()]
        s.record_case(requirement, host, result, directory)
    assert result['result'] == 'passed', f"{host}/{requirement}: {observation['diagnostic']}; evidence: {directory}"
