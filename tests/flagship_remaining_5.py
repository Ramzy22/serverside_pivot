"""Installed host integration assertions; probes retain every actual wire message.

Only public saved-view configuration is seeded. Native controls drive all actions;
probes observe requests, AbortSignals, and real listener registration/removal.
"""
import json
import os
from pathlib import Path
import time
import traceback
from urllib.parse import urlsplit

from tests import flagship_browser_support as support
from tests.flagship_axis_swap import AXIS_PROBE, assert_axis_rows
from tests.flagship_remaining_1 import starting_state, cells, logical_values, _element

HOST_REQUIREMENTS = {
    'dash': 'dash-callback-transport.v1',
    'react': 'react-direct-transport.v1',
    'streamlit': 'streamlit-no-rerun.local-interactions',
}

HOST_PROBE = r"""
if (!window.__pivotHostProbe) {
  const records=[], listeners=[];
  const parse = value => {try {return JSON.parse(value);} catch (_) {return value ?? null;}};
  const add=EventTarget.prototype.addEventListener, remove=EventTarget.prototype.removeEventListener;
  const capture=options => typeof options==='boolean' ? options : !!options?.capture;
  EventTarget.prototype.addEventListener=function(type, listener, options) {
    if ((this===window || this===document) && listener && !options?.once) {
      if (!listeners.some(entry=>entry.target===this && entry.type===type && entry.listener===listener && entry.capture===capture(options) && entry.active))
        listeners.push({id:listeners.length,target:this,type,listener,capture:capture(options),active:true});
    }
    return add.call(this,type,listener,options);
  };
  EventTarget.prototype.removeEventListener=function(type, listener, options) {
    for (const entry of listeners) if (entry.target===this && entry.type===type && entry.listener===listener && entry.capture===capture(options)) entry.active=false;
    return remove.call(this,type,listener,options);
  };
  const nativeFetch=window.fetch.bind(window);
  window.fetch=(input, init={}) => {
    const record={url:String(input?.url || input),method:init.method || input?.method || 'GET',
      body:parse(init.body),at:performance.now(),signalPresent:!!(init.signal || input?.signal),aborted:false,finished:false};
    records.push(record);
    const signal=init.signal || input?.signal;
    const aborted=()=>{record.aborted=true;record.abortAt=performance.now();};
    signal?.addEventListener('abort',aborted,{once:true});
    if (signal?.aborted) aborted();
    return nativeFetch(input,init).then(async response=>{
      record.httpStatus=response.status;
      try {record.response=parse(await response.clone().text());} catch(error) {record.readError=error.name;}
      return response;
    }).catch(error=>{record.error=error.name;throw error;}).finally(()=>{
      record.finished=true;record.finishedAt=performance.now();signal?.removeEventListener('abort',aborted);
    });
  };
  const open=XMLHttpRequest.prototype.open, send=XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open=function(method,url,...args) {this.__hostWire={method,url:String(url)};return open.call(this,method,url,...args);};
  XMLHttpRequest.prototype.send=function(body) {
    const record={...this.__hostWire,body:parse(body),at:performance.now(),finished:false}; records.push(record);
    this.addEventListener('loadend',()=>{record.httpStatus=this.status;record.response=parse(this.responseText);record.finished=true;record.finishedAt=performance.now();},{once:true});
    return send.call(this,body);
  };
  window.__pivotHostProbe={records,listeners:()=>listeners.map(({id,type,target,active})=>({id,type,target:target===window?'window':'document',active}))};
}
"""


def envelopes(value):
    """Retain malformed candidates too; schema validity is asserted afterwards."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return []
    if isinstance(value, dict):
        if 'schemaVersion' in value or ('kind' in value and 'payload' in value):
            return [value]
        return [item for child in value.values() for item in envelopes(child)]
    if isinstance(value, list):
        return [item for child in value for item in envelopes(child)]
    return []


def assert_envelope(value, *, response=False):
    assert isinstance(value, dict) and value.get('schemaVersion') == 1, 'Non-V1 wire envelope'
    assert isinstance(value.get('requestId'), str) and value['requestId'], 'Missing V1 identity'
    assert value.get('kind') in {'data', 'filter_options', 'chart', 'detail', 'drill', 'update', 'transaction', 'export'}, 'Unknown runtime kind'
    assert isinstance(value.get('payload'), dict), 'Missing semantic payload'
    if response:
        assert value.get('status') in {'data', 'error', 'stale', 'ok', 'pending', 'cancelled', 'complete', 'accepted', 'columns', 'export', 'filter_options', 'chart', 'detail'}, 'Missing V1 response status'


def assert_wire(records, host):
    callbacks = [record for record in records if '/_dash-update-component' in record['url']]
    direct = [record for record in records if urlsplit(record['url']).path.rstrip('/') == '/v1/runtime']
    if host == 'react':
        assert not callbacks, 'React requested a Dash callback endpoint'
        assert direct, 'No direct HTTP runtime requests'
        relevant = direct
    else:
        assert host == 'dash' and callbacks, 'No actual Dash callback wrapper observed'
        relevant = []
        for record in callbacks:
            body = record.get('body')
            assert isinstance(body, dict) and isinstance(body.get('inputs'), list) and body.get('output'), 'Invalid Dash callback request wrapper'
            runtime_inputs = [item for item in body['inputs'] if item.get('property') == 'runtimeRequest']
            if runtime_inputs:
                # Validate the exact callback input, including malformed values;
                # finding some other valid nested envelope cannot hide it.
                for item in runtime_inputs:
                    assert_envelope(item.get('value'))
                if record.get('response') is not None:
                    wrapper = record['response']
                    assert isinstance(wrapper, dict) and isinstance(wrapper.get('response'), dict), 'Missing callback output wrapper'
                    outputs = [props['runtimeResponse'] for props in wrapper['response'].values() if isinstance(props, dict) and 'runtimeResponse' in props]
                    assert len(outputs) == 1, 'Callback omitted runtimeResponse output'
                relevant.append(record)
        assert relevant, 'No actual runtimeRequest callback inputs'
    correlated = []
    for record in relevant:
        if host == 'react':
            assert record.get('method', '').upper() == 'POST'
            assert_envelope(record.get('body'))
        requests = envelopes(record.get('body'))
        assert requests, 'Runtime request disappeared from exhaustive wire inventory'
        responses = envelopes(record.get('response'))
        for request in requests:
            assert_envelope(request)
        for response in responses:
            assert_envelope(response, response=True)
            request = next((item for item in requests if item['requestId'] == response['requestId']), None)
            assert request is not None and request['kind'] == response['kind'], 'Uncorrelated response identity'
            for key in ('table', 'clientInstance', 'stateEpoch', 'windowSeq'):
                if key in response:
                    assert response[key] == request.get(key), f'Changed V1 {key}'
            correlated.append((request, response))
        if record.get('finished') and not record.get('error') and record.get('httpStatus') != 204:
            assert responses, 'Completed runtime call returned no V1 envelope'
    assert correlated, 'No correlated successful transport delivery'
    fixture = []
    for request, response in correlated:
        state = request.get('state') or {}
        if state.get('rowFields') != ['region', 'account'] or state.get('colFields') != ['period']:
            continue
        conditions = state.get('filters', {}).get('scope', {}).get('conditions', [])
        if not any(item.get('value') == 'Included' and item.get('type') == 'eq' for item in conditions):
            continue
        grand = next((row for row in response['payload'].get('data', []) if row.get('_isTotal') and row.get('_id') == 'Grand Total'), None)
        if grand and [grand.get('__RowTotal__' + field + '_sum') for field in ('ventes_\u20ac', 'unit\u00e9s', '\u00e9cart')] == [200, 10, -6.75]:
            # General V1 metadata is optional; the installed fixture explicitly
            # qualifies the richer correlated identity/freshness contract.
            for value in (request, response):
                for key in ('table', 'clientInstance'):
                    assert isinstance(value.get(key), str) and value[key], f'Missing typed fixture {key}'
                for key in ('stateEpoch', 'windowSeq'):
                    assert type(value.get(key)) is int and value[key] >= 0, f'Missing typed fixture {key}'
            fixture.append(response['requestId'])
    assert fixture, 'No semantic Included layout with authoritative 200/10/-6.75 totals'
    payloads = [record for record in records if '/v1/runtime/payload/' in record['url']]
    for record in payloads:
        assert record['method'].upper() == 'GET' and record.get('httpStatus') == 200
    return {'runtimeCalls': len(relevant), 'callbackCalls': len(callbacks), 'directCalls': len(direct),
            'correlatedIds': [response['requestId'] for _, response in correlated],
            'fixtureIds': fixture, 'directPayloadFetches': len(payloads)}


def assert_lifecycle(pending, after, baseline, mounted_listeners, final_listeners):
    assert pending and all(item['signalPresent'] and not item['finished'] for item in pending)
    expected = {item['at'] for item in pending}
    cancelled = [item for item in after if item['at'] in expected]
    assert len(cancelled) == len(pending)
    assert all(item['aborted'] and item['finished'] and item.get('error') == 'AbortError' for item in cancelled), 'Unmount left an owned HTTP request running'
    original = {item['id'] for item in baseline if item['active']}
    added = {item['id'] for item in mounted_listeners if item['active']} - original
    assert added, 'No actual component document/window listener registration observed'
    remaining = {item['id'] for item in final_listeners if item['active']}
    assert not (added & remaining), 'Unmount left component listeners registered'
    return {'abortedRequestCount': len(cancelled), 'removedListenerCount': len(added), 'pendingRequestsAfterUnmount': 0}


def assert_no_rerun(samples, before, after):
    assert len(samples) >= 3 and all(sample == samples[0] and isinstance(sample, int) and sample >= 1 for sample in samples), 'Streamlit script reran during a local interaction'
    assert before['selection'], 'A real native grid selection is required before local interactions'
    assert before['selection'] == after['selection'] and before['search'] == after['search'], 'Local interaction changed selection/filter'
    assert before['requestState'].get('rowFields') == ['region', 'account'] and before['requestState'].get('colFields') == ['period'], 'Missing observed grid model'
    assert before['requestState'] == after['requestState'], 'Local interaction changed the grid model'


def fixture_visible(driver):
    values = logical_values(cells(driver))
    expected = {'__RowTotal__ventes_\u20ac_sum': '200.00', '__RowTotal__unit\u00e9s_sum': '10.00', '__RowTotal__\u00e9cart_sum': '-6.75'}
    return all(values.get(('__grand_total__', column)) == text for column, text in expected.items())


def _records(driver):
    return driver.execute_script('return window.__pivotHostProbe.records')


def _listeners(driver):
    return driver.execute_script('return window.__pivotHostProbe.listeners()')


def _click(driver, selector):
    element = _element(driver, selector)
    driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'center'})", element)
    element.click()
    return element


def _seed(driver, host):
    state = starting_state('host-transport')
    state['columnSizing'] = {'hierarchy': 220, **{period + field + '_sum': 112
        for period in ('Q1_', 'Q2_', '__RowTotal__') for field in ('ventes_\u20ac', 'unit\u00e9s', '\u00e9cart')}}
    if host == 'streamlit':
        state['chartCanvasPanes'] = [{'id': 'host-local-chart', 'name': 'Host lifecycle chart', 'source': 'table',
                                     'chartType': 'bar', 'size': 1, 'dockPosition': 'right', 'floating': False, 'width': 360}]
    snapshot = {'version': 1, 'table': 'revenue_operations', 'state': state}
    source = f"localStorage.setItem({json.dumps(f'flagship-{host}-pivot-savedView')},{json.dumps(json.dumps(snapshot))});"
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': source})
    return state


def _fault_marker(name):
    root = Path(os.environ['PIVOT_FLAGSHIP_EVIDENCE_ROOT']).resolve()
    fault = Path(os.environ['PIVOT_FLAGSHIP_TEST_FAULT_FILE']).resolve()
    assert fault == root / 'fail-next-data-request', 'Server fault marker is outside the guarded private path'
    return root / name


def _show_status(driver):
    from tests.test_flagship_certification import _workspace_elements
    if not any(item.is_displayed() for item in _workspace_elements(driver, '[data-pivot-status-action="refresh"]')):
        _click(driver, '[data-pivot-status-action="toggle-details"]')


def _dash_recovery(driver, wait, observation):
    _show_status(driver)
    before = len(_records(driver))
    marker = _fault_marker('fail-next-data-request')
    marker.write_text('one-shot', encoding='ascii')
    try:
        _click(driver, '[data-pivot-status-action="refresh"]')
        wait.until(lambda current: support._visible_error(current))
        failed = [item for record in _records(driver)[before:] for item in envelopes(record.get('response')) if item.get('status') == 'error']
        assert failed and not marker.exists()
        assert 'PIVOT_QUALIFICATION_PRIVATE_SENTINEL' not in support._visible_workspace_text(driver)
        from tests.test_flagship_certification import _workspace_elements
        assert not any(item.is_displayed() for item in _workspace_elements(driver, '[data-pivot-loading-indicator="global"]'))
        checkpoint = len(_records(driver))
        retry = _element(driver, '[role="alert"] button')
        assert retry.text == 'Retry'
        retry.click()
        wait.until(lambda current: fixture_visible(current) and not support._visible_error(current))
        recovered = [item for record in _records(driver)[checkpoint:] for item in envelopes(record.get('response')) if item.get('status') == 'data']
        assert recovered and all(item['requestId'] != failed[-1]['requestId'] for item in recovered)
        observation['recovery'] = {'failedId': failed[-1]['requestId'], 'retryIds': [item['requestId'] for item in recovered], 'privateExceptionHidden': True, 'loadingCleared': True}
    finally:
        marker.unlink(missing_ok=True)


def _dash_stale(driver, wait, observation):
    from selenium.webdriver.common.keys import Keys
    driver.execute_script("window.__pivotSupersession.arm('data')")
    search = _element(driver, '[data-testid="pivot-global-search"]')
    search.send_keys('\u00c9clair')
    wait.until(lambda current: any(item['phase'] == 'ready' for item in current.execute_script('return window.__pivotSupersession.trace')))
    search.send_keys(Keys.CONTROL, 'a', Keys.BACKSPACE)
    wait.until(lambda current: _element(current, '[data-testid="pivot-global-search"]').get_attribute('value') == '')
    _element(driver, '[data-testid="pivot-global-search"]').send_keys('M\u00fcnchen')
    wait.until(lambda current: any(item['phase'] == 'response' and item['term'] == 'M\u00fcnchen' and item['total'] == 79.75 for item in current.execute_script('return window.__pivotSupersession.trace')))
    # Exact semantic leaf values are checked at 2dp, independently of response order.
    expected = {period + field + '_sum': f'{value:.2f}'
                for period, numbers in (('Q1_', (49.75, 3, 4.25)), ('Q2_', (30, 4, -1)), ('__RowTotal__', (79.75, 7, 3.25)))
                for field, value in zip(('ventes_\u20ac', 'unit\u00e9s', '\u00e9cart'), numbers)}
    def visible(current):
        values = logical_values(cells(current))
        return all(values.get((row, column)) == text for row in ('M\u00fcnchen', '__grand_total__') for column, text in expected.items()) and not any(row == '\u00c9clair' for row, _ in values)
    wait.until(visible)
    newer = cells(driver)
    samples = driver.execute_async_script(support._SUPERSESSION_OBSERVE_SCRIPT)
    trace = driver.execute_script('return window.__pivotSupersession.trace')
    old, new = support.supersession_ids(trace)
    assert support.assert_superseded_response(trace, old, new, 79.75)
    assert support.assert_stable_visible_snapshots(newer, samples) and visible(driver)
    assert not support._visible_error(driver) and not driver.execute_script('return window.__pivotSupersession.alerts')
    observation['supersession'] = {'trace': trace, 'newerCells': newer, 'postReleaseSamples': samples}


def _react_lifecycle(driver, wait, observation):
    toggle = '[data-qualification-workspace-toggle]'
    _click(driver, toggle)
    wait.until(lambda current: not cells(current))
    baseline = _listeners(driver)
    _click(driver, toggle)
    wait.until(lambda current: fixture_visible(current))
    mounted = _listeners(driver)
    _show_status(driver)
    marker = _fault_marker('delay-next-data-request')
    checkpoint = len(_records(driver))
    marker.write_text('one-shot', encoding='ascii')
    try:
        _click(driver, '[data-pivot-status-action="refresh"]')
        pending = wait.until(lambda current: [record for record in _records(current)[checkpoint:]
            if urlsplit(record['url']).path == '/v1/runtime' and record.get('method') == 'POST' and not record['finished']])
        wait.until(lambda _: not marker.exists())
        _click(driver, toggle)
        wait.until(lambda current: not cells(current))
        wait.until(lambda current: all(record['finished'] for record in _records(current)[checkpoint:]))
        after, final_listeners = _records(driver), _listeners(driver)
        observation['lifecycle'] = assert_lifecycle(pending, after, baseline, mounted, final_listeners)
        observation['lifecycle'].update(pending=pending, after=after[checkpoint:], baselineListeners=baseline, mountedListeners=mounted, finalListeners=final_listeners)
        _click(driver, toggle)
        wait.until(lambda current: fixture_visible(current))
    finally:
        marker.unlink(missing_ok=True)


def _raw_snapshot(driver, state):
    trace = driver.execute_script('return window.__pivotAxisTrace || []')
    requests = {item['requestId']: item for item in trace if item['phase'] == 'request'
                and item.get('rowFields') == state['rowFields'] and item.get('colFields') == state['colFields']
                and item.get('valueAggregations') == [{'field': value['field'], 'agg': value['agg']} for value in state['valConfigs']]
                and any(condition.get('type') == 'eq' and condition.get('value') == 'Included'
                        for condition in item.get('filters', {}).get('scope', {}).get('conditions', []))}
    for item in reversed(trace):
        if item['phase'] == 'response' and item.get('status') == 'data' and item['requestId'] in requests:
            rows = item.get('rows') or []
            assert_axis_rows(rows, transposed=False)
            numeric = {(row['_path'], column): value for row in rows if '_id' in row
                       for column, value in row.items() if column.endswith('_sum') and isinstance(value, (int, float))}
            observed = logical_values(cells(driver))
            if any(key in observed for key in numeric):
                return {'requestId': item['requestId'], 'rawRows': rows, 'numeric': numeric}
    return False


def _counter(driver):
    return int(driver.find_element('css selector', '[data-pivot-script-runs]').get_attribute('data-pivot-script-runs'))


def _local_state(driver):
    requests = [item for record in _records(driver) for item in envelopes(record.get('body'))
                if item.get('kind') == 'data' and isinstance(item.get('state'), dict)]
    assert requests, 'No actual HTTP grid-model observation'
    return {'selection': sorted(support._selection_ids(driver)),
            'search': _element(driver, '[data-testid="pivot-global-search"]').get_attribute('value'),
            'requestState': requests[-1]['state']}


def _stable_counter(driver, expected, duration=1.5):
    samples = []
    end = time.monotonic() + duration
    while time.monotonic() < end:
        samples.append(_counter(driver))
        assert samples[-1] == expected, 'A local action reran the actual Streamlit script'
        time.sleep(.15)
    return samples


def _streamlit_local(driver, wait, state, observation):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
    from tests.flagship_remaining_1 import _select
    _select(driver, [('\u00c9clair', 'Q1_ventes_\u20ac_sum')])
    raw = wait.until(lambda current: _raw_snapshot(current, state))
    numeric = raw['numeric']
    def values_valid():
        current = logical_values(cells(driver))
        observed = [(key, value) for key, value in current.items() if key in numeric]
        assert observed and all(float(value.replace(',', '')) == numeric[key] for key, value in observed), 'Native local action changed authoritative cell values'
        assert len(current) == len(cells(driver))
    def observe_action(name, action, changed):
        before = _local_state(driver)
        counter = _counter(driver)
        initial = _stable_counter(driver, counter, .4)
        action()
        wait.until(changed)
        samples = initial + _stable_counter(driver, counter)
        after = _local_state(driver)
        assert_no_rerun(samples, before, after)
        values_valid()
        observation.setdefault('localInteractions', []).append({'action': name, 'scriptRuns': samples, 'before': before, 'after': after, 'cells': cells(driver)})
    grid = _element(driver, '[data-pivot-scroll-container]')
    driver.execute_script("arguments[0].scrollIntoView({block:'center'})", grid)
    offset = lambda: driver.execute_script('return arguments[0].scrollLeft', grid)
    original = offset()
    observe_action('native-horizontal-scroll', lambda: ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(grid), 300, 0).perform(), lambda _: offset() > original + 2)
    moved = offset()
    observe_action('native-horizontal-return', lambda: ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(grid), -300, 0).perform(), lambda _: offset() < moved - 2)
    pane = _element(driver, '[data-docked-chart-pane]')
    width = lambda: driver.execute_script('return arguments[0].getBoundingClientRect().width', pane)
    before_width = width()
    handle = _element(driver, '[title="Resize workspace panes"]')
    observe_action('native-chart-pane-resize', lambda: ActionChains(driver).move_to_element(handle).click_and_hold().move_by_offset(-40, 0).release().perform(), lambda _: abs(width() - before_width) >= 20)
    observation['scriptRunCounterUnchanged'] = True
    observation['chartGeometry'] = {'beforeWidth': before_width, 'afterWidth': width()}
    assert abs(observation['chartGeometry']['afterWidth'] - before_width - 40) <= 2


def qualify_hosts(host, requirement):
    import pytest
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import _qualification_chrome_options, _switch_to_workspace, _verify_release_asset_batch, _host_url
    assert HOST_REQUIREMENTS.get(host) == requirement
    if os.environ.get('PIVOT_RUN_FLAGSHIP_BROWSER_TESTS') != '1':
        pytest.skip('Enable installed flagship browser qualification explicitly')
    assert os.environ.get('PIVOT_FLAGSHIP_ARTIFACT_BINDING'), 'Installed artifact binding is required'
    binding = load_artifact_binding(Path(os.environ['PIVOT_FLAGSHIP_ARTIFACT_BINDING']))
    run_id = os.environ['PIVOT_FLAGSHIP_RUN_ID']
    directory = Path(os.environ['PIVOT_FLAGSHIP_EVIDENCE_ROOT']) / 'host-integration' / requirement / host
    assert ('runs', run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / 'downloads'; downloads.mkdir(exist_ok=True)
    result = {'result': 'blocked', 'browser': 'unavailable', 'infrastructureUnavailable': True}
    observation, paths, driver = {'requirement': requirement, 'host': host}, [], None
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        result.update(result='failed', browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}", infrastructureUnavailable=False)
        driver._pivot_artifact_binding, driver._pivot_artifact_host, driver._pivot_loaded_assets = binding, host, {}
        driver.execute_cdp_cmd('Network.enable', {'maxTotalBufferSize': 50000000, 'maxResourceBufferSize': 50000000})
        for source in (HOST_PROBE, AXIS_PROBE) + ((support._SUPERSESSION_PROBE_SCRIPT,) if host == 'dash' else ()):
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': source})
        state = _seed(driver, host)
        if host == 'react':
            # Native public consumer control exists only under this explicit example profile.
            driver.get(_host_url(host) + ('&' if '?' in _host_url(host) else '?') + 'qualification=1')
            driver._pivot_workspace_shadow = False
        else:
            _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 30)
        raw = wait.until(lambda current: _raw_snapshot(current, state))
        observation['rawInitial'] = {key: value for key, value in raw.items() if key != 'numeric'}
        if host != 'streamlit':
            wait.until(lambda current: fixture_visible(current))
        observation['initialCells'] = cells(driver)
        if host == 'dash':
            _dash_recovery(driver, wait, observation)
            _dash_stale(driver, wait, observation)
        elif host == 'react':
            _react_lifecycle(driver, wait, observation)
        else:
            _streamlit_local(driver, wait, state, observation)
        if host != 'streamlit':
            observation['wireSummary'] = assert_wire(_records(driver), host)
        console = driver.get_log('browser')
        observation['console'] = console
        sizing_errors = [entry for entry in console if any(term in entry.get('message', '').lower() for term in ('permission', 'resizeobserver', 'iframe', 'cross-origin')) and entry.get('level') in ('SEVERE', 'ERROR')]
        assert not sizing_errors, f'Host sizing/permission errors: {sizing_errors}'
        result.update(result='passed', actionCompleted=True, v1RequestsCorrelated=host != 'streamlit', fixtureTotalsVerified=True)
        if host == 'dash':
            result.update(callbackWrappersVerified=True, errorRecoveryVerified=True, staleCallbackRejected=True)
        elif host == 'react':
            result.update(allRequestsV1=True, dashCallbacksAbsent=True, consumerUnmountAbortedRequests=True, componentListenersReleased=True)
        else:
            result.update(scriptRunCounterUnchanged=True, nativeScrollVerified=True, nativeResizeVerified=True, gridStatePreserved=True, sizingPermissionErrorsAbsent=True)
    except Exception:
        observation['diagnostic'] = traceback.format_exc()
    finally:
        if driver:
            try:
                observation['wire'] = _records(driver)
                observation['finalCells'] = cells(driver)
                observation['axisTrace'] = driver.execute_script('return window.__pivotAxisTrace || []')
                _verify_release_asset_batch(driver, require_assets=True)
                result['loadedAssetSha256'] = dict(driver._pivot_loaded_assets)
                path = directory / 'final.png'; driver.save_screenshot(str(path)); paths.append(str(path))
            except Exception:
                result['result'] = 'blocked'
                observation['evidenceDiagnostic'] = traceback.format_exc()
            finally:
                driver.quit()
        observation['result'] = result.copy()
        path = directory / 'observation.json'
        path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding='utf-8')
        paths.append(str(path)); result['evidence_paths'] = paths
        support.record_case(requirement, host, result, directory)
    assert result['result'] == 'passed', f"{host} {requirement}: {observation.get('diagnostic', '')}; evidence: {directory}"
