"""Mutation tests for host evidence, plus real guarded adapter execution."""
import asyncio
import copy
from pathlib import Path
import pytest

from tests.flagship_remaining_5 import HOST_REQUIREMENTS, assert_wire, assert_lifecycle, assert_no_rerun


def wire(host='react'):
    request = {'schemaVersion': 1, 'kind': 'data', 'requestId': 'window-a', 'table': 'revenue_operations',
               'clientInstance': 'tab', 'stateEpoch': 2, 'windowSeq': 3,
               'state': {'rowFields': ['region', 'account'], 'colFields': ['period'],
                         'filters': {'scope': {'conditions': [{'type': 'eq', 'value': 'Included'}]}}},
               'payload': {'intent': 'viewport', 'start': 0, 'end': 50}}
    response = {key: value for key, value in request.items() if key != 'state'}
    response.update(status='data', payload={'data': [{'_id': 'Grand Total', '_isTotal': True,
                     '__RowTotal__ventes_\u20ac_sum': 200, '__RowTotal__unit\u00e9s_sum': 10, '__RowTotal__\u00e9cart_sum': -6.75}]})
    if host == 'dash':
        return [{'url': '/_dash-update-component', 'method': 'POST', 'finished': True, 'httpStatus': 200,
                 'body': {'output': 'pivot.runtimeResponse', 'inputs': [{'id': 'pivot', 'property': 'runtimeRequest', 'value': request}]},
                 'response': {'multi': True, 'response': {'pivot': {'runtimeResponse': response}}}}]
    return [{'url': '/v1/runtime', 'method': 'POST', 'finished': True, 'httpStatus': 200, 'body': request, 'response': response}]


@pytest.mark.parametrize('host', ['dash', 'react'])
def test_transport_oracle_accepts_only_correlated_semantic_wire_fixture(host):
    assert assert_wire(wire(host), host)['fixtureIds'] == ['window-a']
    for damage in ('schema', 'identity', 'epoch', 'layout', 'filter', 'total', 'missing-response'):
        bad = copy.deepcopy(wire(host))
        request = bad[0]['body'] if host == 'react' else bad[0]['body']['inputs'][0]['value']
        response = bad[0]['response'] if host == 'react' else bad[0]['response']['response']['pivot']['runtimeResponse']
        if damage == 'schema': request['schemaVersion'] = 2
        elif damage == 'identity': response['requestId'] = 'foreign'
        elif damage == 'epoch': response['stateEpoch'] = 1
        elif damage == 'layout': request['state']['rowFields'] = ['account']
        elif damage == 'filter': request['state']['filters'] = {}
        elif damage == 'total': response['payload']['data'][0]['__RowTotal__\u00e9cart_sum'] = -7
        else: bad[0]['response'] = None
        with pytest.raises(AssertionError): assert_wire(bad, host)


def test_exhaustive_inventory_does_not_hide_non_v1_or_callback_calls():
    bad = wire() + [{'url': '/v1/runtime', 'method': 'POST', 'body': {'legacy': True}, 'finished': True}]
    with pytest.raises(AssertionError, match='Non-V1'): assert_wire(bad, 'react')
    with pytest.raises(AssertionError, match='Dash callback'): assert_wire(wire() + wire('dash'), 'react')
    bad = wire('dash'); bad[0]['body']['inputs'][0]['value'] = {'schemaVersion': 2}
    # A valid unrelated envelope cannot mask the malformed actual callback input.
    bad[0]['body']['state'] = [{'value': wire()[0]['body']}]
    with pytest.raises(AssertionError): assert_wire(bad, 'dash')
    bad = wire('dash'); bad[0]['response']['response'] = {'other': {'data': wire()[0]['response']}}
    with pytest.raises(AssertionError, match='runtimeResponse'): assert_wire(bad, 'dash')


def test_lifecycle_requires_real_pending_signal_abort_finally_and_listener_unsubscribe():
    pending = [{'at': 12, 'signalPresent': True, 'finished': False}]
    done = [{'at': 12, 'aborted': True, 'finished': True, 'error': 'AbortError'}]
    baseline = [{'id': 1, 'active': True}]
    mounted = baseline + [{'id': 2, 'active': True}]
    final = baseline + [{'id': 2, 'active': False}]
    assert assert_lifecycle(pending, done, baseline, mounted, final)['removedListenerCount'] == 1
    for field, value in (('aborted', False), ('finished', False), ('error', 'TypeError')):
        bad = copy.deepcopy(done); bad[0][field] = value
        with pytest.raises(AssertionError): assert_lifecycle(pending, bad, baseline, mounted, final)
    with pytest.raises(AssertionError): assert_lifecycle(pending, done, baseline, mounted, mounted)
    with pytest.raises(AssertionError): assert_lifecycle([], done, baseline, mounted, final)


def test_no_rerun_requires_actual_counter_samples_and_preserved_observed_model():
    state = {'selection': [['group', 'sales']], 'search': '',
             'requestState': {'rowFields': ['region', 'account'], 'colFields': ['period']}}
    assert_no_rerun([2, 2, 2, 2], state, state)
    for samples in ([], [2, 2, 3], [0, 0, 0]):
        with pytest.raises(AssertionError): assert_no_rerun(samples, state, state)
    for field, value in (('selection', []), ('search', 'changed'), ('requestState', {})):
        bad = copy.deepcopy(state); bad[field] = value
        with pytest.raises(AssertionError): assert_no_rerun([2, 2, 2], state, bad)
    with pytest.raises(AssertionError): assert_no_rerun([2, 2, 2], {**state, 'requestState': {}}, {**state, 'requestState': {}})


def test_host_specific_browser_nodes_and_phase_are_exact():
    from scripts.certify_flagship import _product_browser_plan, _product_phase
    from tests.flagship_contract import load_requirements
    plan = _product_browser_plan(load_requirements(Path(__file__).with_name('flagship_requirements.json')))
    for host, requirement in HOST_REQUIREMENTS.items():
        node = f'tests/test_flagship_workflows.py::test_flagship_host_integration[{host}-{requirement}]'
        assert plan[(requirement, host, 'browser.' + requirement)] == node
        assert _product_phase(node) == 'parity'


def test_guarded_delay_consumes_private_marker_then_calls_real_adapter(monkeypatch, tmp_path):
    from examples.flagship_demo_data import _OneShotFaultAdapter
    events = []
    class Adapter:
        async def handle_virtual_scroll_request(self, *args, **kwargs):
            events.append(('adapter', args, kwargs)); return {'real': True}
    async def sleep(seconds): events.append(('sleep', seconds))
    monkeypatch.setattr(asyncio, 'sleep', sleep)
    monkeypatch.setenv('PIVOT_FLAGSHIP_EVIDENCE_ROOT', str(tmp_path))
    monkeypatch.setenv('PIVOT_FLAGSHIP_TEST_FAULT_FILE', str(tmp_path / 'fail-next-data-request'))
    delay = tmp_path / 'delay-next-data-request'; delay.write_bytes(b'one-shot')
    adapter = _OneShotFaultAdapter(Adapter())
    assert asyncio.run(adapter.handle_virtual_scroll_request('actual', start=1)) == {'real': True}
    assert events == [('sleep', 2), ('adapter', ('actual',), {'start': 1})] and not delay.exists()
    events.clear()
    asyncio.run(adapter.handle_virtual_scroll_request())
    assert events == [('adapter', (), {})]
    delay.write_bytes(b'one-shot')
    monkeypatch.setenv('PIVOT_FLAGSHIP_TEST_FAULT_FILE', str(tmp_path.parent / 'outside'))
    events.clear(); asyncio.run(adapter.handle_virtual_scroll_request())
    assert events == [('adapter', (), {})] and delay.exists()


def test_guarded_delay_does_not_change_safe_error_fault(monkeypatch, tmp_path):
    from examples.flagship_demo_data import _OneShotFaultAdapter
    from pivot_engine.backends.execution import DatabaseExecutionError
    class Adapter:
        async def handle_virtual_scroll_request(self): raise AssertionError('Fault must precede adapter')
    monkeypatch.setenv('PIVOT_FLAGSHIP_EVIDENCE_ROOT', str(tmp_path))
    marker = tmp_path / 'fail-next-data-request'; marker.write_bytes(b'one-shot')
    monkeypatch.setenv('PIVOT_FLAGSHIP_TEST_FAULT_FILE', str(marker))
    with pytest.raises(DatabaseExecutionError, match='database_execution_failed'):
        asyncio.run(_OneShotFaultAdapter(Adapter()).handle_virtual_scroll_request())
    assert not marker.exists()


def test_rendered_totals_require_exact_logical_grand_total_cells(monkeypatch):
    from tests import flagship_remaining_5 as helper
    shown = [{'rowid': '__grand_total__', 'colid': '__RowTotal__' + field + '_sum', 'text': text}
             for field, text in zip(('ventes_\u20ac', 'unit\u00e9s', '\u00e9cart'), ('200.00', '10.00', '-6.75'))]
    monkeypatch.setattr(helper, 'cells', lambda _: shown)
    assert helper.fixture_visible(None)
    shown[-1]['text'] = '-7.00'
    assert not helper.fixture_visible(None)
    shown[-1]['text'] = '-6.75'; shown[-1]['rowid'] = 'wrong-row'
    assert not helper.fixture_visible(None)
