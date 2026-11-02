"""Harness regression checks; only installed host records qualify browser cases."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from tests.flagship_remaining_4 import (
    REQUIREMENTS, assert_analysis_csv, assert_merged_selection, assert_part_rows,
    assert_scroll_restored, correlated_responses, ClipboardTable,
)


@pytest.mark.parametrize('damage', [None, 'blank', 'stale', 'wrong-row', 'duplicate'])
def test_analysis_dom_oracle_requires_exact_visible_averages(damage):
    from tests.flagship_remaining_4 import analysis_cells_match
    expected = {('Q1', 'sales_avg'): '75.00', ('__grand_total__', 'sales_avg'): '50.00'}
    cells = [dict(rowid=row, colid=col, text=value) for (row, col), value in expected.items()]
    if damage == 'blank': cells[0]['text'] = ''
    elif damage == 'stale': cells[0]['text'] = '150.00'
    elif damage == 'wrong-row': cells[0]['rowid'] = 'Q2'
    elif damage == 'duplicate': cells.append(dict(cells[0]))
    assert analysis_cells_match(cells, expected) is (damage is None)


def _part_manifest():
    return json.loads(Path(__file__).with_name('data').joinpath('flagship_parts_manifest.json').read_text(encoding='utf-8'))


def test_parts_fixture_is_opt_in_and_has_three_real_intervals(monkeypatch):
    from examples.flagship_demo_data import build_demo_table, workspace_config
    monkeypatch.setenv('PIVOT_FLAGSHIP_PARTS_DATA', '1')
    rows = build_demo_table().to_pylist()
    assert len(rows) == 12002
    included = [row for row in rows if row['scope'] == 'Included']
    assert len(included) == 12001
    for index in (0, 4999, 5000, 9999, 10000, 12000):
        assert included[index] == {'region': f'R{index:05d}', 'sales': index + .25, 'scope': 'Included'}
    assert rows[-1] == {'region': 'Excluded sentinel', 'sales': 999999.5, 'scope': 'Excluded'}
    config = workspace_config('react')
    assert config['rowFields'] == ['region'] and config['showColTotals'] is False
    assert config['filters'] == _part_manifest()['filters']
    monkeypatch.delenv('PIVOT_FLAGSHIP_PARTS_DATA')
    monkeypatch.setenv('PIVOT_FLAGSHIP_EXPORT_JOB_DATA', '1')
    assert len(build_demo_table()) == 512
    assert workspace_config('react')['filters'] == {}
    monkeypatch.delenv('PIVOT_FLAGSHIP_EXPORT_JOB_DATA')
    monkeypatch.setenv('PIVOT_FLAGSHIP_PARITY_DATA', '1')
    assert len(build_demo_table()) == 5
    assert workspace_config('react')['colFields'] == ['period']


def _part_case():
    return ([['Region', 'Sales (Sum)']] + [[f'R{i:05d}', str(i + .25)] for i in range(5000, 10000)], {
        'payload': {'partId': 'r1_c0', 'rowStart': 5000, 'rowEnd': 10000, 'includeHeaders': True,
                    'allRows': False, 'colIds': ['hierarchy', 'sales_sum'], 'style': {
                        'headerLabels': {'hierarchy': 'Region', 'sales_sum': 'Sales (Sum)'},
                        'decimalPlaces': 2, 'numberGroupSeparator': 'none'}},
        'state': {'rowFields': ['region'], 'colFields': [], 'filters': _part_manifest()['filters']},
    })


@pytest.mark.parametrize('damage', [None, 'header', 'duplicate-boundary', 'adjacent-part', 'missing-row', 'wrong-order', 'filter', 'interval'])
def test_part_oracle_checks_exact_values_order_boundaries_headers_and_filter(damage):
    rows, request = _part_case()
    if damage == 'header': rows[0][1] = 'Wrong'
    if damage == 'duplicate-boundary': rows[-1] = rows[1]
    if damage == 'adjacent-part': rows[1] = ['R04999', '4999.25']
    if damage == 'missing-row': rows.pop()
    if damage == 'wrong-order': rows[1], rows[2] = rows[2], rows[1]
    if damage == 'filter': request['state']['filters'] = {}
    if damage == 'interval': request['payload']['rowEnd'] += 1
    if damage:
        with pytest.raises(AssertionError): assert_part_rows(rows, request)
    else:
        assert_part_rows(rows, request)


def test_parts_runtime_export_queries_real_filtered_interval(monkeypatch):
    """Exercise the real adapter/exporter, rather than projecting the manifest."""
    from examples.flagship_demo_data import build_demo_table
    from pivot_engine import create_tanstack_adapter
    from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState, SessionRequestGate
    monkeypatch.setenv('PIVOT_FLAGSHIP_PARTS_DATA', '1')
    adapter = create_tanstack_adapter(backend_uri=':memory:')
    adapter.controller.load_data_from_arrow('parts_fixture', build_demo_table())
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())
    _, request = _part_case()
    response = service.process(PivotViewState(
        row_fields=['region'], col_fields=[], val_configs=[{'field': 'sales', 'agg': 'sum', 'label': 'Sales'}],
        filters=request['state']['filters'], layout_mode='tabular', show_subtotals=False,
        show_row_totals=False, show_col_totals=False,
        export_request={**request['payload'], 'format': 'html'}),
        PivotRequestContext(table='parts_fixture', trigger_prop='pivot.runtimeRequest',
                            session_id='parts-fixture', client_instance='parts-client'))
    assert response.status == 'export', response
    path = Path(response.export_payload['contentPath'])
    try:
        rows = [[cell['text'] for cell in row] for row in ClipboardTable(path.read_text(encoding='utf-8')).rows]
        assert_part_rows(rows, request)
    finally:
        path.unlink(missing_ok=True)


MERGED_HTML = ('<table><thead><tr><th colspan="2">Q1</th><th>Q2</th></tr><tr>'
               '<th>Ventes € (Sum)</th><th>Écart (Sum)</th><th>Unités (Sum)</th></tr></thead>'
               '<tbody><tr><td>100.25</td><td>-12.5</td><td>1</td></tr></tbody></table>')


@pytest.mark.parametrize('damage', [None, 'span', 'group', 'measure', 'body'])
def test_merged_selection_oracle_rejects_lost_ancestry_and_incorrect_cells(damage):
    html = MERGED_HTML
    if damage == 'span': html = html.replace('colspan="2"', 'colspan="3"')
    if damage == 'group': html = html.replace('>Q2<', '>Q1<')
    if damage == 'measure': html = html.replace('Écart', 'Unités')
    if damage == 'body': html = html.replace('>100.25<', '>49.75<')
    if damage:
        with pytest.raises(AssertionError): assert_merged_selection(html, [['100.25', '-12.5', '1']])
    else:
        assert_merged_selection(html, [['100.25', '-12.5', '1']])


@pytest.mark.parametrize('damage', [None, 'origin', 'row', 'column', 'top', 'left', 'viewport', 'font', 'zoom'])
def test_scroll_oracle_requires_nonzero_both_axes_exact_logical_cells_and_tolerance(damage):
    saved = dict(top=2500, left=850, firstRow='R056', firstColumn='P20_unités_sum',
                 viewport=[1200, 700, 1440, 1000, 1], font=['Inter', '13px'], zoom='1')
    restored = deepcopy(saved)
    restored.update(top=2502, left=848)
    if damage == 'origin': saved['top'] = 0
    if damage == 'row': restored['firstRow'] = 'R055'
    if damage == 'column': restored['firstColumn'] = 'P19_unités_sum'
    if damage in ('top', 'left'): restored[damage] = saved[damage] + 2.01
    if damage == 'viewport': restored['viewport'][0] += 1
    if damage == 'font': restored['font'][1] = '14px'
    if damage == 'zoom': restored['zoom'] = '1.1'
    if damage:
        with pytest.raises(AssertionError): assert_scroll_restored(saved, restored)
    else:
        assert_scroll_restored(saved, restored)


def _analysis_case():
    ids = ['hierarchy'] + [f'{prefix}_{field}_{agg}' for prefix in ('Éclair|東京', 'München|القاهرة')
                          for field, agg in (('ventes_€', 'avg'), ('unités', 'sum'), ('écart', 'sum'))]
    ids += ['__RowTotal__unités_sum', '__RowTotal__ventes_€_avg', '__RowTotal__écart_sum']
    request = {'state': {'rowFields': ['period'], 'colFields': ['region', 'account'],
                         'valConfigs': [{'field': f, 'agg': a} for f, a in
                            (('ventes_€', 'avg'), ('unités', 'sum'), ('écart', 'sum'))], 'filters': _part_manifest()['filters']},
               'payload': {'colIds': ids, 'style': {'headerLabels': {c: c for c in ids}}}}
    rows = [list(ids), ['Q1', '100.25', '2', '-12.5', '49.75', '3', '4.25', '5', '75', '-8.25'],
            ['Q2', '20', '1', '2.5', '30', '4', '-1', '5', '25', '1.5'],
            ['Grand Total', '60.125', '3', '-10', '39.875', '7', '3.25', '10', '50', '-6.75']]
    return rows, request


@pytest.mark.parametrize('damage', [None, 'average', 'axis', 'filter', 'row-order', 'header', 'column-order', 'total-order'])
def test_analysis_export_oracle_rejects_wrong_average_axis_order_headers_filter(damage):
    rows, request = _analysis_case()
    if damage == 'average': rows[1][8] = '150'
    if damage == 'axis': request['state']['rowFields'] = ['region']
    if damage == 'filter': request['state']['filters']['scope']['conditions'][0]['value'] = 'Excluded'
    if damage == 'row-order': rows[1], rows[2] = rows[2], rows[1]
    if damage == 'header': rows[0][0] = 'wrong'
    if damage == 'column-order':
        request['payload']['colIds'][1], request['payload']['colIds'][2] = request['payload']['colIds'][2], request['payload']['colIds'][1]
        for row in rows: row[1], row[2] = row[2], row[1]
    if damage == 'total-order':
        request['payload']['colIds'][7], request['payload']['colIds'][8] = request['payload']['colIds'][8], request['payload']['colIds'][7]
        for row in rows: row[7], row[8] = row[8], row[7]
    if damage:
        with pytest.raises(AssertionError): assert_analysis_csv(rows, request)
    else:
        assert_analysis_csv(rows, request)


def test_trace_correlates_request_and_remote_payload_without_accepting_unrelated_data():
    request = {'schemaVersion': 1, 'requestId': 'r1', 'kind': 'data'}
    reply = {**request, 'status': 'data', 'payload': {'payloadRef': {'url': '/payload/1'}}}
    trace = [dict(phase='request', at=1, envelope=request),
             dict(phase='response', at=2, url='https://host/v1/runtime', envelope=reply)]
    assert correlated_responses(trace) == []
    trace.append(dict(phase='payload', at=3, url='https://host/payload/unrelated', payload={'data': [{'wrong': True}]}))
    assert correlated_responses(trace) == []
    trace.append(dict(phase='payload', at=4, url='https://host/payload/1', payload={'data': [{'_id': 'R000'}]}))
    assert correlated_responses(trace)[0][1]['payload']['data'] == [{'_id': 'R000'}]
    assert correlated_responses(trace, checkpoint=1) == []
    assert 'data' not in reply['payload']


def test_all_journeys_have_exact_installed_nodes_and_fixture_phases():
    from scripts.certify_flagship import _product_browser_plan, _product_phase
    from tests.flagship_contract import load_requirements
    plan = _product_browser_plan(load_requirements(Path(__file__).with_name('flagship_requirements.json')))
    for requirement in REQUIREMENTS:
        phase = 'parts' if requirement in ('large-delivery.selected-part', 'copy-selection.unloaded-rows') else ('scroll' if
                 requirement == 'saved-scroll.restore' else 'parity')
        for host in ('dash', 'react', 'streamlit'):
            node = f'tests/test_flagship_workflows.py::test_flagship_combined_journeys[{requirement}-{host}]'
            assert plan[(requirement, host, 'browser.' + requirement)] == node
            assert _product_phase(node) == phase


@pytest.mark.parametrize('missing', [False, True])
def test_frozen_parts_oracle_is_required_copied_and_hashed(tmp_path, missing):
    import hashlib
    from scripts.flagship_artifacts import _copy_flagship_fixtures
    snapshot, app = tmp_path / 'snapshot', tmp_path / 'installed-app'
    source = snapshot / 'tests' / 'data'
    source.mkdir(parents=True)
    for name in ('flagship_parity_rows.json', 'flagship_scroll_rows.json', 'flagship_parts_manifest.json'):
        (source / name).write_bytes(Path(__file__).with_name('data').joinpath(name).read_bytes())
    manifest = source / 'flagship_parts_manifest.json'
    if missing:
        manifest.unlink()
        with pytest.raises(FileNotFoundError, match='flagship_parts_manifest.json'):
            _copy_flagship_fixtures(snapshot, app)
        assert not app.exists()
    else:
        hashes = _copy_flagship_fixtures(snapshot, app)
        staged = app / 'tests' / 'data' / manifest.name
        assert staged.read_bytes() == manifest.read_bytes()
        assert hashes['parts_fixture'] == hashlib.sha256(staged.read_bytes()).hexdigest()
        staged.write_bytes(b'altered')
        assert hashes['parts_fixture'] != hashlib.sha256(staged.read_bytes()).hexdigest()


@pytest.mark.parametrize('damage', [None, 'missing', 'altered'])
def test_part_browser_preflight_rejects_missing_or_altered_frozen_oracle(damage):
    import hashlib
    from tests.flagship_remaining_4 import assert_part_manifest_binding
    digest = hashlib.sha256(Path(__file__).with_name('data').joinpath('flagship_parts_manifest.json').read_bytes()).hexdigest()
    hashes = {'parts_fixture': digest}
    if damage == 'missing': hashes.clear()
    if damage == 'altered': hashes['parts_fixture'] = 'f' * 64
    if damage:
        with pytest.raises(AssertionError, match='Part oracle differs'):
            assert_part_manifest_binding(hashes)
    else:
        assert assert_part_manifest_binding(hashes) == digest

class _AnalysisWait:
    def __init__(self, driver): self.driver = driver
    def until(self, predicate):
        for _ in range(12):
            result = predicate(self.driver)
            if result: return result
        raise AssertionError('display did not settle')


def test_analysis_precision_uses_native_global_controls_before_asserting(monkeypatch):
    from tests import flagship_remaining_4 as journey
    from tests import test_flagship_certification as certification
    state = {'decimals': 0, 'format_open': False, 'clicks': []}
    class Button:
        def __init__(self, name): self.name = name
        def is_displayed(self): return state['format_open']
        def click(self):
            state['clicks'].append(self.name)
            if self.name == 'format': state['format_open'] = True
            else: state['decimals'] += 1
    def element(driver, selector):
        return Button('increase' if 'Increase decimal' in selector else 'format')
    monkeypatch.setattr(certification, '_wait_for_visible_workspace_element', element)
    monkeypatch.setattr(certification, '_workspace_elements', lambda d, selector: [Button('increase')])
    monkeypatch.setattr(journey, '_analysis_format', lambda d: f"Number | {state['decimals']} dp | Thin Space")
    result = journey._analysis_two_decimal_display(object(), _AnalysisWait(object()))
    assert state['clicks'] == ['format', 'increase', 'increase']
    assert result['decimalPlaces'] == 2
    assert '2 dp' in result['displayFormat']


@pytest.mark.parametrize('damage', [None, 'rounded', 'stale', 'wrong-coordinate', 'missing-total'])
def test_analysis_native_column_walk_checks_separate_virtualized_views(monkeypatch, damage):
    from tests import flagship_remaining_4 as journey
    state = {'left': 900, 'wheels': []}
    leaves = {('Q1', 'leaf_a'): '100.25', ('Q1', 'leaf_b'): '49.75'}
    totals = {('Q1', 'total'): '75.00', ('__grand_total__', 'total'): '50.00'}
    def cells(driver):
        expected = {0: {('Q1', 'leaf_a'): '100.25'}, 400: {('Q1', 'leaf_b'): '49.75'}, 800: totals}.get(state['left'], {})
        result = [dict(rowid=r, colid=c, text=t) for (r, c), t in expected.items()]
        if state['left'] == 800:
            if damage == 'rounded': result[0]['text'] = '75'
            if damage == 'stale': result[0]['text'] = '150.00'
            if damage == 'wrong-coordinate': result[0]['rowid'] = 'Q2'
            if damage == 'missing-total': result.pop()
        return result
    def step(driver, direction):
        before = state['left']
        state['wheels'].append(direction)
        state['left'] = 0 if direction < 0 else min(800, state['left'] + 400)
        return {'direction': direction, 'before': {'left': before, 'maxLeft': 800}, 'scroll': {'left': state['left'], 'maxLeft': 800}}
    monkeypatch.setattr(journey, '_analysis_cells', cells)
    monkeypatch.setattr(journey, '_analysis_column_step', step)
    monkeypatch.setattr(journey, '_analysis_scroll', lambda d: {'left': state['left'], 'maxLeft': 800})
    observation = {}
    if damage:
        with pytest.raises(AssertionError, match='display did not settle'):
            journey._analysis_display_columns(object(), _AnalysisWait(object()), {**leaves, **totals}, observation)
    else:
        journey._analysis_display_columns(object(), _AnalysisWait(object()), {**leaves, **totals}, observation)
        assert len(observation['columnViews']) == 3
        assert observation['columnViews'][-1]['cells'] == cells(None)
    assert state['wheels'][0] == -1
    assert 1 in state['wheels']

@pytest.mark.parametrize('damage', [None, 'origin', 'unaligned', 'wrong-request', 'outside-window', 'missing-row', 'duplicate-row', 'stale-request', 'count'])
def test_scroll_window_requires_correlated_aligned_nonzero_request_covering_visible_row(damage):
    from tests.flagship_remaining_4 import scroll_window_response
    def request(identifier, start, end):
        return {'schemaVersion': 1, 'requestId': identifier, 'kind': 'data',
                'payload': {'start': start, 'end': end, 'count': end-start+1}}
    initial, moved = request('initial', 0, 47), request('moved', 48, 143)
    reply = {**moved, 'status': 'data', 'payload': {'data': [{'_id': f'R{i:03d}'} for i in range(48, 120)]}}
    if damage == 'origin': moved['payload'].update(start=0, end=95, count=96)
    if damage == 'unaligned': moved['payload'].update(start=49, end=144)
    if damage == 'wrong-request': reply['requestId'] = 'orphan'
    if damage == 'outside-window': moved['payload'].update(start=96, end=191)
    if damage == 'missing-row': reply['payload']['data'] = [{'_id': 'R048'}]
    if damage == 'duplicate-row': reply['payload']['data'].append({'_id': 'R056'})
    if damage == 'count': moved['payload']['count'] = 48
    trace = [dict(phase='request', at=1, envelope=initial),
             dict(phase='response', at=2, envelope={**initial, 'status':'data', 'payload':{'data':[]}}),
             dict(phase='request', at=3, envelope=moved), dict(phase='response', at=4, envelope=reply)]
    assert bool(scroll_window_response(trace, 'R056', checkpoint=4 if damage == 'stale-request' else -1)) is (damage is None)


@pytest.mark.parametrize('damage', [None, 'no-gap', 'endpoint-not-loaded', 'endpoint-already-loaded', 'over-limit', 'foreign-id', 'first-missing'])
def test_unloaded_parts_range_requires_real_middle_gap_loaded_endpoints_and_safe_size(damage):
    from tests.flagship_remaining_4 import unloaded_range_evidence
    before = {f'R{i:05d}' for i in range(96)}
    after = before | {f'R{i:05d}' for i in range(864, 960)}
    endpoint = 'R00900'
    if damage == 'no-gap': after = {f'R{i:05d}' for i in range(960)}
    if damage == 'endpoint-not-loaded': after.remove(endpoint)
    if damage == 'endpoint-already-loaded': before.add(endpoint)
    if damage == 'over-limit': endpoint = 'R10000'; after.add(endpoint)
    if damage == 'foreign-id': endpoint = 'Grand Total'; after.add(endpoint)
    if damage == 'first-missing': before.remove('R00000')
    if damage:
        with pytest.raises(AssertionError): unloaded_range_evidence(before, after, endpoint)
    else:
        evidence = unloaded_range_evidence(before, after, endpoint)
        assert evidence['endpointIds'] == ['R00000', 'R00900']
        assert evidence['logicalRowCount'] == 901
        assert evidence['unloadedRowIds'] == [f'R{i:05d}' for i in range(96, 864)]

@pytest.mark.parametrize('requirement', ['large-delivery.selected-part', 'copy-selection.unloaded-rows'])
@pytest.mark.parametrize('damage', [None, 'missing', 'changed'])
def test_both_parts_journeys_require_existing_frozen_manifest_binding(requirement, damage):
    import hashlib
    from tests.flagship_remaining_4 import assert_journey_fixture_binding
    digest = hashlib.sha256(Path(__file__).with_name('data').joinpath('flagship_parts_manifest.json').read_bytes()).hexdigest()
    binding = {'parts_fixture': digest}
    if damage == 'missing': binding.clear()
    if damage == 'changed': binding['parts_fixture'] = '0' * 64
    if damage:
        with pytest.raises(AssertionError, match='Part oracle differs'):
            assert_journey_fixture_binding(requirement, binding)
    else:
        assert assert_journey_fixture_binding(requirement, binding) == digest
    assert assert_journey_fixture_binding('saved-scroll.restore', {}) is None

@pytest.mark.parametrize('direction', [-1, 1])
@pytest.mark.parametrize('damage', [None, 'stalled', 'jumped', 'wrong-row'])
def test_analysis_column_step_uses_real_visible_anchor_and_single_right_key(monkeypatch, direction, damage):
    from selenium.webdriver.common.keys import Keys
    from tests import flagship_remaining_4 as journey, flagship_browser_support as support
    from tests import test_flagship_certification as certification
    cells = [dict(rowid='Q1', colid=c, text='1.00', bounds={'x': x, 'y': 100})
             for c, x in [('middle', 600), ('left', 400), ('right', 800)]]
    events = []
    class Cell:
        def click(self): events.append('click')
    class Grid:
        def send_keys(self, *keys): events.append(keys)
    def anchor(driver, row, column):
        events.append((row, column))
        return Cell()
    monkeypatch.setattr(journey, '_analysis_cells', lambda d: cells)
    monkeypatch.setattr(journey, '_analysis_scroll', lambda d: {'left': 0 if direction < 0 else 400, 'maxLeft': 800})
    monkeypatch.setattr(support, '_selection_cell', anchor)
    selected = [('Q1', 'first' if direction < 0 else 'next')]
    if damage == 'stalled': selected = [('Q1', 'left' if direction < 0 else 'right')]
    if damage == 'jumped': selected = [('Q1', 'distant-total')]
    if damage == 'wrong-row': selected = [('Q2', selected[0][1])]
    monkeypatch.setattr(support, '_selection_ids', lambda d: selected)
    monkeypatch.setattr(support, '_selection_column_order', lambda d:
        ['hierarchy', 'first', 'middle', 'right'] if direction < 0 else ['hierarchy', 'left', 'middle', 'right', 'next'])
    monkeypatch.setattr(certification, '_wait_for_visible_workspace_element', lambda d, selector: Grid())
    if damage:
        with pytest.raises(AssertionError, match='stalled or skipped'):
            journey._analysis_column_step(object(), direction)
        return
    result = journey._analysis_column_step(object(), direction)
    assert events[:2] == [('Q1', 'left' if direction < 0 else 'right'), 'click']
    assert events[2] == ((Keys.CONTROL, Keys.ARROW_LEFT) if direction < 0 else (Keys.ARROW_RIGHT,))
    assert result['selected'] == [('Q1', 'first' if direction < 0 else 'next')]

def test_analysis_column_walk_rejects_keyboard_selection_without_scroll_progress(monkeypatch):
    from tests import flagship_remaining_4 as journey
    monkeypatch.setattr(journey, '_analysis_scroll', lambda d: {'left': 0, 'maxLeft': 800})
    monkeypatch.setattr(journey, '_analysis_cells', lambda d: [])
    monkeypatch.setattr(journey, '_analysis_column_step', lambda d, direction:
        {'before': {'left': 0}, 'scroll': {'left': 0}, 'selected': [('Q1', 'other')]})
    observation = {}
    with pytest.raises(AssertionError, match='did not advance'):
        journey._analysis_display_columns(object(), _AnalysisWait(object()), {('Q1', 'total'): '75.00'}, observation)
    assert observation['navigation'][0]['selected'] == [('Q1', 'other')]

@pytest.mark.parametrize('value,labels,ready', [
    ('5000', ['Part 1  (rows 1\u20135,000)', 'Part 2  (rows 5,001\u201310,000)', 'Part 3  (rows 10,001\u201312,001)'], True),
    ('12001', ['Part 1  (rows 1\u201312,001)'], False),
    ('5000', ['Part 1  (rows 1\u201312,001)'], False),
    ('5000', ['Part 1  (rows 1\u20135,000)', 'Part 2  (rows 5,000\u201310,000)', 'Part 3  (rows 10,001\u201312,001)'], False),
    ('10000', ['Part 1  (rows 1\u20135,000)', 'Part 2  (rows 5,001\u201310,000)', 'Part 3  (rows 10,001\u201312,001)'], False),
])
def test_selected_part_readiness_requires_settled_input_and_exact_intervals(value, labels, ready):
    from tests.flagship_remaining_4 import part_configuration_ready
    assert part_configuration_ready(value, labels) is ready


def test_unloaded_notice_retries_stale_status_and_requires_exact_text(monkeypatch):
    from selenium.common.exceptions import StaleElementReferenceException
    from tests import flagship_remaining_4 as journey
    from tests import test_flagship_certification as certification
    message = 'Selected range unavailable: some rows are not loaded. Load the complete range before copying.'
    class Status:
        def __init__(self, text): self._text = text
        def is_displayed(self): return True
        @property
        def text(self):
            if self._text is None: raise StaleElementReferenceException()
            return self._text
    entries = [Status(None)]
    def elements(driver, selector):
        assert selector == '[role="status"] > span'
        return entries
    monkeypatch.setattr(certification, '_workspace_elements', elements)
    assert journey._unloaded_notice(object()) is False
    entries[:] = [Status('Copied'), Status(message + ' wrong')]
    assert journey._unloaded_notice(object()) is False
    entries[:] = [Status(message)]
    assert journey._unloaded_notice(object()) == message


def test_part_size_entry_pastes_whole_value_through_native_keyboard():
    from selenium.webdriver.common.keys import Keys
    from tests.flagship_remaining_4 import _enter_part_size
    events = []
    class Driver:
        def execute_async_script(self, script, value):
            assert 'navigator.clipboard.writeText' in script
            events.append(('clipboard', value))
            return True
    class Number:
        def click(self): events.append(('focus',))
        def send_keys(self, *keys): events.append(('keys', keys))
    _enter_part_size(Driver(), Number())
    assert events == [('clipboard', '5000'), ('focus',),
                      ('keys', (Keys.CONTROL, 'a')), ('keys', (Keys.CONTROL, 'v')),
                      ('keys', (Keys.TAB,))]
