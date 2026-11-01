"""Harness regression checks; only installed host records qualify browser cases."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from tests.flagship_remaining_4 import (
    REQUIREMENTS, assert_analysis_csv, assert_merged_selection, assert_part_rows,
    assert_scroll_restored, correlated_responses, ClipboardTable,
)


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
    ids = ['hierarchy'] + [f'{prefix}_{field}_{agg}' for prefix in ('Éclair|東京', 'München|القاهرة', '__RowTotal_')
                          for field, agg in (('ventes_€', 'avg'), ('unités', 'sum'), ('écart', 'sum'))]
    request = {'state': {'rowFields': ['period'], 'colFields': ['region', 'account'],
                         'valConfigs': [{'field': f, 'agg': a} for f, a in
                            (('ventes_€', 'avg'), ('unités', 'sum'), ('écart', 'sum'))], 'filters': _part_manifest()['filters']},
               'payload': {'colIds': ids, 'style': {'headerLabels': {c: c for c in ids}}}}
    rows = [list(ids), ['Q1', '100.25', '2', '-12.5', '49.75', '3', '4.25', '75', '5', '-8.25'],
            ['Q2', '20', '1', '2.5', '30', '4', '-1', '25', '5', '1.5'],
            ['Grand Total', '60.125', '3', '-10', '39.875', '7', '3.25', '50', '10', '-6.75']]
    return rows, request


@pytest.mark.parametrize('damage', [None, 'average', 'axis', 'filter', 'row-order', 'header', 'column-order'])
def test_analysis_export_oracle_rejects_wrong_average_axis_order_headers_filter(damage):
    rows, request = _analysis_case()
    if damage == 'average': rows[1][7] = '150'
    if damage == 'axis': request['state']['rowFields'] = ['region']
    if damage == 'filter': request['state']['filters']['scope']['conditions'][0]['value'] = 'Excluded'
    if damage == 'row-order': rows[1], rows[2] = rows[2], rows[1]
    if damage == 'header': rows[0][0] = 'wrong'
    if damage == 'column-order':
        request['payload']['colIds'][1], request['payload']['colIds'][2] = request['payload']['colIds'][2], request['payload']['colIds'][1]
        for row in rows: row[1], row[2] = row[2], row[1]
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
        phase = 'parts' if requirement == 'large-delivery.selected-part' else ('scroll' if requirement in
                 ('saved-scroll.restore', 'copy-selection.unloaded-rows') else 'parity')
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
