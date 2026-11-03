"""Pure chart-source oracle regressions; these never create a browser."""
import pytest

from tests.flagship_reference_charts_jobs import SALES, UNITS, SOURCE, _source_rows


def _snapshot():
    return {'series': [{'type': 'bar', 'xKey': 'category', 'yKey': key}
                       for key in (SALES, UNITS)],
            'data': [{'category': {'labels': [label]}, SALES: sales, UNITS: units}
                     for label, sales, units in SOURCE]}


def test_chart_source_oracle_reads_declared_category_and_order():
    assert _source_rows(_snapshot()) == SOURCE


@pytest.mark.parametrize('mutation', ['swapped', 'extra', 'wrong-type', 'mixed-category'])
def test_chart_source_oracle_rejects_corrupted_series(mutation):
    state = _snapshot()
    if mutation == 'swapped':
        state['series'].reverse()
    elif mutation == 'extra':
        state['series'].append(state['series'][0])
    elif mutation == 'wrong-type':
        state['series'][0]['type'] = 'line'
    else:
        state['series'][1]['xKey'] = 'other'
    with pytest.raises(AssertionError):
        _source_rows(state)


def test_chart_source_oracle_preserves_unexpected_data_for_comparison():
    state = _snapshot()
    state['data'][0][SALES] = 999
    assert _source_rows(state) != SOURCE
    state['data'].append({'category': 'Total', SALES: 150, UNITS: 5})
    assert len(_source_rows(state)) == 3
