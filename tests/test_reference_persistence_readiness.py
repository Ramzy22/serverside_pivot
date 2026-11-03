"""Execute the persistence observer's actual JavaScript without a browser."""
import json
import subprocess

import pytest
from selenium.common.exceptions import TimeoutException
from tests import test_flagship_ag_grid_reference as reference


HARNESS = r'''
const input=JSON.parse(require('fs').readFileSync(0,'utf8'));
let left=0, calls=0;
const rect=()=>({left:left===200?20:220,right:left===200?120:320,width:100});
const cell={getAttribute:()=>input.field,getBoundingClientRect:rect};
global.document={querySelector:s=>s.includes('horizontal')?{get scrollLeft(){return left}}:{getBoundingClientRect:()=>({left:0,right:200})},querySelectorAll:()=>[cell,cell,cell]};
global.window={referenceGrid:{getColumn:id=>input.mode==='missing'?null:{id},ensureColumnVisible:()=>{calls++; left=input.mode==='reset' || (input.mode==='late' && input.poll===0)?0:200}},referenceEvidence:{visibleCells:()=>input.values.map((text,i)=>({rowIndex:String(i),colId:input.field,text:input.mode==='wrong'?'wrong':text}))}};
const result=new Function(input.script).apply(null,[input.field]);
process.stdout.write(JSON.stringify({result,calls}));
'''


class Driver:
    def __init__(self, mode):
        self.mode, self.poll, self.calls = mode, 0, 0

    def execute_script(self, script, field):
        result = subprocess.run(['node', '-e', HARNESS], input=json.dumps({
            'script': script, 'field': field, 'mode': self.mode, 'poll': self.poll,
            'values': ['100.25', '20.00', '120.25']}), text=True,
            capture_output=True, encoding='utf-8', check=True)
        output = json.loads(result.stdout)
        self.calls += output['calls']
        self.poll += 1
        return output['result']


class PollThreeTimes:
    def __init__(self, driver, timeout):
        self.driver = driver

    def until(self, predicate):
        for _ in range(3):
            result = predicate(self.driver)
            if result:
                return result
        raise TimeoutException('three unsuccessful actual-JS observations')


@pytest.mark.parametrize('mode', ['missing', 'reset', 'wrong'])
def test_persistence_observer_rejects_invalid_actual_js(monkeypatch, mode):
    monkeypatch.setattr(reference, 'WebDriverWait', PollThreeTimes)
    driver, observations = Driver(mode), []
    with pytest.raises(TimeoutException):
        reference._wait_persistence_column(driver, 'Éclair|東京|ventes_€',
                                          ['100.25', '20.00', '120.25'], observations.append)
    assert len(observations) == 3
    assert observations[-1]['field'] == 'Éclair|東京|ventes_€'
    assert driver.calls == (0 if mode == 'missing' else 3)


@pytest.mark.parametrize('mode,polls', [('visible', 1), ('late', 2)])
def test_persistence_observer_retries_navigation_and_exact_cells(monkeypatch, mode, polls):
    monkeypatch.setattr(reference, 'WebDriverWait', PollThreeTimes)
    driver, observations = Driver(mode), []
    cells = reference._wait_persistence_column(driver, 'Éclair|東京|ventes_€',
                                             ['100.25', '20.00', '120.25'], observations.append)
    assert [cell['text'] for cell in cells] == ['100.25', '20.00', '120.25']
    assert [cell['rowIndex'] for cell in cells] == ['0', '1', '2']
    assert driver.calls == polls
    assert observations[-1]['fullyVisible']
    if mode == 'late':
        assert not observations[0]['fullyVisible']
        assert observations[0]['scrollLeft'] == 0



@pytest.mark.parametrize('wrong_raw', [False, True])
def test_persistence_inspection_caller_preserves_raw_and_rendered_oracles(monkeypatch, wrong_raw):
    monkeypatch.setattr(reference, 'WebDriverWait', PollThreeTimes)
    field = '?clair|??|ventes_?'
    values = dict(zip(('Q1', 'Q2', 'Total'), (100.25, 20.0, 120.25)))
    rows = [{'data': {field: value}} for value in values.values()]
    driver, visible, observations = Driver('late'), {}, []
    if wrong_raw:
        rows[0]['data'][field] = 999
        with pytest.raises(AssertionError):
            reference._inspect_persistence_columns(driver, rows, {field: values}, visible, observations.append)
        assert driver.poll == 0
        assert not visible
    else:
        reference._inspect_persistence_columns(driver, rows, {field: values}, visible, observations.append)
        assert [cell['text'] for cell in visible[field]] == ['100.25', '20.00', '120.25']
        assert driver.calls == 2
        assert len(observations) == 2
