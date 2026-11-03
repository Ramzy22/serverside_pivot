"""Execute the actual readiness caller and its browser script without a browser."""
import json
import shutil
import subprocess
from types import SimpleNamespace

import pytest
from selenium.common.exceptions import TimeoutException
from tests.flagship_embedded_clipboard import _drag_embedded_rectangle


class Driver:
    def __init__(self, states):
        self.states = iter(states)
        self.calls = 0

    def execute_script(self, script, row, columns):
        state = next(self.states)
        self.calls += 1
        program = r'''
const state = STATE;
global.innerWidth = 800; global.innerHeight = 600;
const cells = ['hierarchy', 'Q1_unités_sum'].map((column, i) => ({
  token: state.replaced && i === 1 ? state.tick + ':' + i : String(i),
  isConnected: !state.detached,
  getAttribute: name => name === 'data-rowid' ? (state.wrongRow ? 'other' : ROW) : column,
  getBoundingClientRect: () => ({left: 10 + 120*i, right:110+120*i,
    top: state.offscreen ? 700 : 20+(state.moving ? state.tick : 0),
    bottom:state.offscreen ? 730 : 50+(state.moving ? state.tick : 0)}),
  closest: () => cells[i],
}));
const root = {querySelectorAll: selector => selector === '*' ? [] : cells,
 elementFromPoint: (x,y) => state.covered ? {closest:()=>null} : (state.descendant ? {closest:()=>cells[x<120?0:1]} : cells[x<120?0:1])};
const host = {shadowRoot:root};
global.document = state.shadow ? {querySelectorAll: selector => selector === '*' ? [host] : [], elementFromPoint:()=>host} : root;
const result = new Function(SCRIPT)(ROW, COLUMNS);
console.log(JSON.stringify(result));
'''.replace('STATE', json.dumps(state)).replace('ROW', json.dumps(row)).replace('COLUMNS', json.dumps(columns)).replace('SCRIPT', json.dumps(script))
        result = subprocess.run([shutil.which('node'), '-e', program], capture_output=True, text=True, timeout=10, encoding='utf-8')
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)


class Wait:
    def __init__(self, driver, count):
        self.driver, self.count = driver, count

    def until(self, predicate):
        for _ in range(self.count):
            result = predicate(self.driver)
            if result:
                return result
        raise TimeoutException('Readiness never authorized native drag')


class Actions:
    def __init__(self, driver):
        self.events = []
        self.w3c_actions = SimpleNamespace(pointer_action=self)

    def move_to_location(self, x, y):
        self.events.append(('move', x, y))

    def pointer_down(self):
        self.events.append(('down',))

    def pointer_up(self):
        self.events.append(('up',))

    def perform(self):
        self.events.append(('perform',))


@pytest.mark.parametrize('condition', ['covered', 'moving', 'replaced', 'wrongRow', 'offscreen', 'detached'])
def test_actual_drag_caller_rejects_unready_endpoint(condition):
    driver = Driver([{condition: True, 'tick': tick} for tick in range(4)])
    created = []
    with pytest.raises(TimeoutException):
        _drag_embedded_rectangle(driver, Wait(driver, 4), 'parent|||first\r\nsecond', lambda d: created.append(Actions(d)))
    assert not created, 'No pointer input may be constructed before readiness'


@pytest.mark.parametrize('state', [{}, {'shadow': True}, {'descendant': True}, {'shadow': True, 'descendant': True}])
def test_actual_drag_caller_authorizes_one_native_drag_after_stable_hits(state):
    driver = Driver([state]*3)
    actions = Actions(driver)
    evidence = {}
    _drag_embedded_rectangle(driver, Wait(driver, 3), 'parent|||first\r\nsecond', lambda d: actions, evidence=evidence)
    assert evidence['nativeDragPerformed'] is True
    assert evidence['consecutiveStableSamples'] == 3
    assert [endpoint['hitColumnId'] for endpoint in evidence['endpoints']] == ['hierarchy', 'Q1_unit\u00e9s_sum']
    json.dumps(evidence)
    assert driver.calls == 3
    assert actions.events == [('move',60,35), ('down',), ('move',180,35), ('up',), ('perform',)]


def test_replaced_endpoint_must_settle_before_one_drag():
    driver = Driver([{'replaced':True,'tick':0}, {}, {}, {}])
    actions = Actions(driver)
    _drag_embedded_rectangle(driver, Wait(driver, 4), 'row', lambda d: actions)
    assert driver.calls == 4
    assert actions.events.count(('perform',)) == 1
