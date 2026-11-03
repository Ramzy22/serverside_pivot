"""Browser-free regressions for hierarchy native viewport actions."""
import shutil
import subprocess

import pytest

from tests import flagship_browser_support as support


def test_hierarchy_query_and_outer_scroll_across_shadow_root():
    script = r'''
const assert = require('node:assert/strict');
const query = new Function(process.argv[1]), geometry = new Function(process.argv[2]);
const rect = (top,bottom,left=450,right=495) => ({left,right,top,bottom,width:right-left,height:bottom-top});
const node = (bounds, options={}) => ({
 isConnected:true, disabled:false, bounds,
 style:{display:'block',visibility:'visible',overflowX:'visible',overflowY:'visible'},
 getBoundingClientRect(){return this.bounds}, getRootNode(){return {}},
 contains(hit){return hit === this}, ...options
});
const scope = (cells,children=[]) => ({querySelectorAll(selector){
 return selector === '*' ? children : selector.includes('gridcell') ? cells : [];
}});
const toggle = node(rect(1075,1100));
const wrong = node(rect(1030,1050));
const cell = (id,button) => ({getAttribute(){return id},querySelectorAll(){return [button]}});
const targetCell=cell('Éclair',toggle), otherCell=cell('München',wrong);
const shadow=scope([otherCell,targetCell]);
const page=node(rect(0,1046,0,1658),{scrollTop:0,scrollLeft:0,clientHeight:1046,
 scrollHeight:2100,clientWidth:1658,scrollWidth:1658});
page.style.overflowY='auto';
const host=node(rect(698,1600,134,1515),{parentElement:page,shadowRoot:shadow});
toggle.getRootNode=()=>({host});
global.document={...scope([],[host]),scrollingElement:node(rect(0,1046,0,1658),{
 scrollTop:0,scrollLeft:0,clientHeight:1046,scrollHeight:1046,clientWidth:1658,scrollWidth:1658}),
 elementFromPoint(){return host}};
shadow.elementFromPoint=()=>toggle;
global.getComputedStyle=value=>value.style;
global.innerWidth=1658; global.innerHeight=1046;
assert.equal(query('Éclair'),toggle);
assert.equal(query('Unknown'),null);
toggle.disabled=true; assert.equal(query('Éclair'),null); toggle.disabled=false;
toggle.style.visibility='hidden'; assert.equal(query('Éclair'),null); toggle.style.visibility='visible';
shadow.querySelectorAll=scope([targetCell,targetCell]).querySelectorAll;
assert.equal(query('Éclair'),null,'ambiguous logical row must fail closed');
shadow.querySelectorAll=scope([otherCell,targetCell]).querySelectorAll;
assert.deepEqual(geometry(toggle),{ready:false,wheel:{x:829,y:523,dx:0,dy:565}});
// After native outer-page scrolling, a fresh toggle is entirely inside viewport.
toggle.bounds=rect(500,525);
assert.deepEqual(geometry(toggle),{ready:true});
document.elementFromPoint=()=>wrong;
assert.equal(geometry(toggle).ready,false,'overlay must prevent physical click');
console.log('hierarchy shadow scope and outer-page viewport passed');
'''
    result = subprocess.run([shutil.which('node'), '-e', script, support._HIERARCHY_TOGGLE_QUERY,
                             support._HIERARCHY_CLICK_GEOMETRY], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'hierarchy shadow scope and outer-page viewport passed' in result.stdout


@pytest.mark.parametrize('failure', ['stale', 'intercepted'])
def test_hierarchy_native_click_scrolls_then_requeries_for_expand_and_collapse(monkeypatch, failure):
    from selenium.common.exceptions import ElementClickInterceptedException, StaleElementReferenceException
    events = []
    error = StaleElementReferenceException if failure == 'stale' else ElementClickInterceptedException

    class Toggle:
        def __init__(self, fail=False): self.fail = fail
        def click(self):
            events.append('click')
            if self.fail: raise error('rerender or host intercept')

    toggles = iter([Toggle(), Toggle(True), Toggle(), Toggle()])
    geometry = iter([{'ready': False, 'wheel': {'x': 829, 'y': 523, 'dx': 0, 'dy': 565}},
                     {'ready': True}, {'ready': True}, {'ready': True}])

    class Driver:
        def execute_script(self, script, *args):
            assert script in (support._HIERARCHY_TOGGLE_QUERY, support._HIERARCHY_CLICK_GEOMETRY)
            if script == support._HIERARCHY_TOGGLE_QUERY:
                assert args == ('Éclair',)
                events.append('lookup'); return next(toggles)
            return next(geometry)

    class Actions:
        def __init__(self, driver): pass
        def scroll_to_element(self, element): events.append('scroll_to'); return self
        def scroll_from_origin(self, origin, dx, dy):
            assert (origin.x_offset, origin.y_offset, dx, dy) == (829, 523, 0, 565)
            events.append('wheel'); return self
        def perform(self): return self

    monkeypatch.setattr('selenium.webdriver.ActionChains', Actions)
    driver = Driver()
    support._click_hierarchy_toggle(driver, 'Éclair')  # expand
    support._click_hierarchy_toggle(driver, 'Éclair')  # collapse
    assert events == ['lookup', 'scroll_to', 'wheel', 'lookup', 'click',
                      'lookup', 'scroll_to', 'click', 'lookup', 'scroll_to', 'click']


@pytest.mark.parametrize('geometry', [None, {'ready': False}])
def test_hierarchy_native_click_rejects_missing_or_obscured_toggle(monkeypatch, geometry):
    from selenium.common.exceptions import TimeoutException
    class Toggle:
        def click(self): pytest.fail('unreachable toggle must never be clicked')
    class Driver:
        def execute_script(self, script, *args):
            if script == support._HIERARCHY_TOGGLE_QUERY:
                return Toggle() if geometry else None
            return geometry
    class Actions:
        def __init__(self, driver): pass
        def scroll_to_element(self, element): return self
        def perform(self): return self
    class Wait:
        def __init__(self, driver, timeout): self.driver = driver
        def until(self, attempt, message):
            assert attempt(self.driver) is False
            raise TimeoutException(message)
    monkeypatch.setattr('selenium.webdriver.ActionChains', Actions)
    monkeypatch.setattr('selenium.webdriver.support.ui.WebDriverWait', Wait)
    with pytest.raises(TimeoutException, match='Native hierarchy toggle not reachable'):
        support._click_hierarchy_toggle(Driver(), 'Éclair')


def test_recovery_alert_snapshot_preserves_message_retry_and_visibility():
    script = r'''
const assert = require('node:assert/strict');
const query = new Function(process.argv[1]);
const visible = text => ({innerText:text,isConnected:true,
 style:{display:'block',visibility:'visible'},
 getBoundingClientRect(){return {width:200,height:30}},
 checkVisibility(){return true},querySelectorAll(){return []}});
const retry=visible('Retry');
const alert=visible('We could not load the grid data. Try again.');
alert.querySelectorAll=()=>[retry];
const scope = alerts => ({querySelectorAll(selector){return selector === '*' ? [] : alerts}});
const host={shadowRoot:scope([alert])};
global.document={querySelectorAll(selector){return selector === '*' ? [host] : []}};
global.getComputedStyle=element=>element.style;
assert.deepEqual(query(),[alert]);
alert.innerText='Unrelated warning'; assert.deepEqual(query(),[]);
alert.innerText='We could not load the grid data';
retry.innerText='Retry later'; assert.deepEqual(query(),[]); retry.innerText='Retry';
retry.style.visibility='hidden'; assert.deepEqual(query(),[]); retry.style.visibility='visible';
alert.style.display='none'; assert.deepEqual(query(),[]); alert.style.display='block';
alert.checkVisibility=()=>false; assert.deepEqual(query(),[]); alert.checkVisibility=()=>true;
alert.isConnected=false; assert.deepEqual(query(),[]); alert.isConnected=true;
alert.querySelectorAll=()=>[]; assert.deepEqual(query(),[]); alert.querySelectorAll=()=>[retry];
assert.deepEqual(query(),[alert]);
// Expected dismissal produces one empty snapshot, without remote handle reads.
host.shadowRoot=scope([]); assert.deepEqual(query(),[]);
console.log('atomic recovery alert snapshot passed');
'''
    result = subprocess.run([shutil.which('node'), '-e', script, support._VISIBLE_ERROR_QUERY],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'atomic recovery alert snapshot passed' in result.stdout


def test_visible_error_returns_atomic_nodes_without_remote_handle_reads():
    alert = object()
    class Driver:
        def execute_script(self, script):
            assert script == support._VISIBLE_ERROR_QUERY
            return [alert]
    assert support._visible_error(Driver()) == [alert]
