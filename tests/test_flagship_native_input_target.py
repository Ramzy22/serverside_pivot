"""Unit controls for native-copy target selection; no browser qualification."""

import shutil
import subprocess
import json
from pathlib import Path

import pytest
from selenium.common.exceptions import StaleElementReferenceException

from tests import flagship_browser_support as support


class Candidate:
    def __init__(self, name, *, displayed=True, enabled=True, stale=False):
        self.name, self.displayed, self.enabled, self.stale = name, displayed, enabled, stale

    def is_displayed(self):
        if self.stale:
            raise StaleElementReferenceException()
        return self.displayed

    def is_enabled(self):
        return self.enabled


class ReadOnlyDriver:
    _pivot_workspace_shadow = False

    def __init__(self, candidates, hit):
        self.candidates, self.hit = candidates, hit

    def find_elements(self, by, selector):
        return self.candidates

    def execute_script(self, script, candidate):
        self.script = script
        return candidate is self.hit


def test_covered_populated_input_does_not_win_over_empty_hit_target():
    covered, upper = Candidate("populated"), Candidate("empty")
    driver = ReadOnlyDriver([covered, upper], upper)
    assert support._native_copy_input_target(driver, "input") is upper


@pytest.mark.parametrize("kind", ["covered", "hidden", "disabled", "stale"])
def test_unusable_input_cannot_satisfy_native_copy_target(kind):
    candidate = Candidate(kind, displayed=kind != "hidden", enabled=kind != "disabled",
                          stale=kind == "stale")
    driver = ReadOnlyDriver([candidate], None if kind == "covered" else candidate)
    assert support._native_copy_input_target(driver, "input") is False


def test_no_matching_input_cannot_satisfy_native_copy_target():
    assert support._native_copy_input_target(ReadOnlyDriver([], None), "input") is False


def test_stale_earlier_input_does_not_hide_live_hit_target():
    stale, upper = Candidate("stale", stale=True), Candidate("upper")
    assert support._native_copy_input_target(ReadOnlyDriver([stale, upper], upper), "input") is upper


def test_real_header_renderer_mounts_only_clicked_filter_owner():
    node = shutil.which("node")
    assert node, "Node is required for the actual frontend renderer regression"
    result = subprocess.run([node, str(Path(__file__).with_name("frontend_native_input_filter_owner.cjs"))],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr


def test_actual_hit_test_script_rejects_occlusion_and_descends_shadow_root():
    candidate = Candidate("input")
    driver = ReadOnlyDriver([candidate], candidate)
    support._native_copy_input_target(driver, "input")
    program = """
const assert = require('node:assert/strict');
const hitTest = new Function(SCRIPT);
global.innerWidth = 800; global.innerHeight = 600;
const input = {getBoundingClientRect:()=>({left:20,right:120,top:30,bottom:50})};
global.document = {elementFromPoint:()=>input};
assert.equal(hitTest(input),true);
document.elementFromPoint=()=>({});
assert.equal(hitTest(input),false,'Covered input must be rejected');
document.elementFromPoint=()=>({shadowRoot:{elementFromPoint:()=>input}});
assert.equal(hitTest(input),true,'Shadow host must resolve to actual input');
document.elementFromPoint=()=>({shadowRoot:{elementFromPoint:()=>({})}});
assert.equal(hitTest(input),false,'Covered shadow input must be rejected');
input.getBoundingClientRect=()=>({left:900,right:1000,top:30,bottom:50});
document.elementFromPoint=()=>input;
assert.equal(hitTest(input),false,'Offscreen input must be rejected without scrolling');
input.getBoundingClientRect=()=>({left:-20,right:40,top:30,bottom:50});
document.elementFromPoint=(x,y)=>{assert.equal(x,20);assert.equal(y,40);return input;};
assert.equal(hitTest(input),true,'Partially clipped input uses in-view center');
""".replace("SCRIPT", json.dumps(driver.script))
    result = subprocess.run([shutil.which("node"), "-e", program], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
