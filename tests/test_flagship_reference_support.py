"""Guard against stale or misclassified AG Grid reference evidence."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from dataclasses import asdict

import pytest
from selenium.common.exceptions import TimeoutException

from tests import flagship_reference_support as support
from scripts.flagship_manifest import QualificationBinding, load_case_envelope


@pytest.fixture
def raw_reference(tmp_path):
    binding = QualificationBinding(source_commit="a" * 40, run_id="current-run",
                                   paths={}, sha256={"fixture": "b" * 64})
    support.start_reference_run(tmp_path, binding.run_id)
    case = support.LiveCase("initial-workspace.populated", None, tmp_path, binding.run_id)
    result = case.save("blocked", "private diagnostic")
    output = tmp_path / "runs" / binding.run_id / "reference-cases.json"
    return tmp_path, binding, result, output


@pytest.mark.parametrize("status", ["blocked", "failed"])
def test_reference_envelope_preserves_nonpassing_status_and_binding(raw_reference, status):
    root, binding, result, output = raw_reference
    raw = json.loads((root / "case-results.json").read_text())
    raw["results"][0]["result"] = status
    (root / "case-results.json").write_text(json.dumps(raw))
    support.write_reference_case_envelope(root, output, binding)
    (bound,) = load_case_envelope(output, binding)
    assert bound.result == status
    assert bound.test_id == result.test_id
    assert bound.sha256 == binding.sha256
    assert bound.evidence_sha256.keys() == set(result.evidence_paths)


@pytest.mark.parametrize("damage", ["wrong_run", "missing", "duplicate", "old_path", "public_output"])
def test_reference_envelope_rejects_invalid_inputs_without_replacing_output(raw_reference, damage):
    root, binding, result, output = raw_reference
    raw = json.loads((root / "case-results.json").read_text())
    if damage == "wrong_run":
        raw["run_id"] = "old-run"
    elif damage == "missing":
        Path(result.evidence_paths[0]).unlink()
    elif damage == "duplicate":
        raw["results"].append(asdict(result))
    elif damage == "old_path":
        old = root / "runs" / "old-run" / "observation.json"
        old.parent.mkdir(parents=True)
        old.write_bytes(Path(result.evidence_paths[0]).read_bytes())
        raw["results"][0]["evidence_paths"] = [str(old)]
    else:
        output = root / "public" / "reference-cases.json"
    (root / "case-results.json").write_text(json.dumps(raw))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("previous complete envelope")
    with pytest.raises((ValueError, OSError)):
        support.write_reference_case_envelope(root, output, binding)
    assert output.read_text() == "previous complete envelope"


@pytest.mark.parametrize("opt_in,live_skipped", [(None, True), ("1", False), ("true", True)])
def test_live_reference_collection_requires_explicit_opt_in(opt_in, live_skipped):
    """Removing a gate or gating deterministic support tests breaks collection policy."""
    script = """
import json
import pytest

class CollectionPolicy:
    def pytest_collection_finish(self, session):
        print('REFERENCE_COLLECTION=' + json.dumps([
            {'nodeid': item.nodeid,
             'skipped': any(mark.args[0] for mark in item.iter_markers('skipif'))}
            for item in session.items
        ]))

raise SystemExit(pytest.main([
    'tests/test_flagship_ag_grid_reference.py',
    'tests/test_flagship_reference_support.py', '--collect-only', '-q',
], plugins=[CollectionPolicy()]))
"""
    environment = os.environ.copy()
    environment.pop("AG_GRID_REFERENCE_LIVE", None)
    if opt_in is not None:
        environment["AG_GRID_REFERENCE_LIVE"] = opt_in
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        env=environment, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    items = json.loads(next(
        line.removeprefix("REFERENCE_COLLECTION=")
        for line in result.stdout.splitlines() if line.startswith("REFERENCE_COLLECTION=")
    ))
    browser_items = [item for item in items if "test_flagship_ag_grid_reference.py::" in item["nodeid"]]
    assert browser_items
    assert all(item["skipped"] is live_skipped for item in browser_items)
    support_items = [item for item in items if "test_flagship_reference_support.py::" in item["nodeid"]]
    assert support_items
    for item in support_items:
        expected = live_skipped if item["nodeid"].endswith("::test_live_browser_ui_timeout_records_failed_result") else False
        assert item["skipped"] is expected, item["nodeid"]


def test_new_run_discards_stale_case_results_and_rejects_old_run_id(tmp_path):
    """A partial rerun must not inherit a prior case's pass."""
    first = support.start_reference_run(tmp_path, "first")
    support.LiveCase("initial-workspace.populated", None, tmp_path, first).save("blocked", "probe")
    assert len(support.load_case_results(tmp_path, "first")) == 1

    second = support.start_reference_run(tmp_path, "second")
    assert second == "second"
    assert support.load_case_results(tmp_path, "second") == ()
    with pytest.raises(ValueError, match="run id"):
        support.load_case_results(tmp_path, "first")
    manifest = json.loads((tmp_path / "case-results.json").read_text(encoding="utf-8"))
    assert manifest["run_id"] == "second"
    assert manifest["results"] == []


def test_timeout_after_browser_navigation_is_failed_but_startup_failure_is_blocked():
    """A missing expected UI state is an observed failure once navigation succeeds."""
    timeout = TimeoutException("expected child never appeared")
    assert support.classify_reference_exception(timeout, page_loaded=True) == "failed"
    assert support.classify_reference_exception(timeout, page_loaded=False) == "blocked"


class _BrowserLogDriver:
    capabilities = {"browserName": "chrome", "browserVersion": "reference"}

    def __init__(self, messages):
        self.messages = messages

    def save_screenshot(self, path):
        Path(path).write_bytes(b"reference screenshot")
        return True

    def execute_script(self, _script):
        return {"trace": [{"request": "getRows"}]}

    def get_log(self, kind):
        assert kind == "browser"
        return [{"level": "SEVERE", "source": "console-api", "message": message}
                for message in self.messages]


def test_reference_case_accepts_real_license_banner_from_hashed_bundle(tmp_path):
    """A Vite asset hash must not turn an evaluation banner into a product error."""
    run_id = support.start_reference_run(tmp_path)
    prefix = "http://127.0.0.1:64476/assets/index--VseV9j3.js 20:177198 "
    banner = [
        '"****************************************************************************************************************************"',
        '"***************************************** AG Grid and AG Charts Enterprise License *****************************************"',
        '"************************************************** License Key Not Found ***************************************************"',
        '"* All AG Grid and AG Charts Enterprise features are unlocked for trial.                                                    *"',
        '"* If you want to hide the watermark please email info@ag-grid.com for a trial license key.                                 *"',
        '"****************************************************************************************************************************"',
    ]
    case = support.LiveCase("initial-workspace.populated", _BrowserLogDriver([prefix + line for line in banner]),
                            tmp_path, run_id, observed="Visible rows and SSRM request")
    assert case.save("passed").result == "passed"
    observation = json.loads((tmp_path / "runs" / run_id / "initial-workspace.populated" / "observation.json").read_text())
    assert observation["unexpected_console"] == []


@pytest.mark.parametrize("error_text", ["TypeError: render crashed", ""])
def test_reference_case_rejects_unrelated_severe_error_amid_license_banner(tmp_path, error_text):
    """A genuine error stays fatal even when the evaluation banner is present."""
    run_id = support.start_reference_run(tmp_path)
    prefix = "http://127.0.0.1:64476/assets/index--VseV9j3.js 20:177305 "
    messages = [prefix + '"* AG Grid and AG Charts Enterprise License *"',
                prefix + f'"{error_text}"']
    case = support.LiveCase("initial-workspace.populated", _BrowserLogDriver(messages),
                            tmp_path, run_id, observed="Visible rows and SSRM request")
    with pytest.raises(AssertionError, match="Unexpected browser console error"):
        case.save("passed")
    assert support.load_case_results(tmp_path, run_id)[0].result == "failed"


@pytest.mark.flagship
@pytest.mark.skipif(
    os.environ.get("AG_GRID_REFERENCE_LIVE") != "1",
    reason="Live AG Grid reference requires AG_GRID_REFERENCE_LIVE=1 and a managed server",
)
def test_live_browser_ui_timeout_records_failed_result(tmp_path):
    """A real Chrome session reaching the app must not report missing UI as infrastructure."""
    from selenium.webdriver.support.ui import WebDriverWait

    run_id = support.start_reference_run(tmp_path)
    with pytest.raises(TimeoutException):
        with support.reference_case("hierarchy-expand.round-trip", run_id, tmp_path) as case:
            WebDriverWait(case.driver, 0.1).until(lambda browser: False)
    (result,) = support.load_case_results(tmp_path, run_id)
    assert result.result == "failed"
    assert result.system == "ag_grid"
    assert result.evidence_paths
