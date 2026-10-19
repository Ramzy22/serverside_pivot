"""Guard against stale or misclassified AG Grid reference evidence."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from selenium.common.exceptions import TimeoutException

from tests import flagship_reference_support as support


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
