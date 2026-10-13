"""Guard against stale or misclassified AG Grid reference evidence."""

from __future__ import annotations

import json

import pytest
from selenium.common.exceptions import TimeoutException

from tests import flagship_reference_support as support


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
