"""Browser-free regression coverage for the native hierarchy toggle helper."""

import pytest
from selenium.common.exceptions import StaleElementReferenceException, WebDriverException
from selenium.webdriver.common.by import By

from tests.test_editing_e2e import _toggle_hierarchy_row


class Toggle:
    def __init__(self, failure=None):
        self.failure = failure
        self.successful_clicks = 0

    def _check(self, stage):
        if self.failure == stage:
            raise StaleElementReferenceException(stage)
        return True

    def is_displayed(self):
        return self._check("displayed")

    def is_enabled(self):
        return self._check("enabled")

    def click(self):
        self._check("click")
        self.successful_clicks += 1


class Driver:
    def __init__(self, targets):
        self.targets = iter(targets)
        self.lookups = []

    def find_element(self, by, selector):
        self.lookups.append((by, selector))
        target = next(self.targets)
        if isinstance(target, Exception):
            raise target
        return target


@pytest.mark.parametrize("stage", ["lookup", "displayed", "enabled", "click"])
def test_hierarchy_toggle_reacquires_exact_row_after_staleness(stage):
    stale = StaleElementReferenceException("lookup") if stage == "lookup" else Toggle(stage)
    fresh = Toggle()
    driver = Driver([stale, fresh])

    _toggle_hierarchy_row(driver, "North")

    assert driver.lookups == [(By.CSS_SELECTOR, '[role="gridcell"][data-rowid="North"][data-colid="hierarchy"] button')] * 2
    assert fresh.successful_clicks == 1
    if isinstance(stale, Toggle):
        assert stale.successful_clicks == 0


def test_hierarchy_toggle_propagates_unrelated_native_click_error():
    class BrokenToggle(Toggle):
        def click(self):
            raise WebDriverException("native failure")

    driver = Driver([BrokenToggle(), Toggle()])
    with pytest.raises(WebDriverException, match="native failure"):
        _toggle_hierarchy_row(driver, "North")
    assert len(driver.lookups) == 1


@pytest.mark.parametrize("unready_stage", ["displayed", "enabled"])
def test_hierarchy_toggle_waits_for_visibility_and_enabled(unready_stage):
    class UnreadyToggle(Toggle):
        def _check(self, stage):
            return stage != unready_stage

    unready, fresh = UnreadyToggle(), Toggle()
    driver = Driver([unready, fresh])
    _toggle_hierarchy_row(driver, "North")
    assert len(driver.lookups) == 2
    assert unready.successful_clicks == 0
    assert fresh.successful_clicks == 1


def test_hierarchy_toggle_keeps_twenty_second_wait(monkeypatch):
    from tests import test_editing_e2e as helpers
    from selenium.common.exceptions import TimeoutException

    observed = []

    class DeadlineWait:
        def __init__(self, driver, timeout):
            observed.append(timeout)

        def until(self, predicate):
            raise TimeoutException("bounded wait")

    monkeypatch.setattr(helpers, "WebDriverWait", DeadlineWait)
    with pytest.raises(TimeoutException, match="bounded wait"):
        _toggle_hierarchy_row(Driver([]), "North")
    assert observed == [20]
