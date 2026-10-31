"""The totals driver reacquires detached controls without toggling twice."""

import pytest
from selenium.common.exceptions import ElementClickInterceptedException, StaleElementReferenceException, TimeoutException


class Button:
    def __init__(self, error=None):
        self.error = error
        self.clicks = 0

    def click(self):
        if self.error:
            raise self.error
        self.clicks += 1


class Driver:
    def __init__(self, buttons):
        self.buttons = iter(buttons)

    def execute_script(self, script, *args):
        if "scrollIntoView" in script:
            return None
        return next(self.buttons, None)


def test_totals_control_reacquires_stale_button_and_clicks_replacement_once():
    from tests.flagship_browser_support import _click_totals_control

    stale, live, unused = Button(StaleElementReferenceException()), Button(), Button()
    _click_totals_control(Driver([stale, live, unused]), "Row Total", timeout=1)
    assert stale.clicks == 0 and live.clicks == 1 and unused.clicks == 0


def test_totals_control_does_not_retry_an_intercepted_action():
    from tests.flagship_browser_support import _click_totals_control

    live = Button()
    with pytest.raises(ElementClickInterceptedException):
        _click_totals_control(Driver([Button(ElementClickInterceptedException()), live]), "Row Total", timeout=1)
    assert live.clicks == 0


def test_missing_totals_control_remains_a_failure():
    from tests.flagship_browser_support import _click_totals_control

    with pytest.raises(TimeoutException):
        _click_totals_control(Driver([]), "Missing", timeout=.01)
