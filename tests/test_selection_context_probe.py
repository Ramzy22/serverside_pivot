"""Read-only diagnostics preserve the native selected-copy contract."""

import pytest

from tests import flagship_browser_support as support


class ProbeDriver:
    def __init__(self):
        self.phases = []

    def execute_script(self, script, cell, phase):
        self.phases.append(phase)
        return list(self.phases)

    def execute_async_script(self, script, sentinel):
        assert sentinel == "qualification clipboard checkpoint"
        return True


def native_actions(monkeypatch, calls):
    class Actions:
        def __init__(self, driver):
            pass

        def context_click(self, cell):
            calls.append(("context_click", cell))
            return self

        def perform(self):
            calls.append(("perform",))

    monkeypatch.setattr("selenium.webdriver.common.action_chains.ActionChains", Actions)


def test_context_timeout_retains_first_and_second_native_open_trace(monkeypatch):
    driver, calls, observation, cell = ProbeDriver(), [], {}, object()
    native_actions(monkeypatch, calls)
    support._selection_context_probe(driver, cell, "Copy Selection:before")

    def unavailable(driver, label):
        raise TimeoutError(label)

    monkeypatch.setattr(support, "_selection_menu", unavailable)
    with pytest.raises(TimeoutError, match="Copy Selection with Headers"):
        support._copy_selected(driver, cell, [["header"], ["42"]], observation,
                               menu_label="Copy Selection with Headers")
    assert calls == [("context_click", cell), ("perform",)]
    assert observation["selectionContextTrace"] == [
        "Copy Selection:before", "Copy Selection with Headers:before",
        "Copy Selection with Headers:after-native-context",
        "Copy Selection with Headers:after-menu-wait",
    ]


def test_probe_preserves_menu_click_and_exact_clipboard_assertion(monkeypatch):
    driver, calls, observation, cell = ProbeDriver(), [], {"clipboardLineSeparator": "\r\n"}, object()
    native_actions(monkeypatch, calls)

    class Item:
        def get_attribute(self, name):
            assert name == "title"
            return "Copy Selection with Headers"

        def click(self):
            calls.append(("menu-click",))

    monkeypatch.setattr(support, "_selection_menu", lambda driver, label: Item())
    monkeypatch.setattr("tests.test_flagship_certification._clipboard_text", lambda driver: "units\tprice\r\n3\t50")
    support._copy_selected(driver, cell, [["units", "price"], ["3", "50"]], observation,
                           menu_label="Copy Selection with Headers")
    assert calls == [("context_click", cell), ("perform",), ("menu-click",)]
    assert observation["clipboardText"] == "units\tprice\r\n3\t50"
    assert observation["parsedTSV"] == [["units", "price"], ["3", "50"]]
    with pytest.raises(AssertionError, match="clipboard bytes"):
        support._copy_selected(driver, cell, [["units", "price"], ["50", "3"]], observation,
                               menu_label="Copy Selection with Headers")
