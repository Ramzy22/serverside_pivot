"""Installed-host clipboard qualification for embedded field delimiters."""

import csv
import io
import os

import pytest

from tests.test_flagship_workflows import installed_workflow_hosts
from tests.test_flagship_certification import (
    _clipboard_text,
    _host_url,
    _origin,
    _qualification_chrome_options,
    _switch_to_workspace,
    _workspace_elements,
    _wait_for_visible_workspace_element,
)
from tests.flagship_browser_support import _selection_menu


pytestmark = pytest.mark.skipif(
    os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1",
    reason="Opt in to installed-host clipboard browser qualification",
)


def _browser_tools():
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait

    return webdriver, ActionChains, Keys, WebDriverWait


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize(
    ("field", "source"),
    [("CopyTab", "left\tright"), ("CopyLF", "first\nsecond"), ("CopyCRLF", "first\r\nsecond")],
)
def test_installed_host_copies_embedded_delimiters_losslessly(host, field, source, installed_workflow_hosts, tmp_path):
    """Copy Selection retains one logical cell and its source delimiters."""
    webdriver, ActionChains, Keys, WebDriverWait = _browser_tools()
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
    try:
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": _origin(_host_url(host)),
            "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"],
        })
        _switch_to_workspace(driver, host)
        chips = [chip for chip in _workspace_elements(driver, '[data-sidebar-field-chip="available"]')
                 if chip.is_displayed()]
        chip = next(chip for chip in chips if chip.text.strip() == field)
        chip.send_keys(Keys.ENTER)
        _wait_for_visible_workspace_element(driver, '[data-sidebar-drop-zone="rows"]').send_keys(Keys.ENTER)
        _wait_for_visible_workspace_element(driver, '[title="Expand all hierarchy levels"]').click()

        def source_cell(current):
            return next((cell for cell in _workspace_elements(current, '[role="gridcell"][data-colid="hierarchy"]')
                         if cell.is_displayed() and source in (cell.get_attribute("data-rowid") or "")), None)

        cell = WebDriverWait(driver, 20).until(source_cell)
        sentinel = f"before {host} {field} Copy Selection"
        assert driver.execute_async_script(
            "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
            sentinel,
        )
        ActionChains(driver).click(cell).context_click(cell).perform()
        _selection_menu(driver, "Copy Selection").click()
        copied = WebDriverWait(driver, 8).until(
            lambda current: (value if (value := _clipboard_text(current)) != sentinel else False))
        parsed = list(csv.reader(io.StringIO(copied, newline=""), delimiter="\t", quotechar='"'))
        assert len(parsed) == 1, (host, field, copied, parsed)
        assert len(parsed[0]) == 1, (host, field, copied, parsed)
        # Windows Chromium canonicalizes clipboard plain-text LF to CRLF.
        # Byte-exact source line endings are asserted before the OS clipboard
        # boundary by the shared serializer tests.
        normalized_cell = parsed[0][0].replace("\r\n", "\n")
        normalized_source = source.replace("\r\n", "\n")
        assert normalized_cell.endswith(normalized_source), (host, field, copied, parsed)
    finally:
        driver.quit()


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_installed_host_server_table_copy_preserves_crlf(host, installed_workflow_hosts, tmp_path):
    """Both server copy commands preserve a CRLF cell and table geometry."""
    webdriver, ActionChains, Keys, WebDriverWait = _browser_tools()
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
    try:
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": _origin(_host_url(host)),
            "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"],
        })
        _switch_to_workspace(driver, host)
        chips = [chip for chip in _workspace_elements(driver, '[data-sidebar-field-chip="available"]')
                 if chip.is_displayed()]
        next(chip for chip in chips if chip.text.strip() == "CopyCRLF").send_keys(Keys.ENTER)
        _wait_for_visible_workspace_element(driver, '[data-sidebar-drop-zone="rows"]').send_keys(Keys.ENTER)
        _wait_for_visible_workspace_element(driver, '[title="Expand all hierarchy levels"]').click()

        def target_cell(current):
            return next((cell for cell in _workspace_elements(current, '[role="gridcell"][data-colid="hierarchy"]')
                         if cell.is_displayed() and "first\r\nsecond" in (cell.get_attribute("data-rowid") or "")), None)

        target = WebDriverWait(driver, 20).until(target_cell)
        for menu_label in ("Copy Current Table", "Copy All Records"):
            sentinel = f"before {menu_label}"
            assert driver.execute_async_script(
                "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
                sentinel,
            )
            ActionChains(driver).context_click(target).perform()
            menu_item = WebDriverWait(driver, 8).until(lambda current: next((
                item for item in _workspace_elements(current, '[role="menuitem"]')
                if item.is_displayed() and item.is_enabled() and item.text.strip() == menu_label
            ), None))
            menu_item.click()
            copied = WebDriverWait(driver, 30).until(
                lambda current: (value if (value := _clipboard_text(current)) != sentinel else False))
            parsed = list(csv.reader(io.StringIO(copied, newline=""), delimiter="\t", quotechar='"'))
            assert len(parsed) >= 5 and len(parsed[0]) >= 3, (host, menu_label, parsed)
            assert all(len(row) == len(parsed[0]) for row in parsed), (host, menu_label, parsed)
            normalized = [[cell.replace("\r\n", "\n") for cell in row] for row in parsed]
            matching_rows = [row for row in normalized if any(cell.endswith("first\nsecond") for cell in row)]
            assert len(matching_rows) == 1, (host, menu_label, copied, parsed)
            assert {"50", "3", "4"}.issubset({cell.strip() for cell in matching_rows[0][1:]}), (
                host, menu_label, copied, parsed)
    finally:
        driver.quit()
