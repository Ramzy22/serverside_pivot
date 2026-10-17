"""Installed-host clipboard qualification for embedded field delimiters."""

import csv
import io
import tempfile
from pathlib import Path

import pytest
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

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


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize(
    ("field", "source"),
    [("CopyTab", "left\tright"), ("CopyLF", "first\nsecond"), ("CopyCRLF", "first\r\nsecond")],
)
def test_installed_host_copies_embedded_delimiters_losslessly(host, field, source, installed_workflow_hosts):
    """Copy Selection retains one logical cell and its source delimiters."""
    downloads = Path(tempfile.mkdtemp(prefix="pivot-clipboard-downloads-"))
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
        ActionChains(driver).click(cell).context_click(cell).perform()
        _selection_menu(driver, "Copy Selection").click()
        copied = WebDriverWait(driver, 8).until(lambda current: _clipboard_text(current) or False)
        parsed = list(csv.reader(io.StringIO(copied, newline=""), delimiter="\t", quotechar='"'))
        assert len(parsed) == 1, (host, field, copied, parsed)
        assert len(parsed[0]) == 1, (host, field, copied, parsed)
        assert parsed[0][0].endswith(source), (host, field, copied, parsed)
    finally:
        driver.quit()
