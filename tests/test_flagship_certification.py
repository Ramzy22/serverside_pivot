"""Built-artifact acceptance checks for every published flagship host.

The browser cases are intentionally opt-in for normal unit-test runs.  The
release command enables them after building and launching the three examples.
They are never a substitute for their lower-level runtime tests: these cases
verify what a user can actually copy, download, scroll, and control.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from tests.flagship_capabilities import CAPABILITIES, HOSTS, capability_ids, markdown_matrix
from tests.export_qualification_fixture import (
    FLAGSHIP_COL_FIELDS,
    FLAGSHIP_FILTER,
    FLAGSHIP_QUALIFICATION_SOURCE_ROWS,
    FLAGSHIP_ROW_FIELDS,
    FLAGSHIP_VALUE_CONFIGS,
    assert_flagship_clipboard,
    assert_flagship_download,
)


ROOT = Path(__file__).resolve().parents[1]
RUN_BROWSER_CERTIFICATION = os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") == "1"
RUN_EXPORT_QUALIFICATION = os.environ.get("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA") == "1"
ARTIFACT_ROOT = Path(os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_DIR", ROOT / "artifacts" / "flagship-certification"))


def test_flagship_capability_matrix_covers_every_published_workflow():
    """Keep the release promise explicit, including intentionally disabled work."""
    capability_set = capability_ids()
    assert len(capability_set) == len(CAPABILITIES)
    assert {item.group for item in CAPABILITIES} == {
        "Startup", "Navigation", "Model", "Filtering", "Table", "Selection",
        "Delivery", "Presentation", "Advanced sidebar", "Saved work", "Analysis",
        "Host integration", "Explicitly unavailable",
    }
    unsupported = [item for item in CAPABILITIES if not item.supported]
    assert [(item.id, item.disabled_reason) for item in unsupported] == [
        ("editing", "Editing remains outside the public component contract."),
    ]


def test_flagship_certification_document_is_generated_from_the_matrix_contract():
    document = (ROOT / "docs" / "flagship-functionality-certification.md").read_text(encoding="utf-8")
    assert "tests/flagship_capabilities.py" in document
    assert "python scripts/certify_flagship.py" in document
    matrix = markdown_matrix()
    assert matrix.count("| required | required | required |") == len([item for item in CAPABILITIES if item.supported])


def test_flagship_qualification_data_is_environment_gated(monkeypatch):
    from examples import flagship_demo_data

    monkeypatch.delenv("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA", raising=False)
    default_table = flagship_demo_data.build_demo_table(3)
    default_config = flagship_demo_data.workspace_config("dash")
    assert default_table.num_rows == 3
    assert default_config["rowFields"] == ["region", "segment", "product"]

    monkeypatch.setenv("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA", "1")
    qualification_table = flagship_demo_data.build_demo_table(3)
    qualification_config = flagship_demo_data.workspace_config("dash")
    assert qualification_table.to_pylist() == FLAGSHIP_QUALIFICATION_SOURCE_ROWS
    assert qualification_config["rowFields"] == FLAGSHIP_ROW_FIELDS
    assert qualification_config["colFields"] == FLAGSHIP_COL_FIELDS
    assert qualification_config["valConfigs"] == FLAGSHIP_VALUE_CONFIGS
    assert qualification_config["filters"] == FLAGSHIP_FILTER
    assert qualification_config["columnOrder"] != []


def test_flagship_react_host_authorizes_and_receives_shared_workspace_config():
    """The HTTP-only hosts use the same explicit demo contract as Dash."""
    runtime_server = (ROOT / "examples" / "flagship_runtime_server.py").read_text(encoding="utf-8")
    react_source = (ROOT / "examples" / "flagship_react" / "src" / "main.tsx").read_text(encoding="utf-8")

    assert "RuntimeDatasetAuthorizer" in runtime_server
    assert '"read", "export"' in runtime_server
    assert 'role: str = "viewer"' in runtime_server
    assert 'workspace_config("react")' in runtime_server
    assert '"/v1/workspace-config"' in runtime_server
    assert "fetch('/v1/workspace-config')" in react_source
    assert "<PivotWorkspace {...workspaceConfig} transport={transport} />" in react_source


pytestmark = pytest.mark.flagship


def _selenium():
    selenium = pytest.importorskip("selenium")
    from selenium import webdriver
    from selenium.common.exceptions import WebDriverException
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    return selenium, webdriver, WebDriverException, ChromeOptions, ActionChains, By, EC, WebDriverWait


def _host_url(host: str) -> str:
    defaults = {
        "dash": "http://127.0.0.1:8050",
        "react": "http://127.0.0.1:5174",
        "streamlit": "http://127.0.0.1:8502",
    }
    return os.environ.get(f"PIVOT_FLAGSHIP_{host.upper()}_URL", defaults[host])


def _origin(url: str) -> str:
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    return f"{parsed.scheme}://{parsed.netloc}"


@pytest.fixture(params=HOSTS, ids=HOSTS)
def flagship_host(request):
    if not RUN_BROWSER_CERTIFICATION:
        pytest.skip("set PIVOT_RUN_FLAGSHIP_BROWSER_TESTS=1 to run built-host certification")
    return request.param


@pytest.fixture
def flagship_artifacts(flagship_host):
    directory = ARTIFACT_ROOT / flagship_host
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture
def flagship_driver(flagship_host, flagship_artifacts):
    _, webdriver, WebDriverException, ChromeOptions, _, _, _, _ = _selenium()
    downloads = flagship_artifacts / "downloads"
    downloads.mkdir(exist_ok=True)
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1680,1200")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--force-device-scale-factor=1")
    options.add_experimental_option("prefs", {
        "download.default_directory": str(downloads),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    options.set_capability("goog:loggingPrefs", {"browser": "ALL", "performance": "ALL"})
    try:
        driver = webdriver.Chrome(options=options)
    except WebDriverException as exc:
        pytest.skip(f"Chrome WebDriver is not available: {exc}")
    driver.set_page_load_timeout(30)
    driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
        "behavior": "allow", "downloadPath": str(downloads), "eventsEnabled": True,
    })
    # Chrome denies clipboard reads by default in headless mode.  Granting the
    # permission lets this test verify the user-visible clipboard contents,
    # rather than accepting a misleading toast.
    driver.execute_cdp_cmd("Browser.grantPermissions", {
        "origin": _origin(_host_url(flagship_host)),
        "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"],
    })
    try:
        yield driver
    finally:
        driver.quit()


def _switch_to_workspace(driver, host: str, timeout: int = 30):
    _, _, _, _, _, By, EC, WebDriverWait = _selenium()
    driver.get(_host_url(host))
    wait = WebDriverWait(driver, timeout)
    if host != "streamlit":
        driver._pivot_workspace_shadow = False
        return wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[role="gridcell"]')))

    # Streamlit V2 isolates a component in an open ShadowRoot (rather than
    # the iframe used by the older component API). Selenium's ordinary CSS
    # lookups stop at that boundary, so retain the document context and query
    # through open roots explicitly.
    driver._pivot_workspace_shadow = True
    return wait.until(lambda current: _workspace_element(current, '[role="gridcell"]', required=False))


def _workspace_elements(driver, selector: str):
    if not getattr(driver, "_pivot_workspace_shadow", False):
        _, _, _, _, _, By, _, _ = _selenium()
        return driver.find_elements(By.CSS_SELECTOR, selector)
    return driver.execute_script(
        """
        const selector = arguments[0];
        const collect = (root, values) => {
            values.push(...root.querySelectorAll(selector));
            for (const element of root.querySelectorAll('*')) {
                if (element.shadowRoot) collect(element.shadowRoot, values);
            }
        };
        const values = [];
        collect(document, values);
        return values;
        """,
        selector,
    )


def _workspace_element(driver, selector: str, *, required: bool = True):
    matches = _workspace_elements(driver, selector)
    if matches:
        return matches[0]
    if required:
        raise AssertionError(f"Workspace element not found: {selector}")
    return False


def _wait_for_workspace_element(driver, selector: str, timeout: int = 15):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    return WebDriverWait(driver, timeout).until(lambda current: _workspace_element(current, selector, required=False))


def _wait_for_visible_workspace_element(driver, selector: str, timeout: int = 15):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    return WebDriverWait(driver, timeout).until(lambda current: next((
        element for element in _workspace_elements(current, selector)
        if element.is_displayed() and element.is_enabled()
    ), False))


def _save_evidence(driver, directory: Path, test_name: str):
    browser_logs = driver.get_log("browser")
    performance_logs = driver.get_log("performance")
    driver.save_screenshot(str(directory / f"{test_name}.png"))
    (directory / f"{test_name}.browser.json").write_text(json.dumps(browser_logs, indent=2), encoding="utf-8")
    (directory / f"{test_name}.network.json").write_text(json.dumps(performance_logs, indent=2), encoding="utf-8")
    return browser_logs


def _relevant_browser_errors(logs):
    ignored = ("favicon.ico",)
    return [
        entry["message"] for entry in logs
        if entry.get("level") in {"SEVERE", "ERROR"}
        and not any(token in entry.get("message", "") for token in ignored)
    ]


def _clipboard_text(driver):
    return driver.execute_async_script(
        "const done = arguments[0]; navigator.clipboard.readText().then(done, error => done(`ERROR:${error.message}`));"
    )


def _clipboard_html(driver):
    return driver.execute_async_script(
        """
        const done = arguments[0];
        navigator.clipboard.read().then(async (items) => {
            const item = items.find(candidate => candidate.types.includes('text/html'));
            if (!item) return done('');
            const blob = await item.getType('text/html');
            done(await blob.text());
        }, error => done(`ERROR:${error.message}`));
        """
    )


def _wait_for_download(directory: Path, export_format: str = "xlsx", timeout: int = 20, started_at: float | None = None) -> Path:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        completed = [
            path for path in directory.glob(f"*.{export_format}")
            if not path.with_suffix(path.suffix + ".crdownload").exists()
            and (started_at is None or path.stat().st_mtime >= started_at)
        ]
        if completed:
            return max(completed, key=lambda path: path.stat().st_mtime)
        time.sleep(0.2)
    raise AssertionError(f"No completed {export_format.upper()} download in {directory}")


def _click_menu_item(driver, cell, label: str):
    _, _, _, _, ActionChains, _, _, WebDriverWait = _selenium()
    ActionChains(driver).context_click(cell).perform()
    return WebDriverWait(driver, 15).until(lambda current: next((
        element for element in _workspace_elements(current, '[role="menuitem"]')
        if element.text.strip() == label and element.is_enabled()
    ), False))


def _assert_workspace_controls(driver):
    for selector in (
        "[data-testid='pivot-toggle-sidebar']",
        "[data-testid='pivot-toolbar-view']",
        "[data-testid='pivot-toolbar-format']",
        "[data-testid='pivot-toolbar-charts']",
        "[data-testid='pivot-toolbar-theme']",
        "[data-testid='pivot-export-xlsx']",
        "[data-testid='pivot-export-csv']",
    ):
        _wait_for_workspace_element(driver, selector)


def _click_export(driver, export_format: str):
    if export_format == "csv":
        _wait_for_visible_workspace_element(driver, "[aria-label='Choose export format']").click()
    _wait_for_visible_workspace_element(driver, f"[data-testid='pivot-export-{export_format}']").click()


def _qualification_leaf_order(driver) -> list[str]:
    return [
        element.get_attribute("data-header-column-id")
        for element in _workspace_elements(driver, "[role='columnheader'][data-header-column-id]")
        if element.get_attribute("data-header-column-id")
    ]


def _apply_qualification_column_order(driver):
    _, _, _, _, ActionChains, _, _, WebDriverWait = _selenium()
    from selenium.webdriver.common.keys import Keys

    units_id = "Q1_unités_sum"
    sales_id = "Q1_ventes_€_sum"
    order = _qualification_leaf_order(driver)
    if order.index(units_id) > order.index(sales_id):
        units_header = _wait_for_visible_workspace_element(
            driver, f"[role='columnheader'][data-header-column-id='{units_id}']"
        )
        ActionChains(driver).click(units_header).key_down(Keys.CONTROL).send_keys(
            Keys.ARROW_LEFT
        ).key_up(Keys.CONTROL).perform()
    WebDriverWait(driver, 15).until(
        lambda current: _qualification_leaf_order(current).index(units_id)
        < _qualification_leaf_order(current).index(sales_id)
    )


def test_flagship_startup_navigation_and_controls(flagship_host, flagship_driver, flagship_artifacts):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    _switch_to_workspace(flagship_driver, flagship_host)
    _assert_workspace_controls(flagship_driver)
    assert len(_workspace_elements(flagship_driver, '[role="gridcell"]')) > 0

    # Open/close the main control bands and move the main horizontal scroller.
    for selector in ("[data-testid='pivot-toolbar-view']", "[data-testid='pivot-toolbar-format']", "[data-testid='pivot-toolbar-theme']"):
        _workspace_element(flagship_driver, selector).click()
    flagship_driver.execute_script(
        "const visit = root => root.querySelector('[role=gridcell]') || Array.from(root.querySelectorAll('*')).map(node => node.shadowRoot && visit(node.shadowRoot)).find(Boolean);"
        "const grid = visit(document);"
        "let parent = grid; while (parent && parent.scrollWidth <= parent.clientWidth) parent = parent.parentElement;"
        "if (parent) parent.scrollLeft = Math.min(400, parent.scrollWidth);"
    )
    WebDriverWait(flagship_driver, 15).until(lambda current: _workspace_element(current, '[role="gridcell"]', required=False))
    logs = _save_evidence(flagship_driver, flagship_artifacts, "startup-navigation-controls")
    assert not _relevant_browser_errors(logs)


@pytest.mark.skipif(RUN_EXPORT_QUALIFICATION, reason="ordinary showcase-data certification")
def test_flagship_copy_current_table_all_records_and_export_xlsx(flagship_host, flagship_driver, flagship_artifacts):
    _, _, _, _, ActionChains, _, _, WebDriverWait = _selenium()
    from selenium.webdriver.common.keys import Keys

    first_cell = _switch_to_workspace(flagship_driver, flagship_host)

    current_table_action = _click_menu_item(flagship_driver, first_cell, "Copy Current Table")
    current_table_action.click()
    WebDriverWait(flagship_driver, 20).until(lambda driver: "\t" in str(_clipboard_text(driver)))
    current_table = _clipboard_text(flagship_driver)
    assert "ERROR:" not in current_table
    assert "\t" in current_table and "\n" in current_table
    current_header = current_table.splitlines()[0]
    assert "Revenue" in current_header
    assert not current_header.startswith("APAC")
    current_table_html = _clipboard_html(flagship_driver)
    assert "ERROR:" not in current_table_html
    assert "<thead>" in current_table_html
    assert "<th " in current_table_html
    assert "Revenue" in current_table_html

    # The keyboard equivalent of Copy Current Table must be labelled too.
    flagship_driver.execute_async_script(
        "const done = arguments[0]; navigator.clipboard.writeText('').then(done, done);"
    )
    first_cell.click()
    ActionChains(flagship_driver).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).perform()
    ActionChains(flagship_driver).key_down(Keys.CONTROL).send_keys("c").key_up(Keys.CONTROL).perform()
    WebDriverWait(flagship_driver, 20).until(
        lambda driver: str(_clipboard_text(driver)).splitlines()[:1] == [current_header]
    )
    first_cell = _switch_to_workspace(flagship_driver, flagship_host)

    all_records_action = _click_menu_item(flagship_driver, first_cell, "Copy All Records")
    all_records_action.click()
    WebDriverWait(flagship_driver, 20).until(
        lambda driver: len(str(_clipboard_text(driver)).splitlines()) > len(str(current_table).splitlines())
    )
    all_records = _clipboard_text(flagship_driver)
    assert "ERROR:" not in all_records
    assert len(all_records.splitlines()) > len(current_table.splitlines())
    assert all_records.splitlines()[0] == current_header

    download_started_at = time.time()
    WebDriverWait(flagship_driver, 15).until(lambda current: _workspace_element(current, "[data-testid='pivot-export-xlsx']", required=False)).click()
    export = _wait_for_download(flagship_artifacts / "downloads", started_at=download_started_at)
    assert export.stat().st_size > 1024
    assert export.read_bytes()[:4] == b"PK\x03\x04"
    with zipfile.ZipFile(export) as workbook:
        sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    assert sheet.findall(".//x:mergeCells/x:mergeCell", namespace)
    logs = _save_evidence(flagship_driver, flagship_artifacts, "copy-current-all-and-export")
    assert not _relevant_browser_errors(logs)


@pytest.mark.skipif(not RUN_EXPORT_QUALIFICATION, reason="set PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA=1")
@pytest.mark.parametrize("export_format", ["csv", "xlsx"])
def test_flagship_qualified_download(flagship_host, flagship_driver, flagship_artifacts, export_format):
    _switch_to_workspace(flagship_driver, flagship_host)
    _assert_workspace_controls(flagship_driver)
    _apply_qualification_column_order(flagship_driver)

    download_started_at = time.time()
    _click_export(flagship_driver, export_format)
    export = _wait_for_download(
        flagship_artifacts / "downloads",
        export_format,
        started_at=download_started_at,
    )
    summary = assert_flagship_download(export, export_format)
    (flagship_artifacts / f"qualified-{export_format}.parsed.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    logs = _save_evidence(flagship_driver, flagship_artifacts, f"qualified-{export_format}")
    assert not _relevant_browser_errors(logs)


@pytest.mark.skipif(not RUN_EXPORT_QUALIFICATION, reason="set PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA=1")
@pytest.mark.parametrize("representation", ["tsv", "html"])
def test_flagship_qualified_clipboard(flagship_host, flagship_driver, flagship_artifacts, representation):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    _switch_to_workspace(flagship_driver, flagship_host)
    _apply_qualification_column_order(flagship_driver)
    first_cell = _workspace_element(flagship_driver, '[role="gridcell"]')

    current_action = _click_menu_item(flagship_driver, first_cell, "Copy Current Table")
    current_action.click()
    WebDriverWait(flagship_driver, 20).until(lambda driver: "\t" in str(_clipboard_text(driver)))
    current_document = _clipboard_text(flagship_driver) if representation == "tsv" else _clipboard_html(flagship_driver)
    current_summary = assert_flagship_clipboard(current_document, representation, all_records=False)

    first_cell = _workspace_element(flagship_driver, '[role="gridcell"]')
    all_action = _click_menu_item(flagship_driver, first_cell, "Copy All Records")
    all_action.click()
    WebDriverWait(flagship_driver, 20).until(
        lambda driver: len(str(_clipboard_text(driver)).splitlines()) > current_summary["rows"]
    )
    all_document = _clipboard_text(flagship_driver) if representation == "tsv" else _clipboard_html(flagship_driver)
    all_summary = assert_flagship_clipboard(all_document, representation, all_records=True)
    assert all_summary["rows"] > current_summary["rows"]

    summary = {"currentTable": current_summary, "allRecords": all_summary}
    (flagship_artifacts / f"qualified-clipboard-{representation}.parsed.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    logs = _save_evidence(flagship_driver, flagship_artifacts, f"qualified-clipboard-{representation}")
    assert not _relevant_browser_errors(logs)


def test_flagship_sidebar_filter_report_and_chart_entrypoints(flagship_host, flagship_driver, flagship_artifacts):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    _switch_to_workspace(flagship_driver, flagship_host)
    wait = WebDriverWait(flagship_driver, 15)
    toggle = _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-toggle-sidebar']")
    toggle.click()
    if not _workspace_element(flagship_driver, '[aria-label="Tool Panel"]', required=False):
        toggle.click()
    wait.until(lambda current: _workspace_element(current, '[aria-label="Tool Panel"]', required=False))

    search = _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-global-search']")
    search.clear()
    search.send_keys("APAC")
    wait.until(lambda current: _workspace_element(current, '[role="gridcell"]', required=False))
    search.clear()

    if not _workspace_element(flagship_driver, "[data-add-chart-pane='true']", required=False):
        _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-toolbar-charts']").click()
    _wait_for_workspace_element(flagship_driver, "[data-add-chart-pane='true']").click()
    wait.until(lambda current: next((element for element in _workspace_elements(current, '*') if element.text.strip() == 'New Chart Pane'), False))
    wait.until(lambda current: next((
        element for element in _workspace_elements(current, 'button') if element.text.strip() == 'Cancel'
    ), False)).click()
    _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-mode-report']").click()
    _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-mode-pivot']").click()
    logs = _save_evidence(flagship_driver, flagship_artifacts, "sidebar-filter-report-chart")
    assert not _relevant_browser_errors(logs)
