"""Release-bound browser checks for quoted selected cells and their neighbors."""

import csv
import io
import json
import os
from pathlib import Path
import traceback

from tests import flagship_browser_support as support


SCENARIOS = {
    "tabs": [("CopyTab", "left\tright", "\u00c9clair|||\u6771\u4eac", "2")],
    "newlines": [("CopyLF", "first\nsecond", "\u00c9clair|||\u6771\u4eac", ""),
                 ("CopyCRLF", "first\r\nsecond", "M\u00fcnchen|||\u0627\u0644\u0642\u0627\u0647\u0631\u0629", "3")],
}


def assert_embedded_clipboard(text, source, neighbor):
    expected = [["    " + source.replace("\r\n", "\n"), neighbor]]
    normalized = text.replace("\r\n", "\n")
    output = io.StringIO(newline="")
    csv.writer(output, delimiter="\t", lineterminator="\n").writerows(expected)
    assert normalized == output.getvalue().removesuffix("\n"), "Clipboard bytes differ from the quoted literal rectangle"
    rows = list(csv.reader(io.StringIO(normalized, newline=""), delimiter="\t"))
    assert rows == expected, "Embedded separators changed the logical rectangle"
    return rows


def _cell(driver, row_id, column_id):
    return driver.execute_script("""
      const row = arguments[0], column = arguments[1];
      const visit = root => {
        for (const cell of root.querySelectorAll('[role="gridcell"][data-rowid][data-colid]'))
          if (cell.getAttribute('data-rowid') === row && cell.getAttribute('data-colid') === column &&
              cell.getClientRects().length) return cell;
        for (const element of root.querySelectorAll('*'))
          if (element.shadowRoot) { const found = visit(element.shadowRoot); if (found) return found; }
        return null;
      };
      return visit(document);
    """, row_id, column_id)


def qualify_embedded_clipboard(host, kind):
    import pytest
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _clipboard_text, _host_url, _origin, _qualification_chrome_options,
        _switch_to_workspace, _verify_release_asset_batch, _wait_for_visible_workspace_element,
    )

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "embedded-clipboard" / kind / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"scenarios": []}
    paths, assets = [], {}
    try:
        for field, source, parent, neighbor in SCENARIOS[kind]:
            scenario = {"field": field, "source": source, "expectedNeighbor": neighbor}
            observation["scenarios"].append(scenario)
            scenario_dir = directory / field
            scenario_dir.mkdir(exist_ok=True)
            downloads = scenario_dir / "downloads"
            downloads.mkdir(exist_ok=True)
            driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
            try:
                result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                              infrastructureUnavailable=False)
                driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
                driver._pivot_loaded_assets = {}
                driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
                driver.execute_cdp_cmd("Browser.grantPermissions", {
                    "origin": _origin(_host_url(host)), "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
                _switch_to_workspace(driver, host)
                wait = WebDriverWait(driver, 30)
                wait.until(support._totals)
                chip = _wait_for_visible_workspace_element(driver, f'[data-sidebar-field-chip="available"][aria-label="Move {field}"]')
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", chip)
                chip.send_keys(Keys.ENTER)
                _wait_for_visible_workspace_element(driver, '[data-sidebar-drop-zone="rows"]').send_keys(Keys.ENTER)
                _wait_for_visible_workspace_element(driver, '[title="Expand all hierarchy levels"]').click()
                row_id = parent + "|||" + source
                first = wait.until(lambda current: _cell(current, row_id, "hierarchy"))
                last = wait.until(lambda current: _cell(current, row_id, "Q1_unit\u00e9s_sum"))
                ActionChains(driver).move_to_element(first).click_and_hold().move_to_element(last).release().perform()
                expected_ids = {(row_id, "hierarchy"), (row_id, "Q1_unit\u00e9s_sum")}
                wait.until(lambda current: set(support._selection_ids(current)) == expected_ids)
                selected = support._selection_ids(driver)
                assert len(selected) == 2
                scenario["selectedCells"] = selected
                sentinel = f"before {host} {field} exact selected copy"
                assert driver.execute_async_script(
                    "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));", sentinel)
                ActionChains(driver).context_click(_cell(driver, row_id, "hierarchy")).perform()
                menu = support._selection_menu(driver, "Copy Selection")
                assert menu.get_attribute("title") == "Copy Selection"
                menu.click()
                copied = wait.until(lambda current: value if (value := _clipboard_text(current)) != sentinel else False)
                scenario["clipboardText"] = copied
                scenario["parsedTSV"] = assert_embedded_clipboard(copied, source, neighbor)
            finally:
                try:
                    _verify_release_asset_batch(driver, require_assets=True)
                    scenario["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                    assets.update(driver._pivot_loaded_assets)
                    screenshot = scenario_dir / "final.png"
                    driver.save_screenshot(str(screenshot))
                    paths.append(str(screenshot))
                finally:
                    driver.quit()
        result.update(result="passed", logicalCellsVerified=True, clipboardBytesVerified=True, actionCompleted=True)
        observation["diagnostic"] = ""
    except Exception:
        observation["diagnostic"] = traceback.format_exc()
    finally:
        result["loadedAssetSha256"] = assets
        observation.update(result={key: value for key, value in result.items() if key != "loadedAssetSha256"}, loadedAssetSha256=assets)
        path = directory / "observation.json"
        path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(path))
        result["evidence_paths"] = paths
        support.record_case(f"copy-selection.{kind}", host, result, directory)
    assert result["result"] == "passed", f"{host} {kind}: {observation['diagnostic']}; evidence: {directory}"
