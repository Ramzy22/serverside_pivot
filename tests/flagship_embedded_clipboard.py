"""Release-bound browser checks for quoted selected cells and their neighbors."""

import csv
import io
import json
import os
from pathlib import Path
import traceback

from tests import flagship_browser_support as support
from tests.flagship_clipboard_observer import CLIPBOARD_BOUNDARY_OBSERVER, assert_pre_delivery_rectangle


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


# Sample both endpoints atomically. Match the native pointer's viewport coordinates,
# including shadow-root hit testing; a rendered rect alone does not authorize input.
_ENDPOINT_SAMPLE = """
const row = arguments[0], columns = arguments[1];
const find = (root, column) => {
  for (const cell of root.querySelectorAll('[role="gridcell"][data-rowid][data-colid]'))
    if (cell.getAttribute('data-rowid') === row && cell.getAttribute('data-colid') === column && cell.isConnected) return cell;
  for (const child of root.querySelectorAll('*'))
    if (child.shadowRoot) { const found = find(child.shadowRoot, column); if (found) return found; }
  return null;
};
const samples = columns.map(column => {
  const cell = find(document, column);
  if (!cell) return null;
  const rect = cell.getBoundingClientRect();
  const left = Math.max(0, rect.left), right = Math.min(innerWidth, rect.right);
  const top = Math.max(0, rect.top), bottom = Math.min(innerHeight, rect.bottom);
  if (right <= left || bottom <= top) return null;
  const x = Math.floor((left + right) / 2), y = Math.floor((top + bottom) / 2);
  let hit = document.elementFromPoint(x, y);
  while (hit?.shadowRoot) {
    const nested = hit.shadowRoot.elementFromPoint(x, y);
    if (!nested || nested === hit) break;
    hit = nested;
  }
  const hitCell = hit?.closest('[role="gridcell"]');
  if (hitCell !== cell) return null;
  return {cell, rowId: row, columnId: column,
    hitRowId: hitCell.getAttribute('data-rowid'), hitColumnId: hitCell.getAttribute('data-colid'), rect: [rect.left, rect.top, rect.right, rect.bottom], x, y};
});
return samples.every(Boolean) ? samples : null;
"""


def _drag_embedded_rectangle(driver, wait, row_id, action_factory=None, evidence=None):
    """Authorize one drag only after three unchanged, hittable endpoint samples."""
    from selenium.common.exceptions import StaleElementReferenceException
    from selenium.webdriver.common.action_chains import ActionChains

    previous, stable = None, 0
    evidence = evidence if evidence is not None else {}
    evidence.update(samples=0, consecutiveStableSamples=0, nativeDragPerformed=False)

    def ready(current):
        nonlocal previous, stable
        try:
            sample = current.execute_script(_ENDPOINT_SAMPLE, row_id, ["hierarchy", "Q1_unit\u00e9s_sum"])
        except StaleElementReferenceException:
            sample = None
        stable = stable + 1 if sample and sample == previous else (1 if sample else 0)
        previous = sample
        evidence["samples"] += 1
        evidence["consecutiveStableSamples"] = stable
        evidence["lastSampleHittable"] = bool(sample)
        if sample:
            evidence["endpoints"] = [
                {**{key: value for key, value in endpoint.items() if key != "cell"},
                 "elementId": getattr(endpoint["cell"], "id", None)}
                for endpoint in sample
            ]
        return sample if stable >= 3 else False

    endpoints = wait.until(ready)
    actions = (action_factory or ActionChains)(driver)
    first, last = endpoints
    actions.w3c_actions.pointer_action.move_to_location(first["x"], first["y"])
    actions.w3c_actions.pointer_action.pointer_down()
    actions.w3c_actions.pointer_action.move_to_location(last["x"], last["y"])
    actions.w3c_actions.pointer_action.pointer_up()
    actions.perform()
    evidence["nativeDragPerformed"] = True


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
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": CLIPBOARD_BOUNDARY_OBSERVER})
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
                scenario["selectionReadiness"] = {}
                _drag_embedded_rectangle(driver, wait, row_id, evidence=scenario["selectionReadiness"])
                expected_ids = {(row_id, "hierarchy"), (row_id, "Q1_unit\u00e9s_sum")}
                def exact_rectangle(current):
                    selected = support._selection_ids(current)
                    scenario["postGestureSelectedCells"] = selected
                    return set(selected) == expected_ids
                wait.until(exact_rectangle)
                selected = support._selection_ids(driver)
                assert len(selected) == 2
                scenario["selectedCells"] = selected
                sentinel = f"before {host} {field} exact selected copy"
                assert driver.execute_async_script(
                    "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));", sentinel)
                write_checkpoint = driver.execute_script("return window.__pivotClipboardBoundary.length")
                ActionChains(driver).context_click(_cell(driver, row_id, "hierarchy")).perform()
                menu = support._selection_menu(driver, "Copy Selection")
                assert menu.get_attribute("title") == "Copy Selection"
                menu.click()
                copied = wait.until(lambda current: value if (value := _clipboard_text(current)) != sentinel else False)
                scenario["clipboardText"] = copied
                scenario["parsedTSV"] = assert_embedded_clipboard(copied, source, neighbor)
                def captured(current):
                    events = current.execute_script("return window.__pivotClipboardBoundary.slice(arguments[0])", write_checkpoint)
                    return events if len(events) == 1 and events[0].get("success") is True and "text/plain" in events[0].get("payloads", {}) else False
                deliveries = wait.until(captured)
                payload = deliveries[0]["payloads"]["text/plain"]
                assert payload["utf8"] == list(payload["text"].encode("utf-8"))
                assert_pre_delivery_rectangle(payload["text"], source, neighbor)
                scenario["preDeliveryWrites"] = deliveries
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
        result.update(result="passed", logicalCellsVerified=True, clipboardBytesVerified=True,
                      actionCompleted=True, preDeliveryBytesVerified=True)
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
