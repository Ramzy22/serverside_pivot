"""Observable read-only layout history; cell-edit history is outside scope."""

from tests.flagship_axis_swap import MEASURES, axis_snapshot


# (transposed, measure count, redo enabled), including a branched final edit.
HISTORY_STATES = ((False, 1, False), (False, 2, False), (True, 2, False),
                  (False, 2, True), (False, 1, True), (False, 2, True),
                  (True, 2, False), (False, 2, True), (False, 3, False))
UNDO_TITLE = "Undo the last edit or layout change (Ctrl/Cmd+Z)"
REDO_TITLE = "Redo the last edit or layout change (Ctrl+Y or Cmd/Ctrl+Shift+Z)"


def assert_history_sequence(states):
    assert len(states) == len(HISTORY_STATES)
    assert [(state["transposed"], len(state["measures"]), state["redoEnabled"]) for state in states] == list(HISTORY_STATES)
    assert all(state["measures"] == list(MEASURES[:count]) for state, (_, count, _) in zip(states, HISTORY_STATES))
    assert len({state["requestId"] for state in states}) == len(states)
    revisions = [state["layoutRevision"] for state in states]
    assert all(revisions) and len(set(revisions)) == 4
    for indices in ((0, 4), (1, 3, 5, 7), (2, 6)):
        first = states[indices[0]]
        assert all(states[index]["layoutRevision"] == first["layoutRevision"]
                   and states[index]["columnIds"] == first["columnIds"]
                   and states[index]["visibleRows"] == first["visibleRows"] for index in indices)


def history_snapshot(driver, checkpoint, *, transposed, count, redo_enabled):
    from tests.test_flagship_certification import _workspace_elements

    snapshot = axis_snapshot(driver, checkpoint, transposed=transposed, measures=MEASURES[:count])
    if not snapshot:
        return False
    requests = [entry for entry in driver.execute_script("return window.__pivotAxisTrace || []")
                if entry["phase"] == "request" and entry["requestId"] == snapshot["requestId"]]
    expected = [{"field": field, "agg": "sum"} for field in MEASURES[:count]]
    if len(requests) != 1 or requests[0].get("valueAggregations") != expected:
        return False
    buttons = [button for button in _workspace_elements(driver, f'button[title="{REDO_TITLE}"]') if button.is_displayed()]
    if len(buttons) != 1 or buttons[0].is_enabled() != redo_enabled:
        return False
    return {**snapshot, "transposed": transposed, "measures": list(MEASURES[:count]), "redoEnabled": redo_enabled}


def qualify_layout_history(host):
    import json
    import os
    from pathlib import Path
    import traceback
    import pytest
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.flagship_axis_swap import AXIS_PROBE
    from tests import flagship_browser_support as support
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace, _verify_release_asset_batch,
        _wait_for_visible_workspace_element, _workspace_elements,
    )

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "layout-history" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation, paths, driver = {"states": [], "actions": []}, [], None
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}", infrastructureUnavailable=False)
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": AXIS_PROBE})
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 30)
        wait.until(support._totals)
        for _ in range(2):
            _wait_for_visible_workspace_element(driver, '[title="Increase decimal places"]').click()

        def chip(label, kind="zone"):
            return next((item for item in _workspace_elements(driver, f'[data-sidebar-field-chip="{kind}"]')
                         if item.is_displayed() and label.casefold() in item.text.casefold()), None)

        def sidebar(opened):
            visible = any(item.is_displayed() for item in _workspace_elements(driver, '[data-sidebar-drop-zone="vals"]'))
            if visible != opened:
                _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()

        def remove(label):
            element = wait.until(lambda _: chip(label))
            driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'})", element)
            element.find_elements("css selector", "span")[-1].click()
            wait.until(lambda _: chip(label) is None)

        def capture(index, checkpoint, action):
            transposed, count, redo = HISTORY_STATES[index]
            snapshot = wait.until(lambda current: history_snapshot(current, checkpoint, transposed=transposed, count=count, redo_enabled=redo))
            observation["states"].append(snapshot)
            observation["actions"].append(action)
            path = directory / f"state-{index}.png"
            driver.save_screenshot(str(path))
            paths.append(str(path))

        sidebar(True)
        remove(MEASURES[2])
        checkpoint = driver.execute_script("return performance.now()")
        remove(MEASURES[1])
        sidebar(False)
        capture(0, checkpoint, "baseline: sales only")

        def add_measure(field):
            sidebar(True)
            element = wait.until(lambda _: chip(field, "available"))
            driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'})", element)
            element.send_keys(Keys.SPACE)
            _wait_for_visible_workspace_element(driver, '[data-sidebar-drop-zone="vals"]').send_keys(Keys.ENTER)
            sidebar(False)

        checkpoint = driver.execute_script("return performance.now()")
        add_measure(MEASURES[1])
        capture(1, checkpoint, "add units")
        if support._totals_control_button(driver, "Transpose") is None:
            support._click_totals_control(driver, "View")
        checkpoint = driver.execute_script("return performance.now()")
        support._click_totals_control(driver, "Transpose")
        capture(2, checkpoint, "transpose")
        for index, title in ((3, UNDO_TITLE), (4, UNDO_TITLE), (5, REDO_TITLE), (6, REDO_TITLE), (7, UNDO_TITLE)):
            checkpoint = driver.execute_script("return performance.now()")
            _wait_for_visible_workspace_element(driver, f'button[title="{title}"]').click()
            capture(index, checkpoint, "undo" if title == UNDO_TITLE else "redo")
        checkpoint = driver.execute_script("return performance.now()")
        add_measure(MEASURES[2])
        capture(8, checkpoint, "branch: add variance; redo disabled")
        assert_history_sequence(observation["states"])
        result.update(result="passed", fixtureTotalsVerified=True, v1RequestsCorrelated=True, logicalValuesPreserved=True, actionCompleted=True)
        observation["diagnostic"] = ""
    except Exception:
        observation["diagnostic"] = traceback.format_exc()
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                observation["trace"] = driver.execute_script("return window.__pivotAxisTrace || []")
                observation["finalCells"] = support._navigation_cells(driver)
                screenshot = directory / "final.png"
                driver.save_screenshot(str(screenshot))
                paths.append(str(screenshot))
            except Exception:
                result["result"] = "blocked"
                observation["evidenceDiagnostic"] = traceback.format_exc()
            finally:
                driver.quit()
        observation.update(result={key: value for key, value in result.items() if key != "loadedAssetSha256"}, loadedAssetSha256=result.get("loadedAssetSha256", {}))
        path = directory / "observation.json"
        path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(path))
        result["evidence_paths"] = paths
        support.record_case("undo-redo.layout", host, result, directory)
    assert result["result"] == "passed", f"{host} layout history: {observation['diagnostic']}; evidence: {directory}"
