"""Save/reload geometry and model qualification against installed artifacts."""

from tests.flagship_axis_swap import axis_snapshot, AXIS_PROBE

REQUIREMENTS = ("persistence.reload", "journey-persistence.resize-density-reload")


def assert_saved_snapshot(snapshot, column, width):
    assert snapshot["version"] == 1 and snapshot["table"] == "revenue_operations"
    state = snapshot["state"]
    assert state["rowFields"] == ["period"] and state["colFields"] == ["region", "account"]
    assert state["spacingMode"] == 0 and state["decimalPlaces"] == 2
    assert abs(state["columnSizing"][column] - width) <= 2
    assert state["filters"]["scope"]["conditions"][0]["value"] == "Included"


def persistence_geometry(driver, column):
    from tests import flagship_browser_support as support
    from tests.test_flagship_certification import _workspace_elements
    header = support._geometry_header(driver, column)
    if header is None:
        return None
    cells = [cell for cell in _workspace_elements(driver, f'[role="gridcell"][data-colid="{column}"]') if cell.is_displayed()]
    assert len(cells) == 3
    bounds = [driver.execute_script("const cell=arguments[0];return {...cell.getBoundingClientRect().toJSON(),rowHeight:cell.closest('[role=row]').getBoundingClientRect().height}", cell) for cell in cells]
    width = driver.execute_script("return arguments[0].getBoundingClientRect().width", header)
    assert all(abs(bound["width"] - width) <= 1 for bound in bounds)
    assert max(bound["rowHeight"] for bound in bounds) - min(bound["rowHeight"] for bound in bounds) <= 1
    assert all(abs(bound["height"] - bound["rowHeight"]) <= 1 for bound in bounds)
    density = _workspace_elements(driver, '[title="Cycle row density"]')
    assert len(density) == 1
    return {"width": width, "height": bounds[0]["rowHeight"], "density": density[0].text.strip(), "bounds": bounds}


def geometry_if_restored(driver, column, saved):
    current = persistence_geometry(driver, column)
    return current if current and abs(current["width"] - saved["width"]) <= 2 and abs(current["height"] - saved["height"]) <= 1 and current["density"] == saved["density"] == "Compact" else False


def qualify_saved_view_persistence(host):
    import json
    import os
    from pathlib import Path
    import traceback
    from selenium.webdriver.common.action_chains import ActionChains
    import pytest
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
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
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "saved-view-persistence" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation, paths, driver = {"states": []}, [], None
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
        if any(element.is_displayed() for element in _workspace_elements(driver, '[data-sidebar-drop-zone="rows"]')):
            _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
        if support._totals_control_button(driver, "Transpose") is None:
            support._click_totals_control(driver, "View")
        support._click_totals_control(driver, "Transpose")
        wait.until(lambda current: axis_snapshot(current, -1, transposed=True))
        column = "\u00c9clair|\u6771\u4eac_ventes_\u20ac_sum"
        before = persistence_geometry(driver, column)
        observation["before"] = before
        assert before["height"] == 44
        header = support._geometry_header(driver, column)
        handle = header.find_element("css selector", 'div[style*="cursor: col-resize"]')
        ActionChains(driver).move_to_element(handle).click_and_hold().move_by_offset(40, 0).release().perform()
        wait.until(lambda current: abs(persistence_geometry(current, column)["width"] - before["width"] - 40) <= 2)
        for _ in range(2):
            _wait_for_visible_workspace_element(driver, '[title="Cycle row density"]').click()
        wait.until(lambda current: persistence_geometry(current, column)["height"] == 36)
        saved_state = wait.until(lambda current: axis_snapshot(current, -1, transposed=True))
        saved_geometry = persistence_geometry(driver, column)
        assert abs(saved_geometry["width"] - before["width"] - 40) <= 2
        assert saved_geometry["density"] == "Compact"
        key = f"flagship-{host}-pivot-savedView"
        assert driver.execute_script("return localStorage.getItem(arguments[0])", key) is None
        if support._totals_control_button(driver, "Save View") is None:
            support._click_totals_control(driver, "Theme")
        support._click_totals_control(driver, "Save View")
        snapshot = wait.until(lambda current: current.execute_script("return JSON.parse(localStorage.getItem(arguments[0]))", key))
        assert_saved_snapshot(snapshot, column, saved_geometry["width"])
        observation.update(snapshot=snapshot, before=before, savedGeometry=saved_geometry)
        observation["states"].append(saved_state)
        path = directory / "saved.png"
        driver.save_screenshot(str(path)); paths.append(str(path))
        first_origin = driver.execute_script("return performance.timeOrigin")
        _verify_release_asset_batch(driver, require_assets=True)
        driver.refresh()
        restored_state = wait.until(lambda current: axis_snapshot(current, -1, transposed=True))
        restored_geometry = wait.until(lambda current: geometry_if_restored(current, column, saved_geometry))
        assert driver.execute_script("return performance.timeOrigin") > first_origin
        assert driver.execute_script("return JSON.parse(localStorage.getItem(arguments[0]))", key) == snapshot
        assert restored_state["layoutRevision"] == saved_state["layoutRevision"]
        assert restored_state["columnIds"] == saved_state["columnIds"]
        assert restored_state["visibleRows"] == saved_state["visibleRows"]
        observation["states"].append(restored_state)
        observation["restoredGeometry"] = restored_geometry
        trace = driver.execute_script("return window.__pivotAxisTrace || []")
        assert not any(entry.get("status") == "error" for entry in trace)
        assert not any(element.is_displayed() for element in _workspace_elements(driver, '[role="alert"]'))
        if support._totals_control_button(driver, "Transpose") is None:
            support._click_totals_control(driver, "View")
        checkpoint = driver.execute_script("return performance.now()")
        support._click_totals_control(driver, "Transpose")
        changed = wait.until(lambda current: axis_snapshot(current, checkpoint, transposed=False))
        # A consumed snapshot must not roll back a subsequent props echo.
        import time
        for _ in range(5):
            time.sleep(.1)
            assert axis_snapshot(driver, checkpoint, transposed=False)
        observation["states"].append(changed)
        assert driver.execute_script("return JSON.parse(localStorage.getItem(arguments[0]))", key) == snapshot
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
        for requirement in REQUIREMENTS:
            support.record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} saved view persistence: {observation['diagnostic']}; evidence: {directory}"
