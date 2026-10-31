"""Exact server-backed Unicode/null/empty selected-clipboard qualification."""

import csv
import io

FIELDS = ("copyUnicode", "copyNull", "rowId", "copyEmpty")
UNICODE = "\u00c9clair M\u00fcnchen \u6771\u4eac \u0627\u0644\u0642\u0627\u0647\u0631\u0629 \U0001f680"
EXPECTED = {
    "q1-eclair": ["\u00c9clair", "value", "q1-eclair", "value"],
    "q2-eclair": ["\u6771\u4eac", "value", "q2-eclair", "value"],
    "q1-munchen": ["\u0627\u0644\u0642\u0627\u0647\u0631\u0629", "value", "q1-munchen", "value"],
    "q2-munchen": [UNICODE, None, "q2-munchen", ""],
}

REQUIREMENTS = ("copy-selection.unicode", "copy-selection.empty-values")
COLUMNS = tuple(field + "_first" for field in FIELDS)


def assert_source_rows(rows, aggregation="first"):
    metadata = [row for row in rows if "_id" not in row]
    assert len(metadata) <= 1
    assert all(set(row) == {"_path", "_colorScaleStats"} and row["_path"] == "__color_scale_stats__"
               and isinstance(row["_colorScaleStats"], dict) for row in metadata)
    data = [row for row in rows if "_id" in row]
    by_id = {row["_id"]: row for row in data}
    assert len(data) == len(by_id) == 4 and set(by_id) == set(EXPECTED)
    for row_id, values in EXPECTED.items():
        assert [by_id[row_id][f"{field}_{aggregation}"] for field in FIELDS] == values
    return data


def assert_clipboard_values(text):
    expected = [[UNICODE, "", "q2-munchen", ""]]
    assert text == UNICODE + "\t\tq2-munchen\t", "Clipboard differs from the literal four-field rectangle"
    parsed = list(csv.reader(io.StringIO(text, newline=""), delimiter="\t"))
    assert parsed == expected
    return parsed


def clipboard_source_snapshot(driver):
    from tests import flagship_browser_support as support
    trace = driver.execute_script("return window.__pivotAxisTrace || []")
    requests = {entry["requestId"]: entry for entry in trace if entry["phase"] == "request"
                and entry.get("rowFields") == ["rowId"] and entry.get("colFields") == []
                and entry.get("valueAggregations") == [{"field": field, "agg": "first"} for field in FIELDS]
                and any(item.get("value") == "Included" for item in entry.get("filters", {}).get("scope", {}).get("conditions", []))}
    target = [cell for cell in support._navigation_cells(driver) if cell["rowid"] == "q2-munchen" and cell["colid"] in COLUMNS]
    expected = dict(zip(COLUMNS, [UNICODE, "", "q2-munchen", ""]))
    if len(target) != 4 or len({cell["colid"] for cell in target}) != 4 or any(cell["text"] != expected[cell["colid"]] for cell in target):
        return False
    for reply in reversed(trace):
        if reply["phase"] != "response" or reply.get("status") != "data" or reply["requestId"] not in requests:
            continue
        if reply["at"] < requests[reply["requestId"]]["at"]:
            continue
        schema = reply.get("colSchema") or {}
        columns = schema.get("columns") or []
        if ([column.get("id") for column in columns] != list(COLUMNS)
                or [column.get("index") for column in columns] != list(range(4))
                or reply.get("layoutRevision") != schema.get("layoutRevision")):
            continue
        try:
            rows = assert_source_rows(reply.get("rows") or [])
        except AssertionError:
            continue
        return {"requestId": reply["requestId"], "rawRows": rows, "columns": columns, "cells": target}
    return False


def qualify_clipboard_values(host):
    import json
    import os
    from pathlib import Path
    import traceback
    import pytest
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.flagship_axis_swap import AXIS_PROBE
    from tests.flagship_embedded_clipboard import _cell
    from tests import flagship_browser_support as support
    from tests.test_flagship_certification import (
        _clipboard_text, _host_url, _origin, _qualification_chrome_options,
        _switch_to_workspace, _verify_release_asset_batch,
    )
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "clipboard-values" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"; downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation, paths, driver = {}, [], None
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}", infrastructureUnavailable=False)
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": AXIS_PROBE})
        driver.execute_cdp_cmd("Browser.grantPermissions", {"origin": _origin(_host_url(host)), "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 30)
        wait.until(support._totals)
        # Configure a real persisted view, never rows, cells, responses or clipboard.
        layout = {"version": 1, "table": "revenue_operations", "state": {
            "rowFields": ["rowId"], "colFields": [],
            "valConfigs": [{"field": field, "agg": "first"} for field in FIELDS],
            "filters": {"scope": {"operator": "AND", "conditions": [{"type": "eq", "value": "Included", "caseSensitive": False}]}},
            "sorting": [], "expanded": {}, "showRowTotals": False, "showColTotals": False,
            "showSubtotals": False, "sidebarOpen": False,
            "columnOrder": ["hierarchy", *COLUMNS], "pivotLayoutOrder": [], "columnPinning": {"left": ["hierarchy"], "right": []},
        }}
        observation["setupView"] = layout
        driver.execute_script("localStorage.setItem(arguments[0],JSON.stringify(arguments[1]))", f"flagship-{host}-pivot-savedView", layout)
        _verify_release_asset_batch(driver, require_assets=True)
        driver.refresh()
        observation["source"] = wait.until(clipboard_source_snapshot)
        first = wait.until(lambda current: _cell(current, "q2-munchen", COLUMNS[0]))
        last = wait.until(lambda current: _cell(current, "q2-munchen", COLUMNS[-1]))
        ActionChains(driver).move_to_element(first).click_and_hold().move_to_element(last).release().perform()
        expected_ids = {("q2-munchen", column) for column in COLUMNS}
        wait.until(lambda current: set(support._selection_ids(current)) == expected_ids)
        selected = support._selection_ids(driver)
        assert len(selected) == 4
        observation["selectedCells"] = selected
        sentinel = "before exact Unicode/null/empty selected copy"
        assert driver.execute_async_script("const done=arguments[arguments.length-1];navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));", sentinel)
        ActionChains(driver).context_click(_cell(driver, "q2-munchen", COLUMNS[0])).perform()
        menu = support._selection_menu(driver, "Copy Selection")
        assert menu.get_attribute("title") == "Copy Selection"
        menu.click()
        copied = wait.until(lambda current: value if (value := _clipboard_text(current)) != sentinel else False)
        observation["clipboardText"] = copied
        observation["parsedTSV"] = assert_clipboard_values(copied)
        result.update(result="passed", actionCompleted=True, logicalCellsVerified=True, clipboardBytesVerified=True, v1RequestsCorrelated=True)
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
                path = directory / "final.png"; driver.save_screenshot(str(path)); paths.append(str(path))
            except Exception:
                result["result"] = "blocked"; observation["evidenceDiagnostic"] = traceback.format_exc()
            finally:
                driver.quit()
        observation.update(result={key: value for key, value in result.items() if key != "loadedAssetSha256"}, loadedAssetSha256=result.get("loadedAssetSha256", {}))
        path = directory / "observation.json"; path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8"); paths.append(str(path))
        result["evidence_paths"] = paths
        for requirement in REQUIREMENTS:
            support.record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} Unicode/empty clipboard: {observation['diagnostic']}; evidence: {directory}"
