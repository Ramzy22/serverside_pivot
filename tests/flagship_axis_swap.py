"""Literal axis-swap oracles and installed browser journey."""

from decimal import Decimal


MEASURES = ("ventes_\u20ac", "unit\u00e9s", "\u00e9cart")
GROUPS = (
    ("\u00c9clair|\u6771\u4eac", "\u00c9clair", ((100.25, 2, -12.5), (20, 1, 2.5), (120.25, 3, -10))),
    ("M\u00fcnchen|\u0627\u0644\u0642\u0627\u0647\u0631\u0629", "M\u00fcnchen", ((49.75, 3, 4.25), (30, 4, -1), (79.75, 7, 3.25))),
)
TOTALS = ((150, 5, -8.25), (50, 5, 1.5), (200, 10, -6.75))


def axis_expected_cells(transposed, *, measures=MEASURES):
    result = {}
    if transposed:
        for index, row in enumerate(("Q1", "Q2", "Grand Total")):
            for prefix, _, values in GROUPS:
                result.update({(row, f"{prefix}_{measure}_sum"): value
                               for measure, value in zip(MEASURES, values[index])})
            result.update({(row, f"__RowTotal__{measure}_sum"): value
                           for measure, value in zip(MEASURES, TOTALS[index])})
    else:
        for _, label, values in (*GROUPS, ("", "Grand Total", TOTALS)):
            for period, numbers in zip(("Q1_", "Q2_", "__RowTotal__"), values):
                result.update({(label, f"{period}{measure}_sum"): value
                               for measure, value in zip(MEASURES, numbers)})
    return {key: value for key, value in result.items()
            if any(key[1].endswith(f"_{measure}_sum") for measure in measures)}


def assert_axis_rows(rows, *, transposed, measures=MEASURES):
    expected = axis_expected_cells(transposed, measures=measures)
    metadata = [row for row in rows if "_id" not in row]
    assert len(metadata) <= 1
    assert all(set(row) == {"_path", "_colorScaleStats"} and row["_path"] == "__color_scale_stats__"
               and isinstance(row["_colorScaleStats"], dict) for row in metadata), "Unknown transport-side row"
    rows = [row for row in rows if "_id" in row]
    by_id = {row["_id"]: row for row in rows}
    assert len(rows) == len(by_id) == 3
    assert set(by_id) == {row for row, _ in expected}, "Wrong logical row axis"
    for row_id, row in by_id.items():
        assert {key for key in row if key.endswith("_sum")} == {column for owner, column in expected if owner == row_id}
    for (row_id, column), value in expected.items():
        actual = by_id[row_id][column]
        assert isinstance(actual, (int, float)) and not isinstance(actual, bool)
        assert Decimal(str(actual)) == Decimal(str(value)), f"Wrong {row_id}/{column} value"
    return expected


def axis_dom_matches(cells, *, transposed, measures=MEASURES):
    expected = {(row if row != "Grand Total" else "__grand_total__", column): f"{value:.2f}"
                for (row, column), value in axis_expected_cells(transposed, measures=measures).items()}
    actual = [(cell["rowid"], cell["colid"], cell["text"]) for cell in cells if cell["colid"].endswith("_sum")]
    return (len(actual) == len(expected)
            and len({(row, column) for row, column, _ in actual}) == len(expected)
            and all(expected.get((row, column)) == value for row, column, value in actual))


AXIS_PROBE = r"""
if (!window.__pivotAxisTrace) {
  const trace = window.__pivotAxisTrace = [];
  const visit = (value, phase) => {
    if (typeof value === 'string') { try {value=JSON.parse(value)} catch (_) {return} }
    if (!value || typeof value !== 'object') return;
    if (value.schemaVersion === 1 && value.kind === 'data' && value.requestId) {
      trace.push({phase, at:performance.now(), requestId:value.requestId, status:value.status,
        layoutMode:phase==='request' ? (value.payload?.layoutMode ?? value.payload?.layout_mode ?? value.state?.viewState?.layoutMode ?? 'hierarchy') : null,
        rowFields:value.state?.rowFields, colFields:value.state?.colFields, filters:value.state?.filters,
        valueAggregations:Array.isArray(value.state?.valConfigs) ? value.state.valConfigs.map(config=>({field:config?.field,agg:config?.agg})) : null,
        rows:value.payload?.data, layoutRevision:value.payload?.layoutRevision, colSchema:value.payload?.colSchema});
      return;
    }
    Object.values(value).forEach(child=>visit(child,phase));
  };
  const fetch = window.fetch.bind(window);
  window.fetch = (...args) => {
    visit(args[1]?.body,'request');
    return fetch(...args).then(response=>{
      response.clone().text().then(text=>visit(text,'response')).catch(()=>{});
      return response;
    });
  };
  const send=XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send=function(body) {
    visit(body,'request'); this.addEventListener('load',()=>visit(this.responseText,'response'));
    return send.call(this,body);
  };
}
"""


def axis_snapshot(driver, checkpoint, *, transposed, measures=MEASURES):
    from tests import flagship_browser_support as support

    trace = driver.execute_script("return window.__pivotAxisTrace || []")
    row_fields, col_fields = (["period"], ["region", "account"]) if transposed else (["region", "account"], ["period"])
    requests = {entry["requestId"]: entry for entry in trace if entry["phase"] == "request" and entry["at"] > checkpoint
                and entry.get("rowFields") == row_fields and entry.get("colFields") == col_fields}
    cells = support._navigation_cells(driver)
    if not axis_dom_matches(cells, transposed=transposed, measures=measures):
        return False
    labels = support._navigation_rows(driver)
    expected_labels = {"Q1", "Q2", "Grand Total"} if transposed else {"\u00c9clair", "M\u00fcnchen", "Grand Total"}
    if len(labels) != 3 or {label for _, label in labels} != expected_labels:
        return False
    for reply in reversed(trace):
        if reply["phase"] != "response" or reply.get("status") != "data" or reply["requestId"] not in requests:
            continue
        request = requests[reply["requestId"]]
        if reply.get("at", -1) <= checkpoint or reply["at"] < request["at"]:
            continue
        if not any(item.get("value") == "Included" for item in request.get("filters", {}).get("scope", {}).get("conditions", [])):
            continue
        if not reply.get("layoutRevision") or reply["layoutRevision"] != (reply.get("colSchema") or {}).get("layoutRevision"):
            continue
        schema_columns = (reply.get("colSchema") or {}).get("columns") or []
        expected_columns = {column for _, column in axis_expected_cells(transposed, measures=measures)}
        if (len(schema_columns) != len(expected_columns)
                or {column.get("id") for column in schema_columns} != expected_columns
                or {column.get("index") for column in schema_columns} != set(range(len(expected_columns)))
                or len({column.get("layoutId") for column in schema_columns}) != len(expected_columns)
                or any(not column.get("layoutId") for column in schema_columns)):
            continue
        try:
            assert_axis_rows(reply.get("rows") or [], transposed=transposed, measures=measures)
        except AssertionError:
            continue
        return {"requestId": reply["requestId"], "rowFields": row_fields, "colFields": col_fields,
                "rawRows": reply["rows"], "layoutRevision": reply["layoutRevision"],
                "columnIds": support._navigation_columns(driver), "visibleRows": labels, "visibleCells": cells}
    return False


def qualify_axis_swap(host):
    import json
    import os
    from pathlib import Path
    import traceback
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
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "axis-swap" / host
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
        observation["states"].append(wait.until(lambda current: axis_snapshot(current, -1, transposed=False)))
        for label, transposed in (("transposed", True), ("restored", False)):
            checkpoint = driver.execute_script("return performance.now()")
            support._click_totals_control(driver, "Transpose")
            observation["states"].append(wait.until(lambda current: axis_snapshot(current, checkpoint, transposed=transposed)))
            screenshot = directory / f"{label}.png"
            driver.save_screenshot(str(screenshot))
            paths.append(str(screenshot))
        assert observation["states"][0]["columnIds"] == observation["states"][-1]["columnIds"]
        revisions = [state["layoutRevision"] for state in observation["states"]]
        assert revisions[0] == revisions[2] and revisions[0] != revisions[1]
        assert len({state["requestId"] for state in observation["states"]}) == 3
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
        support.record_case("layout-swap.transpose", host, result, directory)
    assert result["result"] == "passed", f"{host} axis swap: {observation['diagnostic']}; evidence: {directory}"
