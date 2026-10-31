"""Layout presentation oracles for the expanded, subtotal-enabled fixture."""

from decimal import Decimal
from tests.flagship_axis_swap import GROUPS, assert_axis_rows, axis_expected_cells


ACCOUNTS = {region: prefix.split("|", 1)[1] for prefix, region, _ in GROUPS}
PATHS = {region: (region, None, 0) for region in ACCOUNTS}
PATHS.update({region + "|||" + account: (region, account, 1) for region, account in ACCOUNTS.items()})
PATHS["__grand_total__"] = (None, None, 0)


def assert_layout_rows(rows, mode):
    assert mode in ("hierarchy", "outline", "tabular")
    data = [row for row in rows if "_id" in row]
    by_path = {row["_path"]: row for row in data}
    assert len(data) == len(by_path) == 5 and set(by_path) == set(PATHS)
    roots = [row for row in rows if "_id" not in row or row.get("_path") in (*ACCOUNTS, "__grand_total__")]
    assert_axis_rows(roots, transposed=False)
    for path, (region, account, depth) in PATHS.items():
        row = by_path[path]
        assert row.get("region") == region and row.get("account") == account and row["depth"] == depth
        assert row["_id"] == (account or region or "Grand Total")
        if account:
            parent = by_path[region]
            expected_columns = {key for key in parent if key.endswith("_sum")}
            assert {key for key in row if key.endswith("_sum")} == expected_columns
            for column in expected_columns:
                assert isinstance(row[column], (int, float)) and not isinstance(row[column], bool)
                assert Decimal(str(row[column])) == Decimal(str(parent[column]))
    order = [row["_path"] for row in data]
    assert order[-1] == "__grand_total__"
    for region, account in ACCOUNTS.items():
        parent, child = order.index(region), order.index(region + "|||" + account)
        assert (child + 1 == parent) if mode == "tabular" else (parent + 1 == child)
    return data


def layout_expected_cells(mode):
    expected = {}
    root_values = axis_expected_cells(False)
    for path, (region, account, _) in PATHS.items():
        label = account or region or "Grand Total"
        owner = region or "Grand Total"
        expected.update({(path, column): f"{value:.2f}" for (row, column), value in root_values.items() if row == owner})
        if mode == "hierarchy":
            expected[(path, "hierarchy")] = label
        else:
            expected[(path, "region")] = "" if account and mode == "outline" else region or "Grand Total"
            expected[(path, "account")] = account or ""
    return expected


def layout_dom_matches(cells, mode):
    expected = layout_expected_cells(mode)
    identities = [(cell["rowid"], cell["colid"]) for cell in cells]
    if len(cells) != len(expected) or len(set(identities)) != len(expected) or set(identities) != set(expected):
        return False
    if any(expected.get(key) != cell["text"] for key, cell in zip(identities, cells)):
        return False
    tops, bottoms = {}, {}
    for cell in cells:
        if cell["bottom"] <= cell["top"]:
            return False
        previous = tops.setdefault(cell["rowid"], cell["top"])
        previous_bottom = bottoms.setdefault(cell["rowid"], cell["bottom"])
        if abs(previous - cell["top"]) > 1 or abs(previous_bottom - cell["bottom"]) > 1:
            return False
    if len(tops) != 5 or tops["__grand_total__"] <= max(top for row, top in tops.items() if row != "__grand_total__"):
        return False
    order = sorted(tops, key=tops.get)
    if any(tops[right] < bottoms[left] - 1 for left, right in zip(order, order[1:])):
        return False
    by_key = dict(zip(identities, cells))
    for region, account in ACCOUNTS.items():
        child = region + "|||" + account
        expected_child = order.index(region) - 1 if mode == "tabular" else order.index(region) + 1
        if order.index(child) != expected_child:
            return False
        if mode == "hierarchy" and not (by_key[(child, "hierarchy")].get("indent", -1) > by_key[(region, "hierarchy")].get("indent", -1) >= 0):
            return False
    return True


def layout_cells(driver):
    return driver.execute_script("""
      const cells=[];
      const visit=root=>{
        for (const cell of root.querySelectorAll('[role="gridcell"][data-rowid][data-colid]')) {
          if (!cell.getClientRects().length) continue;
          const label=[...cell.querySelectorAll('span')].find(node=>node.style.fontWeight);
          cells.push({rowid:cell.getAttribute('data-rowid'),colid:cell.getAttribute('data-colid'),
            text:cell.innerText.trim(),top:cell.getBoundingClientRect().top,bottom:cell.getBoundingClientRect().bottom,
            indent:cell.getAttribute('data-colid')==='hierarchy' && label
              ? label.getBoundingClientRect().left-cell.getBoundingClientRect().left : null});
        }
        for(const element of root.querySelectorAll('*')) if(element.shadowRoot) visit(element.shadowRoot);
      };
      visit(document); return cells;
    """)


def layout_snapshot(driver, mode, checkpoint):
    from tests import flagship_browser_support as support

    trace = driver.execute_script("return window.__pivotAxisTrace || []")
    requests = {entry["requestId"]: entry for entry in trace if entry["phase"] == "request"
                and entry["at"] > checkpoint and entry.get("layoutMode") == mode
                and entry.get("rowFields") == ["region", "account"] and entry.get("colFields") == ["period"]}
    cells = layout_cells(driver)
    if not layout_dom_matches(cells, mode):
        return False
    if support._totals_control_button(driver, mode.title()) is None:
        return False
    for reply in reversed(trace):
        if reply["phase"] != "response" or reply.get("status") != "data" or reply["requestId"] not in requests:
            continue
        request = requests[reply["requestId"]]
        if reply["at"] < request["at"] or reply["at"] <= checkpoint:
            continue
        if not any(item.get("value") == "Included" for item in request.get("filters", {}).get("scope", {}).get("conditions", [])):
            continue
        schema = reply.get("colSchema") or {}
        expected_columns = {column for _, column in axis_expected_cells(False)}
        columns = schema.get("columns") or []
        if (not reply.get("layoutRevision") or reply["layoutRevision"] != schema.get("layoutRevision")
                or len(columns) != 9 or {column.get("id") for column in columns} != expected_columns
                or {column.get("index") for column in columns} != set(range(9))
                or len({column.get("layoutId") for column in columns}) != 9
                or any(not column.get("layoutId") for column in columns)):
            continue
        try:
            rows = assert_layout_rows(reply.get("rows") or [], mode)
        except AssertionError:
            continue
        tops = {cell["rowid"]: cell["top"] for cell in cells}
        if sorted(tops, key=tops.get) != [row["_path"] for row in rows]:
            continue
        return {"mode": mode, "requestId": reply["requestId"], "rawRows": rows, "cells": cells,
                "visualOrder": sorted(tops, key=tops.get),
                "parentOrder": sorted(ACCOUNTS, key=lambda row: tops[row]),
                "layoutRevision": reply["layoutRevision"], "columnIds": support._navigation_columns(driver)}
    return False


def qualify_layout_modes(host):
    import json
    import os
    from pathlib import Path
    import traceback
    import pytest
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.flagship_axis_swap import AXIS_PROBE
    from tests import flagship_browser_support as support
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace, _verify_release_asset_batch,
        _wait_for_visible_workspace_element,
    )

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "layout-modes" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation, paths, driver = {"states": [], "setup": "Expanded region/account hierarchy with subtotals enabled and two decimal places"}, [], None
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
        if support._totals_control_button(driver, "Hierarchy") is None:
            support._click_totals_control(driver, "View")
        expand = _wait_for_visible_workspace_element(driver, '[title="Expand all hierarchy levels"]')
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", expand)
        expand.click()
        _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
        observation["states"].append(wait.until(lambda current: layout_snapshot(current, "hierarchy", -1)))
        for previous, mode in (("Hierarchy", "outline"), ("Outline", "tabular"), ("Tabular", "hierarchy")):
            checkpoint = driver.execute_script("return performance.now()")
            support._click_totals_control(driver, previous)
            observation["states"].append(wait.until(lambda current: layout_snapshot(current, mode, checkpoint)))
            screenshot = directory / f"{mode}.png"
            driver.save_screenshot(str(screenshot))
            paths.append(str(screenshot))
        assert all(state["parentOrder"] == observation["states"][0]["parentOrder"] for state in observation["states"])
        assert observation["states"][0]["visualOrder"] == observation["states"][-1]["visualOrder"]
        assert observation["states"][0]["columnIds"] == observation["states"][-1]["columnIds"]
        assert len({state["requestId"] for state in observation["states"]}) == 4
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
                observation["finalCells"] = layout_cells(driver)
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
        support.record_case("layout-modes.switch", host, result, directory)
    assert result["result"] == "passed", f"{host} layout modes: {observation['diagnostic']}; evidence: {directory}"
