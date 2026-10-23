"""Installed-host browser evidence for real row and pivot-column windows."""

from __future__ import annotations

import json
import os
from pathlib import Path
import traceback

from tests.flagship_browser_support import _network, record_case


_SCROLL_PROBE = r"""
if (!window.__pivotScrollTrace) {
  const trace = window.__pivotScrollTrace = [];
  const debug = window.__pivotScrollDebug = [];
  const nativeLog = console.log.bind(console);
  console.log = (...args) => {
    if (typeof args[0] === 'string' && (args[0] === '[pivot-grid]' || args[0] === '[pivot-client]')) {
      if (debug.length < 500) debug.push({event: args[1], detail: args[2]});
    }
    nativeLog(...args);
  };
  function visit(raw, phase) {
    let value = raw;
    if (typeof value === 'string') { try { value = JSON.parse(value); } catch (_) { return; } }
    if (!value || typeof value !== 'object') return;
    if (value.schemaVersion === 1 && value.kind === 'data' && typeof value.requestId === 'string') {
      const rows = Array.isArray(value.payload?.data) ? value.payload.data : [];
      trace.push({phase, at: performance.now(), requestId: value.requestId,
        status: value.status || null, start: value.payload?.start ?? null,
        end: value.payload?.end ?? null,
        colStart: value.payload?.col_start ?? value.payload?.colStart ?? null,
        colEnd: value.payload?.col_end ?? value.payload?.colEnd ?? null,
        showRowTotals: value.state?.showRowTotals ?? null,
        showColTotals: value.state?.showColTotals ?? null,
        needsColSchema: value.payload?.needs_col_schema ?? value.payload?.needsColSchema ?? null,
        dataVersion: value.payload?.dataVersion ?? null,
        schema: Array.isArray(value.payload?.colSchema?.columns)
          ? value.payload.colSchema.columns.map(column => ({index:column.index,id:column.id})) : null,
        rows: rows.map(row => ({id: row?._id, path: row?._path,
          first: row?.['P23_unités_sum'], last: row?.['P00_unités_sum'],
          total: row?.['__RowTotal__unités_sum'],
          totalKeys: row?._id === 'R000' ? Object.keys(row).filter(key => key.includes('Total')) : []}))});
      return;
    }
    if (Array.isArray(value)) value.forEach(item => visit(item, phase));
    else Object.values(value).forEach(item => visit(item, phase));
  }
  const nativeFetch = window.fetch.bind(window);
  window.fetch = (...args) => {
    if (args[1]?.body) visit(args[1].body, 'request');
    return nativeFetch(...args).then(response => {
      response.clone().text().then(body => visit(body, 'response')).catch(() => {});
      return response;
    });
  };
  const nativeSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function(body) {
    visit(body, 'request');
    this.addEventListener('load', () => visit(this.responseText, 'response'));
    return nativeSend.call(this, body);
  };
}
"""


def _visible_cells(driver):
    return driver.execute_script("""
      const cells=[];
      const visit=root=>{
        for(const cell of root.querySelectorAll('[role="gridcell"][data-colid][data-rowid]'))
          if(cell.getClientRects().length) cells.push({row:cell.getAttribute('data-rowid'),
            column:cell.getAttribute('data-colid'), text:cell.innerText.trim()});
        for(const element of root.querySelectorAll('*')) if(element.shadowRoot) visit(element.shadowRoot);
      };
      visit(document); return cells;
    """)


def _cell(cells, row, column, value):
    return sum(item == {"row": row, "column": column, "text": value} for item in cells) == 1


def _correlated(trace, *, row=None, minimum_start=None, minimum_column=None,
                require_explicit_state=False):
    requests = {item["requestId"]: item for item in trace if item["phase"] == "request"}
    for reply in trace:
        if reply["phase"] != "response" or reply["status"] != "data":
            continue
        request = requests.get(reply["requestId"])
        if request is None:
            continue
        if require_explicit_state and (request["showRowTotals"] is not True
                                       or request["showColTotals"] is not True):
            continue
        if minimum_start is not None and (request["start"] is None or request["start"] < minimum_start):
            continue
        if minimum_column is not None and (request["colStart"] is None or request["colStart"] < minimum_column):
            continue
        if row is not None and not any(item["id"] == row and item["first"] == 1
                                   for item in reply["rows"]):
            continue
        return {"request": request, "response": reply}
    return None


def qualify_virtual_scroll(host: str, requirement: str) -> None:
    import pytest

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    if os.environ.get("PIVOT_FLAGSHIP_SCROLL_DATA") != "1":
        pytest.fail("Wide scroll fixture is required")

    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace, _wait_for_visible_workspace_element,
    )
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
    from selenium.webdriver.support.ui import WebDriverWait

    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "states": []}
    driver = None
    diagnostic = ""
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _SCROLL_PROBE})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 25)
        scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
        first_column = "P23_unités_sum"
        last_column = "__RowTotal__écart_sum"
        wait.until(lambda current: _cell(_visible_cells(current), "R000", first_column, "1"))
        initial = {"cells": _visible_cells(driver), "trace": driver.execute_script("return window.__pivotScrollTrace || []")}
        observation["states"].append({"phase": "initial", **initial})

        if requirement == "vertical-virtual-scroll.window":
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), 0, 10000).perform()
            observation["states"].append({"phase": "after-scroll", "cells": _visible_cells(driver),
                                          "position": driver.execute_script("return arguments[0].scrollTop", scroller)})
            def later(current):
                cells = _visible_cells(current)
                trace = current.execute_script("return window.__pivotScrollTrace || []")
                return (_cell(cells, "R119", first_column, "1")
                        and _correlated(trace, row="R119", minimum_start=50,
                                        require_explicit_state=host != "dash"))
            correlated = wait.until(later)
            position = driver.execute_script("return {top:arguments[0].scrollTop,max:arguments[0].scrollHeight-arguments[0].clientHeight}", scroller)
            assert position["top"] > 0 and position["max"] > position["top"] - 2
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), 0, -10000).perform()
            wait.until(lambda current: _cell(_visible_cells(current), "R000", first_column, "1"))
            assert not any(item["row"] == "R119" for item in _visible_cells(driver))
        elif requirement == "horizontal-virtual-scroll.window":
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), 100000, 0).perform()
            observation["states"].append({"phase": "after-scroll", "cells": _visible_cells(driver),
                                          "position": driver.execute_script("return arguments[0].scrollLeft", scroller)})
            def later(current):
                cells = _visible_cells(current)
                trace = current.execute_script("return window.__pivotScrollTrace || []")
                return (_cell(cells, "R000", last_column, "0")
                        and _correlated(trace, minimum_column=1,
                                        require_explicit_state=host != "dash"))
            correlated = wait.until(later)
            position = driver.execute_script("return {left:arguments[0].scrollLeft,max:arguments[0].scrollWidth-arguments[0].clientWidth}", scroller)
            assert position["max"] > 0 and abs(position["left"] - position["max"]) <= 2
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), -100000, 0).perform()
            wait.until(lambda current: _cell(_visible_cells(current), "R000", first_column, "1"))
        else:
            raise ValueError(requirement)
        observation["states"].append({"phase": "later", "position": position, "correlated": correlated})
        observation["states"].append({"phase": "restored", "cells": _visible_cells(driver)})
        result.update(result="passed", actionCompleted=True, v1RequestsCorrelated=True,
                      logicalValuesPreserved=True, horizontalScrollVerified=requirement.startswith("horizontal"))
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}"
    finally:
        paths = []
        if driver is not None:
            try:
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["finalScroll"] = driver.execute_script("""
                  const s=document.querySelector('[data-pivot-scroll-container]');
                  return s ? {left:s.scrollLeft,maxLeft:s.scrollWidth-s.clientWidth,
                    top:s.scrollTop,maxTop:s.scrollHeight-s.clientHeight} : null;
                """)
                observation["finalCells"] = _visible_cells(driver)
                observation["trace"] = driver.execute_script("return window.__pivotScrollTrace || []")
                observation["clientDebug"] = driver.execute_script("return window.__pivotScrollDebug || []")
                observation["browserLog"] = [{"level": item.get("level"), "message": item.get("message", "")[:500]}
                                             for item in driver.get_log("browser")]
                _network(driver)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
            finally:
                driver.quit()
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps({"diagnostic": diagnostic, **observation}, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} virtual scroll failed: {diagnostic}; evidence: {directory}"
