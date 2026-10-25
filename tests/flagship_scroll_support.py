"""Installed-host browser evidence for real row and pivot-column windows."""

from __future__ import annotations

import json
import os
from pathlib import Path
import csv
import io
import time
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
    if (value.schemaVersion === 1 && value.kind === 'export' && typeof value.requestId === 'string') {
      trace.push({phase, at: performance.now(), kind: 'export', requestId: value.requestId,
        status: value.status || null, layoutManifest: value.payload?.layoutManifest ?? null,
        layoutRevision: value.payload?.layoutRevision ?? null,
        colIds: value.payload?.colIds ?? null, format: value.payload?.format ?? null,
        payload: value.payload ?? null, state: phase === 'request' ? value.state ?? null : null});
      return;
    }
    if (Array.isArray(value.colSchema?.columns) && (value.layoutRevision || value.colSchema?.layoutRevision)) {
      trace.push({phase: 'payload', at: performance.now(),
        layoutRevision:value.layoutRevision || value.colSchema.layoutRevision,
        schema:value.colSchema.columns.map(column=>({index:column.index,id:column.id,layoutId:column.layoutId}))});
      return;
    }
    if (value.schemaVersion === 1 && value.kind === 'data' && typeof value.requestId === 'string') {
      const rows = Array.isArray(value.payload?.data) ? value.payload.data : [];
      trace.push({phase, at: performance.now(), requestId: value.requestId,
        status: value.status || null, start: value.payload?.start ?? null,
        end: value.payload?.end ?? null,
        colStart: value.payload?.col_start ?? value.payload?.colStart ?? null,
        colEnd: value.payload?.col_end ?? value.payload?.colEnd ?? null,
        layoutRevision: value.payload?.layoutRevision ?? value.payload?.colSchema?.layoutRevision ?? null,
        layoutManifest: value.payload?.layoutManifest ?? null,
        pinnedColumnIds: value.payload?.pinnedColumnIds ?? null,
        showRowTotals: value.state?.showRowTotals ?? null,
        showColTotals: value.state?.showColTotals ?? null,
        needsColSchema: value.payload?.needs_col_schema ?? value.payload?.needsColSchema ?? null,
        dataVersion: value.payload?.dataVersion ?? null,
        schema: Array.isArray(value.payload?.colSchema?.columns)
          ? value.payload.colSchema.columns.map(column => ({index:column.index,id:column.id,layoutId:column.layoutId})) : null,
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
        first_header = _wait_for_visible_workspace_element(
            driver, f'[role="columnheader"][data-header-column-id="{first_column}"]')
        assert "unités" in (first_header.get_attribute("aria-label") or "").casefold()
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
            labels = [item["text"] for item in _visible_cells(driver) if item["column"] == "hierarchy"
                      and item["text"].startswith("R")]
            assert labels == sorted(set(labels)) and "R119" in labels
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
            last_header = _wait_for_visible_workspace_element(
                driver, f'[role="columnheader"][data-header-column-id="{last_column}"]')
            assert "écart" in (last_header.get_attribute("aria-label") or "").casefold()
            observation["states"].append({"phase": "last-header", "column": last_column,
                                          "text": last_header.text,
                                          "ariaLabel": last_header.get_attribute("aria-label")})
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


def qualify_custom_pivot_order(host: str) -> None:
    """Move a remote pivot group to the first display slot through the UI."""
    import pytest
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.flagship_browser_support import _geometry_copy_current_table, _selection_menu
    from tests.test_flagship_certification import (
        _clipboard_text, _host_url, _origin, _qualification_chrome_options,
        _switch_to_workspace, _wait_for_download, _wait_for_visible_workspace_element,
        _workspace_elements,
    )
    from openpyxl import load_workbook
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait

    requirement = "custom-pivot-order.sparse-window"
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
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": _origin(_host_url(host)),
            "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"],
        })
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _SCROLL_PROBE})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 25)
        scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
        first = "P23_unités_sum"
        moved = "P00_unités_sum"
        wait.until(lambda current: _cell(_visible_cells(current), "R000", first, "1"))
        driver.execute_script("arguments[0].scrollLeft=arguments[0].scrollWidth", scroller)
        wait.until(lambda current: _cell(_visible_cells(current), "R000", moved, "1"))
        header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{moved}"]')
        ActionChains(driver).context_click(header).perform()
        ActionChains(driver).move_to_element(_selection_menu(driver, "Move Column")).perform()
        _selection_menu(driver, "Move to Start").click()
        driver.execute_script("arguments[0].scrollLeft=0", scroller)

        def moved_first(current):
            trace = current.execute_script("return window.__pivotScrollTrace || []")
            replies = [entry for entry in trace if entry["phase"] in {"response", "payload"} and entry.get("schema")]
            if not replies:
                return False
            schema = replies[-1]["schema"]
            return bool(schema and schema[0]["id"].startswith("P00_")
                        and _cell(_visible_cells(current), "R000", moved, "1"))

        wait.until(moved_first)
        trace = driver.execute_script("return window.__pivotScrollTrace || []")
        changed = [entry for entry in trace if entry["phase"] == "request" and entry.get("layoutManifest")]
        assert changed, "No saved layout manifest was sent after reordering"
        revisions = [entry["layoutRevision"] for entry in trace if entry["phase"] in {"response", "payload"}
                     and entry.get("layoutRevision")]
        assert len(set(revisions)) >= 2, "Server did not issue a new layout revision"
        observation["states"].append({"phase": "reordered", "trace": trace,
                                      "cells": _visible_cells(driver)})
        layout_id = next(column["layoutId"] for entry in trace if entry.get("schema")
                         for column in entry["schema"] if column["id"] == moved)
        header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{moved}"]')
        width_before = driver.execute_script("return arguments[0].getBoundingClientRect().width", header)
        handle = header.find_element("css selector", 'div[style*="cursor: col-resize"]')
        ActionChains(driver).move_to_element(handle).click_and_hold().move_by_offset(38, 0).release().perform()
        width_after = wait.until(lambda current: (
            width if abs((width := current.execute_script(
                "return arguments[0].getBoundingClientRect().width",
                _wait_for_visible_workspace_element(current, f'[role="columnheader"][data-header-column-id="{moved}"]'),
            )) - width_before) >= 34 else False
        ))
        observation["states"].append({"phase": "resized", "widthBefore": width_before,
                                      "widthAfter": width_after})

        header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{moved}"]')
        ActionChains(driver).context_click(header).perform()
        ActionChains(driver).move_to_element(_selection_menu(driver, "Pin")).perform()
        _selection_menu(driver, "Pin Left").click()
        wait.until(lambda current: _wait_for_visible_workspace_element(
            current, f'[role="columnheader"][data-header-column-id="{moved}"]'
        ).get_attribute("data-pinned") == "left")
        driver.execute_script("arguments[0].scrollLeft=arguments[0].scrollWidth", scroller)
        wait.until(lambda current: any(
            entry["phase"] == "request" and (entry.get("colStart") or 0) > 30
            and layout_id in (entry.get("pinnedColumnIds") or [])
            for entry in current.execute_script("return window.__pivotScrollTrace || []")
        ))
        def delivered_pinned_cell(current):
            cells = [cell for cell in _workspace_elements(
                current, f'[role="gridcell"][data-rowid="R000"][data-colid="{moved}"]'
            ) if cell.is_displayed()]
            return cells[0] if len(cells) == 1 and cells[0].text.strip() == "1" else False

        pinned_cell = wait.until(delivered_pinned_cell)
        observation["states"].append({"phase": "pinned-offscreen", "layoutId": layout_id,
                                      "scrollLeft": driver.execute_script("return arguments[0].scrollLeft", scroller),
                                      "pinnedCell": pinned_cell.text.strip()})

        sentinel = "pivot layout clipboard checkpoint"
        assert driver.execute_async_script(
            "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
            sentinel,
        )
        ActionChains(driver).context_click(pinned_cell).perform()
        _geometry_copy_current_table(driver)
        copied = wait.until(lambda current: (
            value if (value := _clipboard_text(current)) != sentinel else False
        ))
        clipboard_rows = list(csv.reader(io.StringIO(copied), delimiter="\t"))
        observation["clipboard"] = {"rows": clipboard_rows[:6], "rawLength": len(copied)}
        assert clipboard_rows[0][:4] == ["Unités (Sum)", "Ventes € (Sum)", "Écart (Sum)", "Ventes € (Sum)"]
        assert clipboard_rows[1][:4] == ["1", "0", "0", "23"]
        assert clipboard_rows[1][-1] == "R000"
        assert all(len(row) == len(clipboard_rows[0]) for row in clipboard_rows)

        started_at = time.time()
        _wait_for_visible_workspace_element(driver, "[data-testid='pivot-export-xlsx']").click()
        downloaded = _wait_for_download(downloads, timeout=60, started_at=started_at)
        workbook = load_workbook(downloaded, data_only=True)
        sheet = workbook.active
        workbook_rows = list(sheet.values)
        assert sheet.merged_cells.ranges, "Pivot group headers were not merged in XLSX"
        assert workbook_rows[0][1] == "P00"
        assert workbook_rows[1][1] == "Unités (Sum)"
        assert workbook_rows[2][:4] == ("R000", 1, 0.25, 0)
        assert workbook_rows[-1][0] == "Grand Total"
        grand_total_count = sum(row[0] == "Grand Total" for row in workbook_rows)
        observation["workbook"] = {"file": downloaded.name, "rows": workbook_rows[:6],
                                   "rowCount": len(workbook_rows), "grandTotalCount": grand_total_count,
                                   "mergedRanges": [str(item) for item in sheet.merged_cells.ranges]}
        workbook.close()

        assert driver.execute_script("""
          const find = root => {
            for (const button of root.querySelectorAll('button')) {
              if (button.textContent.trim() === 'Close') return button;
            }
            for (const element of root.querySelectorAll('*')) {
              if (element.shadowRoot) {
                const found = find(element.shadowRoot);
                if (found) return found;
              }
            }
            return null;
          };
          const button = find(document);
          if (button) button.click();
          return Boolean(button);
        """)
        _wait_for_visible_workspace_element(driver, "[data-testid='pivot-toolbar-theme']").click()
        save_button = wait.until(lambda current: next((button for button in _workspace_elements(
            current, "button"
        ) if button.is_displayed() and button.text.strip() == "Save View"), False))
        save_button.click()
        driver.refresh()
        _switch_to_workspace(driver, host)
        scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
        restored = wait.until(lambda current: (
            candidate if (candidate := _wait_for_visible_workspace_element(
                current, f'[role="columnheader"][data-header-column-id="{moved}"]'
            )).get_attribute("data-pinned") == "left" else False
        ))
        restored_width = driver.execute_script("return arguments[0].getBoundingClientRect().width", restored)
        assert abs(restored_width - width_after) <= 2
        assert _cell(_visible_cells(driver), "R000", moved, "1")
        observation["states"].append({"phase": "restored", "width": restored_width,
                                      "pinned": restored.get_attribute("data-pinned"),
                                      "cells": _visible_cells(driver)})
        assert grand_total_count == 1
        result.update(result="passed", actionCompleted=True, layoutRevisionVerified=True,
                      sparseWindowVerified=True, pinningVerified=True,
                      clipboardOrderVerified=True, workbookVerified=True,
                      savedLayoutVerified=True)
    except Exception as error:
        frames = traceback.extract_tb(error.__traceback__)
        diagnostic = f"{type(error).__name__}: {str(error)[:300]} at " + " -> ".join(
            f"{Path(frame.filename).name}:{frame.lineno}" for frame in (frames[:1] + frames[-4:])
        )
    finally:
        paths = []
        if driver is not None:
            try:
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["trace"] = driver.execute_script("return window.__pivotScrollTrace || []")
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
    assert result["result"] == "passed", f"{host} custom pivot order failed: {diagnostic}; evidence: {directory}"
