"""Private browser evidence and fail-closed helpers for product workflows.

Runtime error cases arm a one-shot, qualification-only demo-adapter fault so
all hosts receive the same genuine server V1 response. Network transport loss
is a separate host-integration concern, not evidence of a V1 error response.
"""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import csv
import io
import json
import os
from pathlib import Path
import re
import traceback
import time
from typing import Mapping, Sequence

from tests.flagship_contract import HOSTS, RESULTS, load_requirements


ROOT = Path(__file__).resolve().parents[1]


def assert_export_job_trace(trace, *, require_progress=True):
    """Require observed progress from correlated V1 controls for one server job."""
    requests = {item["requestId"]: item for item in trace if item["direction"] == "request"}
    replies = [item for item in trace if item["direction"] == "response"]
    assert replies, "No export job responses"
    job = replies[0]["payload"].get("jobId")
    assert job, "No server-issued job ID"
    rows = size = 0
    advanced = False
    for reply in replies:
        request = requests.get(reply["requestId"])
        assert request, "Uncorrelated response"
        payload = reply["payload"]
        assert payload.get("jobId") == job, "Unexpected job ID"
        assert request["payload"].get("action") in {"start", "status", "cancel"}
        if request["payload"]["action"] != "start":
            assert request["payload"].get("jobId") == job
        current_rows, current_size = payload["rowsWritten"], payload["bytesWritten"]
        assert current_rows >= rows and current_size >= size, "Progress counters regressed"
        advanced |= current_rows > rows or current_size > size
        rows, size = current_rows, current_size
        if payload["phase"] != "completed":
            assert not payload.get("payloadRef"), "Unfinished job published output"
    if require_progress:
        assert advanced, "No measured export progress"
    return job


def assert_export_cancel_ack(trace, ui, cleanup, job_id):
    """Server terminal and visible acknowledgement must follow actual cleanup."""
    assert cleanup["taskDone"] is True and cleanup["nativeRunning"] == 0
    assert cleanup["activeSlots"] == cleanup["reservedBytes"] == 0
    assert not cleanup["temporaryFiles"]
    requests = {item["requestId"]: item for item in trace if item["direction"] == "request"}
    replies = [item for item in trace if item["direction"] == "response"
               and item["payload"].get("jobId") == job_id and item["payload"].get("phase") == "cancelled"]
    assert replies, "No server-acknowledged cancelled response"
    for reply in replies:
        request = requests.get(reply["requestId"])
        assert request and request["payload"].get("action") in {"cancel", "status"}
        assert request["payload"].get("jobId") == job_id
        assert reply["atMs"] >= cleanup["releasedAtMs"], "Server acknowledged before cleanup"
    visible = [event for event in ui if event["phase"] == "cancelled"]
    assert visible, "No visible cancelled acknowledgement"
    assert all(event["atMs"] >= cleanup["releasedAtMs"] for event in visible), "UI acknowledged before cleanup"


def write_export_job_observation(path, observation, result, diagnostic):
    observation.update(diagnostic=diagnostic, result=result,
                       loadedAssetSha256=dict(result.get("loadedAssetSha256", {})))
    path.write_text(json.dumps(observation, indent=2), encoding="utf-8")
SELECTION_COPY_TIMEOUT = 8
_SAFE_OBSERVATIONS = frozenset({
    "measuredProgressVerified", "serverCancellationVerified", "nativeCleanupVerified",
    "noPartialDownloadVerified", "parsedCsvVerified", "parsedXlsxVerified",
    "visibleError", "loadingCleared", "failedRequestCorrelated", "retryCorrelated",
    "fixtureTotalsVerified", "staleCellsAbsent", "newDownloads", "networkFailureCount",
    "infrastructureUnavailable", "actionCompleted", "lateResponseAbsent",
    "lateResponseUnverified", "staleResponseUnverified", "privateExceptionHidden",
    "logicalCellsVerified", "clipboardBytesVerified", "displayOrderVerified",
    "pinnedNoDuplicates", "horizontalScrollVerified", "rangeCleared", "clearedCopyAbsent",
    "selectedCellCount",
    "nativeKeyboardCopyVerified", "clearedClipboardMonitored",
    "expandedChildrenVerified", "collapsedChildrenVerified", "v1RequestsCorrelated",
    "resizeDeltaVerified", "autoSizeFitVerified", "autoSizeIdempotent", "pinRoundTripVerified",
    "geometryScrollVerified", "logicalValuesPreserved",
    "clipboardOrderVerified", "lastColumnVisible",
})


_EXPORT_JOB_PROBE = """
if (!window.__pivotExportJobs) {
  const trace = window.__pivotExportJobs = [];
  function visit(value, direction) {
    if (typeof value === 'string') { try { value = JSON.parse(value); } catch (_) { return; } }
    if (!value || typeof value !== 'object') return;
    if (value.schemaVersion === 1 && value.kind === 'export' && value.requestId) {
      trace.push({direction, requestId: value.requestId, status: value.status,
                  payload: value.payload, at: performance.now(), atMs: Date.now()}); return;
    }
    Object.values(value).forEach(child => visit(child, direction));
  }
  const fetch = window.fetch.bind(window);
  window.fetch = (...args) => {
    visit(args[1]?.body, 'request');
    return fetch(...args).then(response => {
      response.clone().text().then(text => visit(text, 'response')).catch(() => {});
      return response;
    });
  };
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function(body) {
    visit(body, 'request');
    this.addEventListener('load', () => visit(this.responseText, 'response'));
    return send.call(this, body);
  };
  window.__pivotExportUi = [];
  const roots = new WeakSet();
  let previousPhase = null;
  const capture = () => {
    let phase = null;
    const visit = root => {
      if (!roots.has(root)) {
        roots.add(root);
        new MutationObserver(capture).observe(root, {subtree: true, childList: true,
          attributes: true, attributeFilter: ['data-export-phase']});
      }
      const dialog = root.querySelector('[data-export-phase]');
      if (dialog) phase = dialog.getAttribute('data-export-phase');
      for (const element of root.querySelectorAll('*')) if (element.shadowRoot) visit(element.shadowRoot);
    };
    visit(document);
    if (phase && phase !== previousPhase) window.__pivotExportUi.push({phase, atMs: Date.now()});
    previousPhase = phase;
  };
  capture();
  setInterval(capture, 100);
}
"""


def _assert_flat_export(path, fmt, count):
    if fmt == "csv":
        with path.open(encoding="utf-8-sig", newline="") as source:
            rows = list(csv.reader(source))
    else:
        from openpyxl import load_workbook
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            rows = [list(row) for row in workbook.active.iter_rows(values_only=True)]
        finally:
            workbook.close()
    assert rows[0] == ["Region", "Sales (Sum)"], rows[0]
    assert len(rows) == count + 1
    assert [(row[0], float(row[1])) for row in rows[1:]] == [
        (f"R{i:04d} \u6771\u4eac", i + .25) for i in range(count)]
    return {"rows": count, "columns": 2, "headers": rows[0], "firstRow": rows[1], "lastRow": rows[-1],
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def qualify_export_jobs(host, requirement):
    import pytest
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    assert os.environ.get("PIVOT_FLAGSHIP_EXPORT_JOB_DATA") == "1", "Flat export fixture required"
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace, _click_export,
        _workspace_elements, _wait_for_download, _verify_release_asset_batch,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait

    root = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"])
    directory = root / requirement / host
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "downloads": {}, "trace": [], "cleanup": []}
    driver = None
    diagnostic = ""
    try:
        binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _EXPORT_JOB_PROBE})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"Chrome {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 25)

        def phase(value):
            return next((el for el in _workspace_elements(driver, f'[data-export-phase="{value}"]')
                         if el.is_displayed()), False)

        def button(label):
            element = wait.until(lambda _: next((
                el for el in _workspace_elements(driver, '[aria-label="Export progress"] button')
                if el.text == label and el.is_displayed() and el.is_enabled()), False))
            element.click()

        cancelled = requirement != "large-delivery.progress"
        count = 512
        if cancelled:
            previous = set(root.glob("export-worker-*.jsonl"))
            (root / "slow-next-export").write_text("one-shot", encoding="ascii")
            _click_export(driver, "csv")
            wait.until(lambda _: phase("running"))

            def native_running(_):
                files = set(root.glob("export-worker-*.jsonl")) - previous
                return next((path for path in files if any(
                    json.loads(line).get("nativeRunning", 0) > 0 for line in path.read_text().splitlines())), False)

            worker = wait.until(native_running)
            bar = _workspace_elements(driver, '[role="progressbar"]')[0]
            assert bar.get_attribute("aria-valuenow") is None, "Unknown total has fabricated percentage"
            button("Cancel")
            wait.until(lambda _: phase("cancelling"))
            wait.until(lambda _: phase("cancelled"))
            wait.until(lambda _: json.loads(worker.read_text().splitlines()[-1])["executorRunning"] is False)
            samples = [json.loads(line) for line in worker.read_text().splitlines()]
            assert_cancellation_lifetime(samples)
            assert samples[-1]["nativeRunning"] == 0 and not samples[-1]["temporaryFiles"]
            assert samples[-1]["nativeCancellationAttempts"] > samples[0]["nativeCancellationAttempts"]
            trace = driver.execute_script("return window.__pivotExportJobs")
            job = assert_export_job_trace(trace, require_progress=False)
            cleanup = json.loads(worker.with_suffix(".cleanup.json").read_text())
            ui = driver.execute_script("return window.__pivotExportUi")
            assert_export_cancel_ack(trace, ui, cleanup, job)
            assert any(item["direction"] == "request" and item["payload"].get("action") == "cancel"
                       and item["payload"].get("jobId") == job for item in trace)
            assert not list(downloads.iterdir()), "Cancelled export created a partial download"
            observation.update(cleanup=samples, releaseEvent=cleanup, uiPhases=ui,
                               cancelledJobId=job, cancelledTrace=trace)
            driver.save_screenshot(str(directory / "cancelled.png"))
            button("Close")
            search = _workspace_elements(driver, '[data-testid="pivot-global-search"]')[0]
            search.send_keys("R0000")
            wait.until(lambda _: any("R0000 \u6771\u4eac" in el.text for el in _workspace_elements(driver, '[role="gridcell"]')))
            # Wait for the filtered V1 viewport to settle before starting fresh work.
            time.sleep(.8)
            count = 1
            result.update(serverCancellationVerified=True, nativeCleanupVerified=True, noPartialDownloadVerified=True)

        for fmt in ("csv", "xlsx"):
            driver.execute_script("window.__pivotExportJobs.length = 0")
            _click_export(driver, fmt)
            wait.until(lambda _: phase("running"))
            bar = _workspace_elements(driver, '[role="progressbar"]')[0]
            assert bar.get_attribute("aria-valuenow") is None
            if not cancelled:
                def measured(_):
                    trace = driver.execute_script("return window.__pivotExportJobs")
                    return next((item for item in trace if item["direction"] == "response"
                                 and item["payload"].get("phase") == "running"
                                 and item["payload"].get("rowsWritten", 0) > 0), False)
                progress = wait.until(measured)
                assert progress["payload"]["totalRows"] is None
                wait.until(lambda _: any("rows" in el.text and "bytes" in el.text
                                         and not el.text.startswith("0 rows")
                                         for el in _workspace_elements(driver, '[aria-label="Export progress"] span')))
            wait.until(lambda _: phase("completed"))
            path = _wait_for_download(downloads, fmt, timeout=25)
            observation["downloads"][fmt] = _assert_flat_export(path, fmt, count)
            trace = driver.execute_script("return window.__pivotExportJobs")
            fresh_job = assert_export_job_trace(trace)
            assert fresh_job != observation.get("cancelledJobId"), "Retry reused the cancelled job"
            assert trace[-1]["payload"]["phase"] == "completed"
            observation["trace"].extend(trace)
            driver.save_screenshot(str(directory / f"{fmt}.png"))
            button("Close")
        assert not list(downloads.glob("*.crdownload"))
        _verify_release_asset_batch(driver, require_assets=True)
        result.update(result="passed", measuredProgressVerified=True, parsedCsvVerified=True,
                      parsedXlsxVerified=True, loadedAssetSha256=dict(driver._pivot_loaded_assets))
    except Exception:
        diagnostic = traceback.format_exc()
    finally:
        if driver is not None:
            observation["traceAtExit"] = driver.execute_script("return window.__pivotExportJobs || []")
            result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
            driver.save_screenshot(str(directory / "final.png"))
            driver.quit()
        evidence = directory / "observation.json"
        write_export_job_observation(evidence, observation, result, diagnostic)
        result["evidence_paths"] = [str(evidence), *(str(path) for path in directory.glob("*.png")),
                                    *(str(path) for path in downloads.iterdir() if path.is_file())]
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement} failed; evidence: {directory}; {diagnostic}"


def assert_selected_clipboard(text: str, expected: list[list[str]], *, line_separator="\n") -> list[list[str]]:
    """The oracle is literal, including separators and absence of headers/extra rows."""
    assert line_separator in ("\n", "\r\n"), "Unsupported clipboard line separator"
    exact = line_separator.join("\t".join(row) for row in expected)
    assert text == exact, "Selected clipboard bytes differ from the literal fixture oracle"
    parsed = list(csv.reader(io.StringIO(text), delimiter="\t"))
    assert parsed == expected, "Selected clipboard logical cells differ"
    return parsed


def selected_headers_oracle(columns: list[str]) -> list[str]:
    """Literal Q1 fixture labels in the selected displayed order."""
    labels = {"Q1_unités_sum": "Unités", "Q1_ventes_€_sum": "Ventes (€)"}
    assert len(columns) == 2 and set(columns) == set(labels), "Header case must select only Q1 units and sales"
    return [labels[column] for column in columns]


def formatted_selection_oracle(rows, columns: list[str]) -> list[list[str]]:
    """Exact two-decimal Q1 values for the Included fixture rows."""
    values = {
        "Éclair": {"Q1_ventes_€_sum": "100.25", "Q1_unités_sum": "2.00"},
        "München": {"Q1_ventes_€_sum": "49.75", "Q1_unités_sum": "3.00"},
    }
    assert len(rows) == 2 and {row["label"] for row in rows} == set(values)
    assert len(columns) == 2 and set(columns) == set(next(iter(values.values())))
    return [[values[row["label"]][column] for column in columns] for row in rows]


def _product_display_order_copy_oracle(rows):
    """Pin the backend-native visible order independently of clipboard contents."""
    assert [row["label"] for row in rows] == ["Éclair", "München"], (
        "Descending displayed region order differs from the product backend-native fixture oracle")
    return [["2.00", "100.25"], ["3.00", "49.75"]]


def assert_clipboard_stable(read, expected, *, timeout, poll=0.05):
    started = time.monotonic()
    samples = 0
    while True:
        assert read() == expected, "Clipboard changed after selection clear"
        samples += 1
        elapsed = time.monotonic() - started
        if elapsed >= timeout:
            return {"durationSeconds": elapsed, "sampleCount": samples, "pollSeconds": poll}
        time.sleep(min(poll, timeout - elapsed))


def assert_resize_geometry(before, after):
    assert abs((after["width"] - before["width"]) - 40) <= 2, "Resize missed +40±2 CSS px"
    assert after["order"] == before["order"], "Resize changed displayed column order"
    assert after["cells"] == before["cells"], "Resize changed logical cell IDs or values"
    assert after["neighbors"] == before["neighbors"], "Resize changed neighboring cell IDs or values"


def assert_auto_size_geometry(before, first, second, *, min_width, max_width):
    assert first["order"] == before["order"] == second["order"], "Auto-size hid or reordered a column"
    assert min_width <= first["width"] <= max_width, "Auto-size exceeded column bounds"
    assert first["width"] >= before["requiredWidth"], "Auto-size clipped longest visible text"
    assert first["fits"] is True and second["fits"] is True, "Auto-sized rendered text overflows its column"
    assert abs(second["width"] - first["width"]) <= 2, "Repeated auto-size was not idempotent"


def assert_pin_round_trip(before, pinned, scrolled, end, restored, column):
    assert pinned["pinnedLeft"] == [column] and scrolled["pinnedLeft"] == [column], "Target column not pinned left"
    expected_order = [before["order"][0], column, *[id for id in before["order"][1:] if id != column]]
    assert pinned["order"] == expected_order, "Pin did not change logical column order to the requested position"
    assert len(set(pinned["order"])) == len(pinned["order"]), "Pinned column duplicated"
    assert scrolled["order"] == pinned["order"], "Horizontal scroll changed pinned logical order"
    assert scrolled["scrollLeft"] > pinned["scrollLeft"], "Horizontal scroll did not move"
    assert abs(scrolled["pinnedX"] - pinned["pinnedX"]) <= 1, "Pinned column moved during scroll"
    assert scrolled["centerX"] < pinned["centerX"] - 1, "Center column did not move during scroll"
    assert end["pinnedLeft"] == [column] and abs(end["pinnedX"] - pinned["pinnedX"]) <= 1, "Pinned cell did not survive last-column scroll"
    assert end["scrollLeft"] > scrolled["scrollLeft"] and abs(end["scrollLeft"] - end["maxScrollLeft"]) <= 2, "Did not reach last horizontal column"
    assert end["lastColumnVisible"] is True, "Last column is not actually visible"
    assert all(state["cells"] == before["cells"] for state in (pinned, scrolled, end, restored)), "Pinning changed or duplicated logical cells"
    assert restored["pinnedLeft"] == before["pinnedLeft"] == [], "Unpin did not clear left pin"
    assert restored["order"] == before["order"], "Unpin did not restore prior logical order"


def assert_pinned_clipboard_order(rows, order):
    assert len(order) >= 4 and order[0] == "hierarchy", "Pinned clipboard oracle requires the fixed fixture layout"
    assert order[1] == "Q2_ventes_€_sum", "Q2 sales was not first displayed measure"
    assert len(rows) >= 3 and len(rows[0]) == len(order), "Clipboard headers missing or column count changed"
    assert all(label.strip() for label in rows[0]), "Clipboard header label missing"
    assert all(len(row) == len(order) for row in rows[1:]), "Clipboard row width differs from header/order"
    by_label = {row[0]: row for row in rows[1:] if row[0] in ("Éclair", "München")}
    assert len(by_label) == 2 and sum(row[0] in by_label for row in rows[1:]) == 2, "Fixture rows missing or duplicated"
    for label, q2, q1 in (("Éclair", 20, 100), ("München", 30, 50)):
        row = by_label[label]
        assert float(row[order.index("Q2_ventes_€_sum")]) == q2, "Pinned Q2 sales not in clipboard display position"
        assert float(row[order.index("Q1_ventes_€_sum")]) == q1, "Unpinned Q1 sales shifted or changed in clipboard"


def _selection_ids(driver):
    from tests.test_flagship_certification import _workspace_elements

    # Keep the list as well as the set: duplicated pinned DOM nodes must not
    # disappear silently in a set comparison.
    return [(cell.get_attribute("data-rowid"), cell.get_attribute("data-colid"))
            for cell in _workspace_elements(driver, '[role="gridcell"][aria-selected="true"]')
            if cell.is_displayed()]


def _selection_menu(driver, label):
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _workspace_elements

    return WebDriverWait(driver, 8).until(lambda current: next((
        item for item in _workspace_elements(current, '[role="menuitem"]')
        if item.is_displayed() and item.is_enabled() and item.get_attribute("title") == label
    ), False))


def _selection_cell(driver, row_id, column_id):
    from tests.test_flagship_certification import _wait_for_visible_workspace_element

    return _wait_for_visible_workspace_element(
        driver, f'[role="gridcell"][data-rowid="{row_id}"][data-colid="{column_id}"]')


def _selection_rows(driver):
    from selenium.common.exceptions import StaleElementReferenceException
    from tests.test_flagship_certification import _workspace_elements

    for attempt in range(3):
        try:
            return [{"id": cell.get_attribute("data-rowid"), "label": cell.text.strip()}
                    for cell in _workspace_elements(driver, '[role="gridcell"][data-colid="hierarchy"]')
                    if cell.is_displayed() and cell.text.strip() in ("Éclair", "München")]
        except StaleElementReferenceException:
            if attempt == 2:
                raise


def _pinned_boundary_order(driver, expected):
    """Pin metadata can commit before React replaces the virtualized body."""
    order = _selection_column_order(driver)
    return order if len(order) == len(expected) and set(order) == set(expected) else False


def _selection_column_order(driver):
    """Read actual body geometry, excluding duplicate/group header containers."""
    return driver.execute_script("""
      const collect=(root,selector,out)=>{
        out.push(...root.querySelectorAll(selector));
        for(const element of root.querySelectorAll('*'))
          if(element.shadowRoot) collect(element.shadowRoot,selector,out);
      };
      const cells=[];
      collect(document,'[role="gridcell"][data-rowid][data-colid]',cells);
      const visible=element=>{
        const rect=element.getBoundingClientRect();
        return rect.width>0 && rect.height>0 && getComputedStyle(element).visibility!=='hidden';
      };
      const first=cells.find(cell=>cell.dataset.colid==='hierarchy' && visible(cell));
      if(!first) return [];
      return cells.filter(cell=>cell.dataset.rowid===first.dataset.rowid && visible(cell))
        .map(cell=>[cell.getBoundingClientRect().left,cell.dataset.colid])
        .sort((a,b)=>a[0]-b[0]).map(entry=>entry[1]);
    """)


def _reorder_selection_column(driver, column, direction, first, second, observation):
    from selenium.common.exceptions import StaleElementReferenceException
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _wait_for_visible_workspace_element

    rows_before = _selection_rows(driver)
    order_before = _selection_column_order(driver)
    before_key_count = len(driver.execute_script("return window.__pivotHeaderMoveKeys || []"))
    for attempt in range(3):
        header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{column}"]')
        try:
            # Element key input focuses this tabIndex=0 header without a sorting click.
            driver.execute_script("""
      const header = arguments[0];
      window.__pivotHeaderMoveKeys = window.__pivotHeaderMoveKeys || [];
      header.addEventListener('keydown', event => {
        if (event.key === 'ArrowLeft' || event.key === 'ArrowRight')
          window.__pivotHeaderMoveKeys.push({column: header.getAttribute('data-header-column-id'),
            key: event.key, ctrl: event.ctrlKey, trusted: event.isTrusted, at: performance.now()});
      }, true);
            """, header)
            header.send_keys(Keys.CONTROL + direction + Keys.NULL)
            break
        except StaleElementReferenceException:
            # The key may have reached the header just before replacement.
            if len(driver.execute_script("return window.__pivotHeaderMoveKeys || []")) > before_key_count:
                break
            if attempt == 2:
                raise
    observation["headerMoveKeys"] = driver.execute_script("return window.__pivotHeaderMoveKeys || []")
    WebDriverWait(driver, 8).until(lambda current: (
        (order := _selection_column_order(current)) != order_before
        and first in order and second in order and order.index(first) < order.index(second)
        and _selection_rows(current) == rows_before))
    rows_after, order_after = _selection_rows(driver), _selection_column_order(driver)
    observation["actions"].append({"action": "header Ctrl+Arrow", "column": column,
        "direction": "left" if direction == Keys.ARROW_LEFT else "right",
        "columnOrderBefore": order_before, "columnOrderAfter": order_after,
        "rowsBefore": rows_before, "rowsAfter": rows_after})
    assert order_before != order_after, "Column move had no displayed effect"
    assert rows_after == rows_before, "Column reorder unexpectedly sorted the displayed rows"


def _wait_selection_sort(driver, checkpoint, column, observation):
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _wait_for_visible_workspace_element

    def correlated_response(current):
        trace = current.execute_script("return window.__pivotRecoveryProbe || []")
        requests = {entry["requestId"] for entry in trace if entry["phase"] == "request"
                    and entry["kind"] == "data" and entry["at"] > checkpoint}
        return next((entry["requestId"] for entry in trace if entry["phase"] == "response"
                     and entry["kind"] == "data" and entry["status"] == "data"
                     and entry["requestId"] in requests and entry["at"] > checkpoint), None)

    request_id = WebDriverWait(driver, 12).until(correlated_response)
    header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{column}"]')
    assert header.get_attribute("aria-sort") == "desc", "Descending sort indicator absent"
    observation["actions"].append({"action": "sort completed", "column": column,
        "indicator": header.get_attribute("aria-sort"), "requestId": request_id, "displayedRows": _selection_rows(driver)})


def _select_rectangle(driver, row_ids, column_ids):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait

    first = _selection_cell(driver, row_ids[0], column_ids[0])
    last = _selection_cell(driver, row_ids[-1], column_ids[-1])
    ActionChains(driver).move_to_element(first).click_and_hold().move_to_element(last).release().perform()
    expected = {(row, col) for row in row_ids for col in column_ids}
    WebDriverWait(driver, 8).until(lambda current: set(_selection_ids(current)) == expected)
    ids = _selection_ids(driver)
    assert len(ids) == len(expected), "Pinned cells are selected more than once in the displayed DOM"
    return ids


def _selection_context_probe(driver, cell, phase):
    """Observe native interaction across shadow roots without changing event handling."""
    return driver.execute_script("""
      const target=arguments[0], phase=arguments[1];
      const describe=el=>el?.getAttribute ? {tag:el.tagName,role:el.getAttribute('role'),
        title:el.getAttribute('title'),row:el.getAttribute('data-rowid'),
        column:el.getAttribute('data-colid'),text:el.textContent?.trim().slice(0,80)} : {tag:el?.nodeName};
      const roots=[], collect=root=>{
        roots.push(root);
        for(const el of root.querySelectorAll('*')) if(el.shadowRoot) collect(el.shadowRoot);
      };
      collect(document);
      const rect=el=>{const r=el.getBoundingClientRect();
        return {x:r.x,y:r.y,width:r.width,height:r.height};};
      const menus=()=>roots.flatMap(root=>[...root.querySelectorAll('[role="menu"]')])
        .map(el=>({rect:rect(el),items:[...el.querySelectorAll('[role="menuitem"]')]
          .map(describe)}));
      const active=()=>{let el=document.activeElement;
        while(el?.shadowRoot?.activeElement) el=el.shadowRoot.activeElement;
        return describe(el);};
      if(!window.__pivotSelectionContextProbe){
        const trace=[];
        const append=entry=>{trace.push({at:performance.now(),...entry});
          if(trace.length>160) trace.shift();};
        window.__pivotSelectionContextProbe={trace,append};
        for(const type of ['pointerdown','mousedown','mouseup','contextmenu','click','focusin'])
          document.addEventListener(type,event=>{
            const entry={kind:'event',type,trusted:event.isTrusted,button:event.button,
              x:event.clientX,y:event.clientY,target:describe(event.target),
              path:event.composedPath().slice(0,8).map(describe)};
            append(entry);
            setTimeout(()=>append({kind:'after-event',type,
              prevented:event.defaultPrevented,active:active(),menus:menus()}),0);
          },true);
        let previous=JSON.stringify(menus());
        const observer=new MutationObserver(()=>{
          const current=JSON.stringify(menus());
          if(current!==previous){previous=current;
            append({kind:'menu-change',active:active(),menus:JSON.parse(current)});}
        });
        for(const root of roots) observer.observe(root,{childList:true,subtree:true,
          attributes:true,attributeFilter:['style']});
      }
      let hit=null;
      if(target){
        const r=target.getBoundingClientRect();
        const x=(Math.max(0,r.left)+Math.min(innerWidth,r.right))/2;
        const y=(Math.max(0,r.top)+Math.min(innerHeight,r.bottom))/2;
        let el=document.elementFromPoint(x,y);
        const path=[];
        while(el){path.push(describe(el));const next=el.shadowRoot?.elementFromPoint(x,y);
          if(!next||next===el) break;el=next;}
        hit={x,y,path};
      }
      window.__pivotSelectionContextProbe.append({kind:'checkpoint',phase,
        target:describe(target),rect:target?rect(target):null,hit,active:active(),
        viewport:{width:innerWidth,height:innerHeight,scrollX,scrollY},menus:menus()});
      return window.__pivotSelectionContextProbe.trace;
    """, cell, phase)


def _copy_selected(driver, cell, expected, observation, *, menu_label="Copy Selection"):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _clipboard_text

    observation["selectionContextTrace"] = _selection_context_probe(driver, cell, f"{menu_label}:before")
    sentinel = "qualification clipboard checkpoint"
    assert driver.execute_async_script(
        "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
        sentinel)
    try:
        ActionChains(driver).context_click(cell).perform()
        observation["selectionContextTrace"] = _selection_context_probe(driver, cell, f"{menu_label}:after-native-context")
        menu_item = _selection_menu(driver, menu_label)
    finally:
        observation["selectionContextTrace"] = _selection_context_probe(driver, cell, f"{menu_label}:after-menu-wait")
    clicked_title = menu_item.get_attribute("title")
    assert clicked_title == menu_label, "Selection copy menu action differs from the requested action"
    menu_item.click()
    observation["copyMenuAction"] = clicked_title
    text = WebDriverWait(driver, SELECTION_COPY_TIMEOUT).until(lambda current: (
        value if (value := _clipboard_text(current)) != sentinel else False))
    observation["clipboardText"] = text
    observation["parsedTSV"] = list(csv.reader(io.StringIO(text), delimiter="\t"))
    assert_selected_clipboard(text, expected, line_separator=observation["clipboardLineSeparator"])


def _open_selection_number_format(driver, observation=None):
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _workspace_elements, _wait_for_visible_workspace_element

    steps = []
    if observation is not None:
        observation["numberFormatSteps"] = steps
    def active_control():
        return driver.execute_script("""
          let el=document.activeElement;
          while (el?.shadowRoot?.activeElement) el=el.shadowRoot.activeElement;
          return {testId:el?.getAttribute('data-testid'),title:el?.getAttribute('title'),text:el?.textContent};
        """)
    button = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toolbar-format"]')
    already_open = any(item.is_displayed() for item in _workspace_elements(driver, '[title="Open number format options"]'))
    steps.append({"before": "toolbar activation", "formatOptionsVisible": already_open})
    if not already_open:
        button.send_keys(Keys.SPACE)
        steps.append({"after": "toolbar Space", "focusedControl": active_control()})
    options = _wait_for_visible_workspace_element(driver, '[title="Open number format options"]')
    steps.append({"formatBandVisible": options.is_displayed()})
    options.send_keys(Keys.SPACE)
    steps.append({"after": "options Space", "focusedControl": active_control()})
    dialog = '[role="dialog"][aria-label="Format Cells"]'
    _wait_for_visible_workspace_element(driver, dialog)
    return dialog


def _selection_number_format(driver, observation=None):
    """Explicit two-decimal presentation for order/pinning; never claim raw copy."""
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _workspace_elements, _wait_for_visible_workspace_element

    dialog = _open_selection_number_format(driver, observation)
    number = _wait_for_visible_workspace_element(driver, dialog + ' input[type="number"]')
    number.send_keys(Keys.CONTROL, "a")
    number.send_keys("2")
    for button in _workspace_elements(driver, dialog + ' button'):
        if button.text.strip() == "OK":
            button.send_keys(Keys.SPACE)
            break
    WebDriverWait(driver, 8).until(lambda current: not _workspace_elements(current, dialog))


def _inspect_selection_number_format(driver, observation):
    """Record actual UI categories/precision without applying a new format."""
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _workspace_elements, _wait_for_visible_workspace_element

    dialog = _open_selection_number_format(driver, observation)
    observation["rawCopyFormatOptions"] = {
        "dialogText": _wait_for_visible_workspace_element(driver, dialog).text,
        "buttons": [button.text for button in _workspace_elements(driver, dialog + " button")],
        "numberInputs": [{name: field.get_attribute(name) for name in ("value", "min", "max", "step")}
                         for field in _workspace_elements(driver, dialog + ' input[type="number"]')],
    }
    _wait_for_visible_workspace_element(driver, dialog + ' button[title="Close"]').send_keys(Keys.SPACE)
    WebDriverWait(driver, 8).until(lambda current: not _workspace_elements(current, dialog))


def _selection_keyboard_copy(driver, observation):
    """Observe trusted key delivery at the grid without replacing any handler."""
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _wait_for_visible_workspace_element

    grid = _wait_for_visible_workspace_element(driver, '[role="grid"][data-pivot-scroll-container]')
    driver.execute_script("""
      if (!arguments[0].__pivotSelectionKeyObserver) {
        window.__pivotSelectionKeys=window.__pivotSelectionKeys||[];
        arguments[0].__pivotSelectionKeyObserver=true;
        arguments[0].addEventListener('keydown',event=>{
          if (event.ctrlKey && event.key.toLowerCase()==='c')
            window.__pivotSelectionKeys.push({trusted:event.isTrusted,at:performance.now(),
              targetRole:event.target.getAttribute('role')});
        },true);
      }
    """, grid)
    before = driver.execute_script("return window.__pivotSelectionKeys.length")
    grid.send_keys(Keys.CONTROL + "c" + Keys.NULL)
    WebDriverWait(driver, SELECTION_COPY_TIMEOUT).until(lambda current:
        current.execute_script("return window.__pivotSelectionKeys.length") > before)
    events = driver.execute_script("return window.__pivotSelectionKeys")
    assert len(events) == before + 1, "One copy shortcut produced multiple grid key events"
    assert events[-1]["trusted"] is True and events[-1]["targetRole"] == "grid", "Copy key missed the workspace grid"
    observation["nativeCopyKeys"] = events


def qualify_selected_clipboard(host: str, requirement: str) -> None:
    """Physical selection/menu/keyboard actions against fresh installed hosts.

    A clipboard read is an OS/browser read. The boundary observer synchronously
    forwards identical native writes; product handlers and payloads stay intact.
    """
    import pytest
    from tests.flagship_clipboard_observer import CLIPBOARD_BOUNDARY_OBSERVER, monitor_single_clipboard_write

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _host_url, _origin, _qualification_chrome_options, _switch_to_workspace,
        _workspace_elements, _wait_for_visible_workspace_element,
        _clipboard_text, _verify_release_asset_batch,
    )
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait

    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    driver = None
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "actions": [], "expectedRawBody": [["100.25", "2"], ["49.75", "3"]]}
    diagnostic = ""
    paths = []
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _PROBE_SCRIPT})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": CLIPBOARD_BOUNDARY_OBSERVER})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "window.__PIVOT_QUALIFY_KEYBOARD_MOVES__ = true;"})
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": _origin(_host_url(host)), "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        observation["browserPlatform"] = driver.capabilities.get("platformName")
        observation["clipboardLineSeparator"] = "\r\n" if str(driver.capabilities.get("platformName", "")).lower() == "windows" else "\n"
        wait = WebDriverWait(driver, 12)
        sales, units = "Q1_ventes_€_sum", "Q1_unités_sum"
        rows = wait.until(lambda current: (
            candidate if len(candidate := _selection_rows(current)) == 2
            and {row["label"] for row in candidate} == {"Éclair", "München"} else False
        ))
        assert not any("São Paulo" in cell.text for cell in _workspace_elements(driver, '[data-colid="hierarchy"]'))
        observation["initialRows"] = rows
        initial_order = wait.until(lambda current: (
            order if units in (order := _selection_column_order(current)) and sales in order else False))
        observation["initialDisplayedColumnIds"] = initial_order
        columns = sorted([units, sales], key=initial_order.index)
        if requirement == "copy-selection.logical-cells":
            if initial_order.index(sales) < initial_order.index(units):
                _reorder_selection_column(driver, units, Keys.ARROW_LEFT, units, sales, observation)
            _reorder_selection_column(driver, sales, Keys.ARROW_LEFT, sales, units, observation)
            checkpoint = driver.execute_script("return performance.now()")
            header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{sales}"]')
            header.send_keys(Keys.ALT + Keys.ARROW_DOWN + Keys.NULL)
            observation["actions"].append("physical Q1 sales Alt+Down descending sort establishes Éclair then München")
            _wait_selection_sort(driver, checkpoint, sales, observation)
            wait.until(lambda current: [row["label"] for row in _selection_rows(current)] == ["Éclair", "München"])
            columns = [sales, units]
        elif requirement in ("copy-selection.display-order", "copy-selection.pinned-columns",
                             "copy-selection.formatted-values", "copy-selection.hidden-columns"):
            _selection_number_format(driver, observation)
            observation["actions"].append("set displayed numeric precision to 2dp")
        if requirement == "copy-selection.display-order":
            # Exercise a real move in both directions, ending in required units-first order.
            if initial_order.index(units) < initial_order.index(sales):
                _reorder_selection_column(driver, sales, Keys.ARROW_LEFT, sales, units, observation)
            _reorder_selection_column(driver, units, Keys.ARROW_LEFT, units, sales, observation)
            columns = [units, sales]
            header = _wait_for_visible_workspace_element(driver, '[role="columnheader"][data-header-column-id="hierarchy"]')
            checkpoint = driver.execute_script("return performance.now()")
            ActionChains(driver).context_click(header).perform()
            ActionChains(driver).move_to_element(_selection_menu(driver, "Sort")).perform()
            _selection_menu(driver, "Descending").click()
            observation["actions"].append("physical hierarchy Sort > Descending")
            _wait_selection_sort(driver, checkpoint, "hierarchy", observation)
        if requirement == "copy-selection.hidden-columns":
            variance = "Q1_écart_sum"
            assert variance in initial_order, "The column to hide was not initially displayed"
            # Move variance between the endpoints so a hidden-position placeholder
            # would make the two-column clipboard assertion fail.
            for _ in range(3):
                current_order = _selection_column_order(driver)
                left, right = sorted([units, sales], key=current_order.index)
                if current_order.index(left) < current_order.index(variance) < current_order.index(right):
                    break
                assert current_order.index(variance) > current_order.index(right)
                _reorder_selection_column(driver, variance, Keys.ARROW_LEFT, variance, right, observation)
            else:
                raise AssertionError("Could not place variance between selection endpoints")
            observation["beforeHideColumnIds"] = _selection_column_order(driver)
            toggle = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]')
            if not any(item.is_displayed() for item in _workspace_elements(driver, '[data-sidebar-drop-zone="vals"]')):
                toggle.click()
            tab = wait.until(lambda current: next((element for element in _workspace_elements(current, 'div[style*="cursor: pointer"]')
                if element.is_displayed() and element.text == "Columns"
                and element.value_of_css_property("cursor") == "pointer"), None))
            tab.click()
            search = _wait_for_visible_workspace_element(driver, 'input[placeholder="Search columns..."]')
            search.send_keys("écart")
            hide = wait.until(lambda current: next((element for element in _workspace_elements(current, '[role="treeitem"][draggable="true"] [aria-label="Hide Column"]')
                if element.is_displayed()), None))
            # Native Chrome element clicking does not reliably scroll an open
            # shadow-root control through Streamlit's outer page scroller.
            driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'})", hide)
            hide.click()
            wait.until(lambda current: variance not in _selection_column_order(current))
            _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
            columns = sorted([units, sales], key=_selection_column_order(driver).index)
            observation["hiddenColumnId"] = variance
            observation["actions"].append("native Columns sidebar search and Hide Column click for Q1 variance between endpoints")
        rows = wait.until(lambda current: (
            candidate if len(candidate := _selection_rows(current)) == 2
            and {row["label"] for row in candidate} == {row["label"] for row in observation["initialRows"]} else False))
        row_ids = [row["id"] for row in rows]
        observation["selectedRows"] = rows
        if requirement == "copy-selection.pinned-columns":
            # Pin a later leaf without reordering the core columns. Its displayed
            # neighbour is Q1 units, but Q1 sales/variance lie between
            # those endpoints in core order. A core-order rectangle includes
            # those unwanted cells even though the mouse crosses only two leaves.
            later = "Q2_ventes_€_sum"
            core_order = _selection_column_order(driver)
            assert core_order == initial_order, "Pinned qualification must not pre-reorder core columns"
            assert core_order.index(later) > core_order.index(units) + 1
            assert core_order.index(units) < core_order.index(sales) < core_order.index(later)
            header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{later}"]')
            ActionChains(driver).context_click(header).perform()
            ActionChains(driver).move_to_element(_selection_menu(driver, "Pin")).perform()
            _selection_menu(driver, "Pin Left").click()
            wait.until(lambda current: _wait_for_visible_workspace_element(current,
                f'[role="columnheader"][data-header-column-id="{later}"]').get_attribute("data-pinned") == "left")
            # Focusing the later header can reveal it by scrolling the center.
            # Return physically to the start so both boundary cells are exposed.
            scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
            offset = driver.execute_script("return arguments[0].scrollLeft", scroller)
            if offset:
                from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
                ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), -int(offset) - 1, 0).perform()
                wait.until(lambda current: current.execute_script("return arguments[0].scrollLeft", scroller) == 0)
            boundary_order = wait.until(lambda current: _pinned_boundary_order(current, core_order))
            assert boundary_order.index(units) == boundary_order.index(later) + 1, "Pinned and center endpoints are not visually adjacent"
            assert boundary_order != core_order, "Pinning did not distinguish displayed and core order"
            boundary = {"coreColumnIds": core_order, "displayedColumnIds": boundary_order,
                        "columns": [later, units], "clipboardLineSeparator": observation["clipboardLineSeparator"]}
            observation["pinnedBoundarySelection"] = boundary
            observation["actions"].append("physical drag across adjacent pinned Q2 sales and center Q1 units, without core reorder")
            boundary["selectedIds"] = _select_rectangle(driver, row_ids, [later, units])
            assert not any(column == sales for _, column in boundary["selectedIds"]), "Intervening core-order Q1 sales was selected"
            literal_boundary = {"Éclair": ["20.00", "2.00"], "München": ["30.00", "3.00"]}
            boundary["expectedClipboardCells"] = [literal_boundary[row["label"]] for row in rows]
            _copy_selected(driver, _selection_cell(driver, row_ids[0], later),
                           boundary["expectedClipboardCells"], boundary)
            header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{later}"]')
            ActionChains(driver).context_click(header).perform()
            _geometry_pin_submenu(driver, "left")
            _selection_menu(driver, "Unpin").click()
            wait.until(lambda current: _selection_column_order(current) == core_order)
            columns = [units, sales]
            header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{units}"]')
            ActionChains(driver).context_click(header).perform()
            ActionChains(driver).move_to_element(_selection_menu(driver, "Pin")).perform()
            _selection_menu(driver, "Pin Left").click()
            wait.until(lambda current: _wait_for_visible_workspace_element(current,
                f'[role="columnheader"][data-header-column-id="{units}"]').get_attribute("data-pinned") == "left")
            observation["pinnedHeader"] = {"column": units, "side": "left"}
            scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
            def left(column):
                return driver.execute_script("return arguments[0].getBoundingClientRect().left", _selection_cell(driver, row_ids[0], column))
            pinned_before, center_before = left(units), left(sales)
            scroll_before = driver.execute_script("return arguments[0].scrollLeft", scroller)
            # A native wheel action makes horizontal scrolling part of the real interaction.
            from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), 24, 0).perform()
            wait.until(lambda current: current.execute_script("return arguments[0].scrollLeft", scroller) > scroll_before)
            assert abs(left(units) - pinned_before) <= 1, "Pinned units moved with horizontal scroll"
            assert left(sales) < center_before - 1, "Unpinned sales did not scroll"
            observation["scrollLeft"] = driver.execute_script("return arguments[0].scrollLeft", scroller)
            observation["pinGeometry"] = {"pinnedBefore": pinned_before, "pinnedAfter": left(units),
                                          "centerBefore": center_before, "centerAfter": left(sales)}
            result["horizontalScrollVerified"] = True
        order = wait.until(lambda current: (
            current_order if all(column in current_order for column in columns)
            else False) if (current_order := _selection_column_order(current)) else False)
        observation["displayedColumnIds"] = order
        assert order.index(columns[0]) < order.index(columns[1]), "Displayed columns are in the wrong order"
        observation["selectedIds"] = _select_rectangle(driver, row_ids, columns)
        if requirement == "copy-selection.pinned-columns":
            observation["scrollLeftAfterSelection"] = driver.execute_script("return arguments[0].scrollLeft", scroller)
            assert abs(observation["scrollLeftAfterSelection"] - observation["scrollLeft"]) <= 1, "Selecting the scrolled range reset horizontal scroll"
        result.update(logicalCellsVerified=True, selectedCellCount=4)
        if requirement == "copy-selection.logical-cells":
            expected = [["100.25", "2"], ["49.75", "3"]]
        elif requirement == "copy-selection.display-order":
            expected = _product_display_order_copy_oracle(rows)
        elif requirement == "copy-selection.pinned-columns":
            literal = {"Éclair": ["2.00", "100.25"], "München": ["3.00", "49.75"]}
            expected = [literal[row["label"]] for row in rows]
        elif requirement in ("copy-selection.formatted-values", "copy-selection.hidden-columns"):
            expected = formatted_selection_oracle(rows, columns)
            for row, values in zip(rows, expected):
                for column, value in zip(columns, values):
                    assert _selection_cell(driver, row["id"], column).text.strip() == value, (
                        "Selected displayed value does not match the two-decimal fixture oracle")
            observation["displayedSelectedCells"] = expected
        else:
            literal = {"Éclair": {sales: "100", units: "2"}, "München": {sales: "50", units: "3"}}
            expected = [[literal[row["label"]][column] for column in columns] for row in rows]
        observation["expectedClipboardCells"] = expected
        menu_label = "Copy Selection (Raw Values)" if requirement == "copy-selection.logical-cells" else "Copy Selection"
        _copy_selected(driver, _selection_cell(driver, row_ids[0], columns[0]), expected, observation,
                       menu_label=menu_label)
        if requirement == "keyboard-copy.selection":
            from tests.test_flagship_certification import _clipboard_text
            menu_clipboard = observation["clipboardText"]
            sentinel = "qualification keyboard copy checkpoint"
            assert driver.execute_async_script(
                "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
                sentinel)
            write_checkpoint = driver.execute_script("return window.__pivotClipboardBoundary.length")
            _selection_keyboard_copy(driver, observation)
            copied = WebDriverWait(driver, SELECTION_COPY_TIMEOUT).until(lambda current: (
                value if (value := _clipboard_text(current)) != sentinel else False))
            observation["nativePositiveClipboard"] = copied
            assert copied == menu_clipboard, "Keyboard copy differs from Copy Selection"
            assert_selected_clipboard(copied, expected, line_separator=observation["clipboardLineSeparator"])
            observation["shortcutWriteMonitor"] = monitor_single_clipboard_write(
                driver, write_checkpoint, seconds=SELECTION_COPY_TIMEOUT)
            result["singleWritePerKeypressVerified"] = True
            result["nativeKeyboardCopyVerified"] = True
        if requirement in ("copy-selection.headers", "copy-selection.hidden-columns"):
            selected_before_headers = set(observation["selectedIds"])
            assert set(_selection_ids(driver)) == selected_before_headers, "Body copy changed the selected rectangle"
            observation["withoutHeaders"] = {
                "menuAction": observation["copyMenuAction"],
                "clipboardText": observation["clipboardText"],
                "parsedTSV": observation["parsedTSV"],
            }
            with_headers = [selected_headers_oracle(columns), *expected]
            observation["expectedWithHeaders"] = with_headers
            _copy_selected(driver, _selection_cell(driver, row_ids[0], columns[0]),
                           with_headers, observation, menu_label="Copy Selection with Headers")
            assert set(_selection_ids(driver)) == selected_before_headers, "Header copy changed the selected rectangle"
            observation["withHeaders"] = {
                "menuAction": observation["copyMenuAction"],
                "clipboardText": observation["clipboardText"],
                "parsedTSV": observation["parsedTSV"],
            }
            result["headerOptInVerified"] = True
            if requirement == "copy-selection.hidden-columns":
                assert variance not in _selection_column_order(driver)
                assert all(column != variance for _, column in _selection_ids(driver))
                assert all(len(row) == 2 for row in observation["parsedTSV"])
                result["hiddenColumnExcluded"] = True
        if requirement == "copy-selection.pinned-columns":
            observation["scrollLeftAfterCopy"] = driver.execute_script("return arguments[0].scrollLeft", scroller)
            assert abs(observation["scrollLeftAfterCopy"] - observation["scrollLeft"]) <= 1, "Copying the scrolled range reset horizontal scroll"
        result["clipboardBytesVerified"] = True
        if requirement == "cell-selection.range-clear":
            checkpoint = "clipboard before native copy"
            assert driver.execute_async_script("const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));", checkpoint)
            _selection_keyboard_copy(driver, observation)
            positive = WebDriverWait(driver, SELECTION_COPY_TIMEOUT).until(lambda current: (
                value if (value := _clipboard_text(current)) != checkpoint else False))
            observation["nativePositiveClipboard"] = positive
            assert_selected_clipboard(positive, expected, line_separator=observation["clipboardLineSeparator"])
            result["nativeKeyboardCopyVerified"] = True
            _wait_for_visible_workspace_element(driver, '[data-pivot-status-action="clear-selection"]').send_keys(Keys.SPACE)
            wait.until(lambda current: not _selection_ids(current))
            result["rangeCleared"] = True
            # A native copy after clearing must not replay the previous range.
            sentinel = "clipboard after clear"
            assert driver.execute_async_script("const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));", sentinel)
            _selection_keyboard_copy(driver, observation)
            observation["clearMonitor"] = {"requestedSeconds": SELECTION_COPY_TIMEOUT, "startedAt": time.monotonic()}
            observation["clearMonitor"].update(assert_clipboard_stable(
                lambda: _clipboard_text(driver), sentinel, timeout=SELECTION_COPY_TIMEOUT))
            observation["clipboardAfterClear"] = _clipboard_text(driver)
            assert observation["clipboardAfterClear"] == sentinel, "Cleared selection was reused by copy"
            result["clearedCopyAbsent"] = True
            result["clearedClipboardMonitored"] = True
        result.update(result="passed", displayOrderVerified=True,
                      pinnedNoDuplicates=requirement == "copy-selection.pinned-columns")
    except Exception as error:
        observation["failureTraceback"] = traceback.format_exc()
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                observation["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                observation["finalSelectedIds"] = _selection_ids(driver)
                observation["finalVisibleRows"] = [cell.text for cell in _workspace_elements(driver, '[data-colid="hierarchy"]') if cell.is_displayed()]
                observation["finalDisplayedColumnIds"] = _selection_column_order(driver)
                screenshot = directory / "final.png"
                driver.save_screenshot(str(screenshot))
                paths.append(str(screenshot))
                observation["runtimeTrace"] = driver.execute_script("return window.__pivotRecoveryProbe || []")
                observation["keyboardMoveTrace"] = driver.execute_script("return window.__pivotKeyboardMoveTrace || []")
                observation["layoutStateTrace"] = driver.execute_script("return window.__pivotLayoutStateTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, result=result)
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {diagnostic}; evidence: {directory}"


_TABLE_COPY_PROBE = r"""
if (!window.__pivotTableCopyTrace) {
  const trace = window.__pivotTableCopyTrace = [];
  const visit = (raw, phase) => {
    let value = raw;
    if (typeof value === 'string') { try { value = JSON.parse(value); } catch (_) { return; } }
    if (!value || typeof value !== 'object') return;
    if (value.schemaVersion === 1 && value.kind === 'export' && value.requestId) {
      trace.push({phase, requestId:value.requestId, status:value.status || null,
        format:value.payload?.format || null, allRows:value.payload?.allRows ?? null,
        includeHeaders:value.payload?.includeHeaders ?? null,
        colIds:value.payload?.colIds || null, rows:value.payload?.rows ?? null,
        hasPayloadRef:Boolean(value.payload?.payloadRef)});
      return;
    }
    if (Array.isArray(value)) value.forEach(item => visit(item, phase));
    else Object.values(value).forEach(item => visit(item, phase));
  };
  const fetch = window.fetch.bind(window);
  window.fetch = (...args) => {
    if (args[1]?.body) visit(args[1].body, 'request');
    return fetch(...args).then(response => {
      response.clone().text().then(body => visit(body, 'response')).catch(() => {});
      return response;
    });
  };
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function(body) {
    visit(body, 'request');
    this.addEventListener('load', () => visit(this.responseText, 'response'));
    return send.call(this, body);
  };
}
"""


def qualify_table_scope_clipboard(host: str, requirement: str) -> None:
    """Copy collapsed display or full hierarchy through an installed host."""
    import pytest
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _clipboard_text, _host_url, _origin, _qualification_chrome_options,
        _switch_to_workspace, _verify_release_asset_batch, _workspace_elements,
    )

    assert requirement in {"copy-current-table.visible-scope", "copy-all-records.complete-scope"}
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    driver = None
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "actions": []}
    diagnostic, paths = "", []
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _TABLE_COPY_PROBE})
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": _origin(_host_url(host)), "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 15)
        visible_rows = wait.until(lambda current: (
            rows if len(rows := _selection_rows(current)) == 2
            and {row["label"] for row in rows} == {"\u00c9clair", "M\u00fcnchen"} else False))
        assert not any(cell.is_displayed() and cell.text.strip() in {"\u6771\u4eac", "\u0627\u0644\u0642\u0627\u0647\u0631\u0629"}
                       for cell in _workspace_elements(driver, '[role="gridcell"][data-colid="hierarchy"]'))
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        column_ids = _selection_column_order(driver)
        observation["visibleRowsBeforeCopy"] = visible_rows
        observation["displayedColumnIds"] = column_ids
        sentinel = "qualification table copy checkpoint"
        assert driver.execute_async_script(
            "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
            sentinel)
        first_cell = _selection_cell(driver, visible_rows[0]["id"], "Q1_ventes_\u20ac_sum")
        ActionChains(driver).context_click(first_cell).perform()
        action = "Copy Current Table" if requirement == "copy-current-table.visible-scope" else "Copy All Records"
        item = WebDriverWait(driver, 8).until(lambda current: next((
            candidate for candidate in _workspace_elements(current, '[role="menuitem"]')
            if candidate.is_displayed() and candidate.is_enabled() and candidate.text.strip() == action
        ), False))
        item.click()
        observation["actions"].append(action)
        copied = WebDriverWait(driver, 20).until(lambda current: (
            value if (value := _clipboard_text(current)) != sentinel else False))
        parsed = list(csv.reader(io.StringIO(copied), delimiter="\t"))
        observation["clipboardText"] = copied
        observation["parsedTSV"] = parsed
        assert parsed and len(parsed[0]) == len(column_ids), "Clipboard headers do not match displayed columns"
        assert any("Region" in cell for cell in parsed[0]), "Hierarchy header is missing"
        labels = [row[0].strip() for row in parsed[1:]]
        expected_labels = {"\u00c9clair", "M\u00fcnchen", "Grand Total"}
        if requirement == "copy-all-records.complete-scope":
            expected_labels |= {"\u6771\u4eac", "\u0627\u0644\u0642\u0627\u0647\u0631\u0629"}
        assert set(labels) == expected_labels and len(labels) == len(expected_labels), (
            "Clipboard hierarchy differs from the fixture scope")
        assert "S\u00e3o Paulo" not in copied and "\u5317\u4eac" not in copied
        if requirement == "copy-current-table.visible-scope":
            assert "\u6771\u4eac" not in copied and "\u0627\u0644\u0642\u0627\u0647\u0631\u0629" not in copied
        trace = driver.execute_script("return window.__pivotTableCopyTrace || []")
        requests = [entry for entry in trace if entry["phase"] == "request" and entry["format"] == "html"]
        assert len(requests) == 1, "Table copy did not issue exactly one HTML export request"
        request = requests[0]
        assert request["allRows"] is (requirement == "copy-all-records.complete-scope")
        assert request["includeHeaders"] is True
        assert request["colIds"] == column_ids, "Server copy request differs from displayed column order"
        assert any(entry["phase"] == "response" and entry["requestId"] == request["requestId"]
                   and entry["status"] == "ok" and entry["hasPayloadRef"] for entry in trace)
        observation["runtimeTrace"] = trace
        result.update(result="passed", actionCompleted=True, clipboardBytesVerified=True,
                      clipboardOrderVerified=True, fixtureTotalsVerified=True, v1RequestsCorrelated=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                observation["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                observation["finalVisibleRows"] = [cell.text for cell in _workspace_elements(driver, '[data-colid="hierarchy"]') if cell.is_displayed()]
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
            finally:
                driver.quit()
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps({"diagnostic": diagnostic, **observation}, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {diagnostic}; evidence: {directory}"


def _native_copy_input_target(driver, selector):
    """Return an input a native pointer can reach, never a covered DOM match."""
    from selenium.common.exceptions import StaleElementReferenceException
    from tests.test_flagship_certification import _workspace_elements

    for candidate in _workspace_elements(driver, selector):
        try:
            if not candidate.is_displayed() or not candidate.is_enabled():
                continue
            if driver.execute_script("""
                const input = arguments[0], rect = input.getBoundingClientRect();
                const left = Math.max(0, rect.left), right = Math.min(innerWidth, rect.right);
                const top = Math.max(0, rect.top), bottom = Math.min(innerHeight, rect.bottom);
                if (left >= right || top >= bottom) return false;
                const x = (left + right) / 2, y = (top + bottom) / 2;
                let hit = document.elementFromPoint(x, y);
                while (hit && hit.shadowRoot) {
                    const nested = hit.shadowRoot.elementFromPoint(x, y);
                    if (!nested || nested === hit) break;
                    hit = nested;
                }
                return hit === input;
            """, candidate):
                return candidate
        except StaleElementReferenceException:
            continue
    return False


def qualify_native_input_copy(host: str) -> None:
    """A focused input owns Ctrl+C even after a grid rectangle was selected."""
    from tests.flagship_clipboard_observer import CLIPBOARD_BOUNDARY_OBSERVER
    import pytest
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    from selenium import webdriver
    from selenium.common.exceptions import StaleElementReferenceException
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _clipboard_text, _host_url, _origin, _qualification_chrome_options,
        _switch_to_workspace, _verify_release_asset_batch,
        _workspace_elements,
    )

    requirement = "keyboard-copy.native-input"
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    driver = None
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "controls": []}
    diagnostic, paths = "", []
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": CLIPBOARD_BOUNDARY_OBSERVER})
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": _origin(_host_url(host)), "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 12)
        columns = ["Q1_ventes_\u20ac_sum", "Q1_unit\u00e9s_sum"]
        def select_ready_rectangle(current):
            rows = _selection_rows(current)
            if len(rows) != 2:
                return False
            selected = _select_rectangle(current, [row["id"] for row in rows], columns)
            return selected if len(selected) == 4 else False
        selected = WebDriverWait(driver, 12, ignored_exceptions=(StaleElementReferenceException,)).until(
            select_ready_rectangle)
        assert len(selected) == 4
        observation["selectedGridCells"] = selected

        def copy_focused_input(element, expected, label):
            element.click()
            element.send_keys(Keys.CONTROL + "a" + Keys.NULL)
            selected_text = driver.execute_script("""
              const input=arguments[0];
              if (input.getRootNode().activeElement!==input) return null;
              // Chromium's number input does not expose selectionStart/End.
              return input.type==='number' && input.selectionStart===null
                ? input.value : input.value.slice(input.selectionStart,input.selectionEnd);
            """, element)
            observation["controls"].append({"control": label, "expected": expected,
                                            "selectedTextBeforeCopy": selected_text,
                                            "value": element.get_attribute("value"),
                                            "activeTag": driver.execute_script(
                                                "return arguments[0].getRootNode().activeElement?.tagName", element)})
            assert selected_text == expected, f"{label} does not own the selected text"
            sentinel = f"qualification native input {label}"
            assert driver.execute_async_script(
                "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
                sentinel)
            write_checkpoint = driver.execute_script("return window.__pivotClipboardBoundary.length")
            ActionChains(driver).key_down(Keys.CONTROL).send_keys("c").key_up(Keys.CONTROL).perform()
            try:
                copied = WebDriverWait(driver, 8).until(lambda current:
                    value if (value := _clipboard_text(current)) != sentinel else False)
            except Exception:
                observation["controls"][-1]["clipboardAfterTimeout"] = _clipboard_text(driver)
                raise
            observation["controls"][-1]["clipboardText"] = copied
            assert copied == expected, f"Ctrl+C in {label} copied grid contents or changed text"
            observation["controls"][-1]["nativeCopyMonitor"] = assert_clipboard_stable(
                lambda: _clipboard_text(driver), expected, timeout=SELECTION_COPY_TIMEOUT)
            writes = driver.execute_script("return window.__pivotClipboardBoundary.slice(arguments[0])", write_checkpoint)
            observation["controls"][-1]["productClipboardWrites"] = writes
            assert writes == [], f"Grid clipboard handler intercepted native copy in {label}"
            observation["controls"][-1]["focused"] = True

        for attempt in range(3):
            search = wait.until(lambda current: _native_copy_input_target(current, '[data-testid="pivot-global-search"]'))
            try:
                if search.get_attribute("value") != "M\u00fcnchen":
                    search.click()
                    search.send_keys("M\u00fcnchen")
                break
            except StaleElementReferenceException:
                if attempt == 2:
                    raise
        wait.until(lambda current: [row["label"] for row in _selection_rows(current)] == ["M\u00fcnchen"])
        search = wait.until(lambda current: _native_copy_input_target(current, '[data-testid="pivot-global-search"]'))
        copy_focused_input(search, "M\u00fcnchen", "global search")
        filter_button = wait.until(lambda current: next((button for button in _workspace_elements(current,
            '[role="columnheader"][data-header-column-id="hierarchy"] [aria-label="Filter Column"]')
            if button.is_displayed()), False))
        filter_button.click()
        filter_selector = 'input[placeholder="Search values..."], input[placeholder="Search loaded values..."]'
        filter_input = wait.until(lambda current: _native_copy_input_target(current, filter_selector))
        filter_input.click()
        filter_input.send_keys("M\u00fcnchen")
        wait.until(lambda current: filter_input.get_attribute("value") == "M\u00fcnchen")
        copy_focused_input(filter_input, "M\u00fcnchen", "filter search")
        result["filterInputNativeCopyVerified"] = True
        close_filter = wait.until(lambda current: next((button for button in _workspace_elements(current, "button")
            if button.is_displayed() and button.text.strip() == "Clear & Close"), False))
        close_filter.click()
        dialog = _open_selection_number_format(driver, observation)
        number = wait.until(lambda current: _native_copy_input_target(current, dialog + ' input[type="number"]'))
        number.click()
        number.send_keys(Keys.CONTROL + "a" + Keys.NULL)
        number.send_keys("4")
        wait.until(lambda current: number.get_attribute("value") == "4")
        copy_focused_input(number, "4", "format dialog number")
        result.update(result="passed", clipboardBytesVerified=True, nativeKeyboardCopyVerified=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                observation["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                screenshot = directory / "final.png"
                driver.save_screenshot(str(screenshot))
                paths.append(str(screenshot))
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, result=result)
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {diagnostic}; evidence: {directory}"


def _geometry_header(driver, column):
    from tests.test_flagship_certification import _workspace_elements

    candidates = _workspace_elements(driver, f'[role="columnheader"][data-header-column-id="{column}"]')
    return next((header for header in candidates if header.is_displayed() and
                 header.find_elements("css selector", 'div[style*="cursor: col-resize"]')), None)


def _geometry_snapshot(driver, column, *, center="Q1_unités_sum"):
    from tests.test_flagship_certification import _workspace_elements

    header = _geometry_header(driver, column)
    assert header is not None, f"Visible resizable header {column} absent"
    rows = _selection_rows(driver)
    assert len(rows) == 2 and {row["label"] for row in rows} == {"Éclair", "München"}
    ids = [row["id"] for row in rows]
    target_cells = [cell for cell in _workspace_elements(driver, f'[role="gridcell"][data-colid="{column}"]')
                    if cell.is_displayed() and cell.get_attribute("data-rowid") in ids]
    assert len(target_cells) == 2, "Target logical cells are missing or duplicated"
    cells = sorted((cell.get_attribute("data-rowid"), cell.get_attribute("data-colid"), cell.text.strip())
                   for cell in target_cells)
    assert all(value for _, _, value in cells), "Visible target value missing"
    neighbor_cells = [cell for cell in _workspace_elements(driver, '[role="gridcell"][data-colid="Q1_unités_sum"]')
                      if cell.is_displayed() and cell.get_attribute("data-rowid") in ids]
    assert len(neighbor_cells) == 2, "Neighbor logical cells are missing or duplicated"
    neighbors = sorted((cell.get_attribute("data-rowid"), cell.get_attribute("data-colid"), cell.text.strip())
                       for cell in neighbor_cells)
    scroller = next((el for el in _workspace_elements(driver, '[data-pivot-scroll-container]') if el.is_displayed()), None)
    assert scroller is not None, "Grid scroll container absent"
    rect = lambda element: driver.execute_script("return arguments[0].getBoundingClientRect().toJSON()", element)
    center_cell = _selection_cell(driver, ids[0], center)
    fits = driver.execute_script("""
      const content=arguments[0].querySelector('[data-header-content="true"]');
      return content.scrollWidth <= content.clientWidth+2
        && arguments[1].every(cell => cell.scrollWidth <= cell.clientWidth+2);
    """, header, target_cells)
    return {"width": rect(header)["width"], "order": _selection_column_order(driver),
            "cells": cells, "neighbors": neighbors, "fits": fits,
            "pinnedLeft": [column] if header.get_attribute("data-pinned") == "left" else [],
            "pinnedX": rect(target_cells[0])["left"], "centerX": rect(center_cell)["left"],
            "scrollLeft": driver.execute_script("return arguments[0].scrollLeft", scroller)}


def _geometry_required_width(driver, column):
    """Read natural rendered text and computed chrome without altering the DOM."""
    from tests.test_flagship_certification import _workspace_elements

    header = _geometry_header(driver, column)
    assert header is not None
    labels = header.find_elements("css selector", '[data-header-text="true"]')
    cells = [cell for cell in _workspace_elements(driver, f'[role="gridcell"][data-colid="{column}"]')
             if cell.is_displayed()]
    assert labels and cells
    return driver.execute_script("""
      const chrome = (style) => ['paddingLeft','paddingRight','borderLeftWidth','borderRightWidth']
        .reduce((sum,key) => sum + (parseFloat(style[key]) || 0), 0);
      const naturalText = element => {
        const canvas = document.createElement('canvas'), context = canvas.getContext('2d');
        context.font = getComputedStyle(element).font;
        return context.measureText(element.innerText.trim()).width;
      };
      const header = arguments[0], labels = arguments[1], cells = arguments[2];
      const content = header.querySelector('[data-header-content="true"]');
      const label = labels[0];
      const controls = [...content.children].filter(child => child !== label)
        .reduce((sum,child) => {
          const style = getComputedStyle(child);
          return sum + child.getBoundingClientRect().width + (parseFloat(style.marginLeft)||0)
            + (parseFloat(style.marginRight)||0);
        }, 0);
      const headerNeed = naturalText(label) + controls + chrome(getComputedStyle(header))
        + chrome(getComputedStyle(content)) + chrome(getComputedStyle(label));
      const clone = content.cloneNode(true);
      Object.assign(clone.style, {position:'absolute',left:'-100000px',top:'0',visibility:'hidden',
        pointerEvents:'none',width:'max-content',minWidth:'0',maxWidth:'none',overflow:'visible',
        display:'inline-flex',flex:'0 0 auto'});
      for (const text of clone.querySelectorAll('[data-header-text="true"]'))
        Object.assign(text.style, {flex:'0 0 auto',width:'auto',minWidth:'0',maxWidth:'none',
          overflow:'visible',textOverflow:'clip',display:'inline-block'});
      header.appendChild(clone);
      const cloneWidth = Math.ceil(clone.scrollWidth || clone.getBoundingClientRect().width || 0);
      header.removeChild(clone);
      const cellNeed = Math.max(...cells.map(cell => naturalText(cell) + chrome(getComputedStyle(cell))));
      return {requiredWidth: Math.ceil(Math.max(headerNeed, cellNeed)),
        headerTextWidth: naturalText(label), headerControlsWidth: controls,
        headerChrome:chrome(getComputedStyle(header)), contentChrome:chrome(getComputedStyle(content)),
        labelChrome:chrome(getComputedStyle(label)), cloneWidth,
        cellTextWidths: cells.map(naturalText),
        renderedOverflow: content.scrollWidth > content.clientWidth + 2
          || cells.some(cell => cell.scrollWidth > cell.clientWidth + 2)};
    """, header, labels, cells)


def _geometry_at_end(driver, column, last_column):
    from tests.test_flagship_certification import _workspace_elements

    scroller = next(el for el in _workspace_elements(driver, '[data-pivot-scroll-container]') if el.is_displayed())
    target = next(cell for cell in _workspace_elements(driver, f'[role="gridcell"][data-colid="{column}"]')
                  if cell.is_displayed() and cell.get_attribute("data-rowid") == "Éclair")
    last = next(cell for cell in _workspace_elements(driver, f'[role="gridcell"][data-colid="{last_column}"]')
                if cell.is_displayed() and cell.get_attribute("data-rowid") == "Éclair")
    return driver.execute_script("""
      const [scroller,target,last] = arguments;
      const viewport=scroller.getBoundingClientRect(), pinned=target.getBoundingClientRect(), end=last.getBoundingClientRect();
      return {scrollLeft:scroller.scrollLeft,maxScrollLeft:scroller.scrollWidth-scroller.clientWidth,
        pinnedX:pinned.left,lastColumnVisible:end.right<=viewport.right+2 && end.right>viewport.left
          && end.left<viewport.right};
    """, scroller, target, last)


def _geometry_pin_submenu(driver, pinned):
    from selenium.webdriver.common.action_chains import ActionChains

    title = f"Pin ({pinned})" if pinned else "Pin"
    ActionChains(driver).move_to_element(_selection_menu(driver, title)).perform()


def _geometry_copy_current_table(driver):
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _workspace_elements

    item = WebDriverWait(driver, 8).until(lambda current: next((
        item for item in _workspace_elements(current, '[role="menuitem"]')
        if item.is_displayed() and item.is_enabled() and item.text.strip() == "Copy Current Table"
    ), False))
    item.click()


def classify_visible_startup_error(driver, result):
    """A rendered product error is observable even if no grid cells mounted."""
    if _visible_error(driver):
        result.update(result="failed", visibleError=True, infrastructureUnavailable=False,
                      browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}")


def qualify_column_geometry(host: str, requirement: str) -> None:
    """Use physical resize, double-click and pin/menu gestures on installed hosts."""
    import pytest
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _host_url, _origin, _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch,
    )
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
    from selenium.webdriver.support.ui import WebDriverWait

    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "states": [], "actions": []}
    diagnostic, paths, driver = "", [], None
    sales = "Q2_ventes_€_sum" if requirement == "column-pinning.round-trip" else "Q1_ventes_€_sum"
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        if requirement == "column-pinning.round-trip":
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
              window.__pivotGeometryExports = [];
              const captureExport = raw => {
                try {
                  const value = typeof raw === 'string' ? JSON.parse(raw) : raw;
                  const visit = item => {
                    if (!item || typeof item !== 'object') return;
                    if (item.kind === 'export' && item.payload)
                      window.__pivotGeometryExports.push({colIds: item.payload.colIds || null});
                    else Object.values(item).forEach(visit);
                  };
                  visit(value);
                } catch (_) {}
              };
              const originalFetch = window.fetch.bind(window);
              window.fetch = (...args) => {
                if (args[1]?.body) captureExport(args[1].body);
                return originalFetch(...args);
              };
              const originalSend = XMLHttpRequest.prototype.send;
              XMLHttpRequest.prototype.send = function(body) {
                captureExport(body);
                return originalSend.call(this, body);
              };
            """})
            driver.execute_cdp_cmd("Browser.grantPermissions", {
                "origin": _origin(_host_url(host)),
                "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 12)
        before = _geometry_snapshot(driver, sales)
        if requirement == "column-sizing.auto-size":
            before.update(_geometry_required_width(driver, sales))
        observation["states"].append({"phase": "before", **before})
        assert "hierarchy" in before["order"] and sales in before["order"]
        driver.save_screenshot(str(directory / "before.png"))
        paths.append(str(directory / "before.png"))
        header = _geometry_header(driver, sales)
        handle = header.find_element("css selector", 'div[style*="cursor: col-resize"]')
        if requirement == "column-sizing.resize":
            assert 60 + 42 < before["width"] < 600 - 42, "Fixture width not safely inside resize bounds"
            ActionChains(driver).move_to_element(handle).click_and_hold().move_by_offset(40, 0).release().perform()
            observation["actions"].append("trusted mouse drag of visible header resize handle +40 CSS px")
            wait.until(lambda current: abs(_geometry_snapshot(current, sales)["width"] - before["width"]) >= 38)
            after = _geometry_snapshot(driver, sales)
            assert_resize_geometry(before, after)
            observation["states"].append({"phase": "resized", **after})
            scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), 40, 0).perform()
            wait.until(lambda current: current.execute_script("return arguments[0].scrollLeft", scroller) > 0)
            scrolled = _geometry_snapshot(driver, sales)
            observation["states"].append({"phase": "scrolled", **scrolled})
            assert scrolled["cells"] == before["cells"] and scrolled["width"] == after["width"]
            assert len(set(scrolled["order"])) == len(scrolled["order"])
            result.update(resizeDeltaVerified=True, geometryScrollVerified=True, logicalValuesPreserved=True)
        elif requirement == "column-sizing.auto-size":
            ActionChains(driver).double_click(handle).perform()
            observation["actions"].append("trusted double-click of visible header resize handle")
            wait.until(lambda current: abs(_geometry_snapshot(current, sales)["width"] - before["width"]) > 2)
            first = _geometry_snapshot(driver, sales)
            observation["states"].append({"phase": "auto-sized", **first})
            handle = _geometry_header(driver, sales).find_element("css selector", 'div[style*="cursor: col-resize"]')
            ActionChains(driver).double_click(handle).perform()
            observation["actions"].append("repeat trusted double-click on the same column")
            second = _geometry_snapshot(driver, sales)
            observation["states"].append({"phase": "repeated", **second})
            assert_auto_size_geometry(before, first, second, min_width=60, max_width=600)
            assert first["cells"] == second["cells"] == before["cells"]
            result.update(autoSizeFitVerified=True, autoSizeIdempotent=True, logicalValuesPreserved=True)
        elif requirement == "column-pinning.round-trip":
            from selenium.common.exceptions import StaleElementReferenceException
            geometry_wait = WebDriverWait(driver, 12, ignored_exceptions=(AssertionError, StaleElementReferenceException))
            ActionChains(driver).context_click(header).perform()
            _geometry_pin_submenu(driver, None)
            _selection_menu(driver, "Pin Left").click()
            observation["actions"].append("context menu Pin > Pin Left on Q2 sales")
            geometry_wait.until(lambda current: _geometry_snapshot(current, sales)["pinnedLeft"] == [sales])
            pinned = _geometry_snapshot(driver, sales)
            observation["states"].append({"phase": "pinned", **pinned})
            driver.save_screenshot(str(directory / "pinned.png"))
            paths.append(str(directory / "pinned.png"))
            scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), 40, 0).perform()
            wait.until(lambda current: current.execute_script("return arguments[0].scrollLeft", scroller) > pinned["scrollLeft"])
            scrolled = _geometry_snapshot(driver, sales)
            observation["states"].append({"phase": "pinned-scrolled", **scrolled})
            last_column = before["order"][-1]
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), 10000, 0).perform()
            wait.until(lambda current: (
                (end := _geometry_at_end(current, sales, last_column))["maxScrollLeft"] > scrolled["scrollLeft"]
                and abs(end["scrollLeft"] - end["maxScrollLeft"]) <= 2 and end["lastColumnVisible"]))
            end = {**_geometry_snapshot(driver, sales), **_geometry_at_end(driver, sales, last_column)}
            observation["states"].append({"phase": "last-column", **end})
            driver.save_screenshot(str(directory / "last-column.png"))
            paths.append(str(directory / "last-column.png"))
            from tests.test_flagship_certification import _clipboard_text
            sentinel = "geometry clipboard checkpoint"
            assert driver.execute_async_script(
                "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
                sentinel)
            _selection_cell(driver, "Éclair", sales)
            ActionChains(driver).context_click(_selection_cell(driver, "Éclair", sales)).perform()
            _geometry_copy_current_table(driver)
            copied = WebDriverWait(driver, 20).until(lambda current: (
                value if (value := _clipboard_text(current)) != sentinel else False))
            parsed = list(csv.reader(io.StringIO(copied), delimiter="\t"))
            observation["clipboardRows"] = parsed
            observation["exportRequests"] = driver.execute_script("return window.__pivotGeometryExports || []")
            observation["actions"].append("Copy Current Table from pinned cell, parse OS clipboard headers and raw fixture positions")
            assert_pinned_clipboard_order(parsed, pinned["order"])
            ActionChains(driver).context_click(_geometry_header(driver, sales)).perform()
            _geometry_pin_submenu(driver, "left")
            _selection_menu(driver, "Unpin").click()
            observation["actions"].append("context menu Pin > Unpin")
            geometry_wait.until(lambda current: _geometry_snapshot(current, sales)["pinnedLeft"] == [])
            ActionChains(driver).scroll_from_origin(ScrollOrigin.from_element(scroller), -10000, 0).perform()
            observation["actions"].append("trusted horizontal scroll back to the initial position")
            wait.until(lambda current: current.execute_script("return arguments[0].scrollLeft", scroller) <= 2)
            geometry_wait.until(lambda current: _geometry_snapshot(current, sales)["order"] == before["order"])
            restored = _geometry_snapshot(driver, sales)
            observation["states"].append({"phase": "unpinned", **restored})
            assert_pin_round_trip(before, pinned, scrolled, end, restored, sales)
            result.update(pinRoundTripVerified=True, geometryScrollVerified=True,
                          logicalValuesPreserved=True, lastColumnVisible=True, clipboardOrderVerified=True)
        else:
            raise AssertionError(f"Unmapped column geometry requirement {requirement}")
        result["result"] = "passed"
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                screenshot = directory / "final.png"
                classify_visible_startup_error(driver, result)
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
                driver.save_screenshot(str(screenshot))
                paths.append(str(screenshot))
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={k: v for k, v in result.items() if k != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {diagnostic}; evidence: {directory}"


def assert_cancellation_lifetime(samples: Sequence[Mapping[str, object]]) -> None:
    """Reject missing observations and release while the executor is alive.

    The caller must sample the real worker governor and actual executor
    completion; a transport abort or a progress notification is insufficient.
    """
    assert samples, "No executor/admission observations"
    held = [sample for sample in samples if sample.get("executorRunning") is True]
    assert held, "No running executor observation"
    cancelled = [sample for sample in held if sample.get("cancelRequested") is True]
    assert cancelled, "No running executor observation after cancellation"
    initial = held[0]
    reservation, slots = initial.get("reservedBytes", 0), initial.get("activeSlots", 0)
    assert reservation > 0 and slots > 0, "Running executor has no admission reservation/slot"
    for sample in held:
        assert sample.get("reservedBytes", 0) >= reservation, "Reservation released before executor cleanup"
        assert sample.get("activeSlots", 0) >= slots, "Admission slot released before executor cleanup"
    last = samples[-1]
    assert last.get("executorRunning") is False, "Executor cleanup was not observed"
    assert last.get("cancelRequested") is True, "Cancellation was not observed"
    assert last.get("reservedBytes") == 0 and last.get("activeSlots") == 0, "Admission leaked after executor cleanup"


def record_case(requirement_id: str, host: str, observed: Mapping[str, object], evidence_dir: Path) -> None:
    """Append a hash-bound CaseResult; arbitrary data never enters its summary.

    Private evidence may retain diagnostics. Observed summaries admit only
    known boolean/integer measurements. The release publisher remains the
    authority for public fields and independently validates these bindings.
    """
    from scripts.flagship_manifest import BoundCaseResult

    requirements = {item.id: item for item in load_requirements(ROOT / "tests" / "flagship_requirements.json")}
    if requirement_id not in requirements or host not in HOSTS or host not in requirements[requirement_id].hosts:
        raise ValueError("Unmapped workflow requirement/host")
    result = observed.get("result")
    if result not in RESULTS or result == "not_applicable":
        raise ValueError("Workflow result must be explicit and applicable")
    binding_path = os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING")
    if not binding_path:
        raise ValueError("Installed artifact binding is required for workflow evidence")
    binding = json.loads(Path(binding_path).read_text(encoding="utf-8"))
    contract_hash = binding.get("sha256", {}).get("contract")
    if observed.get("result") == "passed" and (not isinstance(contract_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", contract_hash) is None):
        raise ValueError("Passing workflow requires committed contract hash")
    run_id = os.environ.get("PIVOT_FLAGSHIP_RUN_ID")
    if not run_id or binding.get("runId") != run_id:
        raise ValueError("Workflow evidence run binding does not match")
    destination = Path(os.environ["PIVOT_FLAGSHIP_CASE_RESULTS"])
    evidence_dir = evidence_dir.resolve()
    paths = tuple(str(Path(path).resolve(strict=True)) for path in observed.get("evidence_paths", ()))
    if any(not Path(path).is_relative_to(evidence_dir) for path in paths):
        raise ValueError("Workflow evidence is outside its case directory")
    assets = observed.get("loadedAssetSha256", {})
    browser = observed.get("browser", "unavailable")
    if result == "passed" and (not paths or not assets or browser == "unavailable"):
        raise ValueError("Passing workflow requires browser and loaded asset evidence")
    if result == "passed":
        observations = [json.loads(Path(path).read_text(encoding="utf-8"))
                        for path in paths if Path(path).suffix == ".json"]
        if not any(isinstance(item, dict) and item.get("loadedAssetSha256") == assets
                   for item in observations):
            raise ValueError("Passing workflow requires observation-level loaded asset digests")
    safe = {key: value for key, value in observed.items()
            if key in _SAFE_OBSERVATIONS and type(value) in (bool, int)}
    record = BoundCaseResult(
        requirement_id=requirement_id, system="product", host=host,
        test_id=requirements[requirement_id].browser_tests[host][0], result=result,
        observed=json.dumps(safe, sort_keys=True), browser=str(browser), evidence_paths=paths,
        run_id=run_id, source_commit=binding["sourceCommit"], sha256=binding["sha256"],
        loaded_assets=dict(assets),
        evidence_sha256={path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in paths},
    )
    envelope = {"schema_version": 1, "run_id": run_id,
                "source_commit": binding["sourceCommit"], "sha256": binding["sha256"], "results": []}
    if destination.exists():
        previous = json.loads(destination.read_text(encoding="utf-8"))
        if any(previous.get(key) != envelope[key] for key in envelope if key != "results"):
            raise ValueError("Existing workflow evidence binding does not match")
        envelope = previous
    # Do not overwrite another attempt and erase its failed evidence.
    if any(item["requirement_id"] == requirement_id and item["host"] == host for item in envelope["results"]):
        raise ValueError("Duplicate workflow case; use a fresh run for retries")
    envelope["results"].append(asdict(record))
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(envelope, indent=2) + "\n", encoding="utf-8")


_PROBE_SCRIPT = """
if (!window.__pivotRecoveryProbe) {
  const trace = [];
  const visit = (value, phase) => {
    if (typeof value === 'string') { try { value = JSON.parse(value); } catch (_) { return; } }
    if (!value || typeof value !== 'object') return;
    if (value.schemaVersion === 1 && value.kind && typeof value.requestId === 'string') {
      const grand = value.kind === 'data' && Array.isArray(value.payload?.data)
        ? value.payload.data.find(row => row?._id === 'Grand Total' && row?._isTotal === true) : null;
      trace.push({phase, requestId:value.requestId, kind:value.kind,
                  status:value.status || null, at:performance.now(),
                  layoutRevision:value.payload?.layoutRevision || null,
                  layoutManifest:Array.isArray(value.payload?.layoutManifest)
                    ? value.payload.layoutManifest : null,
                  columnIds:Array.isArray(value.payload?.colSchema?.columns)
                    ? value.payload.colSchema.columns.map(column => column?.id) : null,
                  rawExpectedTotal: phase === 'response' && grand !== null
                    && grand['__RowTotal__ventes_€_sum'] === 200
                    && grand['__RowTotal__unités_sum'] === 10
                    && grand['__RowTotal__écart_sum'] === -6.75});
      return;
    }
    Object.values(value).forEach(child => visit(child, phase));
  };
  const original = window.fetch.bind(window);
  window.fetch = (...args) => {
    if (args[1]?.body) visit(args[1].body, 'request');
    return original(...args).then(response => {
      response.clone().text().then(value => visit(value, 'response')).catch(() => {});
      return response;
    });
  };
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function(body) {
    visit(body, 'request');
    this.addEventListener('load', () => visit(this.responseText, 'response'));
    return send.call(this, body);
  };
  window.__pivotRecoveryProbe = trace;
}
"""


def _network(driver):
    from tests.test_flagship_certification import _verify_release_asset_batch

    entries = driver.get_log("performance")
    _verify_release_asset_batch(driver, entries)
    return [json.loads(entry["message"])["message"] for entry in entries]


_VISIBLE_ERROR_QUERY = r"""
const matches = [];
const visible = element => {
  const r = element.getBoundingClientRect(), style = getComputedStyle(element);
  return element.isConnected && r.width > 0 && r.height > 0 &&
    style.display !== 'none' && style.visibility !== 'hidden' &&
    style.visibility !== 'collapse' &&
    (!element.checkVisibility || element.checkVisibility({checkOpacity:true, checkVisibilityCSS:true}));
};
const visit = root => {
  for (const alert of root.querySelectorAll('[role="alert"]')) {
    if (visible(alert) && alert.innerText.includes('We could not load the grid data') &&
        [...alert.querySelectorAll('button')].some(button => visible(button) && button.innerText === 'Retry')) {
      matches.push(alert);
    }
  }
  for (const element of root.querySelectorAll('*')) if (element.shadowRoot) visit(element.shadowRoot);
};
visit(document);
return matches;
"""


def _visible_error(driver):
    # Alert dismissal may unmount it between remote visibility/text reads.
    return driver.execute_script(_VISIBLE_ERROR_QUERY)


def _totals(driver, *, decimal_places=0):
    from tests.test_flagship_certification import _workspace_elements

    rows = [row for row in _workspace_elements(driver, '[role="row"]') if "Grand Total" in row.text]
    for row in rows:
        # Independent literal oracle: four Included fixture rows total these
        # three measures. Strip display punctuation but retain sign/decimals.
        values = [float(token.replace(",", "")) for token in re.findall(r"-?\d[\d,]*(?:\.\d+)?", row.text)]
        # The default display rounds -6.75 to -7; the hierarchy comparison
        # explicitly selects two decimals and must retain the exact value.
        # The V1 response is checked separately for the exact raw total.
        if decimal_places not in (0, 2):
            raise ValueError(f"Unsupported fixture display precision: {decimal_places}")
        expected_values = (200.0, 10.0, -6.75 if decimal_places == 2 else -7.0)
        if all(expected in values for expected in expected_values):
            return True
    return False


def _click_workspace_control(driver, selector):
    """Bring an off-screen Streamlit shadow control into view before a real click."""
    from tests.test_flagship_certification import _wait_for_visible_workspace_element

    element = _wait_for_visible_workspace_element(driver, selector)
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
    element.click()
    return element


def _post_retry_data_response(trace, failed_id, checkpoint):
    """An earlier auto-refresh cannot masquerade as the clicked Retry."""
    requests = {entry["requestId"]: entry["at"] for entry in trace
                if entry.get("phase") == "request" and entry.get("kind") == "data"
                and entry.get("requestId") != failed_id and entry.get("at", -1) > checkpoint}
    return next((entry["requestId"] for entry in trace
                 if entry.get("phase") == "response" and entry.get("kind") == "data"
                 and entry.get("status") == "data" and entry.get("rawExpectedTotal") is True
                 and entry.get("requestId") in requests
                 and entry.get("at", -1) >= requests[entry["requestId"]]), None)


def assert_superseded_response(trace, old_id, new_id, visible_total):
    """Require two distinct successful deliveries in the reverse request order."""
    assert old_id != new_id
    positions = {}
    for index, event in enumerate(trace):
        key = (event.get("requestId"), event.get("phase"))
        if key[0] in (old_id, new_id) and key not in positions:
            positions[key] = index
    assert all((request_id, phase) in positions for request_id in (old_id, new_id)
               for phase in ("request", "response")), "Both requests need observed responses"
    old_request = positions[(old_id, "request")]
    new_request = positions[(new_id, "request")]
    new_response = positions[(new_id, "response")]
    old_response = positions[(old_id, "response")]
    assert old_request < new_request < new_response < old_response
    totals = {event["requestId"]: event["total"] for event in trace
              if event.get("phase") == "response" and event.get("requestId") in (old_id, new_id)}
    assert totals[old_id] is not None and totals[new_id] is not None
    assert totals[old_id] != totals[new_id]
    assert visible_total == totals[new_id], "A late older response replaced the newer visible total"
    return True


def assert_stable_visible_snapshots(expected, samples):
    """Reject even a transient change to any rendered cell after old delivery."""
    def signature(cells):
        values = [(cell["rowid"], cell["colid"], cell["text"]) for cell in cells]
        assert len(values) == len({(rowid, colid) for rowid, colid, _ in values}), (
            "Duplicate visible cells in observation")
        return sorted(values)

    assert expected and len(samples) >= 2, "Missing bounded visible-cell observations"
    baseline = signature(expected)
    for index, sample in enumerate(samples):
        assert signature(sample) == baseline, f"visible cells changed after old response at sample {index}"
    return True


def supersession_fixture_matches(cells):
    """Independently verify every rendered measure for the newer fixture slice."""
    measures = {
        "Q1_ventes_€_sum": "50", "Q1_unités_sum": "3", "Q1_écart_sum": "4",
        "Q2_ventes_€_sum": "30", "Q2_unités_sum": "4", "Q2_écart_sum": "-1",
        "__RowTotal__ventes_€_sum": "80", "__RowTotal__unités_sum": "7",
        "__RowTotal__écart_sum": "3",
    }
    expected = {
        (rowid, colid): value
        for rowid, label in (("München", "München"), ("__grand_total__", "Grand Total"))
        for colid, value in {"hierarchy": label, **measures}.items()
    }
    actual = {(cell["rowid"], cell["colid"]): cell["text"] for cell in cells}
    return len(cells) == len(actual) and actual == expected


def supersession_ids(trace):
    old_id = next(item["requestId"] for item in trace
                  if item.get("phase") == "ready" and item.get("term") == "Éclair"
                  and item.get("total") == 120.25)
    new_id = next(item["requestId"] for item in trace
                  if item.get("phase") == "response" and item.get("term") == "München"
                  and item.get("total") == 79.75)
    return old_id, new_id


_SUPERSESSION_PROBE_SCRIPT = r"""
if (!window.__pivotSupersession) {
  const nativeFetch = window.fetch.bind(window);
  const trace = [];
  let armed = false, releaseOld = null, responseMode = 'data';
  const alerts = [];
  const findEnvelope = (raw, phase, depth = 0) => {
    if (depth > 8 || raw == null) return null;
    if (typeof raw === 'string') {
      if (!raw.trim().startsWith('{') && !raw.trim().startsWith('[')) return null;
      try { return findEnvelope(JSON.parse(raw), phase, depth + 1); } catch (_) { return null; }
    }
    if (typeof raw !== 'object') return null;
    if (raw.schemaVersion === 1 && raw.kind === 'data' && raw.requestId != null
        && (phase === 'request' ? !raw.status : Boolean(raw.status))) return raw;
    for (const value of Object.values(raw)) {
      const found = findEnvelope(value, phase, depth + 1);
      if (found) return found;
    }
    return null;
  };
  const totalOf = envelope => {
    const rows = envelope?.payload?.data;
    const grand = Array.isArray(rows) ? rows.find(row => row?._isTotal && row?._id === 'Grand Total') : null;
    return grand?.['__RowTotal__ventes_€_sum'] ?? null;
  };
  window.fetch = (...args) => {
    const envelope = armed ? findEnvelope(args[1]?.body, 'request') : null;
    const term = envelope?.state?.filters?.global;
    if (term !== 'Éclair' && term !== 'München') return nativeFetch(...args);
    const requestId = String(envelope.requestId);
    trace.push({phase:'request', requestId, term, at:performance.now(), total:null,
      layoutRevision:envelope.payload?.layoutRevision ?? envelope.payload?.layout_revision ?? null,
      manifestCount:Array.isArray(envelope.payload?.layoutManifest) ? envelope.payload.layoutManifest.length :
        Array.isArray(envelope.payload?.layout_manifest) ? envelope.payload.layout_manifest.length : null,
      needsColSchema:envelope.payload?.needs_col_schema ?? envelope.payload?.needsColSchema ?? null});
    const init = term === 'Éclair' ? {...args[1], signal:undefined} : args[1];
    return nativeFetch(args[0], init).then(async response => {
      const responseText = await response.clone().text();
      const returned = findEnvelope(responseText, 'response');
      let responseJson = null;
      try { responseJson = JSON.parse(responseText); } catch (_) {}
      const total = totalOf(returned);
      const details = {httpStatus:response.status, envelopeStatus:returned?.status || null,
        payloadKeys:Object.keys(returned?.payload || {}).slice(0, 12),
        payloadRef:Boolean(returned?.payload?.payloadRef),
        errorCode:returned?.payload?.code || responseJson?.payload?.code || null,
        errorMessage:typeof returned?.message === 'string' ? returned.message.slice(0, 160) : null,
        httpDetail:typeof responseJson?.detail === 'string' ? responseJson.detail.slice(0, 160) : null};
      if (term === 'Éclair' && total !== null && !releaseOld) {
        trace.push({phase:'ready', requestId, term, at:performance.now(), total, ...details});
        await new Promise(resolve => { releaseOld = resolve; });
      }
      let delivered = response;
      if (responseMode === 'busy' && term === '\u00c9clair') {
        // Model an older V1 peer whose optional freshness metadata is absent.
        // The source query is real; only this deliberately late error is injected.
        delivered = new Response(JSON.stringify({schemaVersion:1, kind:'data',
          requestId, status:'error', payload:{code:'resource_exhausted',
            resourceKind:'query', retryAfterMs:1000}}), {
          status:503, headers:{'Content-Type':'application/json'},
        });
        Object.assign(details, {httpStatus:503, envelopeStatus:'error',
          errorCode:'resource_exhausted', simulatedOlderPeer:true});
      }
      trace.push({phase:'response', requestId, term, at:performance.now(), total, ...details});
      return delivered;
    });
  };
  window.__pivotSupersession = {trace, alerts, arm:(mode='data') => {
    armed = true; responseMode = mode; releaseOld = null; trace.length = 0; alerts.length = 0;
  }, release:() => releaseOld?.()};
}
"""


_SUPERSESSION_OBSERVE_SCRIPT = r"""
const done = arguments[arguments.length - 1];
const capture = () => {
  const cells = [];
  const visit = root => {
    for (const alert of root.querySelectorAll('[role="alert"]')) {
      if (alert.getClientRects().length) window.__pivotSupersession.alerts.push(alert.innerText);
    }
    for (const cell of root.querySelectorAll('[role="gridcell"][data-colid][data-rowid]')) {
      if (cell.getClientRects().length) cells.push({
        rowid: cell.getAttribute('data-rowid'),
        colid: cell.getAttribute('data-colid'),
        text: cell.innerText.trim(),
      });
    }
    for (const element of root.querySelectorAll('*')) if (element.shadowRoot) visit(element.shadowRoot);
  };
  visit(document);
  return cells;
};
const samples = [capture()];
window.__pivotSupersession.release();
const interval = setInterval(() => samples.push(capture()), 25);
setTimeout(() => {clearInterval(interval); samples.push(capture()); done(samples);}, 1500);
"""


def qualify_superseded_response(host: str) -> None:
    """Exercise the same two real searched data requests through an installed host."""
    import pytest
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required; browser absence is not a pass")
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _host_url, _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _workspace_elements,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait

    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "superseded-response" / host
    if ("runs", run_id) not in tuple(zip(directory.parts, directory.parts[1:])):
        pytest.fail("Workflow evidence must be inside this run's private evidence tree")
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    driver = None
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    trace, diagnostic = [], ""
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding = binding
        driver._pivot_artifact_host = host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _SUPERSESSION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 15)
        wait.until(_totals)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        from selenium.webdriver.common.keys import Keys
        journeys = []
        for response_mode in (("data",) if host == "dash" else ("data", "busy")):
            search = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-global-search"]')
            if search.get_attribute("value"):
                search.click()
                search.send_keys(Keys.CONTROL, "a", Keys.BACKSPACE)
                wait.until(lambda current: _wait_for_visible_workspace_element(
                    current, '[data-testid="pivot-global-search"]').get_attribute("value") == "")
            driver.execute_script("window.__pivotSupersession.arm(arguments[0])", response_mode)
            search = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-global-search"]')
            search.send_keys("Éclair")
            wait.until(lambda current: any(item["phase"] == "ready" and item["term"] == "Éclair"
                                           for item in current.execute_script("return window.__pivotSupersession.trace")))
            search.click()
            search.send_keys(Keys.CONTROL, "a", Keys.BACKSPACE)
            wait.until(lambda current: _wait_for_visible_workspace_element(
                current, '[data-testid="pivot-global-search"]').get_attribute("value") == "")
            search = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-global-search"]')
            search.send_keys("München")
            search_state = driver.execute_script("""
                const input = arguments[0];
                return {value: input.value, focused: input.getRootNode().activeElement === input,
                        selectionStart: input.selectionStart, selectionEnd: input.selectionEnd};
            """, search)
            wait.until(lambda current: any(item["phase"] == "response" and item["term"] == "München"
                                           and item["total"] == 79.75
                                           for item in current.execute_script("return window.__pivotSupersession.trace")))
            def visible_new(current):
                return supersession_fixture_matches(_navigation_cells(current))
            wait.until(visible_new)
            newer_cells = _navigation_cells(driver)
            samples = driver.execute_async_script(_SUPERSESSION_OBSERVE_SCRIPT)
            wait.until(lambda current: any(item["phase"] == "response" and item["term"] == "Éclair"
                                           for item in current.execute_script("return window.__pivotSupersession.trace")))
            trace = driver.execute_script("return window.__pivotSupersession.trace")
            old_id, new_id = supersession_ids(trace)
            assert assert_superseded_response(trace, old_id, new_id, 79.75)
            assert assert_stable_visible_snapshots(newer_cells, samples)
            assert visible_new(driver), "The late old response restored its rows"
            assert not _visible_error(driver), "The late old response exposed an error"
            assert not driver.execute_script("return window.__pivotSupersession.alerts"), "A late response exposed a transient error"
            if response_mode == "busy":
                old = next(item for item in trace if item["phase"] == "response" and item["requestId"] == old_id)
                assert old["httpStatus"] == 503 and old["errorCode"] == "resource_exhausted"
                assert old["simulatedOlderPeer"] is True
            journeys.append({"responseMode": response_mode, "trace": trace,
                             "postReleaseSamples": samples, "newerVisibleCells": newer_cells})
        result.update(result="passed", actionCompleted=True, staleCellsAbsent=True,
                      v1RequestsCorrelated=True, fixtureTotalsVerified=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}"
    finally:
        paths = []
        if driver is not None:
            try:
                trace = driver.execute_script("return window.__pivotSupersession?.trace || []")
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                _network(driver)
            finally:
                driver.quit()
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps({"diagnostic": diagnostic, "trace": trace,
                                        "journeys": locals().get("journeys", []),
                                        "searchStateAfterReplacement": locals().get("search_state"),
                                        "postReleaseSamples": locals().get("samples", []),
                                        "newerVisibleCells": locals().get("newer_cells", []),
                                        "loadedAssetSha256": result.get("loadedAssetSha256", {}),
                                        "result": {key: value for key, value in result.items()
                                                   if key != "loadedAssetSha256"}}, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("runtime-error.superseded-response", host, result, directory)
    assert result["result"] == "passed", f"{host} superseded response did not qualify: {diagnostic}; evidence: {directory}"


def _visible_workspace_text(driver):
    """Include Streamlit's open shadow roots, not only ordinary page text."""
    return driver.execute_script("""
      const text = [document.body?.innerText || ''];
      const visit = root => {
        for (const element of root.querySelectorAll('*')) if (element.shadowRoot) {
          text.push(element.shadowRoot.textContent || '');
          visit(element.shadowRoot);
        }
      };
      visit(document);
      return text.join('\\n');
    """)


def qualify_runtime_recovery(host: str) -> None:
    """Receive a safe server V1 error, click Retry, and observe real recovery.

    No browser envelope is fabricated. The V1 request/response IDs and visible
    fixture totals must agree. Missing browser/assets/hooks fail closed.
    """
    import pytest

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required; browser absence is not a pass")
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _host_url, _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _workspace_elements,
    )

    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "runtime-recovery" / host
    if ("runs", run_id) not in tuple(zip(directory.parts, directory.parts[1:])):
        pytest.fail("Workflow evidence must be inside this run's private evidence tree")
    fault_file = Path(os.environ.get("PIVOT_FLAGSHIP_TEST_FAULT_FILE", "")).resolve()
    if not fault_file.is_relative_to(Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]).resolve()):
        pytest.fail("Guarded demo fault file must be inside this run's private evidence tree")
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    driver = None
    results = {name: {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
               for name in ("runtime-error.visible", "runtime-error.recovery")}
    events, trace, diagnostic = [], [], ""
    try:
        from selenium import webdriver
        from selenium.webdriver.support.ui import WebDriverWait

        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding = binding
        driver._pivot_artifact_host = host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _PROBE_SCRIPT})
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {"behavior": "allow", "downloadPath": str(downloads)})
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 15)
        driver.execute_script("""
          const visit = root => {
            for (const el of root.querySelectorAll('[data-pivot-scroll-container]')) el.scrollLeft=el.scrollWidth;
            for (const el of root.querySelectorAll('*')) if (el.shadowRoot) visit(el.shadowRoot);
          }; visit(document);
        """)
        wait.until(_totals)
        initial_trace = driver.execute_script("return window.__pivotRecoveryProbe || []")
        assert any(entry["phase"] == "response" and entry["kind"] == "data"
                   and entry["status"] == "data" and entry["rawExpectedTotal"]
                   for entry in initial_trace), "Initial V1 data payload omits exact raw -6.75 total"
        events.extend(_network(driver))
        browser = f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}"
        for result in results.values():
            result.update(result="failed", browser=browser, infrastructureUnavailable=False)
        before = set(downloads.iterdir())
        _click_workspace_control(driver, '[data-pivot-status-action="toggle-details"]')
        before_ids = {entry["requestId"] for entry in initial_trace if entry["phase"] == "request"
                      and entry["kind"] == "data"}
        fault_file.write_text("one-shot", encoding="ascii")
        _click_workspace_control(driver, '[data-pivot-status-action="refresh"]')

        def failed_request(current):
            nonlocal trace
            trace = current.execute_script("return window.__pivotRecoveryProbe || []")
            requests = {entry["requestId"] for entry in trace if entry["phase"] == "request"
                        and entry["kind"] == "data" and entry["requestId"] not in before_ids}
            return next((entry["requestId"] for entry in trace if entry["phase"] == "response"
                         and entry["kind"] == "data" and entry["status"] == "error"
                         and entry["requestId"] in requests), None)

        failed_id = wait.until(failed_request)
        assert not fault_file.exists(), "Demo fault was not consumed by the server"
        secret_hidden = "PIVOT_QUALIFICATION_PRIVATE_SENTINEL" not in _visible_workspace_text(driver)
        assert secret_hidden, "Backend exception detail appeared in visible workspace text"
        visible = False
        try:
            WebDriverWait(driver, 5).until(lambda current: _visible_error(current))
            visible = True
        except Exception as error:
            from selenium.common.exceptions import TimeoutException
            if not isinstance(error, TimeoutException):
                raise
        loading_cleared = not any(item.is_displayed() for item in _workspace_elements(driver, '[data-pivot-loading-indicator="global"]'))
        no_downloads = set(downloads.iterdir()) == before
        assert no_downloads, "Failed request created a completed or partial download"
        results["runtime-error.visible"].update(
            result="passed" if visible and loading_cleared and failed_id and secret_hidden else "failed",
            visibleError=visible, loadingCleared=loading_cleared, failedRequestCorrelated=bool(failed_id),
            privateExceptionHidden=secret_hidden, newDownloads=0,
        )
        driver.save_screenshot(str(directory / "failed.png"))
        retry = _wait_for_visible_workspace_element(driver, '[role="alert"] button')
        assert retry.text == "Retry", "Recovery did not use the visible Retry action"
        retry_checkpoint = driver.execute_script("return performance.now()")
        _click_workspace_control(driver, '[role="alert"] button')

        def recovered(current):
            nonlocal trace
            trace = current.execute_script("return window.__pivotRecoveryProbe || []")
            return _post_retry_data_response(trace, failed_id, retry_checkpoint) and _totals(current)

        wait.until(recovered)
        retry_id = _post_retry_data_response(trace, failed_id, retry_checkpoint)
        assert retry_id != failed_id
        events.extend(_network(driver))
        assert "PIVOT_QUALIFICATION_PRIVATE_SENTINEL" not in _visible_workspace_text(driver)
        assert set(downloads.iterdir()) == before, "Recovery unexpectedly created a download"
        # A terminal server error cannot yield a second, late data response.
        # Delayed-response supersession is a distinct required contract case.
        results["runtime-error.recovery"].update(
            result="passed" if not _visible_error(driver) else "failed",
            retryCorrelated=True, fixtureTotalsVerified=True, actionCompleted=True,
            newDownloads=0,
        )
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}"
    finally:
        paths = []
        if driver is not None:
            try:
                events.extend(_network(driver))
                driver.save_screenshot(str(directory / "final.png"))
                paths.extend(str(path) for path in directory.glob("*.png"))
                trace = driver.execute_script("return window.__pivotRecoveryProbe || []")
                for result in results.values():
                    result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
            finally:
                driver.quit()
        evidence = directory / "observation.json"
        # Network events are reduced before writing: no headers/post bodies,
        # raw response data, exception text or URLs enter this evidence summary.
        evidence.write_text(json.dumps({
            "diagnostic": diagnostic, "trace": trace,
            "network": [{"method": event["method"], "networkRequestId": event.get("params", {}).get("requestId"),
                         "blockedReason": event.get("params", {}).get("blockedReason")}
                        for event in events if event["method"] in ("Network.requestWillBeSent", "Network.loadingFailed", "Network.responseReceived")],
            "loadedAssetSha256": results["runtime-error.visible"].get("loadedAssetSha256", {}),
            "results": {key: {name: value for name, value in result.items() if name != "loadedAssetSha256"}
                        for key, result in results.items()},
        }, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        for requirement, result in results.items():
            result["evidence_paths"] = paths
            record_case(requirement, host, result, directory)
    failed = [name for name, result in results.items() if result["result"] != "passed"]
    assert not failed, f"{host} workflow requirements did not pass: {', '.join(failed)}; {diagnostic}; evidence: {directory}"


_NAVIGATION_PROBE_SCRIPT = r"""
if (!window.__pivotNavigationTrace) {
  const trace = [];
  const visit = (raw, phase) => {
    let value = raw;
    if (typeof value === 'string') { try { value = JSON.parse(value); } catch (_) { return; } }
    if (!value || typeof value !== 'object') return;
    if (value.schemaVersion === 1 && value.kind === 'filter_options' && typeof value.requestId === 'string') {
      trace.push({phase, at: performance.now(), requestId: value.requestId,
        schemaVersion: value.schemaVersion, kind: value.kind, status: value.status || null,
        columnId: value.payload?.columnId ?? null,
        options: Array.isArray(value.payload?.options) ? value.payload.options : null,
        scopeFilter: value.state?.filters?.scope ?? null});
      return;
    }
    if (value.schemaVersion === 1 && value.kind === 'data' && typeof value.requestId === 'string') {
      const rows = Array.isArray(value.payload?.data) ? value.payload.data : [];
      trace.push({phase, at: performance.now(), requestId: value.requestId,
        schemaVersion: value.schemaVersion, status: value.status || null,
        errorCode: value.payload?.errorCode || value.payload?.code || value.code || null,
        message: value.status === 'error' ? value.message || null : null,
        globalFilter: value.state?.filters?.global ?? null,
        filters: value.state?.filters ?? null,
        valueAggregations: Array.isArray(value.state?.valConfigs)
          ? value.state.valConfigs.map(config => ({field:config?.field, agg:config?.agg})) : null,
        showRowTotals: value.state?.showRowTotals ?? null,
        showColTotals: value.state?.showColTotals ?? null,
        sorting: Array.isArray(value.state?.sorting) ? value.state.sorting : null,
        rows: rows.map(row => ({id: row?._id, path: row?._path,
          q1_avg: row?.['Q1_ventes_\u20ac_avg'],
          total: row?.['__RowTotal__ventes_€_sum'],
          q1: row?.['Q1_ventes_€_sum'], q2: row?.['Q2_ventes_€_sum'],
          total_units: row?.['__RowTotal__unités_sum'],
          q1_units: row?.['Q1_unités_sum'], q2_units: row?.['Q2_unités_sum'],
          total_variance: row?.['__RowTotal__écart_sum'],
          q1_variance: row?.['Q1_écart_sum'], q2_variance: row?.['Q2_écart_sum']}))});
      return;
    }
    if (Array.isArray(value)) value.forEach(item => visit(item, phase));
    else Object.values(value).forEach(item => visit(item, phase));
  };
  const originalFetch = window.fetch.bind(window);
  window.fetch = (...args) => {
    if (args[1]?.body) visit(args[1].body, 'request');
    return originalFetch(...args).then(response => {
      if (!response.ok) trace.push({phase:'http-error', at:performance.now(),
        httpStatus:response.status, url:String(args[0]?.url || args[0])});
      response.clone().text().then(body => visit(body, 'response')).catch(() => {});
      return response;
    }).catch(error => {
      trace.push({phase:'fetch-error', at:performance.now(),
        message:String(error?.message || error)});
      throw error;
    });
  };
  const originalSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function(body) {
    visit(body, 'request');
    this.addEventListener('load', () => visit(this.responseText, 'response'));
    return originalSend.call(this, body);
  };
  window.__pivotNavigationTrace = trace;
}
"""


def _navigation_rows(driver):
    return driver.execute_script("""
      const rows = [];
      const visit = root => {
        for (const cell of root.querySelectorAll('[role="gridcell"][data-colid="hierarchy"]')) {
          if (cell.getClientRects().length && cell.innerText.trim())
            rows.push([cell.getAttribute('data-rowid'), cell.innerText.trim()]);
        }
        for (const element of root.querySelectorAll('*')) if (element.shadowRoot) visit(element.shadowRoot);
      };
      visit(document);
      return rows;
    """)


def aggregation_round_trip_matches(states):
    """Require three distinct V1 deliveries and the same visible measure slot."""
    from decimal import Decimal, InvalidOperation

    if len(states) != 3 or [state.get("aggregation") for state in states] != ["sum", "avg", "sum"]:
        return False
    if len({state.get("requestId") for state in states}) != 3:
        return False
    positions = [state.get("columnPosition") for state in states]
    if any(not isinstance(position, int) or position < 0 for position in positions) or len(set(positions)) != 1:
        return False
    try:
        values = [(Decimal(str(state["rawQ1"])),
                   Decimal(str(state["visibleQ1"]).replace(",", ""))) for state in states]
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return False
    return values == [(Decimal("150"), Decimal("150")),
                      (Decimal("75"), Decimal("75")),
                      (Decimal("150"), Decimal("150"))]


def _navigation_columns(driver):
    return driver.execute_script("""
      const ids = [];
      const visit = root => {
        for (const cell of root.querySelectorAll('[role="gridcell"][data-colid]')) {
          if (cell.getClientRects().length && cell.getAttribute('data-rowid') === 'Éclair')
            ids.push(cell.getAttribute('data-colid'));
        }
        for (const element of root.querySelectorAll('*')) if (element.shadowRoot) visit(element.shadowRoot);
      };
      visit(document);
      return ids;
    """)


def _navigation_cells(driver):
    return driver.execute_script("""
      const cells = [];
      const visit = root => {
        for (const cell of root.querySelectorAll('[role="gridcell"][data-colid][data-rowid]')) {
          if (cell.getClientRects().length) cells.push({
            rowid: cell.getAttribute('data-rowid'),
            colid: cell.getAttribute('data-colid'),
            text: cell.innerText.trim(),
          });
        }
        for (const element of root.querySelectorAll('*')) if (element.shadowRoot) visit(element.shadowRoot);
      };
      visit(document);
      return cells;
    """)


def _navigation_dom_matches(cells, expected_children, *, decimal_places=0):
    columns = ("Q1_ventes_€_sum", "Q2_ventes_€_sum", "__RowTotal__ventes_€_sum")
    expected_integer = {
        "Éclair": ("100", "20", "120"),
        "München": ("50", "30", "80"),
        "__grand_total__": ("150", "50", "200"),
        "Éclair|||東京": ("100", "20", "120"),
        "München|||القاهرة": ("50", "30", "80"),
    }
    expected_two_decimals = {
        "Éclair": ("100.25", "20.00", "120.25"),
        "München": ("49.75", "30.00", "79.75"),
        "__grand_total__": ("150.00", "50.00", "200.00"),
        "Éclair|||東京": ("100.25", "20.00", "120.25"),
        "München|||القاهرة": ("49.75", "30.00", "79.75"),
    }
    if decimal_places not in (0, 2):
        raise ValueError(f"Unsupported hierarchy display precision: {decimal_places}")
    expected = expected_two_decimals if decimal_places == 2 else expected_integer
    keys = {"Éclair", "München", "__grand_total__"}
    keys.update({"東京": "Éclair|||東京", "القاهرة": "München|||القاهرة"}[child]
                for child in expected_children)
    actual = [(cell["rowid"], cell["colid"], cell["text"])
              for cell in cells if cell["colid"] in columns]
    if len(actual) != len(keys) * len(columns):
        return False
    if len({(rowid, colid) for rowid, colid, _ in actual}) != len(actual):
        return False
    if {rowid for rowid, _, _ in actual} != keys:
        return False
    return all((rowid, colid, value) in actual
               for rowid in keys for colid, value in zip(columns, expected[rowid]))


def initial_workspace_rows_match(rows) -> bool:
    """Match the literal included fixture, including all three measure totals."""
    rows = [row for row in rows if row.get("path") != "__color_scale_stats__" or row.get("id")]
    fields = ("q1", "q2", "total", "q1_units", "q2_units", "total_units",
              "q1_variance", "q2_variance", "total_variance")
    expected = {
        "Éclair": (100.25, 20.0, 120.25, 2, 1, 3, -12.5, 2.5, -10.0),
        "München": (49.75, 30.0, 79.75, 3, 4, 7, 4.25, -1.0, 3.25),
        "Grand Total": (150.0, 50.0, 200.0, 5, 5, 10, -8.25, 1.5, -6.75),
    }
    return (len(rows) == len(expected)
            and {row.get("id") for row in rows} == set(expected)
            and all(tuple(row.get(field) for field in fields) == expected[row["id"]]
                    for row in rows))


def initial_workspace_expected_cells(rows):
    from decimal import Decimal

    if not initial_workspace_rows_match(rows):
        raise AssertionError("Initial V1 rows differ from the parity fixture")
    expected = {}
    for row in rows:
        if row.get("path") == "__color_scale_stats__" and not row.get("id"):
            continue
        rowid = "__grand_total__" if row["id"] == "Grand Total" else row["id"]
        for period, prefix in (("Q1", "q1"), ("Q2", "q2"), ("__RowTotal__", "total")):
            for measure, suffix in (("ventes_€", ""), ("unités", "_units"), ("écart", "_variance")):
                separator = "" if period == "__RowTotal__" else "_"
                expected[(rowid, f"{period}{separator}{measure}_sum")] = Decimal(str(row[prefix + suffix]))
    return expected


def global_filter_rows_match(rows) -> bool:
    rows = [row for row in rows if row.get("path") != "__color_scale_stats__" or row.get("id")]
    expected = {"München", "Grand Total"}
    return (len(rows) == 2 and {row.get("id") for row in rows} == expected
            and all((row.get("q1"), row.get("q2"), row.get("total")) == (49.75, 30.0, 79.75)
                    for row in rows))


def qualify_header_filter(host: str) -> None:
    """Apply and clear a region equality filter from its grouped header."""
    import pytest
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch,
        _workspace_elements,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "header-filter" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"states": []}
    paths = []
    diagnostic = ""
    driver = None
    region = "\u00c9clair"
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 25)
        wait.until(lambda current: {label for _, label in _navigation_rows(current)} ==
                   {region, "M\u00fcnchen", "Grand Total"})

        def click_filter():
            button = wait.until(lambda current: next((button
                for header in _workspace_elements(current,
                    '[role="columnheader"][data-header-column-id="hierarchy"]')
                if header.is_displayed() and "Region" in header.text
                for button in header.find_elements("css selector", '[role="button"][aria-label="Filter Column"]')
                if button.is_displayed()), False))
            button.click()

        def topmost(element):
            return driver.execute_script("""
                const candidate = arguments[0], rect = candidate.getBoundingClientRect();
                const x = rect.left + rect.width / 2, y = rect.top + rect.height / 2;
                let hit = document.elementFromPoint(x, y);
                while (hit && hit.shadowRoot) {
                    const nested = hit.shadowRoot.elementFromPoint(x, y);
                    if (!nested || nested === hit) break;
                    hit = nested;
                }
                return candidate === hit || candidate.contains(hit);
            """, element)

        def visible_popover_item(selector, label, *, checkbox=False):
            def find(current):
                for item in _workspace_elements(current, selector):
                    try:
                        if item.text.strip() != label:
                            continue
                        target = item.find_element("xpath", "..") if checkbox else item
                        if checkbox and not target.find_elements("css selector", 'input[type="checkbox"]'):
                            continue
                        if target.is_displayed() and topmost(target):
                            return target
                    except Exception:
                        continue
                return False
            return wait.until(find)

        def response_after(checkpoint, selected_region):
            trace = driver.execute_script("return window.__pivotNavigationTrace || []")
            def matches_request(entry):
                if entry["phase"] != "request" or entry["at"] <= checkpoint:
                    return False
                conditions = (entry.get("filters") or {}).get("region", {}).get("conditions", [])
                return (len(conditions) == 1 and conditions[0].get("type") == "in"
                        and conditions[0].get("value") == [selected_region]) if selected_region else not conditions
            requests = {entry["requestId"] for entry in trace if matches_request(entry)}
            expected_ids = {selected_region, "Grand Total"} if selected_region else {region, "M\u00fcnchen", "Grand Total"}
            expected_total = ({region: 120.25, "M\u00fcnchen": 79.75}.get(selected_region)
                              if selected_region else 200.0)
            return next((entry for entry in trace if entry["phase"] == "response"
                         and entry["requestId"] in requests and entry["status"] == "data"
                         and {row["id"] for row in entry["rows"] if row["id"]} == expected_ids
                         and next((row["total"] for row in entry["rows"] if row["id"] == "Grand Total"), None)
                         == expected_total), None)

        click_filter()
        def option_response(current):
            trace = current.execute_script("return window.__pivotNavigationTrace || []")
            requests = {entry["requestId"] for entry in trace if entry.get("kind") == "filter_options"
                        and entry["phase"] == "request" and entry.get("columnId") == "region"
                        and (entry.get("scopeFilter") or {}).get("conditions") ==
                        [{"type": "eq", "value": "Included", "caseSensitive": False}]}
            return next((entry for entry in trace if entry.get("kind") == "filter_options"
                         and entry["phase"] == "response" and entry["requestId"] in requests
                         and entry["status"] == "ok" and set(entry.get("options") or []) ==
                         {region, "M\u00fcnchen"} and len(entry["options"]) == 2), None)
        options = wait.until(option_response)
        observation["filterOptions"] = {"requestId": options["requestId"], "options": options["options"]}
        visible_popover_item("span", region, checkbox=True).click()
        checkpoint = driver.execute_script("return performance.now()")
        visible_popover_item("button", "Apply").click()
        wait.until(lambda current: response_after(checkpoint, region)
                   and {label for _, label in _navigation_rows(current)} == {region, "Grand Total"})
        applied = response_after(checkpoint, region)
        observation["states"].append({"phase": "applied", "requestId": applied["requestId"],
                                      "rows": applied["rows"], "visibleRows": _navigation_rows(driver)})

        click_filter()
        clear_at = driver.execute_script("return performance.now()")
        visible_popover_item("button", "Clear & Close").click()
        wait.until(lambda current: response_after(clear_at, None)
                   and {label for _, label in _navigation_rows(current)} ==
                   {region, "M\u00fcnchen", "Grand Total"})
        restored = response_after(clear_at, None)
        observation["states"].append({"phase": "restored", "requestId": restored["requestId"],
                                      "rows": restored["rows"], "visibleRows": _navigation_rows(driver)})
        click_filter()
        visible_popover_item("span", "M\u00fcnchen", checkbox=True).click()
        munich_at = driver.execute_script("return performance.now()")
        visible_popover_item("button", "Apply").click()
        wait.until(lambda current: response_after(munich_at, "M\u00fcnchen")
                   and {label for _, label in _navigation_rows(current)} == {"M\u00fcnchen", "Grand Total"})
        munich = response_after(munich_at, "M\u00fcnchen")
        observation["states"].append({"phase": "other-option", "requestId": munich["requestId"],
                                      "rows": munich["rows"], "visibleRows": _navigation_rows(driver)})
        click_filter()
        reset_at = driver.execute_script("return performance.now()")
        visible_popover_item("button", "Clear & Close").click()
        wait.until(lambda current: response_after(reset_at, None)
                   and {label for _, label in _navigation_rows(current)} ==
                   {region, "M\u00fcnchen", "Grand Total"})
        reset = response_after(reset_at, None)
        observation["states"].append({"phase": "other-option-cleared", "requestId": reset["requestId"],
                                      "rows": reset["rows"], "visibleRows": _navigation_rows(driver)})
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      v1RequestsCorrelated=True)
    except Exception as error:
        frames = traceback.extract_tb(error.__traceback__)
        frame = next((item for item in reversed(frames)
                      if Path(item.filename).name == "flagship_browser_support.py"), frames[-1])
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("header-filter.apply-clear", host, result, directory)
        record_case("filter-options.values", host, result, directory)
    assert result["result"] == "passed", f"{host} header filter: {diagnostic}; evidence: {directory}"


def qualify_sort_direction(host: str) -> None:
    """Sort the installed workspace by total sales in both directions."""
    import pytest
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch,
    )
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "sort-direction" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"states": []}
    paths = []
    diagnostic = ""
    driver = None
    column = "__RowTotal__ventes_\u20ac_sum"
    expected_values = {"M\u00fcnchen": 79.75, "\u00c9clair": 120.25, "Grand Total": 200.0}
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 25)
        wait.until(lambda current: _totals(current) and
                   {label for _, label in _navigation_rows(current)} ==
                   {"\u00c9clair", "M\u00fcnchen", "Grand Total"})
        for direction, expected_order in (("Ascending", ["M\u00fcnchen", "\u00c9clair", "Grand Total"]),
                                          ("Descending", ["\u00c9clair", "M\u00fcnchen", "Grand Total"])):
            header = _wait_for_visible_workspace_element(
                driver, f'[role="columnheader"][data-header-column-id="{column}"]')
            checkpoint = driver.execute_script("return performance.now()")
            ActionChains(driver).context_click(header).perform()
            ActionChains(driver).move_to_element(_selection_menu(driver, "Sort")).perform()
            _selection_menu(driver, direction).click()

            def settled(current):
                trace = current.execute_script("return window.__pivotNavigationTrace || []")
                requests = {entry["requestId"] for entry in trace if entry["phase"] == "request"
                            and entry["at"] > checkpoint and entry.get("sorting") ==
                            [{"id": column, "desc": direction == "Descending"}]}
                response = next((entry for entry in trace if entry["phase"] == "response"
                                 and entry["requestId"] in requests and entry["status"] == "data"
                                 and [row["id"] for row in entry["rows"] if row["id"]] == expected_order
                                 and all(row["total"] == expected_values[row["id"]]
                                         for row in entry["rows"] if row["id"])), None)
                if not response or [label for _, label in _navigation_rows(current)] != expected_order:
                    return False
                indicator = _wait_for_visible_workspace_element(
                    current, f'[role="columnheader"][data-header-column-id="{column}"]').get_attribute("aria-sort")
                if indicator != ("asc" if direction == "Ascending" else "desc"):
                    return False
                return {"direction": direction, "requestId": response["requestId"],
                        "responseRows": response["rows"], "visibleRows": _navigation_rows(current),
                        "indicator": indicator}

            observation["states"].append(wait.until(settled))
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      v1RequestsCorrelated=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("sorting.direction-order", host, result, directory)
    assert result["result"] == "passed", f"{host} sorting: {diagnostic}; evidence: {directory}"


def qualify_aggregation_round_trip(host: str) -> None:
    """Change sales Sum -> Avg -> Sum in a built host and check V1 plus cells."""
    import pytest
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch,
        _workspace_elements,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import Select, WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "aggregation-round-trip" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"states": []}
    paths = []
    diagnostic = ""
    driver = None
    sales = "ventes_\u20ac"

    def aggregation_select():
        return next((select for chip in _workspace_elements(driver, '[data-sidebar-field-chip="zone"]')
                     if chip.is_displayed() and "ventes" in chip.text.lower()
                     for select in chip.find_elements("css selector", 'select[title="Aggregation"]')
                     if select.is_displayed()), None)

    def observed_state(aggregation, checkpoint):
        trace = driver.execute_script("return window.__pivotNavigationTrace || []")
        requests = {entry["requestId"] for entry in trace
                    if entry.get("phase") == "request" and entry.get("at", -1) > checkpoint
                    and entry.get("status") is None
                    and any(config.get("field") == sales and config.get("agg") == aggregation
                            for config in entry.get("valueAggregations") or [])}
        key = f"Q1_{sales}_{aggregation}"
        cell = next((cell for cell in _navigation_cells(driver)
                     if cell["rowid"] == "__grand_total__" and cell["colid"] == key), None)
        columns = _navigation_columns(driver)
        if cell is None or key not in columns:
            return None
        for entry in trace:
            if entry.get("phase") != "response" or entry.get("status") != "data" \
                    or entry.get("requestId") not in requests:
                continue
            grand = next((row for row in entry.get("rows") or [] if row.get("id") == "Grand Total"), None)
            raw = grand.get("q1_avg" if aggregation == "avg" else "q1") if grand else None
            if raw is not None:
                return {"aggregation": aggregation, "rawQ1": raw, "visibleQ1": cell["text"],
                        "columnPosition": columns.index(key), "requestId": entry["requestId"]}
        return None

    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 30)
        if aggregation_select() is None:
            _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
        wait.until(lambda _: aggregation_select())
        initial = wait.until(lambda _: observed_state("sum", -1))
        observation["states"].append(initial)

        checkpoint = driver.execute_script("return performance.now()")
        Select(aggregation_select()).select_by_value("avg")
        average = wait.until(lambda _: observed_state("avg", checkpoint))
        observation["states"].append(average)
        assert Select(aggregation_select()).first_selected_option.get_attribute("value") == "avg"

        checkpoint = driver.execute_script("return performance.now()")
        Select(aggregation_select()).select_by_value("sum")
        restored = wait.until(lambda _: observed_state("sum", checkpoint))
        observation["states"].append(restored)
        assert Select(aggregation_select()).first_selected_option.get_attribute("value") == "sum"
        assert aggregation_round_trip_matches(observation["states"]), observation["states"]
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      v1RequestsCorrelated=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("layout-values.aggregation", host, result, directory)
    assert result["result"] == "passed", f"{host} aggregation: {diagnostic}; evidence: {directory}"


_EXACT_EXPORT_PROBE_SCRIPT = r"""
if (!window.__pivotExactExports) {
  const trace = [];
  window.__pivotExactExportPending = 0;
  const digest = (blob, entry) => {
    window.__pivotExactExportPending++;
    blob.arrayBuffer().then(bytes => crypto.subtle.digest('SHA-256', bytes)).then(hash => {
      trace.push({...entry, bytes:blob.size,
        sha256:Array.from(new Uint8Array(hash), byte => byte.toString(16).padStart(2,'0')).join('')});
    }).catch(() => trace.push({...entry, digestFailed:true})).finally(() => window.__pivotExactExportPending--);
  };
  const capture = (raw, phase, base=location.href, depth=0) => {
    if (depth > 8 || raw == null) return;
    if (typeof raw === 'string') { try {capture(JSON.parse(raw), phase, base, depth+1)} catch (_) {} return; }
    if (typeof raw !== 'object') return;
    if (raw.schemaVersion === 1 && raw.kind === 'export' && raw.requestId) {
      const payload = raw.payload || {};
      trace.push({phase, at:performance.now(), requestId:raw.requestId,
        schemaVersion:raw.schemaVersion, status:raw.status || null, format:payload.format || null,
        action:payload.action || null, jobId:payload.jobId || null, jobPhase:payload.phase || null,
        payloadUrl:payload.payloadRef?.url ? new URL(payload.payloadRef.url, base).href : null,
        colIds:payload.colIds || null, allRows:payload.allRows === true,
        colStart:payload.col_start ?? payload.colStart ?? null,
        colEnd:payload.col_end ?? payload.colEnd ?? null,
        scopeFilter:raw.state?.filters?.scope ?? null});
      return;
    }
    Object.values(raw).forEach(value => capture(value, phase, base, depth+1));
  };
  const originalFetch = window.fetch.bind(window);
  window.fetch = (...args) => {
    capture(args[1]?.body, 'request');
    return originalFetch(...args).then(response => {
      if (new URL(response.url).pathname.includes('/payload/')) {
        window.__pivotExactExportPending++;
        response.clone().blob().then(blob => digest(blob, {phase:'delivery', at:performance.now(),
          url:response.url, status:response.status})).finally(() => window.__pivotExactExportPending--);
      } else {
        window.__pivotExactExportPending++;
        response.clone().text().then(body => capture(body, 'response', response.url)).catch(() => {})
          .finally(() => window.__pivotExactExportPending--);
      }
      return response;
    });
  };
  const originalSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function(body) {
    capture(body, 'request');
    this.addEventListener('load', () => capture(this.responseText, 'response', this.responseURL));
    return originalSend.call(this, body);
  };
  const create = URL.createObjectURL.bind(URL);
  URL.createObjectURL = blob => {
    const url = create(blob);
    if (blob instanceof Blob) digest(blob, {phase:'blob', at:performance.now(), url});
    return url;
  };
  const click = HTMLAnchorElement.prototype.click;
  HTMLAnchorElement.prototype.click = function(...args) {
    if (this.download) trace.push({phase:'download', at:performance.now(), url:this.href, filename:this.download});
    return click.apply(this,args);
  };
  window.__pivotExactExports = trace;
}
"""


def assert_single_exact_download(directory, downloaded):
    assert sorted(directory.iterdir()) == [downloaded], "Expected exactly one completed download and no partial files"


def assert_exact_export_delivery(trace, export_format, checkpoint, sha256, filename, size):
    """Bind one export lifecycle through payload bytes and blob to its real file."""
    trace = [entry for entry in trace if entry["at"] > checkpoint]
    requests = [entry for entry in trace if entry["phase"] == "request"]
    starts = [entry for entry in requests if entry.get("action") == "start"]
    assert len(starts) == 1 and starts[0].get("format") == export_format
    by_id = {entry["requestId"]: entry for entry in requests}
    assert len(by_id) == len(requests)
    responses = [entry for entry in trace if entry["phase"] == "response"]
    assert len(responses) == len(requests)
    assert {entry["requestId"] for entry in responses} == set(by_id)
    assert len({entry["requestId"] for entry in responses}) == len(responses)
    jobs = {entry.get("jobId") for entry in responses}
    assert len(jobs) == 1 and None not in jobs
    job_id, = jobs
    assert all(entry.get("status") == "export_job" and entry.get("jobPhase") in ("queued", "running", "completed") for entry in responses)
    assert all(entry.get("action") == "start" or entry.get("action") == "status" and entry.get("jobId") == job_id for entry in requests)
    completed = [entry for entry in responses if entry.get("jobPhase") == "completed"]
    assert len(completed) == 1 and completed[0].get("payloadUrl")
    assert all(not entry.get("payloadUrl") for entry in responses if entry not in completed)
    deliveries = [entry for entry in trace if entry["phase"] == "delivery"]
    assert len(deliveries) == 1
    delivery, = deliveries
    assert delivery.get("url") == completed[0]["payloadUrl"] and delivery.get("status") == 200
    assert delivery.get("sha256") == sha256 and delivery.get("bytes") == size
    downloads = [entry for entry in trace if entry["phase"] == "download"]
    assert len(downloads) == 1 and downloads[0].get("filename") == filename
    blobs = [entry for entry in trace if entry["phase"] == "blob" and entry.get("url") == downloads[0]["url"]]
    assert len(blobs) == 1 and blobs[0].get("sha256") == sha256 and blobs[0].get("bytes") == size
    return job_id


def qualify_exact_download(host: str, export_format: str) -> None:
    """Download the real included hierarchy and compare every cell and header."""
    import hashlib
    import time
    import pytest
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.export_qualification_fixture import assert_exact_flagship_download
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace, _verify_release_asset_batch,
        _apply_qualification_column_order, _click_export, _wait_for_download,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    requirement = {"csv": "export-csv.exact-document", "xlsx": "export-xlsx.exact-workbook"}[export_format]
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "exact-download" / export_format / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    driver = None
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation, paths, diagnostic = {"format": export_format}, [], ""
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _EXACT_EXPORT_PROBE_SCRIPT})
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 30)
        wait.until(_totals)
        _apply_qualification_column_order(driver)
        checkpoint = driver.execute_script("return performance.now()")
        started_at = time.time()
        _click_export(driver, export_format)
        downloaded = _wait_for_download(downloads, export_format, timeout=60, started_at=started_at)
        time.sleep(1)
        wait.until(lambda current: current.execute_script("return window.__pivotExactExportPending === 0"))
        assert_single_exact_download(downloads, downloaded)
        paths.append(str(downloaded))
        observation["download"] = {"file": downloaded.name, "bytes": downloaded.stat().st_size,
                                   "sha256": hashlib.sha256(downloaded.read_bytes()).hexdigest()}
        parsed = assert_exact_flagship_download(downloaded, export_format)
        parsed_path = directory / "parsed-document.json"
        parsed_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(parsed_path))
        trace = driver.execute_script("return window.__pivotExactExports || []")
        observation["jobId"] = assert_exact_export_delivery(trace, export_format, checkpoint,
            observation["download"]["sha256"], downloaded.name, downloaded.stat().st_size)
        requests = [entry for entry in trace if entry["phase"] == "request" and entry["at"] > checkpoint
                    and entry["format"] == export_format]
        assert requests, "No V1 export request matched the downloaded format"
        expected_ids = ["hierarchy"] + [f"{period}_{field}_sum"
            for period in ("Q1", "Q2") for field in ("unit\u00e9s", "ventes_\u20ac", "\u00e9cart")] + [
                f"__RowTotal__{field}_sum" for field in ("unit\u00e9s", "ventes_\u20ac", "\u00e9cart")]
        for request in requests:
            assert request["schemaVersion"] == 1 and request["allRows"] is True
            if request["colIds"] is not None:
                assert request["colIds"] == expected_ids
            assert request["colStart"] in (None, 0)
            assert request["colEnd"] is None or request["colEnd"] >= 8
            assert any(condition.get("value") == "Included" for condition in
                       (request["scopeFilter"] or {}).get("conditions", []))
        observation["trace"] = trace
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      v1RequestsCorrelated=True, parsedCsvVerified=export_format == "csv",
                      parsedXlsxVerified=export_format == "xlsx")
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["trace"] = driver.execute_script("return window.__pivotExactExports || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {export_format} exact download: {diagnostic}; evidence: {directory}"


def measure_add_remove_matches(states):
    """Require exact totals and complete removal of unit headers and cells."""
    from decimal import Decimal, InvalidOperation

    if len(states) != 3 or [state.get("hasUnits") for state in states] != [False, True, False]:
        return False
    if len({state.get("requestId") for state in states}) != 3 or not all(state.get("requestId") for state in states):
        return False
    try:
        for state in states:
            for key in ("rawSales", "visibleSales"):
                if [Decimal(str(value).replace(",", "")) for value in state[key]] != [150, 50, 200]:
                    return False
            for key in ("rawUnits", "visibleUnits"):
                expected = [5, 5, 10] if state["hasUnits"] else []
                if [Decimal(str(value).replace(",", "")) for value in state[key]] != expected:
                    return False
            if state["unitHeaderCount"] != (3 if state["hasUnits"] else 0):
                return False
            if state["unitCellCount"] != (9 if state["hasUnits"] else 0):
                return False
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return False
    return True


def qualify_measure_add_remove(host: str) -> None:
    """Use actual sidebar controls for sales-only -> sales/units -> sales-only."""
    import pytest
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch, _workspace_elements,
    )
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "measure-add-remove" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"states": [], "setup": "Remove units and variance through sidebar close controls to obtain a sales-only pivot"}
    paths, diagnostic, driver = [], "", None
    sales, units = "ventes_\u20ac", "unit\u00e9s"

    def chip(label, kind="zone"):
        return next((item for item in _workspace_elements(driver, f'[data-sidebar-field-chip="{kind}"]')
                     if item.is_displayed() and label.casefold() in item.text.casefold()), None)

    def remove(label):
        element = wait.until(lambda _: chip(label))
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'})", element)
        # The close icon is the chip's final span; clicking it exercises the
        # rendered remove action, without invoking internal React state.
        element.find_elements("css selector", "span")[-1].click()
        wait.until(lambda _: chip(label) is None)

    def state(has_units, checkpoint):
        trace = driver.execute_script("return window.__pivotNavigationTrace || []")
        fields = {sales, units} if has_units else {sales}
        requests = {entry["requestId"] for entry in trace
                    if entry.get("phase") == "request" and entry.get("at", -1) > checkpoint
                    and {config.get("field") for config in entry.get("valueAggregations") or []} == fields
                    and all(config.get("agg") == "sum" for config in entry.get("valueAggregations") or [])}
        response = next((entry for entry in reversed(trace) if entry.get("phase") == "response"
                         and entry.get("status") == "data" and entry.get("requestId") in requests), None)
        grand = next((row for row in (response or {}).get("rows") or [] if row.get("id") == "Grand Total"), None)
        if grand is None:
            return None
        cells = _navigation_cells(driver)
        by_id = {cell["colid"]: cell["text"] for cell in cells if cell["rowid"] == "__grand_total__"}
        keys = lambda field: [f"Q1_{field}_sum", f"Q2_{field}_sum", f"__RowTotal__{field}_sum"]
        observation["lastProbe"] = {"grandCellIds": list(by_id), "hasUnits": has_units}
        if any(key not in by_id for key in keys(sales)) or has_units and any(key not in by_id for key in keys(units)):
            return None
        headers = [header for header in _workspace_elements(driver, '[role="columnheader"][data-header-column-id]')
                   if header.is_displayed() and units in (header.get_attribute("data-header-column-id") or "")]
        unit_cell_count = sum(units in cell["colid"] for cell in cells)
        # The total leaf is also represented at the parent header level.
        # Count logical column IDs, while retaining both DOM nodes in evidence.
        unit_header_ids = [header.get_attribute("data-header-column-id") for header in headers]
        unit_header_count = len(set(unit_header_ids))
        observation["lastProbe"].update(
            unitHeaderIds=unit_header_ids,
            unitCellCount=unit_cell_count)
        if unit_header_count != (3 if has_units else 0) or unit_cell_count != (9 if has_units else 0):
            return None
        raw_units = [grand.get(key) for key in ("q1_units", "q2_units", "total_units")]
        return {"requestId": response["requestId"], "hasUnits": has_units,
                "rawSales": [grand.get(key) for key in ("q1", "q2", "total")],
                "visibleSales": [by_id[key] for key in keys(sales)],
                "rawUnits": raw_units if has_units else [value for value in raw_units if value is not None],
                "visibleUnits": [by_id[key] for key in keys(units)] if has_units else [],
                "unitHeaderCount": unit_header_count,
                "unitCellCount": unit_cell_count}

    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 30)
        if chip("ventes") is None:
            _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
        remove("\u00e9cart")
        checkpoint = driver.execute_script("return performance.now()")
        remove(units)
        before = wait.until(lambda _: state(False, checkpoint))
        observation["states"].append(before)
        checkpoint = driver.execute_script("return performance.now()")
        wait.until(lambda _: chip(units, "available")).send_keys(Keys.SPACE)
        _wait_for_visible_workspace_element(driver, '[data-sidebar-drop-zone="vals"]').send_keys(Keys.ENTER)
        _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
        added = wait.until(lambda _: state(True, checkpoint))
        observation["states"].append(added)
        driver.save_screenshot(str(directory / "added.png"))
        paths.append(str(directory / "added.png"))
        _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toggle-sidebar"]').click()
        checkpoint = driver.execute_script("return performance.now()")
        remove(units)
        removed = wait.until(lambda _: state(False, checkpoint))
        observation["states"].append(removed)
        assert measure_add_remove_matches(observation["states"]), observation["states"]
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      v1RequestsCorrelated=True, staleCellsAbsent=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("layout-values.add-remove", host, result, directory)
    assert result["result"] == "passed", f"{host} measure add/remove: {diagnostic}; evidence: {directory}"


def _totals_control_button(driver, label):
    return driver.execute_script("""
      const visit = root => {
        for (const button of root.querySelectorAll('button')) {
          if (button.textContent.trim() === arguments[0] && !button.disabled &&
              button.getClientRects().length && getComputedStyle(button).visibility !== 'hidden') return button;
        }
        for (const element of root.querySelectorAll('*')) {
          if (element.shadowRoot) { const found = visit(element.shadowRoot); if (found) return found; }
        }
        return null;
      };
      return visit(document);
    """, label)


def _click_totals_control(driver, label, *, timeout=10):
    from selenium.common.exceptions import StaleElementReferenceException
    from selenium.webdriver.support.ui import WebDriverWait

    def click(current):
        try:
            button = _totals_control_button(current, label)
            if button is None:
                return False
            current.execute_script("arguments[0].scrollIntoView({block:'center',inline:'center'});", button)
            button.click()
            return True
        except StaleElementReferenceException:
            return False

    WebDriverWait(driver, timeout, poll_frequency=.1).until(click, f"Unable to click {label}")


def qualify_totals_controls(host: str) -> None:
    """Observe row-total columns and the grand row through real View controls."""
    import pytest
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch,
        _workspace_elements,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "totals-controls" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"states": []}
    paths = []
    diagnostic = ""
    driver = None
    row_total_key = "__RowTotal__ventes_\u20ac_sum"
    q1_key, q2_key = "Q1_ventes_\u20ac_sum", "Q2_ventes_\u20ac_sum"

    def visible_button(label):
        return _totals_control_button(driver, label)

    def click_control(label):
        _click_totals_control(driver, label)

    def observed_state(show_row, show_col, checkpoint):
        trace = driver.execute_script("return window.__pivotNavigationTrace || []")
        requests = {entry["requestId"] for entry in trace
                    if entry.get("phase") == "request" and entry.get("at", -1) > checkpoint
                    and entry.get("showRowTotals") is show_row
                    and entry.get("showColTotals") is show_col}
        columns = _navigation_columns(driver)
        rows = _navigation_rows(driver)
        cells = {(cell["rowid"], cell["colid"]): cell["text"] for cell in _navigation_cells(driver)}
        grand_visible = any(label == "Grand Total" for _, label in rows)
        if (row_total_key in columns) is not show_row or grand_visible is not show_col:
            return None
        if q1_key not in columns or q2_key not in columns:
            return None
        for entry in trace:
            if entry.get("phase") != "response" or entry.get("status") != "data" \
                    or entry.get("requestId") not in requests:
                continue
            data_rows = entry.get("rows") or []
            groups = {row.get("id"): row for row in data_rows}
            first, second = groups.get("\u00c9clair"), groups.get("M\u00fcnchen")
            if first is None or second is None or (first.get("q1"), first.get("q2")) != (100.25, 20):
                continue
            if (second.get("q1"), second.get("q2")) != (49.75, 30):
                continue
            grand = next((row for row in data_rows if row.get("id") == "Grand Total"), None)
            if (grand is not None) is not show_col:
                continue
            if show_col and (grand.get("q1"), grand.get("q2")) != (150, 50):
                continue
            if show_row and show_col and grand.get("total") != 200:
                continue
            if show_col and (("__grand_total__", q1_key) not in cells or
                             ("__grand_total__", q2_key) not in cells):
                continue
            return {"showRowTotals": show_row, "showColTotals": show_col,
                    "requestId": entry["requestId"], "columnIds": columns,
                    "visibleRows": rows, "grandQ1": cells.get(("__grand_total__", q1_key)),
                    "grandQ2": cells.get(("__grand_total__", q2_key)),
                    "grandAll": cells.get(("__grand_total__", row_total_key)),
                    "rawGrand": grand}
        return None

    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 30)
        if visible_button("Row Total") is None:
            _click_totals_control(driver, "View")
        wait.until(lambda _: visible_button("Row Total") and visible_button("Col Total"))
        observation["states"].append(wait.until(lambda _: observed_state(True, True, -1)))
        for label, row_enabled, col_enabled in (
            ("Row Total", False, True), ("Row Total", True, True),
            ("Col Total", True, False), ("Col Total", True, True),
        ):
            checkpoint = driver.execute_script("return performance.now()")
            click_control(label)
            observation["states"].append(wait.until(
                lambda _: observed_state(row_enabled, col_enabled, checkpoint)))
        ids = [state["requestId"] for state in observation["states"]]
        assert len(set(ids)) == 5, observation["states"]
        from decimal import Decimal
        for state in (observation["states"][0], observation["states"][-1]):
            assert [Decimal(str(state[key]).replace(",", "")) for key in
                    ("grandQ1", "grandQ2", "grandAll")] == [150, 50, 200]
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      v1RequestsCorrelated=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("totals.controls", host, result, directory)
    assert result["result"] == "passed", f"{host} totals: {diagnostic}; evidence: {directory}"


def qualify_global_filter(host: str) -> None:
    """Apply and clear a real global search through an installed host."""
    import pytest
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "global-filter" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"states": []}
    paths = []
    diagnostic = ""
    driver = None
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 25)
        wait.until(lambda current: _totals(current)
                   and {label for _, label in _navigation_rows(current)} == {"Éclair", "München", "Grand Total"})

        def response_after(checkpoint, term, matcher):
            trace = driver.execute_script("return window.__pivotNavigationTrace || []")
            requests = {entry["requestId"] for entry in trace if entry["phase"] == "request"
                        and entry["at"] > checkpoint and entry.get("globalFilter") == term}
            return next((entry for entry in trace if entry["phase"] == "response"
                         and entry["requestId"] in requests and entry["at"] > checkpoint
                         and entry["status"] == "data" and matcher(entry["rows"])), None)

        search = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-global-search"]')
        start = driver.execute_script("return performance.now()")
        search.send_keys("München")
        wait.until(lambda current: _wait_for_visible_workspace_element(
            current, '[data-testid="pivot-global-search"]').get_attribute("value") == "München"
                   and {label for _, label in _navigation_rows(current)} == {"München", "Grand Total"}
                   and response_after(start, "München", global_filter_rows_match))
        filtered = response_after(start, "München", global_filter_rows_match)
        assert filtered is not None
        observation["states"].append({"phase": "filtered", "requestId": filtered["requestId"],
                                      "rows": filtered["rows"], "visibleRows": _navigation_rows(driver)})

        search = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-global-search"]')
        cleared_at = driver.execute_script("return performance.now()")
        from selenium.webdriver.common.keys import Keys
        search.click()
        search.send_keys(Keys.CONTROL, "a", Keys.BACKSPACE)
        wait.until(lambda current: _wait_for_visible_workspace_element(
            current, '[data-testid="pivot-global-search"]').get_attribute("value") == ""
                   and {label for _, label in _navigation_rows(current)} == {"Éclair", "München", "Grand Total"}
                   and _totals(current)
                   and response_after(cleared_at, None, initial_workspace_rows_match))
        restored = response_after(cleared_at, None, initial_workspace_rows_match)
        assert restored is not None
        observation["states"].append({"phase": "restored", "requestId": restored["requestId"],
                                      "rows": restored["rows"], "visibleRows": _navigation_rows(driver)})
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      v1RequestsCorrelated=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("global-filter.apply-clear", host, result, directory)
    assert result["result"] == "passed", f"{host} global filter: {diagnostic}; evidence: {directory}"


def qualify_initial_workspace(host: str) -> None:
    """Check the literal parity fixture through a fresh installed host."""
    import pytest
    from decimal import Decimal, InvalidOperation
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace,
        _wait_for_visible_workspace_element, _verify_release_asset_batch, _workspace_elements,
    )
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "initial-workspace" / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"states": []}
    paths = []
    diagnostic = ""
    driver = None
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 20)

        def exact_response(current):
            trace = current.execute_script("return window.__pivotNavigationTrace || []")
            requests = {item["requestId"] for item in trace if item["phase"] == "request"}
            return next((item for item in trace if item["phase"] == "response"
                         and item["status"] == "data" and item["requestId"] in requests
                         and initial_workspace_rows_match(item["rows"])), None)

        response = wait.until(exact_response)
        wait.until(lambda current: {label for _, label in _navigation_rows(current)}
                   == {"Éclair", "München", "Grand Total"} and _totals(current))
        observation["responseRequestId"] = response["requestId"]
        observation["rawRows"] = response["rows"]

        # Read every rendered leaf through horizontal virtualization. The
        # authoritative V1 response above supplies the exact unrounded values;
        # visible text is checked after selecting two decimal places.
        format_toggle = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toolbar-format"]')
        increase = [item for item in _workspace_elements(driver, '[title="Increase decimal places"]')
                    if item.is_displayed()]
        if not increase:
            format_toggle.click()
        increase_button = _wait_for_visible_workspace_element(driver, '[title="Increase decimal places"]')
        increase_button.click()
        increase_button.click()
        scroller = _wait_for_visible_workspace_element(driver, '[data-pivot-scroll-container]')
        expected = initial_workspace_expected_cells(response["rows"])
        seen = {}
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline and set(seen) != set(expected):
            for fraction in (0, 0.25, 0.5, 0.75, 1):
                driver.execute_script("arguments[0].scrollLeft = Math.round((arguments[0].scrollWidth - arguments[0].clientWidth) * arguments[1])", scroller, fraction)
                time.sleep(0.2)
                for cell in _navigation_cells(driver):
                    key = (cell["rowid"], cell["colid"])
                    if key not in expected:
                        continue
                    try:
                        number = Decimal(cell["text"].replace(",", ""))
                    except InvalidOperation:
                        continue
                    if number == expected[key]:
                        seen[key] = cell["text"]
            if set(seen) == set(expected):
                break
        assert set(seen) == set(expected), f"Missing or incorrect rendered fixture cells: {sorted(set(expected) - set(seen))}"
        observation["states"].append({"phase": "all-columns", "displayedCells": [
            {"rowid": rowid, "colid": colid, "text": value}
            for (rowid, colid), value in sorted(seen.items())]})
        result.update(result="passed", fixtureTotalsVerified=True, v1RequestsCorrelated=True,
                      actionCompleted=True)
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={key: value for key, value in result.items() if key != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case("initial-workspace.populated", host, result, directory)
    assert result["result"] == "passed", f"{host} initial workspace: {diagnostic}; evidence: {directory}"


def _navigation_response_matches(entry, expected_children):
    rows = [row for row in entry["rows"] if row.get("id")]
    by_id = {row["id"]: row for row in rows}
    if len(by_id) != len(rows):
        return False
    expected = {
        "Grand Total": (200, 150, 50),
        "Éclair": (120.25, 100.25, 20),
        "München": (79.75, 49.75, 30),
        "東京": (120.25, 100.25, 20),
        "القاهرة": (79.75, 49.75, 30),
    }
    for key in ("Grand Total", "Éclair", "München", *expected_children):
        row = by_id.get(key)
        if not row or (row.get("total"), row.get("q1"), row.get("q2")) != expected[key]:
            return False
        expected_path = {
            "Grand Total": "__grand_total__",
            "Éclair": "Éclair",
            "München": "München",
            "東京": "Éclair|||東京",
            "القاهرة": "München|||القاهرة",
        }[key]
        if row.get("path") != expected_path:
            return False
    return {key for key in ("東京", "القاهرة") if key in by_id} == set(expected_children)


def _navigation_response_after(trace, checkpoint, expected_children):
    requests = {entry["requestId"] for entry in trace
                if entry["phase"] == "request" and entry["at"] > checkpoint}
    return next((entry for entry in trace if entry["phase"] == "response"
                 and entry["requestId"] in requests and entry["status"] == "data"
                 and entry["at"] > checkpoint
                 and _navigation_response_matches(entry, expected_children)), None)


def _set_hierarchy_two_decimal_display(driver):
    """Set global display precision through the visible toolbar before navigation."""
    from tests.test_flagship_certification import _workspace_elements, _wait_for_visible_workspace_element
    from selenium.webdriver.support.ui import WebDriverWait

    toggle = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-toolbar-format"]')
    if not any(item.is_displayed() for item in _workspace_elements(driver, '[title="Increase decimal places"]')):
        toggle.click()
    increase = _wait_for_visible_workspace_element(driver, '[title="Increase decimal places"]')
    for _ in range(2):
        increase.click()
    WebDriverWait(driver, 10).until(
        lambda current: _navigation_dom_matches(_navigation_cells(current), (), decimal_places=2))
    return {"control": "Format > Increase decimal places", "clicks": 2,
            "decimalPlaces": 2, "selectionScoped": False}


_HIERARCHY_TOGGLE_QUERY = r"""
const rowId = arguments[0], matches = [];
const visit = root => {
  for (const cell of root.querySelectorAll('[role="gridcell"][data-colid="hierarchy"]')) {
    if (cell.getAttribute('data-rowid') !== rowId) continue;
    for (const button of cell.querySelectorAll('button')) {
      const r = button.getBoundingClientRect(), style = getComputedStyle(button);
      if (button.isConnected && !button.disabled && r.width > 0 && r.height > 0 &&
          style.display !== 'none' && style.visibility !== 'hidden' &&
          style.visibility !== 'collapse') matches.push(button);
    }
  }
  for (const element of root.querySelectorAll('*')) if (element.shadowRoot) visit(element.shadowRoot);
};
visit(document);
return matches.length === 1 ? matches[0] : null;
"""


_HIERARCHY_CLICK_GEOMETRY = r"""
const element = arguments[0], r = element.getBoundingClientRect();
if (!element.isConnected || !r.width || !r.height) return {ready:false};
const ancestors = [];
for (let node = element.parentElement || element.getRootNode().host; node;
     node = node.parentElement || node.getRootNode().host) ancestors.push(node);
if (!ancestors.includes(document.scrollingElement)) ancestors.push(document.scrollingElement);
const operations = [];
let clipped = false;
for (const node of ancestors) {
  const root = node === document.scrollingElement;
  const style = getComputedStyle(node), b = root ?
    {left:0, top:0, right:innerWidth, bottom:innerHeight} : node.getBoundingClientRect();
  const clipsX = root || /auto|scroll|hidden|clip/.test(style.overflowX);
  const clipsY = root || /auto|scroll|hidden|clip/.test(style.overflowY);
  const outsideX = clipsX && (r.left < b.left + 2 || r.right > b.right - 2);
  const outsideY = clipsY && (r.top < b.top + 2 || r.bottom > b.bottom - 2);
  clipped ||= outsideX || outsideY;
  const dx = outsideX ? Math.round((r.left+r.right-b.left-b.right)/2) : 0;
  const dy = outsideY ? Math.round((r.top+r.bottom-b.top-b.bottom)/2) : 0;
  const canX = (root || /auto|scroll/.test(style.overflowX)) &&
    (dx < 0 ? node.scrollLeft > 0 : dx > 0 && node.scrollLeft + node.clientWidth < node.scrollWidth);
  const canY = (root || /auto|scroll/.test(style.overflowY)) &&
    (dy < 0 ? node.scrollTop > 0 : dy > 0 && node.scrollTop + node.clientHeight < node.scrollHeight);
  const left = Math.max(1,b.left), right = Math.min(innerWidth-1,b.right);
  const top = Math.max(1,b.top), bottom = Math.min(innerHeight-1,b.bottom);
  if ((canX || canY) && right > left && bottom > top) operations.push({
    x:Math.round((left+right)/2), y:Math.round((top+bottom)/2),
    dx:canX ? dx : 0, dy:canY ? dy : 0});
}
// Reveal outer containers before aiming a wheel at a nested sidebar.
if (operations.length) return {ready:false, wheel:operations.at(-1)};
const x = (r.left+r.right)/2, y = (r.top+r.bottom)/2;
let hit = document.elementFromPoint(x,y);
while (hit?.shadowRoot) {
  const child = hit.shadowRoot.elementFromPoint(x,y);
  if (!child || child === hit) break;
  hit = child;
}
return {ready:!clipped && !!hit && (hit === element || element.contains(hit))};
"""


def _click_hierarchy_toggle(driver, row_id):
    """Reveal a fresh logical row toggle using native scroll and click actions."""
    from selenium.common.exceptions import StaleElementReferenceException, ElementClickInterceptedException
    from selenium.webdriver import ActionChains
    from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
    from selenium.webdriver.support.ui import WebDriverWait

    scrolled = False

    def attempt(current):
        nonlocal scrolled
        try:
            toggle = current.execute_script(_HIERARCHY_TOGGLE_QUERY, row_id)
            if not toggle:
                return False
            if not scrolled:
                ActionChains(current).scroll_to_element(toggle).perform()
                scrolled = True
            geometry = current.execute_script(_HIERARCHY_CLICK_GEOMETRY, toggle)
            if wheel := geometry.get("wheel"):
                ActionChains(current).scroll_from_origin(
                    ScrollOrigin.from_viewport(wheel["x"], wheel["y"]), wheel["dx"], wheel["dy"]).perform()
                return False
            if not geometry["ready"]:
                return False
            toggle.click()
            return True
        except (StaleElementReferenceException, ElementClickInterceptedException):
            scrolled = False
            return False

    WebDriverWait(driver, 10).until(attempt, f"Native hierarchy toggle not reachable: {row_id}")


def qualify_hierarchy_navigation(host: str, requirement: str) -> None:
    """Exercise hierarchy controls on installed artifacts and record exact V1/DOM evidence."""
    import pytest

    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace, _wait_for_visible_workspace_element,
        _verify_release_asset_batch,
    )
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait

    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    driver = None
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "states": []}
    paths = []
    diagnostic = ""
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _NAVIGATION_PROBE_SCRIPT})
        _switch_to_workspace(driver, host)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}",
                      infrastructureUnavailable=False)
        wait = WebDriverWait(driver, 15)
        def initial_ready(current):
            rows = _navigation_rows(current)
            labels = [label for _, label in rows]
            return rows if (labels.count("Éclair") == labels.count("München") == 1
                            and "Grand Total" in labels
                            and not any(label in ("東京", "القاهرة") for label in labels)
                            and _totals(current)) else False

        initial = wait.until(initial_ready)
        observation["displayPrecondition"] = _set_hierarchy_two_decimal_display(driver)
        initial_columns = _navigation_columns(driver)
        assert {"Q1_ventes_€_sum", "Q2_ventes_€_sum"}.issubset(initial_columns)
        initial_cells = _navigation_cells(driver)
        assert _navigation_dom_matches(initial_cells, (), decimal_places=2)
        observation["states"].append({"phase": "initial", "rows": initial, "columns": initial_columns,
                                      "salesCells": [cell for cell in initial_cells if "ventes_€_sum" in cell["colid"]]})

        if requirement == "hierarchy-expand.round-trip":
            parent_id = next(row_id for row_id, label in initial if label == "Éclair")
            checkpoint = driver.execute_script("return performance.now()")
            _click_hierarchy_toggle(driver, parent_id)
            expected_children = ("東京",)
        else:
            from tests.test_flagship_certification import _wait_for_visible_workspace_element
            header = _wait_for_visible_workspace_element(driver, '[role="columnheader"][data-header-column-id="hierarchy"]')
            ActionChains(driver).context_click(header).perform()
            checkpoint = driver.execute_script("return performance.now()")
            _selection_menu(driver, "Expand All Rows").click()
            expected_children = ("東京", "القاهرة")

        def expanded(current):
            rows = _navigation_rows(current)
            labels = [label for _, label in rows]
            trace = current.execute_script("return window.__pivotNavigationTrace || []")
            return (all(labels.count(child) == 1 for child in expected_children)
                    and _navigation_response_after(trace, checkpoint, expected_children)
                    and _totals(current, decimal_places=2))

        wait.until(expanded)
        expanded_rows = _navigation_rows(driver)
        labels = [label for _, label in expanded_rows]
        assert labels.count("Éclair") == labels.count("München") == 1
        assert labels.index("Éclair") < labels.index("東京")
        assert "München" not in labels[labels.index("Éclair") + 1:labels.index("東京")]
        if requirement in ("hierarchy-expand-all.round-trip", "totals.subtotals"):
            assert labels.index("München") < labels.index("القاهرة")
            assert "Éclair" not in labels[labels.index("München") + 1:labels.index("القاهرة")]
        response = _navigation_response_after(
            driver.execute_script("return window.__pivotNavigationTrace || []"), checkpoint, expected_children)
        assert response is not None
        if requirement == "totals.subtotals":
            by_id = {row["id"]: row for row in response["rows"] if row.get("id")}
            assert by_id["\u00c9clair"]["total"] == by_id["\u6771\u4eac"]["total"] == 120.25
            assert by_id["M\u00fcnchen"]["total"] == by_id["\u0627\u0644\u0642\u0627\u0647\u0631\u0629"]["total"] == 79.75
            assert by_id["Grand Total"]["total"] == 200
            assert by_id["\u00c9clair"]["total"] + by_id["M\u00fcnchen"]["total"] == by_id["Grand Total"]["total"]
        expanded_columns = _navigation_columns(driver)
        assert set(expanded_columns) == set(initial_columns)
        expanded_cells = _navigation_cells(driver)
        assert _navigation_dom_matches(expanded_cells, expected_children, decimal_places=2)
        observation["states"].append({"phase": "expanded", "rows": expanded_rows,
                                      "columns": expanded_columns, "responseRequestId": response["requestId"],
                                      "salesCells": [cell for cell in expanded_cells if "ventes_€_sum" in cell["colid"]]})
        driver.save_screenshot(str(directory / "expanded.png"))
        paths.append(str(directory / "expanded.png"))

        checkpoint = driver.execute_script("return performance.now()")
        if requirement == "hierarchy-expand.round-trip":
            _click_hierarchy_toggle(driver, parent_id)
        else:
            header = _wait_for_visible_workspace_element(driver, '[role="columnheader"][data-header-column-id="hierarchy"]')
            ActionChains(driver).context_click(header).perform()
            _selection_menu(driver, "Collapse All Rows").click()

        def collapsed(current):
            rows = _navigation_rows(current)
            labels = [label for _, label in rows]
            trace = current.execute_script("return window.__pivotNavigationTrace || []")
            return (labels.count("Éclair") == labels.count("München") == 1
                    and all(child not in labels for child in expected_children)
                    and _navigation_response_after(trace, checkpoint, ())
                    and _totals(current, decimal_places=2))

        wait.until(collapsed)
        final_rows = _navigation_rows(driver)
        final_labels = [label for _, label in final_rows]
        assert final_labels.count("Éclair") == final_labels.count("München") == 1
        assert all(child not in final_labels for child in ("東京", "القاهرة"))
        collapsed_response = _navigation_response_after(
            driver.execute_script("return window.__pivotNavigationTrace || []"), checkpoint, ())
        assert collapsed_response is not None
        collapsed_columns = _navigation_columns(driver)
        assert set(collapsed_columns) == set(initial_columns)
        collapsed_cells = _navigation_cells(driver)
        assert _navigation_dom_matches(collapsed_cells, (), decimal_places=2)
        observation["states"].append({"phase": "collapsed", "rows": final_rows,
                                      "columns": collapsed_columns,
                                      "responseRequestId": collapsed_response["requestId"],
                                      "salesCells": [cell for cell in collapsed_cells if "ventes_€_sum" in cell["colid"]]})
        result.update(result="passed", actionCompleted=True, fixtureTotalsVerified=True,
                      expandedChildrenVerified=True, collapsedChildrenVerified=True,
                      v1RequestsCorrelated=True,
                      subtotalNoDoubleCountVerified=requirement == "totals.subtotals")
    except Exception as error:
        frame = traceback.extract_tb(error.__traceback__)[-1]
        diagnostic = f"{type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}: {error}"
    finally:
        if driver is not None:
            try:
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                driver.save_screenshot(str(directory / "final.png"))
                paths.append(str(directory / "final.png"))
                observation["v1Trace"] = driver.execute_script("return window.__pivotNavigationTrace || []")
            except Exception as error:
                result["result"] = "blocked"
                diagnostic += f"; evidence collection: {type(error).__name__}"
            finally:
                driver.quit()
        observation.update(diagnostic=diagnostic, loadedAssetSha256=dict(result.get("loadedAssetSha256", {})),
                           result={k: v for k, v in result.items() if k != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {diagnostic}; evidence: {directory}"
