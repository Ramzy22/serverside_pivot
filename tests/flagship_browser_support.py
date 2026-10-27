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
    labels = {"Q1_unités_sum": "Unités (Sum)", "Q1_ventes_€_sum": "Ventes € (Sum)"}
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
    from tests.test_flagship_certification import _workspace_elements

    return [{"id": cell.get_attribute("data-rowid"), "label": cell.text.strip()}
            for cell in _workspace_elements(driver, '[role="gridcell"][data-colid="hierarchy"]')
            if cell.is_displayed() and cell.text.strip() in ("Éclair", "München")]


def _selection_column_order(driver):
    """Read actual body geometry, excluding duplicate/group header containers."""
    from tests.test_flagship_certification import _workspace_elements

    rows = _selection_rows(driver)
    if not rows:
        return []  # A structural request may briefly clear the rendered rows.
    first_row = rows[0]["id"]
    cells = _workspace_elements(driver, f'[role="gridcell"][data-rowid="{first_row}"]')
    positions = [(driver.execute_script("return arguments[0].getBoundingClientRect().left", cell),
                  cell.get_attribute("data-colid")) for cell in cells if cell.is_displayed()]
    return [column for _, column in sorted(positions)]


def _reorder_selection_column(driver, column, direction, first, second, observation):
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _wait_for_visible_workspace_element

    rows_before = _selection_rows(driver)
    order_before = _selection_column_order(driver)
    header = _wait_for_visible_workspace_element(driver, f'[role="columnheader"][data-header-column-id="{column}"]')
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


def _copy_selected(driver, cell, expected, observation, *, menu_label="Copy Selection"):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _clipboard_text

    sentinel = "qualification clipboard checkpoint"
    assert driver.execute_async_script(
        "const done=arguments[arguments.length-1]; navigator.clipboard.writeText(arguments[0]).then(()=>done(true),()=>done(false));",
        sentinel)
    ActionChains(driver).context_click(cell).perform()
    menu_item = _selection_menu(driver, menu_label)
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
    assert events[-1]["trusted"] is True and events[-1]["targetRole"] == "grid", "Copy key missed the workspace grid"
    observation["nativeCopyKeys"] = events


def qualify_selected_clipboard(host: str, requirement: str) -> None:
    """Physical selection/menu/keyboard actions against fresh installed hosts.

    A clipboard read is an OS/browser read. No copy event, clipboard method,
    React state, runtime payload, or product handler is replaced by this case.
    """
    import pytest

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
        initial_order = _selection_column_order(driver)
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
                             "copy-selection.formatted-values"):
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
        rows = _selection_rows(driver)
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
            boundary_order = _selection_column_order(driver)
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
        order = _selection_column_order(driver)
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
        elif requirement == "copy-selection.formatted-values":
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
        if requirement == "copy-selection.headers":
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
            ActionChains(driver).context_click(header).perform()
            _geometry_pin_submenu(driver, None)
            _selection_menu(driver, "Pin Left").click()
            observation["actions"].append("context menu Pin > Pin Left on Q2 sales")
            wait.until(lambda current: _geometry_snapshot(current, sales)["pinnedLeft"] == [sales])
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
            geometry_wait = WebDriverWait(driver, 12, ignored_exceptions=(AssertionError,))
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


def _visible_error(driver):
    from tests.test_flagship_certification import _workspace_elements

    return [element for element in _workspace_elements(driver, '[role="alert"]')
            if element.is_displayed() and "We could not load the grid data" in element.text
            and any(button.is_displayed() and button.text == "Retry"
                    for button in element.find_elements("css selector", "button"))]


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
  let armed = false, releaseOld = null;
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
    trace.push({phase:'request', requestId, term, at:performance.now(), total:null});
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
      trace.push({phase:'response', requestId, term, at:performance.now(), total, ...details});
      return response;
    });
  };
  window.__pivotSupersession = {trace, arm:() => {armed = true}, release:() => releaseOld?.()};
}
"""


_SUPERSESSION_OBSERVE_SCRIPT = r"""
const done = arguments[arguments.length - 1];
const capture = () => {
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
        driver.execute_script("window.__pivotSupersession.arm()")
        search = _wait_for_visible_workspace_element(driver, '[data-testid="pivot-global-search"]')
        search.send_keys("Éclair")
        wait.until(lambda current: any(item["phase"] == "ready" and item["term"] == "Éclair"
                                       for item in current.execute_script("return window.__pivotSupersession.trace")))
        from selenium.webdriver.common.keys import Keys
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
    if (value.schemaVersion === 1 && value.kind === 'data' && typeof value.requestId === 'string') {
      const rows = Array.isArray(value.payload?.data) ? value.payload.data : [];
      trace.push({phase, at: performance.now(), requestId: value.requestId,
        schemaVersion: value.schemaVersion, status: value.status || null,
        globalFilter: value.state?.filters?.global ?? null,
        rows: rows.map(row => ({id: row?._id, path: row?._path,
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
      response.clone().text().then(body => visit(body, 'response')).catch(() => {});
      return response;
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
            parent = _selection_cell(driver, parent_id, "hierarchy")
            checkpoint = driver.execute_script("return performance.now()")
            parent.find_element("css selector", "button").click()
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
        if requirement == "hierarchy-expand-all.round-trip":
            assert labels.index("München") < labels.index("القاهرة")
            assert "Éclair" not in labels[labels.index("München") + 1:labels.index("القاهرة")]
        response = _navigation_response_after(
            driver.execute_script("return window.__pivotNavigationTrace || []"), checkpoint, expected_children)
        assert response is not None
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
            _selection_cell(driver, parent_id, "hierarchy").find_element("css selector", "button").click()
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
                           result={k: v for k, v in result.items() if k != "loadedAssetSha256"})
        evidence = directory / "observation.json"
        evidence.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        result["evidence_paths"] = paths
        record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {diagnostic}; evidence: {directory}"
