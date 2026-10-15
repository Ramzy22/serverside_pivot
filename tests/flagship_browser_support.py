"""Private browser evidence and fail-closed helpers for product workflows.

Runtime error cases arm a one-shot, qualification-only demo-adapter fault so
all hosts receive the same genuine server V1 response. Network transport loss
is a separate host-integration concern, not evidence of a V1 error response.
"""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import traceback
from typing import Mapping, Sequence

from tests.flagship_contract import HOSTS, RESULTS, load_requirements


ROOT = Path(__file__).resolve().parents[1]
_SAFE_OBSERVATIONS = frozenset({
    "visibleError", "loadingCleared", "failedRequestCorrelated", "retryCorrelated",
    "fixtureTotalsVerified", "staleCellsAbsent", "newDownloads", "networkFailureCount",
    "infrastructureUnavailable", "actionCompleted", "lateResponseAbsent",
})


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


def _envelopes(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return
    if isinstance(value, dict):
        if value.get("schemaVersion") == 1 and value.get("requestId") and value.get("kind"):
            yield value
        else:
            for child in value.values():
                yield from _envelopes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _envelopes(child)


def _network(driver):
    from tests.test_flagship_certification import _verify_release_asset_batch

    entries = driver.get_log("performance")
    _verify_release_asset_batch(driver, entries)
    return [json.loads(entry["message"])["message"] for entry in entries]


def _correlated_failed_request(events):
    failed = {event["params"]["requestId"] for event in events if event["method"] == "Network.loadingFailed"
              and event["params"].get("blockedReason") == "inspector"}
    return next((envelope["requestId"] for event in events
                 if event["method"] == "Network.requestWillBeSent" and event["params"]["requestId"] in failed
                 for envelope in _envelopes(event["params"]["request"].get("postData", ""))
                 if envelope["kind"] == "data"), None)


def _visible_error(driver):
    from tests.test_flagship_certification import _workspace_elements

    return [element for element in _workspace_elements(driver, '[role="alert"]')
            if element.is_displayed() and "We could not load the grid data" in element.text
            and any(button.is_displayed() and button.text == "Retry"
                    for button in element.find_elements("css selector", "button"))]


def _totals(driver):
    from tests.test_flagship_certification import _workspace_elements

    rows = [row for row in _workspace_elements(driver, '[role="row"]') if "Grand Total" in row.text]
    for row in rows:
        # Independent literal oracle: four Included fixture rows total these
        # three measures. Strip display punctuation but retain sign/decimals.
        values = [float(token.replace(",", "")) for token in re.findall(r"-?\d[\d,]*(?:\.\d+)?", row.text)]
        # The default visible number format rounds the raw -6.75 to -7.
        # The V1 response is checked separately for the exact raw total.
        if all(expected in values for expected in (200.0, 10.0, -7.0)):
            return True
    return False


def _click_workspace_control(driver, selector):
    """Bring an off-screen Streamlit shadow control into view before a real click."""
    from tests.test_flagship_certification import _wait_for_visible_workspace_element

    element = _wait_for_visible_workspace_element(driver, selector)
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
    element.click()
    return element


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
            result="passed" if visible and loading_cleared and failed_id else "failed",
            visibleError=visible, loadingCleared=loading_cleared, failedRequestCorrelated=bool(failed_id), newDownloads=0,
        )
        driver.save_screenshot(str(directory / "failed.png"))
        retry = _wait_for_visible_workspace_element(driver, '[role="alert"] button')
        assert retry.text == "Retry", "Recovery did not use the visible Retry action"
        _click_workspace_control(driver, '[role="alert"] button')

        def recovered(current):
            nonlocal trace
            trace = current.execute_script("return window.__pivotRecoveryProbe || []")
            responses = {entry["requestId"] for entry in trace if entry["phase"] == "response"
                         and entry["kind"] == "data" and entry["status"] == "data"
                         and entry["rawExpectedTotal"]}
            requests = [entry for entry in trace if entry["phase"] == "request" and entry["kind"] == "data"]
            failed_at = next((entry["at"] for entry in requests if entry["requestId"] == failed_id), None)
            return failed_at is not None and any(entry["at"] > failed_at and entry["requestId"] != failed_id
                                                 and entry["requestId"] in responses for entry in requests) and _totals(current)

        wait.until(recovered)
        snapshot = [cell.text for cell in _workspace_elements(driver, '[role="gridcell"]')]
        # The failed V1 request is terminal: confirm no late successful
        # response for it, and unchanged fixture cells after a fresh event turn.
        driver.execute_async_script("const done=arguments[0]; requestAnimationFrame(() => requestAnimationFrame(done));")
        stable = snapshot == [cell.text for cell in _workspace_elements(driver, '[role="gridcell"]')]
        events.extend(_network(driver))
        late_absent = not any(entry["phase"] == "response" and entry["status"] == "data"
                              and entry["requestId"] == failed_id for entry in trace)
        results["runtime-error.recovery"].update(
            result="passed" if stable and late_absent and not _visible_error(driver) else "failed",
            retryCorrelated=True, fixtureTotalsVerified=True, staleCellsAbsent=stable,
            lateResponseAbsent=late_absent, newDownloads=len(set(downloads.iterdir()) - before),
        )
        assert set(downloads.iterdir()) == before, "Recovery unexpectedly created a download"
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
