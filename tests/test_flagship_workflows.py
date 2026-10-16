"""Recovery qualification: installed browsers are opt-in and fail closed.

Large-delivery browser requirements remain absent until actual executor,
progress and selected-part output can be observed; shared cleanup coverage is
not a substitute for those browser results.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
from uuid import uuid4

import pytest


def test_visible_totals_use_display_format_without_losing_exact_protocol_oracle(monkeypatch):
    from tests import test_flagship_certification
    from tests.flagship_browser_support import _totals

    class Row:
        text = "Grand Total 200 10 -7"

    monkeypatch.setattr(test_flagship_certification, "_workspace_elements",
                        lambda driver, selector: [Row()] if selector == '[role="row"]' else [])
    assert _totals(object())
    Row.text = "Grand Total 200 10 -8"
    assert not _totals(object())


def test_workspace_control_scrolls_into_view_before_physical_click(monkeypatch):
    from tests import test_flagship_certification
    from tests.flagship_browser_support import _click_workspace_control

    events = []

    class Element:
        def click(self):
            events.append("click")

    class Driver:
        def execute_script(self, source, element):
            assert "scrollIntoView" in source
            events.append("scroll")

    element = Element()
    monkeypatch.setattr(test_flagship_certification, "_wait_for_visible_workspace_element",
                        lambda driver, selector: element)
    assert _click_workspace_control(Driver(), '[role="alert"] button') is element
    assert events == ["scroll", "click"]


def test_recovery_proof_rejects_a_success_requested_before_retry_click():
    from tests.flagship_browser_support import _post_retry_data_response

    trace = [
        {"phase": "request", "kind": "data", "requestId": "failed", "at": 1},
        {"phase": "response", "kind": "data", "requestId": "failed", "status": "error", "at": 2},
        {"phase": "request", "kind": "data", "requestId": "auto-refresh", "at": 9},
        {"phase": "response", "kind": "data", "requestId": "auto-refresh", "status": "data",
         "rawExpectedTotal": True, "at": 11},
    ]
    assert _post_retry_data_response(trace, "failed", 10) is None
    trace.extend([
        {"phase": "request", "kind": "data", "requestId": "clicked-retry", "at": 12},
        {"phase": "response", "kind": "data", "requestId": "clicked-retry", "status": "data",
         "rawExpectedTotal": True, "at": 13},
    ])
    assert _post_retry_data_response(trace, "failed", 10) == "clicked-retry"


def test_recovery_probe_reads_exact_raw_grand_total_from_real_v1_envelope(monkeypatch):
    from fastapi.testclient import TestClient
    from examples import flagship_demo_data
    from examples.flagship_runtime_server import app
    from tests.flagship_browser_support import _PROBE_SCRIPT

    monkeypatch.setenv("PIVOT_FLAGSHIP_PARITY_DATA", "1")
    flagship_demo_data.get_demo_adapter.cache_clear()
    config = flagship_demo_data.workspace_config("react")
    state_keys = ("rowFields", "colFields", "valConfigs", "filters", "sorting", "expanded",
                  "showRowTotals", "showColTotals", "showSubtotals")
    request = {
        "schemaVersion": 1, "kind": "data", "requestId": "probe-total",
        "table": config["table"], "sessionId": "probe", "clientInstance": "probe",
        "stateEpoch": 1, "windowSeq": 1,
        "state": {key: config[key] for key in state_keys},
        "payload": {"intent": "viewport", "start": 0, "end": 95},
    }
    try:
        response = TestClient(app).post("/v1/runtime", json=request)
        assert response.status_code == 200
        envelope = response.json()
    finally:
        flagship_demo_data.get_demo_adapter.cache_clear()
    grand = next(row for row in envelope["payload"]["data"] if row.get("_isTotal"))
    assert grand["__RowTotal__ventes_€_sum"] == 200
    assert grand["__RowTotal__unités_sum"] == 10
    assert grand["__RowTotal__écart_sum"] == -6.75

    script = f"""
      const fs = require('fs');
      const envelope = JSON.parse(fs.readFileSync(0, 'utf8'));
      global.window = {{fetch: () => Promise.resolve({{clone: () => ({{text: () => Promise.resolve(JSON.stringify(envelope))}})}})}};
      global.XMLHttpRequest = function() {{}};
      XMLHttpRequest.prototype.send = function() {{}};
      global.performance = {{now: () => 1}};
      eval({json.dumps(_PROBE_SCRIPT)});
      window.fetch('/v1/runtime').then(() => setImmediate(() =>
        process.stdout.write(JSON.stringify(window.__pivotRecoveryProbe))));
    """

    def observed(payload):
        result = subprocess.run(["node", "-e", script], input=json.dumps(payload), text=True,
                                capture_output=True, check=True)
        return json.loads(result.stdout)[0]["rawExpectedTotal"]

    assert observed(envelope) is True
    wrong = json.loads(json.dumps(envelope))
    wrong_grand = next(row for row in wrong["payload"]["data"] if row.get("_isTotal"))
    wrong_grand["__RowTotal__écart_sum"] = -6.5
    assert observed(wrong) is False


def test_guarded_demo_fault_yields_one_safe_v1_error_then_real_data(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from examples import flagship_demo_data
    from examples.flagship_runtime_server import app

    monkeypatch.setenv("PIVOT_FLAGSHIP_PARITY_DATA", "1")
    marker = tmp_path / "fail-next-data-request"
    monkeypatch.setenv("PIVOT_FLAGSHIP_TEST_FAULT_FILE", str(marker))
    monkeypatch.setenv("PIVOT_FLAGSHIP_EVIDENCE_ROOT", str(tmp_path))
    flagship_demo_data.get_demo_adapter.cache_clear()
    config = flagship_demo_data.workspace_config("react")
    state_keys = ("rowFields", "colFields", "valConfigs", "filters", "sorting", "expanded",
                  "showRowTotals", "showColTotals", "showSubtotals")
    request = {
        "schemaVersion": 1, "kind": "data", "requestId": "first-failed",
        "table": config["table"], "sessionId": "test", "clientInstance": "test",
        "stateEpoch": 1, "windowSeq": 1,
        "state": {key: config[key] for key in state_keys},
        "payload": {"intent": "viewport", "start": 0, "end": 95},
    }
    marker.write_text("one-shot", encoding="ascii")
    try:
        client = TestClient(app)
        failed = client.post("/v1/runtime", json=request)
        assert failed.status_code == 200
        error = failed.json()
        assert error["requestId"] == "first-failed"
        assert error["status"] == "error"
        assert "PIVOT_QUALIFICATION_PRIVATE_SENTINEL" not in failed.text
        assert not marker.exists()
        recovered = client.post("/v1/runtime", json={**request, "requestId": "fresh-retry", "windowSeq": 2})
        assert recovered.status_code == 200
        payload = recovered.json()
        assert payload["requestId"] == "fresh-retry"
        assert payload["status"] == "data"
        grand = next(row for row in payload["payload"]["data"] if row.get("_isTotal"))
        assert grand["__RowTotal__écart_sum"] == -6.75
    finally:
        flagship_demo_data.get_demo_adapter.cache_clear()


def test_demo_fault_switch_is_inert_without_parity_qualification(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from examples import flagship_demo_data
    from examples.flagship_runtime_server import app

    monkeypatch.delenv("PIVOT_FLAGSHIP_PARITY_DATA", raising=False)
    monkeypatch.setenv("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA", "1")
    marker = tmp_path / "normal-demo-marker"
    marker.write_text("one-shot", encoding="ascii")
    monkeypatch.setenv("PIVOT_FLAGSHIP_TEST_FAULT_FILE", str(marker))
    flagship_demo_data.get_demo_adapter.cache_clear()
    config = flagship_demo_data.workspace_config("react")
    try:
        response = TestClient(app).post("/v1/runtime", json={
            "schemaVersion": 1, "kind": "data", "requestId": "normal-demo",
            "table": config["table"], "sessionId": "normal-demo-session", "clientInstance": "normal-demo-client",
            "stateEpoch": 10, "windowSeq": 10,
            "state": {"rowFields": config["rowFields"], "colFields": config["colFields"],
                      "valConfigs": config["valConfigs"], "filters": config["filters"]},
            "payload": {"intent": "viewport", "start": 0, "end": 95},
        })
        assert response.status_code == 200
        assert response.json()["status"] == "data"
        assert marker.exists()
    finally:
        flagship_demo_data.get_demo_adapter.cache_clear()


@pytest.mark.parametrize("name,contents", [
    ("customer-data.txt", "one-shot"),
    ("fail-next-data-request", "customer bytes"),
])
def test_guarded_demo_fault_never_consumes_an_unrelated_env_path(monkeypatch, tmp_path, name, contents):
    from fastapi.testclient import TestClient
    from examples import flagship_demo_data
    from examples.flagship_runtime_server import app

    monkeypatch.setenv("PIVOT_FLAGSHIP_PARITY_DATA", "1")
    monkeypatch.setenv("PIVOT_FLAGSHIP_EVIDENCE_ROOT", str(tmp_path))
    unrelated = tmp_path / name
    unrelated.write_text(contents, encoding="ascii")
    monkeypatch.setenv("PIVOT_FLAGSHIP_TEST_FAULT_FILE", str(unrelated))
    flagship_demo_data.get_demo_adapter.cache_clear()
    config = flagship_demo_data.workspace_config("react")
    try:
        response = TestClient(app).post("/v1/runtime", json={
            "schemaVersion": 1, "kind": "data", "requestId": "unrelated-path",
            "table": config["table"], "sessionId": f"unrelated-{name}-{contents}",
            "clientInstance": f"unrelated-{name}-{contents}",
            "stateEpoch": 1, "windowSeq": 1,
            "state": {"rowFields": config["rowFields"], "colFields": config["colFields"],
                      "valConfigs": config["valConfigs"], "filters": config["filters"]},
            "payload": {"intent": "viewport", "start": 0, "end": 95},
        })
        assert response.status_code == 200
        assert response.json()["status"] == "data"
        assert unrelated.read_text(encoding="ascii") == contents
    finally:
        flagship_demo_data.get_demo_adapter.cache_clear()


@pytest.fixture(scope="session")
def installed_workflow_hosts():
    """Opt-in current-commit installed hosts for standalone workflow runs.

    The release orchestrator supplies its own binding and hosts; this fixture
    only creates them when a developer explicitly requests a standalone run.
    """
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        yield
        return
    if os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        yield
        return
    if os.environ.get("PIVOT_FLAGSHIP_BUILD_WORKFLOW_ARTIFACTS") != "1":
        pytest.fail("Installed artifact binding required for workflow browsers")
    from scripts.flagship_artifacts import (
        ArtifactBinding, _create_python_environment, build_artifacts,
        cleanup_python_environment, start_installed_hosts,
    )

    root = Path(__file__).resolve().parents[1]
    evidence = Path(tempfile.mkdtemp(prefix="pf-workflow-"))
    reuse = os.environ.get("PIVOT_FLAGSHIP_REUSE_RELEASE_DIR")
    if reuse:
        release = Path(reuse).resolve(strict=True)
        previous_binding = json.loads((release / "artifact-binding.json").read_text(encoding="utf-8"))
        wheel, = (release / "release-artifacts" / "wheels").glob("*.whl")
        npm_tarball, = (release / "release-artifacts").glob("*.tgz")
        assert previous_binding["sourceCommit"] == subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(), "Reused artifact is not current commit"
        for name, path in (("wheel", wheel), ("npm_tarball", npm_tarball)):
            assert hashlib.sha256(path.read_bytes()).hexdigest() == previous_binding["sha256"][name]
        record = release / "release-artifacts" / "python-env.location.json"
        python = _create_python_environment(release / "release-artifacts" / "python-env")
        subprocess.run([str(python), "-m", "pip", "--isolated", "install", "--force-reinstall",
                        f"{wheel}[streamlit,api]"], cwd=release, check=True, capture_output=True)
        binding = ArtifactBinding(
            source_commit=previous_binding["sourceCommit"], sha256=previous_binding["sha256"],
            versions=previous_binding["versions"],
            expected_loaded_assets=previous_binding["expectedLoadedAssets"],
            paths={"python": python, "environment": python.parent.parent, "environment_record": record,
                   "app": release / "release-artifacts" / "installed-app"},
        )
    else:
        wheelhouse = os.environ.get("PIVOT_FLAGSHIP_BUILD_WHEELHOUSE")
        binding = build_artifacts(root, evidence / "release-artifacts",
                                  build_wheelhouse=Path(wheelhouse) if wheelhouse else None)
    run_id = uuid4().hex
    selected = {
        "PIVOT_FLAGSHIP_ARTIFACT_BINDING": str(evidence / "artifact-binding.json"),
        "PIVOT_FLAGSHIP_RUN_ID": run_id,
        "PIVOT_FLAGSHIP_CASE_RESULTS": str(evidence / "private" / "runs" / run_id / "product-case-results.json"),
        "PIVOT_FLAGSHIP_EVIDENCE_ROOT": str(evidence / "private" / "runs" / run_id),
        "PIVOT_FLAGSHIP_TEST_FAULT_FILE": str(evidence / "private" / "runs" / run_id / "fail-next-data-request"),
    }
    previous = {key: os.environ.get(key) for key in (*selected, *(f"PIVOT_FLAGSHIP_{host.upper()}_URL" for host in ("dash", "react", "streamlit")))}
    try:
        Path(selected["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]).mkdir(parents=True, exist_ok=True)
        os.environ.update(selected)
        with start_installed_hosts(binding, evidence / "installed-hosts") as urls:
            for host, url in urls.items():
                selected[f"PIVOT_FLAGSHIP_{host.upper()}_URL"] = url
            (evidence / "artifact-binding.json").write_text(json.dumps({
                "runId": run_id, "sourceCommit": binding.source_commit,
                "sha256": binding.sha256, "versions": binding.versions,
                "expectedLoadedAssets": binding.expected_loaded_assets,
                "hostOrigins": binding.host_origins,
            }, indent=2) + "\n", encoding="utf-8")
            os.environ.update(selected)
            print(f"Installed workflow evidence: {evidence}")
            yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        cleanup_python_environment(binding)


def test_cancellation_assertion_rejects_early_reservation_or_slot_release():
    from tests.flagship_browser_support import assert_cancellation_lifetime

    for broken in ({"reservedBytes": 0, "activeSlots": 1},
                   {"reservedBytes": 64, "activeSlots": 0}):
        samples = [
            {"executorRunning": True, "cancelRequested": False, "reservedBytes": 64, "activeSlots": 1},
            {"executorRunning": True, "cancelRequested": True, **broken},
            {"executorRunning": False, "cancelRequested": True, "reservedBytes": 0, "activeSlots": 0},
        ]
        with pytest.raises(AssertionError, match="executor"):
            assert_cancellation_lifetime(samples)


@pytest.mark.parametrize("samples", [[], [
    {"executorRunning": False, "cancelRequested": True, "reservedBytes": 0, "activeSlots": 0},
]])
def test_cancellation_assertion_requires_inflight_and_completed_observations(samples):
    from tests.flagship_browser_support import assert_cancellation_lifetime

    with pytest.raises(AssertionError):
        assert_cancellation_lifetime(samples)


@pytest.mark.asyncio
async def test_cancelled_executor_retains_real_admission_until_cleanup():
    """Fails if lifecycle cancellation frees bytes or query slots prematurely."""
    from tests.flagship_browser_support import assert_cancellation_lifetime
    from pivot_engine.backends.execution import CancellationUnavailableError
    from pivot_engine.backends.ibis_backend import IbisBackend
    from pivot_engine.resource_governor import ResourceExhaustedError, ResourceGovernor, ResourceLimits
    from pivot_engine.runtime import PivotRequestContext
    from pivot_engine.runtime.lifecycle import RuntimeRequestLifecycle

    governor = ResourceGovernor(ResourceLimits.for_testing(max_queries=1, query_bytes=64), rss_bytes_getter=lambda: 0)
    lifecycle = RuntimeRequestLifecycle(resource_governor=governor)
    context = PivotRequestContext(table="sales")
    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    class Connection:
        name = "unsupported"

    class Expression:
        def execute(self):
            started.set()
            try:
                assert release.wait(5), "executor was not released"
                return [{"value": 1}]
            finally:
                finished.set()

    backend = IbisBackend(connection=Connection())
    task = asyncio.create_task(lifecycle.run_backend_operation(
        context, "data", lambda: backend.execute_async({"ibis_expr": Expression()}, return_arrow=False),
        reservation_bytes=64,
    ))
    samples = []

    def sample(cancelled):
        state = governor.snapshot()
        samples.append({"executorRunning": started.is_set() and not finished.is_set(),
                        "cancelRequested": cancelled,
                        "reservedBytes": state["reserved_bytes_by_kind"]["query"],
                        "activeSlots": state["active_slots"]["query"]})

    try:
        assert await asyncio.to_thread(started.wait, 2)
        sample(False)
        task.cancel()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), 0.05)
        sample(True)
        with pytest.raises(ResourceExhaustedError):
            await lifecycle.run_backend_operation(context, "data", lambda: None)
    finally:
        release.set()
        try:
            with pytest.raises(CancellationUnavailableError):
                await asyncio.wait_for(task, 2)
        finally:
            backend.close()
    sample(True)
    assert_cancellation_lifetime(samples)
    assert await lifecycle.run_backend_operation(context, "data", lambda: "retry") == "retry"


def test_record_case_is_bound_and_drops_raw_observations(tmp_path, monkeypatch):
    from tests.flagship_browser_support import record_case

    binding = {"runId": "workflow-test", "sourceCommit": "a" * 40,
               "sha256": {"wheel": "b" * 64}, "expectedLoadedAssets": {}, "versions": {}}
    binding_file = tmp_path / "binding.json"
    binding_file.write_text(json.dumps(binding), encoding="utf-8")
    destination = tmp_path / "results.json"
    monkeypatch.setenv("PIVOT_FLAGSHIP_ARTIFACT_BINDING", str(binding_file))
    monkeypatch.setenv("PIVOT_FLAGSHIP_CASE_RESULTS", str(destination))
    monkeypatch.setenv("PIVOT_FLAGSHIP_RUN_ID", "workflow-test")
    evidence = tmp_path / "private" / "runs" / "workflow-test" / "dash"
    evidence.mkdir(parents=True)
    record_case("runtime-error.visible", "dash", {
        "result": "failed", "browser": "chrome 1", "visibleError": False,
        "token": "SECRET", "rows": ["RAW_CELL"], "exception": "PRIVATE_EXCEPTION",
    }, evidence)
    raw = destination.read_text(encoding="utf-8")
    assert all(secret not in raw for secret in ("SECRET", "RAW_CELL", "PRIVATE_EXCEPTION"))
    record = json.loads(raw)["results"][0]
    assert record["test_id"] == "browser.runtime-error.visible"
    assert record["source_commit"] == "a" * 40
    assert record["run_id"] == "workflow-test"
    assert json.loads(record["observed"])["visibleError"] is False


def test_record_case_cannot_pass_without_browser_asset_evidence(tmp_path):
    from tests.flagship_browser_support import record_case

    with pytest.raises((AssertionError, ValueError), match="evidence|binding"):
        record_case("runtime-error.visible", "dash", {"result": "passed", "browser": "chrome 1"}, tmp_path)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_runtime_error_recovery(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_runtime_recovery

    qualify_runtime_recovery(host)


@pytest.mark.parametrize("text", ["100\t2\n50\t3", "100.25\t2\t2\n49.75\t3\t3",
                                      "49.75\t3\n100.25\t2", "100.25\t2\n49.75\t3\n"])
def test_selected_clipboard_oracle_rejects_rounding_duplicates_order_and_extra_rows(text):
    from tests.flagship_browser_support import assert_selected_clipboard

    with pytest.raises(AssertionError):
        assert_selected_clipboard(text, [["100.25", "2"], ["49.75", "3"]])


def test_selected_clipboard_oracle_checks_exact_bytes_and_parsed_cells():
    from tests.flagship_browser_support import assert_selected_clipboard

    assert assert_selected_clipboard("100.25\t2\n49.75\t3", [["100.25", "2"], ["49.75", "3"]]) == [
        ["100.25", "2"], ["49.75", "3"]]


def test_clipboard_monitor_rejects_a_write_after_the_old_single_read_window():
    import time
    from tests.flagship_browser_support import assert_clipboard_stable

    started = time.monotonic()
    def delayed_clipboard():
        return "checkpoint" if time.monotonic() - started < 0.5 else "stale selection"

    with pytest.raises(AssertionError, match="changed"):
        assert_clipboard_stable(delayed_clipboard, "checkpoint", timeout=0.75, poll=0.02)


def test_clipboard_monitor_observes_the_whole_interval():
    from tests.flagship_browser_support import assert_clipboard_stable

    result = assert_clipboard_stable(lambda: "checkpoint", "checkpoint", timeout=0.08, poll=0.02)
    assert result["durationSeconds"] >= 0.08
    assert result["sampleCount"] >= 3


@pytest.mark.flagship
def test_number_format_helper_uses_dialog_ok_and_commits_precision(tmp_path):
    """Exercise the helper against real DOM events; no product behavior is mocked."""
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable Chrome helper regression explicitly")
    from urllib.parse import quote
    from selenium import webdriver
    from tests.flagship_browser_support import _selection_number_format
    from tests.test_flagship_certification import _qualification_chrome_options

    html = """<button data-testid="pivot-toolbar-format">Format</button>
      <button title="Open number format options" onclick="document.querySelector('[role=dialog]').hidden=false">Number Format</button>
      <div role="dialog" aria-label="Format Cells" hidden><input type="number" value="0">
      <button onclick="document.querySelector('output').textContent=document.querySelector('input').value; this.parentElement.remove()">OK</button></div>
      <output></output>"""
    driver = webdriver.Chrome(options=_qualification_chrome_options(tmp_path))
    try:
        driver.get("data:text/html," + quote(html))
        _selection_number_format(driver)
        assert driver.find_element("css selector", "output").text == "2"
        assert not driver.find_elements("css selector", '[role="dialog"]')
    finally:
        driver.quit()


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("requirement", ["cell-selection.range-clear", "copy-selection.logical-cells",
                                         "copy-selection.display-order", "copy-selection.pinned-columns"])
def test_flagship_selected_clipboard(host, requirement, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_selected_clipboard

    qualify_selected_clipboard(host, requirement)
