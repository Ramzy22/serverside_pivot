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


def test_export_qualification_fixture_is_flat_and_does_not_replace_parity(monkeypatch):
    from examples import flagship_demo_data as demo

    monkeypatch.setenv("PIVOT_FLAGSHIP_PARITY_DATA", "1")
    monkeypatch.setenv("PIVOT_FLAGSHIP_EXPORT_JOB_DATA", "1")
    config = demo.workspace_config("react")
    assert config.get("viewState", {}).get("layoutMode") == "tabular"
    from dash_tanstack_pivot import DashTanstackPivot
    assert DashTanstackPivot(**config).id == "flagship-react-pivot"
    assert config["colFields"] == [] and config["showSubtotals"] is False
    table = demo.build_demo_table()
    assert table.num_rows == 512
    assert table.to_pylist()[0] == {"region": "R0000 \u6771\u4eac", "sales": 0.25}
    monkeypatch.delenv("PIVOT_FLAGSHIP_EXPORT_JOB_DATA")
    assert demo.workspace_config("react")["colFields"] == ["period"]


def test_export_fixture_samples_a_snapshot_while_writer_paths_change(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from examples import flagship_demo_data as demo

    assert hasattr(demo, "_existing_export_paths"), "Private sampler must snapshot mutable writer paths"
    first, late = tmp_path / "first", tmp_path / "late"
    first.touch()
    late.touch()
    control = SimpleNamespace(paths={str(first)})
    def writer_changes_paths(path):
        control.paths.add(str(late))
        return Path(path)
    monkeypatch.setattr(demo, "Path", writer_changes_paths)
    assert demo._existing_export_paths(control) == [str(first)]


def test_export_fixture_copies_writer_path_set_before_iteration(tmp_path):
    from types import SimpleNamespace
    from examples.flagship_demo_data import _existing_export_paths

    path = tmp_path / "writer.csv"
    path.touch()
    class ConcurrentPaths(set):
        def __iter__(self):
            raise RuntimeError("set changed size during iteration")
        def copy(self):
            return {str(path)}
    assert _existing_export_paths(SimpleNamespace(paths=ConcurrentPaths())) == [str(path)]


def test_export_fixture_holds_only_after_real_measured_progress():
    from types import SimpleNamespace
    from examples.flagship_demo_data import _hold_measured_progress

    observed = []
    control = SimpleNamespace(progress=lambda rows, size: observed.append((rows, size)))
    _hold_measured_progress(control, lambda: observed.append(("paused", tuple(observed))))
    control.progress(128, 2048)
    assert observed == [(128, 2048), ("paused", ((128, 2048),))]
    control.progress(256, 4096)
    assert observed[-1] == (256, 4096)
    assert len(observed) == 3


def test_export_progress_proof_rejects_fabricated_or_uncorrelated_counters():
    from tests import flagship_browser_support as support

    assert hasattr(support, "assert_export_job_trace"), "Missing correlated export job proof"
    trace = [
        {"direction": "request", "requestId": "start", "payload": {"action": "start"}},
        {"direction": "response", "requestId": "start", "payload": {
            "jobId": "server-job", "phase": "running", "rowsWritten": 0, "bytesWritten": 0, "totalRows": None}},
        {"direction": "request", "requestId": "poll", "payload": {"action": "status", "jobId": "server-job"}},
        {"direction": "response", "requestId": "poll", "payload": {
            "jobId": "server-job", "phase": "running", "rowsWritten": 128, "bytesWritten": 1024, "totalRows": None}},
    ]
    assert support.assert_export_job_trace(trace) == "server-job"
    for broken in (
        [trace[0], trace[1], trace[2], {**trace[3], "requestId": "unrelated"}],
        [*trace, {**trace[3], "payload": {**trace[3]["payload"], "rowsWritten": 1}}],
    ):
        with pytest.raises(AssertionError):
            support.assert_export_job_trace(broken)


def test_cancel_trace_allows_zero_writer_progress_before_native_query_stops():
    from tests.flagship_browser_support import assert_export_job_trace

    trace = [
        {"direction": "request", "requestId": "start", "payload": {"action": "start"}},
        {"direction": "response", "requestId": "start", "payload": {
            "jobId": "job", "phase": "running", "rowsWritten": 0, "bytesWritten": 0}},
        {"direction": "request", "requestId": "cancel", "payload": {"action": "cancel", "jobId": "job"}},
        {"direction": "response", "requestId": "cancel", "payload": {
            "jobId": "job", "phase": "cancelling", "rowsWritten": 0, "bytesWritten": 0}},
        {"direction": "request", "requestId": "poll", "payload": {"action": "status", "jobId": "job"}},
        {"direction": "response", "requestId": "poll", "payload": {
            "jobId": "job", "phase": "cancelled", "rowsWritten": 0, "bytesWritten": 0}},
    ]
    assert assert_export_job_trace(trace, require_progress=False) == "job"
    with pytest.raises(AssertionError, match="No measured export progress"):
        assert_export_job_trace(trace)


def test_export_cancel_ack_requires_server_terminal_and_cleanup_before_visible_cancelled():
    from tests import flagship_browser_support as support

    assert hasattr(support, "assert_export_cancel_ack"), "Missing cleanup/acknowledgement ordering proof"
    cleanup = {"releasedAtMs": 1000, "taskDone": True, "nativeRunning": 0,
               "activeSlots": 0, "reservedBytes": 0, "temporaryFiles": []}
    trace = [
        {"direction": "request", "requestId": "cancel", "payload": {"action": "cancel", "jobId": "job"}},
        {"direction": "response", "requestId": "cancel", "atMs": 1010,
         "payload": {"jobId": "job", "phase": "cancelled"}},
    ]
    ui = [{"phase": "cancelling", "atMs": 900}, {"phase": "cancelled", "atMs": 1020}]
    support.assert_export_cancel_ack(trace, ui, cleanup, "job")
    for broken_trace, broken_ui, broken_cleanup in (
        (trace[:1], ui, cleanup),
        (trace, [*ui[:1], {"phase": "cancelled", "atMs": 999}], cleanup),
        (trace, ui, {**cleanup, "nativeRunning": 1}),
        (trace, ui, {**cleanup, "taskDone": False}),
        (trace, ui, {**cleanup, "activeSlots": 1}),
        ([trace[0], {**trace[1], "atMs": 999}], ui, cleanup),
    ):
        with pytest.raises(AssertionError):
            support.assert_export_cancel_ack(broken_trace, broken_ui, broken_cleanup, "job")


def test_installed_export_runner_selects_workflows_and_rejects_missing_cases(monkeypatch, tmp_path):
    from scripts import certify_flagship as runner

    commands = []
    class Completed:
        returncode = 0
    monkeypatch.setattr(runner.subprocess, "run", lambda command, **kwargs: commands.append(command) or Completed())
    runner._run_browser_suite(tmp_path, {"PIVOT_FLAGSHIP_EXPORT_JOB_DATA": "1"})
    assert "tests/test_flagship_workflows.py" in commands[0]
    assert "tests/test_flagship_certification.py" not in commands[0]
    report = tmp_path / "junit.xml"
    names = [f"test_flagship_export_jobs[{requirement}-{host}]"
             for requirement in ("large-delivery.progress", "large-delivery.cancel-retry", "journey-recovery.cancel-new-work")
             for host in ("dash", "react", "streamlit")]
    cases = [f'<testcase name="{name}"/>' for name in names]
    report.write_text('<testsuite tests="9">' + ''.join(cases) + '</testsuite>')
    assert runner._installed_browser_report_passed(report, export_jobs=True)
    report.write_text('<testsuite tests="9">' + ''.join(cases[:-1]) + '</testsuite>')
    assert not runner._installed_browser_report_passed(report, export_jobs=True)
    report.write_text('<testsuite tests="9">' + ''.join(cases[:-1]) +
                      f'<testcase name="{names[-1]}"><skipped/></testcase></testsuite>')
    assert not runner._installed_browser_report_passed(report, export_jobs=True)


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


def test_visible_totals_use_exact_two_decimal_fixture_when_requested(monkeypatch):
    from tests import test_flagship_certification
    from tests.flagship_browser_support import _totals

    class Row:
        text = "Grand Total 200.00 10.00 -6.75"

    monkeypatch.setattr(test_flagship_certification, "_workspace_elements",
                        lambda driver, selector: [Row()] if selector == '[role="row"]' else [])
    assert _totals(object(), decimal_places=2)
    Row.text = "Grand Total 200.00 10.00 -7.00"
    assert not _totals(object(), decimal_places=2)


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


def test_superseded_response_oracle_requires_late_old_success_without_visible_rollback():
    from tests.flagship_browser_support import assert_superseded_response

    trace = [
        {"phase": "request", "requestId": "old", "at": 1, "total": None},
        {"phase": "request", "requestId": "new", "at": 2, "total": None},
        {"phase": "response", "requestId": "new", "at": 3, "total": 79.75},
        {"phase": "response", "requestId": "old", "at": 4, "total": 200.0},
    ]
    assert assert_superseded_response(trace, "old", "new", 79.75) is True
    with pytest.raises(AssertionError):
        assert_superseded_response(trace, "old", "new", 200.0)
    with pytest.raises(AssertionError):
        assert_superseded_response(trace[:3], "old", "new", 79.75)
    with pytest.raises(AssertionError):
        assert_superseded_response([*trace[:2], trace[3], trace[2]], "old", "new", 79.75)


def test_supersession_ids_ignore_metadata_responses_without_rows():
    from tests.flagship_browser_support import supersession_ids

    trace = [
        {"phase": "ready", "requestId": "old-rows", "term": "Éclair", "total": 120.25},
        {"phase": "response", "requestId": "new-metadata", "term": "München", "total": None},
        {"phase": "response", "requestId": "new-rows", "term": "München", "total": 79.75},
    ]
    assert supersession_ids(trace) == ("old-rows", "new-rows")


def test_payload_network_evidence_records_delivery_without_exposing_token():
    from tests.flagship_scroll_support import _payload_network_events

    events = [
        {"method": "Network.requestWillBeSent", "params": {"requestId": "1",
            "request": {"url": "http://host/_dash_tanstack_pivot/payload/private-token"}}},
        {"method": "Network.responseReceived", "params": {"requestId": "1",
            "response": {"url": "http://host/_dash_tanstack_pivot/payload/private-token", "status": 200}}},
        {"method": "Network.responseReceived", "params": {"requestId": "2",
            "response": {"url": "http://host/health", "status": 200}}},
    ]
    observed = _payload_network_events(events)
    assert [entry["phase"] for entry in observed] == ["Network.requestWillBeSent", "Network.responseReceived"]
    assert observed[1]["status"] == 200
    assert "private-token" not in str(observed)


def test_supersession_observation_rejects_non_sales_cell_rollback():
    from tests.flagship_browser_support import (assert_stable_visible_snapshots,
                                                supersession_fixture_matches)

    newer = [
        {"rowid": "München", "colid": "__RowTotal__ventes_€_sum", "text": "80"},
        {"rowid": "München", "colid": "__RowTotal__unités_sum", "text": "7"},
    ]
    rolled_back = [newer[0], {**newer[1], "text": "3"}]
    assert assert_stable_visible_snapshots(newer, [newer, newer])
    with pytest.raises(AssertionError, match="visible cells changed"):
        assert_stable_visible_snapshots(newer, [newer, rolled_back, newer])
    assert not supersession_fixture_matches(rolled_back)


def test_superseded_response_has_an_installed_browser_case_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    requirements = load_requirements(Path(__file__).parent / "flagship_requirements.json")
    plan = _product_browser_plan(requirements)
    for host in ("dash", "react", "streamlit"):
        assert plan[("runtime-error.superseded-response", host,
                     "browser.runtime-error.superseded-response")] == (
            f"tests/test_flagship_workflows.py::test_flagship_superseded_response[{host}]"
        )


def test_initial_workspace_oracle_requires_all_fixture_measures():
    from tests.flagship_browser_support import initial_workspace_rows_match, initial_workspace_expected_cells

    rows = [
        {"id": "Éclair", "q1": 100.25, "q2": 20.0, "total": 120.25,
         "q1_units": 2, "q2_units": 1, "total_units": 3,
         "q1_variance": -12.5, "q2_variance": 2.5, "total_variance": -10.0},
        {"id": "München", "q1": 49.75, "q2": 30.0, "total": 79.75,
         "q1_units": 3, "q2_units": 4, "total_units": 7,
         "q1_variance": 4.25, "q2_variance": -1.0, "total_variance": 3.25},
        {"id": "Grand Total", "q1": 150.0, "q2": 50.0, "total": 200.0,
         "q1_units": 5, "q2_units": 5, "total_units": 10,
         "q1_variance": -8.25, "q2_variance": 1.5, "total_variance": -6.75},
    ]
    assert initial_workspace_rows_match(rows)
    assert initial_workspace_rows_match([{"path": "__color_scale_stats__"}, *rows])
    assert len(initial_workspace_expected_cells([{"path": "__color_scale_stats__"}, *rows])) == 27
    assert not initial_workspace_rows_match([*rows, {**rows[0], "id": "São Paulo"}])
    assert not initial_workspace_rows_match([{**rows[2], "q2_variance": 1.0}, *rows[:2]])


def test_initial_workspace_has_an_installed_browser_case_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).parent / "flagship_requirements.json"))
    for host in ("dash", "react", "streamlit"):
        assert plan[("initial-workspace.populated", host, "browser.initial-workspace.populated")] == (
            f"tests/test_flagship_workflows.py::test_flagship_initial_workspace[{host}]"
        )


def test_global_filter_oracle_rejects_old_and_excluded_rows():
    from tests.flagship_browser_support import global_filter_rows_match

    rows = [
        {"id": "München", "total": 79.75, "q1": 49.75, "q2": 30.0},
        {"id": "Grand Total", "total": 79.75, "q1": 49.75, "q2": 30.0},
    ]
    assert global_filter_rows_match(rows)
    assert global_filter_rows_match([{"path": "__color_scale_stats__"}, *rows])
    assert not global_filter_rows_match([*rows, {"id": "Éclair", "total": 120.25}])
    assert not global_filter_rows_match([{**rows[1], "total": 200.0}, rows[0]])


def test_global_filter_has_an_installed_browser_case_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).parent / "flagship_requirements.json"))
    for host in ("dash", "react", "streamlit"):
        assert plan[("global-filter.apply-clear", host, "browser.global-filter.apply-clear")] == (
            f"tests/test_flagship_workflows.py::test_flagship_global_filter[{host}]"
        )


def test_aggregation_round_trip_has_an_installed_browser_case_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).parent / "flagship_requirements.json"))
    for host in ("dash", "react", "streamlit"):
        assert plan[("layout-values.aggregation", host, "browser.layout-values.aggregation")] == (
            f"tests/test_flagship_workflows.py::test_flagship_aggregation_round_trip[{host}]"
        )


def test_aggregation_round_trip_oracle_requires_correlated_raw_and_visible_values():
    from tests.flagship_browser_support import aggregation_round_trip_matches

    def state(agg, raw, visible, request_id, position=2):
        return {"aggregation": agg, "rawQ1": raw, "visibleQ1": visible,
                "requestId": request_id, "columnPosition": position}

    initial = state("sum", 150, "150.00", "one")
    average = state("avg", 75, "75.00", "two")
    restored = state("sum", 150, "150.00", "three")
    assert aggregation_round_trip_matches([initial, average, restored])
    assert not aggregation_round_trip_matches([initial, average, {**restored, "requestId": "two"}])
    assert not aggregation_round_trip_matches([initial, {**average, "visibleQ1": "150.00"}, restored])
    assert not aggregation_round_trip_matches([initial, {**average, "columnPosition": 3}, restored])


def test_totals_controls_have_an_installed_browser_case_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).parent / "flagship_requirements.json"))
    for host in ("dash", "react", "streamlit"):
        assert plan[("totals.controls", host, "browser.totals.controls")] == (
            f"tests/test_flagship_workflows.py::test_flagship_totals_controls[{host}]"
        )


def test_subtotals_have_an_installed_browser_case_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).parent / "flagship_requirements.json"))
    for host in ("dash", "react", "streamlit"):
        assert plan[("totals.subtotals", host, "browser.totals.subtotals")] == (
            f"tests/test_flagship_workflows.py::test_flagship_hierarchy_navigation[totals.subtotals-{host}]"
        )


def test_measure_add_remove_has_an_installed_browser_case_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).parent / "flagship_requirements.json"))
    for host in ("dash", "react", "streamlit"):
        assert plan[("layout-values.add-remove", host, "browser.layout-values.add-remove")] == (
            f"tests/test_flagship_workflows.py::test_flagship_measure_add_remove[{host}]"
        )


def test_exact_downloads_have_installed_browser_mappings_for_every_host():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).parent / "flagship_requirements.json"))
    for kind, requirement in (("csv", "export-csv.exact-document"), ("xlsx", "export-xlsx.exact-workbook")):
        for host in ("dash", "react", "streamlit"):
            assert plan[(requirement, host, f"browser.{requirement}")] == (
                f"tests/test_flagship_workflows.py::test_flagship_exact_download[{kind}-{host}]"
            )


def test_axis_swap_has_executable_installed_browser_and_shared_mappings():
    from scripts.certify_flagship import _product_browser_plan
    from scripts.flagship_shared_runner import SHARED_ASSERTIONS
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).with_name("flagship_requirements.json")))
    for host in ("dash", "react", "streamlit"):
        assert plan[("layout-swap.transpose", host, "browser.layout-swap.transpose")] == (
            f"tests/test_flagship_workflows.py::test_flagship_axis_swap[{host}]")
    assert SHARED_ASSERTIONS["shared.layout-swap.transpose"][0].node == (
        "tests/test_flagship_shared_runtime.py::test_flagship_axis_swap_round_trip_preserves_exact_values")


def test_layout_modes_have_executable_installed_browser_and_shared_mappings():
    from scripts.certify_flagship import _product_browser_plan
    from scripts.flagship_shared_runner import SHARED_ASSERTIONS
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).with_name("flagship_requirements.json")))
    for host in ("dash", "react", "streamlit"):
        assert plan[("layout-modes.switch", host, "browser.layout-modes.switch")] == (
            f"tests/test_flagship_workflows.py::test_flagship_layout_modes[{host}]")
    assert SHARED_ASSERTIONS["shared.layout-modes.switch"][0].node == (
        "tests/test_flagship_shared_runtime.py::test_flagship_layout_modes_preserve_exact_keys_and_totals")


def test_measure_add_remove_oracle_rejects_stale_cells_headers_and_requests():
    from tests.flagship_browser_support import measure_add_remove_matches

    def state(request_id, units):
        return {"requestId": request_id, "hasUnits": units,
                "rawSales": [150, 50, 200], "visibleSales": ["150", "50", "200"],
                "rawUnits": [5, 5, 10] if units else [],
                "visibleUnits": ["5", "5", "10"] if units else [],
                "unitHeaderCount": 3 if units else 0, "unitCellCount": 9 if units else 0}

    before, added, removed = state("before", False), state("added", True), state("removed", False)
    assert measure_add_remove_matches([before, added, removed])
    for broken in ({**removed, "unitHeaderCount": 1}, {**removed, "unitCellCount": 1},
                   {**removed, "requestId": "added"}, {**removed, "rawSales": [150, 50, 400]}):
        assert not measure_add_remove_matches([before, added, broken])
    assert not measure_add_remove_matches([before, {**added, "visibleUnits": ["5", "5", "9"]}, removed])


def test_visible_startup_error_is_a_failed_product_case(monkeypatch):
    import tests.flagship_browser_support as support

    class Driver:
        capabilities = {"browserName": "chrome", "browserVersion": "156"}

    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    monkeypatch.setattr(support, "_visible_error", lambda driver: False)
    support.classify_visible_startup_error(Driver(), result)
    assert result["result"] == "blocked"
    monkeypatch.setattr(support, "_visible_error", lambda driver: True)
    support.classify_visible_startup_error(Driver(), result)
    assert result == {"result": "failed", "browser": "chrome 156",
                      "infrastructureUnavailable": False, "visibleError": True}


def test_scroll_windows_have_dedicated_installed_browser_phase():
    from scripts.certify_flagship import _product_browser_plan, _product_phase
    from tests.flagship_contract import load_requirements

    requirements = load_requirements(Path(__file__).parent / "flagship_requirements.json")
    plan = _product_browser_plan(requirements)
    for name in ("vertical-virtual-scroll.window", "horizontal-virtual-scroll.window"):
        for host in ("dash", "react", "streamlit"):
            node = plan[(name, host, f"browser.{name}")]
            assert node == f"tests/test_flagship_workflows.py::test_flagship_virtual_scroll[{name}-{host}]"
            assert _product_phase(node) == "scroll"
    assert _product_phase("tests/test_flagship_workflows.py::test_flagship_hierarchy_navigation[hierarchy-expand.round-trip-dash]") == "parity"
    assert _product_phase("tests/test_flagship_workflows.py::test_flagship_export_jobs[large-delivery.progress-dash]") == "exports"


def test_supersession_probe_selects_outgoing_dash_request_not_nested_previous_response():
    from tests.flagship_browser_support import _SUPERSESSION_PROBE_SCRIPT

    script = f"""
      const oldRequest = {{schemaVersion:1, kind:'data', requestId:'old',
        state:{{filters:{{global:'Éclair'}}}}}};
      const metadataRequest = {{...oldRequest, requestId:'metadata'}};
      const newRequest = {{schemaVersion:1, kind:'data', requestId:'new',
        state:{{filters:{{global:'München'}}}}}};
      const previous = {{schemaVersion:1, kind:'data', requestId:'previous', status:'data',
        state:{{filters:{{global:'Éclair'}}}}}};
      const envelope = (request, total) => ({{schemaVersion:1, kind:'data',
        requestId:request.requestId, status:'data', payload:{{data:[{{_isTotal:true,
        _id:'Grand Total', '__RowTotal__ventes_€_sum':total}}]}}}});
      let next = 0;
      global.window = {{fetch: async () => {{
        const index = next++;
        const body = index === 0 ? {{...envelope(metadataRequest, null), payload:{{data:[]}}}}
          : index === 1 ? envelope(oldRequest, 120.25) : envelope(newRequest, 79.75);
        return {{clone:() => ({{text:async () => JSON.stringify(body)}})}};
      }}}};
      global.performance = {{now: () => next}};
      eval({json.dumps(_SUPERSESSION_PROBE_SCRIPT)});
      (async () => {{
        window.__pivotSupersession.arm();
        await window.fetch('/_dash-update-component', {{body:JSON.stringify({{
          inputs:[{{value:JSON.stringify(previous)}}, {{value:JSON.stringify(metadataRequest)}}]
        }})}});
        const old = window.fetch('/_dash-update-component', {{body:JSON.stringify({{
          inputs:[{{value:JSON.stringify(previous)}}, {{value:JSON.stringify(oldRequest)}}]
        }})}});
        await new Promise(resolve => setImmediate(resolve));
        const newer = window.fetch('/_dash-update-component', {{body:JSON.stringify({{
          inputs:[{{value:JSON.stringify(previous)}}, {{value:JSON.stringify(newRequest)}}]
        }})}});
        await newer;
        window.__pivotSupersession.release();
        await old;
        process.stdout.write(JSON.stringify(window.__pivotSupersession.trace));
      }})().catch(error => {{console.error(error); process.exitCode = 1}});
    """
    completed = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True, timeout=5)
    trace = json.loads(completed.stdout)
    from tests.flagship_browser_support import assert_superseded_response
    assert assert_superseded_response(trace, "old", "new", 79.75)


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
    from scripts.flagship_manifest import read_committed_contract

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
    binding.sha256["contract"] = hashlib.sha256(read_committed_contract(binding)).hexdigest()
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


def test_passing_record_requires_observed_asset_digests(tmp_path, monkeypatch):
    from tests.flagship_browser_support import record_case

    binding_file = tmp_path / "binding.json"
    binding_file.write_text(json.dumps({
        "runId": "bound-run", "sourceCommit": "a" * 40,
        "sha256": {"contract": "c" * 64},
    }), encoding="utf-8")
    monkeypatch.setenv("PIVOT_FLAGSHIP_ARTIFACT_BINDING", str(binding_file))
    monkeypatch.setenv("PIVOT_FLAGSHIP_CASE_RESULTS", str(tmp_path / "results.json"))
    monkeypatch.setenv("PIVOT_FLAGSHIP_RUN_ID", "bound-run")
    directory = tmp_path / "private" / "runs" / "bound-run" / "dash"
    directory.mkdir(parents=True)
    observation = directory / "observation.json"
    observation.write_text(json.dumps({"diagnostic": ""}), encoding="utf-8")
    case = {"result": "passed", "browser": "chrome 155", "evidence_paths": [str(observation)],
            "loadedAssetSha256": {"/entry.js": "d" * 64}}
    missing_contract = json.loads(binding_file.read_text(encoding="utf-8"))
    del missing_contract["sha256"]["contract"]
    binding_file.write_text(json.dumps(missing_contract), encoding="utf-8")
    with pytest.raises(ValueError, match="committed contract hash"):
        record_case("custom-pivot-order.sparse-window", "dash", case, directory)
    missing_contract["sha256"]["contract"] = "c" * 64
    binding_file.write_text(json.dumps(missing_contract), encoding="utf-8")
    with pytest.raises(ValueError, match="observation-level loaded asset"):
        record_case("custom-pivot-order.sparse-window", "dash", case, directory)
    observation.write_text(json.dumps({"diagnostic": "", "loadedAssetSha256": case["loadedAssetSha256"]}), encoding="utf-8")
    record_case("custom-pivot-order.sparse-window", "dash", case, directory)
    assert json.loads((tmp_path / "results.json").read_text())["results"][0]["result"] == "passed"


def test_record_case_cannot_pass_without_browser_asset_evidence(tmp_path):
    from tests.flagship_browser_support import record_case

    with pytest.raises((AssertionError, ValueError), match="evidence|binding"):
        record_case("runtime-error.visible", "dash", {"result": "passed", "browser": "chrome 1"}, tmp_path)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_initial_workspace(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_initial_workspace

    qualify_initial_workspace(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_global_filter(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_global_filter

    qualify_global_filter(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_aggregation_round_trip(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_aggregation_round_trip

    qualify_aggregation_round_trip(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_measure_add_remove(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_measure_add_remove

    qualify_measure_add_remove(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("export_format", ["csv", "xlsx"])
def test_flagship_exact_download(host, export_format, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_exact_download

    qualify_exact_download(host, export_format)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_totals_controls(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_totals_controls

    qualify_totals_controls(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_axis_swap(host, installed_workflow_hosts):
    from tests.flagship_axis_swap import qualify_axis_swap

    qualify_axis_swap(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_layout_modes(host, installed_workflow_hosts):
    from tests.flagship_layout_modes import qualify_layout_modes

    qualify_layout_modes(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("kind", ["tabs", "newlines"])
def test_flagship_embedded_clipboard(host, kind, installed_workflow_hosts):
    from tests.flagship_embedded_clipboard import qualify_embedded_clipboard

    qualify_embedded_clipboard(host, kind)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_sort_direction(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_sort_direction

    qualify_sort_direction(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_header_filter(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_header_filter

    qualify_header_filter(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_runtime_error_recovery(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_runtime_recovery

    qualify_runtime_recovery(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_superseded_response(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_superseded_response

    qualify_superseded_response(host)


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


def test_selected_headers_oracle_has_only_selected_displayed_columns():
    from tests.flagship_browser_support import selected_headers_oracle, assert_selected_clipboard

    columns = ["Q1_unités_sum", "Q1_ventes_€_sum"]
    assert selected_headers_oracle(columns) == ["Unités (Sum)", "Ventes € (Sum)"]
    assert_selected_clipboard(
        "Unités (Sum)\tVentes € (Sum)\n2\t100\n3\t50",
        [selected_headers_oracle(columns), ["2", "100"], ["3", "50"]],
    )
    with pytest.raises(AssertionError):
        assert_selected_clipboard(
            "Unités (Sum)\tVentes € (Sum)\tÉcart (Sum)\n2\t100\n3\t50",
            [selected_headers_oracle(columns), ["2", "100"], ["3", "50"]],
        )


def test_formatted_selection_oracle_requires_two_decimal_display_values():
    from tests.flagship_browser_support import formatted_selection_oracle, assert_selected_clipboard

    rows = [{"label": "München"}, {"label": "Éclair"}]
    columns = ["Q1_ventes_€_sum", "Q1_unités_sum"]
    expected = formatted_selection_oracle(rows, columns)
    assert expected == [["49.75", "3.00"], ["100.25", "2.00"]]
    assert_selected_clipboard("49.75\t3.00\n100.25\t2.00", expected)
    with pytest.raises(AssertionError):
        assert_selected_clipboard("50\t3\n100\t2", expected)


def test_selected_clipboard_oracle_checks_native_windows_bytes_without_normalizing_cells():
    from tests.flagship_browser_support import assert_selected_clipboard

    body = [["100.25", "2"], ["49.75", "3"]]
    assert assert_selected_clipboard("100.25\t2\r\n49.75\t3", body, line_separator="\r\n") == body
    for wrong in ("100.25\t2\n49.75\t3", "100.25\t2\r\n49.75\t3\r\n",
                  "100.25\t2\r\n49.75\t3\t", "100.25\t2\r\n49.75\n3"):
        with pytest.raises(AssertionError):
            assert_selected_clipboard(wrong, body, line_separator="\r\n")


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
@pytest.mark.parametrize("initial_format_open", [True, False])
def test_number_format_helper_uses_dialog_ok_and_commits_precision(tmp_path, initial_format_open):
    """Exercise the helper against real DOM events; no product behavior is mocked."""
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable Chrome helper regression explicitly")
    from urllib.parse import quote
    from selenium import webdriver
    from tests.flagship_browser_support import _selection_number_format
    from tests.test_flagship_certification import _qualification_chrome_options

    html = """<button data-testid="pivot-toolbar-format" onclick="const options=document.querySelector('[title]'); options.hidden=!options.hidden">Format</button>
      <button title="Open number format options" INITIAL_HIDDEN onclick="document.querySelector('[role=dialog]').hidden=false">Number Format</button>
      <div role="dialog" aria-label="Format Cells" hidden><input type="number" value="0">
      <button onclick="document.querySelector('output').textContent=document.querySelector('input').value; this.parentElement.remove()">OK</button></div>
      <output></output>""".replace("INITIAL_HIDDEN", "" if initial_format_open else "hidden")
    driver = webdriver.Chrome(options=_qualification_chrome_options(tmp_path))
    try:
        driver.get("data:text/html," + quote(html))
        _selection_number_format(driver)
        assert driver.find_element("css selector", "output").text == "2"
        assert not driver.find_elements("css selector", '[role="dialog"]')
    finally:
        driver.quit()


@pytest.mark.flagship
@pytest.mark.parametrize("label,child", [("Sort", "Descending"), ("Pin", "Pin Left")])
def test_submenu_helper_matches_exact_title_with_child_indicator_and_opens_child(tmp_path, label, child):
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable Chrome helper regression explicitly")
    from urllib.parse import quote
    from selenium import webdriver
    from selenium.webdriver.common.action_chains import ActionChains
    from tests.flagship_browser_support import _selection_menu
    from tests.test_flagship_certification import _qualification_chrome_options

    html = f'''<div role="menu"><div role="menuitem" title="{label} unrelated">{label} unrelated</div>
      <div role="menuitem" title="{label}" aria-haspopup="menu" tabindex="0"
        onmouseenter="document.querySelector('#child').hidden=false"><span>{label}</span><span>▶</span></div>
      </div><div role="menu" id="child" hidden><div role="menuitem" title="{child}">{child}</div></div>'''
    driver = webdriver.Chrome(options=_qualification_chrome_options(tmp_path))
    try:
        driver.get("data:text/html;charset=utf-8," + quote(html))
        parent = _selection_menu(driver, label)
        assert parent.get_attribute("role") == "menuitem"
        assert parent.get_attribute("title") == label
        assert "▶" in parent.text
        ActionChains(driver).move_to_element(parent).perform()
        target = _selection_menu(driver, child)
        assert target.is_displayed()
        assert target.get_attribute("title") == child
        assert driver.find_element("css selector", "#child").is_displayed()
    finally:
        driver.quit()


@pytest.mark.flagship
def test_selection_copy_helper_chooses_raw_menu_action_in_real_browser(tmp_path):
    """A rounded default action must not satisfy a raw-value clipboard case."""
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable Chrome helper regression explicitly")
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from selenium import webdriver
    from tests.flagship_browser_support import _copy_selected
    from tests.test_flagship_certification import _qualification_chrome_options

    html = b'''<!doctype html><button id="cell" oncontextmenu="event.preventDefault(); document.querySelector('#menu').hidden=false">Cell</button>
      <div id="menu" role="menu" hidden>
      <button role="menuitem" title="Copy Selection" onclick="navigator.clipboard.writeText('100\\t2\\r\\n50\\t3')">Copy Selection</button>
      <button role="menuitem" title="Copy Selection (Raw Values)" onclick="navigator.clipboard.writeText('100.25\\t2\\r\\n49.75\\t3')">Copy Selection (Raw Values)</button>
      </div>'''

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    driver = webdriver.Chrome(options=_qualification_chrome_options(tmp_path))
    try:
        origin = f"http://127.0.0.1:{server.server_port}"
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": origin, "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        driver.get(origin)
        observation = {"clipboardLineSeparator": "\r\n"}
        _copy_selected(driver, driver.find_element("css selector", "#cell"),
                       [["100.25", "2"], ["49.75", "3"]], observation,
                       menu_label="Copy Selection (Raw Values)")
        assert observation["copyMenuAction"] == "Copy Selection (Raw Values)"
        assert observation["parsedTSV"] == [["100.25", "2"], ["49.75", "3"]]
    finally:
        driver.quit()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("requirement", ["cell-selection.range-clear", "copy-selection.logical-cells",
                                         "copy-selection.display-order", "copy-selection.pinned-columns",
                                         "copy-selection.headers", "copy-selection.formatted-values",
                                         "keyboard-copy.selection"])
def test_flagship_selected_clipboard(host, requirement, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_selected_clipboard

    qualify_selected_clipboard(host, requirement)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("requirement", ["copy-current-table.visible-scope", "copy-all-records.complete-scope"])
def test_flagship_table_scope_clipboard(host, requirement, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_table_scope_clipboard

    qualify_table_scope_clipboard(host, requirement)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_native_input_copy(host, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_native_input_copy

    qualify_native_input_copy(host)


def test_navigation_response_rejects_child_under_wrong_parent():
    from tests.flagship_browser_support import _navigation_response_matches

    rows = [
        {"id": "Grand Total", "path": "__grand_total__", "total": 200, "q1": 150, "q2": 50},
        {"id": "Éclair", "path": "Éclair", "total": 120.25, "q1": 100.25, "q2": 20},
        {"id": "München", "path": "München", "total": 79.75, "q1": 49.75, "q2": 30},
        {"id": "東京", "path": "München|||東京", "total": 120.25, "q1": 100.25, "q2": 20},
    ]
    assert not _navigation_response_matches({"rows": rows}, ("東京",))


def test_navigation_dom_rejects_wrong_rendered_group_total():
    from tests.flagship_browser_support import _navigation_dom_matches

    cells = [
        {"rowid": rowid, "colid": colid, "text": value}
        for rowid, values in {
            "Éclair": ("100", "20", "120"),
            "München": ("50", "30", "80"),
            "__grand_total__": ("150", "50", "200"),
            "Éclair|||東京": ("100", "20", "999"),
        }.items()
        for colid, value in zip(
            ("Q1_ventes_€_sum", "Q2_ventes_€_sum", "__RowTotal__ventes_€_sum"), values)
    ]
    assert not _navigation_dom_matches(cells, ("東京",))


def test_navigation_dom_rejects_duplicate_cell_with_conflicting_value():
    from tests.flagship_browser_support import _navigation_dom_matches

    columns = ("Q1_ventes_€_sum", "Q2_ventes_€_sum", "__RowTotal__ventes_€_sum")
    cells = [
        {"rowid": rowid, "colid": colid, "text": value}
        for rowid, values in {
            "Éclair": ("100", "20", "120"),
            "München": ("50", "30", "80"),
            "__grand_total__": ("150", "50", "200"),
        }.items()
        for colid, value in zip(columns, values)
    ]
    cells.append({"rowid": "Éclair", "colid": columns[2], "text": "999"})
    assert not _navigation_dom_matches(cells, ())


def test_navigation_dom_requires_exact_two_decimal_display_when_requested():
    from tests.flagship_browser_support import _navigation_dom_matches

    columns = ("Q1_ventes_€_sum", "Q2_ventes_€_sum", "__RowTotal__ventes_€_sum")
    values = {
        "Éclair": ("100.25", "20.00", "120.25"),
        "München": ("49.75", "30.00", "79.75"),
        "__grand_total__": ("150.00", "50.00", "200.00"),
        "Éclair|||東京": ("100.25", "20.00", "120.25"),
    }
    cells = [
        {"rowid": rowid, "colid": colid, "text": value}
        for rowid, row_values in values.items()
        for colid, value in zip(columns, row_values)
    ]
    assert _navigation_dom_matches(cells, ("東京",), decimal_places=2)
    rounded = [dict(cell, text="120") if cell["rowid"] == "Éclair" and
               cell["colid"] == columns[2] else cell for cell in cells]
    assert not _navigation_dom_matches(rounded, ("東京",), decimal_places=2)


def test_resize_geometry_requires_exact_delta_and_unchanged_logical_cells():
    from tests.flagship_browser_support import assert_resize_geometry

    before = {"width": 120, "order": ["hierarchy", "sales", "units"],
              "cells": [("Éclair", "sales", "100")], "neighbors": [("Éclair", "units", "2")]}
    good = {**before, "width": 160}
    assert_resize_geometry(before, good)
    for broken in ({**good, "width": 157}, {**good, "order": ["hierarchy", "units", "sales"]},
                   {**good, "cells": [("Éclair", "sales", "999")]},
                   {**good, "neighbors": [("Éclair", "units", "999")]}):
        with pytest.raises(AssertionError):
            assert_resize_geometry(before, broken)


def test_auto_size_geometry_requires_fit_idempotence_and_all_columns():
    from tests.flagship_browser_support import assert_auto_size_geometry

    base = {"width": 120, "requiredWidth": 180, "order": ["hierarchy", "sales", "units"], "fits": True}
    assert_auto_size_geometry(base, {**base, "width": 184}, {**base, "width": 185}, min_width=60, max_width=600)
    for broken in ({**base, "width": 175}, {**base, "width": 610},
                   {**base, "width": 184, "order": ["hierarchy", "sales"]}):
        with pytest.raises(AssertionError):
            assert_auto_size_geometry(base, broken, {**base, "width": 185}, min_width=60, max_width=600)
    with pytest.raises(AssertionError):
        assert_auto_size_geometry(base, {**base, "width": 184}, {**base, "width": 188}, min_width=60, max_width=600)
    with pytest.raises(AssertionError):
        assert_auto_size_geometry(base, {**base, "width": 184, "fits": False}, {**base, "width": 185}, min_width=60, max_width=600)


def test_pin_round_trip_requires_stationary_unique_cell_and_restored_order():
    from tests.flagship_browser_support import assert_pin_round_trip

    before = {"order": ["hierarchy", "sales", "units", "q2_sales"], "cells": [("Éclair", "q2_sales", "20")],
              "pinnedLeft": [], "pinnedX": 250, "centerX": 400, "scrollLeft": 0}
    pinned = {**before, "order": ["hierarchy", "q2_sales", "sales", "units"], "pinnedLeft": ["q2_sales"]}
    scrolled = {**pinned, "pinnedX": 250, "centerX": 360, "scrollLeft": 40}
    restored = {**before, "scrollLeft": 0}
    end = {**scrolled, "scrollLeft": 900, "maxScrollLeft": 900, "lastColumnVisible": True}
    assert_pin_round_trip(before, pinned, scrolled, end, restored, "q2_sales")
    for broken in ({**scrolled, "pinnedX": 260}, {**scrolled, "centerX": 400},
                   {**scrolled, "cells": [("Éclair", "q2_sales", "20"), ("Éclair", "q2_sales", "20")]},
                   {**scrolled, "scrollLeft": 0}):
        with pytest.raises(AssertionError):
            assert_pin_round_trip(before, pinned, broken, end, restored, "q2_sales")
    with pytest.raises(AssertionError):
        assert_pin_round_trip(before, pinned, scrolled, end, {**restored, "order": pinned["order"]}, "q2_sales")
    with pytest.raises(AssertionError):
        assert_pin_round_trip(before, pinned, scrolled, {**end, "lastColumnVisible": False}, restored, "q2_sales")
    with pytest.raises(AssertionError):
        assert_pin_round_trip(before, pinned, scrolled, {**end, "scrollLeft": 800}, restored, "q2_sales")


def test_pinned_clipboard_order_requires_raw_fixture_values_in_pinned_positions():
    from tests.flagship_browser_support import assert_pinned_clipboard_order

    order = ["hierarchy", "Q2_ventes_€_sum", "Q1_ventes_€_sum", "Q1_unités_sum"]
    rows = [["Region > Account", "Ventes €", "Ventes €", "Unités"],
            ["Éclair", "20", "100", "2"], ["München", "30", "50", "3"]]
    assert_pinned_clipboard_order(rows, order)
    with pytest.raises(AssertionError):
        assert_pinned_clipboard_order([rows[0], ["Éclair", "100", "20", "2"], rows[2]], order)
    with pytest.raises(AssertionError):
        assert_pinned_clipboard_order([rows[0], rows[1], rows[1]], order)


@pytest.mark.flagship
def test_pin_submenu_helper_uses_stateful_live_title(tmp_path):
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable Chrome helper regression explicitly")
    from urllib.parse import quote
    from selenium import webdriver
    from tests.flagship_browser_support import _geometry_pin_submenu
    from tests.test_flagship_certification import _qualification_chrome_options

    html = '''<div role="menu"><div role="menuitem" title="Pin" onmouseenter="document.querySelector('#unused').hidden=false">Pin</div>
      <div role="menuitem" title="Pin (left)" onmouseenter="document.querySelector('#child').hidden=false">Pin (left)</div></div>
      <div id="unused" role="menu" hidden><div role="menuitem" title="Wrong">Wrong</div></div>
      <div id="child" role="menu" hidden><div role="menuitem" title="Unpin">Unpin</div></div>'''
    driver = webdriver.Chrome(options=_qualification_chrome_options(tmp_path))
    try:
        driver.get("data:text/html;charset=utf-8," + quote(html))
        _geometry_pin_submenu(driver, "left")
        assert driver.find_element("css selector", "#child").is_displayed()
        assert not driver.find_element("css selector", "#unused").is_displayed()
    finally:
        driver.quit()


@pytest.mark.flagship
def test_geometry_copy_menu_chooses_exact_visible_label_not_description(tmp_path):
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable Chrome helper regression explicitly")
    from urllib.parse import quote
    from selenium import webdriver
    from tests.flagship_browser_support import _geometry_copy_current_table
    from tests.test_flagship_certification import _qualification_chrome_options

    html = '''<div role="menu"><div role="menuitem" title="Copy Current Table other" onclick="document.querySelector('output').textContent='wrong'">Copy Current Table other</div>
      <div role="menuitem" title="Copy the current expanded/collapsed table, with its active sorting, filters, and visible column order."
      onclick="document.querySelector('output').textContent='clicked'">Copy Current Table</div></div><output></output>'''
    driver = webdriver.Chrome(options=_qualification_chrome_options(tmp_path))
    try:
        driver.get("data:text/html;charset=utf-8," + quote(html))
        _geometry_copy_current_table(driver)
        assert driver.find_element("css selector", "output").text == "clicked"
    finally:
        driver.quit()


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("requirement", ["hierarchy-expand.round-trip", "hierarchy-expand-all.round-trip", "totals.subtotals"])
def test_flagship_hierarchy_navigation(host, requirement, installed_workflow_hosts):
    """A real installed grid must change rows and correlate V1 traffic after expansion."""
    from tests.flagship_browser_support import qualify_hierarchy_navigation

    qualify_hierarchy_navigation(host, requirement)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("requirement", ["vertical-virtual-scroll.window", "horizontal-virtual-scroll.window"])
def test_flagship_virtual_scroll(host, requirement, installed_workflow_hosts):
    """Qualify later row and column windows against the wide installed fixture."""
    from tests.flagship_scroll_support import qualify_virtual_scroll

    qualify_virtual_scroll(host, requirement)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
def test_flagship_custom_pivot_order(host, installed_workflow_hosts):
    from tests.flagship_scroll_support import qualify_custom_pivot_order

    qualify_custom_pivot_order(host)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("requirement", ["column-sizing.resize", "column-sizing.auto-size",
                                         "column-pinning.round-trip"])
def test_flagship_column_geometry(host, requirement, installed_workflow_hosts):
    """Qualify physical column gestures against each installed host."""
    from tests.flagship_browser_support import qualify_column_geometry

    qualify_column_geometry(host, requirement)


@pytest.mark.flagship
@pytest.mark.parametrize("host", ["dash", "react", "streamlit"])
@pytest.mark.parametrize("requirement", ["large-delivery.progress", "large-delivery.cancel-retry",
                                        "journey-recovery.cancel-new-work"])
def test_flagship_export_jobs(host, requirement, installed_workflow_hosts):
    from tests.flagship_browser_support import qualify_export_jobs

    qualify_export_jobs(host, requirement)
