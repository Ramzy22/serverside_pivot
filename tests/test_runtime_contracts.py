"""Regression tests for the versioned browser-to-Dash runtime contract."""

import pytest
import json
from pathlib import Path
import subprocess

from pivot_engine.runtime.contracts import (
    RUNTIME_SCHEMA_VERSION,
    RuntimeContractError,
    build_runtime_response,
    normalize_runtime_request,
    validate_runtime_response_v1,
    validate_runtime_request_v1,
)


def test_v1_request_normalizes_metadata_and_state_at_the_boundary():
    request = normalize_runtime_request(
        pivot_id="pivot-grid",
        runtime_request={
            "schemaVersion": RUNTIME_SCHEMA_VERSION,
            "kind": "data",
            "requestId": "v1-window-7",
            "table": "sales",
            "sessionId": "session-a",
            "clientInstance": "browser-a",
            "stateEpoch": 3,
            "windowSeq": 7,
            "abortGeneration": 1,
            "state": {
                "rowFields": ["region"],
                "showColTotals": False,
            },
            "payload": {"start": 96, "end": 143, "intent": "viewport"},
        },
    )

    assert request["legacy"] is False
    assert request["kind"] == "data"
    assert request["request_id"] == "v1-window-7"
    assert request["trigger_prop"] == "pivot-grid.viewport"
    assert request["payload"]["table"] == "sales"
    assert request["payload"]["session_id"] == "session-a"
    assert request["payload"]["state_override"] == {
        "rowFields": ["region"],
        "showColTotals": False,
    }


def test_v1_request_rejects_unknown_envelope_members():
    request = normalize_runtime_request(
        pivot_id="pivot-grid",
        runtime_request={
            "schemaVersion": RUNTIME_SCHEMA_VERSION,
            "kind": "data",
            "requestId": "bad-request",
            "payload": {},
            "unreviewedField": True,
        },
    )

    assert request["error_code"] == "invalid_runtime_request"
    assert "unreviewedField" in request["error"]


def test_v1_request_rejects_unknown_view_state_members():
    request = normalize_runtime_request(
        pivot_id="pivot-grid",
        runtime_request={
            "schemaVersion": RUNTIME_SCHEMA_VERSION,
            "kind": "data",
            "requestId": "bad-state",
            "state": {"rowFields": ["region"], "unreviewedState": True},
            "payload": {},
        },
    )

    assert request["error_code"] == "invalid_runtime_request"
    assert "unreviewedState" in request["error"]


def test_unknown_schema_version_is_not_silently_treated_as_legacy():
    request = normalize_runtime_request(
        pivot_id="pivot-grid",
        runtime_request={"schemaVersion": 999, "kind": "data", "payload": {}},
    )

    assert request["error_code"] == "unsupported_runtime_schema_version"


def test_v1_response_is_the_single_canonical_camel_case_envelope():
    response = build_runtime_response(
        kind="data",
        request_id="response-1",
        status="data",
        payload={"rows": []},
        session_id="session-a",
        client_instance="browser-a",
        state_epoch=2,
        window_seq=9,
    )

    assert response["schemaVersion"] == RUNTIME_SCHEMA_VERSION
    assert response["sessionId"] == "session-a"
    assert response["clientInstance"] == "browser-a"
    assert response["stateEpoch"] == 2
    assert response["windowSeq"] == 9
    assert "session_id" not in response
    assert "client_instance" not in response
    assert "state_epoch" not in response
    assert "window_seq" not in response
    assert validate_runtime_response_v1(response) == response


def export_request(payload):
    return {"schemaVersion": 1, "kind": "export", "requestId": "export-1",
            "table": "sales", "payload": payload}


@pytest.mark.parametrize("payload", [
    {"format": "csv", "scope": "all"},
    {"action": "start", "format": "xlsx", "scope": "all"},
    {"action": "status", "jobId": "server-job"},
    {"action": "cancel", "jobId": "server-job"},
])
def test_export_actions_and_existing_synchronous_exports_are_accepted(payload):
    request = export_request(payload)
    assert validate_runtime_request_v1(request) == request
    normalized = normalize_runtime_request(pivot_id="pivot-grid", runtime_request=request)
    assert normalized["kind"] == "export"
    assert normalized["payload"].items() >= payload.items()


@pytest.mark.parametrize("payload", [
    {"action": "resume"},
    {"action": "status"},
    {"action": "cancel"},
    {"action": "status", "jobId": ""},
    {"action": "cancel", "jobId": 123},
    {"action": "start", "jobId": "forged-job"},
    {"jobId": "forged-job"},
])
def test_export_actions_reject_missing_or_forged_job_ids(payload):
    with pytest.raises(RuntimeContractError):
        validate_runtime_request_v1(export_request(payload))


def test_non_export_payload_actions_keep_their_existing_contract():
    request = export_request({"action": "unrelated-transaction-action"})
    request["kind"] = "transaction"
    assert validate_runtime_request_v1(request) == request


def export_job_response(**overrides):
    payload = {"jobId": "server-job", "phase": "running", "rowsWritten": 3,
               "bytesWritten": 42, "totalRows": None}
    payload.update(overrides)
    return build_runtime_response(kind="export", request_id="export-1", status="export_job", payload=payload)


@pytest.mark.parametrize("phase", ["queued", "running", "cancelling", "cancelled", "failed", "interrupted"])
def test_export_job_response_preserves_observed_progress_and_unknown_total(phase):
    response = export_job_response(phase=phase)
    assert validate_runtime_response_v1(response) == response


@pytest.mark.parametrize("overrides", [
    {"phase": "invented"}, {"jobId": ""}, {"rowsWritten": -1},
    {"bytesWritten": 1.5}, {"totalRows": -1}, {"phase": "completed"},
    {"payloadRef": {"token": "premature"}},
])
def test_export_job_response_rejects_invalid_progress_or_premature_output(overrides):
    with pytest.raises(RuntimeContractError):
        validate_runtime_response_v1(export_job_response(**overrides))


def test_completed_export_job_requires_a_payload_reference():
    response = export_job_response(phase="completed", payloadRef={"token": "opaque", "url": "/payload/opaque"})
    assert validate_runtime_response_v1(response) == response


def test_export_job_status_cannot_omit_its_observations():
    response = build_runtime_response(kind="export", request_id="job-status", status="export_job", payload={})
    with pytest.raises(RuntimeContractError):
        validate_runtime_response_v1(response)


def test_export_job_response_builder_emits_only_public_observations():
    from pivot_engine.runtime import contracts
    from pivot_engine.runtime.export_jobs import ExportJobSnapshot

    assert hasattr(contracts, "build_export_job_response"), "The job response codec is missing"
    snapshot = ExportJobSnapshot("job", "alice", "trusted-account", "sales")
    response = contracts.build_export_job_response(snapshot, request_id="start-1")
    assert response == {"schemaVersion": 1, "kind": "export", "requestId": "start-1", "status": "export_job",
                        "payload": {"jobId": "job", "phase": "queued", "rowsWritten": 0, "bytesWritten": 0, "totalRows": None}}
    assert validate_runtime_response_v1(response) == response


def test_typescript_boundary_guards_enforce_export_job_contract():
    root = Path(__file__).resolve().parents[1]
    script = r"""
const fs = require('node:fs');
const ts = require('./dash_tanstack_pivot/node_modules/typescript');
const source = fs.readFileSync('./dash_tanstack_pivot/src/lib/runtime/runtimeContract.ts', 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS }}).outputText;
const output = { exports: {} };
new Function('module', 'exports', compiled)(output, output.exports);
const contract = output.exports;
const requests = JSON.parse(fs.readFileSync(0, 'utf8'));
const results = requests.map(payload => contract.isRuntimeRequestV1({schemaVersion: 1, kind: 'export', requestId: 'id', payload}));
const response = {schemaVersion: 1, kind: 'export', status: 'export_job', payload: {jobId: 'job', phase: 'running', rowsWritten: 1, bytesWritten: 10, totalRows: null}};
results.push(contract.isRuntimeResponseV1(response));
results.push(contract.isRuntimeResponseV1({...response, payload: {...response.payload, payloadRef: {token: 'early'}}}));
results.push(contract.isRuntimeResponseV1({...response, payload: {...response.payload, phase: 'completed'}}));
process.stdout.write(JSON.stringify(results));
"""
    result = subprocess.run(["node", "-e", script], cwd=root, input=json.dumps([
        {"format": "csv"}, {"action": "start"}, {"action": "status", "jobId": "job"},
        {"action": "status"}, {"action": "cancel", "jobId": ""},
        {"action": "start", "jobId": "forged"}, {"action": "invented"},
    ]), text=True, capture_output=True, check=True)
    assert json.loads(result.stdout) == [True, True, True, False, False, False, False, True, False, False]
