"""Regression tests for the versioned browser-to-Dash runtime contract."""

from pivot_engine.runtime.contracts import (
    RUNTIME_SCHEMA_VERSION,
    build_runtime_response,
    normalize_runtime_request,
    validate_runtime_response_v1,
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
