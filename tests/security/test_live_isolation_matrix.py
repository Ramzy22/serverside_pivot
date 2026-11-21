"""Live-stack tenant isolation: subscriptions, SSE, snapshots, baselines (REQ-SEC-001, REQ-LIVE-SEC)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine.runtime import PivotRuntimeService
from pivot_engine.runtime.http_transport import register_runtime_http_transport
from pivot_engine.tanstack_adapter import create_tanstack_adapter

from .isolation_fixtures import TABLE, claims_authorizer, leaks, tenant_table
from .test_tenant_isolation_matrix import _rows_from


@dataclass(frozen=True)
class _P:
    id: str
    role: str = "viewer"
    attributes: Dict[str, Any] = field(default_factory=dict)


ALICE = _P("tenant-a-user", attributes={"tenant": "A"})
BOB = _P("tenant-b-user", attributes={"tenant": "B"})

STATE = {
    "rowFields": ["region"],
    "valConfigs": [{"field": "amount", "agg": "sum", "alias": "amount_sum"}],
    "filters": {},
    "expanded": True,
}

# Every subscription action must be classified here (coverage guard below).
SUBSCRIPTION_ACTIONS = {
    "subscribe", "poll", "sync", "stream_sync", "ack", "modify_viewport", "set_baseline",
    "update_options", "pause", "resume", "unfreeze", "set_pause_mode", "freeze",
    "unsubscribe", "capture_baseline", "capture_snapshot", "delete_baseline",
    "clear_baseline", "list_baselines", "push_changes", "capabilities",
}


class _Env:
    def __init__(self):
        self.adapter = create_tanstack_adapter(backend_uri=":memory:")
        self.adapter.controller.load_data_from_arrow(TABLE, tenant_table())
        service = PivotRuntimeService(adapter_getter=lambda: self.adapter)
        self.current = ALICE
        app = FastAPI()
        register_runtime_http_transport(
            app,
            lambda: service,
            current_user_dependency=lambda: self.current,
            dataset_authorizer=claims_authorizer(),
        )
        self.client = TestClient(app, raise_server_exceptions=False)

    def cmd(self, principal, action, payload=None, state=None):
        self.current = principal
        return self.client.post(
            "/v1/runtime/subscription",
            json={"table": TABLE, "action": action, "state": state or STATE, "payload": payload or {}},
        )


def _assert_clean(label, response, principal_tenant="A"):
    if response.status_code >= 400:
        assert not leaks(response.text), f"{label}: error body leaks"
        return
    body = response.json()
    found = leaks(json.dumps(body, default=str), _rows_from(body))
    assert not found, f"{label}: {found}"


def test_subscription_action_registry_is_covered():
    import inspect

    from pivot_engine.runtime.subscription_service import RuntimeSubscriptionService

    source = inspect.getsource(RuntimeSubscriptionService.handle_command)
    import re

    body = source.split("raise ValueError")[0]
    actions = set(re.findall(r'act == "([a-z_]+)"', body))
    for group in re.findall(r"act in \{([^}]*)\}", body):
        actions |= set(re.findall(r'"([a-z_]+)"', group))
    assert actions <= SUBSCRIPTION_ACTIONS, f"unclassified actions: {sorted(actions - SUBSCRIPTION_ACTIONS)}"


def test_subscribe_snapshot_is_scoped():
    env = _Env()
    _assert_clean("subscribe", env.cmd(ALICE, "subscribe", {"start": 0, "end": 50}))


def test_poll_after_foreign_tenant_change_stays_scoped():
    env = _Env()
    snap = env.cmd(ALICE, "subscribe", {"start": 0, "end": 50}).json()
    env.adapter.push_stream_changes(TABLE, [{"id": "n5", "amount": 77777}], mode="update", key_fields=["id"])
    env.adapter.push_stream_changes(TABLE, [{"id": "n0", "amount": 2}], mode="update", key_fields=["id"])
    poll = env.cmd(ALICE, "poll", {"subscriptionId": snap["subscriptionId"], "ackSeq": snap.get("seq")})
    _assert_clean("poll", poll)
    assert "77777" not in poll.text


@pytest.mark.parametrize("action", ["poll", "ack", "modify_viewport", "pause", "unsubscribe"])
def test_foreign_principal_cannot_drive_anothers_subscription(action):  # REQ-LIVE-SEC-001
    env = _Env()
    snap = env.cmd(ALICE, "subscribe", {"start": 0, "end": 50}).json()
    hijack = env.cmd(BOB, action, {"subscriptionId": snap["subscriptionId"], "ackSeq": 0, "start": 0, "end": 50})
    if hijack.status_code < 400:
        body = hijack.json()
        assert body.get("type") in {"ERROR", "AUTH_REVOKED"} or body.get("closed") is False or body.get("ok") is False, (
            f"{action}: tenant B drove tenant A's subscription -> {str(body)[:200]}"
        )
        assert "A-secret" not in hijack.text


def test_baseline_capture_compare_and_list_are_scoped():  # REQ-LIVE-SEC-001
    env = _Env()
    cap = env.cmd(ALICE, "capture_baseline", {"label": "eod"})
    _assert_clean("capture_baseline", cap)
    snap_id = cap.json()["snapshot"]["snapshotId"]
    env.adapter.push_stream_changes(TABLE, [{"id": "n0", "amount": 0}], mode="update", key_fields=["id"])
    sub = env.cmd(ALICE, "subscribe", {"start": 0, "end": 50, "baselineId": snap_id})
    _assert_clean("subscribe+baseline", sub)
    _assert_clean("list_baselines", env.cmd(ALICE, "list_baselines"))
    # Tenant B must not see or acquire tenant A's baseline.
    listed = env.cmd(BOB, "list_baselines")
    assert snap_id not in listed.text
    stolen = env.cmd(BOB, "subscribe", {"start": 0, "end": 50, "baselineId": snap_id})
    assert stolen.status_code >= 400 or "A-secret" not in stolen.text
    deleted = env.cmd(BOB, "delete_baseline", {"baselineId": snap_id})
    assert deleted.status_code >= 400 or deleted.json().get("deleted") is False, "tenant B deleted tenant A's baseline"


def test_push_changes_requires_mutation_policy():
    env = _Env()
    r = env.cmd(ALICE, "push_changes", {"rows": [{"id": "n5", "tenant": "B", "amount": 1}], "keyFields": ["id"], "mode": "update"})
    assert r.status_code == 403


def test_sse_stream_initial_frame_is_scoped(monkeypatch):
    env = _Env()
    env.current = ALICE
    from pivot_engine.streaming.subscription_broker import SubscriptionBroker

    async def _no_more_events(self, *_args, **_kwargs):  # end the stream after the snapshot
        return
        yield  # pragma: no cover

    monkeypatch.setattr(SubscriptionBroker, "stream_events", _no_more_events)
    with env.client.stream(
        "POST", "/v1/runtime/stream",
        json={"table": TABLE, "state": STATE, "payload": {"start": 0, "end": 50, "maxFrames": 1}},
    ) as response:
        first = b""
        for chunk in response.iter_bytes():
            first += chunk
            if b"\n\n" in first:
                break
    text = first.decode("utf-8", "replace")
    data = [line[5:].strip() for line in text.splitlines() if line.startswith("data:")]
    assert data, text[:200]
    body = json.loads(data[0])
    found = leaks(json.dumps(body), _rows_from(body))
    assert not found, f"sse: {found}"
