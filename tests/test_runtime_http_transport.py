"""Contract tests for the host-neutral HTTP Pivot Runtime Protocol V1."""

from dataclasses import dataclass, field
import logging
import asyncio
import threading
import time
import os
import uuid
from pathlib import Path
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine.runtime import (
    PivotServiceResponse,
    RuntimeHttpGateway,
    RuntimeDatasetAuthorizer,
    register_runtime_http_transport,
)
import pivot_engine.runtime.payload_store as payload_store_module
from pivot_engine.runtime.distributed import DistributedRuntimeSettings, RedisRuntimeState
from pivot_engine.runtime.payload_store import RedisPayloadBackend, RuntimePayloadStore
from pivot_engine.resource_governor import ResourceExhaustedError, ResourceGovernor, ResourceKind, ResourceLimits
from pivot_engine.runtime import PivotRuntimeService
from pivot_engine.runtime.models import PivotRequestContext
from pivot_engine.runtime.export_jobs import ExportJobSnapshot
from pivot_engine.runtime.export_job_store import LocalExportJobStore
from pivot_engine.runtime.contracts import validate_runtime_request_v1
from pivot_engine.tanstack_adapter import TanStackResponse


def _job_request(action, job_id=None, *, request_id=None, **payload):
    return {
        **_runtime_request(request_id=request_id or (f"export-start-{uuid.uuid4().hex}" if action == "start" else f"export-{action}")),
        "kind": "export", "state": {},
        "payload": {"action": action, **({"jobId": job_id} if job_id else {}), **payload},
    }


class _JobAdapter:
    def __init__(self, *, slow=False):
        self.started = threading.Event()
        self.slow = slow
        self.calls = 0

    async def handle_request(self, *_args, **_kwargs):
        self.calls += 1
        self.started.set()
        if self.slow:
            await asyncio.Event().wait()
        return TanStackResponse(data=[{"value": 17}], columns=[{"id": "value"}], total_rows=1)

    handle_virtual_scroll_request = handle_request


def _job_host(*, slow=False, distributed=None, governor=None):
    adapter = _JobAdapter(slow=slow)
    service = PivotRuntimeService(lambda: adapter, resource_governor=governor)
    current = [_User("alice", {"accounting_tenant": "trusted"})]
    app = FastAPI()
    gateway = register_runtime_http_transport(
        app, lambda: service, current_user_dependency=lambda: current[0],
        dataset_authorizer=_dataset_policy("export"), distributed_runtime=distributed,
    )
    return app, gateway, adapter, current


def test_v1_data_envelope_binds_cells_and_schema_to_layout_revision():
    _app, gateway, _adapter, _current = _job_host()
    context = PivotRequestContext.from_frontend(
        table="sales", trigger_prop="grid.viewport", viewport={"start": 0, "end": 1},
    )
    revision = "a" * 64
    envelope = gateway._response_from_service(
        PivotServiceResponse(status="data", data=[{"sales": 7}], total_rows=1,
                             col_schema={"layoutRevision": revision, "columns": [],
                                         "total_center_cols": 0}, layout_revision=revision),
        request={"kind": "data", "requestId": "layout-data"}, context=context,
    )
    assert envelope["payload"]["layoutRevision"] == revision
    assert envelope["payload"]["colSchema"]["layoutRevision"] == revision
    retry = gateway._response_from_service(
        PivotServiceResponse(status="layout_required"),
        request={"kind": "data", "requestId": "layout-retry"}, context=context,
    )
    assert retry["status"] == "layout_required"


def _wait_job(client, job_id, phase):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.post("/v1/runtime", json=_job_request("status", job_id))
        assert response.status_code == 200, response.text
        if response.json()["payload"]["phase"] == phase:
            return response.json()["payload"]
        time.sleep(.01)
    raise AssertionError(f"Job did not become {phase}: {response.text}")


def test_export_start_replay_uses_one_server_job_and_new_request_starts_another():
    app, gateway, adapter, _current = _job_host()
    with TestClient(app) as client:
        envelope = _job_request("start", format="csv", request_id="export-lost-ack")
        first = client.post("/v1/runtime", json=envelope)
        assert first.status_code == 200, first.text
        replay = client.post("/v1/runtime", json=envelope)
        assert replay.status_code == 200, replay.text
        assert replay.json()["payload"]["jobId"] == first.json()["payload"]["jobId"]
        _wait_job(client, first.json()["payload"]["jobId"], "completed")
        assert adapter.calls == 1
        # Publication precedes final file/lease cleanup. Admission is only
        # promised after that cleanup, not on the first completed status.
        deadline = time.monotonic() + 5
        while gateway.resource_governor.snapshot()["active_slots"]["export"]:
            assert time.monotonic() < deadline, "completed export did not release admission"
            time.sleep(.01)
        second = client.post("/v1/runtime", json=_job_request("start", format="csv"))
        assert second.status_code == 200, second.text
        assert second.json()["payload"]["jobId"] != first.json()["payload"]["jobId"]
        _wait_job(client, second.json()["payload"]["jobId"], "completed")
        assert adapter.calls == 2


def test_gateway_shutdown_closes_active_runner_without_removed_fastapi_app_hook():
    class CurrentFastAPI(FastAPI):
        def __getattribute__(self, name):
            if name == "add_event_handler":
                raise AttributeError("Removed from FastAPI 0.145; register on router")
            return super().__getattribute__(name)

    adapter = _JobAdapter(slow=True)
    service = PivotRuntimeService(lambda: adapter)
    app = CurrentFastAPI()
    gateway = register_runtime_http_transport(
        app, lambda: service, current_user_dependency=lambda: _User("alice"),
        dataset_authorizer=_dataset_policy("export"),
    )
    with TestClient(app) as client:
        response = client.post("/v1/runtime", json=_job_request("start", format="csv"))
        assert response.status_code == 200
        assert adapter.started.wait(3)
        runner = gateway.export_job_runner
        assert not runner._closed
    assert runner._closed
    assert service._router.lifecycle.resource_governor.snapshot()["active_slots"]["export"] == 0


def test_http_export_job_completes_downloads_and_status_does_not_requery():
    app, gateway, adapter, current = _job_host()
    with TestClient(app) as client:
        response = client.post("/v1/runtime", json=_job_request("start", format="csv"))
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert "jobId" in payload, payload
        done = _wait_job(client, payload["jobId"], "completed")
        assert done["rowsWritten"] == 1 and done["bytesWritten"] > 0
        assert done["totalRows"] is None
        assert adapter.calls == 1
        assert done["payloadRef"]["url"].startswith("/v1/runtime/payload/")
        download = client.get(done["payloadRef"]["url"])
        assert download.status_code == 200
        assert download.content.decode("utf-8-sig").splitlines() == ["Value", "17"]
        current[0] = _User("alice", {"accounting_tenant": "other"})
        assert client.get(done["payloadRef"]["url"]).status_code == 404
        current[0] = _User("bob", {"accounting_tenant": "trusted"})
        assert client.get(done["payloadRef"]["url"]).status_code == 404
    assert gateway.resource_governor.snapshot()["active_slots"]["export"] == 0
    gateway.payload_store.clear()


def test_export_job_uses_frontend_state_override_for_flat_layout():
    app, gateway, adapter, current = _job_host()
    envelope = _job_request("start", format="csv", layout_mode="tabular", state_override={
        "rowFields": ["region"], "colFields": [],
        "valConfigs": [{"field": "sales", "agg": "sum"}],
        "showSubtotals": False, "showRowTotals": False, "showColTotals": False,
    })
    request = validate_runtime_request_v1(envelope)
    scope = gateway.dataset_authorizer.authorize(
        principal=current[0], table="sales", operation="export",
    )
    state = gateway._state_from_request(request, principal=current[0], row_scope=scope)
    assert state.row_fields == ["region"]
    assert state.val_configs == [{"field": "sales", "agg": "sum"}]
    assert state.show_subtotals is False
    assert state.layout_mode == "tabular"


@pytest.mark.parametrize("action", ["status", "cancel"])
@pytest.mark.parametrize("attack", ["subject", "account", "table", "unknown"])
def test_export_job_lookup_hides_identity_and_progress_and_logs_denial(action, attack, caplog):
    app, gateway, adapter, current = _job_host()
    permission = {"roles": {"viewer"}, "operations": {"export"}, "rowSecurity": {"mode": "global"}}
    gateway.dataset_authorizer = RuntimeDatasetAuthorizer({"sales": permission, "other": permission})
    # A private in-progress snapshot makes any leaked counters observable.
    store = gateway.export_job_store
    store.create(ExportJobSnapshot("secret-job", "alice", "trusted", "sales"))
    owner = store.claim("secret-job", "private-owner")
    store.advance("secret-job", owner.owner_epoch, {"rows_written": 4711, "bytes_written": 9001})
    envelope = _job_request(action, "missing-job" if attack == "unknown" else "secret-job",
                            accounting_tenant="trusted", subject="alice")
    if attack == "subject":
        current[0] = _User("bob", {"accounting_tenant": "trusted"})
    elif attack == "account":
        current[0] = _User("alice", {"accounting_tenant": "other"})
    elif attack == "table":
        envelope["table"] = "other"
    with TestClient(app) as client, caplog.at_level(logging.WARNING):
        response = client.post("/v1/runtime", json=envelope)
    assert response.status_code == 404
    assert response.json()["payload"] == {"code": "export_job_not_found"}
    assert all(secret not in response.text for secret in ["4711", "9001", "trusted", "secret-job", "private-owner"])
    assert "event=authorization_denied" in caplog.text and "operation=export" in caplog.text
    assert adapter.calls == 0
    assert store.get("secret-job", "alice", "sales", accounting_account_id="trusted").phase == "running"


def test_export_jobs_cross_worker_cancel_uses_registry_and_no_view_query():
    shared = fakeredis.FakeRedis(decode_responses=False)
    app_a, gateway_a, adapter_a, _ = _job_host(slow=True, distributed=_distributed_payload_state(shared))
    app_b, gateway_b, adapter_b, _ = _job_host(distributed=_distributed_payload_state(shared))
    with TestClient(app_a) as owner, TestClient(app_b) as other:
        response = owner.post("/v1/runtime", json=_job_request("start", format="csv"))
        assert "jobId" in response.json()["payload"], response.text
        job_id = response.json()["payload"]["jobId"]
        assert adapter_a.started.wait(2)
        cancelling = other.post("/v1/runtime", json=_job_request("cancel", job_id))
        assert cancelling.status_code == 200
        assert cancelling.json()["payload"]["phase"] in {"cancelling", "cancelled"}
        _wait_job(other, job_id, "cancelled")
        assert adapter_a.calls == 1 and adapter_b.calls == 0
        assert gateway_a.resource_governor.snapshot()["active_slots"]["export"] == 0
        assert gateway_b._export_job_runner is None


def test_export_start_replay_on_another_worker_keeps_original_owner():
    shared = fakeredis.FakeRedis(decode_responses=False)
    app_a, gateway_a, adapter_a, _ = _job_host(slow=True, distributed=_distributed_payload_state(shared))
    app_b, gateway_b, adapter_b, _ = _job_host(distributed=_distributed_payload_state(shared))
    envelope = _job_request("start", format="csv", request_id="lost-start-cross-worker")
    with TestClient(app_a) as owner, TestClient(app_b) as other:
        first = owner.post("/v1/runtime", json=envelope)
        assert first.status_code == 200, first.text
        assert adapter_a.started.wait(2)
        replay = other.post("/v1/runtime", json=envelope)
        assert replay.status_code == 200, replay.text
        assert replay.json()["payload"]["jobId"] == first.json()["payload"]["jobId"]
        assert adapter_a.calls == 1 and adapter_b.calls == 0
        cancelled = other.post("/v1/runtime", json=_job_request("cancel", first.json()["payload"]["jobId"]))
        assert cancelled.status_code == 200
        _wait_job(other, first.json()["payload"]["jobId"], "cancelled")


def test_export_job_admission_rejection_is_stable_v1_error():
    governor = ResourceGovernor(ResourceLimits.for_testing(max_exports=1), rss_bytes_getter=lambda: 0)
    app, gateway, adapter, _ = _job_host(governor=governor)
    with TestClient(app) as client, governor.reserve(ResourceKind.EXPORT, 1):
        response = client.post("/v1/runtime", json=_job_request("start", format="csv"))
        assert response.status_code == 503
        assert response.json()["payload"] == {
            "code": "resource_exhausted", "resourceKind": "export", "retryAfterMs": 1000,
        }
        assert "jobId" not in response.json()["payload"] and adapter.calls == 0
    gateway.payload_store.clear()


@pytest.mark.parametrize("action", ["start", "status", "cancel"])
def test_distributed_job_registry_outage_is_stable_and_fail_closed(action):
    class OfflineRedis:
        def ping(self):
            return True

        def __getattr__(self, _name):
            def fail(*_args, **_kwargs):
                raise ConnectionError("SECRET redis credentials")
            return fail
    app, gateway, adapter, _ = _job_host(distributed=_distributed_payload_state(OfflineRedis()))
    with TestClient(app) as client:
        response = client.post("/v1/runtime", json=_job_request(action, "missing" if action != "start" else None))
        assert response.status_code == 503
        assert response.json()["payload"] == {"code": "distributed_runtime_unavailable"}
        assert "SECRET" not in response.text and adapter.calls == 0


def test_expired_export_job_is_missing_and_shutdown_cleans_running_work():
    app, gateway, adapter, _ = _job_host(slow=True)
    now = [100.0]
    gateway._export_job_store = LocalExportJobStore(ttl_seconds=1, clock=lambda: now[0])
    gateway.export_job_store.create(ExportJobSnapshot("expired", "alice", "trusted", "sales"))
    gateway.export_job_store.request_cancel("expired")
    now[0] = 102
    with TestClient(app) as client:
        missing = client.post("/v1/runtime", json=_job_request("status", "expired"))
        assert missing.status_code == 404
        start = client.post("/v1/runtime", json=_job_request("start", format="csv"))
        assert "jobId" in start.json()["payload"], start.text
        assert adapter.started.wait(2)
    assert gateway.resource_governor.snapshot()["active_slots"]["export"] == 0
    assert gateway.export_job_store.get(start.json()["payload"]["jobId"], "alice", "sales").phase == "cancelled"


def test_dash_export_jobs_share_authenticated_semantics_and_completed_download():
    from dash import Dash, html
    from dash_tanstack_pivot import DashTanstackPivot
    from pivot_engine.runtime.dash_callbacks import register_dash_pivot_transport_callback
    adapter = _JobAdapter()
    service = PivotRuntimeService(lambda: adapter)
    current = [_User("alice", {"accounting_tenant": "trusted"})]
    app = Dash(__name__)
    app.layout = html.Div([DashTanstackPivot(id="jobs", table="sales")])
    register_dash_pivot_transport_callback(
        app, lambda: service, pivot_id="jobs", dataset_authorizer=_dataset_policy("export"),
        principal_getter=lambda _request: current[0], require_dataset_authorization=True,
    )
    client = app.server.test_client()
    callback = app.callback_map["jobs.runtimeResponse"]

    def post(envelope):
        response = client.post("/_dash-update-component", json={
            "output": "jobs.runtimeResponse", "outputs": {"id": "jobs", "property": "runtimeResponse"},
            "changedPropIds": ["jobs.runtimeRequest"],
            "inputs": [{**item, "value": envelope if item["property"] == "runtimeRequest" else None}
                       for item in callback["inputs"]],
            "state": [{**item, "value": "sales" if item["property"] == "table" else None}
                      for item in callback["state"]],
        })
        assert response.status_code == 200, response.data
        return response.json["response"]["jobs"]["runtimeResponse"]

    try:
        started = post(_job_request("start", format="csv"))
        assert "jobId" in started["payload"], started
        job_id = started["payload"]["jobId"]
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            completed = post(_job_request("status", job_id))
            if completed["payload"]["phase"] == "completed":
                break
            time.sleep(.01)
        assert completed["payload"]["phase"] == "completed"
        assert adapter.calls == 1
        url = completed["payload"]["payloadRef"]["url"]
        download = client.get(url)
        assert download.status_code == 200
        assert download.data.decode("utf-8-sig").splitlines() == ["Value", "17"]
        current[0] = _User("alice", {"accounting_tenant": "other"})
        denied = post(_job_request("cancel", job_id))
        assert denied["payload"] == {"code": "export_job_not_found"}
        assert client.get(url).status_code == 404
        current[0] = _User("bob", {"accounting_tenant": "trusted"})
        assert post(_job_request("status", job_id))["payload"] == {"code": "export_job_not_found"}
        assert client.get(url).status_code == 404
        assert adapter.calls == 1
    finally:
        _close_dash_jobs(app)


def _dash_job_host(registrations):
    from dash import Dash, html
    from dash_tanstack_pivot import DashTanstackPivot
    from pivot_engine.runtime.dash_callbacks import register_dash_pivot_transport_callback
    app = Dash(__name__)
    app.layout = html.Div([DashTanstackPivot(id=name, table=table)
                           for name, table, *_ in registrations])
    for name, table, service, policy, getter in registrations:
        register_dash_pivot_transport_callback(
            app, lambda service=service: service, pivot_id=name, dataset_authorizer=policy,
            principal_getter=getter, require_dataset_authorization=True,
        )
    return app


def _dash_job_post(app, client, name, envelope, table="sales"):
    callback = app.callback_map[f"{name}.runtimeResponse"]
    response = client.post("/_dash-update-component", json={
        "output": f"{name}.runtimeResponse", "outputs": {"id": name, "property": "runtimeResponse"},
        "changedPropIds": [f"{name}.runtimeRequest"],
        "inputs": [{**item, "value": envelope if item["property"] == "runtimeRequest" else None}
                   for item in callback["inputs"]],
        "state": [{**item, "value": table if item["property"] == "table" else None}
                  for item in callback["state"]],
    })
    assert response.status_code == 200, response.data
    return response.json["response"][name]["runtimeResponse"]


def _dash_job_done(app, client, name, job_id, table="sales"):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = _dash_job_post(app, client, name, _job_request("status", job_id), table)
        if response["payload"].get("phase") == "completed":
            return response["payload"]
        assert response["status"] != "error", response
        time.sleep(.01)
    raise AssertionError(response)


def _close_dash_jobs(app):
    close = getattr(app, "_pivot_runtime_close_export_jobs", None)
    if close:
        close()
    else:
        gateway = getattr(app, "_pivot_runtime_export_gateway", None)
        if gateway:
            gateway.close()
    store = getattr(app, "_pivot_runtime_payload_store", None)
    if store:
        store.clear()


@pytest.mark.parametrize("table_b", ["sales", "other"])
def test_dash_export_jobs_use_each_pivots_service_and_policy(table_b):
    adapters = [_JobAdapter(), _JobAdapter()]
    services = [PivotRuntimeService(lambda adapter=adapter: adapter) for adapter in adapters]
    principal = _User("alice", {"accounting_tenant": "trusted"})
    policy_b = RuntimeDatasetAuthorizer({table_b: {
        "roles": {"viewer"}, "operations": {"export"}, "rowSecurity": {"mode": "global"},
    }})
    app = _dash_job_host([
        ("a", "sales", services[0], _dataset_policy("export"), lambda _: principal),
        ("b", table_b, services[1], policy_b, lambda _: principal),
    ])
    client = app.server.test_client()
    try:
        for name, table in [("a", "sales"), ("b", table_b)]:
            start = _dash_job_post(app, client, name, _job_request("start", format="csv"), table)
            assert "jobId" in start["payload"], start
            _dash_job_done(app, client, name, start["payload"]["jobId"], table)
        assert [adapter.calls for adapter in adapters] == [1, 1]
    finally:
        _close_dash_jobs(app)


def test_dash_export_download_keeps_own_registration_auth_and_session():
    service = PivotRuntimeService(lambda: _JobAdapter())
    current = [_User("alice", {"accounting_tenant": "trusted"})]
    other = _User("alice", {"accounting_tenant": "trusted"})
    app = _dash_job_host([
        ("a", "sales", service, _dataset_policy("export"), lambda _: current[0]),
        ("b", "sales", service, _dataset_policy("export"), lambda _: other),
    ])
    client = app.server.test_client()
    try:
        start = _dash_job_post(app, client, "a", _job_request("start", format="csv"))
        done = _dash_job_done(app, client, "a", start["payload"]["jobId"])
        url = done["payloadRef"]["url"]
        assert client.get(url).status_code == 200
        assert app.server.test_client().get(url).status_code == 404
        current[0] = _User("alice", {"accounting_tenant": "trusted"}, role="revoked")
        assert client.get(url).status_code == 404
    finally:
        _close_dash_jobs(app)


def test_dash_concurrent_first_export_starts_remain_pollable(monkeypatch):
    import pivot_engine.runtime.http_transport as transport
    constructor = transport.RuntimeHttpGateway
    gateways = []
    def delayed_gateway(*args, **kwargs):
        time.sleep(.05)
        gateway = constructor(*args, **kwargs)
        gateways.append(gateway)
        return gateway
    monkeypatch.setattr(transport, "RuntimeHttpGateway", delayed_gateway)
    adapter = _JobAdapter()
    governor = ResourceGovernor(ResourceLimits(tenant_max_exports=2), rss_bytes_getter=lambda: 0)
    service = PivotRuntimeService(lambda: adapter, resource_governor=governor)
    app = _dash_job_host([("a", "sales", service, _dataset_policy("export"),
                          lambda _: _User("alice", {"accounting_tenant": "trusted"}))])
    # Finish Flask setup before the competing requests.
    app.server.test_client().get("/_dash-layout")
    barrier = threading.Barrier(2)
    def start():
        client = app.server.test_client()
        barrier.wait()
        return client, _dash_job_post(app, client, "a", _job_request("start", format="csv"))
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: start(), range(2)))
        for client, response in results:
            assert "jobId" in response["payload"], response
            _dash_job_done(app, client, "a", response["payload"]["jobId"])
        assert adapter.calls == 2
    finally:
        for gateway in gateways:
            gateway.close()
        _close_dash_jobs(app)


@pytest.mark.parametrize("resource", ["export_job_store", "export_job_runner", "payload_store"])
def test_gateway_concurrent_lazy_resources_have_one_owner(resource, monkeypatch):
    import pivot_engine.runtime.http_transport as transport
    factories = {"export_job_store": "export_job_store_for_runtime",
                 "export_job_runner": "ExportJobRunner", "payload_store": "runtime_payload_store_from_env"}
    factory_name = factories[resource]
    original = getattr(transport, factory_name)
    created = []
    def delayed_factory(*args, **kwargs):
        time.sleep(.05)
        value = original(*args, **kwargs)
        created.append(value)
        return value
    monkeypatch.setattr(transport, factory_name, delayed_factory)
    gateway = RuntimeHttpGateway(lambda: service)
    service = PivotRuntimeService(lambda: _JobAdapter())
    barrier = threading.Barrier(2)
    def get_resource(_):
        barrier.wait()
        return getattr(gateway, resource)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.map(get_resource, range(2))
        assert first is second
    finally:
        for value in created:
            if resource == "export_job_runner":
                value.close()
        gateway.close()


@pytest.mark.parametrize("prejoin_hook", [True, False])
@pytest.mark.parametrize("pivot_count", [1, 2])
def test_dash_shutdown_hook_stops_running_export_and_releases_admission(monkeypatch, prejoin_hook, pivot_count):
    import atexit
    callbacks = []
    monkeypatch.setattr(atexit, "register", lambda function, *args: callbacks.append((function, args)))
    if not prejoin_hook:
        monkeypatch.setattr(threading, "_register_atexit", None)
    adapter = _JobAdapter(slow=True)
    governor = ResourceGovernor(ResourceLimits(tenant_max_exports=2), rss_bytes_getter=lambda: 0)
    service = PivotRuntimeService(lambda: adapter, resource_governor=governor)
    principal = _User("alice", {"accounting_tenant": "trusted"})
    app = _dash_job_host([
        (name, "sales", service, _dataset_policy("export"), lambda _: principal) for name in ["a", "b"]
    ])
    client = app.server.test_client()
    try:
        starts = {name: _dash_job_post(app, client, name, _job_request("start", format="csv"))
                  for name in ["a", "b"][:pivot_count]}
        assert adapter.started.wait(2)
        assert service.resource_governor.snapshot()["active_slots"]["export"] == pivot_count
        assert len(callbacks) == 1
        function, args = callbacks[0]
        function(*args)
        assert service.resource_governor.snapshot()["active_slots"]["export"] == 0
        for name, start in starts.items():
            status = _dash_job_post(app, client, name, _job_request("status", start["payload"]["jobId"]))
            assert status["payload"]["phase"] == "cancelled"
        app._pivot_runtime_close_export_jobs()  # explicit host cleanup is idempotent
    finally:
        _close_dash_jobs(app)


def test_dash_process_exit_cancels_exports_before_executor_threads_join():
    script = textwrap.dedent('''
        import runpy
        import atexit
        helpers = runpy.run_path("tests/test_runtime_http_transport.py")
        globals().update({key: value for key, value in helpers.items() if not key.startswith("__")})
        adapter = _JobAdapter(slow=True)
        service = PivotRuntimeService(lambda: adapter)
        app = _dash_job_host([("a", "sales", service, _dataset_policy("export"),
                              lambda _: _User("alice", {"accounting_tenant": "trusted"}))])
        response = _dash_job_post(app, app.server.test_client(), "a", _job_request("start", format="csv"))
        assert "jobId" in response["payload"], response
        assert adapter.started.wait(2)
        def verify_cleanup():
            assert service.resource_governor.snapshot()["active_slots"]["export"] == 0
            print("export-cleanup-complete", flush=True)
        atexit.register(verify_cleanup)
    ''')
    env = dict(os.environ)
    root = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = str(root / "dash_tanstack_pivot" / "pivot_engine") + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run([sys.executable, "-c", script], cwd=root, env=env,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert "export-cleanup-complete" in result.stdout, result.stderr


@dataclass
class _User:
    id: str
    attributes: dict = field(default_factory=dict)
    role: str = "viewer"


class _Service:
    def __init__(self):
        self.calls = []

    async def process_async(self, state, context):
        self.calls.append((state, context))
        return PivotServiceResponse(
            status="data",
            data=[{"region": "North", "amount_sum": 12}],
            total_rows=1,
            columns=[{"id": "region"}],
            data_offset=context.start_row,
            data_version=context.window_seq,
        )


def _runtime_request(*, request_id="window-1"):
    return {
        "schemaVersion": 1,
        "kind": "data",
        "requestId": request_id,
        "table": "sales",
        # This must not become the server session namespace.
        "sessionId": "untrusted-browser-session",
        "clientInstance": "browser-tab-a",
        "stateEpoch": 3,
        "windowSeq": 7,
        "state": {
            "rowFields": ["region"],
            "filters": {"country": {"type": "eq", "value": "ignored"}},
        },
        "payload": {"intent": "viewport", "start": 0, "end": 49},
    }


def test_http_overload_is_safe_retryable_v1_error():
    class ExhaustedService(_Service):
        async def process_async(self, state, context):
            raise ResourceExhaustedError(ResourceKind.QUERY, reason="SECRET backend memory/path", retry_after_ms=1501)

    app = FastAPI()
    register_runtime_http_transport(
        app, lambda: ExhaustedService(), current_user_dependency=lambda: _User("alice"),
        dataset_authorizer=_dataset_policy("read"),
    )
    response = TestClient(app).post("/v1/runtime", json=_runtime_request())
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "2"
    assert response.json()["status"] == "error"
    assert response.json()["requestId"] == "window-1"
    assert response.json()["payload"] == {
        "code": "resource_exhausted", "resourceKind": "query", "retryAfterMs": 1501,
    }
    assert "SECRET" not in response.text


def test_http_filter_options_uses_service_query_governor():
    governor = ResourceGovernor(ResourceLimits.for_testing(max_queries=1), rss_bytes_getter=lambda: 0)

    class Adapter:
        async def handle_request(self, *_args, **_kwargs):
            pytest.fail("filter request must be rejected before reaching backend")

    service = PivotRuntimeService(lambda: Adapter(), resource_governor=governor)
    app = FastAPI()
    register_runtime_http_transport(
        app, lambda: service, current_user_dependency=lambda: _User("alice"),
        dataset_authorizer=_dataset_policy("read"),
    )
    envelope = {**_runtime_request(), "kind": "filter_options", "payload": {"columnId": "region"}}
    with governor.reserve(ResourceKind.QUERY, 1):
        response = TestClient(app).post("/v1/runtime", json=envelope)
    assert response.status_code == 503
    assert response.json()["payload"]["resourceKind"] == "query"


def test_gateway_payload_retention_shares_service_resource_budget():
    governor = ResourceGovernor(ResourceLimits.for_testing(payload_bytes=128), rss_bytes_getter=lambda: 0)
    service = PivotRuntimeService(lambda: None, resource_governor=governor)
    gateway = RuntimeHttpGateway(lambda: service)
    with pytest.raises(ResourceExhaustedError):
        gateway.payload_store.put_bytes(b"x" * 129)
    gateway.payload_store.put_bytes(b"1234")
    assert 4 <= governor.snapshot()["reserved_bytes_by_kind"]["payload"] <= 128
    gateway.payload_store.clear()
    assert governor.snapshot()["reserved_bytes"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("file_payload", [False, True])
async def test_download_slot_covers_sending_and_releases_on_disconnect(tmp_path, file_payload):
    governor = ResourceGovernor(ResourceLimits.for_testing(max_downloads=1, download_bytes=64), rss_bytes_getter=lambda: 0)
    service = PivotRuntimeService(lambda: None, resource_governor=governor)
    app = FastAPI()
    gateway = register_runtime_http_transport(
        app, lambda: service, current_user_dependency=lambda: _User("alice"),
    )
    if file_payload:
        payload_path = tmp_path / "export.csv"
        payload_path.write_bytes(b"hello")
        reference = gateway.payload_store.put_file(str(payload_path), metadata={"subject": "alice"})
    else:
        reference = gateway.payload_store.put_bytes(b"hello", metadata={"subject": "alice"})
    token = reference["id"]
    sending, release = asyncio.Event(), asyncio.Event()
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET",
        "scheme": "http", "path": f"/v1/runtime/payload/{token}", "raw_path": b"/",
        "query_string": b"", "headers": [], "client": ("127.0.0.1", 1), "server": ("test", 80),
    }

    async def receive():
        await asyncio.Event().wait()

    async def send(message):
        if message["type"] == "http.response.body":
            sending.set()
            await release.wait()
            raise ConnectionError("client disconnected")

    task = asyncio.create_task(app(scope, receive, send))
    try:
        await asyncio.wait_for(sending.wait(), 2)
        assert governor.snapshot()["active_slots"]["download"] == 1
        with pytest.raises(ResourceExhaustedError):
            governor.reserve(ResourceKind.DOWNLOAD, 1)
        response = TestClient(app).get(f"/v1/runtime/payload/{token}")
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "1"
        assert response.json()["payload"] == {
            "code": "resource_exhausted", "resourceKind": "download", "retryAfterMs": 1000,
        }
    finally:
        release.set()
        with pytest.raises(ConnectionError):
            await task
        gateway.payload_store.clear()
    assert governor.snapshot()["reserved_bytes"] == 0


def _dataset_policy(
    *operations: str,
    row_security: dict | None = None,
) -> RuntimeDatasetAuthorizer:
    permission = {
        "roles": {"viewer", "editor"},
        "rowSecurity": row_security or {"mode": "global"},
    }
    if operations:
        permission["operations"] = set(operations)
    return RuntimeDatasetAuthorizer({"sales": permission})


def _distributed_payload_state(client):
    return RedisRuntimeState(
        DistributedRuntimeSettings(
            enabled=True,
            redis_url="rediss://user:pass@example/0",
            key_prefix="http-payload-test",
        ),
        client=client,
    )


def test_http_gateway_uses_configured_distributed_payload_store(monkeypatch):
    state = _distributed_payload_state(fakeredis.FakeRedis(decode_responses=False))
    monkeypatch.setattr(
        payload_store_module,
        "distributed_runtime_from_env",
        lambda **_kwargs: state,
    )

    gateway = RuntimeHttpGateway(lambda: _Service())

    assert isinstance(gateway.payload_store._backend, RedisPayloadBackend)


def test_distributed_http_transport_rejects_all_edit_commands_before_policy_or_service():
    class _MutationAuthorizer:
        def authorize(self, **_kwargs):
            raise AssertionError("distributed edit denial must precede mutation authorization")

    service = _Service()

    async def current_user():
        return _User("alice", {"tenant": "A"}, "editor")

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy("mutate"),
        mutation_authorizer=_MutationAuthorizer(),
        distributed_runtime=_distributed_payload_state(
            fakeredis.FakeRedis(decode_responses=False)
        ),
    )
    client = TestClient(app)

    for kind, payload in (
        ("update", {"updates": [{"rowId": "North", "value": 1}]}),
        ("transaction", {"action": "undo"}),
        ("transaction", {"action": "redo"}),
        ("transaction", {"action": "history"}),
    ):
        response = client.post(
            "/v1/runtime",
            json={
                **_runtime_request(request_id=f"distributed-{kind}-{payload.get('action', 'update')}"),
                "kind": kind,
                "payload": payload,
            },
        )

        assert response.status_code == 200
        assert response.json()["status"] == "error"
        assert response.json()["message"] == "editing_not_available_in_distributed_mode"

    assert service.calls == []


def test_http_transport_downloads_shared_payload_from_another_worker():
    server = fakeredis.FakeServer()
    producer_store = RuntimePayloadStore(
        backend=RedisPayloadBackend(
            _distributed_payload_state(
                fakeredis.FakeRedis(server=server, decode_responses=False)
            )
        )
    )
    consumer_store = RuntimePayloadStore(
        backend=RedisPayloadBackend(
            _distributed_payload_state(
                fakeredis.FakeRedis(server=server, decode_responses=False)
            )
        )
    )
    active_user = {"value": _User("alice")}

    async def current_user():
        return active_user["value"]

    producer_app = FastAPI()
    register_runtime_http_transport(
        producer_app,
        lambda: _Service(),
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy("read"),
        payload_store=producer_store,
        inline_payload_bytes=1,
    )
    consumer_app = FastAPI()
    register_runtime_http_transport(
        consumer_app,
        lambda: _Service(),
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy("read"),
        payload_store=consumer_store,
        inline_payload_bytes=1,
    )

    reference = TestClient(producer_app).post("/v1/runtime", json=_runtime_request()).json()["payload"]["payloadRef"]
    assert TestClient(consumer_app).get(reference["url"]).status_code == 200

    active_user["value"] = _User("mallory")
    assert TestClient(consumer_app).get(reference["url"]).status_code == 404


def test_http_transport_uses_canonical_v1_and_server_bound_identity():
    service = _Service()
    active_user = {"value": _User("alice", {"tenant": "acme"})}

    async def current_user():
        return active_user["value"]

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy(
            "read",
            row_security={"mode": "claims", "claims": {"tenant": "tenant"}},
        ),
        inline_payload_bytes=1024 * 1024,
    )
    client = TestClient(app)

    response = client.post("/v1/runtime", json=_runtime_request())

    assert response.status_code == 200
    envelope = response.json()
    assert envelope["schemaVersion"] == 1
    assert envelope["kind"] == "data"
    assert envelope["sessionId"] == "principal:alice"
    assert envelope["clientInstance"] == "browser-tab-a"
    assert "session_id" not in envelope
    assert envelope["payload"]["data"][0]["amount_sum"] == 12
    assert envelope["profile"]["transport"]["protocol"] == "v1"

    state, context = service.calls[0]
    assert context.session_id == "principal:alice"
    assert context.principal.id == "alice"
    assert state.filters["tenant"] == {"type": "eq", "value": "acme"}


def test_http_transport_externalizes_large_data_and_scopes_payload_to_principal():
    service = _Service()
    active_user = {"value": _User("alice")}

    async def current_user():
        return active_user["value"]

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy("read"),
        inline_payload_bytes=1,
    )
    client = TestClient(app)

    response = client.post("/v1/runtime", json=_runtime_request())
    assert response.status_code == 200
    payload_ref = response.json()["payload"]["payloadRef"]
    assert payload_ref["url"].startswith("/v1/runtime/payload/")

    payload_response = client.get(payload_ref["url"])
    assert payload_response.status_code == 200
    assert payload_response.json()["data"][0]["region"] == "North"
    assert payload_response.headers["etag"]

    active_user["value"] = _User("mallory")
    assert client.get(payload_ref["url"]).status_code == 404


def test_http_transport_rejects_noncanonical_envelopes():
    app = FastAPI()

    async def current_user():
        return _User("alice")

    register_runtime_http_transport(
        app,
        lambda: _Service(),
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy("read"),
    )
    response = TestClient(app).post(
        "/v1/runtime",
        json={**_runtime_request(), "legacyOnly": True},
    )

    assert response.status_code == 422
    assert response.json()["schemaVersion"] == 1
    assert response.json()["status"] == "error"
    assert "legacyOnly" in response.json()["message"]


def test_http_transport_dataset_denial_emits_safe_security_event(caplog):
    async def current_user():
        return _User("viewer-42", {"tenant": "A", "secret": "must-not-log"})

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: _Service(),
        current_user_dependency=current_user,
        dataset_authorizer=RuntimeDatasetAuthorizer(),
    )

    with caplog.at_level(logging.WARNING, logger="pivot_engine.security_events"):
        response = TestClient(app).post("/v1/runtime", json=_runtime_request())

    assert response.status_code == 403
    assert "event=authorization_denied" in caplog.text
    assert "subject=viewer-42" in caplog.text
    assert "table=sales" in caplog.text
    assert "operation=read" in caplog.text
    assert "must-not-log" not in caplog.text


def test_http_transport_externalizes_export_files_as_authorized_downloads(tmp_path):
    export_path = tmp_path / "pivot_export.csv"
    export_path.write_bytes(b"Region,Sales\r\nNorth,100\r\n")

    class ExportService:
        async def process_async(self, _state, _context):
            return PivotServiceResponse(
                status="export",
                export_payload={
                    "contentPath": str(export_path),
                    "contentLength": export_path.stat().st_size,
                    "contentType": "text/csv;charset=utf-8",
                    "format": "csv",
                    "filename": "pivot_export.csv",
                    "rows": 1,
                    "columns": 2,
                },
            )

    active_user = {"value": _User("alice")}

    async def current_user():
        return active_user["value"]

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: ExportService(),
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy("export"),
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runtime",
        json={**_runtime_request(request_id="export-1"), "kind": "export", "payload": {"format": "csv"}},
    )

    assert response.status_code == 200
    payload = response.json()["payload"]
    assert payload["payloadInline"] is False
    assert "contentPath" not in payload
    payload_ref = payload["payloadRef"]
    assert payload_ref["url"].startswith("/v1/runtime/payload/")
    assert payload_ref["filename"] == "pivot_export.csv"

    download = client.get(payload_ref["url"])
    assert download.status_code == 200
    assert download.content == b"Region,Sales\r\nNorth,100\r\n"
    assert download.headers["content-type"].startswith("text/csv")
    assert 'filename="pivot_export.csv"' in download.headers["content-disposition"]

    active_user["value"] = _User("mallory")
    assert client.get(payload_ref["url"]).status_code == 404


def test_http_export_limit_error_preserves_exact_v1_code_and_request_id():
    class LimitedExportService:
        async def process_async(self, _state, _context):
            return PivotServiceResponse(
                status="error",
                message="Export limit exceeded",
                export_payload={"limitCode": "PIVOT_EXPORT_MAX_ROWS"},
            )

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: LimitedExportService(),
        current_user_dependency=lambda: _User("alice"),
        dataset_authorizer=_dataset_policy("export"),
    )
    response = TestClient(app).post(
        "/v1/runtime",
        json={
            **_runtime_request(request_id="export-limit-1"),
            "kind": "export",
            "payload": {"format": "csv"},
        },
    )

    assert response.status_code == 200
    envelope = response.json()
    assert envelope["schemaVersion"] == 1
    assert envelope["kind"] == "export"
    assert envelope["status"] == "error"
    assert envelope["payload"] == {"limitCode": "PIVOT_EXPORT_MAX_ROWS"}
    assert envelope["requestId"] == "export-limit-1"
    assert envelope["message"] == "Export limit exceeded"


def test_http_transport_mutations_are_default_deny_and_tenant_scoped_including_history():
    """A browser may name another tenant's group, but cannot mutate it."""
    import pyarrow as pa

    from pivot_engine import create_tanstack_adapter
    from pivot_engine.runtime import (
        PivotRuntimeService,
        RuntimeMutationAuthorizer,
        SessionRequestGate,
    )

    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales",
        pa.Table.from_pylist(
            [
                {"tenant": "A", "region": "North", "sales": 100, "cost": 10},
                {"tenant": "B", "region": "South", "sales": 200, "cost": 20},
            ]
        ),
    )
    service = PivotRuntimeService(
        adapter_getter=lambda: adapter,
        session_gate=SessionRequestGate(),
    )
    active_user = {"value": _User("shared", {"tenant": "A"}, "viewer")}

    async def current_user():
        return active_user["value"]

    policy = RuntimeMutationAuthorizer(
        {
            "sales": {
                "roles": {"editor"},
                "columns": {"tenant", "region", "sales"},
                "operations": {"add", "remove", "update", "upsert", "aggregate", "history"},
            }
        }
    )
    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        mutation_authorizer=policy,
        dataset_authorizer=_dataset_policy(
            "mutate",
            row_security={"mode": "claims", "claims": {"tenant": "tenant"}},
        ),
    )
    client = TestClient(app)

    def transaction(payload, *, table="sales", request_id="mutation-1"):
        return {
            "schemaVersion": 1,
            "kind": "transaction",
            "requestId": request_id,
            "table": table,
            "clientInstance": "attacker-tab",
            "stateEpoch": 1,
            "windowSeq": 1,
            "state": {
                "rowFields": ["tenant", "region"],
                "valConfigs": [
                    {"field": "sales", "agg": "sum"},
                    {"field": "cost", "agg": "sum"},
                ],
            },
            "payload": {"refreshMode": "none", **payload},
        }

    def sales_by_tenant():
        return {
            row["tenant"]: row["sales"]
            for row in adapter.controller.planner.con.table("sales")
            .execute()
            .to_dict("records")
        }

    cross_tenant_update = {
        "keyFields": ["tenant", "region"],
        "update": [
            {
                "keys": {"tenant": "B", "region": "South"},
                "values": {"sales": 999},
            }
        ],
    }

    # A viewer is rejected at the HTTP boundary before the service or adapter.
    denied = client.post("/v1/runtime", json=transaction(cross_tenant_update))
    assert denied.status_code == 403
    assert sales_by_tenant() == {"A": 100, "B": 200}

    active_user["value"] = _User("shared", {"tenant": "A"}, "editor")

    # Even an editor cannot override the server-derived tenant predicate.
    denied = client.post("/v1/runtime", json=transaction(cross_tenant_update))
    assert denied.status_code == 403
    assert sales_by_tenant() == {"A": 100, "B": 200}

    # A row path/key that omits tenant is still ANDed with tenant=A, so a
    # hidden South row in B is not touched.
    no_match = client.post(
        "/v1/runtime",
        json=transaction(
            {
                "keyFields": ["region"],
                "update": [
                    {"keys": {"region": "South"}, "values": {"sales": 999}}
                ],
            },
            request_id="scoped-key",
        ),
    )
    assert no_match.status_code == 200
    assert no_match.json()["payload"]["transaction"]["applied"]["update"] == 0
    assert sales_by_tenant() == {"A": 100, "B": 200}

    # Policy grants sales but not cost. The column check occurs after client
    # aliases are normalized to physical columns and before controller SQL.
    denied_column = client.post(
        "/v1/runtime",
        json=transaction(
            {
                "keyFields": ["tenant", "region"],
                "update": [
                    {
                        "keys": {"tenant": "A", "region": "North"},
                        "values": {"cost": 999},
                    }
                ],
            },
            request_id="forbidden-column",
        ),
    )
    assert denied_column.status_code == 403
    assert sales_by_tenant() == {"A": 100, "B": 200}

    # Inserts must name the principal's tenant; inserting a B row is rejected.
    denied_insert = client.post(
        "/v1/runtime",
        json=transaction(
            {"add": [{"tenant": "B", "region": "East", "sales": 1}]},
            request_id="cross-tenant-insert",
        ),
    )
    assert denied_insert.status_code == 403
    assert sales_by_tenant() == {"A": 100, "B": 200}

    # A table omitted from the server policy is not a mutation target.
    denied_table = client.post(
        "/v1/runtime",
        json=transaction(cross_tenant_update, table="unapproved_sales", request_id="other-table"),
    )
    assert denied_table.status_code == 403

    # Create B-owned history under the same authenticated subject, then change
    # its server claims to tenant A. Undo materializes the saved inverse, which
    # is scoped again and must not replay across tenants.
    active_user["value"] = _User("shared", {"tenant": "B"}, "editor")
    b_update = client.post(
        "/v1/runtime",
        json=transaction(
            {
                "keyFields": ["tenant", "region"],
                "update": [
                    {
                        "keys": {"tenant": "B", "region": "South"},
                        "values": {"sales": 999},
                    }
                ],
            },
            request_id="b-edit",
        ),
    )
    assert b_update.status_code == 200
    event_id = b_update.json()["payload"]["transaction"]["eventId"]
    assert sales_by_tenant() == {"A": 100, "B": 999}

    active_user["value"] = _User("shared", {"tenant": "A"}, "editor")
    denied_history = client.post(
        "/v1/runtime",
        json=transaction(
            {"eventAction": "undo", "eventId": event_id},
            request_id="cross-tenant-history",
        ),
    )
    assert denied_history.status_code == 403
    assert sales_by_tenant() == {"A": 100, "B": 999}


def test_http_transport_mutations_are_read_only_without_an_explicit_policy():
    """Registering the HTTP transport alone cannot enable writes."""
    service = _Service()

    async def current_user():
        return _User("editor", {"tenant": "A"}, "editor")

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        dataset_authorizer=_dataset_policy(
            "mutate",
            row_security={"mode": "claims", "claims": {"tenant": "tenant"}},
        ),
    )
    response = TestClient(app).post(
        "/v1/runtime",
        json={
            **_runtime_request(request_id="read-only"),
            "kind": "transaction",
            "payload": {"update": [{"keys": {"region": "North"}, "values": {"amount": 1}}]},
        },
    )

    assert response.status_code == 403
    assert service.calls == []


def test_http_transaction_history_cannot_override_the_authenticated_session():
    """A forged sessionId/clientInstance cannot undo another user's edit."""
    import pyarrow as pa

    from pivot_engine import create_tanstack_adapter
    from pivot_engine.runtime import PivotRuntimeService, RuntimeMutationAuthorizer, SessionRequestGate

    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales",
        pa.Table.from_pylist([{"tenant": "A", "region": "North", "sales": 100}]),
    )
    service = PivotRuntimeService(
        adapter_getter=lambda: adapter,
        session_gate=SessionRequestGate(),
    )
    active_user = {"value": _User("victim", {"tenant": "A"}, "editor")}

    async def current_user():
        return active_user["value"]

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        mutation_authorizer=RuntimeMutationAuthorizer(
            {
                "sales": {
                    "roles": {"editor"},
                    "columns": {"tenant", "region", "sales"},
                    "operations": {"update", "history"},
                }
            }
        ),
        dataset_authorizer=_dataset_policy(
            "mutate",
            row_security={"mode": "claims", "claims": {"tenant": "tenant"}},
        ),
    )
    client = TestClient(app)

    def request(payload, *, request_id, client_instance):
        return {
            "schemaVersion": 1,
            "kind": "transaction",
            "requestId": request_id,
            "table": "sales",
            "clientInstance": client_instance,
            "stateEpoch": 1,
            "windowSeq": 1,
            "state": {
                "rowFields": ["tenant", "region"],
                "valConfigs": [{"field": "sales", "agg": "sum"}],
            },
            "payload": {"refreshMode": "none", **payload},
        }

    victim_edit = client.post(
        "/v1/runtime",
        json=request(
            {
                "keyFields": ["tenant", "region"],
                "update": [
                    {
                        "keys": {"tenant": "A", "region": "North"},
                        "values": {"sales": 999},
                    }
                ],
            },
            request_id="victim-edit",
            client_instance="victim-tab",
        ),
    )
    assert victim_edit.status_code == 200, victim_edit.text
    event_id = victim_edit.json()["payload"]["transaction"]["eventId"]

    active_user["value"] = _User("attacker", {"tenant": "A"}, "editor")
    forged_undo = client.post(
        "/v1/runtime",
        json=request(
            {
                "eventAction": "undo",
                "eventId": event_id,
                "sessionId": "principal:victim",
                "clientInstance": "victim-tab",
            },
            request_id="forged-history-session",
            client_instance="attacker-tab",
        ),
    )

    current_sales = (
        adapter.controller.planner.con.table("sales").execute().to_dict("records")[0]["sales"]
    )
    assert forged_undo.status_code == 403
    assert current_sales == 999


def test_http_transport_preserves_an_explicit_permitted_tenant_key():
    """A multi-tenant grant must not widen an explicit tenant predicate."""
    import pyarrow as pa

    from pivot_engine import create_tanstack_adapter
    from pivot_engine.runtime import (
        PivotRuntimeService,
        RuntimeMutationAuthorizer,
        SessionRequestGate,
    )

    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales",
        pa.Table.from_pylist(
            [
                {"tenant": "A", "region": "North", "sales": 100},
                {"tenant": "B", "region": "North", "sales": 200},
            ]
        ),
    )
    service = PivotRuntimeService(
        adapter_getter=lambda: adapter,
        session_gate=SessionRequestGate(),
    )

    async def current_user():
        return _User("editor", {"tenant": ["A", "B"]}, "editor")

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        mutation_authorizer=RuntimeMutationAuthorizer(
            {
                "sales": {
                    "roles": {"editor"},
                    "columns": {"tenant", "region", "sales"},
                    "operations": {"update"},
                }
            }
        ),
        dataset_authorizer=_dataset_policy("mutate"),
    )

    response = TestClient(app).post(
        "/v1/runtime",
        json={
            "schemaVersion": 1,
            "kind": "transaction",
            "requestId": "preserve-tenant-key",
            "table": "sales",
            "clientInstance": "editor-tab",
            "stateEpoch": 1,
            "windowSeq": 1,
            "state": {
                "rowFields": ["tenant", "region"],
                "valConfigs": [{"field": "sales", "agg": "sum"}],
            },
            "payload": {
                "refreshMode": "none",
                "keyFields": ["tenant", "region"],
                "update": [
                    {
                        "keys": {"tenant": "A", "region": "North"},
                        "values": {"sales": 999},
                    }
                ],
            },
        },
    )

    rows_by_tenant = {
        row["tenant"]: row["sales"]
        for row in adapter.controller.planner.con.table("sales")
        .execute()
        .to_dict("records")
    }
    assert response.status_code == 200
    assert response.json()["payload"]["transaction"]["applied"]["update"] == 1
    assert rows_by_tenant == {"A": 999, "B": 200}
