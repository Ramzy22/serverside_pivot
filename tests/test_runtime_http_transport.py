"""Contract tests for the host-neutral HTTP Pivot Runtime Protocol V1."""

from dataclasses import dataclass, field
import logging

import fakeredis
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
