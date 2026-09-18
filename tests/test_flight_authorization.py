"""Public Arrow Flight authorization-boundary regressions."""

import json

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from pivot_engine.flight_server import (
    FlightPrincipalResolver,
    PivotFlightServer,
    _PrincipalMiddlewareFactory,
)
from pivot_engine.runtime import RuntimeDatasetAuthorizer


class _Principal:
    def __init__(self, user_id="viewer", tenant="A", role="viewer"):
        self.id = user_id
        self.username = user_id
        self.role = role
        self.attributes = {"tenant": tenant}


class _Resolver:
    def __init__(self):
        self.received = None

    def resolve(self, headers):
        self.received = headers
        return _Principal()


def test_flight_cannot_disable_authentication_in_production(monkeypatch):
    class _Controller:
        pass

    monkeypatch.setenv("ENV", "production")

    with pytest.raises(RuntimeError, match="cannot be disabled in production"):
        PivotFlightServer(_Controller(), location=None, require_auth=False)


def test_flight_requires_tls_location_and_certificates_in_production(monkeypatch):
    class _Controller:
        pass

    monkeypatch.setenv("ENV", "production")
    authorizer = RuntimeDatasetAuthorizer(
        {
            "sales": {
                "roles": {"viewer"},
                "operations": {"read"},
                "rowSecurity": {"mode": "global"},
            }
        }
    )

    with pytest.raises(RuntimeError, match="grpc\\+tls"):
        PivotFlightServer(
            _Controller(),
            location="grpc://127.0.0.1:8080",
            principal_resolver=lambda _token: _Principal(),
            dataset_authorizer=authorizer,
        )
    with pytest.raises(RuntimeError, match="TLS certificates"):
        PivotFlightServer(
            _Controller(),
            location="grpc+tls://127.0.0.1:8080",
            principal_resolver=lambda _token: _Principal(),
            dataset_authorizer=authorizer,
        )


def test_flight_host_resolver_receives_normalized_request_headers():
    resolver = _Resolver()
    factory = _PrincipalMiddlewareFactory(resolver)

    middleware = factory.start_call(
        None,
        {"authorization": [b"Bearer opaque-token"], "x-host-context": [b"tenant-a"]},
    )

    assert getattr(FlightPrincipalResolver, "_is_protocol", False)
    assert middleware.principal.id == "viewer"
    assert resolver.received == {
        "authorization": ("Bearer opaque-token",),
        "x-host-context": ("tenant-a",),
    }


def test_flight_rejects_missing_row_claim_before_controller_execution():
    class Controller:
        def __init__(self):
            self.calls = 0

        def run_pivot_arrow(self, _spec):
            self.calls += 1
            return pa.table({"value": [1]})

    class Context:
        def get_middleware(self, _name):
            return type("Auth", (), {"principal": _Principal(tenant=None)})()

    authorizer = RuntimeDatasetAuthorizer(
        {
            "sales": {
                "roles": {"viewer"},
                "operations": {"read"},
                "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}},
            }
        }
    )
    controller = Controller()
    server = PivotFlightServer(
        controller,
        location=None,
        principal_resolver=lambda _token: _Principal(),
        dataset_authorizer=authorizer,
    )
    try:
        with pytest.raises(fl.FlightUnauthorizedError):
            list(server.do_action(Context(), fl.Action("pivot", b'{"table":"sales"}')))
        assert controller.calls == 0
    finally:
        server.shutdown()


def test_flight_restricts_cache_administration_to_admin_principals():
    class Controller:
        def __init__(self):
            self.cleared = 0

        def clear_cache(self):
            self.cleared += 1

    class Context:
        def __init__(self, principal):
            self.principal = principal

        def get_middleware(self, _name):
            return type("Auth", (), {"principal": self.principal})()

    authorizer = RuntimeDatasetAuthorizer(
        {
            "sales": {
                "roles": {"viewer", "admin"},
                "operations": {"read"},
                "rowSecurity": {"mode": "global"},
            }
        }
    )
    controller = Controller()
    server = PivotFlightServer(
        controller,
        location=None,
        principal_resolver=lambda _token: _Principal(),
        dataset_authorizer=authorizer,
    )
    try:
        with pytest.raises(fl.FlightUnauthorizedError):
            list(server.do_action(Context(_Principal()), fl.Action("clear_cache", b"")))
        list(server.do_action(Context(_Principal(role="admin")), fl.Action("clear_cache", b"")))
        assert controller.cleared == 1
    finally:
        server.shutdown()


def test_flight_ticket_cache_retains_only_a_non_reversible_scope_fingerprint():
    class Controller:
        def run_pivot_arrow(self, _spec):
            return pa.table({"value": [1]})

    class Context:
        def get_middleware(self, _name):
            return type("Auth", (), {"principal": _Principal(tenant="confidential-A")})()

    authorizer = RuntimeDatasetAuthorizer(
        {
            "sales": {
                "roles": {"viewer"},
                "operations": {"read"},
                "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}},
            }
        }
    )
    server = PivotFlightServer(
        Controller(),
        location=None,
        principal_resolver=lambda _token: _Principal(),
        dataset_authorizer=authorizer,
    )
    try:
        [result] = list(server.do_action(Context(), fl.Action("pivot", b'{"table":"sales"}')))
        ticket = json.loads(result.body.to_pybytes())["ticket"]
        entry = server._load_result_entry(ticket)

        assert entry.scope_fingerprint
        assert "confidential-A" not in repr(entry)
        assert not hasattr(entry, "owner_scope")
    finally:
        server.shutdown()


def test_flight_expired_ticket_is_a_not_found_response():
    class Controller:
        def run_pivot_arrow(self, _spec):
            return pa.table({"value": [1]})

    server = PivotFlightServer(
        Controller(), location=None, require_auth=False, result_ttl_seconds=-1
    )
    try:
        [result] = list(server.do_action(None, fl.Action("pivot", b'{"value":1}')))
        ticket = json.loads(result.body.to_pybytes())["ticket"]
        with pytest.raises(KeyError, match="not found or has expired"):
            server.do_get(None, fl.Ticket(json.dumps({"ticket": ticket}).encode("utf-8")))
    finally:
        server.shutdown()
