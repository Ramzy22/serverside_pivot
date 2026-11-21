"""Flight tenant isolation over a real controller (REQ-SEC-001, REQ-SEC-005)."""

from __future__ import annotations

import json

import pyarrow.flight as fl

from pivot_engine.flight_server import PivotFlightServer
from pivot_engine.scalable_pivot_controller import ScalablePivotController

from .isolation_fixtures import TABLE, Principal, claims_authorizer, leaks, tenant_table


class _Ctx:
    def get_middleware(self, _name):
        return type("Auth", (), {"principal": Principal()})()


def test_flight_pivot_action_and_ticket_are_scoped():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow(TABLE, tenant_table())
    server = PivotFlightServer(
        controller,
        location=None,
        principal_resolver=lambda _token: Principal(),
        dataset_authorizer=claims_authorizer(),
    )
    try:
        spec = {
            "table": TABLE,
            "rows": ["region"],
            "measures": [{"field": "amount", "agg": "sum", "alias": "amount_sum"}],
            "filters": [],
            "totals": True,
        }
        [result] = list(server.do_action(_Ctx(), fl.Action("pivot", json.dumps(spec).encode("utf-8"))))
        ticket = json.loads(result.body.to_pybytes())["ticket"]
        table = server._load_authorized_result(ticket, Principal())
        rows = table.to_pylist()
        assert rows, "scoped Flight pivot returned nothing"
        assert not leaks(rows, rows), leaks(rows, rows)
    finally:
        server.shutdown()
