"""REST tenant-isolation matrix, enumerated from ``app.routes`` (REQ-SEC-005).

A route without a case here (and not in ``EXEMPT`` with a reason) fails the
suite, so new endpoints cannot ship unexamined.
"""

from __future__ import annotations

import json

import pytest
import fastapi.routing as fastapi_routing
from fastapi.routing import APIRoute, APIWebSocketRoute
from fastapi.testclient import TestClient
from starlette.routing import WebSocketRoute

from pivot_engine.complete_rest_api import CompletePivotAPI
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.security import User

from .isolation_fixtures import TABLE, claims_authorizer, leaks, tenant_table
from .test_tenant_isolation_matrix import _csv_rows, _rows_from

SPEC = {
    "table": TABLE,
    "rows": ["region"],
    "columns": [],
    "measures": [{"field": "amount", "agg": "sum", "alias": "amount_sum"}],
    "filters": [],
    "limit": 100,
    "totals": True,
}
HIER = {"spec": SPEC, "expanded_paths": [["North"]]}
TANSTACK = {
    "operation": "get_data",
    "table": TABLE,
    "columns": [{"id": "region"}, {"id": "amount_sum", "aggregationFn": "sum", "aggregationField": "amount"}],
    "filters": {},
    "sorting": [],
    "grouping": ["region"],
    "aggregations": [],
}

CASES = {
    ("POST", "/pivot"): [SPEC],
    ("POST", "/pivot/export"): [{"spec": SPEC, "format": "csv"}],
    ("POST", "/pivot/tanstack"): [
        TANSTACK,
        {**TANSTACK, "operation": "get_unique_values", "global_filter": "tenant"},
    ],
    ("POST", "/pivot/hierarchical"): [HIER],
    ("POST", "/pivot/virtual-scroll"): [{**HIER, "start_row": 0, "end_row": 100}],
    ("POST", "/pivot/progressive-load"): [HIER],
    ("POST", "/pivot/pruned-hierarchical"): [HIER],
    ("POST", "/pivot/intelligent-prefetch"): [HIER],
    ("POST", "/pivot/streaming-aggregation"): [SPEC],
    ("POST", "/pivot/batch-load"): [HIER],
}

EXEMPT = {
    ("GET", "/health"): "no dataset access",
    ("GET", "/admin/dashboard"): "static HTML, no dataset rows",
    ("GET", "/pivot/schema/{table_name}"): "column metadata only (names/types), authorized per dataset",
    ("POST", "/pivot/cdc/setup"): "write/admin boundary; covered by CDC authorization tests",
    ("POST", "/pivot/cdc/push-setup"): "write/admin boundary; covered by CDC authorization tests",
    ("POST", "/pivot/cdc/push/{table_name}"): "write boundary; scope-checked per row image",
    ("GET", "/admin/cache/stats"): "admin-only (require_admin); counters, no dataset rows",
    ("GET", "/admin/jobs/background"): "admin-only (require_admin); job metadata",
    ("GET", "/admin/security/keys"): "admin-only (require_admin); key metadata",
    ("POST", "/admin/security/keys"): "admin-only (require_admin); key rotation",
    ("GET", "/admin/system/health"): "admin-only (require_admin); no dataset rows",
    ("GET", "/metrics"): "process metrics, no dataset rows",
    ("POST", "/v1/runtime"): "covered per kind by test_tenant_isolation_matrix (http edge)",
    ("GET", "/v1/runtime/payload/{token}"): "covered by test_tenant_isolation_matrix export payloadRef fetch",
    ("POST", "/v1/runtime/subscription"): "covered by test_live_isolation_matrix",
    ("POST", "/v1/runtime/stream"): "covered by test_live_isolation_matrix (SSE)",
}


def _columnar_rows(obj):
    """``{"columns": [...], "rows": [[...]]}`` result shape -> dict rows."""
    found = []
    if isinstance(obj, dict):
        cols, rows = obj.get("columns"), obj.get("rows")
        if isinstance(cols, list) and isinstance(rows, list) and rows and isinstance(rows[0], list):
            names = [c if isinstance(c, str) else (c or {}).get("name") for c in cols]
            found.extend(dict(zip(names, r, strict=False)) for r in rows)
        for value in obj.values():
            found.extend(_columnar_rows(value))
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                found.extend(_columnar_rows(item))
    return found


def _api():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow(TABLE, tenant_table())
    api = CompletePivotAPI(controller, dataset_authorizer=claims_authorizer())

    async def current_user():
        return User(id="tenant-a-user", username="tenant-a-user", role="viewer", attributes={"tenant": "A"})

    import pivot_engine.complete_rest_api as rest_api_module
    import pivot_engine.security as security_module

    api.app.dependency_overrides[rest_api_module.get_current_user] = current_user
    api.app.dependency_overrides[security_module.get_current_user] = current_user
    return api


def _flat_routes(app):
    """(route, full path) of every route, through included routers.

    FastAPI >= 0.16x keeps an included router as one ``_IncludedRouter`` entry in ``app.routes``;
    ``iter_route_contexts`` flattens it. Older releases list every route directly."""
    flatten = getattr(fastapi_routing, "iter_route_contexts", None)
    if flatten is None:
        return [(route, getattr(route, "path", None)) for route in app.routes]
    return [(context.route, context.path) for context in flatten(app.routes)]


def _routes(app):
    found = set()
    for route, path in _flat_routes(app):
        if isinstance(route, APIRoute):
            for method in route.methods - {"HEAD", "OPTIONS"}:
                found.add((method, path))
        elif isinstance(route, (APIWebSocketRoute, WebSocketRoute)):
            found.add(("WS", path))
    return {r for r in found if not r[1].startswith(("/docs", "/openapi", "/redoc"))}


def test_every_rest_route_has_an_isolation_case_or_documented_exemption():
    routes = _routes(_api().app)
    missing = routes - set(CASES) - set(EXEMPT)
    assert not missing, f"REST route(s) without an isolation case: {sorted(missing)}"
    stale = (set(CASES) | set(EXEMPT)) - routes
    assert not stale, f"Isolation cases for removed routes: {sorted(stale)}"


@pytest.mark.parametrize(
    "method,path,index",
    [(m, p, i) for (m, p), bodies in sorted(CASES.items()) for i in range(len(bodies))],
)
def test_rest_reads_are_scoped_or_denied(method, path, index):
    body = CASES[(method, path)][index]
    client = TestClient(_api().get_app(), raise_server_exceptions=False)
    response = client.request(method, path, json=body)
    if response.status_code >= 400:
        text = response.text
        found = leaks(text)
        assert not found, f"{path}: error body leaks {found}"
        return
    content_type = response.headers.get("content-type", "")
    if "csv" in content_type:
        rows = _csv_rows(response.content)
        payload = response.text
    else:
        payload = response.json()
        rows = _rows_from(payload) + _columnar_rows(payload)
    tenant_values = ()
    if body.get("operation") == "get_unique_values":
        data = payload.get("data") if isinstance(payload, dict) else None
        inner = data.get("data") if isinstance(data, dict) else data
        tenant_values = [d.get("value") if isinstance(d, dict) else d for d in (inner or [])]
    found = leaks(json.dumps(payload, default=str) if not isinstance(payload, str) else payload, rows, tenant_values=tenant_values)
    assert not found, f"{method} {path}: tenant isolation violated -> {found}"


def test_schema_route_returns_column_metadata_without_rows():
    assert TestClient(_api().get_app()).get(f"/pivot/schema/{TABLE}").status_code == 403  # "schema" not granted
    from pivot_engine.runtime.dataset_authorization import RuntimeDatasetAuthorizer

    import pivot_engine.complete_rest_api as rest_api_module

    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow(TABLE, tenant_table())
    api = CompletePivotAPI(controller, dataset_authorizer=RuntimeDatasetAuthorizer({TABLE: {
        "roles": {"viewer"}, "operations": {"read", "schema"},
        "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}}}}))

    async def current_user():
        return User(id="tenant-a-user", username="tenant-a-user", role="viewer", attributes={"tenant": "A"})

    api.app.dependency_overrides[rest_api_module.get_current_user] = current_user
    client = TestClient(api.get_app(), raise_server_exceptions=False)
    response = client.get(f"/pivot/schema/{TABLE}")
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert [c["name"] for c in data["columns"]] == list(tenant_table().column_names)
    assert all(c["type"] != "unknown" for c in data["columns"])
    assert not leaks(response.json())
    assert client.get("/pivot/schema/no_such_table").status_code in (403, 404)
