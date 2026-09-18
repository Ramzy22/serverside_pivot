"""Dataset-level authorization regressions for HTTP and legacy API transports."""

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine.complete_rest_api import CompletePivotAPI
from pivot_engine.runtime import (
    PivotServiceResponse,
    RuntimeDatasetAuthorizationError,
    RuntimeDatasetAuthorizer,
    RuntimeMutationAuthorizer,
    register_runtime_http_transport,
)
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.security import User, get_current_user


@dataclass
class _User:
    id: str = "viewer-a"
    role: str = "viewer"
    attributes: dict | None = None


class _Service:
    def __init__(self):
        self.calls = []

    async def process_async(self, _state, context):
        self.calls.append(context.table)
        return PivotServiceResponse(status="data", data=[], total_rows=0)


def _runtime_request(table: str):
    return {
        "schemaVersion": 1,
        "kind": "data",
        "requestId": f"read-{table}",
        "table": table,
        "stateEpoch": 1,
        "windowSeq": 1,
        "state": {},
        "payload": {"intent": "structural"},
    }


def _override_complete_api_user(api, resolver):
    """Cover both auth dependencies when security.py was reloaded by a test."""
    import pivot_engine.complete_rest_api as rest_api_module
    import pivot_engine.security as security_module

    api.app.dependency_overrides[rest_api_module.get_current_user] = resolver
    api.app.dependency_overrides[security_module.get_current_user] = resolver


def test_runtime_read_policy_rejects_unlisted_dataset_before_service_execution():
    service = _Service()

    async def current_user():
        return _User(attributes={})

    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {"sales": {"roles": {"viewer"}, "rowSecurity": {"mode": "global"}}}
        ),
    )
    client = TestClient(app)

    assert client.post("/v1/runtime", json=_runtime_request("sales")).status_code == 200
    denied = client.post("/v1/runtime", json=_runtime_request("other_tenant_sales"))

    assert denied.status_code == 403
    assert service.calls == ["sales"]


def test_claim_scoped_dataset_rejects_missing_claims_and_builds_an_immutable_scope():
    authorizer = RuntimeDatasetAuthorizer(
        {
            "sales": {
                "roles": {"viewer"},
                "operations": {"read"},
                "rowSecurity": {
                    "mode": "claims",
                    "claims": {"tenant": "tenant"},
                },
            }
        }
    )

    with pytest.raises(RuntimeDatasetAuthorizationError):
        authorizer.authorize(
            principal=_User(attributes={}), table="sales", operation="read"
        )

    scope = authorizer.authorize(
        principal=_User(attributes={"tenant": "A"}),
        table="sales",
        operation="read",
    )
    assert scope.field_values == {"tenant": ("A",)}
    assert scope.apply_to_filters({"tenant": {"type": "eq", "value": "B"}}) == {
        "tenant": {"type": "eq", "value": "A"}
    }


def test_claim_scoped_dataset_denies_legacy_and_v1_reads_without_claims():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow(
        "tenant_sales",
        pa.table(
            {
                "tenant": ["A", "B"],
                "region": ["North", "South"],
                "sales": [100, 200],
            }
        ),
    )
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "tenant_sales": {
                    "roles": {"viewer"},
                    "operations": {"read"},
                    "rowSecurity": {
                        "mode": "claims",
                        "claims": {"tenant": "tenant"},
                    },
                }
            }
        ),
    )

    async def no_claim_viewer():
        return User(
            id="unscoped-viewer",
            username="unscoped-viewer",
            role="viewer",
            attributes={},
        )

    _override_complete_api_user(api, no_claim_viewer)
    client = TestClient(api.get_app())
    legacy = client.post(
        "/pivot/tanstack",
        json={
            "operation": "get_unique_values",
            "table": "tenant_sales",
            "columns": [],
            "filters": {},
            "sorting": [],
            "grouping": [],
            "aggregations": [],
            "global_filter": "region",
        },
    )
    v1 = client.post(
        "/v1/runtime",
        json={
            "schemaVersion": 1,
            "kind": "filter_options",
            "requestId": "missing-row-claim",
            "table": "tenant_sales",
            "clientInstance": "security-test",
            "stateEpoch": 1,
            "windowSeq": 1,
            "state": {"rowFields": ["region"], "valConfigs": []},
            "payload": {"columnId": "region"},
        },
    )

    assert legacy.status_code == 403
    assert v1.status_code == 403


def test_legacy_tanstack_read_policy_rejects_an_unlisted_dataset():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow(
        "sales",
        pa.table({"tenant": ["A"], "region": ["North"], "sales": [100]}),
    )
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {"sales": {"roles": {"viewer"}, "rowSecurity": {"mode": "global"}}}
        ),
    )

    async def current_user():
        return User(
            id="tenant-a-viewer",
            username="tenant-a-viewer",
            role="viewer",
            attributes={"tenant": "A"},
        )

    _override_complete_api_user(api, current_user)
    response = TestClient(api.get_app()).post(
        "/pivot/tanstack",
        json={
            "operation": "get_unique_values",
            "table": "other_tenant_sales",
            "columns": [],
            "filters": {},
            "sorting": [],
            "grouping": [],
            "aggregations": [],
            "global_filter": "region",
        },
    )

    assert response.status_code == 403


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (
            "/pivot",
            {"table": "other_tenant_sales", "measures": []},
        ),
        (
            "/pivot/schema/other_tenant_sales",
            None,
        ),
    ],
)
def test_legacy_table_read_routes_cannot_bypass_dataset_policy(path, payload):
    controller = ScalablePivotController(backend_uri=":memory:")
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {"sales": {"roles": {"viewer"}, "rowSecurity": {"mode": "global"}}}
        ),
    )

    async def current_user():
        return User(
            id="tenant-a-viewer",
            username="tenant-a-viewer",
            role="viewer",
            attributes={"tenant": "A"},
        )

    _override_complete_api_user(api, current_user)
    client = TestClient(api.get_app())
    response = (
        client.get(path)
        if payload is None
        else client.post(path, json=payload)
    )

    assert response.status_code == 403


@pytest.mark.parametrize("policy", [{"roles": []}, {"operations": []}])
def test_empty_mutation_policy_lists_do_not_expand_to_default_permissions(policy):
    with pytest.raises(ValueError):
        RuntimeMutationAuthorizer({"sales": policy})


def test_legacy_cdc_push_rejects_a_viewer_before_controller_execution():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.push_change_event = AsyncMock()
    api = CompletePivotAPI(
        controller,
        # Deliberately allow the dataset operation: this proves the separate
        # operation-level admin requirement cannot be bypassed by a viewer.
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "sales": {
                    "roles": {"viewer", "admin"},
                    "operations": {"cdc"},
                    "rowSecurity": {"mode": "global"},
                }
            }
        ),
    )

    async def viewer():
        return User(
            id="viewer-a",
            username="viewer-a",
            role="viewer",
            attributes={"tenant": "A"},
        )

    _override_complete_api_user(api, viewer)
    response = TestClient(api.get_app()).post(
        "/pivot/cdc/push/sales",
        json={"type": "UPDATE", "new_row": {"tenant": "A", "sales": 999}},
    )

    assert response.status_code == 403
    controller.push_change_event.assert_not_awaited()


def test_legacy_cdc_push_applies_the_admins_row_scope_before_ingestion():
    """An admin role does not widen a claims-scoped CDC write to another tenant."""
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.push_change_event = AsyncMock()
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "sales": {
                    "roles": {"admin"},
                    "operations": {"cdc"},
                    "rowSecurity": {
                        "mode": "claims",
                        "claims": {"tenant": "tenant"},
                    },
                }
            }
        ),
    )

    async def tenant_a_admin():
        return User(
            id="admin-a",
            username="admin-a",
            role="admin",
            attributes={"tenant": "A"},
        )

    _override_complete_api_user(api, tenant_a_admin)
    response = TestClient(api.get_app()).post(
        "/pivot/cdc/push/sales",
        json={"type": "UPDATE", "new_row": {"tenant": "B", "sales": 999}},
    )

    assert response.status_code == 403
    controller.push_change_event.assert_not_awaited()


def test_list_valued_rls_is_membership_not_literal_list_equality_for_legacy_and_v1():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow(
        "tenant_sales",
        pa.table(
            {
                "tenant": ["A", "B", "['A', 'B']"],
                "region": ["North", "South", "SECRET-LIST-TENANT"],
                "sales": [100, 200, 999],
            }
        ),
    )
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "tenant_sales": {
                    "roles": {"viewer"},
                    "operations": {"read"},
                    "rowSecurity": {
                        "mode": "claims",
                        "claims": {"tenant": "tenant"},
                    },
                }
            }
        ),
    )

    async def multi_tenant_viewer():
        return User(
            id="multi-tenant-viewer",
            username="multi-tenant-viewer",
            role="viewer",
            attributes={"tenant": ["A", "B"]},
        )

    _override_complete_api_user(api, multi_tenant_viewer)
    # Security-config tests reload the module to exercise environment startup
    # behavior. Override both references captured by the legacy app and the
    # runtime transport so this regression still exercises both endpoints.
    import pivot_engine.complete_rest_api as complete_rest_api_module
    import pivot_engine.security as security_module
    api.app.dependency_overrides[complete_rest_api_module.get_current_user] = multi_tenant_viewer
    api.app.dependency_overrides[security_module.get_current_user] = multi_tenant_viewer
    client = TestClient(api.get_app())

    legacy = client.post(
        "/pivot/tanstack",
        json={
            "operation": "get_unique_values",
            "table": "tenant_sales",
            "columns": [],
            "filters": {},
            "sorting": [],
            "grouping": [],
            "aggregations": [],
            "global_filter": "region",
        },
    )
    assert legacy.status_code == 200, legacy.text
    assert {item["value"] for item in legacy.json()["data"]["data"]} == {"North", "South"}

    v1 = client.post(
        "/v1/runtime",
        json={
            "schemaVersion": 1,
            "kind": "filter_options",
            "requestId": "multi-tenant-filter-options",
            "table": "tenant_sales",
            "clientInstance": "security-test",
            "stateEpoch": 1,
            "windowSeq": 1,
            "state": {"rowFields": ["region"], "valConfigs": []},
            "payload": {"columnId": "region"},
        },
    )
    assert v1.status_code == 200, v1.text
    assert set(v1.json()["payload"]["options"]) == {"North", "South"}


@pytest.mark.parametrize(
    "job",
    [
        {"status": "completed", "table": "secret_sales", "owner": "viewer-a", "hierarchy_name": "secret-hierarchy"},
        {"status": "completed", "table": "sales", "owner": "another-user", "hierarchy_name": "another-hierarchy"},
    ],
    ids=["forbidden-dataset", "different-owner"],
)
def test_materialization_job_status_hides_forbidden_or_foreign_job_metadata(job):
    controller = MagicMock(spec=ScalablePivotController)
    controller.get_materialization_status.return_value = job
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "sales": {
                    "roles": {"viewer"},
                    "operations": {"read"},
                    "rowSecurity": {"mode": "global"},
                }
            }
        ),
    )

    async def current_user():
        return User(
            id="viewer-a",
            username="viewer-a",
            role="viewer",
            attributes={},
        )

    _override_complete_api_user(api, current_user)
    response = TestClient(api.get_app()).get("/pivot/jobs/job-from-another-request")

    assert response.status_code == 404
    assert "secret" not in response.text
    assert "hierarchy" not in response.text


def test_materialization_job_status_allows_the_owner_after_dataset_authorization():
    controller = MagicMock(spec=ScalablePivotController)
    controller.get_materialization_status.return_value = {
        "status": "completed",
        "table": "sales",
        "owner": "viewer-a",
        "hierarchy_name": "sales-hierarchy",
    }
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "sales": {
                    "roles": {"viewer"},
                    "operations": {"read"},
                    "rowSecurity": {"mode": "global"},
                }
            }
        ),
    )

    async def current_user():
        return User(id="viewer-a", username="viewer-a", role="viewer", attributes={})

    _override_complete_api_user(api, current_user)
    response = TestClient(api.get_app()).get("/pivot/jobs/my-job")

    assert response.status_code == 200
    assert response.json()["data"]["hierarchy_name"] == "sales-hierarchy"
