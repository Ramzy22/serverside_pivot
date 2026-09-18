"""Authorization regressions for the retained legacy TanStack endpoint."""

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from pivot_engine.complete_rest_api import CompletePivotAPI
from pivot_engine.runtime import RuntimeDatasetAuthorizer
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.security import User, get_current_user


@pytest.mark.parametrize(
    "client_filters",
    [
        {},
        {"tenant": {"type": "eq", "value": "B"}},
    ],
    ids=["no-client-tenant-filter", "forged-client-tenant-filter"],
)
def test_legacy_tanstack_unique_values_are_scoped_to_the_authenticated_tenant(
    client_filters,
):
    """Distinct filter options must obey server-owned RLS, not request filters."""
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

    async def tenant_a_viewer():
        return User(
            id="tenant-a-viewer",
            username="tenant-a-viewer",
            role="viewer",
            attributes={"tenant": "A"},
        )

    api.app.dependency_overrides[get_current_user] = tenant_a_viewer
    client = TestClient(api.get_app())

    response = client.post(
        "/pivot/tanstack",
        json={
            "operation": "get_unique_values",
            "table": "tenant_sales",
            "columns": [],
            # A hostile client must not be able to replace the principal's
            # tenant predicate with its own value.
            "filters": client_filters,
            "sorting": [],
            "grouping": [],
            "aggregations": [],
            "global_filter": "region",
        },
    )

    assert response.status_code == 200, response.text
    result = response.json()["data"]
    assert result["data"] == [{"value": "North"}]
    assert result["pagination"]["totalRows"] == 1
