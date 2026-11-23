"""REST hierarchy endpoints answer with the core hierarchy view (M4: legacy loaders removed)."""

from __future__ import annotations

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from pivot_engine.complete_rest_api import CompletePivotAPI
from pivot_engine.runtime import RuntimeDatasetAuthorizer
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.security import User

SPEC = {"table": "h", "rows": ["region", "city"], "columns": [], "filters": [], "limit": 100, "totals": True,
        "measures": [{"field": "v", "agg": "sum", "alias": "v_sum"}, {"field": "v", "agg": "avg", "alias": "v_avg"}]}


def _client():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow("h", pa.table({"region": ["N", "N", "S"], "city": ["a", "b", "c"],
                                                   "v": [1.0, 3.0, 10.0]}))
    api = CompletePivotAPI(controller, dataset_authorizer=RuntimeDatasetAuthorizer(
        {"h": {"roles": {"viewer"}, "operations": {"read"}, "rowSecurity": {"mode": "global"}}}))

    async def current_user():
        return User(id="u", username="u", role="viewer", attributes={})

    import pivot_engine.complete_rest_api as rest_api_module
    import pivot_engine.security as security_module
    api.app.dependency_overrides[rest_api_module.get_current_user] = current_user
    api.app.dependency_overrides[security_module.get_current_user] = current_user
    return TestClient(api.get_app())


@pytest.mark.parametrize("path", ["/pivot/hierarchical", "/pivot/progressive-load", "/pivot/pruned-hierarchical",
                                  "/pivot/intelligent-prefetch", "/pivot/batch-load"])
def test_hierarchy_endpoints_return_the_visible_tree(path):
    response = _client().post(path, json={"spec": SPEC, "expanded_paths": [["N"]]})
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    got = [(r["_path"], r["v_sum"], r["v_avg"]) for r in data["rows"]]
    # the visible nodes in tree order, then the grand total (the run_hierarchy_view contract)
    assert got == [("N", 4.0, 2.0), ("N|||a", 1.0, 1.0), ("N|||b", 3.0, 3.0), ("S", 10.0, 10.0),
                   ("__grand_total__", 14.0, pytest.approx(14 / 3))]
    assert data["total_rows"] == 5


def test_virtual_scroll_endpoint_returns_the_requested_window():
    response = _client().post("/pivot/virtual-scroll",
                              json={"spec": SPEC, "expanded_paths": [["N"]], "start_row": 1, "end_row": 2})
    assert response.status_code == 200, response.text
    body = response.json()
    assert [r["_path"] for r in body["data"]["rows"]] == ["N|||a", "N|||b"]
    assert body["data"]["total_rows"] == 5
    assert body["metadata"] == {"start_row": 1, "end_row": 2, "range_size": 1}


def test_legacy_precomputation_endpoints_are_gone():
    client = _client()
    assert client.post("/pivot/materialized-hierarchy", json=SPEC).status_code == 404
    assert client.get("/pivot/jobs/x").status_code == 404


def test_a_request_the_core_does_not_express_is_a_typed_422():
    spec = {**SPEC, "filters": [{"field": "region", "op": "regex", "value": "N"}]}
    response = _client().post("/pivot/hierarchical", json={"spec": spec, "expanded_paths": []})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "unsupported_query"
    assert response.json()["detail"]["reasons"] == ["filter operator 'regex'"]
