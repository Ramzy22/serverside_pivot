"""Tenant-isolation matrix (REQ-SEC-001, REQ-SEC-002, REQ-SEC-005).

Every externally reachable read path must either return only rows the
principal's row grant allows, or deny.  "The request carries a scope" is not
sufficient: each case asserts on what actually came back.

Two edges are exercised for every runtime protocol kind:

* ``http``  — the real ``/v1/runtime`` FastAPI transport (pre-scopes filters);
* ``dash``  — the shape Dash callbacks use: browser-controlled state with the
  server-issued grant only on ``PivotRequestContext.row_scope``.
"""

from __future__ import annotations

import asyncio
import csv
import dataclasses
import io
from typing import Any, Dict, List

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState
from pivot_engine.runtime.contracts import RUNTIME_COMMAND_KINDS
from pivot_engine.runtime.http_transport import register_runtime_http_transport
from pivot_engine.tanstack_adapter import create_tanstack_adapter

from .isolation_fixtures import TABLE, Principal, claims_authorizer, leaks, tenant_table

# Write kinds are covered by the mutation-authorization suite; for reads of a
# claims-scoped dataset without a mutation policy they must be denied.
WRITE_KINDS = {"update", "transaction"}

TREE_CONFIG = {
    "sourceType": "adjacency",
    "idField": "id",
    "parentIdField": "parent_id",
    "valueFields": ["amount"],
    "extraFields": ["tenant", "secret", "region"],
}

BASE_STATE: Dict[str, Any] = {
    "rowFields": ["region"],
    "colFields": [],
    "valConfigs": [{"field": "amount", "agg": "sum"}],
    "filters": {},
    "expanded": True,
    "showColTotals": True,
}

# kind -> (state overrides, payload, trigger_prop for the Dash shape)
READ_CASES: Dict[str, tuple] = {
    "data": ({}, {"intent": "structural", "start": 0, "end": 100}, "pivot.viewport"),
    "data_tree": ({"viewMode": "tree", "treeConfig": TREE_CONFIG}, {"intent": "structural", "start": 0, "end": 100}, "pivot.viewport"),
    "filter_options": ({}, {"columnId": "tenant"}, "pivot.filterRequest"),
    "chart": ({}, {"paneId": "p1", "requestSignature": "s1"}, "pivot.chartRequest"),
    "detail": ({}, {"rowPath": "North", "rowFields": ["region"], "page": 0, "pageSize": 100}, "pivot.detailRequest"),
    "detail_tree": ({"viewMode": "tree", "treeConfig": TREE_CONFIG}, {"rowPath": "n0", "rowFields": [], "page": 0, "pageSize": 100}, "pivot.detailRequest"),
    # n5 belongs to tenant B: asking for it by id must not disclose it.
    "detail_tree_foreign": ({"viewMode": "tree", "treeConfig": TREE_CONFIG}, {"rowPath": "n5", "rowFields": [], "page": 0, "pageSize": 100}, "pivot.detailRequest"),
    "drill": ({}, {"rowPath": "North", "rowFields": ["region"], "page": 0, "pageSize": 100}, "pivot.drillThrough"),
    "drill_legacy_keys": ({}, {"row_path": "North", "row_fields": ["region"], "page": 0, "page_size": 100}, "pivot.drillThrough"),
    "export": ({}, {"format": "csv"}, "pivot.exportRequest"),
}
PROTOCOL_KIND = {case: case.split("_tree")[0].split("_legacy")[0] for case in READ_CASES}


def _adapter():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(TABLE, tenant_table())
    return adapter


def _csv_rows(content: Any) -> List[Dict[str, Any]]:
    if not isinstance(content, (bytes, bytearray, str)):
        return []
    text = content.decode("utf-8", "replace") if isinstance(content, (bytes, bytearray)) else content
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        parsed = {}
        for key, value in row.items():
            try:
                parsed[key] = float(value) if value not in ("", None) else value
            except (TypeError, ValueError):
                parsed[key] = value
        rows.append(parsed)
    return rows


def _rows_from(obj: Any) -> List[Dict[str, Any]]:
    """Collect every record-like list from a service response or V1 payload."""
    rows: List[Dict[str, Any]] = []
    if dataclasses.is_dataclass(obj):
        obj = {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in {"data", "rows", "drill_records", "records"} and isinstance(value, list):
                rows.extend(r for r in value if isinstance(r, dict))
            elif key == "content":
                rows.extend(_csv_rows(value))
            elif key == "contentPath" and isinstance(value, str) and value.endswith(".csv"):
                with open(value, "rb") as handle:
                    rows.extend(_csv_rows(handle.read()))
            elif isinstance(value, (dict, list)) or dataclasses.is_dataclass(value):
                rows.extend(_rows_from(value))
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                rows.extend(_rows_from(item))
    return rows


def _tenant_options(obj: Any) -> List[Any]:
    if dataclasses.is_dataclass(obj):
        obj = {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
    if isinstance(obj, dict):
        for key in ("filter_options", "filterOptions", "payload"):
            inner = obj.get(key)
            if isinstance(inner, dict) and isinstance(inner.get("options"), list):
                return list(inner["options"])
        for value in obj.values():
            if isinstance(value, (dict, list)) or dataclasses.is_dataclass(value):
                found = _tenant_options(value)
                if found:
                    return found
    return []


def _snake_state(state: Dict[str, Any]) -> PivotViewState:
    return PivotViewState(
        row_fields=list(state.get("rowFields") or []),
        col_fields=list(state.get("colFields") or []),
        val_configs=list(state.get("valConfigs") or []),
        filters=dict(state.get("filters") or {}),
        expanded=state.get("expanded", {}),
        show_col_totals=bool(state.get("showColTotals", True)),
        view_mode=state.get("viewMode", "pivot"),
        tree_config=state.get("treeConfig"),
    )


def _assert_scoped_or_denied(label: str, outcome: Any, *, filter_kind: bool = False) -> None:
    if isinstance(outcome, PermissionError):
        return  # an authorization denial is an acceptable outcome
    status = getattr(outcome, "status", None) if not isinstance(outcome, dict) else outcome.get("status")
    if status in {"error", "denied", "forbidden"} and not _rows_from(outcome):
        return
    found = leaks(outcome, _rows_from(outcome), tenant_values=_tenant_options(outcome) if filter_kind else ())
    assert not found, f"{label}: tenant isolation violated -> {found}"


# --------------------------------------------------------------------------- guard


def test_matrix_covers_every_runtime_protocol_kind():
    covered = set(PROTOCOL_KIND.values()) | WRITE_KINDS
    assert covered >= set(RUNTIME_COMMAND_KINDS), (
        "New runtime kind(s) without an isolation case: "
        f"{sorted(set(RUNTIME_COMMAND_KINDS) - covered)}"
    )


# --------------------------------------------------------------------------- dash shape


@pytest.mark.parametrize("case", sorted(READ_CASES))
def test_dash_shape_reads_are_scoped_or_denied(case):
    """Browser state is unscoped; only ``context.row_scope`` carries the grant."""
    overrides, payload, trigger_prop = READ_CASES[case]
    adapter = _adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    principal = Principal()
    grant = claims_authorizer().authorize(
        principal=principal, table=TABLE, operation="export" if PROTOCOL_KIND[case] == "export" else "read"
    )
    state = _snake_state({**BASE_STATE, **overrides})
    kind = PROTOCOL_KIND[case]
    if kind == "drill":
        state.drill_through = dict(payload)
    elif kind == "detail":
        state.detail_request = dict(payload)
    elif kind == "export":
        state.export_request = dict(payload)
    elif kind == "chart":
        state.chart_request = dict(payload)
    elif kind == "filter_options":
        pytest.skip("Dash filter options call the adapter directly; covered by test_dash_filter_options_scoped")
    context = PivotRequestContext(
        table=TABLE,
        trigger_prop=trigger_prop,
        start_row=0,
        end_row=100,
        viewport_active=True,
        needs_col_schema=True,
        include_grand_total=True,
        principal=principal,
        row_scope=grant,
    )
    try:
        outcome: Any = asyncio.run(service.process_async(state, context))
    except PermissionError as exc:  # only an authorization denial counts
        outcome = exc
    _assert_scoped_or_denied(f"dash:{case}", outcome)


def test_dash_filter_options_scoped():
    """Mirror ``dash_callbacks`` filter-option path: adapter.handle_request(row_scope=grant)."""
    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest

    adapter = _adapter()
    principal = Principal()
    grant = claims_authorizer().authorize(principal=principal, table=TABLE, operation="read")
    request = TanStackRequest(
        operation=TanStackOperation.GET_UNIQUE_VALUES,
        table=TABLE,
        columns=[{"id": "tenant"}],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 100, "offset": 0, "search": ""},
        global_filter="tenant",
    )
    response = asyncio.run(adapter.handle_request(request, user=principal, row_scope=grant))
    values = [d.get("value") for d in (response.data or [])]
    found = leaks(response, tenant_values=values)
    assert not found, f"dash:filter_options: {found}"


# --------------------------------------------------------------------------- http edge


def _http_client(adapter) -> TestClient:
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    app = FastAPI()

    async def current_user():
        return Principal()

    register_runtime_http_transport(
        app,
        lambda: service,
        current_user_dependency=current_user,
        dataset_authorizer=claims_authorizer(),
    )
    return TestClient(app)


@pytest.mark.parametrize("case", sorted(READ_CASES))
def test_http_reads_are_scoped_or_denied(case):
    overrides, payload, _ = READ_CASES[case]
    client = _http_client(_adapter())
    envelope = {
        "schemaVersion": 1,
        "kind": PROTOCOL_KIND[case],
        "requestId": f"iso-{case}",
        "table": TABLE,
        "clientInstance": "isolation",
        "stateEpoch": 1,
        "windowSeq": 1,
        "state": {**BASE_STATE, **overrides},
        "payload": payload,
    }
    response = client.post("/v1/runtime", json=envelope)
    if response.status_code in (401, 403):
        return
    body = response.json()
    payload_ref = (body.get("payload") or {}).get("payloadRef") if isinstance(body.get("payload"), dict) else None
    if isinstance(payload_ref, dict) and payload_ref.get("url"):
        fetched = client.get(payload_ref["url"])
        body = {"envelope": body, "content": fetched.content}
    _assert_scoped_or_denied(f"http:{case}", body, filter_kind=PROTOCOL_KIND[case] == "filter_options")


@pytest.mark.parametrize("kind", sorted(WRITE_KINDS))
def test_http_writes_without_mutation_policy_are_denied(kind):
    client = _http_client(_adapter())
    envelope = {
        "schemaVersion": 1,
        "kind": kind,
        "requestId": f"iso-{kind}",
        "table": TABLE,
        "clientInstance": "isolation",
        "stateEpoch": 1,
        "windowSeq": 1,
        "state": BASE_STATE,
        "payload": {"rowId": "North", "colId": "amount_sum", "value": 5}
        if kind == "update"
        else {"update": [{"tenant": "B", "amount": 1}]},
    }
    response = client.post("/v1/runtime", json=envelope)
    if response.status_code in (401, 403):
        return
    body = response.json()
    assert body.get("status") in {"error", "denied", "forbidden"}, f"http:{kind} not denied: {body.get('status')}"


# --------------------------------------------------------------------------- cache reuse


def test_cache_is_not_shared_across_tenants():
    """Tenant B's identical request after tenant A must not see A's cached rows."""
    adapter = _adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    authorizer = claims_authorizer()

    def run(principal) -> Any:
        grant = authorizer.authorize(principal=principal, table=TABLE, operation="read")
        state = _snake_state(BASE_STATE)
        context = PivotRequestContext(
            table=TABLE, trigger_prop="pivot.viewport", start_row=0, end_row=100,
            viewport_active=True, include_grand_total=True, principal=principal, row_scope=grant,
        )
        return asyncio.run(service.process_async(state, context))

    first = run(Principal())
    second = run(Principal(id="tenant-b-user", attributes={"tenant": "B"}))
    second_text = str(_rows_from(second))
    assert "A-secret" not in second_text
    a_total = [r for r in _rows_from(first) if isinstance(r, dict)]
    b_total = [r for r in _rows_from(second) if isinstance(r, dict)]
    assert a_total != b_total or not a_total, "tenant B received tenant A's cached result"


# --------------------------------------------------------------------------- through the core (M2 gate)


@pytest.mark.parametrize("case", ["data", "drill", "drill_legacy_keys"])
def test_core_serves_scoped_reads(case, monkeypatch):
    """With PIVOT_CORE=on the core answers (not a legacy fallback) and stays scoped."""
    from pivot_engine.runtime import core_shadow

    monkeypatch.setenv("PIVOT_CORE", "on")
    kind = PROTOCOL_KIND[case]
    served = f"core_served_{'drill' if kind == 'drill' else 'hierarchy'}"
    before = core_shadow.STATS[served]
    overrides, payload, trigger_prop = READ_CASES[case]
    adapter = _adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    principal = Principal()
    grant = claims_authorizer().authorize(principal=principal, table=TABLE, operation="read")
    state = _snake_state({**BASE_STATE, **overrides})
    if kind == "drill":
        state.drill_through = dict(payload)
    context = PivotRequestContext(
        table=TABLE, trigger_prop=trigger_prop, start_row=0, end_row=100, viewport_active=True,
        needs_col_schema=True, include_grand_total=True, principal=principal, row_scope=grant,
    )
    outcome = asyncio.run(service.process_async(state, context))
    assert core_shadow.STATS[served] > before, f"{case} was not served by the core"
    assert _rows_from(outcome), f"{case}: the core returned nothing"
    _assert_scoped_or_denied(f"core:{case}", outcome)


@pytest.mark.parametrize("column", ["tenant", "region", "secret"])
def test_core_serves_scoped_filter_options(column, monkeypatch):
    """Filter options come from the core under PIVOT_CORE=on and expose only the grant's values."""
    from pivot_engine.runtime import core_shadow
    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest

    monkeypatch.setenv("PIVOT_CORE", "on")
    before = core_shadow.STATS["core_served_unique_values"]
    adapter = _adapter()
    principal = Principal()
    grant = claims_authorizer().authorize(principal=principal, table=TABLE, operation="read")
    request = TanStackRequest(
        operation=TanStackOperation.GET_UNIQUE_VALUES, table=TABLE, columns=[{"id": column}], filters={},
        sorting=[], grouping=["region"], aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 100, "offset": 0, "search": ""}, global_filter=column,
    )
    response = asyncio.run(adapter.handle_request(request, user=principal, row_scope=grant))
    values = [d.get("value") for d in (response.data or [])]
    assert core_shadow.STATS["core_served_unique_values"] > before
    assert values, "the core returned no options"
    found = leaks(response, tenant_values=values if column == "tenant" else ())
    assert not found, f"core:filter_options:{column}: {found}"


def test_core_serves_scoped_full_export(monkeypatch):
    """A full (all rows) export is produced by the core under PIVOT_CORE=on and stays scoped."""
    from pivot_engine.runtime import core_shadow

    monkeypatch.setenv("PIVOT_CORE", "on")
    before = core_shadow.STATS["core_served_hierarchy"]
    adapter = _adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    principal = Principal()
    grant = claims_authorizer().authorize(principal=principal, table=TABLE, operation="export")
    state = _snake_state(dict(BASE_STATE))
    state.export_request = {"format": "csv", "allRows": True}
    context = PivotRequestContext(
        table=TABLE, trigger_prop="pivot.exportRequest", start_row=0, end_row=100, viewport_active=True,
        needs_col_schema=True, include_grand_total=True, principal=principal, row_scope=grant,
    )
    outcome = asyncio.run(service.process_async(state, context))
    assert core_shadow.STATS["core_served_hierarchy"] > before, "the export was not served by the core"
    _assert_scoped_or_denied("core:export", outcome)
