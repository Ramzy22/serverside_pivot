"""Executable derived-model runtime checks and mutation-tested browser oracles."""
import copy
from pathlib import Path

import pyarrow as pa
import pytest

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState, SessionRequestGate
from tests.export_qualification_fixture import FLAGSHIP_QUALIFICATION_SOURCE_ROWS, FLAGSHIP_FILTER
from tests.flagship_remaining_2 import (
    REQUIREMENTS, SALES, ECLAIR, MUNICH, expected_sales, assert_model_values,
    assert_model_dom, assert_zones, starting_state,
)


def _service():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("derived_models", pa.Table.from_pylist(FLAGSHIP_QUALIFICATION_SOURCE_ROWS))
    return PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())


def _response(service, sequence, *, context_changes=None, **changes):
    kwargs = {"row_fields": ["region", "account"], "col_fields": ["period"],
              "val_configs": [{"field": SALES, "agg": "sum", "label": "Sales"}],
              "filters": copy.deepcopy(FLAGSHIP_FILTER), "show_row_totals": True,
              "show_col_totals": True, "expanded": {}}
    kwargs.update(changes)
    context = dict(
        table="derived_models", trigger_prop="derived.viewport", session_id="derived-models",
        client_instance="derived-runtime", state_epoch=sequence, window_seq=sequence,
        viewport_active=True, start_row=0, end_row=100, needs_col_schema=True, include_grand_total=True)
    context.update(context_changes or {})
    response = service.process(PivotViewState(**kwargs), PivotRequestContext(**context))
    assert response.status == "data", response.error
    return response


def test_all_derived_model_cases_have_installed_host_mappings():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements
    plan = _product_browser_plan(load_requirements(Path(__file__).with_name("flagship_requirements.json")))
    for requirement in REQUIREMENTS:
        for host in ("dash", "react", "streamlit"):
            assert plan[(requirement, host, "browser." + requirement)] == (
                f"tests/test_flagship_workflows.py::test_flagship_derived_models[{requirement}-{host}]")


def test_shared_field_zone_move_restores_exact_sparse_intersections():
    service = _service()
    before = _response(service, 1, row_fields=["region", "account", "scope"])
    moved = _response(service, 2, row_fields=["region"], col_fields=["period", "account"])
    restored = _response(service, 3, row_fields=["region", "account", "scope"])
    assert_model_values(before.data, expected_sales())
    assert_model_values(moved.data, expected_sales("moved"))
    assert_model_values(restored.data, expected_sales())
    assert before.col_schema == restored.col_schema
    assert before.layout_revision != moved.layout_revision


def test_shared_formula_add_remove_uses_post_sum_totals():
    service = _service()
    baseline = _response(service, 1)
    config = {"field": "formula_1", "label": "double_sales", "formulaRef": "double_sales",
              "agg": "formula", "formula": f"2 * [{SALES}]", "formulaScope": "measures"}
    added = _response(service, 2, val_configs=[{"field": SALES, "agg": "sum", "label": "Sales"}, config])
    restored = _response(service, 3)
    assert_model_values(added.data, expected_sales(formula_field="formula_1"))
    for response in (baseline, restored):
        assert_model_values(response.data, expected_sales())
    assert baseline.col_schema == restored.col_schema


def test_shared_custom_category_add_remove_preserves_source_data():
    source = copy.deepcopy(FLAGSHIP_QUALIFICATION_SOURCE_ROWS)
    service = _service()
    baseline = _response(service, 1)
    field = "__custom_category__includedgroup"
    dimension = {"id": "includedgroup", "field": field, "name": "IncludedGroup", "fallbackLabel": "Other",
                 "rules": [{"id": "cities", "label": "Included cities", "condition": {
                     "op": "AND", "clauses": [{"field": "region", "operator": "in", "values": [ECLAIR, MUNICH]}]}}]}
    grouped = _response(service, 2, row_fields=[field], custom_dimensions=[dimension])
    restored = _response(service, 3)
    assert_model_values(grouped.data, expected_sales("custom"))
    assert_model_values(restored.data, expected_sales())
    assert restored.data == baseline.data
    assert FLAGSHIP_QUALIFICATION_SOURCE_ROWS == source


def test_shared_report_definition_round_trip_preserves_period_sales():
    service = _service()
    report = {"root": {"field": "region", "label": "Region", "branches": []}}
    before = _response(service, 1, view_mode="report", report_def=report)
    changed = _response(service, 2, view_mode="report", report_def={"root": {"field": "account", "label": "Account", "branches": []}})
    restored = _response(service, 3, view_mode="report", report_def=copy.deepcopy(report))
    assert_model_values(before.data, expected_sales())
    assert_model_values(restored.data, expected_sales())
    assert before.layout_revision == before.col_schema["layoutRevision"]
    assert before.data == restored.data and before.col_schema == restored.col_schema
    assert {row.get("_id") for row in changed.data if "_id" in row} != {row.get("_id") for row in restored.data if "_id" in row}
    assert_model_values(_response(service, 4).data, expected_sales())


def test_report_period_schema_sparse_children_other_and_grand_respect_row_scope():
    from types import SimpleNamespace
    from pivot_engine.runtime import RuntimeDatasetAuthorizer
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("derived_models", pa.Table.from_pylist([
        {"tenant": "A", "scope": "Included", "region": "North", "account": "First", "period": "Q1", SALES: 10},
        {"tenant": "A", "scope": "Included", "region": "North", "account": "Second", "period": "Q2", SALES: 30},
        {"tenant": "B", "scope": "Included", "region": "North", "account": "PRIVATE", "period": "PRIVATE", SALES: 999},
    ]))
    calls = []
    class ObservedAdapter:
        def __getattr__(self, name): return getattr(adapter, name)
        async def handle_request(self, request, **kwargs):
            calls.append(("aggregate", request, kwargs))
            return await adapter.handle_request(request, **kwargs)
        async def handle_virtual_scroll_request(self, request, *args, **kwargs):
            calls.append(("pivot", request, kwargs))
            return await adapter.handle_virtual_scroll_request(request, *args, **kwargs)
    user = SimpleNamespace(id="viewer-A", role="viewer", attributes={"tenant": "A"})
    scope = RuntimeDatasetAuthorizer({"derived_models": {"roles": {"viewer"}, "operations": {"read"},
        "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}}}}).authorize(
            principal=user, table="derived_models", operation="read")
    response = _response(PivotRuntimeService(adapter_getter=lambda: ObservedAdapter()), 1,
        context_changes={"principal": user, "row_scope": scope}, view_mode="report", expanded=True,
        report_def={"root": {"field": "region", "defaultChild": {"field": "account", "topN": 1,
            "sortBy": f"__RowTotal__{SALES}_sum", "sortDir": "desc"}}})
    expected = {(row, f"{prefix}{SALES}_sum"): value
                for row, values in {"North": (10, 30, 40), "North|||Second": (None, 30, 30),
                                    "North|||__other__": (10, None, 10), "__grand_total__": (10, 30, 40)}.items()
                for prefix, value in zip(("Q1_", "Q2_", "__RowTotal__"), values)}
    assert_model_values(response.data, expected)
    assert {column["id"] for column in response.col_schema["columns"]} == {column for _, column in expected}
    assert {kind for kind, _, _ in calls} == {"aggregate", "pivot"}
    assert len(calls) == 4  # root, child, Other, grand; every one is scoped.
    assert all(kwargs["user"] is user and kwargs["row_scope"] is scope for _, _, kwargs in calls)
    assert all("PRIVATE" not in str(row) for row in response.data)


def test_saved_report_layout_runtime_and_frontend_modules_compile():
    import shutil
    import subprocess
    node = shutil.which("node")
    assert node, "Node is required to exercise actual saved report runtime"
    result = subprocess.run([node, str(Path(__file__).with_name("frontend_flagship_reports.cjs"))],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "saved report runtime passed" in result.stdout


@pytest.mark.parametrize("damage", ["value", "missing", "extra", "duplicate", "null", "group"])
def test_model_oracles_reject_incomplete_or_wrong_evidence(damage):
    expected = expected_sales("moved")
    rows = []
    for row in dict.fromkeys(owner for owner, _ in expected):
        rows.append({"_id": row, "_path": row, **{column: value for (owner, column), value in expected.items() if owner == row}})
    assert_model_values(rows, expected)
    cells = [{"rowid": row, "colid": column, "text": "" if value is None else f"{value:.2f}"} for (row, column), value in expected.items()]
    assert_model_dom(cells, expected)
    bad_rows, bad_cells = copy.deepcopy(rows), copy.deepcopy(cells)
    column = next(column for (owner, column), value in expected.items() if owner == bad_rows[0]["_path"] and value is not None)
    if damage == "value": bad_rows[0][column] = 999; bad_cells[0]["text"] = "999"
    elif damage == "missing": del bad_rows[0][column]; bad_cells.pop()
    elif damage == "extra": bad_rows[0]["stale_sum"] = 9; bad_cells.append({"rowid": ECLAIR, "colid": "stale_sum", "text": "9"})
    elif damage == "duplicate": bad_rows.append(dict(bad_rows[0])); bad_cells.append(dict(bad_cells[0]))
    elif damage == "null":
        null_column = next(key for key, value in bad_rows[0].items() if value is None)
        bad_rows[0][null_column] = 0
        next(cell for cell in bad_cells if cell["rowid"] == bad_rows[0]["_path"] and cell["colid"] == null_column)["text"] = "0.00"
    else: bad_rows[0]["_path"] = "Excluded"; bad_cells[0]["rowid"] = "Excluded"
    with pytest.raises(AssertionError): assert_model_values(bad_rows, expected)
    with pytest.raises(AssertionError): assert_model_dom(bad_cells, expected)


def test_field_zone_oracle_rejects_duplicate_or_misplaced_fields():
    zones = {"rows": ["Region"], "cols": ["Period", "Account"], "filter": ["Scope"], "vals": ["Sales"]}
    assert_zones(zones, moved=True)
    for zone, label in (("rows", "Account"), ("cols", "Scope"), ("filter", "Scope")):
        bad = copy.deepcopy(zones); bad[zone].append(label)
        with pytest.raises(AssertionError): assert_zones(bad, moved=True)


def test_report_gear_metadata_cannot_hide_extra_or_duplicate_cells():
    expected = expected_sales()
    cells = [{"rowid": row, "colid": column, "text": f"{value:.2f}"}
             for (row, column), value in expected.items()]
    gears = [{"rowid": row, "colid": "__report_config__", "text": ""}
             for row in dict.fromkeys(row for row, _ in expected)]
    assert_model_dom(cells + gears, expected)
    for damage in ("value", "duplicate", "missing"):
        bad = copy.deepcopy(gears)
        if damage == "value": bad[0]["text"] = "999"
        elif damage == "duplicate": bad.append(dict(bad[0]))
        else: bad.pop()
        with pytest.raises(AssertionError): assert_model_dom(cells + bad, expected)


def test_startup_configuration_has_no_authored_formula_category_or_report():
    for requirement in REQUIREMENTS:
        state = starting_state(requirement)
        assert state["customDimensions"] == state["savedReports"] == []
        assert state["reportDef"] == {"levels": []}
        assert state["valConfigs"] == [{"field": SALES, "agg": "sum", "label": "Sales"}]
