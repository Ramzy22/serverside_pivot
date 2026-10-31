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


def _response(service, sequence, **changes):
    kwargs = {"row_fields": ["region", "account"], "col_fields": ["period"],
              "val_configs": [{"field": SALES, "agg": "sum", "label": "Sales"}],
              "filters": copy.deepcopy(FLAGSHIP_FILTER), "show_row_totals": True,
              "show_col_totals": True, "expanded": {}}
    kwargs.update(changes)
    response = service.process(PivotViewState(**kwargs), PivotRequestContext(
        table="derived_models", trigger_prop="derived.viewport", session_id="derived-models",
        client_instance="derived-runtime", state_epoch=sequence, window_seq=sequence,
        viewport_active=True, start_row=0, end_row=100, needs_col_schema=True))
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


def test_report_runtime_definition_round_trip_preserves_group_totals():
    """Supporting report calculation only; not mapped as period-column coverage."""
    service = _service()
    report = {"root": {"field": "region", "label": "Region", "branches": []}}
    before = _response(service, 1, view_mode="report", report_def=report)
    changed = _response(service, 2, view_mode="report", report_def={"root": {"field": "account", "label": "Account", "branches": []}})
    restored = _response(service, 3, view_mode="report", report_def=copy.deepcopy(report))
    for response in (before, restored):
        rows = {row["_id"]: row for row in response.data}
        assert set(rows) == {ECLAIR, MUNICH}
        assert rows[ECLAIR][f"{SALES}_sum"] == 120.25
        assert rows[MUNICH][f"{SALES}_sum"] == 79.75
        assert sum(row[f"{SALES}_sum"] for row in rows.values()) == 200
    assert before.data == restored.data and before.col_schema == restored.col_schema
    assert {row.get("_id") for row in changed.data if "_id" in row} != {row.get("_id") for row in restored.data if "_id" in row}
    assert_model_values(_response(service, 4).data, expected_sales())


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


def test_startup_configuration_has_no_authored_formula_category_or_report():
    for requirement in REQUIREMENTS:
        state = starting_state(requirement)
        assert state["customDimensions"] == state["savedReports"] == []
        assert state["reportDef"] == {"levels": []}
        assert state["valConfigs"] == [{"field": SALES, "agg": "sum", "label": "Sales"}]
