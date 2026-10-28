"""Exact shared runtime assertions for the read-only flagship fixture."""

import pyarrow as pa

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState, SessionRequestGate
from tests.export_qualification_fixture import (
    FLAGSHIP_COL_FIELDS,
    FLAGSHIP_FILTER,
    FLAGSHIP_QUALIFICATION_SOURCE_ROWS,
    FLAGSHIP_ROW_FIELDS,
    FLAGSHIP_VALUE_CONFIGS,
)


def _runtime():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "flagship_shared", pa.Table.from_pylist(FLAGSHIP_QUALIFICATION_SOURCE_ROWS))
    return PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())


def _response(service, expanded, sequence):
    return service.process(
        PivotViewState(
            row_fields=FLAGSHIP_ROW_FIELDS,
            col_fields=FLAGSHIP_COL_FIELDS,
            val_configs=FLAGSHIP_VALUE_CONFIGS,
            filters=FLAGSHIP_FILTER,
            expanded=expanded,
        ),
        PivotRequestContext(
            table="flagship_shared",
            trigger_prop="pivot.viewport",
            session_id="shared-flagship",
            client_instance="shared-client",
            state_epoch=sequence,
            window_seq=sequence,
            viewport_active=True,
            start_row=0,
            end_row=100,
        ),
    )


def _measure(period, index):
    return f"{period}_{FLAGSHIP_VALUE_CONFIGS[index]['field']}_sum"


def _by_id(response):
    assert response.status == "data"
    assert response.total_rows == len(response.data)
    return {row["_id"]: row for row in response.data}


def _assert_grand_totals(response):
    grand = _by_id(response)["Grand Total"]
    assert grand["_isTotal"] is True
    assert [grand[_measure(period, 0)] for period in ("Q1", "Q2")] == [150.0, 50.0]
    assert [grand[_measure(period, 1)] for period in ("Q1", "Q2")] == [5, 5]
    assert [grand[_measure(period, 2)] for period in ("Q1", "Q2")] == [-8.25, 1.5]
    assert sum(grand[_measure(period, 0)] for period in ("Q1", "Q2")) == 200.0
    assert sum(grand[_measure(period, 1)] for period in ("Q1", "Q2")) == 10
    assert sum(grand[_measure(period, 2)] for period in ("Q1", "Q2")) == -6.75


def test_flagship_initial_workspace_has_filtered_pivot_totals():
    response = _response(_runtime(), {}, 1)
    first = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["region"]
    second = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[2]["region"]
    excluded = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[4]["region"]
    rows = _by_id(response)
    assert set(rows) == {first, second, "Grand Total"}
    assert excluded not in rows
    assert all(rows[label]["depth"] == 0 for label in (first, second))
    assert [rows[first][_measure(period, 0)] for period in ("Q1", "Q2")] == [100.25, 20.0]
    assert [rows[second][_measure(period, 0)] for period in ("Q1", "Q2")] == [49.75, 30.0]
    _assert_grand_totals(response)


def test_flagship_one_parent_expansion_round_trip_preserves_totals_and_columns():
    service = _runtime()
    parent = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["region"]
    child = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["account"]
    collapsed = _response(service, {}, 1)
    expanded = _response(service, {parent: True}, 2)
    restored = _response(service, {parent: False}, 3)

    assert len(expanded.data) == len(collapsed.data) + 1
    rows = _by_id(expanded)
    assert rows[child]["depth"] == 1
    assert rows[child]["_path"] == f"{parent}|||{child}"
    assert rows[child][_measure("Q1", 0)] == 100.25
    assert rows[child][_measure("Q2", 0)] == 20.0
    assert sum(rows[child][_measure(period, 0)] for period in ("Q1", "Q2")) == 120.25
    assert rows[parent][_measure("Q1", 0)] == 100.25
    assert rows[parent][_measure("Q2", 0)] == 20.0
    assert child not in _by_id(restored)
    assert {column["id"] for column in collapsed.columns} == {column["id"] for column in expanded.columns}
    assert {column["id"] for column in collapsed.columns} == {column["id"] for column in restored.columns}
    _assert_grand_totals(expanded)
    _assert_grand_totals(restored)


def test_flagship_expand_all_and_collapse_all_have_unique_logical_rows():
    service = _runtime()
    parents = {FLAGSHIP_QUALIFICATION_SOURCE_ROWS[index]["region"] for index in (0, 2)}
    children = {FLAGSHIP_QUALIFICATION_SOURCE_ROWS[index]["account"] for index in (0, 2)}
    expanded = _response(service, {"__ALL__": True}, 1)
    collapsed = _response(service, {}, 2)
    expanded_rows = _by_id(expanded)
    assert set(expanded_rows) == parents | children | {"Grand Total"}
    assert len(expanded.data) == len(expanded_rows)
    assert all(expanded_rows[child]["depth"] == 1 for child in children)
    assert set(_by_id(collapsed)) == parents | {"Grand Total"}
    assert len(collapsed.data) == len(_by_id(collapsed))
    _assert_grand_totals(expanded)
    _assert_grand_totals(collapsed)
