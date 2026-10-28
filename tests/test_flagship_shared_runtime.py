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


def _response(service, expanded, sequence, *, filters=None, val_configs=None,
              sorting=None, show_row_totals=False):
    return service.process(
        PivotViewState(
            row_fields=FLAGSHIP_ROW_FIELDS,
            col_fields=FLAGSHIP_COL_FIELDS,
            val_configs=val_configs or FLAGSHIP_VALUE_CONFIGS,
            filters={**FLAGSHIP_FILTER, **(filters or {})},
            expanded=expanded,
            sorting=sorting or [],
            show_row_totals=show_row_totals,
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


def test_flagship_global_search_and_clear_preserve_base_scope_filter():
    service = _runtime()
    matching = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[2]["region"]
    other = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["region"]
    excluded = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[4]["region"]
    filtered = _response(service, {}, 1, filters={"global": matching})
    rows = _by_id(filtered)
    assert set(rows) == {matching, "Grand Total"}
    assert rows["Grand Total"][_measure("Q1", 0)] == 49.75
    assert rows["Grand Total"][_measure("Q2", 0)] == 30.0
    assert sum(rows["Grand Total"][_measure(period, 0)] for period in ("Q1", "Q2")) == 79.75
    restored = _response(service, {}, 2)
    assert set(_by_id(restored)) == {matching, other, "Grand Total"}
    assert excluded not in _by_id(restored)
    _assert_grand_totals(restored)


def test_flagship_header_filter_and_clear_preserve_base_scope_filter():
    service = _runtime()
    matching = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["region"]
    other = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[2]["region"]
    filtered = _response(service, {}, 1, filters={
        "region": {"operator": "AND", "conditions": [
            {"type": "eq", "value": matching, "caseSensitive": False}]},
    })
    rows = _by_id(filtered)
    assert set(rows) == {matching, "Grand Total"}
    assert [rows["Grand Total"][_measure(period, 0)] for period in ("Q1", "Q2")] == [100.25, 20.0]
    assert sum(rows["Grand Total"][_measure(period, 0)] for period in ("Q1", "Q2")) == 120.25
    restored = _response(service, {}, 2)
    assert set(_by_id(restored)) == {matching, other, "Grand Total"}
    _assert_grand_totals(restored)


def test_flagship_sum_average_sum_round_trip_preserves_measure_position():
    service = _runtime()
    summed = _response(service, {}, 1)
    average_configs = [{**FLAGSHIP_VALUE_CONFIGS[0], "agg": "avg"}, *FLAGSHIP_VALUE_CONFIGS[1:]]
    averaged = _response(service, {}, 2, val_configs=average_configs)
    restored = _response(service, {}, 3)
    sum_key = _measure("Q1", 0)
    average_key = f"Q1_{FLAGSHIP_VALUE_CONFIGS[0]['field']}_avg"
    assert _by_id(summed)["Grand Total"][sum_key] == 150.0
    assert _by_id(averaged)["Grand Total"][average_key] == 75.0
    assert _by_id(restored)["Grand Total"][sum_key] == 150.0
    assert [column["id"] for column in summed.columns].index(sum_key) == [
        column["id"] for column in averaged.columns].index(average_key)
    assert [column["id"] for column in summed.columns] == [column["id"] for column in restored.columns]


def test_flagship_subtotals_do_not_double_count_expanded_children():
    service = _runtime()
    response = _response(service, {"__ALL__": True}, 1)
    rows = _by_id(response)
    first = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["region"]
    second = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[2]["region"]
    assert sum(rows[first][_measure(period, 0)] for period in ("Q1", "Q2")) == 120.25
    assert sum(rows[second][_measure(period, 0)] for period in ("Q1", "Q2")) == 79.75
    assert sum(rows[parent][_measure(period, 0)] for parent in (first, second)
               for period in ("Q1", "Q2")) == 200.0
    assert len(response.data) == len(rows) == 5
    _assert_grand_totals(response)


def test_flagship_total_sales_sort_reverses_groups_but_keeps_grand_total_last():
    service = _runtime()
    first = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["region"]
    second = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[2]["region"]
    total_key = f"__RowTotal__{FLAGSHIP_VALUE_CONFIGS[0]['field']}_sum"
    ascending = _response(service, {}, 1, sorting=[{"id": total_key, "desc": False}],
                          show_row_totals=True)
    descending = _response(service, {}, 2, sorting=[{"id": total_key, "desc": True}],
                           show_row_totals=True)
    assert [row["_id"] for row in ascending.data] == [second, first, "Grand Total"]
    assert [row["_id"] for row in descending.data] == [first, second, "Grand Total"]
    for response in (ascending, descending):
        rows = _by_id(response)
        assert [rows[label][total_key] for label in (second, first, "Grand Total")] == [79.75, 120.25, 200.0]
        assert total_key in [column["id"] for column in response.columns]
        _assert_grand_totals(response)
