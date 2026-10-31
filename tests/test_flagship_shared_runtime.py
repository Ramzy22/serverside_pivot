"""Exact shared runtime assertions for the read-only flagship fixture."""

import asyncio
from dataclasses import dataclass, field
import pyarrow as pa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine import create_tanstack_adapter
from pivot_engine.backends.execution import DatabaseExecutionError
from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest
from pivot_engine.runtime import (
    PivotRequestContext, PivotRuntimeService, PivotViewState, SessionRequestGate,
    RuntimeDatasetAuthorizer, register_runtime_http_transport,
)
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


def test_flagship_http_error_is_safe_and_next_request_recovers_exact_totals():
    """An actual V1 request fails once, then a distinct retry returns fixture data."""
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "flagship_shared", pa.Table.from_pylist(FLAGSHIP_QUALIFICATION_SOURCE_ROWS))

    class FailOnce:
        def __init__(self):
            self.failed = False

        def __getattr__(self, name):
            return getattr(adapter, name)

        async def handle_virtual_scroll_request(self, *args, **kwargs):
            if not self.failed:
                self.failed = True
                raise DatabaseExecutionError("PRIVATE_BACKEND_SENTINEL")
            return await adapter.handle_virtual_scroll_request(*args, **kwargs)

    @dataclass
    class Viewer:
        id: str = "flagship-shared"
        role: str = "viewer"
        attributes: dict = field(default_factory=dict)

    wrapped_adapter = FailOnce()
    service = PivotRuntimeService(adapter_getter=lambda: wrapped_adapter)
    app = FastAPI()
    register_runtime_http_transport(
        app, lambda: service, current_user_dependency=lambda: Viewer(),
        dataset_authorizer=RuntimeDatasetAuthorizer({
            "flagship_shared": {"roles": {"viewer"}, "operations": {"read"},
                                "rowSecurity": {"mode": "global"}},
        }),
        inline_payload_bytes=5_000_000,
    )
    state = {"rowFields": FLAGSHIP_ROW_FIELDS, "colFields": FLAGSHIP_COL_FIELDS,
             "valConfigs": FLAGSHIP_VALUE_CONFIGS, "filters": FLAGSHIP_FILTER}

    def request(request_id, sequence):
        return {"schemaVersion": 1, "kind": "data", "requestId": request_id,
                "table": "flagship_shared", "sessionId": "shared-error-retry",
                "clientInstance": "shared-browser", "stateEpoch": sequence,
                "windowSeq": sequence, "state": state,
                "payload": {"intent": "viewport", "start": 0, "end": 20,
                            "needsColSchema": True}}

    with TestClient(app) as client:
        failed = client.post("/v1/runtime", json=request("failed-window", 1))
        assert failed.status_code == 200, failed.text
        assert failed.json()["requestId"] == "failed-window"
        assert failed.json()["status"] == "error"
        assert "PRIVATE_BACKEND_SENTINEL" not in failed.text
        recovered = client.post("/v1/runtime", json=request("recovered-window", 2))
    assert recovered.status_code == 200
    body = recovered.json()
    assert body["requestId"] == "recovered-window"
    assert body["status"] == "data"
    rows = body["payload"]["data"]
    grand = next(row for row in rows if row.get("_id") == "Grand Total")
    assert [grand[f"{period}_{field}_sum"] for period, field in
            (("Q1", "ventes_€"), ("Q2", "ventes_€"), ("Q1", "unités"), ("Q2", "unités"))] == [150, 50, 5, 5]
    assert grand["Q1_écart_sum"] + grand["Q2_écart_sum"] == -6.75


def test_flagship_busy_rejection_preserves_generation_and_recovers_exact_data():
    from pivot_engine import ScalablePivotController
    from pivot_engine.tanstack_adapter import TanStackPivotAdapter
    from pivot_engine.resource_governor import ResourceGovernor, ResourceKind, ResourceLimits

    governor = ResourceGovernor(ResourceLimits(max_queries=1, query_bytes=16 * 1024 * 1024),
                                rss_bytes_getter=lambda: 0)
    controller = ScalablePivotController(backend_uri=":memory:", resource_governor=governor)
    controller.load_data_from_arrow("flagship_shared", pa.Table.from_pylist(FLAGSHIP_QUALIFICATION_SOURCE_ROWS))
    adapter = TanStackPivotAdapter(controller)
    service = PivotRuntimeService(lambda: adapter, resource_governor=governor,
                                  session_gate=SessionRequestGate())

    @dataclass
    class Viewer:
        id: str = "flagship-busy-test"
        role: str = "viewer"
        attributes: dict = field(default_factory=dict)

    app = FastAPI()
    register_runtime_http_transport(
        app, lambda: service, current_user_dependency=lambda: Viewer(),
        dataset_authorizer=RuntimeDatasetAuthorizer({
            "flagship_shared": {"roles": {"viewer"}, "operations": {"read"},
                                "rowSecurity": {"mode": "global"}},
        }), inline_payload_bytes=5_000_000,
    )
    def request(sequence):
        return {"schemaVersion": 1, "kind": "data", "requestId": f"busy-window-{sequence}",
                "table": "flagship_shared", "clientInstance": "busy-client", "stateEpoch": 3,
                "windowSeq": sequence, "state": {"rowFields": FLAGSHIP_ROW_FIELDS,
                    "colFields": FLAGSHIP_COL_FIELDS, "valConfigs": FLAGSHIP_VALUE_CONFIGS,
                    "filters": FLAGSHIP_FILTER},
                "payload": {"intent": "viewport", "start": 0, "end": 20, "needsColSchema": True}}

    with TestClient(app) as client:
        with governor.reserve(ResourceKind.QUERY, 1):
            rejected = client.post("/v1/runtime", json=request(7))
        assert rejected.status_code == 503
        assert rejected.headers["Retry-After"] == "1"
        busy = rejected.json()
        assert (busy["requestId"], busy["clientInstance"], busy["stateEpoch"], busy["windowSeq"]) == (
            "busy-window-7", "busy-client", 3, 7)
        assert busy["payload"] == {"code": "resource_exhausted", "resourceKind": "query", "retryAfterMs": 1000}
        recovered = client.post("/v1/runtime", json=request(8))
    assert recovered.status_code == 200
    body = recovered.json()
    assert body["status"] == "data" and body["windowSeq"] == 8
    grand = next(row for row in body["payload"]["data"] if row["_id"] == "Grand Total")
    assert [grand[_measure(period, 0)] for period in ("Q1", "Q2")] == [150, 50]
    assert [grand[_measure(period, 1)] for period in ("Q1", "Q2")] == [5, 5]
    assert governor.snapshot()["active_slots"]["query"] == 0


def _response(service, expanded, sequence, *, filters=None, val_configs=None,
              sorting=None, show_row_totals=False, show_col_totals=True,
              layout_manifest=None, layout_revision=None, needs_col_schema=False,
              col_start=0, col_end=None, pinned_column_ids=(), row_fields=None, col_fields=None, layout_mode=None):
    return service.process(
        PivotViewState(
            row_fields=FLAGSHIP_ROW_FIELDS if row_fields is None else row_fields,
            col_fields=FLAGSHIP_COL_FIELDS if col_fields is None else col_fields,
            val_configs=val_configs or FLAGSHIP_VALUE_CONFIGS,
            filters={**FLAGSHIP_FILTER, **(filters or {})},
            expanded=expanded,
            sorting=sorting or [],
            show_row_totals=show_row_totals,
            show_col_totals=show_col_totals,
            layout_mode=layout_mode,
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
            layout_manifest=layout_manifest,
            layout_revision=layout_revision,
            col_start=col_start,
            col_end=col_end,
            pinned_column_ids=pinned_column_ids,
            needs_col_schema=needs_col_schema,
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


def test_flagship_axis_swap_round_trip_preserves_exact_values():
    from tests.flagship_axis_swap import assert_axis_rows

    service = _runtime()
    before = _response(service, {}, 1, show_row_totals=True, needs_col_schema=True)
    swapped = _response(service, {}, 2, show_row_totals=True, needs_col_schema=True,
                        row_fields=["period"], col_fields=["region", "account"])
    restored = _response(service, {}, 3, show_row_totals=True, needs_col_schema=True)
    for response, transposed in ((before, False), (swapped, True), (restored, False)):
        assert response.status == "data"
        assert_axis_rows(response.data, transposed=transposed)
        assert response.layout_revision == response.col_schema["layoutRevision"]
    assert swapped.layout_revision != before.layout_revision
    assert restored.layout_revision == before.layout_revision
    assert restored.col_schema == before.col_schema


def test_flagship_layout_modes_preserve_exact_keys_and_totals():
    from tests.flagship_layout_modes import assert_layout_rows

    service = _runtime()
    observations = []
    for sequence, mode in enumerate(("hierarchy", "outline", "tabular", "hierarchy"), start=1):
        response = _response(service, {"__ALL__": True}, sequence, show_row_totals=True,
                             needs_col_schema=True, layout_mode=mode)
        assert response.status == "data"
        assert_layout_rows(response.data, mode)
        assert response.layout_revision == response.col_schema["layoutRevision"]
        observations.append(response)
    assert [row["_path"] for row in observations[0].data] == [row["_path"] for row in observations[-1].data]
    assert observations[0].col_schema == observations[-1].col_schema


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


def test_flagship_region_filter_options_follow_scope_and_exact_selection():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "flagship_shared", pa.Table.from_pylist(FLAGSHIP_QUALIFICATION_SOURCE_ROWS))
    request = TanStackRequest(
        operation=TanStackOperation.GET_UNIQUE_VALUES,
        table="flagship_shared",
        columns=[], filters=FLAGSHIP_FILTER, sorting=[], grouping=[], aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 250, "offset": 0},
        global_filter="region",
    )
    response = asyncio.run(adapter.handle_request(request))
    first = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[0]["region"]
    second = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[2]["region"]
    excluded = FLAGSHIP_QUALIFICATION_SOURCE_ROWS[4]["region"]
    values = [row["value"] for row in response.data]
    assert len(values) == 2 and set(values) == {first, second}
    assert excluded not in values
    assert response.pagination["totalRows"] == 2

    filtered = _response(_runtime(), {}, 1, filters={
        "region": {"operator": "AND", "conditions": [
            {"type": "in", "value": [second], "caseSensitive": False}]},
    })
    rows = _by_id(filtered)
    assert set(rows) == {second, "Grand Total"}
    assert sum(rows["Grand Total"][_measure(period, 0)] for period in ("Q1", "Q2")) == 79.75


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


def test_custom_measure_order_survives_aggregation_change():
    service = _runtime()
    initial = _response(service, {}, 1, show_row_totals=True, needs_col_schema=True)
    assert initial.col_schema and initial.col_schema["columns"]
    entries = {entry["id"]: entry["layoutId"] for entry in initial.col_schema["columns"] if entry}
    sales = FLAGSHIP_VALUE_CONFIGS[0]["field"]
    units = FLAGSHIP_VALUE_CONFIGS[1]["field"]
    variance = FLAGSHIP_VALUE_CONFIGS[2]["field"]
    preferred_ids = [f"{period}{'' if period == '__RowTotal__' else '_'}{field}_sum"
                     for period in ("Q1", "Q2", "__RowTotal__")
                     for field in (units, sales, variance)]
    manifest = tuple(entries[column_id] for column_id in preferred_ids)
    summed = _response(service, {}, 2, show_row_totals=True,
                       layout_manifest=manifest, needs_col_schema=True)
    average_configs = [{**FLAGSHIP_VALUE_CONFIGS[0], "agg": "avg"}, *FLAGSHIP_VALUE_CONFIGS[1:]]
    averaged = _response(service, {}, 3, val_configs=average_configs,
                         show_row_totals=True, layout_manifest=manifest,
                         needs_col_schema=True)
    summed_ids = [column["id"] for column in summed.columns]
    averaged_ids = [column["id"] for column in averaged.columns]
    assert summed_ids.index(f"Q1_{sales}_sum") == averaged_ids.index(f"Q1_{sales}_avg")
    assert summed_ids.index(f"Q2_{sales}_sum") == averaged_ids.index(f"Q2_{sales}_avg")
    assert next(row for row in averaged.data if row["_id"] == "Grand Total")[f"Q1_{sales}_avg"] == 75


def test_duplicate_source_measures_keep_distinct_layout_identities():
    service = _runtime()
    sales = FLAGSHIP_VALUE_CONFIGS[0]["field"]
    configs = [FLAGSHIP_VALUE_CONFIGS[0], {**FLAGSHIP_VALUE_CONFIGS[0], "agg": "avg"}]
    response = _response(service, {}, 1, val_configs=configs, needs_col_schema=True)
    entries = {entry["id"]: entry["layoutId"] for entry in response.col_schema["columns"] if entry}
    assert entries[f"Q1_{sales}_sum"] != entries[f"Q1_{sales}_avg"]


def test_secondary_aggregation_changes_revision_without_moving_saved_identity():
    service = _runtime()
    initial = _response(service, {}, 1, needs_col_schema=True)
    manifest = tuple(entry["layoutId"] for entry in initial.col_schema["columns"] if entry)
    baseline = _response(service, {}, 2, layout_manifest=manifest, needs_col_schema=True)
    configs = [FLAGSHIP_VALUE_CONFIGS[0], {**FLAGSHIP_VALUE_CONFIGS[1], "agg": "avg"},
               FLAGSHIP_VALUE_CONFIGS[2]]
    averaged = _response(service, {}, 3, val_configs=configs, layout_manifest=manifest,
                         needs_col_schema=True)
    before = {entry["index"]: entry for entry in initial.col_schema["columns"] if entry}
    after = {entry["index"]: entry for entry in averaged.col_schema["columns"] if entry}
    unit_key = _measure("Q1", 1)
    position = next(index for index, entry in before.items() if entry["id"] == unit_key)
    assert after[position]["layoutId"] == before[position]["layoutId"]
    assert after[position]["id"] == f"Q1_{FLAGSHIP_VALUE_CONFIGS[1]['field']}_avg"
    assert baseline.layout_revision != averaged.layout_revision
    assert averaged.col_schema["layoutRevision"] == averaged.layout_revision


def test_flagship_custom_order_sparse_window_and_off_window_pin():
    service = _runtime()
    initial = _response(service, {}, 1, needs_col_schema=True)
    catalog = {entry["id"]: entry["layoutId"] for entry in initial.col_schema["columns"] if entry}
    order = [_measure(period, measure) for period in ("Q2", "Q1") for measure in (1, 0, 2)]
    manifest = tuple(catalog[column] for column in order)
    pin = (catalog[_measure("Q1", 0)],)
    moved = _response(service, {}, 2, needs_col_schema=True, layout_manifest=manifest,
                      col_start=0, col_end=1, pinned_column_ids=pin)
    token_only = _response(service, {}, 3, needs_col_schema=True,
                           layout_revision=moved.layout_revision,
                           col_start=0, col_end=1, pinned_column_ids=pin)
    for response in (moved, token_only):
        assert response.status == "data"
        assert response.layout_revision == moved.layout_revision
        assert response.col_schema["layoutRevision"] == response.layout_revision
        entries = [entry for entry in response.col_schema["columns"] if entry]
        assert [(entry["index"], entry["id"]) for entry in entries] == [
            (0, _measure("Q2", 1)), (1, _measure("Q2", 0)), (4, _measure("Q1", 0))]
        grand = next(row for row in response.data if row["_id"] == "Grand Total")
        assert [grand[_measure(period, measure)] for period, measure in
                (("Q2", 1), ("Q2", 0), ("Q1", 0))] == [5, 50, 150]
        assert _measure("Q1", 1) not in grand
        assert _measure("Q2", 2) not in grand


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


def test_flagship_totals_controls_remove_only_requested_totals_and_restore():
    service = _runtime()
    variants = [
        _response(service, {}, 1, show_row_totals=True, show_col_totals=True),
        _response(service, {}, 2, show_row_totals=False, show_col_totals=True),
        _response(service, {}, 3, show_row_totals=True, show_col_totals=False),
        _response(service, {}, 4, show_row_totals=False, show_col_totals=False),
        _response(service, {}, 5, show_row_totals=True, show_col_totals=True),
    ]
    first, no_row_totals, no_grand, neither, restored = variants
    expected_groups = {FLAGSHIP_QUALIFICATION_SOURCE_ROWS[index]["region"] for index in (0, 2)}
    for response, has_row_totals, has_grand in zip(
        variants, (True, False, True, False, True), (True, True, False, False, True)):
        rows = _by_id(response)
        assert set(rows) == expected_groups | ({"Grand Total"} if has_grand else set())
        total_columns = [column["id"] for column in response.columns
                         if column["id"].startswith("__RowTotal__")]
        assert len(total_columns) == (3 if has_row_totals else 0)
        assert len(total_columns) == len(set(total_columns))
        assert [column["id"] for column in response.columns if column["id"].startswith("Q1_")]
    assert _by_id(first)["Grand Total"][_measure("Q1", 0)] == 150.0
    assert _by_id(first)["Grand Total"][_measure("Q2", 0)] == 50.0
    assert _by_id(first)["Grand Total"][f"__RowTotal__{FLAGSHIP_VALUE_CONFIGS[0]['field']}_sum"] == 200.0
    assert [column["id"] for column in first.columns] == [column["id"] for column in restored.columns]
    assert _by_id(first)["Grand Total"] == _by_id(restored)["Grand Total"]


def test_flagship_add_and_remove_units_measure_preserves_sales():
    service = _runtime()
    sales_only = [FLAGSHIP_VALUE_CONFIGS[0]]
    with_units = FLAGSHIP_VALUE_CONFIGS[:2]
    before = _response(service, {}, 1, val_configs=sales_only)
    added = _response(service, {}, 2, val_configs=with_units)
    restored = _response(service, {}, 3, val_configs=sales_only)
    units_keys = [_measure(period, 1) for period in ("Q1", "Q2")]
    for response in (before, restored):
        assert all(key not in _by_id(response)["Grand Total"] for key in units_keys)
        assert all(key not in [column["id"] for column in response.columns] for key in units_keys)
        assert [response.data[-1][_measure(period, 0)] for period in ("Q1", "Q2")] == [150.0, 50.0]
    assert [added.data[-1][key] for key in units_keys] == [5, 5]
    assert sum(added.data[-1][key] for key in units_keys) == 10
    assert [added.data[-1][_measure(period, 0)] for period in ("Q1", "Q2")] == [150.0, 50.0]
    assert [column["id"] for column in before.columns] == [column["id"] for column in restored.columns]


def test_flagship_layout_history_models_preserve_exact_values():
    """Authoritative restored models; browser tests own actual history controls."""
    from tests.flagship_axis_swap import MEASURES, assert_axis_rows
    service = _runtime()
    models = ((False, 1), (False, 2), (True, 2), (False, 2), (False, 1),
              (False, 2), (True, 2), (False, 2), (False, 3))
    observations = []
    for sequence, (transposed, count) in enumerate(models, start=1):
        response = _response(service, {}, sequence, show_row_totals=True, needs_col_schema=True,
                             val_configs=FLAGSHIP_VALUE_CONFIGS[:count],
                             row_fields=["period"] if transposed else None,
                             col_fields=["region", "account"] if transposed else None)
        assert response.status == "data"
        assert_axis_rows(response.data, transposed=transposed, measures=MEASURES[:count])
        assert response.layout_revision == response.col_schema["layoutRevision"]
        observations.append(response)
    for indices in ((0, 4), (1, 3, 5, 7), (2, 6)):
        first = observations[indices[0]]
        assert all(observations[index].data == first.data and observations[index].col_schema == first.col_schema
                   and observations[index].layout_revision == first.layout_revision for index in indices)
    assert len({response.layout_revision for response in observations}) == 4


def test_flagship_saved_model_round_trip_preserves_exact_values():
    """Backend restored-model equivalence; browser owns storage and geometry."""
    import json
    from tests.flagship_axis_swap import assert_axis_rows
    model = {"rows": ["period"], "cols": ["region", "account"], "values": FLAGSHIP_VALUE_CONFIGS,
             "filters": FLAGSHIP_FILTER, "columnSizing": {"sales": 170}, "spacingMode": 0}
    restored = json.loads(json.dumps(model))
    service = _runtime()
    responses = [_response(service, {}, index, show_row_totals=True, needs_col_schema=True,
                           row_fields=state["rows"], col_fields=state["cols"],
                           val_configs=state["values"], filters=state["filters"])
                 for index, state in enumerate((model, restored), start=1)]
    for response in responses:
        assert response.status == "data"
        assert_axis_rows(response.data, transposed=True)
        assert response.layout_revision == response.col_schema["layoutRevision"]
    assert responses[0].data == responses[1].data
    assert responses[0].col_schema == responses[1].col_schema
    assert responses[0].layout_revision == responses[1].layout_revision
