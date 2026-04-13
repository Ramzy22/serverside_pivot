import asyncio
import os
import sys

import pyarrow as pa
import pytest

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "pivot_engine"))

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState, SessionRequestGate
from pivot_engine.tanstack_adapter import TanStackResponse


def _make_adapter():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "South", "South"],
            "country": ["USA", "Canada", "Brazil", "Chile"],
            "sales": [100, 80, 120, 70],
            "cost": [10, 8, 12, 7],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    return adapter


def _make_duplicate_group_adapter():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "North", "South"],
            "country": ["USA", "USA", "Canada", "Brazil"],
            "sales": [100.0, 20.0, 80.0, 120.0],
            "cost": [10.0, 2.0, 8.0, 12.0],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    return adapter


def _make_weighted_average_adapter():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "South"],
            "rate": [0.10, 0.20, 0.40],
            "notional": [100.0, 300.0, 200.0],
        }
    )
    adapter.controller.load_data_from_arrow("rate_data", table)
    return adapter


def _make_absolute_sort_adapter():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "book": ["Low", "Large Loss", "Gain", "Mid Loss"],
            "pnl": [5.0, -100.0, 30.0, -50.0],
        }
    )
    adapter.controller.load_data_from_arrow("risk_data", table)
    return adapter


def test_runtime_service_works_without_dash():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="custom.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-runtime",
            "client_instance": "client-a",
            "intent": "viewport",
        },
    )
    state = PivotViewState(
        row_fields=["region", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        filters={},
        sorting=[],
        expanded={"North": True},
        show_row_totals=False,
        show_col_totals=True,
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert isinstance(response.data, list)
    assert response.total_rows is not None
    assert response.total_rows >= len(response.data)


@pytest.mark.asyncio
async def test_runtime_service_cancels_superseded_viewport_work():
    class SlowAdapter:
        def __init__(self):
            self.first_started = asyncio.Event()
            self.first_cancelled = False
            self.calls = 0

        async def handle_virtual_scroll_request(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                self.first_started.set()
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    self.first_cancelled = True
                    raise
            return TanStackResponse(
                data=[{"region": "North", "sales_sum": 1}],
                columns=[],
                total_rows=1,
                version=2,
            )

    adapter = SlowAdapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())
    state = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
    )
    base_viewport = {
        "start": 0,
        "end": 20,
        "state_epoch": 1,
        "abort_generation": 1,
        "session_id": "sess-cancel",
        "client_instance": "grid-a",
        "intent": "viewport",
    }
    first_context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="custom.viewport",
        viewport={**base_viewport, "window_seq": 1},
    )
    second_context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="custom.viewport",
        viewport={**base_viewport, "window_seq": 2},
    )

    first_task = asyncio.create_task(service.process_async(state, first_context))
    await asyncio.wait_for(adapter.first_started.wait(), timeout=1)
    second_response = await service.process_async(state, second_context)
    first_response = await asyncio.wait_for(first_task, timeout=1)

    assert second_response.status == "data"
    assert first_response.status == "stale"
    assert adapter.first_cancelled


def test_runtime_service_includes_profile_when_requested():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="custom.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 2,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-profile",
            "client_instance": "client-profile",
            "intent": "viewport",
            "requestId": "req-profile-1",
            "profile": True,
        },
    )
    state = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert response.profile is not None
    assert response.profile["request"]["requestId"] == "req-profile-1"
    assert response.profile["request"]["viewMode"] == "pivot"
    assert response.profile["service"]["totalMs"] is not None
    assert response.profile["adapter"]["operation"] == "virtual_scroll"
    assert response.profile["controller"]["operation"] == "hierarchy_view"


def test_runtime_service_preserves_absolute_sort_metadata():
    adapter = _make_absolute_sort_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="risk_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-absolute-sort",
            "client_instance": "grid-absolute-sort",
            "intent": "viewport",
            "include_grand_total": False,
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["book"],
        val_configs=[{"field": "pnl", "agg": "sum"}],
        sorting=[{"id": "pnl_sum", "desc": True, "sortType": "absolute", "absoluteSort": True}],
        show_row_totals=False,
        show_col_totals=False,
    )

    response = service.process(state, context)

    assert response.status == "data"
    ordered_books = [
        row["book"]
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]
    assert ordered_books == ["Large Loss", "Mid Loss", "Gain", "Low"]


def test_runtime_service_drillthrough_preserves_request_metadata():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.drillThrough",
        viewport={
            "window_seq": 3,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-drill",
            "client_instance": "client-drill",
            "intent": "structural",
        },
    )
    state = PivotViewState(
        row_fields=["region", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        drill_through={
            "row_path": "North",
            "row_fields": ["region"],
            "page": 0,
            "page_size": 1,
            "sort_col": "sales",
            "sort_dir": "desc",
            "filter": "",
        },
    )

    response = service.process(state, context)

    assert response.status == "drillthrough"
    assert response.drill_payload is not None
    assert response.drill_payload["page"] == 0
    assert response.drill_payload["pageSize"] == 1
    assert response.drill_payload["totalRows"] == 2
    assert response.drill_payload["rowPath"] == "North"
    assert response.drill_payload["rowFields"] == ["region"]
    assert len(response.drill_payload["rows"]) == 1
    assert response.drill_payload["rows"][0]["sales"] == 100


def test_request_context_accepts_camel_case_runtime_metadata():
    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.runtimeRequest",
        viewport={
            "start": 5,
            "end": 25,
            "windowSeq": 11,
            "stateEpoch": 3,
            "abortGeneration": 7,
            "sessionId": "sess-camel",
            "clientInstance": "client-camel",
            "intent": "viewport",
            "colStart": 2,
            "colEnd": 9,
            "needsColSchema": True,
            "includeGrandTotal": True,
        },
    )

    assert context.start_row == 5
    assert context.end_row == 25
    assert context.window_seq == 11
    assert context.state_epoch == 3
    assert context.abort_generation == 7
    assert context.session_id == "sess-camel"
    assert context.client_instance == "client-camel"
    assert context.col_start == 2
    assert context.col_end == 9
    assert context.needs_col_schema is True


def test_runtime_service_weighted_average_root_level_returns_rows():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "tenor": ["1M", "2W", "1D", "6Y"],
            "rate": [0.0310, 0.0280, 0.0250, 0.0450],
            "notional": [120.0, 80.0, 150.0, 60.0],
        }
    )
    adapter.controller.load_data_from_arrow("tenor_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="tenor_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-tenor-runtime",
            "client_instance": "grid-tenor-runtime",
            "intent": "viewport",
            "include_grand_total": False,
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["tenor"],
        val_configs=[
            {"field": "notional", "agg": "sum"},
            {"field": "rate", "agg": "weighted_avg", "weightField": "notional"},
        ],
        sorting=[{"id": "tenor", "desc": False, "semanticType": "tenor"}],
        filters={},
        expanded={},
        show_row_totals=False,
        show_col_totals=False,
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert response.total_rows == 4
    by_tenor = {
        row["tenor"]: row
        for row in (response.data or [])
        if isinstance(row, dict) and row.get("tenor")
    }
    assert set(by_tenor) == {"1D", "2W", "1M", "6Y"}
    assert by_tenor["1M"]["notional_sum"] == 120.0
    assert by_tenor["1M"]["rate_weighted_avg"] == pytest.approx(0.0310)


def test_runtime_service_batch_updates_refresh_visible_window():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    base_context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 8,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-update-batch",
            "client_instance": "grid-update-batch",
            "intent": "viewport",
            "needs_col_schema": True,
        },
    )
    base_state = PivotViewState(
        row_fields=["region", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        expanded={"North": True, "South": True},
        show_col_totals=False,
    )

    initial_response = service.process(base_state, base_context)
    assert initial_response.status == "data"
    north_usa_before = next(
        row for row in (initial_response.data or [])
        if row.get("_path") == "North|||USA"
    )
    assert north_usa_before["sales_sum"] == 100

    update_response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={"North": True, "South": True},
            show_col_totals=False,
            cell_updates=[
                {"rowId": "North|||USA", "colId": "sales_sum", "value": 999},
                {"rowId": "South|||Brazil", "colId": "sales_sum", "value": 321},
            ],
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.cellUpdates",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 9,
                "state_epoch": 1,
                "abort_generation": 2,
                "session_id": "sess-update-batch",
                "client_instance": "grid-update-batch",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert update_response.status == "data"
    rows_by_path = {
        row.get("_path"): row
        for row in (update_response.data or [])
        if isinstance(row, dict) and row.get("_path")
    }
    assert rows_by_path["North|||USA"]["sales_sum"] == 999
    assert rows_by_path["South|||Brazil"]["sales_sum"] == 321
    assert rows_by_path["North"]["sales_sum"] == 1079
    assert rows_by_path["South"]["sales_sum"] == 391


def test_runtime_service_row_transaction_supports_add_remove_update_and_upsert():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={"North": True, "South": True, "West": True},
            show_col_totals=False,
            transaction_request={
                "keyFields": ["region", "country"],
                "refreshMode": "smart",
                "add": [
                    {"region": "West", "country": "Mexico", "sales": 50, "cost": 5},
                ],
                "remove": [
                    {"keys": {"region": "South", "country": "Chile"}},
                ],
                "update": [
                    {"keys": {"region": "South", "country": "Brazil"}, "values": {"sales": 321}},
                ],
                "upsert": [
                    {"keys": {"region": "North", "country": "Canada"}, "rowData": {"sales": 180}},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 30,
                "window_seq": 10,
                "state_epoch": 1,
                "abort_generation": 3,
                "session_id": "sess-transaction",
                "client_instance": "grid-transaction",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert response.status == "data"
    assert response.transaction_result is not None
    assert response.transaction_result["requested"] == {"add": 1, "remove": 1, "update": 1, "upsert": 1}
    assert response.transaction_result["applied"]["add"] == 1
    assert response.transaction_result["applied"]["remove"] == 1
    assert response.transaction_result["applied"]["update"] == 1
    assert response.transaction_result["applied"]["upsertUpdated"] == 1
    assert response.transaction_result["refreshMode"] == "structural"
    assert response.transaction_result["history"]["captureable"] is True
    assert response.transaction_result["inverseTransaction"]["refreshMode"] == "structural"
    assert response.transaction_result["inverseTransaction"]["remove"] == [
        {"keys": {"region": "West", "country": "Mexico"}}
    ]
    assert {"keys": {"region": "South", "country": "Brazil"}, "values": {"sales": 120}} in response.transaction_result["inverseTransaction"]["update"]
    assert {"keys": {"region": "North", "country": "Canada"}, "values": {"sales": 80}} in response.transaction_result["inverseTransaction"]["update"]
    assert {"region": "South", "country": "Chile", "sales": 70, "cost": 7} in response.transaction_result["inverseTransaction"]["add"]
    assert response.transaction_result["redoTransaction"]["refreshMode"] == "structural"
    assert {"region": "West", "country": "Mexico", "sales": 50, "cost": 5} in response.transaction_result["redoTransaction"]["add"]
    assert {"keys": {"region": "South", "country": "Chile"}} in response.transaction_result["redoTransaction"]["remove"]
    assert {"keys": {"region": "South", "country": "Brazil"}, "values": {"sales": 321}} in response.transaction_result["redoTransaction"]["update"]
    assert {"keys": {"region": "North", "country": "Canada"}, "rowData": {"region": "North", "country": "Canada", "sales": 180}} in response.transaction_result["redoTransaction"]["upsert"]
    rows_by_path = {
        row.get("_path"): row
        for row in (response.data or [])
        if isinstance(row, dict) and row.get("_path")
    }
    assert rows_by_path["North|||Canada"]["sales_sum"] == 180
    assert rows_by_path["South|||Brazil"]["sales_sum"] == 321
    assert rows_by_path["West|||Mexico"]["sales_sum"] == 50
    assert "South|||Chile" not in rows_by_path


def test_runtime_service_aggregate_sum_edit_propagates_across_duplicate_source_rows():
    adapter = _make_duplicate_group_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={"North": True, "South": True},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "viewport",
                "update": [
                    {"rowId": "North|||USA", "colId": "sales_sum", "value": 180, "oldValue": 120},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 12,
                "state_epoch": 1,
                "abort_generation": 5,
                "session_id": "sess-aggregate-sum",
                "client_instance": "grid-aggregate-sum",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert response.status == "data"
    assert response.transaction_result is not None
    propagation = response.transaction_result["propagation"][0]
    assert propagation["aggregationFn"] == "sum"
    assert propagation["strategy"] == "equal_delta"
    assert propagation["execution"] == "set_based_sql"
    assert propagation["updatedRowCount"] == 2
    assert response.transaction_result["applied"]["update"] == 2
    inverse_update = response.transaction_result["inverseTransaction"]["update"][0]
    assert inverse_update["rowId"] == "North|||USA"
    assert inverse_update["colId"] == "sales_sum"
    assert inverse_update["value"] == pytest.approx(120)
    assert inverse_update["oldValue"] == pytest.approx(180)
    rows_by_path = {
        row.get("_path"): row
        for row in (response.data or [])
        if isinstance(row, dict) and row.get("_path")
    }
    assert rows_by_path["North|||USA"]["sales_sum"] == pytest.approx(180)
    assert rows_by_path["North"]["sales_sum"] == pytest.approx(260)
    persisted_rows = adapter.controller.planner.con.table("sales_data").execute().to_dict("records")
    north_usa_sales = sorted(
        row["sales"]
        for row in persisted_rows
        if row["region"] == "North" and row["country"] == "USA"
    )
    assert north_usa_sales == pytest.approx([50.0, 130.0])

    undo_response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={"North": True, "South": True},
            show_col_totals=False,
            transaction_request=response.transaction_result["inverseTransaction"],
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 12_1,
                "state_epoch": 1,
                "abort_generation": 5,
                "session_id": "sess-aggregate-sum-undo",
                "client_instance": "grid-aggregate-sum",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )
    assert undo_response.status == "data"
    persisted_rows = adapter.controller.planner.con.table("sales_data").execute().to_dict("records")
    north_usa_sales = sorted(
        row["sales"]
        for row in persisted_rows
        if row["region"] == "North" and row["country"] == "USA"
    )
    assert north_usa_sales == pytest.approx([20.0, 100.0])


def test_runtime_service_pivoted_aggregate_edit_resolves_column_scope():
    adapter = _make_duplicate_group_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region"],
            col_fields=["country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "viewport",
                "update": [
                    {"rowId": "North", "colId": "USA_sales_sum", "value": 180, "oldValue": 120},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 13,
                "state_epoch": 1,
                "abort_generation": 6,
                "session_id": "sess-pivoted-aggregate",
                "client_instance": "grid-pivoted-aggregate",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert response.status == "data"
    north_row = next(
        row for row in (response.data or [])
        if isinstance(row, dict) and row.get("_path") == "North"
    )
    assert north_row["USA_sales_sum"] == pytest.approx(180)
    assert north_row["Canada_sales_sum"] == pytest.approx(80)
    persisted_rows = adapter.controller.planner.con.table("sales_data").execute().to_dict("records")
    north_canada_sales = [
        row["sales"]
        for row in persisted_rows
        if row["region"] == "North" and row["country"] == "Canada"
    ]
    assert north_canada_sales == pytest.approx([80.0])


def test_runtime_service_integer_sum_aggregate_edit_uses_balanced_distribution_and_reversible_history():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "viewport",
                "update": [
                    {"rowId": "North", "colId": "sales_sum", "value": 181, "oldValue": 180},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 13_1,
                "state_epoch": 1,
                "abort_generation": 6,
                "session_id": "sess-int-aggregate",
                "client_instance": "grid-int-aggregate",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert response.status == "data"
    assert response.transaction_result is not None
    propagation = response.transaction_result["propagation"][0]
    assert propagation["aggregationFn"] == "sum"
    assert propagation["strategy"] == "balanced_delta"
    assert propagation["execution"] == "set_based_sql"
    assert propagation["updatedRowCount"] == 1

    persisted_rows = adapter.controller.planner.con.table("sales_data").execute().to_dict("records")
    north_sales = sorted(
        row["sales"]
        for row in persisted_rows
        if row["region"] == "North"
    )
    assert north_sales == [80, 101]

    inverse_update = response.transaction_result["inverseTransaction"]["update"][0]
    assert inverse_update == {
        "rowId": "North",
        "colId": "sales_sum",
        "value": 180.0,
        "oldValue": 181.0,
    }

    undo_response = service.process(
        PivotViewState(
            row_fields=["region"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={},
            show_col_totals=False,
            transaction_request=response.transaction_result["inverseTransaction"],
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 13_2,
                "state_epoch": 1,
                "abort_generation": 7,
                "session_id": "sess-int-aggregate-undo",
                "client_instance": "grid-int-aggregate",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert undo_response.status == "data"
    persisted_rows = adapter.controller.planner.con.table("sales_data").execute().to_dict("records")
    north_sales = sorted(
        row["sales"]
        for row in persisted_rows
        if row["region"] == "North"
    )
    assert north_sales == [80, 100]


def test_runtime_service_patch_refresh_returns_visible_row_patch_instead_of_full_window():
    adapter = _make_duplicate_group_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={"North": True, "South": True},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "patch",
                "visibleRowPaths": ["North", "North|||USA", "__grand_total__"],
                "visibleCenterColumnIds": ["sales_sum"],
                "update": [
                    {"rowId": "North|||USA", "colId": "sales_sum", "value": 180, "oldValue": 120},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 13_3,
                "state_epoch": 1,
                "abort_generation": 8,
                "session_id": "sess-patch-refresh",
                "client_instance": "grid-patch-refresh",
                "intent": "viewport",
                "needs_col_schema": False,
            },
        ),
    )

    assert response.status == "patched"
    assert response.transaction_result is not None
    assert response.transaction_result["refreshMode"] == "patch"
    assert response.patch_payload is not None
    patch_rows_by_path = {
        row.get("_path"): row
        for row in (response.patch_payload.get("rows") or [])
        if isinstance(row, dict) and row.get("_path")
    }
    assert patch_rows_by_path["North|||USA"]["sales_sum"] == pytest.approx(180)
    assert patch_rows_by_path["North"]["sales_sum"] == pytest.approx(260)
    assert patch_rows_by_path["__grand_total__"]["sales_sum"] == pytest.approx(380)


def test_runtime_service_patch_refresh_covers_visible_descendants_for_parent_edit():
    adapter = _make_duplicate_group_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={"East": True, "North": True, "South": True, "West": True},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "patch",
                "visibleRowPaths": [
                    "North",
                    "North|||Canada",
                    "North|||USA",
                    "South",
                    "South|||Brazil",
                    "__grand_total__",
                ],
                "visibleCenterColumnIds": ["sales_sum"],
                "update": [
                    {"rowId": "North", "rowPath": "North", "colId": "sales_sum", "value": 300, "oldValue": 200},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 13_4,
                "state_epoch": 1,
                "abort_generation": 9,
                "session_id": "sess-patch-branch",
                "client_instance": "grid-patch-branch",
                "intent": "viewport",
                "needs_col_schema": False,
            },
        ),
    )

    assert response.status == "patched"
    assert response.patch_payload is not None
    patch_rows_by_path = {
        row.get("_path"): row
        for row in (response.patch_payload.get("rows") or [])
        if isinstance(row, dict) and row.get("_path")
    }
    assert response.transaction_result["deferredViewportRefresh"] is False
    assert response.patch_payload["deferredViewportRefresh"] is False
    assert list(patch_rows_by_path) == ["North", "North|||Canada", "North|||USA", "__grand_total__"]
    assert patch_rows_by_path["North"]["sales_sum"] == pytest.approx(300)
    assert patch_rows_by_path["North|||Canada"]["sales_sum"] == pytest.approx(113.33333333333334)
    assert patch_rows_by_path["North|||USA"]["sales_sum"] == pytest.approx(186.66666666666669)
    assert patch_rows_by_path["__grand_total__"]["sales_sum"] == pytest.approx(420)


def test_runtime_service_parent_aggregate_edit_supports_proportional_formula():
    adapter = _make_duplicate_group_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            expanded={"North": True, "South": True},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "patch",
                "visibleRowPaths": ["North", "North|||Canada", "North|||USA", "__grand_total__"],
                "visibleCenterColumnIds": ["sales_sum"],
                "update": [
                    {
                        "rowId": "North",
                        "rowPath": "North",
                        "colId": "sales_sum",
                        "value": 260,
                        "oldValue": 200,
                        "propagationStrategy": "proportional",
                    },
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 13_5,
                "state_epoch": 1,
                "abort_generation": 10,
                "session_id": "sess-patch-proportional",
                "client_instance": "grid-patch-proportional",
                "intent": "viewport",
                "needs_col_schema": False,
            },
        ),
    )

    assert response.status == "patched"
    assert response.transaction_result is not None
    assert response.transaction_result["deferredViewportRefresh"] is False
    propagation = response.transaction_result["propagation"][0]
    assert propagation["strategy"] == "proportional"
    patch_rows_by_path = {
        row.get("_path"): row
        for row in (response.patch_payload.get("rows") or [])
        if isinstance(row, dict) and row.get("_path")
    }
    assert patch_rows_by_path["North"]["sales_sum"] == pytest.approx(260)
    assert patch_rows_by_path["North|||USA"]["sales_sum"] == pytest.approx(156)
    assert patch_rows_by_path["North|||Canada"]["sales_sum"] == pytest.approx(104)
    assert patch_rows_by_path["__grand_total__"]["sales_sum"] == pytest.approx(380)


def test_runtime_service_weighted_average_edit_shifts_source_rows_consistently():
    adapter = _make_weighted_average_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region"],
            val_configs=[{"field": "rate", "agg": "weighted_avg", "weightField": "notional"}],
            expanded={},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "viewport",
                "update": [
                    {"rowId": "North", "colId": "rate_weighted_avg", "value": 0.2, "oldValue": 0.175},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="rate_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 14,
                "state_epoch": 1,
                "abort_generation": 7,
                "session_id": "sess-weighted-aggregate",
                "client_instance": "grid-weighted-aggregate",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert response.status == "data"
    assert response.transaction_result["propagation"][0]["aggregationFn"] == "weighted_avg"
    assert response.transaction_result["propagation"][0]["strategy"] == "uniform_shift"
    persisted_rows = adapter.controller.planner.con.table("rate_data").execute().to_dict("records")
    north_rates = sorted(row["rate"] for row in persisted_rows if row["region"] == "North")
    assert north_rates == pytest.approx([0.125, 0.225])


def test_runtime_service_count_aggregate_edit_is_rejected():
    adapter = _make_duplicate_group_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "count"}],
            expanded={"North": True},
            show_col_totals=False,
            transaction_request={
                "refreshMode": "viewport",
                "update": [
                    {"rowId": "North|||USA", "colId": "sales_count", "value": 9, "oldValue": 2},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 15,
                "state_epoch": 1,
                "abort_generation": 8,
                "session_id": "sess-count-aggregate",
                "client_instance": "grid-count-aggregate",
                "intent": "viewport",
                "needs_col_schema": True,
            },
        ),
    )

    assert response.status == "data"
    assert response.transaction_result["applied"]["update"] == 0
    assert "Count-based aggregates cannot be edited" in response.transaction_result["warnings"][0]
    persisted_rows = adapter.controller.planner.con.table("sales_data").execute().to_dict("records")
    north_usa_sales = sorted(
        row["sales"]
        for row in persisted_rows
        if row["region"] == "North" and row["country"] == "USA"
    )
    assert north_usa_sales == pytest.approx([20.0, 100.0])


def test_runtime_service_transaction_refresh_mode_none_returns_transaction_result_only():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            row_fields=["region", "country"],
            val_configs=[{"field": "sales", "agg": "sum"}],
            transaction_request={
                "keyFields": ["region", "country"],
                "refreshMode": "none",
                "update": [
                    {"keys": {"region": "North", "country": "USA"}, "values": {"sales": 444}},
                ],
            },
        ),
        PivotRequestContext.from_frontend(
            table="sales_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 11,
                "state_epoch": 1,
                "abort_generation": 4,
                "session_id": "sess-transaction-none",
                "client_instance": "grid-transaction-none",
                "intent": "viewport",
            },
        ),
    )

    assert response.status == "transaction_applied"
    assert response.transaction_result is not None
    assert response.transaction_result["applied"]["update"] == 1
    assert response.transaction_result["refreshMode"] == "none"
    assert response.transaction_result["history"]["captureable"] is True
    assert response.transaction_result["inverseTransaction"]["refreshMode"] == "smart"
    assert response.transaction_result["inverseTransaction"]["update"] == [
        {"keys": {"region": "North", "country": "USA"}, "values": {"sales": 100}}
    ]
    assert response.transaction_result["redoTransaction"]["update"] == [
        {"keys": {"region": "North", "country": "USA"}, "values": {"sales": 444}}
    ]
    persisted_rows = adapter.controller.planner.con.table("sales_data").execute().to_dict("records")
    north_usa = next(row for row in persisted_rows if row["region"] == "North" and row["country"] == "USA")
    assert north_usa["sales"] == 444


def test_initial_structural_load_includes_col_schema_payload_without_sentinel():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.rowFields",
        viewport=None,
    )
    state = PivotViewState(
        row_fields=["region", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        filters={},
        sorting=[],
        expanded={},
        show_row_totals=False,
        show_col_totals=True,
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert isinstance(response.columns, list)
    assert isinstance(response.col_schema, dict)
    assert response.col_schema.get("columns")
    assert not any(
        isinstance(col, dict) and col.get("id") == "__col_schema"
        for col in response.columns
    )


def test_runtime_service_isolates_instances_by_client_instance():
    adapter = _make_adapter()
    gate = SessionRequestGate()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=gate)

    shared_viewport = {
        "start": 0,
        "end": 10,
        "window_seq": 5,
        "state_epoch": 2,
        "abort_generation": 2,
        "session_id": "sess-shared",
        "intent": "viewport",
    }

    context_a = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="custom.viewport",
        viewport={**shared_viewport, "client_instance": "grid-a"},
    )
    context_b = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="custom.viewport",
        viewport={**shared_viewport, "client_instance": "grid-b"},
    )

    state = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
    )

    response_a = service.process(state, context_a)
    response_b = service.process(state, context_b)

    assert response_a.status == "data"
    assert response_b.status == "data"


def test_tree_runtime_service_adjacency_shows_roots_until_expanded():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "tree_data",
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "parent_id": [None, 1, 1, 2],
                "name": ["Root", "Child A", "Child B", "Grandchild"],
                "sales": [100, 40, 60, 10],
            }
        ),
    )
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="tree_data",
        trigger_prop="pivot-grid.runtimeRequest",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-tree",
            "client_instance": "grid-tree",
            "intent": "viewport",
        },
    )

    state = PivotViewState(
        view_mode="tree",
        tree_config={
            "sourceType": "adjacency",
            "idField": "id",
            "parentIdField": "parent_id",
            "labelField": "name",
            "valueFields": ["sales"],
        },
        detail_config={"enabled": True, "defaultKind": "records"},
    )

    collapsed_response = service.process(state, context)
    assert collapsed_response.status == "data"
    assert collapsed_response.total_rows == 1
    assert collapsed_response.data[0]["_id"] == "Root"
    assert collapsed_response.data[0]["_path"] == "1"
    assert collapsed_response.data[0]["_has_children"] is True

    expanded_response = service.process(
        PivotViewState(
            view_mode="tree",
            tree_config=state.tree_config,
            detail_config=state.detail_config,
            expanded={"1": True, "1|||2": True},
        ),
        PivotRequestContext.from_frontend(
            table="tree_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 2,
                "state_epoch": 1,
                "abort_generation": 1,
                "session_id": "sess-tree",
                "client_instance": "grid-tree",
                "intent": "viewport",
            },
        ),
    )

    assert expanded_response.status == "data"
    assert expanded_response.total_rows == 4
    labels = [row["_id"] for row in expanded_response.data]
    assert labels == ["Root", "Child A", "Grandchild", "Child B"]
    depths = {row["_id"]: row["_depth"] for row in expanded_response.data}
    assert depths["Root"] == 0
    assert depths["Child A"] == 1
    assert depths["Grandchild"] == 2


def test_tree_runtime_service_adjacency_data_path_avoids_full_row_materialization(monkeypatch):
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "tree_data",
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "parent_id": [None, 1, 1, 2],
                "name": ["Root", "Child A", "Child B", "Grandchild"],
                "sales": [100, 40, 60, 10],
            }
        ),
    )
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    def fail_full_load(*_args, **_kwargs):
        raise AssertionError("tree data request should not materialize the full filtered row set")

    monkeypatch.setattr(service._tree_service, "_load_source_rows", fail_full_load)

    response = service.process(
        PivotViewState(
            view_mode="tree",
            tree_config={
                "sourceType": "adjacency",
                "idField": "id",
                "parentIdField": "parent_id",
                "labelField": "name",
                "valueFields": ["sales"],
            },
            detail_config={"enabled": True, "defaultKind": "records"},
            expanded={"1": True},
        ),
        PivotRequestContext.from_frontend(
            table="tree_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 3,
                "state_epoch": 1,
                "abort_generation": 1,
                "session_id": "sess-tree-adj-fast",
                "client_instance": "grid-tree-adj-fast",
                "intent": "viewport",
            },
        ),
    )

    assert response.status == "data"
    assert response.total_rows == 3
    assert [row["_id"] for row in response.data] == ["Root", "Child A", "Child B"]


def test_tree_runtime_service_path_data_path_avoids_full_row_materialization(monkeypatch):
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "tree_data",
        pa.Table.from_pydict(
            {
                "path": [
                    "Root",
                    "Root|||Child A",
                    "Root|||Child B",
                    "Root|||Child A|||Grandchild",
                ],
                "name": ["Root", "Child A", "Child B", "Grandchild"],
                "sales": [100, 40, 60, 10],
            }
        ),
    )
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    def fail_full_load(*_args, **_kwargs):
        raise AssertionError("tree path request should not materialize the full filtered row set")

    monkeypatch.setattr(service._tree_service, "_load_source_rows", fail_full_load)

    response = service.process(
        PivotViewState(
            view_mode="tree",
            tree_config={
                "sourceType": "path",
                "pathField": "path",
                "labelField": "name",
                "valueFields": ["sales"],
            },
            detail_config={"enabled": True, "defaultKind": "records"},
            expanded={"Root": True, "Root|||Child A": True},
        ),
        PivotRequestContext.from_frontend(
            table="tree_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 4,
                "state_epoch": 1,
                "abort_generation": 1,
                "session_id": "sess-tree-path-fast",
                "client_instance": "grid-tree-path-fast",
                "intent": "viewport",
            },
        ),
    )

    assert response.status == "data"
    assert response.total_rows == 4
    assert [row["_id"] for row in response.data] == ["Root", "Child A", "Grandchild", "Child B"]


def test_tree_runtime_service_parent_id_mode_alias_and_group_default_expanded():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "tree_data",
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "parent_id": [None, 1, 1, 2],
                "name": ["Root", "Child A", "Child B", "Grandchild"],
                "sales": [100, 40, 60, 10],
            }
        ),
    )
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            view_mode="tree",
            tree_config={
                "mode": "parentId",
                "idField": "id",
                "parentIdField": "parent_id",
                "labelField": "name",
                "valueFields": ["sales"],
                "groupDefaultExpanded": 1,
            },
            detail_config={"enabled": True, "defaultKind": "records"},
        ),
        PivotRequestContext.from_frontend(
            table="tree_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 5,
                "state_epoch": 1,
                "abort_generation": 1,
                "session_id": "sess-tree-parent-id",
                "client_instance": "grid-tree-parent-id",
                "intent": "viewport",
            },
        ),
    )

    assert response.status == "data"
    assert response.total_rows == 3
    assert [row["_id"] for row in response.data] == ["Root", "Child A", "Child B"]
    assert response.data[0]["_is_expanded"] is True


def test_tree_runtime_service_explicit_expanded_state_overrides_group_default_expanded():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "tree_data",
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "parent_id": [None, 1, 1, 2],
                "name": ["Root", "Child A", "Child B", "Grandchild"],
                "sales": [100, 40, 60, 10],
            }
        ),
    )
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            view_mode="tree",
            tree_config={
                "mode": "parentId",
                "idField": "id",
                "parentIdField": "parent_id",
                "labelField": "name",
                "valueFields": ["sales"],
                "groupDefaultExpanded": 1,
            },
            detail_config={"enabled": True, "defaultKind": "records"},
            expanded={"1": False},
        ),
        PivotRequestContext.from_frontend(
            table="tree_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 6,
                "state_epoch": 1,
                "abort_generation": 1,
                "session_id": "sess-tree-parent-id-override",
                "client_instance": "grid-tree-parent-id-override",
                "intent": "viewport",
            },
        ),
    )

    assert response.status == "data"
    assert response.total_rows == 1
    assert [row["_id"] for row in response.data] == ["Root"]
    assert response.data[0]["_is_expanded"] is False


def test_tree_runtime_service_open_by_default_paths_expand_specific_branch():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "tree_data",
        pa.Table.from_pydict(
            {
                "path": [
                    "Root",
                    "Root|||Child A",
                    "Root|||Child B",
                    "Root|||Child A|||Grandchild",
                ],
                "name": ["Root", "Child A", "Child B", "Grandchild"],
                "sales": [100, 40, 60, 10],
            }
        ),
    )
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            view_mode="tree",
            tree_config={
                "sourceType": "path",
                "pathField": "path",
                "labelField": "name",
                "valueFields": ["sales"],
                "openByDefault": [["Root"], ["Root", "Child A"]],
            },
            detail_config={"enabled": True, "defaultKind": "records"},
        ),
        PivotRequestContext.from_frontend(
            table="tree_data",
            trigger_prop="pivot-grid.runtimeRequest",
            viewport={
                "start": 0,
                "end": 20,
                "window_seq": 7,
                "state_epoch": 1,
                "abort_generation": 1,
                "session_id": "sess-tree-open-by-default",
                "client_instance": "grid-tree-open-by-default",
                "intent": "viewport",
            },
        ),
    )

    assert response.status == "data"
    assert response.total_rows == 4
    assert [row["_id"] for row in response.data] == ["Root", "Child A", "Grandchild", "Child B"]


def test_tree_detail_request_returns_records_payload():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "tree_data",
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "parent_id": [None, 1, 1, 2],
                "name": ["Root", "Child A", "Child B", "Grandchild"],
                "sales": [100, 40, 60, 10],
            }
        ),
    )
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    response = service.process(
        PivotViewState(
            view_mode="tree",
            tree_config={
                "sourceType": "adjacency",
                "idField": "id",
                "parentIdField": "parent_id",
                "labelField": "name",
                "valueFields": ["sales"],
            },
            detail_config={"enabled": True, "defaultKind": "records"},
            detail_request={
                "rowPath": "1",
                "rowKey": "1",
                "page": 0,
                "pageSize": 10,
                "detailKind": "records",
            },
        ),
        PivotRequestContext.from_frontend(
            table="tree_data",
            trigger_prop="pivot-grid.detailRequest",
            viewport={
                "window_seq": 3,
                "state_epoch": 1,
                "abort_generation": 1,
                "session_id": "sess-tree",
                "client_instance": "grid-tree",
                "intent": "structural",
            },
        ),
    )

    assert response.status == "detail_data"
    assert response.detail_payload is not None
    assert response.detail_payload["rowPath"] == "1"
    assert response.detail_payload["detailKind"] == "records"
    assert response.detail_payload["totalRows"] == 4
    assert [row["name"] for row in response.detail_payload["rows"]] == [
        "Root",
        "Child A",
        "Child B",
        "Grandchild",
    ]


def test_collapsed_hierarchy_total_rows_counts_only_visible_root_level():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "cost": [1, 1, 1, 1, 2, 2, 2, 2],
            "region": ["North", "North", "South", "South", "North", "North", "South", "South"],
            "country": ["USA", "Canada", "USA", "Canada", "USA", "Canada", "USA", "Canada"],
            "date": ["2023-01-01", "2023-01-02", "2023-01-01", "2023-01-02"] * 2,
            "sales": [10, 20, 30, 40, 50, 60, 70, 80],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 50,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-collapsed",
            "client_instance": "grid-collapsed",
            "intent": "viewport",
            "include_grand_total": True,
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["cost", "region", "country"],
        col_fields=["date"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        filters={},
        sorting=[],
        expanded={},
        show_row_totals=False,
        show_col_totals=True,
    )

    response = service.process(state, context)

    # Collapsed tree should contain only root-level groups + grand total.
    assert response.status == "data"
    assert response.total_rows == 3
    assert isinstance(response.data, list)
    assert len(response.data) == 3


def test_runtime_service_ignores_stale_expanded_paths_across_row_field_type_changes():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "sales": [998, 998, 1],
            "region": ["West", "South", "North"],
            "country": ["USA", "Canada", "Brazil"],
            "cost": [5, 6, 7],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.expanded",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 7,
            "state_epoch": 3,
            "abort_generation": 3,
            "session_id": "sess-stale-expanded",
            "client_instance": "grid-stale-expanded",
            "intent": "expansion",
            "include_grand_total": True,
        },
    )
    state = PivotViewState(
        row_fields=["sales", "region"],
        val_configs=[{"field": "cost", "agg": "sum"}],
        expanded={
            "998": True,
            "West": True,
            "South": True,
        },
        show_row_totals=False,
        show_col_totals=True,
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert response.total_rows == 5
    child_rows = [
        row for row in (response.data or [])
        if isinstance(row, dict) and row.get("depth") == 1 and row.get("sales") == 998
    ]
    assert len(child_rows) == 2
    assert {row.get("region") for row in child_rows} == {"West", "South"}


def test_runtime_service_ignores_stale_deeper_expanded_paths_after_row_field_change():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "sales": [998, 998, 1],
            "region": ["West", "South", "North"],
            "country": ["USA", "Canada", "Brazil"],
            "cost": [5, 6, 7],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.expanded",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 8,
            "state_epoch": 3,
            "abort_generation": 3,
            "session_id": "sess-stale-expanded-deep",
            "client_instance": "grid-stale-expanded-deep",
            "intent": "expansion",
            "include_grand_total": True,
        },
    )
    state = PivotViewState(
        row_fields=["sales", "region", "country"],
        val_configs=[{"field": "cost", "agg": "sum"}],
        expanded={
            "998": True,
            "998|||West": True,
            "South|||Canada": True,
        },
        show_row_totals=False,
        show_col_totals=True,
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert response.total_rows == 6
    visible_paths = [row.get("_path") for row in (response.data or []) if isinstance(row, dict)]
    assert "998|||West|||USA" in visible_paths
    assert "998|||South|||Canada" not in visible_paths


def test_runtime_service_preserves_sorting_when_stale_expanded_paths_are_dropped():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "sales": [998, 998, 1],
            "region": ["West", "South", "North"],
            "country": ["USA", "Canada", "Brazil"],
            "cost": [5, 6, 7],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.expanded",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 9,
            "state_epoch": 3,
            "abort_generation": 3,
            "session_id": "sess-stale-expanded-sort",
            "client_instance": "grid-stale-expanded-sort",
            "intent": "expansion",
            "include_grand_total": True,
        },
    )
    state = PivotViewState(
        row_fields=["sales", "region"],
        val_configs=[{"field": "cost", "agg": "sum"}],
        sorting=[{"id": "cost_sum", "desc": True}],
        expanded={
            "998": True,
            "West": True,
            "South": True,
        },
        show_row_totals=False,
        show_col_totals=True,
    )

    response = service.process(state, context)

    assert response.status == "data"
    top_level_rows = [
        row for row in (response.data or [])
        if isinstance(row, dict) and row.get("depth") == 0 and not row.get("_isTotal")
    ]
    assert [row.get("sales") for row in top_level_rows] == [998, 1]


def test_curve_pillar_tenor_sort_uses_hidden_sort_key_and_keeps_display_field():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "Curve Pillar": ["1M", "2W", "1D", "1M", "2W", "1D"],
            "__sortkey__Curve Pillar": [30, 14, 1, 30, 14, 1],
            "sales": [3, 2, 1, 6, 5, 4],
        }
    )
    adapter.controller.load_data_from_arrow("curve_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="curve_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-curve-sort",
            "client_instance": "grid-curve-sort",
            "intent": "viewport",
            "include_grand_total": False,
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["Curve Pillar"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        sorting=[{"id": "Curve Pillar", "desc": False}],
        sort_options={
            "columnOptions": {
                "Curve Pillar": {
                    "sortType": "curve_pillar_tenor",
                    "sortKeyField": "__sortkey__Curve Pillar",
                }
            }
        },
        show_row_totals=False,
        show_col_totals=False,
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert isinstance(response.data, list)

    ordered_labels = [
        row.get("Curve Pillar")
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]
    assert ordered_labels == ["1D", "2W", "1M"]
    assert all(
        "__sortkey__Curve Pillar" not in row
        for row in response.data
        if isinstance(row, dict)
    )


def test_chart_request_returns_chart_data_payload():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.chartRequest",
        viewport={
            "start": 0,
            "end": 10,
            "col_start": 0,
            "col_end": 0,
            "window_seq": 3,
            "state_epoch": 2,
            "abort_generation": 2,
            "session_id": "sess-chart",
            "client_instance": "grid-chart",
            "intent": "chart",
            "include_grand_total": True,
            "needs_col_schema": False,
        },
    )
    state = PivotViewState(
        row_fields=["region", "country"],
        col_fields=[],
        val_configs=[{"field": "sales", "agg": "sum"}],
        filters={},
        sorting=[],
        expanded={},
        show_row_totals=False,
        show_col_totals=True,
        chart_request={
            "needs_col_schema": False,
            "pane_id": "chart-pane-1",
            "request_signature": "sig-1",
        },
    )

    response = service.process(state, context)

    assert response.status == "chart_data"
    assert isinstance(response.chart_data, dict)
    assert isinstance(response.chart_data.get("rows"), list)
    assert response.chart_data.get("stateEpoch") == 2
    assert response.chart_data.get("paneId") == "chart-pane-1"
    assert response.chart_data.get("requestSignature") == "sig-1"


def test_report_mode_sorts_and_trims_visible_siblings_by_metric():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "South", "South", "East", "East"],
            "country": ["USA", "Canada", "Brazil", "Chile", "Japan", "China"],
            "sales": [100, 80, 120, 70, 90, 60],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-report-sort",
            "client_instance": "grid-report-sort",
            "intent": "viewport",
            "include_grand_total": False,
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["region", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        expanded=True,
        show_row_totals=False,
        show_col_totals=False,
        view_mode="report",
        report_def={
            "levels": [
                {
                    "field": "region",
                    "label": "Region",
                    "topN": 2,
                    "sortBy": "sales_sum",
                    "sortDir": "desc",
                },
                {
                    "field": "country",
                    "label": "Country",
                    "topN": 1,
                    "sortBy": "sales_sum",
                    "sortDir": "asc",
                },
            ]
        },
    )

    response = service.process(state, context)

    assert response.status == "data"
    visible_rows = [
        (row.get("depth"), row.get("_id"))
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]
    assert visible_rows == [
        (0, "South"),
        (1, "Chile"),
        (0, "North"),
        (1, "Canada"),
    ]
    assert response.total_rows == 4
    assert response.data[0].get("_levelLabel") == "Region"
    assert response.data[1].get("_levelLabel") == "Country"
    assert response.data[1].get("_groupTotalCount") == 2
    assert "__reportSortBy" not in response.data[0]
    assert "__reportSortDir" not in response.data[0]


def test_report_mode_conditional_children_override_child_rules():
    adapter = _make_adapter()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-report-conditional",
            "client_instance": "grid-report-conditional",
            "intent": "viewport",
            "include_grand_total": False,
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["region", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        expanded=True,
        show_row_totals=False,
        show_col_totals=False,
        view_mode="report",
        report_def={
            "levels": [
                {
                    "field": "region",
                    "label": "Region",
                    "conditionalChildren": {
                        "South": {
                            "field": "country",
                            "label": "South Country",
                            "topN": 1,
                            "sortBy": "sales_sum",
                            "sortDir": "desc",
                        },
                        "*": {
                            "field": "country",
                            "label": "Country",
                            "topN": 1,
                            "sortBy": "sales_sum",
                            "sortDir": "asc",
                        },
                    },
                }
            ]
        },
    )

    response = service.process(state, context)

    assert response.status == "data"
    visible_rows = [
        (row.get("depth"), row.get("_id"), row.get("_levelLabel"))
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]
    assert visible_rows == [
        (0, "North", "Region"),
        (1, "Canada", "Country"),
        (0, "South", "Region"),
        (1, "Brazil", "South Country"),
    ]
    assert response.total_rows == 4


def test_branching_report_root_supports_per_value_child_fields():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "East", "East", "East"],
            "country": ["USA", "Canada", "France", "Germany", "France"],
            "product": ["A", "B", "C", "D", "C"],
            "sales": [100, 80, 90, 60, 30],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())

    context = PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.viewport",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": 1,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": "sess-branch-tree",
            "client_instance": "grid-branch-tree",
            "intent": "viewport",
            "include_grand_total": False,
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["region", "product", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        expanded={"North": True, "East": True},
        show_row_totals=False,
        show_col_totals=False,
        view_mode="report",
        report_def={
            "root": {
                "field": "region",
                "label": "Region",
                "sortBy": "sales_sum",
                "sortDir": "desc",
                "childrenByValue": {
                    "North": {
                        "field": "product",
                        "label": "North Product",
                        "sortBy": "sales_sum",
                        "sortDir": "desc",
                    },
                    "East": {
                        "field": "country",
                        "label": "East Country",
                        "sortBy": "sales_sum",
                        "sortDir": "desc",
                    },
                },
            }
        },
    )

    response = service.process(state, context)

    assert response.status == "data"
    assert [
        (row.get("depth"), row.get("_id"), row.get("_levelField"))
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ] == [
        (0, "East", "region"),
        (1, "France", "country"),
        (1, "Germany", "country"),
        (0, "North", "region"),
        (1, "A", "product"),
        (1, "B", "product"),
    ]
    assert response.data[1].get("_pathFields") == ["region", "country"]
    assert response.data[4].get("_pathFields") == ["region", "product"]
    assert response.total_rows == 6
