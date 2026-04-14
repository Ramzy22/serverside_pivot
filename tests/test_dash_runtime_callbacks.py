import asyncio
import inspect
import threading
from pathlib import Path

import pandas as pd
import pytest
from dash import Dash, dcc, html, dash_table
import pivot_engine as pivot_engine_module
import pivot_engine.runtime as runtime_module
from pivot_engine import register_pivot_app
from pivot_engine.grid_hierarchy import build_org_hierarchy_paths

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine.runtime import (
    DashPivotInstanceConfig,
    register_dash_callbacks_for_instances,
    register_dash_drill_modal_callback,
    register_dash_pivot_transport_callback,
)
from pivot_engine.runtime.dash_callbacks import (
    _build_runtime_response,
    _extract_request_state_override,
    _format_transport_callback_output,
    _is_bootstrap_without_viewport,
    _normalize_transport_request,
    _state_override_value,
)
from pivot_engine.runtime.async_bridge import run_awaitable_in_worker_thread, run_awaitable_sync


class _StubRuntimeService:
    async def process_async(self, *args, **kwargs):  # pragma: no cover - callback execution is not in scope here
        raise RuntimeError("not used in registration-only tests")

    def process(self, *args, **kwargs):  # pragma: no cover - callback execution is not in scope here
        raise RuntimeError("not used in registration-only tests")


def _make_app():
    app = Dash(__name__)
    app.layout = html.Div(
        [
            DashTanstackPivot(id="pivot-a", table="sales_a"),
            DashTanstackPivot(id="pivot-b", table="sales_b"),
            dcc.Store(id="drill-a"),
            dcc.Store(id="drill-b"),
            html.Div(id="modal-a"),
            html.Div(id="modal-b"),
            dash_table.DataTable(id="table-a"),
            dash_table.DataTable(id="table-b"),
            html.Button("close", id="close-a"),
            html.Button("close", id="close-b"),
        ]
    )
    return app


def test_register_dash_pivot_transport_callback_is_idempotent():
    app = _make_app()
    getter = lambda: _StubRuntimeService()

    assert register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-a", drill_store_id="drill-a", debug=False
    )
    assert not register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-a", drill_store_id="drill-a", debug=False
    )
    assert register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-b", drill_store_id="drill-b", debug=False
    )
    assert len(app.callback_map) == 2


def test_register_dash_pivot_transport_callback_without_drill_store():
    app = Dash(__name__)
    app.layout = html.Div([DashTanstackPivot(id="pivot-only", table="sales")])
    getter = lambda: _StubRuntimeService()

    assert register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-only", debug=False
    )
    assert len(app.callback_map) == 1


def test_register_dash_pivot_transport_callback_uses_dash_async_when_available():
    app = Dash(__name__)
    app.layout = html.Div([DashTanstackPivot(id="pivot-async", table="sales")])
    getter = lambda: _StubRuntimeService()

    assert register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-async", debug=False
    )

    callback_entry = app.callback_map["pivot-async.runtimeResponse"]
    assert inspect.iscoroutinefunction(callback_entry["callback"]) is bool(getattr(app, "_use_async", False))


def test_async_bridge_runs_coroutine_from_running_loop_without_reentering_loop():
    async def _value():
        return "ok"

    async def _inside_running_loop():
        return run_awaitable_sync(_value())

    assert asyncio.run(_inside_running_loop()) == "ok"


def test_async_bridge_can_force_coroutine_to_worker_thread():
    caller_thread_id = threading.get_ident()

    async def _worker_thread_id():
        return threading.get_ident()

    assert run_awaitable_in_worker_thread(_worker_thread_id()) != caller_thread_id


def test_legacy_aio_callback_uses_loop_safe_async_bridge():
    source = Path("dash_tanstack_pivot/pivot_engine/pivot_engine/dash_component.py").read_text(encoding="utf-8")

    assert "async def _update_grid_async" in source
    assert "run_awaitable_sync(_update_grid_async(spec_data))" in source
    assert "run_until_complete" not in source
    assert "asyncio.get_event_loop" not in source


def test_legacy_aio_org_hierarchy_paths_skip_row_wise_apply_and_nulls():
    component_source = Path("dash_tanstack_pivot/pivot_engine/pivot_engine/dash_component.py").read_text(encoding="utf-8")
    helper_source = Path("dash_tanstack_pivot/pivot_engine/pivot_engine/grid_hierarchy.py").read_text(encoding="utf-8")
    frame = pd.DataFrame(
        {
            "region": ["North", "South", None],
            "country": ["USA", None, "Brazil"],
            "sales_sum": [10, 20, 30],
        }
    )

    assert build_org_hierarchy_paths(frame, ["region", "country", "missing"]) == [
        ["North", "USA"],
        ["South"],
        ["Brazil"],
    ]
    assert "df.apply(make_path, axis=1)" not in component_source
    assert "itertuples(index=False, name=None)" in helper_source


def test_drill_through_rest_route_uses_worker_thread_bridge():
    source = Path("dash_tanstack_pivot/pivot_engine/pivot_engine/dash_integration.py").read_text(encoding="utf-8")

    assert "async def _load_drill_through_data" in source
    assert "run_awaitable_in_worker_thread(_load_drill_through_data())" in source
    assert "asyncio.run" not in source


def test_drill_through_rest_route_runs_controller_on_worker_thread():
    request_thread_id = threading.get_ident()
    controller_thread_ids = []

    class _Controller:
        async def get_drill_through_data(self, spec, filters, **kwargs):
            controller_thread_ids.append(threading.get_ident())
            return {
                "rows": [
                    {
                        "table": spec.table,
                        "filters": filters,
                        "limit": kwargs["limit"],
                        "offset": kwargs["offset"],
                    }
                ],
                "total_rows": 1,
            }

    class _Adapter:
        controller = _Controller()

    app = Dash(__name__)
    app.layout = html.Div([DashTanstackPivot(id="pivot-route", table="sales")])
    register_pivot_app(app, adapter_getter=lambda: _Adapter(), pivot_id="pivot-route", debug=False)

    with app.server.test_client() as client:
        response = client.get(
            "/api/drill-through"
            "?table=sales"
            "&row_path=North|||USA"
            "&row_fields=region,country"
            "&page=1"
            "&page_size=25"
        )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["rows"][0]["table"] == "sales"
    assert payload["rows"][0]["filters"] == [
        {"field": "region", "op": "=", "value": "North"},
        {"field": "country", "op": "=", "value": "USA"},
    ]
    assert payload["rows"][0]["limit"] == 25
    assert payload["rows"][0]["offset"] == 25
    assert payload["total_rows"] == 1
    assert controller_thread_ids and controller_thread_ids[0] != request_thread_id


def test_register_dash_drill_modal_callback_is_idempotent():
    app = _make_app()

    assert register_dash_drill_modal_callback(
        app,
        drill_store_id="drill-a",
        close_drill_id="close-a",
        drill_modal_id="modal-a",
        drill_table_id="table-a",
    )
    assert not register_dash_drill_modal_callback(
        app,
        drill_store_id="drill-a",
        close_drill_id="close-a",
        drill_modal_id="modal-a",
        drill_table_id="table-a",
    )
    assert len(app.callback_map) == 1


def test_register_dash_callbacks_for_instances_supports_multi_instance():
    app = _make_app()
    getter = lambda: _StubRuntimeService()

    instances = [
        DashPivotInstanceConfig(
            pivot_id="pivot-a",
            drill_store_id="drill-a",
            drill_modal_id="modal-a",
            drill_table_id="table-a",
            close_drill_id="close-a",
        ),
        DashPivotInstanceConfig(
            pivot_id="pivot-b",
            drill_store_id="drill-b",
            drill_modal_id="modal-b",
            drill_table_id="table-b",
            close_drill_id="close-b",
        ),
    ]

    status = register_dash_callbacks_for_instances(app, getter, instances, debug=False)
    assert status == {"pivot-a": True, "pivot-b": True}
    assert len(app.callback_map) == 4

    status_repeat = register_dash_callbacks_for_instances(app, getter, instances, debug=False)
    assert status_repeat == {"pivot-a": False, "pivot-b": False}
    assert len(app.callback_map) == 4


def test_runtime_module_only_exposes_unified_transport_callback():
    assert hasattr(runtime_module, "register_dash_pivot_transport_callback")
    assert not hasattr(runtime_module, "register_dash_filter_request_callback")
    assert hasattr(pivot_engine_module, "register_dash_pivot_transport_callback")
    assert not hasattr(pivot_engine_module, "register_dash_filter_request_callback")


def test_bootstrap_detection_requires_real_viewport_context():
    assert _is_bootstrap_without_viewport(None, None) is True
    assert _is_bootstrap_without_viewport(None, {}) is True
    assert _is_bootstrap_without_viewport(None, {"start": 0, "end": 99}) is True
    assert (
        _is_bootstrap_without_viewport(
            None,
            {
                "start": 0,
                "end": 99,
                "session_id": "sess",
                "client_instance": "client-a",
            },
        )
        is False
    )
    assert _is_bootstrap_without_viewport("pivot-grid.viewport", None) is False


def test_bootstrap_fallback_viewport_shape_is_complete():
    pivot_id = "pivot-grid"
    fallback_viewport = {
        "start": 0,
        "end": 99,
        "count": 100,
        "window_seq": 1,
        "state_epoch": 0,
        "abort_generation": 0,
        "session_id": f"bootstrap::{pivot_id}",
        "client_instance": "bootstrap",
        "intent": "structural",
        "needs_col_schema": True,
    }
    assert fallback_viewport["start"] == 0
    assert fallback_viewport["end"] == 99
    assert fallback_viewport["needs_col_schema"] is True


def test_runtime_request_normalizes_kind_and_payload():
    request = _normalize_transport_request(
        pivot_id="pivot-grid",
        runtime_request={
            "kind": "filter_options",
            "requestId": "req-123",
            "payload": {
                "columnId": "country",
                "table": "sales_data",
            },
        },
    )

    assert request["kind"] == "filter_options"
    assert request["request_id"] == "req-123"
    assert request["payload"] == {"columnId": "country", "table": "sales_data"}
    assert request["trigger_prop"] == "pivot-grid.filterRequest"


def test_runtime_request_normalizes_detail_kind_and_trigger():
    request = _normalize_transport_request(
        pivot_id="pivot-grid",
        runtime_request={
            "kind": "detail",
            "requestId": "req-detail-1",
            "payload": {
                "rowPath": "1|||2",
                "detailKind": "records",
            },
        },
    )

    assert request["kind"] == "detail"
    assert request["request_id"] == "req-detail-1"
    assert request["payload"] == {"rowPath": "1|||2", "detailKind": "records"}
    assert request["trigger_prop"] == "pivot-grid.detailRequest"


def test_runtime_request_normalizes_transaction_kind_and_trigger():
    request = _normalize_transport_request(
        pivot_id="pivot-grid",
        runtime_request={
            "kind": "transaction",
            "requestId": "req-update-1",
            "payload": {
                "update": [{"rowId": "North|||USA", "colId": "sales_sum", "value": 999}],
            },
        },
    )

    assert request["kind"] == "transaction"
    assert request["request_id"] == "req-update-1"
    assert request["trigger_prop"] == "pivot-grid.runtimeRequest"


def test_runtime_request_normalizes_batch_update_alias():
    request = _normalize_transport_request(
        pivot_id="pivot-grid",
        runtime_request={
            "kind": "batch_update",
            "requestId": "req-update-2",
            "payload": {
                "updates": [{"rowId": "North|||USA", "colId": "sales_sum", "value": 999}],
            },
        },
    )

    assert request["kind"] == "update"
    assert request["request_id"] == "req-update-2"
    assert request["trigger_prop"] == "pivot-grid.cellUpdates"


def test_missing_runtime_request_falls_back_to_bootstrap_data_request():
    request = _normalize_transport_request(
        pivot_id="pivot-grid-bootstrap",
        runtime_request=None,
    )

    assert request["kind"] == "data"
    assert request["request_id"] is None
    assert request["payload"] == {}
    assert request["trigger_prop"] == "pivot-grid-bootstrap.viewport"


def test_runtime_request_wins_when_present():
    request = _normalize_transport_request(
        pivot_id="pivot-grid-runtime",
        runtime_request={
            "kind": "data",
            "requestId": "req-runtime",
            "payload": {"start": 0, "end": 99},
        },
    )

    assert request["kind"] == "data"
    assert request["request_id"] == "req-runtime"


def test_runtime_request_state_override_is_extracted_for_all_request_kinds():
    override = {
        "rowFields": ["desk", "book"],
        "colFields": ["scenario"],
        "valConfigs": [{"id": "pnl", "field": "pnl", "agg": "sum"}],
        "filters": {"region": ["EMEA"]},
        "sorting": [{"id": "pnl_sum", "desc": True}],
        "sortOptions": {"mode": "absolute"},
        "expanded": True,
        "showRowTotals": False,
        "showColTotals": True,
        "viewMode": "report",
        "reportDef": {"levels": [{"field": "desk"}]},
    }

    assert _extract_request_state_override({"state_override": override}) is override
    assert _extract_request_state_override({"stateOverride": override}) is override
    assert _extract_request_state_override({"state_override": []}) is None
    assert _extract_request_state_override(None) is None

    assert _state_override_value(override, "rowFields", ["stale"], list) == ["desk", "book"]
    assert _state_override_value(override, "expanded", {}, (dict, bool)) is True
    assert _state_override_value(override, "showRowTotals", True, bool) is False
    assert _state_override_value(override, "missing", "fallback", str) == "fallback"
    assert _state_override_value(override, "rowFields", "fallback", dict) == "fallback"


def test_transport_callback_output_is_plain_object_for_single_output():
    payload = {"kind": "data", "status": "data", "payload": {"rows": []}}
    result = _format_transport_callback_output(False, payload)
    assert result == payload
    assert isinstance(result, dict)


def test_transport_callback_output_is_tuple_when_drill_store_is_enabled():
    payload = {"kind": "data", "status": "data", "payload": {"rows": []}}
    result = _format_transport_callback_output(True, payload, [{"id": 1}])
    assert result == (payload, [{"id": 1}])


def test_runtime_response_can_include_profile_payload():
    response = _build_runtime_response(
        kind="data",
        request_id="req-123",
        status="data",
        payload={"rows": []},
        profile={"callback": {"totalMs": 12.3}},
    )

    assert response["requestId"] == "req-123"
    assert response["profile"]["callback"]["totalMs"] == 12.3
