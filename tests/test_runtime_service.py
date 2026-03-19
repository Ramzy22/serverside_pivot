import os
import sys

import pyarrow as pa

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "pivot_engine"))

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState, SessionRequestGate


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

    response = service.process(state, context, current_filter_options={})

    assert response.status == "data"
    assert isinstance(response.data, list)
    assert response.total_rows is not None
    assert response.total_rows >= len(response.data)


def test_initial_structural_load_includes_col_schema_sentinel():
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

    response = service.process(state, context, current_filter_options={})

    assert response.status == "data"
    assert isinstance(response.columns, list)
    assert any(
        isinstance(col, dict) and col.get("id") == "__col_schema" and col.get("col_schema")
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

    response_a = service.process(state, context_a, current_filter_options={})
    response_b = service.process(state, context_b, current_filter_options={})

    assert response_a.status == "data"
    assert response_b.status == "data"


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

    response = service.process(state, context, current_filter_options={})

    # Collapsed tree should contain only root-level groups + grand total.
    assert response.status == "data"
    assert response.total_rows == 3
    assert isinstance(response.data, list)
    assert len(response.data) == 3


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

    response = service.process(state, context, current_filter_options={})

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

    response = service.process(state, context, current_filter_options={})

    assert response.status == "chart_data"
    assert isinstance(response.chart_data, dict)
    assert isinstance(response.chart_data.get("rows"), list)
    assert response.chart_data.get("stateEpoch") == 2
    assert response.chart_data.get("paneId") == "chart-pane-1"
    assert response.chart_data.get("requestSignature") == "sig-1"
