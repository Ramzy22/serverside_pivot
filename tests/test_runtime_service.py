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

    response = service.process(state, context)

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

    response = service.process(state, context)

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

    response_a = service.process(state, context_a)
    response_b = service.process(state, context_b)

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

    response = service.process(state, context)

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
        pivot_mode="report",
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
        pivot_mode="report",
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
        pivot_mode="report",
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
