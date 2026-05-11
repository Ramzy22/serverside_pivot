import pytest
import pyarrow as pa

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime.service import PivotRuntimeService
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest


def _controller_with_measure_axis_data(table_name: str = "measure_axis_hierarchy"):
    controller = ScalablePivotController(planner_name="ibis", backend_uri=":memory:")
    controller.load_data_from_arrow(
        table_name,
        pa.table(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "sales": [100, 50, 80, 20],
                "cost": [40, 10, 30, 5],
                "peak": [7, 5, 9, 4],
            }
        ),
    )
    return controller


def _amounts(rows, *fields):
    return {
        tuple(row.get(field) for field in fields): row.get("Amount")
        for row in rows
    }


@pytest.mark.asyncio
async def test_measure_axis_hierarchy_middle_level_keeps_measure_name_at_its_level():
    controller = _controller_with_measure_axis_data("measure_axis_hierarchy_simple")
    spec = controller._normalize_spec(
        {
            "table": "measure_axis_hierarchy_simple",
            "rows": ["region", "Measure Name", "desk"],
            "measures": [
                {"field": "sales", "agg": "sum", "alias": "sales_sum"},
                {"field": "cost", "agg": "sum", "alias": "cost_sum"},
            ],
            "measureAxis": {
                "placement": "rows",
                "labelField": "Measure Name",
                "valueField": "Amount",
                "members": [
                    {"measureAlias": "sales_sum", "label": "Sales", "order": 0},
                    {"measureAlias": "cost_sum", "label": "Cost", "order": 1},
                ],
            },
            "sort": [
                {
                    "field": "Measure Name",
                    "order": "asc",
                    "sortKeyField": "__sortkey__Measure Name",
                }
            ],
            "totals": False,
            "limit": 100,
        }
    )

    result = await controller.run_hierarchical_pivot_batch_load(
        spec.to_dict(),
        [["North"], ["North", "Sales"]],
        max_levels=len(spec.rows),
    )

    assert _amounts(result[""], "region") == {("North",): 200.0, ("South",): 135.0}
    assert all("Measure Name" not in row for row in result[""])

    assert _amounts(result["North"], "Measure Name") == {
        ("Sales",): 150.0,
        ("Cost",): 50.0,
    }
    assert all("desk" not in row for row in result["North"])

    assert _amounts(result["North|||Sales"], "desk") == {
        ("Rates",): 100.0,
        ("Credit",): 50.0,
    }

    view = await controller.run_hierarchy_view(
        spec,
        [["North"], ["North", "Sales"]],
        start_row=0,
        end_row=20,
    )
    assert {
        row.get("_path"): (row.get("depth"), row.get("_pathFields"))
        for row in view["rows"]
    } == {
        "North": (0, ["region"]),
        "North|||Sales": (1, ["region", "Measure Name"]),
        "North|||Sales|||Credit": (2, ["region", "Measure Name", "desk"]),
        "North|||Sales|||Rates": (2, ["region", "Measure Name", "desk"]),
        "North|||Cost": (1, ["region", "Measure Name"]),
        "South": (0, ["region"]),
    }


@pytest.mark.asyncio
async def test_measure_axis_hierarchy_first_and_last_levels_work_with_aggregate_first():
    controller = _controller_with_measure_axis_data("measure_axis_hierarchy_mixed")
    base_spec = {
        "table": "measure_axis_hierarchy_mixed",
        "measures": [
            {"field": "sales", "agg": "sum", "alias": "sales_sum"},
            {"field": "peak", "agg": "max", "alias": "peak_max"},
        ],
        "measureAxis": {
            "placement": "rows",
            "labelField": "Measure Name",
            "valueField": "Amount",
            "members": [
                {"measureAlias": "sales_sum", "label": "Sales", "order": 0},
                {"measureAlias": "peak_max", "label": "Peak", "order": 1},
            ],
        },
        "sort": [
            {
                "field": "Measure Name",
                "order": "asc",
                "sortKeyField": "__sortkey__Measure Name",
            }
        ],
        "totals": False,
        "limit": 100,
    }

    measure_first_spec = controller._normalize_spec(
        {**base_spec, "rows": ["Measure Name", "region", "desk"]}
    )
    measure_first = await controller.run_hierarchical_pivot_batch_load(
        measure_first_spec.to_dict(),
        [["Sales"]],
        max_levels=len(measure_first_spec.rows),
    )

    assert _amounts(measure_first[""], "Measure Name") == {
        ("Sales",): 250.0,
        ("Peak",): 9.0,
    }
    assert _amounts(measure_first["Sales"], "region") == {
        ("North",): 150.0,
        ("South",): 100.0,
    }

    measure_last_spec = controller._normalize_spec(
        {**base_spec, "rows": ["region", "desk", "Measure Name"]}
    )
    measure_last = await controller.run_hierarchical_pivot_batch_load(
        measure_last_spec.to_dict(),
        [["North"], ["North", "Rates"]],
        max_levels=len(measure_last_spec.rows),
    )

    assert _amounts(measure_last[""], "region") == {
        ("North",): 157.0,
        ("South",): 109.0,
    }
    assert all("Measure Name" not in row for row in measure_last[""])

    assert _amounts(measure_last["North"], "desk") == {
        ("Rates",): 107.0,
        ("Credit",): 55.0,
    }
    assert all("Measure Name" not in row for row in measure_last["North"])

    assert _amounts(measure_last["North|||Rates"], "Measure Name") == {
        ("Sales",): 100.0,
        ("Peak",): 7.0,
    }


@pytest.mark.asyncio
async def test_measure_axis_hierarchy_column_window_uses_value_field_not_hidden_measure_placeholders():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "measure_axis_hierarchy_window",
        pa.table(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "sales": [100, 50, 80, 20],
                "cost": [40, 10, 30, 5],
            }
        ),
    )
    row_fields = ["region", "Measure Name", "desk"]
    val_configs = [
        {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
        {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
    ]
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="measure_axis_hierarchy_window",
        columns=PivotRuntimeService._build_request_columns(row_fields, [], val_configs),
        filters={},
        custom_dimensions=[],
        measure_axis={
            "placement": "rows",
            "labelField": "Measure Name",
            "valueField": "Amount",
            "members": [
                {"measureAlias": "sales_sum", "label": "Sales", "order": 0},
                {"measureAlias": "cost_sum", "label": "Cost", "order": 1},
            ],
        },
        sorting=[
            {
                "id": "Measure Name",
                "desc": False,
                "sortKeyField": "__sortkey__Measure Name",
            }
        ],
        grouping=row_fields,
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 100},
        totals=False,
        row_totals=False,
        include_subtotals=False,
        version=2,
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        0,
        99,
        expanded_paths=[["North"], ["North", "Sales"]],
        needs_col_schema=True,
        col_start=0,
        col_end=1,
    )

    assert [column["id"] for column in response.col_schema["columns"]] == ["Amount"]
    assert "sales_sum" not in {column["id"] for column in response.columns}
    assert "cost_sum" not in {column["id"] for column in response.columns}
    assert "Amount" in response.data[0]
    assert "_pathFields" in response.data[0]


@pytest.mark.asyncio
async def test_measure_axis_hierarchy_preserves_array_agg_series_values():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    point_type = pa.struct([("x", pa.string()), ("y", pa.float64())])
    adapter.controller.load_data_from_arrow(
        "measure_axis_array_series",
        pa.table(
            {
                "instrument": ["AAA", "AAA", "BBB", "BBB"],
                "price_point": pa.array(
                    [
                        {"x": "d1", "y": 1.0},
                        {"x": "d2", "y": 2.0},
                        {"x": "d1", "y": 3.0},
                        {"x": "d2", "y": 4.0},
                    ],
                    type=point_type,
                ),
                "pnl_point": pa.array(
                    [
                        {"x": "d1", "y": 10.0},
                        {"x": "d2", "y": 20.0},
                        {"x": "d1", "y": 30.0},
                        {"x": "d2", "y": 40.0},
                    ],
                    type=point_type,
                ),
            }
        ),
    )
    row_fields = ["instrument", "Measure Name"]
    val_configs = [
        {"field": "price_point", "agg": "array_agg", "alias": "price_series", "label": "Price 20D"},
        {"field": "pnl_point", "agg": "array_agg", "alias": "pnl_series", "label": "PnL 20D"},
    ]
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="measure_axis_array_series",
        columns=PivotRuntimeService._build_request_columns(row_fields, [], val_configs),
        filters={},
        custom_dimensions=[],
        measure_axis={
            "placement": "rows",
            "labelField": "Measure Name",
            "valueField": "Amount",
            "members": [
                {"measureAlias": "price_series", "label": "Price 20D", "order": 0},
                {"measureAlias": "pnl_series", "label": "PnL 20D", "order": 1},
            ],
        },
        sorting=[
            {
                "id": "Measure Name",
                "desc": False,
                "sortKeyField": "__sortkey__Measure Name",
            }
        ],
        grouping=row_fields,
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 100},
        totals=False,
        row_totals=False,
        include_subtotals=False,
        version=1,
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        0,
        99,
        expanded_paths=[["AAA"]],
        needs_col_schema=True,
    )

    amount_by_path = {row["_path"]: row.get("Amount") for row in response.data}
    assert response.col_schema["columns"][0]["id"] == "Amount"
    assert amount_by_path["AAA|||Price 20D"] == [{"x": "d1", "y": 1.0}, {"x": "d2", "y": 2.0}]
    assert amount_by_path["AAA|||PnL 20D"] == [{"x": "d1", "y": 10.0}, {"x": "d2", "y": 20.0}]

    spec = adapter.convert_tanstack_request_to_pivot_spec(request)
    hierarchy = await adapter.controller.run_hierarchical_pivot_batch_load(
        spec.to_dict(),
        [["AAA"]],
        max_levels=len(row_fields),
    )
    root_amounts = {row["instrument"]: row.get("Amount") for row in hierarchy[""]}
    assert root_amounts == {"AAA": None, "BBB": None}
