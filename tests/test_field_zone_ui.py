import os
import sys

import pyarrow as pa
import pytest

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "pivot_engine"))
sys.path.append(os.path.join(os.getcwd(), "dash_tanstack_pivot"))

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine.tanstack_adapter import (
    TanStackOperation,
    TanStackRequest,
    create_tanstack_adapter,
)


@pytest.fixture
def adapter():
    pivot_adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.table(
        {
            "region": ["North", "North", "South", "South"],
            "product": ["Laptop", "Phone", "Laptop", "Phone"],
            "sales": [100, 200, 300, 50],
        }
    )
    pivot_adapter.controller.load_data_from_arrow("test", table)
    return pivot_adapter


def _find_group_row(rows, group_id):
    return next((row for row in rows if row.get("region") == group_id or row.get("_id") == group_id), None)


@pytest.mark.asyncio
async def test_min_aggregation_is_server_side(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[
            {"id": "region"},
            {"id": "sales_min", "aggregationField": "sales", "aggregationFn": "min"},
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)
    north = _find_group_row(response.data, "North")

    assert north is not None
    assert north["sales_min"] == 100


@pytest.mark.asyncio
async def test_max_aggregation_is_server_side(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[
            {"id": "region"},
            {"id": "sales_max", "aggregationField": "sales", "aggregationFn": "max"},
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)
    south = _find_group_row(response.data, "South")

    assert south is not None
    assert south["sales_max"] == 300


@pytest.mark.asyncio
async def test_count_aggregation_works_with_pivot_columns(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_count", "aggregationField": "sales", "aggregationFn": "count"},
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)
    north = _find_group_row(response.data, "North")

    assert north is not None
    assert north["Laptop_sales_count"] == 1
    assert north["Phone_sales_count"] == 1


@pytest.mark.asyncio
async def test_percent_of_row_window_works_with_pivot_columns(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {
                "id": "sales_sum",
                "aggregationField": "sales",
                "aggregationFn": "sum",
                "windowFn": "percent_of_row",
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        row_totals=True,
    )

    response = await adapter.handle_request(request)
    north = _find_group_row(response.data, "North")

    assert north is not None
    assert north["Laptop_sales_sum"] == pytest.approx(100 / 300)
    assert north["Phone_sales_sum"] == pytest.approx(200 / 300)
    assert north["__RowTotal__sales_sum"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_percent_of_col_window_works_with_pivot_columns(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {
                "id": "sales_sum",
                "aggregationField": "sales",
                "aggregationFn": "sum",
                "windowFn": "percent_of_col",
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)
    north = _find_group_row(response.data, "North")
    south = _find_group_row(response.data, "South")

    assert north is not None and south is not None
    assert north["Laptop_sales_sum"] == pytest.approx(100 / 400)
    assert south["Laptop_sales_sum"] == pytest.approx(300 / 400)
    assert north["Phone_sales_sum"] == pytest.approx(200 / 250)
    assert south["Phone_sales_sum"] == pytest.approx(50 / 250)


@pytest.mark.asyncio
async def test_percent_of_grand_total_alias_is_supported(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[
            {"id": "region"},
            {
                "id": "sales_sum",
                "aggregationField": "sales",
                "aggregationFn": "sum",
                "windowFn": "percent_of_grand_total",
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)
    north = _find_group_row(response.data, "North")
    south = _find_group_row(response.data, "South")

    assert north is not None and south is not None
    assert north["sales_sum"] == pytest.approx(300 / 650)
    assert south["sales_sum"] == pytest.approx(350 / 650)


@pytest.mark.asyncio
async def test_row_fields_group_output_round_trip(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="test",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)
    group_ids = {row.get("region") or row.get("_id") for row in response.data if row.get("region") in {"North", "South"}}

    assert group_ids == {"North", "South"}


def test_dash_component_preserves_field_zone_props_and_filters():
    component = DashTanstackPivot(
        rowFields=["region"],
        colFields=["product"],
        valConfigs=[{"field": "sales", "agg": "sum"}],
        filters={
            "region": {
                "operator": "AND",
                "conditions": [{"type": "eq", "value": "North"}],
            }
        },
    )

    props = component.to_plotly_json()["props"]

    assert props["rowFields"] == ["region"]
    assert props["colFields"] == ["product"]
    assert props["valConfigs"] == [{"field": "sales", "agg": "sum"}]
    assert props["filters"] == {
        "region": {
            "operator": "AND",
            "conditions": [{"type": "eq", "value": "North"}],
        }
    }
