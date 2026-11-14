import os
import sys

import pyarrow as pa
import pytest


sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "serverside_pivot"))

from pivot_engine.tanstack_adapter import (  # noqa: E402
    TanStackOperation,
    TanStackRequest,
    create_tanstack_adapter,
)


ROW_META_KEYS = {
    "_id",
    "_path",
    "_isTotal",
    "_level",
    "_expanded",
    "_parentPath",
    "_has_children",
    "_is_expanded",
    "_pathFields",
    "depth",
    "_depth",
    "uuid",
    "subRows",
    "region",
    "country",
}


@pytest.fixture
def horizontal_adapter():
    return create_tanstack_adapter(backend_uri=":memory:")


def load_wide_hierarchy(horizontal_adapter, center_column_count=24):
    rows = []
    for region, country in (
        ("North", "USA"),
        ("North", "Canada"),
        ("South", "Brazil"),
        ("South", "Chile"),
    ):
        for index in range(center_column_count):
            rows.append(
                {
                    "region": region,
                    "country": country,
                    "product": f"Product_{index:03d}",
                    "sales": float((index + 1) * (len(country) + 1)),
                }
            )
    horizontal_adapter.controller.load_data_from_arrow(
        "horizontal_virtual_scroll",
        pa.Table.from_pylist(rows),
    )


def make_horizontal_request():
    return TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="horizontal_virtual_scroll",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region", "country"],
        aggregations=[],
        totals=False,
    )


def response_center_ids(response):
    return {
        key
        for row in response.data
        for key in row
        if key not in ROW_META_KEYS
    }


def response_value_snapshot(response, center_ids):
    return {
        row["_path"]: tuple(row.get(column_id) for column_id in center_ids)
        for row in response.data
        if not row.get("_isTotal")
    }


async def schema_ids_for_window(adapter, request, start, end, expanded_paths):
    response = await adapter.handle_virtual_scroll_request(
        request,
        0,
        20,
        expanded_paths,
        col_start=start,
        col_end=end,
        needs_col_schema=True,
        _allow_prefetch=False,
    )
    assert response.col_schema is not None
    return [entry["id"] for entry in response.col_schema["columns"]]


@pytest.mark.asyncio
async def test_horizontal_windows_round_trip_without_cross_window_values(horizontal_adapter):
    """Alternating windows must not leak stale values or lose the active slice."""
    load_wide_hierarchy(horizontal_adapter)
    request = make_horizontal_request()

    bootstrap = await horizontal_adapter.handle_virtual_scroll_request(
        request, 0, 20, [], col_start=0, col_end=3,
        needs_col_schema=True, _allow_prefetch=False,
    )
    assert bootstrap.col_schema is not None
    total = bootstrap.col_schema["total_center_cols"]
    assert total >= 12

    windows = [
        (0, 2),
        (total // 2, (total // 2) + 2),
        (total - 3, total - 1),
        (0, 2),
        (total - 3, total - 1),
    ]
    snapshots = {}
    for start, end in windows:
        expected_ids = await schema_ids_for_window(
            horizontal_adapter, request, start, end, []
        )
        response = await horizontal_adapter.handle_virtual_scroll_request(
            request,
            0,
            20,
            [],
            col_start=start,
            col_end=end,
            needs_col_schema=False,
            _allow_prefetch=False,
        )
        observed_ids = response_center_ids(response)
        assert observed_ids == set(expected_ids)
        snapshot = response_value_snapshot(response, expected_ids)
        assert snapshot
        window_key = (start, end)
        if window_key in snapshots:
            assert snapshot == snapshots[window_key]
        else:
            snapshots[window_key] = snapshot


@pytest.mark.asyncio
async def test_horizontal_right_edge_survives_expand_collapse_reexpand(horizontal_adapter):
    """Hierarchy changes retain the active right-edge column window."""
    load_wide_hierarchy(horizontal_adapter)
    request = make_horizontal_request()

    bootstrap = await horizontal_adapter.handle_virtual_scroll_request(
        request, 0, 20, [], needs_col_schema=True, _allow_prefetch=False
    )
    assert bootstrap.col_schema is not None
    total = bootstrap.col_schema["total_center_cols"]
    start, end = total - 3, total - 1
    expected_ids = await schema_ids_for_window(
        horizontal_adapter, request, start, end, [["North"]]
    )

    expanded = await horizontal_adapter.handle_virtual_scroll_request(
        request, 0, 20, [["North"]], col_start=start, col_end=end,
        needs_col_schema=False, _allow_prefetch=False,
    )
    collapsed = await horizontal_adapter.handle_virtual_scroll_request(
        request, 0, 20, [], col_start=start, col_end=end,
        needs_col_schema=False, _allow_prefetch=False,
    )
    reexpanded = await horizontal_adapter.handle_virtual_scroll_request(
        request, 0, 20, [["North"]], col_start=start, col_end=end,
        needs_col_schema=False, _allow_prefetch=False,
    )

    for response in (expanded, collapsed, reexpanded):
        assert response_center_ids(response) == set(expected_ids)

    north_child_paths = {
        row["_path"]
        for row in expanded.data
        if row.get("region") == "North" and row.get("depth") == 1
    }
    assert north_child_paths
    assert response_value_snapshot(reexpanded, expected_ids) == response_value_snapshot(
        expanded, expected_ids
    )
