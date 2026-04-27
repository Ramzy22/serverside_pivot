import pytest

from pivot_engine.hierarchy_rows import (
    build_hierarchy_row_window,
    finalize_hierarchy_rows,
    normalize_expanded_paths,
)
from pivot_engine.tanstack_adapter import TanStackOperation, TanStackPivotAdapter, TanStackRequest
from pivot_engine.types.pivot_spec import Measure, PivotSpec


def _sales_spec():
    return PivotSpec(
        table="sales",
        rows=["region", "country"],
        measures=[Measure(field="sales", agg="sum", alias="sales_sum")],
    )


def test_hierarchy_row_window_owns_visibility_total_placement_and_paths():
    hierarchy_result = {
        "": [
            {"region": "North", "sales_sum": 100},
            {"region": None, "sales_sum": 180},
            {"region": "South", "sales_sum": 80},
            {"region": None, "sales_sum": 180},
        ],
        "North": [
            {"region": "North", "country": "USA", "sales_sum": 60},
            {"region": "North", "country": None, "sales_sum": 100},
            {"region": "North", "country": "Canada", "sales_sum": 40},
        ],
        "South": [
            {"region": "South", "country": "Brazil", "sales_sum": 80},
        ],
    }

    window = build_hierarchy_row_window(
        _sales_spec(),
        hierarchy_result,
        [["North"]],
        start_row=0,
        end_row=4,
        collect_formula_source_rows=True,
    )

    assert [row["_id"] for row in window["rows"]] == ["North", "USA", "Canada", "South", "Grand Total"]
    assert [row["depth"] for row in window["rows"]] == [0, 1, 1, 0, 0]
    assert window["rows"][-1]["_path"] == "__grand_total__"
    assert window["total_rows"] == 5
    assert window["grand_total_row"]["_id"] == "Grand Total"
    assert [row["_id"] for row in window["grand_total_formula_source_rows"]] == ["North", "South"]
    assert window["color_scale_stats"]["byCol"]["sales_sum"] == {"min": 40, "max": 100}


def test_hierarchy_normalization_and_finalization_are_shared_policy():
    assert normalize_expanded_paths(True) == (("__ALL__",),)
    assert normalize_expanded_paths([["B"], ["A"], ["A"]]) == (("A",), ("B",))

    rows = [
        {"_id": "Canada", "_path": "North|||Canada"},
        {"_id": "North", "_path": "North"},
        {"_id": "Grand Total", "_path": "__grand_total__", "_isTotal": True},
        {"_id": "Grand Total", "_path": "__grand_total__", "_isTotal": True},
    ]

    assert [row["_id"] for row in finalize_hierarchy_rows(rows)] == ["North", "Canada", "Grand Total"]
    assert [row["_id"] for row in finalize_hierarchy_rows(rows, preserve_window_order=True)] == [
        "Canada",
        "North",
        "Grand Total",
    ]


@pytest.mark.asyncio
async def test_adapter_legacy_hierarchy_fallback_slices_viewport_with_shared_policy():
    class BatchOnlyController:
        async def run_hierarchical_pivot_batch_load(self, _spec_dict, _target_paths, max_levels):
            assert max_levels == 2
            return {
                "": [
                    {"region": "North", "sales_sum": 100},
                    {"region": "South", "sales_sum": 80},
                ],
                "North": [
                    {"region": "North", "country": "USA", "sales_sum": 60},
                    {"region": "North", "country": "Canada", "sales_sum": 40},
                ],
            }

    adapter = TanStackPivotAdapter(BatchOnlyController())
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region", "country"],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 2},
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        1,
        2,
        expanded_paths=[["North"]],
    )

    assert [row["_id"] for row in response.data] == ["USA", "Canada"]
    assert [row["_path"] for row in response.data] == ["North|||USA", "North|||Canada"]
    assert response.total_rows == 4
