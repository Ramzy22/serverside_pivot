import pytest
import pyarrow as pa

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


@pytest.mark.asyncio
async def test_adapter_no_subtotal_request_uses_flat_leaf_rows():
    class FlatOnlyController:
        def __init__(self):
            self.calls = []

        async def run_pivot_async(self, spec, return_format="arrow", force_refresh=False, profile_sink=None):
            self.calls.append({
                "rows": list(spec.rows or []),
                "limit": spec.limit,
                "offset": spec.offset,
                "totals": spec.totals,
            })
            rows = [
                {"region": "North", "country": "USA", "sales_sum": 60},
                {"region": "North", "country": "Canada", "sales_sum": 40},
                {"region": "South", "country": "Brazil", "sales_sum": 80},
            ]
            if spec.limit:
                rows = rows[int(spec.offset or 0):int(spec.offset or 0) + int(spec.limit)]
            return pa.Table.from_pylist(rows)

    controller = FlatOnlyController()
    adapter = TanStackPivotAdapter(controller)
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
        pagination={"pageIndex": 0, "pageSize": 1},
        totals=False,
        include_subtotals=False,
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        1,
        1,
        expanded_paths=[],
    )

    assert [row["_id"] for row in response.data] == ["Canada"]
    assert [row["region"] for row in response.data] == ["North"]
    assert [row["country"] for row in response.data] == ["Canada"]
    assert [row["depth"] for row in response.data] == [1]
    assert [row["_path"] for row in response.data] == ["North|||Canada"]
    assert response.total_rows == 3
    assert controller.calls[0]["rows"] == ["region", "country"]
    assert controller.calls[0]["offset"] == 1
    assert controller.calls[1]["limit"] == 0


def test_iter_visible_hierarchy_rows_sets_path_and_pathfields_before_yield():
    """_path and _pathFields must be set on every row so tabular layout mode can
    show parent labels for child rows even when parent field values are absent."""
    from pivot_engine.hierarchy_rows import iter_visible_hierarchy_rows

    class _Spec:
        rows = ["region", "country"]

    hierarchy_result = {
        "": [{"region": "North", "sales_sum": 100}],
        "North": [
            # Simulate controller that omits parent field in child rows
            {"country": "USA", "sales_sum": 60},
            {"country": "Canada", "sales_sum": 40},
        ],
    }

    rows_out = list(iter_visible_hierarchy_rows(_Spec(), hierarchy_result, [["North"]]))
    north_row = next(r for r in rows_out if r.get("_id") == "North")
    usa_row = next(r for r in rows_out if r.get("_id") == "USA")
    canada_row = next(r for r in rows_out if r.get("_id") == "Canada")

    assert north_row["_path"] == "North"
    assert north_row["_pathFields"] == ["region"]
    assert north_row["depth"] == 0

    assert usa_row["_path"] == "North|||USA"
    assert usa_row["_pathFields"] == ["region", "country"]
    assert usa_row["depth"] == 1

    assert canada_row["_path"] == "North|||Canada"
    assert canada_row["_pathFields"] == ["region", "country"]
    assert canada_row["depth"] == 1


def test_no_subtotal_flag_participates_in_adapter_cache_fingerprint():
    class DummyController:
        pass

    adapter = TanStackPivotAdapter(DummyController())
    base_request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[{"id": "region"}],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        include_subtotals=True,
    )
    no_subtotal_request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[{"id": "region"}],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        include_subtotals=False,
    )

    assert adapter._request_structure_fingerprint(base_request) != adapter._request_structure_fingerprint(no_subtotal_request)
