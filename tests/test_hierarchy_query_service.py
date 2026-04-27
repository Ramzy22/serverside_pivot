import pytest

from pivot_engine.hierarchy_query_service import (
    HierarchyQuery,
    HierarchyQueryService,
    LegacyHierarchyAdapter,
)
from pivot_engine.types.pivot_spec import Measure, PivotSpec


class _FakeHierarchyController:
    def __init__(self):
        self.calls = []

    async def _run_hierarchy_view_legacy(self, spec, expanded_paths, **kwargs):
        self.calls.append((spec, expanded_paths, kwargs))
        return {
            "rows": [{"_id": "Consumer", "depth": 0}],
            "total_rows": 12,
            "grand_total_row": None,
            "grand_total_formula_source_rows": None,
            "color_scale_stats": {"byCol": {}, "table": None},
            "profile": {"controller": {"path": "legacy"}},
        }


@pytest.mark.asyncio
async def test_legacy_hierarchy_adapter_owns_total_rows_and_profiles_adapter():
    controller = _FakeHierarchyController()
    service = HierarchyQueryService(LegacyHierarchyAdapter(controller))
    spec = PivotSpec(
        table="sales",
        rows=["segment"],
        measures=[Measure(field="sales", agg="sum", alias="sales_sum")],
    )

    response = await service.execute(
        HierarchyQuery(
            spec=spec,
            expanded_paths=[["Consumer"]],
            start_row=0,
            end_row=10,
            include_grand_total_row=True,
            profiling=True,
        )
    )

    assert response["rows"] == [{"_id": "Consumer", "depth": 0}]
    assert response["total_rows"] == 12
    assert response["profile"]["controller"]["path"] == "legacy"
    assert response["profile"]["hierarchyService"]["adapter"] == "legacy_python_hierarchy"
    assert response["profile"]["hierarchyService"]["statsMode"] == "legacy_adapter_total_rows"
    assert controller.calls[0][1] == [["Consumer"]]
    assert controller.calls[0][2]["start_row"] == 0
    assert controller.calls[0][2]["end_row"] == 10
