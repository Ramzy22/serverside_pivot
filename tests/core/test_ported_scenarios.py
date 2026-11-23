"""Scenarios of the deleted legacy planner/builder tests, answered by the semantic core.

Ported from test_visual_totals, test_mixed_filter and test_multi_condition_filters;
the expected values are the semantics (SEMANTICS.md), not the legacy engine's output.
"""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.transport.core_bridge import (
    CoreUnsupported,
    core_hierarchy_view,
    core_records,
    run_core_read,
)
from pivot_engine.types.pivot_spec import PivotSpec


@pytest.fixture()
def controller():
    c = ScalablePivotController(backend_uri=":memory:")
    c.load_data_from_arrow("sales", pa.table({
        "region": ["West", "West", "West", "East", "East", "East"],
        "country": ["France", "France", "USA", "Germany", "Germany", "Germany"],
        "sales": [100, 200, 120, 300, 400, 500],
    }))
    c.load_data_from_arrow("products", pa.table({
        "product": ["Laptop", "Phone", "Tablet", "Headphones"],
        "region": ["North", "South", "North", "South"],
        "sales": [100, 200, 150, 50],
    }))
    yield c
    c.close()


def _roots(controller, spec):
    view = asyncio.run(run_core_read(controller, spec.table, lambda resolve: core_hierarchy_view(
        resolve, spec, [], 0, None)))
    return {row["region"]: row for row in view["rows"] if not row.get("_isTotal")}


@pytest.mark.parametrize(("agg", "west", "east"), [("sum", 420, 1200), ("avg", 140, 400)])
def test_parent_totals_are_recomputed_from_the_records_under_a_measure_filter(controller, agg, west, east):
    # REQ-TOT-007 + REQ-TOT-003: the filter applies at the country level; a region is the measure over
    # the records of its passing countries (avg West = (100 + 200 + 120) / 3, never an average of averages).
    alias = f"sales_{agg}"
    spec = PivotSpec.from_dict({
        "table": "sales", "rows": ["region", "country"],
        "measures": [{"field": "sales", "agg": agg, "alias": alias}],
        "filters": [{"field": alias, "op": ">", "value": 110}],
    })
    roots = _roots(controller, spec)
    assert roots["West"][alias] == pytest.approx(west)
    assert roots["East"][alias] == pytest.approx(east)


def test_a_measure_filter_removes_a_country_from_its_parent_total(controller):  # REQ-TOT-007
    spec = PivotSpec.from_dict({
        "table": "sales", "rows": ["region", "country"],
        "measures": [{"field": "sales", "agg": "sum", "alias": "s"}],
        "filters": [{"field": "s", "op": ">", "value": 150}],  # USA (120) fails, France (300) passes
    })
    assert _roots(controller, spec)["West"]["s"] == 300


@pytest.mark.parametrize("order", ["dimension_first", "measure_first"])
def test_a_mixed_and_group_splits_into_a_record_filter_and_a_measure_filter(controller, order):
    # REQ-TOT-007: a flat AND of dimension and measure terms is the conjunction of both kinds of filter,
    # whatever the term order.
    terms = [{"field": "region", "op": "eq", "value": "North"}, {"field": "total", "op": "gt", "value": 150}]
    if order == "measure_first":
        terms.reverse()
    controller.load_data_from_arrow("mixed", pa.table({
        "region": ["North", "South", "North", "South"], "sales": [100, 200, 300, 400]}))
    result = controller.run_pivot({
        "table": "mixed", "rows": ["region"],
        "measures": [{"field": "sales", "agg": "sum", "alias": "total"}],
        "filters": [{"op": "AND", "conditions": terms}],
    }, return_format="dict")
    assert result["rows"] == [["North", 400]]


def test_a_mixed_or_group_is_a_typed_error(controller):  # REQ-TOT-007: no exact meaning, never guessed
    with pytest.raises(CoreUnsupported):
        controller.run_pivot({
            "table": "products", "rows": ["region"],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total"}],
            "filters": [{"op": "OR", "conditions": [
                {"field": "region", "op": "eq", "value": "North"}, {"field": "total", "op": "gt", "value": 150}]}],
        }, return_format="dict")


def _products(controller, filters):
    spec = PivotSpec.from_dict({"table": "products", "rows": ["product"],
                                "measures": [{"field": "sales", "agg": "sum", "alias": "s"}]})
    relation = core_records(controller.resolve_table("products"), spec, filters)
    return sorted(relation.to_pyarrow().column("product").to_pylist())


def test_a_multi_condition_and_group_selects_records(controller):  # REQ-DET-001 (E0 records)
    filters = [{"op": "AND", "conditions": [
        {"field": "product", "op": "contains", "value": "h"},
        {"field": "region", "op": "eq", "value": "South"},
    ]}]
    assert _products(controller, filters) == ["Headphones", "Phone"]


def test_a_single_condition_selects_records(controller):  # REQ-DET-001
    assert _products(controller, [{"field": "sales", "op": ">", "value": 120}]) == ["Phone", "Tablet"]


def test_record_filters_ignore_measure_aliases(controller):  # REQ-DET-001: post-aggregation, not records
    assert _products(controller, [{"field": "s", "op": ">", "value": 1000}]) == [
        "Headphones", "Laptop", "Phone", "Tablet"]


def test_an_unmappable_record_filter_fails_closed(controller):  # REQ-SEC-001: never a wider record set
    with pytest.raises(CoreUnsupported):
        _products(controller, [{"field": "tenant", "op": "=", "value": "A"}])
