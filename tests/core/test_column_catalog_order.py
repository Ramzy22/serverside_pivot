"""REQ-PIV-002: the column catalog order (the order the pivot's column members are shown in)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from pivot_engine.core.ir import ColumnOrder, MeasureFilter, MeasureRef, PivotQuery, Predicate
from pivot_engine.core.exec import column_members
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.transport.core_bridge import core_column_catalog
from pivot_engine.types.pivot_spec import PivotSpec

DATA = pa.table({
    "r": ["x", "x", "x", "x", "y", "y"],
    "c": ["b", "a", "c", None, "a", "c"],
    "k": [2, 3, 1, 9, 3, 1],
    "t": ["1Y", "6M", "2W", None, "6M", "2W"],
    "v": [5.0, -40.0, 10.0, 1.0, 2.0, 30.0],
})


@pytest.fixture()
def resolve():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow("d", DATA)
    yield controller.resolve_table
    controller.close()


def _members(resolve, *orders, column="c", **query):
    q = PivotQuery(dataset="d", rows=("r",), measures=(MeasureRef("s", "sum", "v"),), columns=(column,), **query)
    return [m[0] for m in column_members(q, resolve, order=orders)]


def test_by_value_nulls_last_without_an_order(resolve):  # REQ-PIV-001
    assert _members(resolve) == ["a", "b", "c", None]


def test_by_total_descending_ties_by_value(resolve):  # REQ-PIV-002
    # totals over E0: a = -38, b = 5, c = 40, NULL = 1
    assert _members(resolve, ColumnOrder("c", True, total="s")) == ["c", "b", None, "a"]
    assert _members(resolve, ColumnOrder("c", False, total="s")) == ["a", None, "b", "c"]


def test_by_absolute_total(resolve):  # REQ-PIV-002
    assert _members(resolve, ColumnOrder("c", True, total="s", absolute=True)) == ["c", "a", "b", None]


def test_total_is_over_e0_not_after_measure_filters(resolve):  # REQ-PIV-002
    # a measure filter removes nodes from the view, never reorders the catalog
    got = _members(resolve, ColumnOrder("c", True, total="s"),
                   measure_filters=(MeasureFilter(1, "s", ">", 100.0),))
    assert got == ["c", "b", None, "a"]


def test_row_filters_change_the_totals(resolve):  # REQ-PIV-002: E0 includes the row filters
    got = _members(resolve, ColumnOrder("c", True, total="s"), filters=(Predicate("r", "=", "y"),))
    assert got == ["c", "a"]


def test_by_member_key(resolve):  # REQ-PIV-002 with REQ-SORT-003
    assert _members(resolve, ColumnOrder("c", False, by="k")) == ["c", "b", "a", None]
    assert _members(resolve, ColumnOrder("c", True, by="k")) == [None, "a", "b", "c"]


def test_an_unknown_member_key_is_ignored(resolve):  # REQ-PIV-002 with REQ-SORT-003
    assert _members(resolve, ColumnOrder("c", True, by="nope")) == ["c", "b", "a", None]


def test_by_tenor(resolve):  # REQ-PIV-002 with REQ-SORT-005
    assert _members(resolve, ColumnOrder("t", False, semantic="tenor"), column="t") == ["2W", "6M", "1Y", None]


def test_nulls_first(resolve):  # REQ-PIV-002 with REQ-SORT-006
    assert _members(resolve, ColumnOrder("c", False, nulls_first=True)) == [None, "a", "b", "c"]
    assert _members(resolve, ColumnOrder("c", True, total="s", nulls_first=True)) == ["c", "b", None, "a"]


def _catalog(resolve, **spec):
    base = {"table": "d", "rows": ["r"], "columns": ["c"], "pivot_config": {"enabled": True},
            "measures": [{"field": "v", "agg": "sum", "alias": "s"}]}
    return [e["c"] for e in core_column_catalog(resolve, PivotSpec.from_dict({**base, **spec}))]


def test_the_request_default_is_the_first_measure_total_descending(resolve):  # REQ-PIV-002
    assert _catalog(resolve) == ["c", "b", None, "a"]


def test_request_label_mode_and_direction(resolve):  # REQ-PIV-002
    assert _catalog(resolve, column_sort_options={"c": {"pivotSortMode": "label"}}) == ["a", "b", "c", None]
    assert _catalog(resolve, column_sort_options={"c": {"pivotDirection": "desc"}}) == ["c", "b", "a", None]
    assert _catalog(resolve, column_sort_options={"c": {"pivotSortMode": "total", "pivotDirection": "asc"}}) == [
        "a", None, "b", "c"]


def test_the_catalog_entries_carry_the_display_key_and_honour_a_cursor(resolve):  # REQ-PIV-002
    spec = {"table": "d", "rows": ["r"], "columns": ["c"],
            "pivot_config": {"enabled": True, "column_cursor": "a"},
            "measures": [{"field": "v", "agg": "sum", "alias": "s"}],
            "column_sort_options": {"c": {"pivotSortMode": "label"}}}
    entries = core_column_catalog(resolve, PivotSpec.from_dict(spec))
    # keys sort as text: "None" < "a", so the NULL member is before the cursor
    assert entries == [{"c": "b", "_col_key": "b"}, {"c": "c", "_col_key": "c"}]
