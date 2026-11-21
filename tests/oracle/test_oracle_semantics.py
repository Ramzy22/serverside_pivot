"""Hand-computed checks that pin the oracle to SEMANTICS.md."""

from __future__ import annotations

from decimal import Decimal

import pytest

from .oracle import Measure, MeasureFilter, Pred, Query, aggregate, eligible_sets, in_scope, pivot

DATA = [
    {"r": "N", "c": "x", "v": 1},
    {"r": "N", "c": "y", "v": 9},
    {"r": "S", "c": "x", "v": 4},
    {"r": "S", "c": "z", "v": 5},
    {"r": None, "c": "x", "v": 100},
]
SUM = Measure("v_sum", "sum", "v")
AVG = Measure("v_avg", "avg", "v")


def _values(rows):
    return {row.path: dict(row.values) for row in rows}


def test_null_dimension_is_its_own_group_not_grand_total():  # REQ-NULL-002
    rows = _values(pivot(Query(rows=["r"], measures=[SUM]), DATA))
    assert rows[()] == {"v_sum": 119}
    assert rows[(None,)] == {"v_sum": 100}
    assert list(rows)[-1] == (None,)  # REQ-BACK-001 NULLS LAST


def test_parent_avg_is_from_records_not_children():  # REQ-TOT-003
    rows = _values(pivot(Query(rows=["r", "c"], measures=[AVG]), DATA))
    assert rows[("N",)]["v_avg"] == 5.0
    assert rows[()]["v_avg"] == pytest.approx(119 / 5)


def test_measure_filter_evaluated_on_e0_and_totals_visible_vs_all():  # REQ-TOT-001..004
    q = Query(rows=["r", "c"], measures=[SUM], measure_filters=[MeasureFilter(2, "v_sum", ">", 3)])
    e0, e = eligible_sets(q, DATA)
    assert len(e0) == 5 and sorted(r["v"] for r in e) == [4, 5, 9, 100]
    visible = _values(pivot(q, DATA))
    assert visible[()]["v_sum"] == 118 and visible[("N",)]["v_sum"] == 9
    assert ("N", "x") not in visible  # REQ-TOT-002
    q.totals = "all_data"
    all_data = _values(pivot(q, DATA))
    assert all_data[()]["v_sum"] == 119 and all_data[("N",)]["v_sum"] == 10


def test_expansion_never_changes_totals():  # REQ-TOT-005
    full = _values(pivot(Query(rows=["r", "c"], measures=[SUM]), DATA))
    collapsed = _values(pivot(Query(rows=["r", "c"], measures=[SUM], expanded=[]), DATA))
    assert set(collapsed) == {(), ("N",), ("S",), (None,)}
    assert all(collapsed[k] == full[k] for k in collapsed)


def test_scope_is_exact():  # REQ-SEC-003
    assert in_scope({"t": "A"}, {"t": ("A",)})
    assert not in_scope({"t": "a"}, {"t": ("A",)})
    assert not in_scope({"t": None}, {"t": ("A",)})
    assert in_scope({"t": None}, {"t": (None,)})
    assert not in_scope({"t": 1}, {"t": ("1",)})
    assert not in_scope({"t": True}, {"t": (1,)})


def test_aggregation_null_and_empty_rules():  # REQ-AGG-001/002/004
    rs = [{"v": None}, {"v": None}]
    assert aggregate("sum", rs, "v") is None
    assert aggregate("count", rs, "v") == 0
    assert aggregate("count", rs, "*") == 2
    assert aggregate("sum", [{"v": Decimal("0.1")}, {"v": Decimal("0.2")}], "v") == Decimal("0.3")
    with pytest.raises(ValueError):
        aggregate("summ", rs, "v")


def test_filters_follow_sql_null_logic():
    assert not Pred("v", "!=", 3)({"v": None})
    assert Pred("v", "is_null")({"v": None})
