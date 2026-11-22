"""Logical planning units: visibility from expansion, level pruning, filter grouping."""

from __future__ import annotations

from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery, SortKey
from pivot_engine.core.logical import _visible_parents, plan

M = (MeasureRef("s", "sum", "v"),)


def test_visible_parents_require_every_ancestor_expanded():
    expanded = frozenset({("a",), ("a", "x"), ("b", "y"), ("a", "x", "1"), ("c",)})
    assert _visible_parents(None, 3) is None
    assert _visible_parents(expanded, 0) is None
    assert _visible_parents(expanded, 1) is None
    assert _visible_parents(expanded, 2) == frozenset({("a",), ("c",)})
    assert _visible_parents(expanded, 3) == frozenset({("a", "x")})  # ("b","y") lacks ("b",)
    assert _visible_parents(expanded, 4) == frozenset({("a", "x", "1")})
    assert _visible_parents(frozenset(), 2) == frozenset()


def test_levels_without_expanded_parents_are_pruned():
    q = PivotQuery(dataset="t", rows=("r", "c", "d"), measures=M, expanded=frozenset({("a",)}))
    p = plan(q)
    assert [lvl.depth for lvl in p.levels] == [0, 1, 2]
    assert [lvl.keys for lvl in p.levels] == [(), ("r",), ("r", "c")]
    assert p.levels[2].visible_parents == frozenset({("a",)})
    full = plan(PivotQuery(dataset="t", rows=("r", "c"), measures=M))
    assert [(lvl.depth, lvl.visible_parents) for lvl in full.levels] == [(0, None), (1, None), (2, None)]
    collapsed = plan(PivotQuery(dataset="t", rows=("r", "c"), measures=M, expanded=frozenset()))
    assert [lvl.depth for lvl in collapsed.levels] == [0, 1]


def test_filters_grouped_by_level_in_order_and_value_source():
    f1, f2, f3 = MeasureFilter(2, "s", ">", 1), MeasureFilter(1, "s", "<", 9), MeasureFilter(2, "s", "!=", 4)
    p = plan(PivotQuery(dataset="t", rows=("r", "c"), measures=M, measure_filters=(f1, f2, f3),
                        sort=(SortKey("c", True), SortKey("r"))))
    assert p.filters_by_level == {2: (f1, f3), 1: (f2,)}
    assert p.values_from == "E"
    assert p.descending == {"c": True, "r": False}
    assert p.rows == ("r", "c")
    assert plan(PivotQuery(dataset="t", rows=("r",), measures=M, totals="all_data")).values_from == "E0"
