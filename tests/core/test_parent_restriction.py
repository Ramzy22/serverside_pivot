"""The parent restriction (each level aggregates only the records under visible parents) is exact.

Three plain row levels, expansion sets mixing one- and two-member paths with NULL and look-alike members,
measure sorts, cell sorts, member keys, measure filters and both totals modes; every strategy and both
lowerings of a multi-key restriction (literal predicate, semi join) equal the oracle.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pivot_engine.core.exec import clear_compile_cache, execute
from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery, SortKey, Window
from pivot_engine.core.logical import plan as logical_plan
from pivot_engine.core.physical import leaf_partials, union_all

from .test_core_semantics import DIM, RECORDS, _assert_equal, _con, _oracle, _table, examples

ROWS = ("r", "c", "k")


@st.composite
def queries(draw):
    agg = draw(st.sampled_from(["sum", "avg", "count", "min", "max", "median"]))
    measures = (MeasureRef("m", agg, "v"), MeasureRef("n", "count", None))
    one = st.tuples(DIM)
    two = st.tuples(DIM, DIM)
    expanded = frozenset(draw(st.sets(one | two, max_size=6)))
    sort = draw(st.sampled_from([
        (), (SortKey("m", desc=True),), (SortKey("r", desc=True), SortKey("n")),
        (SortKey("c", by="w"),),  # REQ-SORT-003: a member key per level
    ]))
    mfs = ()
    if draw(st.booleans()):
        mfs = (MeasureFilter(draw(st.integers(1, 3)), "n", ">", draw(st.integers(0, 2))),)
    return PivotQuery(
        dataset="t", rows=ROWS, measures=measures, measure_filters=mfs, sort=sort,
        expanded=expanded, totals=draw(st.sampled_from(["visible", "all_data"])),
        subtotals=draw(st.sampled_from(["before", "after"])),
        window=Window(0, None),
    )


@pytest.mark.parametrize("literal_groups", [union_all._LITERAL_GROUPS, 0], ids=["literal", "semi_join"])
@settings(max_examples=examples(120), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=RECORDS, query=queries())
def test_restricted_levels_equal_the_oracle(literal_groups, records, query):
    default = union_all._LITERAL_GROUPS
    union_all._LITERAL_GROUPS = literal_groups
    clear_compile_cache()  # the plan tier must not serve a tree lowered under the other threshold
    try:
        con = _con(_table(records))
        expected = _oracle(records, query)
        strategies = ["union_all"]
        if leaf_partials.supports(logical_plan(query), con.table("t").schema()):
            strategies.append("leaf_partials")
        for strategy in strategies:
            got = [(r["path"], r["values"]) for r in execute(query, con.table, strategy=strategy).records()]
            _assert_equal(got, expected)
    finally:
        union_all._LITERAL_GROUPS = default
        clear_compile_cache()


def test_a_level_reads_only_records_under_visible_parents():
    """The restriction is applied to the records, before the level's aggregation (no post-aggregate join)."""
    query = PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("m", "sum", "v"),),
                       expanded=frozenset({("A",), (None,)}))
    level = logical_plan(query).levels[-1]
    assert level.parent_restriction() == (("r",), frozenset({("A",), (None,)}))
    con = _con(_table([{"r": "A", "c": "x", "v": 1}, {"r": None, "c": "y", "v": 2}, {"r": "B", "c": "z", "v": 4}]))
    sql = str(con.compile(union_all.lower(logical_plan(query), con.table("t"))))
    assert "IN ('A')" in sql and "IS NULL" in sql and "SEMI" not in sql.upper()
