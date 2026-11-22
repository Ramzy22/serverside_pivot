"""Every physical strategy is result-equivalent to the union_all reference."""

from __future__ import annotations

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pivot_engine.core.exec import execute
from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery, SortKey, Window
from pivot_engine.core.logical import plan as logical_plan
from pivot_engine.core.physical import choose, leaf_partials

from .test_core_semantics import DIM, _assert_equal, _con, _table, examples

RECORDS = st.lists(
    st.fixed_dictionaries({"r": DIM, "c": DIM, "v": st.one_of(st.none(), st.integers(-20, 20))}),
    min_size=0, max_size=20,
)


@st.composite
def algebraic_queries(draw):
    rows = draw(st.sampled_from([("r",), ("r", "c"), ("c", "r")]))
    aggs = draw(st.lists(st.sampled_from(["sum", "avg", "min", "max", "count"]), min_size=1, max_size=3, unique=True))
    measures = tuple(MeasureRef(f"m_{a}", a, "v") for a in aggs)
    if draw(st.booleans()):
        measures += (MeasureRef("n", "count", None),)
    mfs = ()
    if draw(st.booleans()):
        mfs = (MeasureFilter(draw(st.integers(1, len(rows))), measures[0].alias, ">", draw(st.integers(-5, 5))),)
    return PivotQuery(
        dataset="t", rows=rows, measures=measures, measure_filters=mfs,
        sort=(SortKey(rows[0], desc=True),) if draw(st.booleans()) else (),
        totals=draw(st.sampled_from(["visible", "all_data"])),
        subtotals=draw(st.sampled_from(["before", "after"])),
        window=Window(draw(st.integers(0, 3)), draw(st.one_of(st.none(), st.integers(0, 8)))),
    )


def _run(con, query, strategy):
    return [(r["path"], r["values"]) for r in execute(query, con.table, strategy=strategy).records()]


@settings(max_examples=examples(120), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=RECORDS, query=algebraic_queries())
def test_leaf_partials_equals_union_all(records, query):
    con = _con(_table(records))
    _assert_equal(_run(con, query, "leaf_partials"), _run(con, query, "union_all"))


@pytest.mark.parametrize("agg", ["median", "count_distinct"])
def test_holistic_measures_are_not_claimed_by_leaf_partials(agg):
    con = _con(_table([{"r": "A", "c": "x", "v": 1}]))
    q = PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("m", agg, "v"),))
    plan = logical_plan(q)
    schema = con.table("t").schema()
    assert not leaf_partials.supports(plan, schema)
    assert choose(plan, schema) == "union_all"
    with pytest.raises(ValueError):
        execute(q, con.table, strategy="leaf_partials")


def test_decimal_average_is_not_claimed_by_leaf_partials():
    con = _con(pa.table({"r": ["A"], "c": ["x"], "v": pa.array([1], pa.decimal128(10, 2))}))
    q = PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("m", "avg", "v"),))
    assert not leaf_partials.supports(logical_plan(q), con.table("t").schema())


def test_chooser_follows_the_measured_rule():
    from pivot_engine.core.physical import MAX_PARTIAL_GRAIN

    cols = {f"d{i}": ["x"] for i in range(MAX_PARTIAL_GRAIN + 1)}
    con = _con(pa.table({**cols, "v": [1]}))
    schema = con.table("t").schema()

    def chosen(depth, agg="sum", expanded=None):
        q = PivotQuery(dataset="t", rows=tuple(f"d{i}" for i in range(depth)),
                       measures=(MeasureRef("m", agg, "v"),), expanded=expanded)
        return choose(logical_plan(q), schema)

    assert chosen(2) == "leaf_partials"
    assert chosen(MAX_PARTIAL_GRAIN) == "leaf_partials"
    assert chosen(MAX_PARTIAL_GRAIN + 1) == "union_all"  # near-unique grain
    assert chosen(MAX_PARTIAL_GRAIN + 1, expanded=frozenset({("x",)})) == "leaf_partials"  # visible grain is 2
    assert chosen(2, agg="median") == "union_all"
