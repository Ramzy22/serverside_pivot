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
        subtotals=draw(st.sampled_from(["before", "after", "none"])),
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
    assert chosen(1) == "union_all"  # one visible level: a scan plus a merge, no gain


def test_chooser_leaves_pivot_columns_to_union_all():
    con = _con(pa.table({"r": ["A"], "c": ["x"], "k": ["p"], "v": [1]}))
    q = PivotQuery(dataset="t", rows=("r", "c"), columns=("k",), column_members=(("p",),),
                   measures=(MeasureRef("m", "sum", "v"),))
    plan = logical_plan(q)
    assert leaf_partials.supports(plan, con.table("t").schema())  # exact, but slower per member
    assert choose(plan, con.table("t").schema()) == "union_all"

@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
@pytest.mark.parametrize("totals", ["visible", "all_data"])
@pytest.mark.parametrize("subtotals", ["before", "none"])
def test_pivot_cells_sorts_and_filters_merge_from_the_partials(backend, totals, subtotals):
    from .test_core_semantics import _oracle
    from .test_cross_backend import _connect

    records = [
        {"r": r, "c": c, "p": p, "v": v, "k": k}
        for r, c, p, v, k in [
            ("A", "x", "shown", 2, 9), ("A", "x", "hidden", 8, 3),
            ("A", "y", "shown", 1, 1), ("A", "y", "hidden", -4, 2),
            ("B", "x", "shown", 4, 5), ("B", "x", "hidden", 3, 4),
            (None, "z", "hidden", 7, None), (None, "z", "shown", None, 8),
        ]
    ]
    con = _connect(backend, pa.Table.from_pylist(records))
    q = PivotQuery(dataset="t", rows=("r", "c"), columns=("p",),
                   column_members=(("shown",),), measures=(
                       MeasureRef("m", "sum", "v"), MeasureRef("a", "avg", "v"),
                       MeasureRef("mn", "min", "v"), MeasureRef("mx", "max", "v"),
                       MeasureRef("n", "count", None), MeasureRef("nf", "count", "v")),
                   sort=(SortKey("m", desc=True, member=("hidden",)), SortKey("r", by="k")),
                   measure_filters=(MeasureFilter(2, "m", ">", 0), MeasureFilter(1, "m", ">", 0, True)),
                   totals=totals, subtotals=subtotals)
    got = _run(con, q, "leaf_partials")
    _assert_equal(got, _oracle(records, q))
    _assert_equal(got, _run(con, q, "union_all"))


@st.composite
def cube_queries(draw):
    """Only mergeable queries, with pivot columns, cell/member sorts, cell filters and expansion."""
    from dataclasses import replace

    from .test_core_semantics import FILTERS

    q = draw(algebraic_queries())
    members = tuple((member,) for member in draw(st.lists(DIM, max_size=3, unique=True)))
    sorts = draw(st.sampled_from(["member", "measure", "cell", "dimension"]))
    if sorts == "member":
        sort = (SortKey(q.rows[0], by="w", desc=draw(st.booleans()), nulls_first=draw(st.booleans())),)
    elif sorts == "measure":
        sort = (SortKey(q.measures[0].alias, desc=draw(st.booleans()), absolute=draw(st.booleans())),)
    elif sorts == "cell":
        sort = (SortKey(q.measures[0].alias, member=(draw(DIM),), desc=draw(st.booleans()),
                        nulls_first=draw(st.booleans())),)
    else:
        sort = q.sort
    mfs = q.measure_filters
    if draw(st.booleans()):
        mfs += (MeasureFilter(draw(st.integers(1, len(q.rows))), q.measures[0].alias,
                              draw(st.sampled_from([">", "!=", "<="])), draw(st.integers(-3, 3)), True),)
    expanded = None
    if len(q.rows) == 2 and draw(st.booleans()):
        expanded = frozenset((member,) for member in draw(st.sets(DIM, max_size=3)))
    return replace(q, columns=("k",), column_members=members, sort=sort, measure_filters=mfs,
                   expanded=expanded, filters=tuple(draw(st.lists(FILTERS, max_size=2))))


@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
def test_leaf_partials_equal_the_oracle_property_on_every_backend(backend):
    from .test_core_semantics import RECORDS as FULL_RECORDS
    from .test_core_semantics import _oracle
    from .test_cross_backend import _connect

    @settings(max_examples=examples(40), deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(records=FULL_RECORDS, query=cube_queries())
    def check(records, query):
        con = _connect(backend, _table(records))
        assert leaf_partials.supports(logical_plan(query), con.table("t").schema())
        got = _run(con, query, "leaf_partials")
        _assert_equal(got, _oracle(records, query))
        _assert_equal(got, _run(con, query, "union_all"))

    check()


@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
def test_leaf_partials_full_query_property_on_every_backend(backend):
    """The full query generator, restricted to mergeable IR; a backend refusal must match the reference's."""
    from pivot_engine.core.physical import BackendCapabilityError

    from .test_core_semantics import RECORDS as FULL_RECORDS
    from .test_core_semantics import _oracle, queries
    from .test_cross_backend import _connect

    supported_queries = queries().filter(
        lambda q: q.ragged is None and all(m.agg in {"sum", "avg", "min", "max", "count"} for m in q.measures))
    executed = []

    @settings(max_examples=examples(40), deadline=None,
              suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much])
    @given(records=FULL_RECORDS, query=supported_queries)
    def check(records, query):
        con = _connect(backend, _table(records))
        try:
            reference = _run(con, query, "union_all")
        except BackendCapabilityError:
            with pytest.raises(BackendCapabilityError):
                _run(con, query, "leaf_partials")
            return
        got = _run(con, query, "leaf_partials")
        _assert_equal(got, _oracle(records, query))
        _assert_equal(got, reference)
        executed.append(True)

    check()
    assert executed, "the property must execute leaf partials, not only certify backend refusals"
