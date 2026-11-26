"""Every physical strategy is result-equivalent to the union_all reference."""

from __future__ import annotations

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pivot_engine.core.exec import execute
from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery, SortKey, Window
from pivot_engine.core.logical import plan as logical_plan
from pivot_engine.core.physical.fetch import to_pyarrow  # the core's one result boundary
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

@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
@pytest.mark.parametrize("totals", ["visible", "all_data"])
@pytest.mark.parametrize("subtotals", ["before", "none"])
def test_materialized_cube_pivot_sorts_and_filters(backend, totals, subtotals):
    from pivot_engine.core.physical import union_all

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
    plan = logical_plan(q)
    raw = con.table("t")
    cube = leaf_partials.partials(union_all.filtered(raw, q), plan)
    con.create_table("cube", to_pyarrow(cube))
    whole = leaf_partials.from_partials(plan, raw, con.table("cube"))
    rows = to_pyarrow(whole).to_pylist()
    got = [(tuple(row[k] for k in q.rows[:row["__depth"]]),
            {k: row[k] for k in q.value_columns()}) for row in rows]
    _assert_equal(got, _oracle(records, q))
    _assert_equal(got, _run(con, q, "union_all"))

@st.composite
def cube_queries(draw):
    """Only mergeable queries; every draw executes a cube, with no chooser/fallback."""
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
def test_materialized_cube_equals_oracle_property_on_every_backend(backend):
    from dataclasses import replace

    from .test_core_semantics import RECORDS as FULL_RECORDS
    from .test_core_semantics import _oracle
    from .test_cross_backend import _connect
    from .test_physical_cube import _rows

    @settings(max_examples=examples(40), deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(records=FULL_RECORDS, query=cube_queries())
    def check(records, query):
        from pivot_engine.core.physical import union_all

        con = _connect(backend, _table(records))
        raw = con.table("t")
        plan = logical_plan(query)
        assert leaf_partials.supports(plan, raw.schema())
        cube = leaf_partials.partials(union_all.filtered(raw, query), plan)
        con.create_table("cube", to_pyarrow(cube))
        expr = leaf_partials.from_partials(plan, raw, con.table("cube"))
        whole = _rows(expr, query, con)
        end = None if query.window.limit is None else query.window.offset + query.window.limit
        got = whole[query.window.offset:end]
        _assert_equal(got, _oracle(records, query))
        reference = union_all.lower(logical_plan(replace(query, window=Window())), raw)
        _assert_equal(got, _rows(reference, query, con)[query.window.offset:end])

    check()

@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
def test_materialized_cube_full_query_property_on_every_backend(backend):
    """Use the existing full query generator, explicitly selecting cube-supported IR.

    A backend refusal is accepted only if the direct reference expression
    also refuses; it never substitutes a reference result for cube execution.
    """
    from dataclasses import replace

    from pivot_engine.core.physical import (
        BackendCapabilityError,
        check_capabilities,
        union_all,
    )

    from .test_core_semantics import RECORDS as FULL_RECORDS
    from .test_core_semantics import _oracle, queries
    from .test_cross_backend import _connect
    from .test_physical_cube import _rows

    supported_queries = queries().filter(
        lambda q: q.ragged is None and all(m.agg in {"sum", "avg", "min", "max", "count"} for m in q.measures))
    executed = []

    @settings(max_examples=examples(40), deadline=None,
              suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much])
    @given(records=FULL_RECORDS, query=supported_queries)
    def check(records, query):
        con = _connect(backend, _table(records))
        raw = con.table("t")
        plan = logical_plan(query)
        assert leaf_partials.supports(plan, union_all.prepared(raw, query).schema())
        reference = union_all.lower(logical_plan(replace(query, window=Window())), raw)
        cube = leaf_partials.partials(union_all.filtered(raw, query), plan)
        # Check the direct reference's capabilities before materializing a
        # potentially unsupported preparation (e.g. an unpivot backend gap).
        try:
            check_capabilities(reference)
        except BackendCapabilityError:
            expr = leaf_partials.from_partials(plan, raw, cube)
            with pytest.raises(BackendCapabilityError):
                check_capabilities(expr)
            return
        con.create_table("cube", to_pyarrow(cube))
        expr = leaf_partials.from_partials(plan, raw, con.table("cube"))
        check_capabilities(expr)
        whole = _rows(expr, query, con)
        end = None if query.window.limit is None else query.window.offset + query.window.limit
        got = whole[query.window.offset:end]
        _assert_equal(got, _oracle(records, query))
        _assert_equal(got, _rows(reference, query, con)[query.window.offset:end])
        executed.append(True)

    check()
    assert executed, "the property must execute cubes, not only certify backend refusals"
