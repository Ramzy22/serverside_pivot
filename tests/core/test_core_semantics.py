"""The semantic core against the oracle and the conformance vectors.

Unlike the legacy engine, the core has no expected failures: every vector and
every generated case must match (REQ-TOT-*, REQ-NULL-*, REQ-AGG-*, REQ-PAGE-001).
Generated filters (nested groups, NULL in value lists, case folding) cover
REQ-NULL-003.
"""

from __future__ import annotations

import json
import math
import os
import pathlib
from decimal import Decimal

import ibis
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pivot_engine.core.exec import execute
from pivot_engine.core.ir import from_wire, to_wire
from pivot_engine.core.ir import (
    Category, CategoryRule, DerivedMeasure, IRValidationError, MeasureFilter, MeasureRef, PivotQuery, Predicate,
    PredicateGroup, SortKey, Unpivot, UnpivotMember, Window,
)

from ..oracle.oracle import Measure as OMeasure
from ..oracle.oracle import MeasureFilter as OMeasureFilter
from ..oracle.oracle import Pred as OPred
from ..oracle.oracle import PredGroup as OPredGroup
from ..oracle.oracle import Query as OQuery
from ..oracle.oracle import pivot as oracle_pivot



def examples(n: int) -> int:
    """Example budget; PIVOT_PROPERTY_SCALE lowers it for mutation runs (default 1 = full)."""
    return max(5, int(n * float(os.environ.get("PIVOT_PROPERTY_SCALE", "1"))))


VECTORS = json.loads((pathlib.Path(__file__).parents[1] / "conformance" / "vectors.json").read_text())["vectors"]
_ARROW = {"string": pa.string(), "int64": pa.int64(), "float64": pa.float64()}


def _con(table: pa.Table):
    con = ibis.duckdb.connect()
    con.create_table("t", table)
    return con


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    return a == b


def _core(con, query):
    return [(r["path"], r["values"]) for r in execute(query, con.table).records()]


def _to_oracle_filter(f):
    if isinstance(f, PredicateGroup):
        return OPredGroup(f.op, tuple(_to_oracle_filter(i) for i in f.items))
    return OPred(f.field, f.op, f.value, f.case_sensitive)


def _oracle(records, query: PivotQuery):
    oq = OQuery(
        filters=[_to_oracle_filter(f) for f in query.filters],
        rows=list(query.rows),
        measures=[OMeasure(m.alias, m.agg, m.field, m.weight, m.q, m.order_by) for m in query.measures],
        measure_filters=[OMeasureFilter(f.level, f.measure, f.op, f.value) for f in query.measure_filters],
        totals=query.totals,
        subtotals=query.subtotals,
        expanded=None if query.expanded is None else list(query.expanded),
        sort=[(s.field, s.desc, s.absolute, s.by, s.member) for s in query.sort],
        columns=list(query.columns),
        column_members=list(query.column_members),
        derived=list(query.derived),
        categories=[(c.name, [(_to_oracle_filter(r.when), r.label) for r in c.rules], c.fallback)
                    for c in query.categories],
        unpivot=None if query.unpivot is None else (
            query.unpivot.label, query.unpivot.value, query.unpivot.key,
            [(m.label, m.field, m.order, m.agg, m.weight, m.q, m.order_by) for m in query.unpivot.members],
            query.unpivot.drop_nulls),
    )
    rows = [(r.path, dict(r.values)) for r in oracle_pivot(oq, records)]
    end = None if query.window.limit is None else query.window.offset + query.window.limit
    return rows[query.window.offset:end]


def _assert_equal(core, oracle):
    assert [p for p, _ in core] == [p for p, _ in oracle], "paths/order differ"
    for (path, cv), (_, ov) in zip(core, oracle, strict=True):
        bad = {k: (cv[k], ov[k]) for k in ov if not _same(cv[k], ov[k])}
        assert not bad, f"{path}: (core, oracle) {bad}"


# ------------------------------------------------------------------ vectors


@pytest.mark.parametrize("vector", VECTORS, ids=[v["id"] for v in VECTORS])
def test_core_conforms_to_every_vector(vector):
    cols = vector["data"]["columns"]
    con = _con(pa.table({k: pa.array(c["values"], _ARROW[c["type"]]) for k, c in cols.items()}))
    q = vector["query"]
    query = PivotQuery(
        dataset="t",
        rows=tuple(q["rows"]),
        measures=tuple(MeasureRef(m["alias"], m["agg"], m["field"]) for m in q["measures"]),
        measure_filters=tuple(MeasureFilter(f["level"], f["measure"], f["op"], f["value"]) for f in q["measureFilters"]),
        totals=q["totals"],
    )
    expected = [(tuple(r["path"]), r["values"]) for r in vector["expected"]["rows"]]
    _assert_equal(_core(con, query), expected)


# ------------------------------------------------------------------ properties

DIM = st.sampled_from(["A", "a", "B", "", " ", "x|y", "x_y", None])
RECORDS = st.lists(
    st.fixed_dictionaries({"r": DIM, "c": DIM, "k": DIM, "v": st.one_of(st.none(), st.integers(-20, 20)),
                           "w": st.one_of(st.none(), st.integers(0, 4))}),
    min_size=0, max_size=16,
)
AGG = st.sampled_from(["sum", "avg", "min", "max", "count", "count_distinct", "median",
                       "stddev", "variance", "percentile", "weighted_avg", "first", "last", "array_agg"])


@st.composite
def queries(draw):
    categories = ()
    if draw(st.booleans()):  # REQ-DIM-001: a text category (a row dimension) and an integer one (its member key)
        text = st.sampled_from(["G1", "g1", "Other", "", "x|y"])
        rules = tuple(CategoryRule(draw(FILTERS), draw(text)) for _ in range(draw(st.integers(0, 3))))
        categories = (Category("g", rules, draw(text)),
                      Category("n", tuple(CategoryRule(r.when, i) for i, r in enumerate(rules)), len(rules)))
        rows = tuple(draw(st.sampled_from([("g",), ("g", "r"), ("r", "g")])))
    else:
        rows = tuple(draw(st.sampled_from([("r",), ("r", "c"), ("c", "r")])))
    agg = draw(AGG)
    alias = f"m_{agg}"
    measures = (MeasureRef(alias, agg, "v",
                           weight="w" if agg == "weighted_avg" else None,
                           q=draw(st.sampled_from([0.0, 0.25, 0.5, 0.9, 1.0])) if agg == "percentile" else None,
                           # REQ-AGG-008: an integer order with NULLs and ties, or a text order
                           order_by=draw(st.sampled_from(["w", "k"])) if agg in {"first", "last"} else (
                               # REQ-AGG-009: a series ordered by a field (ties, NULL orders) or by value
                               draw(st.sampled_from([None, "w", "k"])) if agg == "array_agg" else None)),)
    unpivot = None
    key_fields = ["w", "k", "n"] if categories else ["w", "k"]
    if draw(st.booleans()):  # REQ-DIM-002: the measure axis as a row level (members over v and w)
        fields = draw(st.lists(st.sampled_from(["v", "w"]), min_size=1, max_size=2, unique=True))
        labels = draw(st.lists(st.sampled_from(["V", "W", "", "x|y"]), min_size=len(fields), max_size=len(fields),
                               unique=True))
        orders = draw(st.lists(st.integers(-1, 2), min_size=len(fields), max_size=len(fields)))
        per_member = draw(st.booleans())  # REQ-DIM-003: each member its own aggregation
        series_axis = per_member and draw(st.booleans())  # every member a series, or none (one value column)
        member_aggs = [("array_agg" if series_axis else draw(st.sampled_from(
                           ["sum", "avg", "min", "max", "count", "first", "last"]))) if per_member
                       else None for _ in fields]
        unpivot = Unpivot("mn", "mv", "mk", tuple(
            UnpivotMember(lab, f, o, a, order_by="k" if a in {"first", "last"} or (a == "array_agg" and o > 0) else None)
            for lab, f, o, a in zip(labels, fields, orders, member_aggs)), drop_nulls=draw(st.booleans()))
        position = draw(st.integers(0, len(rows)))
        rows = rows[:position] + ("mn",) + rows[position:]
        if per_member:
            agg, alias = "member", "m_member"
            measures = (MeasureRef(alias, "member", "mv"),)
        else:
            measures = (MeasureRef(alias, agg, "mv", weight=measures[0].weight, q=measures[0].q,
                                   order_by=measures[0].order_by),)
        key_fields = key_fields + ["mk"]
    series = agg == "array_agg" or (unpivot is not None and unpivot.per_member
                                    and unpivot.members[0].agg == "array_agg")
    mfs = ()
    if not series and draw(st.booleans()):
        mfs = (MeasureFilter(draw(st.integers(1, len(rows))), alias, draw(st.sampled_from([">", "<=", "="])),
                             draw(st.integers(-5, 10))),)
    expanded = None
    if len(rows) == 2 and draw(st.booleans()):
        expanded = frozenset((v,) for v in draw(st.sets(DIM, max_size=3)))
    sort_choices = [SortKey(rows[0], desc=True), SortKey(rows[-1], desc=draw(st.booleans())),
                    *([] if series else [SortKey(alias, desc=draw(st.booleans()),
                                                 absolute=agg not in {"min", "max"} and draw(st.booleans()))]),
                    # REQ-SORT-003: a member key (an integer and a string column, both with NULLs)
                    SortKey(rows[draw(st.integers(0, len(rows) - 1))], desc=draw(st.booleans()),
                            by=draw(st.sampled_from(key_fields)))]
    sort = tuple(draw(st.lists(st.sampled_from(sort_choices), max_size=2, unique_by=lambda k: k.field)))
    window = Window()
    if draw(st.booleans()):
        window = Window(offset=draw(st.integers(0, 6)), limit=draw(st.integers(0, 6)))
    filters = tuple(draw(st.lists(FILTERS, max_size=2)))
    derived = ()
    if not series and draw(st.booleans()):  # REQ-AGG-005 / REQ-AGG-007
        func = draw(st.sampled_from(["ratio", "percent_of_total", "rank", "dense_rank", "cumulative", "moving_avg"]))
        if draw(st.booleans()):  # several derived measures share one tree (mixed joins and windows)
            derived = (DerivedMeasure("pt", "percent_of_total", alias), DerivedMeasure("rk", "rank", alias))
        elif func == "ratio":
            measures = measures + (MeasureRef("k_count", "count", "v"),)
            derived = (DerivedMeasure("d", "ratio", alias, "k_count", null_value=draw(st.sampled_from([None, -1.0]))),)
        elif func in {"cumulative", "moving_avg"}:
            frame = draw(st.sampled_from([None, (None, 0), (-1, 1), (-2, 0), (0, None), (1, 2)]))
            derived = (DerivedMeasure("d", func, alias, frame=frame),)
        else:
            derived = (DerivedMeasure("d", func, alias, desc=draw(st.booleans())),)
    columns, members = (), ()
    if draw(st.booleans()):  # REQ-PIV-001: explicit column window, members may be absent from the data
        columns = ("k",)
        members = tuple((v,) for v in draw(st.lists(DIM, max_size=3, unique=True)))
        if not series and draw(st.booleans()) and alias not in {k.field for k in sort}:
            # REQ-SORT-004: a pivot cell, possibly outside the returned column window
            sort = sort + (SortKey(alias, desc=draw(st.booleans()), member=(draw(DIM),)),)
    return PivotQuery(dataset="t", rows=rows, measures=measures, measure_filters=mfs, sort=sort, filters=filters,
                      totals=draw(st.sampled_from(["visible", "all_data"])), subtotals=draw(st.sampled_from(["before", "after", "none"])),
                      expanded=expanded, window=window,
                      columns=columns, column_members=members, derived=derived, categories=categories,
                      unpivot=unpivot)


NEEDLE = st.sampled_from(["a", "A", "", "|", "x_", "b"])


@st.composite
def leaf_filters(draw):
    kind = draw(st.sampled_from(["str_cmp", "str_op", "member", "num", "null", "between"]))
    field = draw(st.sampled_from(["r", "c"]))
    case = draw(st.booleans())
    if kind == "str_cmp":
        return Predicate(field, draw(st.sampled_from(["=", "!="])), draw(NEEDLE), case)
    if kind == "str_op":
        op = draw(st.sampled_from(["contains", "not_contains", "starts_with", "ends_with"]))
        return Predicate(field, op, draw(NEEDLE), case)
    if kind == "member":
        values = tuple(draw(st.lists(DIM, max_size=3)))
        return Predicate(field, draw(st.sampled_from(["in", "not_in"])), values, case)
    if kind == "num":
        return Predicate("v", draw(st.sampled_from(["=", "!=", ">", ">=", "<", "<="])), draw(st.integers(-5, 5)))
    if kind == "null":
        return Predicate(draw(st.sampled_from(["r", "c", "v"])), draw(st.sampled_from(["is_null", "is_not_null", "is_empty", "is_not_empty"])))
    low = draw(st.integers(-10, 10))
    return Predicate("v", "between", (low, low + draw(st.integers(0, 10))))


FILTERS = st.recursive(
    leaf_filters(),
    lambda children: st.builds(PredicateGroup, st.sampled_from(["and", "or"]),
                               st.lists(children, min_size=1, max_size=3).map(tuple)),
    max_leaves=4,
)


def _table(records):
    return pa.table({
        "r": pa.array([x["r"] for x in records], pa.string()),
        "c": pa.array([x["c"] for x in records], pa.string()),
        "k": pa.array([x.get("k") for x in records], pa.string()),
        "v": pa.array([x["v"] for x in records], pa.int64()),
        "w": pa.array([x.get("w") for x in records], pa.int64()),
    })


@settings(max_examples=examples(150), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=RECORDS, query=queries())
def test_core_equals_oracle(records, query):
    _assert_equal(_core(_con(_table(records)), query), _oracle(records, query))


@pytest.mark.parametrize("sort", [(), (SortKey("m_member", member=("a",)),)])
def test_per_member_axis_cell_without_records_takes_the_member_empty_rule(sort):
    """REQ-DIM-003 (found by the property test): an empty pivot cell is the member's own aggregation over
    no records (count 0, sum NULL), and a cell sort orders by it (NULLS LAST)."""
    records = [{"r": "A", "c": "A", "k": "A", "v": None, "w": None}] * 3
    axis = Unpivot("mn", "mv", "mk", (UnpivotMember("V", "v", 0, "sum"), UnpivotMember("W", "w", 0, "count")))
    query = PivotQuery(dataset="t", rows=("mn", "r", "c"), measures=(MeasureRef("m_member", "member", "mv"),),
                       sort=sort, columns=("k",), column_members=(("A",), ("a",)), unpivot=axis)
    _assert_equal(_core(_con(_table(records)), query), _oracle(records, query))


@settings(max_examples=examples(150), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(query=queries())
def test_wire_form_round_trips(query):
    assert from_wire(to_wire(query)) == query


@settings(max_examples=examples(40), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=RECORDS, query=queries(), page=st.integers(1, 4))
def test_core_pages_concatenate_to_unpaged(records, query, page):  # REQ-PAGE-001
    con = _con(_table(records))
    full = _core(con, PivotQuery(**{**query.__dict__, "window": Window()}))
    paged = []
    for offset in range(0, len(full) + page, page):
        paged += _core(con, PivotQuery(**{**query.__dict__, "window": Window(offset, page)}))
    assert [p for p, _ in paged] == [p for p, _ in full]


# ------------------------------------------------------------------ validation


@pytest.mark.parametrize("bad", [
    dict(rows=("nope",)),
    dict(measures=(MeasureRef("x", "summ", "v"),)),
    dict(measures=(MeasureRef("x", "avg", "r"),)),
    dict(measure_filters=(MeasureFilter(3, "m", ">", 1),)),
    dict(totals="everything"),
])
def test_invalid_queries_are_typed_errors(bad):  # REQ-AGG-001
    base = dict(dataset="t", rows=("r",), measures=(MeasureRef("m", "sum", "v"),))
    with pytest.raises(IRValidationError):
        execute(PivotQuery(**{**base, **bad}), _con(_table([{"r": "A", "c": "x", "v": 1}])).table)


def test_measure_sort_orders_every_level_with_nulls_last():  # REQ-SORT-001
    records = [{"r": "A", "c": "x", "v": 1, "w": 1}, {"r": "A", "c": "y", "v": 9, "w": 1},
               {"r": "B", "c": "x", "v": 20, "w": 1}, {"r": "C", "c": "z", "v": None, "w": 1}]
    q = PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("s", "sum", "v"),),
                   sort=(SortKey("s", desc=True),))
    got = [p for p, _ in _core(_con(_table(records)), q)]
    assert got == [(), ("B",), ("B", "x"), ("A",), ("A", "y"), ("A", "x"), ("C",), ("C", "z")]


@pytest.mark.parametrize("agg, q, w, expected", [  # REQ-AGG-006
    ("stddev", None, None, None), ("variance", None, None, None),
    ("percentile", 0.5, None, 4.0), ("weighted_avg", None, "w", None),
])
def test_parametric_aggregations_on_a_single_input(agg, q, w, expected):
    records = [{"r": "A", "c": "x", "v": 4, "w": 0}]
    query = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("m", agg, "v", w, q),))
    assert [v["m"] for _, v in _core(_con(_table(records)), query)] == [expected, expected]


def test_window_measures_rank_by_measure_within_the_parent():  # REQ-AGG-005, REQ-AGG-007
    records = [{"r": "A", "c": "x", "v": 1, "w": 1}, {"r": "A", "c": "y", "v": 9, "w": 1},
               {"r": "A", "c": "z", "v": 9, "w": 1}, {"r": "B", "c": "x", "v": 20, "w": 1},
               {"r": "B", "c": "y", "v": None, "w": 1}]
    q = PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("s", "sum", "v"), MeasureRef("n", "count", "v")),
                   derived=(DerivedMeasure("rk", "rank", "s"), DerivedMeasure("dr", "dense_rank", "s"),
                            DerivedMeasure("cum", "cumulative", "s"), DerivedMeasure("mv", "moving_avg", "s", frame=(-1, 0)),
                            DerivedMeasure("pct", "percent_of_total", "s"),
                            DerivedMeasure("avg", "ratio", "s", "n", null_value=-1.0)))
    got = {p: (v["rk"], v["dr"], v["cum"], v["mv"], v["pct"], v["avg"]) for p, v in _core(_con(_table(records)), q)}
    assert got[()] == (1, 1, 39, 39.0, 1.0, 9.75)
    assert got[("A",)] == (2, 2, 19, 19.0, 19 / 39, 19 / 3)  # B=20 ranks first among the roots
    assert got[("B",)] == (1, 1, 39, 19.5, 20 / 39, 20.0)
    assert got[("A", "x")] == (3, 2, 1, 1.0, 1 / 39, 1.0)
    assert got[("A", "y")][:2] == (1, 1) and got[("A", "z")][:2] == (1, 1)  # ties share the rank
    assert got[("B", "y")] == (None, None, 20, 20.0, None, -1.0)  # NULL has no rank; zero count -> null_value


def test_absolute_sort_orders_by_magnitude():  # REQ-SORT-002
    records = [{"r": b, "c": "x", "v": v, "w": 1} for b, v in (("Low", 5), ("Large Loss", -100), ("Gain", 30),
                                                                ("Mid Loss", -50))]
    q = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("s", "sum", "v"),),
                   sort=(SortKey("s", desc=True, absolute=True),))
    assert [p for p, _ in _core(_con(_table(records)), q)] == [(), ("Large Loss",), ("Mid Loss",), ("Gain",), ("Low",)]
    with pytest.raises(IRValidationError, match="absolute sort needs a numeric value, not 'r'."):
        execute(PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("s", "sum", "v"),),
                           sort=(SortKey("r", absolute=True),)), _con(_table(records)).table)


def test_grand_total_exists_over_empty_data():  # REQ-TOT-008
    q = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("s", "sum", "v"), MeasureRef("n", "count", "v")))
    assert _core(_con(_table([])), q) == [((), {"s": None, "n": 0})]
