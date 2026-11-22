"""IR validation rules, one by one (REQ-AGG-001: typed errors, never silent)."""

from __future__ import annotations

import re

import pytest

from pivot_engine.core.ir import (
    DerivedMeasure,
    PREDICATE_OPS, STRING_OPS, IRValidationError, MeasureFilter, MeasureRef, PivotQuery, Predicate, PredicateGroup,
    SortKey, Window, from_wire,
)

SCHEMA = {"r": "string", "c": "string", "v": "integer", "f": "floating", "flag": "boolean", "w": "integer"}
M = (MeasureRef("s", "sum", "v"),)


def q(**kw):
    base = dict(dataset="t", rows=("r", "c"), measures=M)
    return PivotQuery(**{**base, **kw})


def bad(message, **kw):
    with pytest.raises(IRValidationError, match="^" + re.escape(message) + "$"):
        q(**kw).validate(SCHEMA)


def test_valid_query_returns_itself():
    query = q()
    assert query.validate(SCHEMA) is query


@pytest.mark.parametrize("kw, message", [
    (dict(dataset=""), "dataset is required."),
    (dict(rows=("r", "r")), "row dimensions must be unique."),
    (dict(rows=("r", "zz")), "row dimension references unknown column 'zz'."),
    (dict(measures=()), "at least one measure is required."),
    (dict(measures=(MeasureRef("s", "sum", "v"), MeasureRef("s", "max", "v"))), "measure aliases must be unique."),
    (dict(measures=(MeasureRef("c", "sum", "v"), MeasureRef("r", "sum", "v"))),
     "measure alias collides with a dimension: ['c', 'r']"),
    (dict(measures=(MeasureRef("s", "summ", "v"),)), "unknown aggregation 'summ'"),
    (dict(measures=(MeasureRef("s", "sum", None),)), "sum requires a field."),
    (dict(measures=(MeasureRef("s", "sum", "zz"),)), "measure 's' references unknown column 'zz'."),
    (dict(measures=(MeasureRef("s", "avg", "r"),)), "avg does not accept string column 'r'."),
    (dict(measures=(MeasureRef("s", "median", "flag"),)), "median does not accept boolean column 'flag'."),
    (dict(filters=(Predicate("zz", "=", 1),)), "filter references unknown column 'zz'."),
    (dict(filters=(Predicate("v", "~", 1),)), "unsupported filter operator '~'."),
    (dict(filters=(Predicate("v", "in", 1),)), "in requires a list value."),
    (dict(filters=(Predicate("v", "not_in", "ab"),)), "not_in requires a list value."),
    (dict(measure_filters=(MeasureFilter(1, "zz", ">", 1),)), "measure filter references unknown measure 'zz'."),
    (dict(measure_filters=(MeasureFilter(0, "s", ">", 1),)), "measure filter level 0 outside 1..2."),
    (dict(measure_filters=(MeasureFilter(3, "s", ">", 1),)), "measure filter level 3 outside 1..2."),
    (dict(measure_filters=(MeasureFilter(1, "s", "in", 1),)), "unsupported measure filter operator 'in'."),
    (dict(sort=(SortKey("v"),)), "sort key 'v' is neither a row dimension nor a measure."),
    (dict(totals="all"), "totals must be 'visible' or 'all_data', not 'all'."),
    (dict(window=Window(-1, 5)), "window offset/limit must be non-negative."),
    (dict(window=Window(0, -1)), "window offset/limit must be non-negative."),
    (dict(expanded=frozenset({("a", "b")})), "expanded path ('a', 'b') is not a parent path."),
    (dict(expanded=frozenset({()})), "expanded path () is not a parent path."),
    (dict(expanded=frozenset({"a"})), "expanded path 'a' is not a parent path."),
    (dict(rows=("r",), expanded=frozenset({("a",)})), "expanded path ('a',) is not a parent path."),
])
def test_each_rule_is_a_typed_error_with_its_message(kw, message):
    bad(message, **kw)


@pytest.mark.parametrize("kw", [
    dict(rows=()),  # grand total only
    dict(measures=(MeasureRef("n", "count", None),)),
    dict(measures=(MeasureRef("n", "count", "r"), MeasureRef("d", "count_distinct", "flag"))),
    dict(measures=(MeasureRef("a", "avg", "f"),)),
    dict(measure_filters=(MeasureFilter(1, "s", ">", 1), MeasureFilter(2, "s", "<=", 9))),
    dict(sort=(SortKey("c", True),)),
    dict(sort=(SortKey("s", True), SortKey("r"))),
    dict(totals="all_data"),
    dict(window=Window(0, 0)),
    dict(window=Window(7, None)),
    dict(expanded=frozenset({("a",), (None,)})),
    dict(filters=tuple(
        Predicate("r", op, "a") if op in STRING_OPS
        else Predicate("v", op, [1] if op in {"in", "not_in"} else (1, 2) if op == "between" else 1)
        for op in sorted(PREDICATE_OPS))),
    dict(filters=(PredicateGroup("or", (Predicate("r", "=", "a", False),
                                         PredicateGroup("and", (Predicate("v", ">", 1), Predicate("c", "is_null"))))),)),
    dict(filters=(Predicate("v", "in", (1, 2)), Predicate("v", "not_in", frozenset({3})), Predicate("v", "in", {4}))),
])
def test_boundaries_that_are_valid(kw):
    q(**kw).validate(SCHEMA)


@pytest.mark.parametrize("op", ["=", "!=", ">", ">=", "<", "<="])
def test_every_comparison_operator_is_allowed_in_measure_filters(op):
    q(measure_filters=(MeasureFilter(2, "s", op, 1),)).validate(SCHEMA)


def test_from_wire_defaults_and_explicit_dataset():
    query = from_wire({"rows": ["r"], "measures": [{"alias": "s", "agg": "sum", "field": "v"}]}, dataset="t")
    assert query == PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("s", "sum", "v"),))
    assert query.totals == "visible" and query.window == Window(0, None) and query.expanded is None
    assert from_wire({"dataset": "d", "rows": [], "measures": [{"alias": "n", "agg": "count"}]}).dataset == "d"
    explicit = from_wire({"dataset": "d", "rows": [], "measures": [{"alias": "n", "agg": "count"}]}, dataset="x")
    assert explicit.dataset == "x" and explicit.measures[0].field is None


def test_from_wire_maps_every_section():
    query = from_wire({
        "dataset": "d", "rows": ["r", "c"], "measures": [{"alias": "s", "agg": "sum", "field": "v"}],
        "filters": [{"field": "v", "op": "in", "value": [1, 2]}, {"field": "r", "op": "is_null"}],
        "measureFilters": [{"level": "2", "measure": "s", "op": ">", "value": 3}],
        "sort": [{"field": "r", "desc": 1}, {"field": "c"}], "totals": "all_data",
        "expanded": [["a"], ["b"]], "window": {"offset": "4", "limit": 9},
    })
    assert query.filters == (Predicate("v", "in", (1, 2)), Predicate("r", "is_null", None))
    assert query.measure_filters == (MeasureFilter(2, "s", ">", 3),)
    assert query.sort == (SortKey("r", True), SortKey("c", False))
    assert query.expanded == frozenset({("a",), ("b",)})
    assert query.window == Window(4, 9) and query.totals == "all_data"
    query.validate(SCHEMA)


@pytest.mark.parametrize("wire", [
    {"rows": ["r"], "measures": [{"alias": "s", "agg": "sum"}]},  # no dataset given anywhere
    {"dataset": "d", "measures": []},
    {"dataset": "d", "rows": ["r"], "measures": [{"agg": "sum"}]},
    {"dataset": "d", "rows": ["r"], "measures": [], "measureFilters": [{"level": "x", "measure": "s", "op": ">", "value": 1}]},
    {"dataset": "d", "rows": ["r"], "measures": [], "window": {"offset": "abc"}},
    {"dataset": "d", "rows": 5, "measures": []},
])
def test_from_wire_structural_errors_are_typed(wire):
    with pytest.raises(IRValidationError, match="^malformed query: "):
        from_wire(wire)


@pytest.mark.parametrize("flt, message", [
    (Predicate("v", "contains", "a"), "contains requires a string column, not integer 'v'."),
    (Predicate("r", "starts_with", 1), "starts_with requires a string value."),
    (Predicate("v", "between", 1), "between requires [low, high]."),
    (Predicate("v", "between", (1, 2, 3)), "between requires [low, high]."),
    (PredicateGroup("xor", (Predicate("v", ">", 1),)), "unsupported filter group operator 'xor'."),
    (PredicateGroup("and", ()), "a filter group needs at least one condition."),
    (PredicateGroup("or", (Predicate("zz", ">", 1),)), "filter references unknown column 'zz'."),
    ("v > 1", "filter 'v > 1' is not a predicate or group."),
])
def test_filter_rules(flt, message):
    bad(message, filters=(flt,))


def test_from_wire_parses_nested_groups_and_case_flags():
    query = from_wire({"dataset": "d", "rows": ["r"], "measures": [{"alias": "s", "agg": "sum", "field": "v"}],
                       "filters": [{"op": "OR", "conditions": [
                           {"field": "r", "op": "contains", "value": "a", "caseSensitive": False},
                           {"op": "and", "conditions": [{"field": "v", "op": "between", "value": [1, 3]}]}]}]})
    assert query.filters == (PredicateGroup("or", (
        Predicate("r", "contains", "a", False),
        PredicateGroup("and", (Predicate("v", "between", (1, 3), True),)))),)
    query.validate(SCHEMA)


@pytest.mark.parametrize("measure, message", [
    (MeasureRef("p", "percentile", "v"), "percentile requires q."),
    (MeasureRef("p", "sum", "v", q=0.5), "sum does not take q."),
    (MeasureRef("p", "percentile", "v", q=1.5), "percentile q must be within [0, 1], not 1.5."),
    (MeasureRef("p", "percentile", "v", q=-0.1), "percentile q must be within [0, 1], not -0.1."),
    (MeasureRef("p", "percentile", "v", q="0.5"), "percentile q must be within [0, 1], not '0.5'."),
    (MeasureRef("p", "weighted_avg", "v"), "weighted_avg requires a weight field."),
    (MeasureRef("p", "avg", "v", weight="w"), "avg does not take a weight field."),
    (MeasureRef("p", "weighted_avg", "v", weight="zz"), "weight of measure 'p' references unknown column 'zz'."),
    (MeasureRef("p", "weighted_avg", "v", weight="r"), "weight column 'r' must be numeric."),
])
def test_parametric_measure_rules(measure, message):
    bad(message, measures=(measure,))


def test_parametric_measures_valid_and_from_wire():
    q(measures=(MeasureRef("p", "percentile", "v", q=0), MeasureRef("p1", "percentile", "f", q=1),
                MeasureRef("w", "weighted_avg", "v", weight="f"))).validate(SCHEMA)
    query = from_wire({"dataset": "d", "rows": [], "measures": [
        {"alias": "p", "agg": "percentile", "field": "v", "q": 0.9},
        {"alias": "w", "agg": "weighted_avg", "field": "v", "weight": "w"}]})
    assert query.measures == (MeasureRef("p", "percentile", "v", None, 0.9), MeasureRef("w", "weighted_avg", "v", "w"))


@pytest.mark.parametrize("kw, message", [
    (dict(sort=(SortKey("r"), SortKey("r", True))), "each sort field may appear once."),
    (dict(columns=("c", "c")), "column dimensions must be unique."),
    (dict(columns=("zz",)), "column dimension references unknown column 'zz'."),
    (dict(columns=("r",)), "a dimension cannot be both a row and a column: ['r']"),
    (dict(column_members=(("x",),)), "column members require column dimensions."),
    (dict(columns=("flag",), column_members=(("x", "y"),)),
     "column member ('x', 'y') does not match the columns ['flag']."),
    (dict(columns=("flag",), column_members=(["x"],)), "column member ['x'] does not match the columns ['flag']."),
    (dict(columns=("flag",), column_members=((True,), (True,))), "column members must be unique."),
])
def test_column_pivot_rules(kw, message):  # REQ-PIV-001
    bad(message, **kw)


def test_column_pivot_cells_and_wire():
    query = q(columns=("flag",), column_members=((True,), (None,)))
    query.validate(SCHEMA)
    assert query.value_columns() == ["s", "__c0__s", "__c1__s"]
    assert [c[:2] for c in query.cells()] == [(0, (True,)), (1, (None,))]
    wire = from_wire({"dataset": "d", "rows": ["r"], "measures": [{"alias": "s", "agg": "sum", "field": "v"}],
                      "columns": ["flag"], "columnMembers": [[True], [None]]})
    assert (wire.columns, wire.column_members) == (("flag",), ((True,), (None,)))


D = DerivedMeasure


@pytest.mark.parametrize("derived, message", [  # REQ-AGG-007
    (D("s", "rank", "s"), "derived measure alias 's' collides with another name."),
    (D("r", "rank", "s"), "derived measure alias 'r' collides with another name."),
    (D("d", "median_rank", "s"), "unknown derived measure function 'median_rank'."),
    (D("d", "rank", "zz"), "derived measure 'd' references unknown measure 'zz'."),
    (D("d", "ratio", "s"), "ratio requires a denominator."),
    (D("d", "rank", "s", of2="s"), "rank does not take a denominator."),
    (D("d", "ratio", "s", of2="zz"), "derived measure 'd' references unknown measure 'zz'."),
    (D("d", "rank", "s", null_value=0.0), "null_value applies to a ratio and must be a number."),
    (D("d", "ratio", "s", of2="s", null_value="0"), "null_value applies to a ratio and must be a number."),
    (D("d", "rank", "s", frame=(None, 0)), "rank does not take a frame."),
    (D("d", "moving_avg", "s", frame=(1,)), "frame (1,) must be (start, end) row offsets."),
    (D("d", "moving_avg", "s", frame=(0.5, 1)), "frame (0.5, 1) must be (start, end) row offsets."),
    (D("d", "moving_avg", "s", frame=(2, 1)), "frame start 2 is after its end 1."),
])
def test_derived_measure_rules(derived, message):
    bad(message, derived=(derived,))


def test_derived_measures_valid_and_from_wire():
    query = q(derived=(D("a", "ratio", "s", "s", null_value=0), D("b", "moving_avg", "s", frame=(-2, None)),
                       D("cu", "cumulative", "s", frame=(None, None)), D("e", "dense_rank", "s", desc=False)))
    query.validate(SCHEMA)
    assert query.value_columns() == ["s", "a", "b", "cu", "e"] and query.aggregate_columns() == ["s"]
    wire = from_wire({"dataset": "d", "rows": ["r"], "measures": [{"alias": "s", "agg": "sum", "field": "v"}],
                      "derived": [{"alias": "m", "func": "moving_avg", "of": "s", "frame": [-1, 1]},
                                  {"alias": "x", "func": "ratio", "of": "s", "of2": "s", "nullValue": 0}]})
    assert wire.derived == (D("m", "moving_avg", "s", None, True, (-1, 1)), D("x", "ratio", "s", "s", True, None, 0))
