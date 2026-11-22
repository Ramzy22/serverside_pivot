"""IR validation rules, one by one (REQ-AGG-001: typed errors, never silent)."""

from __future__ import annotations

import re

import pytest

from pivot_engine.core.ir import (
    PREDICATE_OPS, IRValidationError, MeasureFilter, MeasureRef, PivotQuery, Predicate, SortKey, Window, from_wire,
)

SCHEMA = {"r": "string", "c": "string", "v": "integer", "f": "floating", "flag": "boolean"}
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
    (dict(sort=(SortKey("v"),)), "sort key 'v' is not a row dimension."),
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
    dict(totals="all_data"),
    dict(window=Window(0, 0)),
    dict(window=Window(7, None)),
    dict(expanded=frozenset({("a",), (None,)})),
    dict(filters=tuple(Predicate("v", op, [1] if op in {"in", "not_in"} else 1) for op in sorted(PREDICATE_OPS))),
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
