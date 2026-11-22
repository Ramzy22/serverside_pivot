"""Legacy spec -> core IR mapping and the run_hierarchy_view contract (M2)."""

from __future__ import annotations

from dataclasses import replace

import ibis
import pyarrow as pa
import pytest

from pivot_engine.core.ir import MeasureFilter, MeasureRef, Predicate, PredicateGroup, SortKey, Window
from pivot_engine.transport.core_bridge import CoreUnsupported, core_hierarchy_view, spec_to_query
from pivot_engine.types.pivot_spec import Measure, MeasureAxisConfig, PivotConfig, PivotSpec


def _con():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({
        "r": ["A", "A", None, "B"], "c": ["x", "y", "y", None], "v": [1, 5, 2, 7], "n": [1.5, 2.5, None, 4.0],
    }))
    return con


SCHEMA = _con().table("t").schema()


def spec(**kw):
    base = dict(table="t", rows=["r", "c"], measures=[Measure(field="v", agg="sum", alias="v_sum")])
    return PivotSpec(**{**base, **kw})


def test_basic_mapping_and_window():
    q = spec_to_query(spec(), [["__ALL__"]], SCHEMA, start_row=2, end_row=4)
    assert q.dataset == "t" and q.rows == ("r", "c")
    assert q.measures == (MeasureRef("v_sum", "sum", "v"),)
    assert q.expanded is None
    assert q.window == Window(3, 3)  # +1: the core orders the grand total first


@pytest.mark.parametrize("agg, expected", [
    ("mean", "avg"), ("distinct_count", "count_distinct"), ("wavg", "weighted_avg"), ("MAX", "max"),
])
def test_aggregation_names(agg, expected):
    m = Measure(field="v", agg=agg, alias="m", weighted_field="n" if expected == "weighted_avg" else None)
    assert spec_to_query(spec(measures=[m]), True, SCHEMA).measures[0].agg == expected


def test_parametric_measures_and_count_star():
    q = spec_to_query(spec(measures=[Measure(field="v", agg="percentile", alias="p", percentile=0.9),
                                     Measure(field="v", agg="weighted_avg", alias="w", weighted_field="n"),
                                     Measure(field="*", agg="count", alias="k")]), True, SCHEMA)
    assert q.measures == (MeasureRef("p", "percentile", "v", None, 0.9), MeasureRef("w", "weighted_avg", "v", "n"),
                          MeasureRef("k", "count", None))


def test_dimension_filters_groups_and_coercion():
    q = spec_to_query(spec(filters=[
        {"field": "v", "op": ">", "value": "2"},
        {"field": "r", "op": "in", "value": ["A", None]},
        {"op": "OR", "conditions": [{"field": "r", "op": "contains", "value": "a", "caseSensitive": False},
                                    {"field": "n", "op": "between", "value": ["1", "3.5"]}]},
        {"field": "c", "op": "not in", "value": "x"},
    ]), True, SCHEMA)
    assert q.filters == (
        Predicate("v", ">", 2, False),
        Predicate("r", "in", ("A", None), False),
        PredicateGroup("or", (Predicate("r", "contains", "a", False), Predicate("n", "between", (1.0, 3.5), False))),
        Predicate("c", "not_in", ("x",), False),
    )


def test_measure_filter_applies_at_the_deepest_level():  # REQ-TOT-007
    q = spec_to_query(spec(filters=[{"field": "v_sum", "op": ">", "value": 3}]), True, SCHEMA)
    assert q.measure_filters == (MeasureFilter(2, "v_sum", ">", 3.0),)
    assert q.filters == ()


def test_sort_on_dimensions_maps_and_others_are_unsupported():
    q = spec_to_query(spec(sort=[{"field": "r", "order": "desc"}, {"field": "c", "order": "asc"}]), True, SCHEMA)
    assert q.sort == (SortKey("r", True), SortKey("c", False))


def test_expansion_paths_are_coerced_and_filtered():
    q = spec_to_query(spec(rows=["v", "r"]), [["5"], ["1", "A"], []], SCHEMA)
    assert q.expanded == frozenset({(5,)})
    assert spec_to_query(spec(), [], SCHEMA).expanded == frozenset()
    assert spec_to_query(spec(), [["A"], ["__ALL__"]], SCHEMA).expanded is None


@pytest.mark.parametrize("kw, reason", [
    (dict(columns=["c"]), "column pivot"),
    (dict(custom_dimensions=[{"id": "x"}]), "custom dimensions"),
    (dict(having=[{"field": "v_sum"}]), "having"),
    (dict(group_allow_unbalanced=True), "unbalanced hierarchy"),
    (dict(measure_axis=MeasureAxisConfig(placement="rows")), "measure axis"),
    (dict(pivot_config=PivotConfig(enabled=True, top_n=5)), "column pivot options"),
    (dict(sort=[{"field": "r", "order": "asc", "sortKeyField": "__sortkey__r"}]), "custom member order"),
    (dict(measures=[Measure(field="v", agg="mode", alias="m")]), "aggregation 'mode'"),
    (dict(measures=[Measure(field="v", agg="sum", alias="m", window_func="rank")]), "window measure"),
    (dict(measures=[Measure(field="v", agg="sum", alias="m", null_handling="as_zero")]), "null handling 'as_zero'"),
    (dict(filters=[{"field": "zz", "op": "=", "value": 1}]), "filter on unknown field 'zz'"),
    (dict(filters=[{"field": "v", "op": "=", "value": "abc"}]), "value 'abc' is not a integer"),
    (dict(filters=[{"op": "OR", "conditions": [{"field": "v_sum", "op": ">", "value": 1},
                                               {"field": "r", "op": "=", "value": "A"}]}]), "compound measure filter"),
    (dict(rows=["r", "zz"]), "unknown row fields ['zz']"),
])
def test_unsupported_features_are_named(kw, reason):
    with pytest.raises(CoreUnsupported) as info:
        spec_to_query(spec(**kw), True, SCHEMA)
    assert reason in info.value.reasons


def test_all_reasons_are_reported_together():
    with pytest.raises(CoreUnsupported) as info:
        spec_to_query(spec(columns=["c"], custom_dimensions=[{"id": "x"}]), True, SCHEMA)
    assert info.value.reasons == ("column pivot", "custom dimensions")


def test_hierarchy_view_contract():
    con = _con()
    view = core_hierarchy_view(con.table, spec(), [["__ALL__"]], 0, 100, include_grand_total_row=True)
    paths = [(r["_path"], r["v_sum"], r["depth"], r["_pathFields"]) for r in view["rows"]]
    assert paths == [
        ("A", 6, 0, ["r"]), ("A|||x", 1, 1, ["r", "c"]), ("A|||y", 5, 1, ["r", "c"]),
        ("B", 7, 0, ["r"]), ("B|||", 7, 1, ["r", "c"]),
        ("", 2, 0, ["r"]), ("|||y", 2, 1, ["r", "c"]),
    ]
    assert view["rows"][5]["r"] is None and view["rows"][5]["_key"] != view["rows"][6]["_key"]
    gt = view["grand_total_row"]
    assert (gt["_id"], gt["_isTotal"], gt["_path"], gt["v_sum"], gt["r"]) == ("Grand Total", True, "__grand_total__", 15,
                                                                               None)
    assert view["total_rows"] == 8  # 7 nodes + the grand total
    assert view["profile"] == {"engine": "core"}


def test_hierarchy_view_windows_and_collapsed_paths():
    con = _con()
    window = core_hierarchy_view(con.table, spec(), [["__ALL__"]], 1, 2)
    assert [r["_path"] for r in window["rows"]] == ["A|||x", "A|||y"]
    assert window["grand_total_row"] is None
    collapsed = core_hierarchy_view(con.table, spec(), [["A"]], 0, 100)
    assert [r["_path"] for r in collapsed["rows"]] == ["A", "A|||x", "A|||y", "B", ""]
    assert collapsed["total_rows"] == 6


def test_total_rows_cache_is_keyed_by_query_meaning():
    con = _con()
    cache: dict = {}
    core_hierarchy_view(con.table, spec(), [["__ALL__"]], 0, 1, total_cache=cache)
    core_hierarchy_view(con.table, spec(), [["__ALL__"]], 5, 9, total_cache=cache)  # same meaning, other window
    core_hierarchy_view(con.table, spec(), [["A"]], 0, 1, total_cache=cache)
    assert sorted(cache.values()) == [6, 8]


def test_measure_filter_view_has_visible_totals():
    con = _con()
    s = spec(filters=[{"field": "v_sum", "op": ">", "value": 3}])
    view = core_hierarchy_view(con.table, s, [["__ALL__"]], 0, 100, include_grand_total_row=True)
    assert [(r["_path"], r["v_sum"]) for r in view["rows"]] == [("A", 5), ("A|||y", 5), ("B", 7), ("B|||", 7)]
    assert view["grand_total_row"]["v_sum"] == 12


def test_spec_offset_is_rejected_not_reinterpreted():
    with pytest.raises(CoreUnsupported, match="spec offset"):
        spec_to_query(replace(spec(), offset=10), True, SCHEMA)
