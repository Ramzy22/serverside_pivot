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
    (dict(columns=["c"]), "column pivot without pivot_config"),
    (dict(rows=["r"], columns=["c"], pivot_config=PivotConfig(enabled=True), sort=[{"field": "v_sum"}]),
     "sort by measure with column pivot"),
    (dict(rows=["r"], columns=["c"], pivot_config=PivotConfig(enabled=True),
          filters=[{"field": "v_sum", "op": ">", "value": 1}]), "measure filter with column pivot"),
    (dict(rows=["r"], columns=["zz"], pivot_config=PivotConfig(enabled=True)), "unknown column fields ['zz']"),
    (dict(custom_dimensions=[{"id": "x"}]), "custom dimensions"),
    (dict(having=[{"field": "v_sum"}]), "having"),
    (dict(group_allow_unbalanced=True), "unbalanced hierarchy"),
    (dict(measure_axis=MeasureAxisConfig(placement="rows")), "measure axis"),
    (dict(rows=["r"], columns=["c"], pivot_config=PivotConfig(enabled=True, top_n=5)), "column pivot options"),
    (dict(sort=[{"field": "r", "order": "asc", "sortKeyField": "__sortkey__r"}]), "custom member order"),
    (dict(measures=[Measure(field="v", agg="mode", alias="m")]), "aggregation 'mode'"),
    (dict(measures=[Measure(field="v", agg="sum", alias="m", window_func="ntile")]), "window function 'ntile'"),
    (dict(measures=[Measure(field="v", agg="sum", alias="m", window_func="rank", window_group_by=["r"])]),
     "window partition or order override"),
    (dict(measures=[Measure(field="v", agg="sum", alias="m", ratio_numerator="v_sum", ratio_denominator="zz")]),
     "ratio of a derived or unknown measure"),
    (dict(rows=["r"], columns=["c"], pivot_config=PivotConfig(enabled=True),
          measures=[Measure(field="v", agg="sum", alias="m", window_func="rank")]), "derived measure with column pivot"),
    (dict(measures=[Measure(field="v", agg="sum", alias="m", null_handling="as_zero")]), "null handling 'as_zero'"),
    (dict(filters=[{"field": "zz", "op": "=", "value": 1}]), "filter on unknown field 'zz'"),
    (dict(filters=[{"field": "v", "op": "=", "value": "abc"}]), "value 'abc' is not a integer"),
    (dict(filters=[{"op": "OR", "conditions": [{"field": "v_sum", "op": ">", "value": 1},
                                               {"field": "r", "op": "=", "value": "A"}]}]), "compound measure filter"),
    (dict(rows=["r", "zz"]), "unknown row fields ['zz']"),
    (dict(full_rows=["r", "c", "hidden"]), "spec field 'full_rows'"),
    (dict(unbalanced_values=["x"]), "spec field 'unbalanced_values'"),
    (dict(sort=[{"field": "r", "order": "asc", "collation": "nocase"}]), "sort keys ['collation']"),
    (dict(sort=[{"field": "r", "nulls": "first"}]), "nulls 'first'"),
    (dict(sort=[{"field": "r", "semanticType": "tenor"}]), "semantic sort 'tenor'"),
    (dict(filters=[{"field": "r", "op": "=", "value": "A", "regex": True}]), "filter keys ['regex']"),
])
def test_unsupported_features_are_named(kw, reason):
    with pytest.raises(CoreUnsupported) as info:
        spec_to_query(spec(**kw), True, SCHEMA)
    assert reason in info.value.reasons


def test_all_reasons_are_reported_together():
    with pytest.raises(CoreUnsupported) as info:
        spec_to_query(spec(columns=["c"], custom_dimensions=[{"id": "x"}]), True, SCHEMA)
    assert info.value.reasons == ("column pivot without pivot_config", "custom dimensions")


def test_hierarchy_view_contract():
    con = _con()
    view = core_hierarchy_view(con.table, spec(totals=True), [["__ALL__"]], 0, 100, include_grand_total_row=True)
    paths = [(r["_path"], r["v_sum"], r["depth"], r.get("_pathFields")) for r in view["rows"]]
    assert paths == [
        ("A", 6, 0, ["r"]), ("A|||x", 1, 1, ["r", "c"]), ("A|||y", 5, 1, ["r", "c"]),
        ("B", 7, 0, ["r"]), ("B|||", 7, 1, ["r", "c"]),
        ("", 2, 0, ["r"]), ("|||y", 2, 1, ["r", "c"]),
        ("__grand_total__", 15, 0, None),  # legacy places the grand total last in the flattened list
    ]
    assert view["rows"][5]["r"] is None and view["rows"][5]["_rowKey"] != view["rows"][6]["_rowKey"]
    gt = view["grand_total_row"]
    assert (gt["_id"], gt["_isTotal"], gt["_path"], gt["v_sum"], gt["r"]) == ("Grand Total", True, "__grand_total__", 15,
                                                                               None)
    assert view["total_rows"] == 8  # 7 nodes + the grand total
    assert [r["_path"] for r in view["grand_total_formula_source_rows"]] == ["A", "B", ""]
    assert view["profile"]["controller"]["engine"] == "core" and view["profile"]["controller"]["operation"] == "hierarchy_view"


def test_without_totals_there_is_no_grand_total():
    con = _con()
    view = core_hierarchy_view(con.table, spec(), [["__ALL__"]], 0, 100, include_grand_total_row=True)
    assert [r["_path"] for r in view["rows"]][-1] == "|||y"
    assert view["total_rows"] == 7 and view["grand_total_row"] is None


def test_hierarchy_view_windows_and_collapsed_paths():
    con = _con()
    window = core_hierarchy_view(con.table, spec(totals=True), [["__ALL__"]], 1, 2)
    assert [r["_path"] for r in window["rows"]] == ["A|||x", "A|||y"]
    assert window["grand_total_row"] is None and window["grand_total_formula_source_rows"] is None
    tail = core_hierarchy_view(con.table, spec(totals=True), [["__ALL__"]], 6, 9)
    assert [r["_path"] for r in tail["rows"]] == ["|||y", "__grand_total__"]
    collapsed = core_hierarchy_view(con.table, spec(totals=True), [["A"]], 0, 100)
    assert [r["_path"] for r in collapsed["rows"]] == ["A", "A|||x", "A|||y", "B", "", "__grand_total__"]
    assert collapsed["total_rows"] == 6


def test_column_pivot_cells_use_legacy_keys():  # REQ-PIV-001
    con = _con()
    s = spec(rows=["r"], columns=["c"], pivot_config=PivotConfig(enabled=True), totals=True)
    view = core_hierarchy_view(con.table, s, [], 0, 100, include_grand_total_row=True)
    a = view["rows"][0]
    assert {k: v for k, v in a.items() if not k.startswith("_") and k not in {"depth", "r"}} == {
        "x_v_sum": 1, "y_v_sum": 5, "None_v_sum": None}
    assert view["grand_total_row"]["None_v_sum"] == 7 and "v_sum" not in view["grand_total_row"]
    windowed = replace(s, pivot_config=PivotConfig(enabled=True, materialized_column_values=["y", "zz"],
                                                   include_totals_column=True))
    row = core_hierarchy_view(con.table, windowed, [], 0, 0)["rows"][0]
    assert {k: v for k, v in row.items() if k.endswith("v_sum")} == {"y_v_sum": 5, "__RowTotal__v_sum": 6}


def test_ambiguous_column_keys_are_unsupported():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"r": ["A", "A"], "c": ["None", None], "v": [1, 2]}))
    s = spec(rows=["r"], columns=["c"], pivot_config=PivotConfig(enabled=True))
    with pytest.raises(CoreUnsupported, match="ambiguous pivot column key 'None'"):
        core_hierarchy_view(con.table, s, [], 0, 10)


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
    view = core_hierarchy_view(con.table, replace(s, totals=True), [["__ALL__"]], 0, 100, include_grand_total_row=True)
    assert [(r["_path"], r["v_sum"]) for r in view["rows"]] == [("A", 5), ("A|||y", 5), ("B", 7), ("B|||", 7),
                                                                ("__grand_total__", 12)]
    assert view["grand_total_row"]["v_sum"] == 12


def test_spec_offset_is_rejected_not_reinterpreted():
    with pytest.raises(CoreUnsupported, match="spec offset"):
        spec_to_query(replace(spec(), offset=10), True, SCHEMA)


def test_window_and_ratio_measures_map_to_derived():  # REQ-AGG-005, REQ-AGG-007
    con = _con()
    s = spec(rows=["r"], totals=True, measures=[
        Measure(field="v", agg="sum", alias="v_sum"), Measure(field="v", agg="count", alias="n"),
        Measure(field="v", agg="sum", alias="rk", window_func="rank"),
        Measure(field="v", agg="sum", alias="mv", window_func="moving_avg", window_frame_start=-1),
        Measure(field="v", agg="sum", alias="avg", ratio_numerator="v_sum", ratio_denominator="n"),
    ])
    q = spec_to_query(s, True, SCHEMA)
    assert [m.alias for m in q.measures] == ["v_sum", "n", "__base_rk", "__base_mv"]
    assert [(d.alias, d.func, d.of, d.of2, d.frame) for d in q.derived] == [
        ("rk", "rank", "__base_rk", None, None), ("mv", "moving_avg", "__base_mv", None, (-1, 0)),
        ("avg", "ratio", "v_sum", "n", None)]
    rows = core_hierarchy_view(con.table, s, [], 0, 10)["rows"]
    assert [(r["_path"], r["rk"], r["mv"], r["avg"]) for r in rows] == [
        ("A", 2, 6.0, 3.0), ("B", 1, 6.5, 7.0), ("", 3, 4.5, 2.0), ("__grand_total__", 1, 15.0, 3.75)]
    assert not any(k.startswith("__base_") for r in rows for k in r)


def test_absolute_sort_maps_and_orders_by_magnitude():  # REQ-SORT-002
    q = spec_to_query(spec(sort=[{"field": "v_sum", "order": "desc", "sortType": "absolute"}]), True, SCHEMA)
    assert q.sort == (SortKey("v_sum", True, True),)
    q = spec_to_query(spec(sort=[{"field": "v_sum", "order": "desc", "absoluteSort": True}]), True, SCHEMA)
    assert q.sort == (SortKey("v_sum", True, True),)


def _drill(mode, payload, monkeypatch):
    import asyncio

    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    monkeypatch.setenv("PIVOT_CORE", mode)
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("d", pa.table({"region": ["North", "North", "South"], "v": [1, 2, 3]}))
    request = TanStackRequest(operation=TanStackOperation.GET_DATA, table="d", columns=[{"id": "v_sum", "aggregationField": "v",
                              "aggregationFn": "sum"}], filters={}, sorting=[], grouping=["region"], aggregations=[])
    result = asyncio.run(adapter.handle_drill_through(request, payload))
    return result.get("total_rows", result.get("totalRows"))


@pytest.mark.parametrize("mode", [
    pytest.param("off", marks=pytest.mark.xfail(strict=True, reason="F-DRILL-FIELDS: legacy ignores rowFields")),
    "on",
])
def test_legacy_drill_honours_row_fields(mode, monkeypatch):  # REQ-DET-001
    assert _drill(mode, {"rowPath": "North", "rowFields": ["region"]}, monkeypatch) == 2
    assert _drill(mode, {"row_path": "North", "row_fields": ["region"]}, monkeypatch) == 2


@pytest.mark.parametrize("totals", [False, True])
@pytest.mark.parametrize("window", [(0, 99), (0, 2), (3, 6), (5, 9)])
def test_tabular_subtotals_post_order_matches_legacy(totals, window):
    """REQ-LAYOUT-001: tabular subtotals are the post-order of the fully expanded tree (grand total last)."""
    import asyncio
    import os

    import pyarrow as pa

    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    def run(mode):
        previous = os.environ.get("PIVOT_CORE")
        os.environ["PIVOT_CORE"] = mode
        try:
            adapter = create_tanstack_adapter(backend_uri=":memory:")
            adapter.controller.load_data_from_arrow("s", pa.Table.from_pydict({
                "region": ["North", "North", "South", "East"], "country": ["USA", "Canada", "Brazil", "Japan"],
                "sales": [1.0, 2.0, 4.0, 8.0]}))
            request = TanStackRequest(
                operation=TanStackOperation.GET_DATA, table="s",
                columns=[{"id": "region"}, {"id": "country"},
                         {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
                filters={}, sorting=[], grouping=["region", "country"], aggregations=[],
                pagination={"pageIndex": 0, "pageSize": 100}, totals=totals, include_subtotals=True,
                layout_mode="tabular", show_subtotal_footers=True)
            response = asyncio.run(adapter.handle_virtual_scroll_request(
                request, window[0], window[1], expanded_paths=[["North"]]))  # expansion is ignored
            return response.total_rows, [(r.get("_path"), bool(r.get("_isTabularSubtotal")), bool(r.get("_isTotal")),
                                          r.get("sales_sum")) for r in response.data]
        finally:
            if previous is None:
                os.environ.pop("PIVOT_CORE", None)
            else:
                os.environ["PIVOT_CORE"] = previous

    from pivot_engine.runtime import core_shadow

    served = core_shadow.STATS["core_served_hierarchy"]
    core = run("on")
    assert core_shadow.STATS["core_served_hierarchy"] > served, "the core must serve the tabular subtotal layout"
    assert core == run("off")
