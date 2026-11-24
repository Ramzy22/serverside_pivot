"""Legacy spec -> core IR mapping and the run_hierarchy_view contract (M2)."""

from __future__ import annotations

from dataclasses import replace

import ibis
import pyarrow as pa
import pytest

from pivot_engine.core.ir import Expr, MeasureFilter, MeasureRef, Predicate, PredicateGroup, SortKey, Window
from pivot_engine.transport.core_bridge import CoreUnsupported, core_hierarchy_view, spec_to_query

from .contract_snapshots import assert_contract
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
    (dict(rows=["r"], columns=["zz"], pivot_config=PivotConfig(enabled=True)), "unknown column fields ['zz']"),
    (dict(custom_dimensions=[{"id": "x", "rules": [{"label": "L", "field": "nope", "op": "=", "value": 1}]}]),
     "filter on unknown field 'nope'"),
    (dict(having=[{"field": "region", "op": "=", "value": "x"}]), "having on a non-measure field"),
    (dict(group_allow_unbalanced=True, rows=["r", "v"]), "ragged hierarchy over fields of different types"),
    (dict(measure_axis=MeasureAxisConfig(placement="diagonal")), "measure axis on diagonal"),
    (dict(rows=[], measure_axis=MeasureAxisConfig(placement="columns", suppress_zero_members=True)),
     "measure axis zero suppression without row levels"),
    (dict(rows=["r"], columns=["c"], pivot_config=PivotConfig(enabled=True, top_n=5)), "column pivot options"),
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
    (dict(group_allow_unbalanced=True, rows=["r", "c"], measure_axis=MeasureAxisConfig(
        placement="rows", label_field="M", value_field="V", members=[{"measureAlias": "v_sum", "label": "S"}])),
     "ragged hierarchy with a measure axis"),
    (dict(sort=[{"field": "r", "order": "asc", "collation": "nocase"}]), "sort keys ['collation']"),
    (dict(sort=[{"field": "r", "nulls": "middle"}]), "nulls 'middle'"),
    (dict(sort=[{"field": "r", "semanticType": "calendar"}]), "semantic sort 'calendar'"),  # tenor: REQ-SORT-005
    (dict(filters=[{"field": "r", "op": "=", "value": "A", "regex": True}]), "filter keys ['regex']"),
])
def test_unsupported_features_are_named(kw, reason):
    with pytest.raises(CoreUnsupported) as info:
        spec_to_query(spec(**kw), True, SCHEMA)
    assert reason in info.value.reasons


def test_all_reasons_are_reported_together():
    with pytest.raises(CoreUnsupported) as info:
        spec_to_query(spec(rows=["r", "v"], columns=["c"], group_allow_unbalanced=True), True, SCHEMA)
    assert info.value.reasons == ("column pivot without pivot_config", "ragged hierarchy over fields of different types")


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


def _drill(payload):
    import asyncio

    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("d", pa.table({"region": ["North", "North", "South"], "v": [1, 2, 3]}))
    request = TanStackRequest(operation=TanStackOperation.GET_DATA, table="d", columns=[{"id": "v_sum", "aggregationField": "v",
                              "aggregationFn": "sum"}], filters={}, sorting=[], grouping=["region"], aggregations=[])
    result = asyncio.run(adapter.handle_drill_through(request, payload))
    return result.get("total_rows", result.get("totalRows"))


def test_drill_honours_row_fields():  # REQ-DET-001 (legacy ignored rowFields: F-DRILL-FIELDS)
    assert _drill({"rowPath": "North", "rowFields": ["region"]}) == 2
    assert _drill({"row_path": "North", "row_fields": ["region"]}) == 2


@pytest.mark.parametrize("totals", [False, True])
@pytest.mark.parametrize("window", [(0, 99), (0, 2), (3, 6), (5, 9)])
def test_tabular_subtotals_post_order_matches_legacy(totals, window, request):
    """REQ-LAYOUT-001: tabular subtotals are the post-order of the fully expanded tree (grand total last)."""
    import asyncio
    import os

    import pyarrow as pa

    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    def run():
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


    core = run()
    assert_contract(request, core)


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_member_key_sort_is_core_served_and_matches_legacy(order, request):
    """REQ-SORT-003: a sortKeyField orders members by MIN(key) (tenor labels by days), served by the core."""
    import asyncio
    import os

    import pyarrow as pa

    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    pillars = [("1D", 1), ("1M", 30), ("10M", 304), ("1Y", 365), ("10Y", 3650), ("100Y", 36500)]

    def run():
        adapter = create_tanstack_adapter(backend_uri=":memory:")
        adapter.controller.load_data_from_arrow("t", pa.Table.from_pydict({
            "pillar": [p for p, _ in pillars] * 2, "__sortkey__pillar": [d for _, d in pillars] * 2,
            "desk": ["X"] * len(pillars) + ["Y"] * len(pillars), "pv": [float(i) for i in range(2 * len(pillars))]}))
        request = TanStackRequest(
            operation=TanStackOperation.GET_DATA, table="t",
            columns=[{"id": "desk"}, {"id": "pillar"}, {"id": "pv_sum", "aggregationField": "pv", "aggregationFn": "sum"}],
            filters={}, sorting=[{"id": "pillar", "desc": order == "desc", "sortKeyField": "__sortkey__pillar"}],
            grouping=["desk", "pillar"], aggregations=[], pagination={"pageIndex": 0, "pageSize": 100}, totals=False)
        response = asyncio.run(adapter.handle_virtual_scroll_request(request, 0, 99, expanded_paths=[["X"], ["Y"]]))
        return [(r.get("_path"), r.get("pv_sum")) for r in response.data]

    core = run()
    labels = [path.split("|||")[1] for path, _ in core if path and "|||" in path and path.startswith("X")]
    expected = [p for p, _ in pillars]
    assert labels == (expected if order == "asc" else expected[::-1])
    assert_contract(request, core)


CUSTOM_DIMENSION = {
    "id": "Sales Band", "order": "creation", "fallbackLabel": "Small",
    "rules": [
        {"label": "Huge", "condition": {"op": "AND", "clauses": [{"field": "sales", "operator": "gte", "value": 100}]}},
        {"label": "Mid", "field": "sales", "operator": "between", "value": [10, 99.5]},
        {"label": "West-ish", "condition": {"op": "OR", "clauses": [
            {"field": "region", "operator": "in", "values": "West, North"},
            {"field": "region", "operator": "is null"}]}},
    ],
}


@pytest.mark.parametrize("rows, sort", [
    (["__custom_category__sales_band"], []),
    (["__custom_category__sales_band", "region"], []),
    (["region", "__custom_category__sales_band"], [{"id": "__custom_category__sales_band", "desc": True,
                                                    "sortKeyField": "__sortkey____custom_category__sales_band"}]),
])
def test_custom_dimension_is_core_served_and_matches_legacy(rows, sort, request):
    """REQ-DIM-001: first matching rule's label, else the fallback; creation order via the hidden key."""
    import asyncio
    import os

    import pyarrow as pa

    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    def run():
        adapter = create_tanstack_adapter(backend_uri=":memory:")
        # A NULL region only where region is not a row level: legacy drops a NULL leaf and renders
        # a NULL root as the grand total (F-NULL-LEAF / F-NULL-GT); the core keeps both.
        null_region = None if "region" not in rows else "Central"
        adapter.controller.load_data_from_arrow("t", pa.Table.from_pydict({
            "region": ["West", "East", "North", null_region, "East", "South"],
            "sales": [150.0, 50.0, 5.0, 1.0, 99.5, 120.0]}))
        request = TanStackRequest(
            operation=TanStackOperation.GET_DATA, table="t",
            columns=[*({"id": r} for r in rows), {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
            filters={}, sorting=sort, grouping=rows, aggregations=[], pagination={"pageIndex": 0, "pageSize": 100},
            totals=True, custom_dimensions=[CUSTOM_DIMENSION])
        response = asyncio.run(adapter.handle_virtual_scroll_request(request, 0, 99, expanded_paths=[["__ALL__"]]))
        return response.total_rows, [(r.get("_path"), r.get("sales_sum")) for r in response.data]

    core = run()
    assert_contract(request, core)


def test_custom_dimension_keeps_a_null_member_below_a_category():
    """REQ-NULL-001 under a category: the NULL region is its own leaf (legacy drops it, F-NULL-LEAF)."""
    import pyarrow as pa

    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"region": ["West", None], "sales": [150.0, 1.0]}))
    s = spec(rows=["__custom_category__sales_band", "region"], measures=[Measure(field="sales", agg="sum", alias="s")],
             custom_dimensions=[CUSTOM_DIMENSION])
    view = core_hierarchy_view(lambda _n: con.table("t"), s, [["__ALL__"]], 0, 99)
    assert [(r["_path"], r["s"]) for r in view["rows"]] == [
        ("Huge", 150.0), ("Huge|||West", 150.0), ("West-ish", 1.0), ("West-ish|||", 1.0)]
    assert view["rows"][-1]["region"] is None


@pytest.mark.parametrize("field", ["pnl_sum", "__RowTotal__pnl_sum", "Q1_pnl_sum", "Q2_pnl_sum", "Q9_pnl_sum"])
@pytest.mark.parametrize("desc", [False, True])
def test_column_pivot_sorts_are_core_served_and_match_legacy(field, desc, request):
    """REQ-SORT-004: row total and pivot-cell sorts (NULL last); a plain alias or an absent member is ignored."""
    import os

    import pyarrow as pa

    from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState
    from pivot_engine.tanstack_adapter import create_tanstack_adapter

    data = {"region": ["A", "A", "B", "B", "C", "C", "D"], "q": ["Q1", "Q2", "Q1", "Q2", "Q1", "Q2", "Q2"],
            "pnl": [5.0, 1.0, 1.0, 9.0, 3.0, 3.0, 4.0]}

    def run():
        adapter = create_tanstack_adapter(backend_uri=":memory:")
        adapter.controller.load_data_from_arrow("t", pa.Table.from_pydict(data))
        service = PivotRuntimeService(adapter_getter=lambda: adapter)
        state = PivotViewState(row_fields=["region"], col_fields=["q"], val_configs=[{"field": "pnl", "agg": "sum"}],
                               sorting=[{"id": field, "desc": desc}], expanded={}, show_col_totals=False,
                               show_row_totals=True)
        ctx = PivotRequestContext.from_frontend(table="t", trigger_prop="p.runtimeRequest", viewport={
            "start": 0, "end": 50, "window_seq": 1, "state_epoch": 1, "session_id": "s", "client_instance": "c",
            "intent": "structural"})
        return [(r.get("region"), r.get("Q1_pnl_sum"), r.get("Q2_pnl_sum")) for r in (service.process(state, ctx).data or [])]

    core = run()
    assert_contract(request, core)


@pytest.mark.parametrize("totals", [False, True])
@pytest.mark.parametrize("window", [(0, 99), (0, 1), (2, 4), (4, 9)])
@pytest.mark.parametrize("sorting", [[], [{"id": "sales_sum", "desc": True}], [{"id": "country", "desc": True}]])
def test_flat_tabular_layout_is_core_served_and_matches_legacy(totals, window, sorting, request):
    """REQ-LAYOUT-002: tabular without subtotals = the leaves in sort order, grand total last."""
    import asyncio
    import os

    import pyarrow as pa

    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    def run():
        adapter = create_tanstack_adapter(backend_uri=":memory:")
        adapter.controller.load_data_from_arrow("s", pa.Table.from_pydict({
            "region": ["North", "North", "South", "East", "East"], "country": ["USA", "Canada", "Brazil", "Japan", "Korea"],
            "sales": [1.0, 2.0, 4.0, 8.0, 3.0]}))
        request = TanStackRequest(
            operation=TanStackOperation.GET_DATA, table="s",
            columns=[{"id": "region"}, {"id": "country"},
                     {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
            filters={}, sorting=sorting, grouping=["region", "country"], aggregations=[],
            pagination={"pageIndex": 0, "pageSize": 100}, totals=totals, include_subtotals=False,
            layout_mode="tabular", show_subtotal_footers=False)
        response = asyncio.run(adapter.handle_virtual_scroll_request(request, window[0], window[1], expanded_paths=[]))
        return response.total_rows, [(r.get("_path"), bool(r.get("_isTotal")), r.get("sales_sum"), r.get("depth"),
                                      bool(r.get("_has_children"))) for r in response.data]

    core = run()
    assert_contract(request, core)


@pytest.mark.parametrize("subtotals", [True, False])
@pytest.mark.parametrize("rows", [["region", "Measure Name", "desk"], ["Measure Name", "region"], ["region", "Measure Name"]])
@pytest.mark.parametrize("agg", ["sum", "avg", "min", "count"])
@pytest.mark.parametrize("variant", ["plain", "suppress_empty", "reversed_order", "label_desc"])
def test_measure_axis_is_core_served_and_matches_legacy(rows, agg, variant, subtotals, request):
    """REQ-DIM-002: one common aggregation over plain measures = legacy's unpivot, exactly.

    Every level carries a sort entry: with only the label sorted, legacy leaves the other
    levels in engine order (F-AXIS-ORDER).  Legacy's flat layout reports the first member's
    grand total (F-AXIS-FLAT-GT); the core's equals its (and legacy's) hierarchy grand total.
    """
    import asyncio
    import os

    import pyarrow as pa

    from pivot_engine.runtime.service import PivotRuntimeService
    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    val_configs = [{"field": "sales", "agg": agg, "alias": f"sales_{agg}"}, {"field": "cost", "agg": agg, "alias": f"cost_{agg}"}]
    orders = (1, 0) if variant == "reversed_order" else (0, 1)
    sort = [{"id": "Measure Name", "desc": variant == "label_desc", "sortKeyField": "__sortkey__Measure Name"},
            *({"id": r, "desc": False} for r in rows if r != "Measure Name")]

    def run(include_subtotals):
        adapter = create_tanstack_adapter(backend_uri=":memory:")
        adapter.controller.load_data_from_arrow("t", pa.Table.from_pydict({
            "region": ["North", "North", "South", "South", "East"], "desk": ["Rates", "Credit", "Rates", "Credit", "Rates"],
            "sales": [100.0, 50.0, 80.0, 20.0, 7.0], "cost": [40.0, None, 30.0, 5.0, None]}))
        request = TanStackRequest(
            operation=TanStackOperation.GET_DATA, table="t",
            columns=PivotRuntimeService._build_request_columns([r for r in rows if r != "Measure Name"], [], val_configs),
            filters={}, custom_dimensions=[],
            measure_axis={"placement": "rows", "labelField": "Measure Name", "valueField": "Amount",
                          "suppressEmptyMembers": variant == "suppress_empty",
                          "members": [{"measureAlias": f"sales_{agg}", "label": "Sales", "order": orders[0]},
                                      {"measureAlias": f"cost_{agg}", "label": "Cost", "order": orders[1]}]},
            sorting=sort, grouping=rows, aggregations=[], pagination={"pageIndex": 0, "pageSize": 100},
            totals=True, row_totals=False, include_subtotals=include_subtotals, version=2)
        response = asyncio.run(adapter.handle_virtual_scroll_request(request, 0, 99, expanded_paths=[["__ALL__"]]))
        return response.total_rows, [(r.get("_path"), r.get("Amount"), r.get("depth")) for r in response.data]

    def split(view):
        total_rows, data = view
        return total_rows, [r for r in data if r[0] != "__grand_total__"], [r for r in data if r[0] == "__grand_total__"]

    core = split(run(subtotals))
    assert_contract(request, core)


def test_first_last_without_an_order_field_is_a_typed_error():
    """REQ-AGG-008: the core never returns an arbitrary record; with an order field it is served."""
    import pyarrow as pa

    from pivot_engine.core.ir import IRValidationError
    from pivot_engine.transport.core_bridge import core_hierarchy_view
    from pivot_engine.types.pivot_spec import Measure, PivotSpec

    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"g": ["a", "a", "b"], "v": [1, 2, 3], "ts": [2, 1, 5]}))

    def view(order_by):
        spec = PivotSpec(table="t", rows=["g"], measures=[Measure(field="v", agg="last", alias="v_last", order_by=order_by)],
                         filters=[], totals=True)
        return core_hierarchy_view(con.table, spec, [["__ALL__"]], 0, 10)

    with pytest.raises(IRValidationError, match="needs an order field"):
        view(None)
    rows = view("ts")["rows"]  # a: ts 2 -> 1; b: 3; the grand total: ts 5 -> 3
    assert [(r.get("g"), bool(r.get("_isTotal")), r["v_last"]) for r in rows] == [
        ("a", False, 1), ("b", False, 3), (None, True, 3)]


def test_core_pivot_table_contract():
    """The run_pivot contract from the core: leaves in the flat order (NULLS LAST), the grand total last
    with totals, limit 0 = no limit, offset/limit windows, legacy cell names, one row without row fields."""
    import pyarrow as pa

    from pivot_engine.transport.core_bridge import core_pivot_table
    from pivot_engine.types.pivot_spec import Measure, PivotConfig, PivotSpec

    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"region": ["N", "N", "S", None, "E"], "q": ["Q1", "Q2", "Q1", "Q2", "Q1"],
                                    "v": [1.0, 2.0, 4.0, 8.0, None]}))
    m = [Measure(field="v", agg="sum", alias="v_sum")]

    def table(**kw):
        spec = PivotSpec(table="t", rows=kw.pop("rows", ["region"]), measures=kw.pop("measures", m), filters=[], **kw)
        return core_pivot_table(con.table, spec)

    leaves = [("E", None), ("N", 3.0), ("S", 4.0), (None, 8.0)]
    assert [(r["region"], r["v_sum"]) for r in table(limit=0).to_pylist()] == leaves
    assert [(r["region"], r["v_sum"]) for r in table(totals=True).to_pylist()] == leaves + [(None, 15.0)]
    assert [(r["region"], r["v_sum"]) for r in table(limit=2, offset=1).to_pylist()] == leaves[1:3]
    assert table(rows=[], totals=True).to_pylist() == [{"v_sum": 15.0}]  # one row (legacy duplicated it)
    pivot = table(columns=["q"], pivot_config=PivotConfig(enabled=True, include_totals_column=True))
    assert pivot.column_names == ["region", "Q1_v_sum", "Q2_v_sum", "__RowTotal__v_sum"]
    assert pivot.to_pylist()[1] == {"region": "N", "Q1_v_sum": 1.0, "Q2_v_sum": 2.0, "__RowTotal__v_sum": 3.0}
    assert str(pivot.schema.field("Q1_v_sum").type) == "double"


@pytest.mark.parametrize("rows", [["region", "desk", "Measure Name"], ["Measure Name", "region"]])
def test_per_member_measure_axis_is_exact_below_the_label_and_blank_above(rows):
    """REQ-DIM-003 (decided): each member keeps its own aggregation; no value above the label."""
    import asyncio
    import os

    import pyarrow as pa

    from pivot_engine.runtime.service import PivotRuntimeService
    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter

    val_configs = [{"field": "sales", "agg": "sum", "alias": "sales_sum"},
                   {"field": "peak", "agg": "max", "alias": "peak_max"},
                   {"field": "price", "agg": "weighted_avg", "alias": "px", "weightField": "qty"}]
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("t", pa.Table.from_pydict({
        "region": ["North", "North", "South", "South"], "desk": ["Rates", "Credit", "Rates", "Credit"],
        "sales": [100.0, 50.0, 80.0, 20.0], "peak": [7.0, 5.0, 9.0, 4.0],
        "price": [10.0, 20.0, 30.0, 40.0], "qty": [1.0, 3.0, 1.0, 1.0]}))
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA, table="t",
        columns=PivotRuntimeService._build_request_columns([r for r in rows if r != "Measure Name"], [], val_configs),
        filters={}, custom_dimensions=[],
        measure_axis={"placement": "rows", "labelField": "Measure Name", "valueField": "Amount", "members": [
            {"measureAlias": "sales_sum", "label": "Sales", "order": 0},
            {"measureAlias": "peak_max", "label": "Peak", "order": 1},
            {"measureAlias": "px", "label": "Price", "order": 2}]},
        sorting=[{"id": "Measure Name", "desc": False, "sortKeyField": "__sortkey__Measure Name"},
                 *({"id": r, "desc": False} for r in rows if r != "Measure Name")],
        grouping=rows, aggregations=[], pagination={"pageIndex": 0, "pageSize": 100},
        totals=True, row_totals=False, include_subtotals=True, version=2)
    response = asyncio.run(adapter.handle_virtual_scroll_request(request, 0, 99, expanded_paths=[["__ALL__"]]))
    got = {r["_path"]: r.get("Amount") for r in response.data}
    if rows[0] == "region":
        assert got["North"] is None and got["North|||Rates"] is None and got["__grand_total__"] is None
        assert (got["North|||Rates|||Sales"], got["North|||Rates|||Peak"], got["North|||Rates|||Price"]) == (100.0, 7.0, 10.0)
        assert (got["South|||Credit|||Sales"], got["South|||Credit|||Peak"]) == (20.0, 4.0)
    else:
        assert got["__grand_total__"] is None
        assert (got["Sales"], got["Peak"]) == (250.0, 9.0)
        assert got["Price"] == pytest.approx((10 * 1 + 20 * 3 + 30 * 1 + 40 * 1) / 6)
        assert (got["Sales|||North"], got["Peak|||South"]) == (150.0, 9.0)
        assert got["Price|||North"] == pytest.approx((10 * 1 + 20 * 3) / 4)


def test_tenor_sort_and_supplied_member_keys_map_to_the_core():  # REQ-SORT-005, REQ-SORT-003
    schema = ibis.schema({"r": "string", "c": "string", "k": "int64", "v": "int64"})

    def mapped(sort, rows=("r", "c")):
        spec = PivotSpec(table="t", rows=list(rows), measures=[Measure(field="v", agg="sum", alias="v_sum")],
                         sort=sort)
        return spec_to_query(spec, [["__ALL__"]], schema).sort

    assert mapped([{"field": "r", "order": "desc", "semanticType": "tenor", "sortKeyField": "__sortkey__r"}]) == (
        SortKey("r", True, semantic="tenor"),)
    assert mapped([{"field": "c", "sortType": "curve_pillar_tenor"}]) == (SortKey("c", semantic="tenor"),)
    # a supplied key column is the order (it wins over the semantic type)
    assert mapped([{"field": "r", "semanticType": "tenor", "sortType": "curve_pillar_tenor", "sortKeyField": "k"}]) == (
        SortKey("r", by="k"),)
    # a member order for a dimension that is not a row level orders nothing
    assert mapped([{"field": "c", "sortKeyField": "k"}], rows=("r",)) == ()


def test_full_rows_equal_to_the_rows_is_a_no_op():
    schema = ibis.schema({"r": "string", "c": "string", "v": "int64"})
    spec = PivotSpec(table="t", rows=["r", "c"], full_rows=["r", "c"],
                     measures=[Measure(field="v", agg="sum", alias="v_sum")])
    assert spec_to_query(spec, [["__ALL__"]], schema).rows == ("r", "c")


def test_ragged_hierarchy_view_maps_each_member_to_its_own_field():  # REQ-DIM-005
    con = ibis.duckdb.connect()
    con.create_table("g", pa.table({"l1": ["Root", "Root", "Root", "Direct", ""], "l2": ["Desk", None, "Desk", None, "Orphan"],
                                    "h": ["A", "B", "C", None, "D"], "v": [100.0, -250.0, 50.0, 30.0, 15.0]}))
    s = PivotSpec(table="g", rows=["l1", "l2", "h"], measures=[Measure(field="v", agg="sum", alias="v_sum")],
                  group_allow_unbalanced=True, totals=True)
    view = core_hierarchy_view(con.table, s, [["__ALL__"]], 0, 100, include_grand_total_row=True)
    got = {r["_path"]: (r["v_sum"], r["depth"], r.get("_pathFields"), r.get("_has_children")) for r in view["rows"]}
    assert got["Root"] == (-100.0, 0, ["l1"], True)
    assert got["Root|||B"] == (-250.0, 1, ["l1", "h"], False)
    assert got["Root|||Desk|||C"] == (50.0, 2, ["l1", "l2", "h"], False)
    assert got["Orphan|||D"] == (15.0, 1, ["l2", "h"], False)
    assert got["Direct"] == (30.0, 0, ["l1"], False)
    row = next(r for r in view["rows"] if r["_path"] == "Root|||B")
    assert (row["l1"], row["h"], "l2" in row) == ("Root", "B", False)
    with pytest.raises(CoreUnsupported, match="flat layout"):
        spec_to_query(s, True, con.table("g").schema(), subtotals="none")


def test_formulas_map_to_core_measures_over_the_node_measures():  # REQ-AGG-010
    schema = ibis.schema({"r": "string", "c": "string", "s": "float64", "k": "float64"})

    def formulas(*exprs, sort=None):
        spec = PivotSpec(table="t", rows=["r"], measures=[Measure(field="s", agg="sum", alias="s_sum"),
                                                          Measure(field="k", agg="sum", alias="k_sum")],
                         formulas=[{"id": f"f{i}", "expr": e} for i, e in enumerate(exprs)], sort=sort or [])
        query = spec_to_query(spec, [["__ALL__"]], schema)
        return {m.alias: m.expr for m in query.measures if m.agg == "formula"}, query.sort

    ref = lambda a: Expr("ref", (a,))  # noqa: E731
    got, _ = formulas("(s_sum - k_sum) / s_sum", "round(f0 * -100, 2) + abs(-k_sum) ** 2", "[S_SUM] + int(k_sum)")
    assert got["f0"] == Expr("/", (Expr("-", (ref("s_sum"), ref("k_sum"))), ref("s_sum")))
    assert got["f1"] == Expr("+", (Expr("round", (Expr("*", (ref("f0"), Expr("num", (-100,)))), 2)),
                                   Expr("pow", (Expr("abs", (Expr("neg", (ref("k_sum"),)),)), 2))))
    assert got["f2"] == Expr("+", (ref("s_sum"), Expr("trunc", (ref("k_sum"),))))
    # unknown names, bad syntax, unknown functions and reference cycles are NULL (the adapter's None)
    got, _ = formulas("nope + 1", "s_sum +", "sqrt(s_sum)", "f4 + 1", "f3 * 2", "f5")
    assert got["f0"] == Expr("+", (Expr("null"), Expr("num", (1,))))
    assert got["f1"] == got["f2"] == got["f3"] == got["f4"] == got["f5"] == Expr("null")
    # a formula can be sorted on, at the row level and in the row-total column
    _, sort = formulas("s_sum / k_sum", sort=[{"field": "f0", "order": "desc"}])
    assert sort == (SortKey("f0", True),)
    with pytest.raises(CoreUnsupported, match="computed exponent"):
        formulas("s_sum ** k_sum")


def test_formula_totals_are_the_formula_of_the_total_measures():  # REQ-AGG-010
    con = ibis.duckdb.connect()
    con.create_table("m", pa.table({"region": ["North", "North", "South", "South"],
                                    "sales": [100.0, 100.0, 80.0, 120.0], "cost": [60.0, 80.0, 20.0, 90.0]}))
    s = PivotSpec(table="m", rows=["region"], totals=True,
                  measures=[Measure(field="sales", agg="sum", alias="sales_sum"),
                            Measure(field="cost", agg="sum", alias="cost_sum")],
                  formulas=[{"id": "margin", "expr": "(sales_sum - cost_sum) / sales_sum"}],
                  sort=[{"field": "margin", "order": "desc"}])
    view = core_hierarchy_view(con.table, s, [["__ALL__"]], 0, 100, include_grand_total_row=True)
    assert [(r["_path"], r["margin"]) for r in view["rows"]] == [
        ("South", pytest.approx(90 / 200)), ("North", pytest.approx(60 / 200)),
        ("__grand_total__", pytest.approx(150 / 400))]
    assert view["grand_total_formula_source_rows"] == []


def test_sorts_naming_no_column_of_the_view_are_ignored():  # REQ-SORT-004
    schema = ibis.schema({"r": "string", "c": "string", "s": "float64"})

    def sort(entries, columns=()):
        spec = PivotSpec(table="t", rows=["r"], columns=list(columns), pivot_config=PivotConfig(enabled=True),
                         measures=[Measure(field="s", agg="sum", alias="revenue")], sort=entries)
        return spec_to_query(spec, [["__ALL__"]], schema).sort

    assert sort([{"field": "s", "order": "desc"}]) == ()  # a source field that is not a row
    assert sort([{"field": "s_sum", "order": "desc"}], columns=["c"]) == ()  # field_agg, not the alias
    assert sort([{"field": "revenue", "order": "desc"}]) == (SortKey("revenue", True),)
    with pytest.raises(CoreUnsupported, match="sort on 'nope'"):
        sort([{"field": "nope"}])


def test_measure_axis_on_columns_and_zero_suppression_map_to_the_core():  # REQ-DIM-006
    schema = ibis.schema({"r": "string", "c": "string", "s": "float64", "k": "float64"})
    measures = [Measure(field="s", agg="sum", alias="s_sum"), Measure(field="k", agg="sum", alias="k_sum")]

    def query(placement, rows=("r",), columns=(), zero=False):
        axis = MeasureAxisConfig(placement=placement, label_field="M", value_field="Amount",
                                 suppress_zero_members=zero)
        spec = PivotSpec(table="t", rows=list(rows), columns=list(columns), measures=measures, measure_axis=axis,
                         pivot_config=PivotConfig(enabled=True))
        return spec_to_query(spec, [["__ALL__"]], schema)

    q = query("columns", columns=["c"], zero=True)
    assert (q.rows, q.columns) == (("r",), ("c", "M"))  # the label is the innermost column unless placed
    assert q.measure_filters == (MeasureFilter(1, "Amount", "!=", 0, cells=True),)
    assert query("columns", columns=["M", "c"]).columns == ("M", "c")
    q = query("rows", rows=("r", "M"), zero=True)
    assert (q.rows, q.columns, q.measure_filters) == (("r", "M"), (), (MeasureFilter(2, "Amount", "!=", 0),))


def test_measure_axis_label_columns_follow_the_member_order():  # REQ-DIM-006
    con = ibis.duckdb.connect()
    con.create_table("ax", pa.table({"r": ["A", "A", "B"], "zeta": [1.0, 0.0, 0.0], "alpha": [2.0, 0.0, 0.0]}))
    axis = MeasureAxisConfig(placement="columns", label_field="M", value_field="Amount", suppress_zero_members=True,
                             members=[{"sourceField": "zeta", "label": "zeta", "order": 0},
                                      {"sourceField": "alpha", "label": "alpha", "order": 1}])
    s = PivotSpec(table="ax", rows=["r"], measures=[Measure(field="zeta", agg="sum", alias="zeta_sum"),
                                                     Measure(field="alpha", agg="sum", alias="alpha_sum")],
                  measure_axis=axis, pivot_config=PivotConfig(enabled=True), totals=True)
    view = core_hierarchy_view(con.table, s, [["__ALL__"]], 0, 100, include_grand_total_row=True)
    rows = {r["_path"]: {k: v for k, v in r.items() if k.endswith("_Amount")} for r in view["rows"]}
    assert list(rows["A"]) == ["zeta_Amount", "alpha_Amount"]  # member order, not alphabetical
    assert rows["A"] == {"zeta_Amount": 1.0, "alpha_Amount": 2.0}
    assert "B" not in rows  # every cell of B is zero: suppressed
    assert rows["__grand_total__"] == {"zeta_Amount": 1.0, "alpha_Amount": 2.0}


def test_formula_axis_members_evaluate_on_the_member_records():  # REQ-DIM-007
    con = ibis.duckdb.connect()
    con.create_table("fx", pa.table({"cat": ["A", "A", "B"], "sales": [10.0, 20.0, 5.0], "cost": [4.0, 8.0, 2.0]}))
    axis = MeasureAxisConfig(placement="rows", label_field="M", value_field="Amount",
                             members=[{"measureAlias": "sales_sum", "label": "Sales", "order": 0},
                                      {"measureAlias": "ratio", "label": "Ratio", "order": 1},
                                      {"measureAlias": "margin", "label": "Margin", "order": 2}])
    s = PivotSpec(table="fx", rows=["cat", "M"], totals=True, measure_axis=axis, measures=[
        Measure(field="sales", agg="sum", alias="sales_sum"), Measure(field="cost", agg="sum", alias="cost_sum"),
        Measure(field=None, agg="formula", alias="margin", expression="sales_sum - cost_sum"),
        Measure(field=None, agg="formula", alias="ratio", expression="margin / sales_sum")])
    view = core_hierarchy_view(con.table, s, [["__ALL__"]], 0, 100, include_grand_total_row=True)
    got = {r["_path"]: r["Amount"] for r in view["rows"]}
    assert got["A|||Sales"] == 30.0 and got["A|||Margin"] == 18.0 and got["A|||Ratio"] == pytest.approx(0.6)
    assert got["B|||Margin"] == 3.0 and got["B|||Ratio"] == pytest.approx(0.6)
    assert got["A"] is None and got["__grand_total__"] is None  # above the label (REQ-DIM-003)
    assert [r["_path"] for r in view["rows"]][:4] == ["A", "A|||Sales", "A|||Ratio", "A|||Margin"]


def test_nulls_first_and_keyed_sorts_with_a_sort_type_map_to_the_core():  # REQ-SORT-006, REQ-SORT-003
    q = spec_to_query(spec(sort=[{"field": "r", "nulls": "first", "order": "desc"}]), [], SCHEMA)
    assert q.sort == (SortKey("r", True, nulls_first=True),)
    q = spec_to_query(spec(sort=[{"field": "r", "sortKeyField": "v", "sortType": "tenor", "absoluteSort": True}]),
                      [], SCHEMA)
    assert q.sort == (SortKey("r", False, False, "v"),)  # the supplied key is the order


def test_an_and_group_of_measure_conditions_is_one_measure_filter_each():  # REQ-TOT-007
    group = {"op": "AND", "conditions": [{"field": "v_sum", "op": ">", "value": 1},
                                         {"field": "v_sum", "op": "<", "value": 9}]}
    q = spec_to_query(spec(filters=[group]), [], SCHEMA)
    assert q.measure_filters == (MeasureFilter(2, "v_sum", ">", 1.0), MeasureFilter(2, "v_sum", "<", 9.0))
    single_or = {"op": "OR", "conditions": [{"field": "v_sum", "op": ">", "value": 1}]}
    assert spec_to_query(spec(filters=[single_or]), [], SCHEMA).measure_filters == (MeasureFilter(2, "v_sum", ">", 1.0),)
    with pytest.raises(CoreUnsupported, match="compound measure filter"):
        spec_to_query(spec(filters=[{"op": "OR", "conditions": group["conditions"]}]), [], SCHEMA)


def test_ragged_helper_measures_never_reach_a_pivoted_response():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"r": ["A", "A", "B"], "s": ["x", None, "y"], "c": ["p", "q", "p"], "v": [1, 5, 7]}))
    ragged = spec(rows=["r", "s"], columns=["c"], pivot_config=PivotConfig(enabled=True, include_totals_column=True),
                  group_allow_unbalanced=True)
    view = core_hierarchy_view(con.table, ragged, [["__ALL__"]], 0, 100, include_grand_total_row=True)
    keys = {k for row in view["rows"] for k in row}
    assert not [k for k in keys if "__base_" in k], keys
    assert {"p_v_sum", "q_v_sum"} <= keys


def test_next_cursor_continues_the_same_order_until_the_last_page():
    import asyncio

    from pivot_engine.scalable_pivot_controller import ScalablePivotController

    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow("t", pa.table({"r": [f"k{i:02d}" for i in range(7)], "v": list(range(7))}))
    base = dict(rows=["r"], sort=[{"field": "r", "order": "desc"}])
    whole = asyncio.run(controller.run_pivot_async(spec(**base, limit=0), return_format="dict"))
    pages, cursor = [], None
    while True:
        page = asyncio.run(controller.run_pivot_async(spec(**base, limit=3, cursor=cursor), return_format="dict"))
        pages += page["rows"]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert pages == whole["rows"] and len(pages) == 7
    with pytest.raises(CoreUnsupported):
        asyncio.run(controller.run_pivot_async(spec(**base, limit=3, cursor={"r": "k03"}), return_format="dict"))


def test_a_text_filter_condition_is_a_typed_refusal():
    from pivot_engine.transport.core_bridge import core_record_filter

    with pytest.raises(CoreUnsupported) as info:
        core_record_filter(_con().table("t"), "region = 'East'")
    assert info.value.reasons == ("filter of type str",)
