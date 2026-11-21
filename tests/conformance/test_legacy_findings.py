"""Audit findings on the legacy spec API (PivotController / REST /pivot).

Each test asserts the *correct* answer and is a strict xfail: when the core
replaces the legacy path (M2) it XPASSes, which fails the suite until the
marker is removed.  Finding ids are listed in docs/semantics/FINDINGS.md.
"""

from __future__ import annotations

import contextlib
import io

import pyarrow as pa
import pytest


def _legacy(finding: str):
    return pytest.mark.xfail(reason=f"legacy finding {finding}", strict=True)


def _controller(table=None):
    from pivot_engine.controller import PivotController

    c = PivotController(planner_name="ibis", backend_uri=":memory:")
    c.load_data_from_arrow("t", table or pa.table({"g": ["A", "B", "B"], "q": [3, 3, 4], "t": ["a", "b", "b"]}))
    return c


def _run(c, spec):
    with contextlib.redirect_stdout(io.StringIO()):
        return c.run_pivot(spec, return_format="dict")


BASE = {"table": "t", "rows": ["g"], "columns": [], "measures": [{"field": "q", "agg": "sum", "alias": "q"}],
        "filters": [], "totals": True}


@_legacy("F-TOT-CACHE")
def test_grand_total_survives_a_cache_hit():  # REQ-TOT-006
    c = _controller()
    first, second = _run(c, dict(BASE)), _run(c, dict(BASE))
    assert first["rows"] == second["rows"] == [["A", 3], ["B", 7], [None, 10]]


@_legacy("F-TOT-PAGE")
def test_grand_total_survives_page_two():  # REQ-TOT-006
    assert _run(_controller(), {**BASE, "limit": 1, "offset": 1})["rows"] == [["B", 7], [None, 10]]


@_legacy("F-TOT-MF-PARENT")
def test_measure_filter_grand_total_is_visible_total():  # REQ-TOT-003
    assert _run(_controller(), {**BASE, "filters": [{"field": "q", "op": ">", "value": 5}]})["rows"] == [["B", 7], [None, 7]]


@_legacy("F-HAVING-IGNORED")
def test_having_is_applied():  # REQ-TOT-001
    rows = _run(_controller(), {**BASE, "having": [{"field": "q", "op": ">", "value": 5}]})["rows"]
    assert ["A", 3] not in rows


@_legacy("F-NULL-GT")
def test_null_group_and_grand_total_are_both_present():  # REQ-NULL-002
    rows = _run(_controller(pa.table({"g": [None, "A", "B"], "q": [5, 3, 7]})), dict(BASE))["rows"]
    assert sorted(map(tuple, rows), key=repr) == sorted([("A", 3), ("B", 7), (None, 5), (None, 15)], key=repr)


@_legacy("F-RANK")
def test_rank_is_one_based_by_measure():  # REQ-AGG-005
    c = _controller(pa.table({"g": ["A", "B", "C"], "q": [30, 10, 20]}))
    rows = _run(c, {"table": "t", "rows": ["g"], "measures": BASE["measures"] + [
        {"field": "q", "agg": "sum", "alias": "rk", "window_func": "rank"}]})["rows"]
    assert {r[0]: r[2] for r in rows} == {"A": 1, "C": 2, "B": 3}


@_legacy("F-MOVING-AVG")
def test_moving_average_is_computed():  # REQ-AGG-005
    c = _controller(pa.table({"d": ["2024-01", "2024-02", "2024-03"], "q": [3, 3, 4]}))
    rows = _run(c, {"table": "t", "rows": ["d"], "measures": BASE["measures"] + [
        {"field": "q", "agg": "sum", "alias": "ma", "window_func": "moving_avg",
         "window_frame_start": -1, "window_frame_end": 0}]})["rows"]
    assert [r[2] for r in rows] == [3.0, 3.0, 3.5]


@_legacy("F-ROLLUP-CRASH")
@pytest.mark.parametrize("mode", ["rollup", "cube"])
def test_rollup_and_cube_execute(mode):
    c = _controller(pa.table({"g": ["A", "B"], "h": ["x", "y"], "q": [3, 4]}))
    result = _run(c, {"table": "t", "rows": ["g", "h"], "measures": BASE["measures"], "grouping_config": {"mode": mode}})
    assert [None, None, 7] in result["rows"]


@_legacy("F-PIVOT-MEDIAN")
def test_pivot_keeps_every_measure():
    c = _controller(pa.table({"g": ["A", "B"], "h": ["x", "y"], "q": [3, 4]}))
    result = _run(c, {"table": "t", "rows": ["g"], "columns": ["h"], "pivot_config": {"enabled": True},
                      "measures": [{"field": "q", "agg": "median", "alias": "med"}, {"field": "q", "agg": "sum", "alias": "s"}]})
    assert any("med" in str(col) for col in result["columns"])


@_legacy("F-PIVOT-DELIM")
def test_pivot_column_keys_with_delimiters_do_not_collide():  # REQ-ID-001
    c = _controller(pa.table({"g": ["A", "A"], "c1": ["x|y", "x"], "c2": ["z", "y|z"], "q": [1, 2]}))
    result = _run(c, {"table": "t", "rows": ["g"], "columns": ["c1", "c2"], "pivot_config": {"enabled": True},
                      "measures": BASE["measures"]})
    assert len(result["columns"]) == 3


@_legacy("F-TOT-FALLBACK")
def test_fallback_hierarchy_parents_are_aggregated_from_records():  # REQ-TOT-003
    from pivot_engine.tanstack_adapter import create_tanstack_adapter
    from pivot_engine.types.pivot_spec import PivotSpec

    a = create_tanstack_adapter(":memory:")
    a.controller.load_data_from_arrow("t", pa.table({"r": ["N", "N", "S"], "c": ["a", "b", "a"], "v": [10, 50, 7], "u": ["k", "k", "k"]}))
    spec = PivotSpec.from_dict({"table": "t", "rows": ["r", "c"], "measures": [
        {"field": "v", "agg": "avg", "alias": "v_avg"}, {"field": "v", "agg": "max", "alias": "v_max"},
        {"field": "u", "agg": "count_distinct", "alias": "u_cd"}]})
    with contextlib.redirect_stdout(io.StringIO()):
        res = a.controller.run_virtual_scroll_hierarchical(spec, 0, 50, [["__ALL__"]])
    rows = res.get("rows") if isinstance(res, dict) else res
    parent = next(r for r in rows if r.get("r") == "N" and r.get("c") is None)
    assert (parent["v_avg"], parent["v_max"], parent["u_cd"]) == (30.0, 50, 1)
