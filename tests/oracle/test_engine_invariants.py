"""Engine invariants that need no oracle, plus the security property (REQ-PAGE-001, REQ-SEC-003)."""

from __future__ import annotations

import json
import math
import pathlib

import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from pivot_engine.runtime.dataset_authorization import RowScopeGrant

from .engine_harness import build_adapter, by_path, row_path, run_view
from .oracle import Measure, Query, in_scope, pivot

_XFAIL = json.loads((pathlib.Path(__file__).parents[1] / "conformance" / "legacy_xfail.json").read_text())
SETTINGS = settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])

DIM = st.sampled_from(["A", "a", "B", "", "x|y", "x_y", None])
RECORDS = st.lists(
    st.fixed_dictionaries({"r": DIM, "c": DIM, "v": st.one_of(st.none(), st.integers(-50, 50)), "tenant": DIM}),
    min_size=1, max_size=14,
)


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)


def _legacy(name):
    return pytest.mark.xfail(name in _XFAIL, reason="legacy divergence; tests/conformance/legacy_xfail.json", strict=False)


@_legacy("test_pages_concatenate_to_unpaged")
@SETTINGS
@given(records=RECORDS, page=st.integers(1, 5))
def test_pages_concatenate_to_unpaged(records, page):  # REQ-PAGE-001
    adapter = build_adapter(records)
    full = [row_path(r) for r in run_view(adapter, rows=["r", "c"], agg="sum")]
    paged = []
    # Viewport windows are inclusive: [start, end] (runtime contract).
    for start in range(0, len(full) + page, page):
        paged.extend(row_path(r) for r in run_view(adapter, rows=["r", "c"], agg="sum", start=start, end=start + page - 1))
    # The grand total may be repeated per window; compare the visible tree only.
    assert [p for p in paged if p != ()] == [p for p in full if p != ()]


@SETTINGS
@given(records=RECORDS)
def test_repeated_request_is_deterministic(records):
    adapter = build_adapter(records)
    first = run_view(adapter, rows=["r", "c"], agg="sum")
    second = run_view(adapter, rows=["r", "c"], agg="sum")
    assert [row_path(r) for r in first] == [row_path(r) for r in second]
    assert by_path(first, "sum") == by_path(second, "sum")


# Grouping keys exclude NULL here: NULL-group rendering is a separate legacy
# finding (F-NULL-GT); this property isolates row scoping.
SCOPE_RECORDS = st.lists(
    st.fixed_dictionaries({"r": st.sampled_from(["A", "a", "B", "", "x|y"]), "c": DIM,
                           "v": st.one_of(st.none(), st.integers(-50, 50)), "tenant": DIM}),
    min_size=1, max_size=14,
)


@SETTINGS
# Grants are non-null scalars: the row policy rejects NULL claims
# (dataset_authorization._scope_values), so NULL-tenant rows are never visible.
@given(records=SCOPE_RECORDS, allowed=st.sets(st.sampled_from(["A", "a", "B", "", "x|y", "x_y"]), min_size=1, max_size=3))
def test_scoped_totals_equal_oracle_over_scoped_records(records, allowed):  # REQ-SEC-001/003
    """Security property: whatever the data and grant, the engine sees exactly the oracle's E0."""
    grant = RowScopeGrant(
        table="t", subject="u", field_values={"tenant": tuple(allowed)},
        scope_fingerprint=repr(sorted(map(repr, allowed))),
    )
    adapter = build_adapter(records)
    rows = run_view(adapter, rows=["r"], agg="sum", row_scope=grant)
    scope = {"tenant": tuple(allowed)}
    visible = [x for x in records if in_scope(x, scope)]
    expected_total = pivot(Query(rows=["r"], measures=[Measure("v_sum", "sum", "v")]), visible)[0].values["v_sum"]
    totals = [r.get("v_sum") for r in rows if row_path(r) == ()]
    if visible:
        assert totals and _same(totals[0], expected_total), (totals, expected_total)
    # Whatever the grouping does, no foreign value may contribute: the sum
    # of all scoped contributions bounds every visible row.
    allowed_sum = sum(abs(x["v"]) for x in visible if x["v"] is not None)
    assert all(r.get("v_sum") is None or abs(float(r["v_sum"])) <= allowed_sum + 1e-9 for r in rows)


TOT_RECORDS = st.lists(
    st.fixed_dictionaries({"r": st.sampled_from(["A", "B"]), "c": st.sampled_from(["x", "y", "z"]),
                           "v": st.integers(0, 20)}),
    min_size=2, max_size=14,
)


@_legacy("test_measure_filter_visible_totals_match_oracle")
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=TOT_RECORDS, threshold=st.integers(0, 15), agg=st.sampled_from(["sum", "avg", "max"]))
@example(records=[{"r": "A", "c": "x", "v": 1}, {"r": "A", "c": "y", "v": 10}, {"r": "B", "c": "x", "v": 2}],
         threshold=5, agg="sum")
def test_measure_filter_visible_totals_match_oracle(records, threshold, agg):  # REQ-TOT-001..003, 006
    from .oracle import MeasureFilter

    alias = f"v_{agg}"
    query = Query(rows=["r", "c"], measures=[Measure(alias, agg, "v")],
                  measure_filters=[MeasureFilter(2, alias, ">", threshold)])
    expected = {row.path: row.values[alias] for row in pivot(query, records)}
    adapter = build_adapter(records)
    actual = by_path(run_view(adapter, rows=["r", "c"], agg=agg,
                              filters={alias: {"type": "gt", "value": threshold}}), agg)
    shown = {p for p in expected if p == () or expected[p] is not None}
    assert {p for p in actual if p != ()} == {p for p in shown if p != ()}, "visible nodes differ"
    bad = {p: (actual.get(p), expected[p]) for p in shown if not _same(actual.get(p), expected[p])}
    assert not bad, f"(engine, oracle): {bad}"
