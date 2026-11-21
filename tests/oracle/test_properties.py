"""Generated data x generated specs: engine == oracle (REQ-TOT-003, REQ-NULL-002, REQ-BACK-001/002).

Runs against whatever engine serves the runtime today.  Known legacy
divergences are listed in tests/conformance/legacy_xfail.json; the test is
then an expected failure until the core replaces the engine (M2), at which
point the xfail entry must be removed (strict once removed).
"""

from __future__ import annotations

import asyncio
import json
import math
import pathlib

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from .oracle import Measure, Query, pivot

_XFAIL = json.loads((pathlib.Path(__file__).parents[1] / "conformance" / "legacy_xfail.json").read_text())

DIM_VALUES = st.sampled_from(["A", "a", "B", "", "x|y", "x_y", None])
RECORDS = st.lists(
    st.fixed_dictionaries({"r": DIM_VALUES, "c": DIM_VALUES, "v": st.one_of(st.none(), st.integers(-50, 50))}),
    min_size=1,
    max_size=12,
)
AGGS = st.sampled_from(["sum", "avg", "min", "max", "count"])


def _engine_rows(records, agg):
    from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState
    from pivot_engine.tanstack_adapter import create_tanstack_adapter

    table = pa.table(
        {
            "r": pa.array([x["r"] for x in records], pa.string()),
            "c": pa.array([x["c"] for x in records], pa.string()),
            "v": pa.array([x["v"] for x in records], pa.int64()),
        }
    )
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("t", table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    state = PivotViewState(
        row_fields=["r", "c"], col_fields=[], val_configs=[{"field": "v", "agg": agg}],
        filters={}, expanded=True, show_col_totals=True,
    )
    ctx = PivotRequestContext(
        table="t", trigger_prop="p.viewport", start_row=0, end_row=10_000,
        viewport_active=True, include_grand_total=True,
    )
    response = asyncio.run(service.process_async(state, ctx))
    out = {}
    for row in response.data or []:
        if row.get("_isTotal") and row.get("_path") == "__grand_total__":
            path = ()
        else:
            path = tuple(row.get(f) for f in row.get("_pathFields") or [])
        out[path] = row.get(f"v_{agg}")
    return out


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)


@pytest.mark.xfail(
    "test_engine_matches_oracle_hierarchy" in _XFAIL,
    reason="legacy engine divergence; see tests/conformance/legacy_xfail.json",
    strict=False,
)
@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=RECORDS, agg=AGGS)
def test_engine_matches_oracle_hierarchy(records, agg):
    expected = {
        row.path: row.values[f"v_{agg}"]
        for row in pivot(Query(rows=["r", "c"], measures=[Measure(f"v_{agg}", agg, "v")]), records)
    }
    actual = _engine_rows(records, agg)
    assert set(actual) == set(expected), f"paths differ: missing={set(expected)-set(actual)} extra={set(actual)-set(expected)}"
    bad = {p: (actual[p], expected[p]) for p in expected if not _same(actual[p], expected[p])}
    assert not bad, f"values differ (engine, oracle): {bad}"
