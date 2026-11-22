"""Run tests/conformance/vectors.json against the Python runtime engine.

Vectors listed under ``vectors`` in legacy_xfail.json are expected failures of
the legacy engine; each must flip to passing when the core takes over (M2).
"""

from __future__ import annotations

import asyncio
import json
import os
import math
import pathlib

import pyarrow as pa
import pytest

HERE = pathlib.Path(__file__).parent
VECTORS = json.loads((HERE / "vectors.json").read_text(encoding="utf-8"))["vectors"]
LEGACY = json.loads((HERE / "legacy_xfail.json").read_text(encoding="utf-8")).get("vectors", {})
# With the semantic core serving (PIVOT_CORE=on) the legacy divergences are gone: the vectors must pass.
CORE_SERVES = os.environ.get("PIVOT_CORE", "off").strip().lower() == "on"
_ARROW = {"string": pa.string(), "int64": pa.int64(), "float64": pa.float64()}
_OPS = {">": "gt", ">=": "gte", "<": "lt", "<=": "lte", "=": "eq", "!=": "ne"}


def _run_engine(vector):
    from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState
    from pivot_engine.tanstack_adapter import create_tanstack_adapter

    cols = vector["data"]["columns"]
    table = pa.table({k: pa.array(c["values"], _ARROW[c["type"]]) for k, c in cols.items()})
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("t", table)
    q = vector["query"]
    keys = {m["alias"]: f'{m["field"]}_{m["agg"]}' for m in q["measures"]}
    leaf = len(q["rows"])
    filters = {}
    for mf in q["measureFilters"]:
        if mf["level"] != leaf:
            pytest.skip("runtime state has no per-level measure filters below the leaf")
        filters[keys[mf["measure"]]] = {"type": _OPS[mf["op"]], "value": mf["value"]}
    state = PivotViewState(
        row_fields=q["rows"], col_fields=[],
        val_configs=[{"field": m["field"], "agg": m["agg"]} for m in q["measures"]],
        filters=filters, expanded=True, show_col_totals=True,
    )
    ctx = PivotRequestContext(table="t", trigger_prop="p.viewport", start_row=0, end_row=10_000,
                              viewport_active=True, include_grand_total=True)
    rows = asyncio.run(PivotRuntimeService(adapter_getter=lambda: adapter).process_async(state, ctx)).data or []
    out = {}
    for row in rows:
        path = () if (row.get("_isTotal") and row.get("_path") == "__grand_total__") else tuple(
            row.get(f) for f in row.get("_pathFields") or [])
        out[path] = {alias: row.get(key, row.get(alias)) for alias, key in keys.items()}
    return out


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)


@pytest.mark.parametrize(
    "vector",
    [pytest.param(v, id=v["id"], marks=[pytest.mark.xfail(condition=not CORE_SERVES, reason=LEGACY[v["id"]], strict=True)] if v["id"] in LEGACY else [])
     for v in VECTORS],
)
def test_python_engine_conforms(vector):
    expected = {tuple(r["path"]): r["values"] for r in vector["expected"]["rows"]}
    actual = _run_engine(vector)
    assert set(actual) == set(expected), f"paths: missing={set(expected)-set(actual)} extra={set(actual)-set(expected)}"
    bad = {p: (actual[p], expected[p]) for p in expected
           if any(not _same(actual[p].get(k), v) for k, v in expected[p].items())}
    assert not bad, f"(engine, expected): {bad}"


def test_vectors_are_regenerated_from_the_oracle():
    from tests.conformance.generate_vectors import VECTORS as fresh

    assert fresh == VECTORS, "vectors.json is stale: run python -m tests.conformance.generate_vectors"
