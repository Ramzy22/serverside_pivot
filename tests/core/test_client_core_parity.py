"""Client mode and server mode answer the same Protocol v1 request identically (M3 gate).

The client path is the real TypeScript code: ``ClientSqlPlanner.compile`` then
``runClientCore`` (``src/lib/runtime/core/clientCoreRuntime.ts``), whose
DuckDB connection is this process's DuckDB (the engine DuckDB-Wasm embeds).
The server path is ``PivotRuntimeService`` with ``PIVOT_CORE=on``.  For the
same state over the same data both must return the same visible rows, in the
same order, with the same values and the same grand total.
"""

from __future__ import annotations

import asyncio
import json
import math
import pathlib
import shutil
import subprocess

import duckdb
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, event, given, settings
from hypothesis import strategies as st

from pivot_engine.runtime import PivotRuntimeService
from pivot_engine.runtime.models import PivotRequestContext, PivotViewState
from pivot_engine.tanstack_adapter import create_tanstack_adapter

from .test_core_semantics import examples

ROOT = pathlib.Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "serverside_pivot"
ESBUILD = FRONTEND / "node_modules" / ".bin" / ("esbuild.cmd" if shutil.which("cmd") else "esbuild")
TABLE = "client_t"

MEMBER = st.sampled_from(["A", "B", "a", "", "x|y"])
RECORD = st.fixed_dictionaries({
    "r": st.one_of(st.none(), MEMBER), "c": st.one_of(st.none(), MEMBER), "k": st.sampled_from(["K1", "K2", "k1"]),
    "v": st.one_of(st.none(), st.integers(-50, 50).map(float)), "n": st.one_of(st.none(), st.integers(-9, 9)),
})
AGGS = ["sum", "count", "avg", "min", "max", "count_distinct", "median"]


@st.composite
def states(draw):
    rows = draw(st.sampled_from([["r"], ["r", "c"], ["c", "r"]]))
    cols = draw(st.sampled_from([[], [], ["k"]]))
    aggs = draw(st.lists(st.sampled_from(AGGS), min_size=1, max_size=3, unique=True))
    vals = [{"field": draw(st.sampled_from(["v", "n"])), "agg": a} for a in aggs]
    vals = list({(v["field"], v["agg"]): v for v in vals}.values())
    sorting = []
    if draw(st.booleans()):
        sorting.append({"id": draw(st.sampled_from(rows)), "desc": draw(st.booleans())})
    if not cols and draw(st.booleans()):
        sorting.append({"id": f"{vals[0]['field']}_{vals[0]['agg']}", "desc": draw(st.booleans())})
    filters = {}
    if draw(st.booleans()):
        filters["n"] = {"operator": "AND", "conditions": [{"type": draw(st.sampled_from(["gt", "lte", "ne"])),
                                                            "value": draw(st.integers(-5, 5))}]}
    if draw(st.booleans()):
        filters["r"] = {"operator": "OR", "conditions": [{"type": "in", "value": draw(st.lists(MEMBER, min_size=1, max_size=2))}]}
    expanded = draw(st.one_of(st.just(True), st.just({}), st.sampled_from([{"A": True}, {"B": True}, {"": True}])))
    return {"rowFields": rows, "colFields": cols, "valConfigs": vals, "sorting": sorting, "filters": filters,
            "expanded": expanded}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    node = shutil.which("node")
    if node is None or not ESBUILD.exists():
        pytest.skip("node / esbuild unavailable")
    tmp = tmp_path_factory.mktemp("client_core")
    entry = tmp / "entry.ts"
    src = (FRONTEND / "src" / "lib" / "runtime").as_posix()
    entry.write_text(f'export {{ ClientSqlPlanner }} from "{src}/client/clientSqlPlanner";\n'
                     f'export {{ runClientCore, EmitterUnsupported }} from "{src}/core/clientCoreRuntime";\n')
    out = tmp / "client.mjs"
    subprocess.run([str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node", f"--outfile={out}"],
                   check=True, capture_output=True)
    proc = subprocess.Popen([node, str(pathlib.Path(__file__).with_name("client_core_runner.mjs")), str(out)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8")

    def run(state, table: pa.Table):
        con = duckdb.connect()
        con.register(TABLE, table)
        request = {"kind": "data", "state": state, "payload": {"start": 0, "end": 1000, "include_grand_total": True,
                                                                "includeGrandTotal": True}}
        proc.stdin.write(json.dumps({"request": request, "table": TABLE}) + "\n")
        proc.stdin.flush()
        while True:
            msg = json.loads(proc.stdout.readline())
            if "sql" not in msg:
                return msg
            try:
                rows = con.execute(msg["sql"]).fetch_arrow_table().to_pylist()
                reply = {"rows": rows}
            except duckdb.Error as exc:
                reply = {"error": str(exc)}
            proc.stdin.write(json.dumps(reply, default=str) + "\n")
            proc.stdin.flush()

    yield run
    proc.stdin.close()
    proc.wait(timeout=10)


def _table(records):
    return pa.table({
        "r": pa.array([x["r"] for x in records], pa.string()), "c": pa.array([x["c"] for x in records], pa.string()),
        "k": pa.array([x["k"] for x in records], pa.string()), "v": pa.array([x["v"] for x in records], pa.float64()),
        "n": pa.array([x["n"] for x in records], pa.int64()),
    })


def _server(state, table: pa.Table):
    import os

    from pivot_engine.runtime import core_shadow

    os.environ["PIVOT_CORE"] = "on"
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(TABLE, table)
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    view = PivotViewState(row_fields=state["rowFields"], col_fields=state["colFields"], val_configs=state["valConfigs"],
                          filters=state["filters"], sorting=state["sorting"], expanded=state["expanded"])
    before = core_shadow.STATS["core_served_hierarchy"]
    ctx = PivotRequestContext(table=TABLE, trigger_prop="pivot.viewport", start_row=0, end_row=1000,
                              viewport_active=True, needs_col_schema=True, include_grand_total=True)
    try:
        out = asyncio.run(service.process_async(view, ctx))
    finally:
        os.environ.pop("PIVOT_CORE", None)
    return out, core_shadow.STATS["core_served_hierarchy"] > before


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    return a == b


def _split(rows):
    body = [r for r in rows if not str(r.get("_path", "")).startswith("__") and not r.get("_isTotal")]
    gt = next((r for r in rows if r.get("_path") == "__grand_total__"), None)
    return body, gt


@settings(max_examples=examples(60), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=st.lists(RECORD, max_size=14), state=states())
def test_client_mode_equals_server_mode(client, records, state):
    table = _table(records)
    reply = client(state, table)
    assert "error" not in reply, reply.get("error")
    if "unsupported" in reply:  # a declared, exact fallback to the legacy client path
        event(f"unsupported: {reply['unsupported']}")
        return
    server, served = _server(state, table)
    assert served, "the server request was not answered by the core"
    s_body, s_gt = _split(server.data or [])
    c_body, c_gt = reply["result"]["rows"], reply["result"]["grandTotalRow"]
    assert [(r["_path"], r["depth"]) for r in c_body] == [(r["_path"], r["depth"]) for r in s_body]
    keys = [k for k in (s_body[0] if s_body else (s_gt or {})) if not k.startswith("_") and k not in ("depth", "uuid")
            and k not in state["rowFields"]]
    for c_row, s_row in zip(c_body, s_body, strict=True):
        bad = {k: (c_row.get(k), s_row.get(k)) for k in keys if not _same(c_row.get(k), s_row.get(k))}
        assert not bad, f"{s_row['_path']}: (client, server) {bad}"
    assert (c_gt is None) == (s_gt is None)
    if s_gt:
        bad = {k: (c_gt.get(k), s_gt.get(k)) for k in keys if not _same(c_gt.get(k), s_gt.get(k))}
        assert not bad, f"grand total: (client, server) {bad}"


def test_client_core_maps_the_common_request_without_fallback(client):
    """The everyday shapes (grouping, pivot, filters, sorts, expansion) are served by the core, not legacy JS."""
    table = _table([{"r": "A", "c": "x", "k": "K1", "v": 1.0, "n": 1}, {"r": None, "c": "y", "k": "K2", "v": 2.0, "n": 2}])
    for state in [
        {"rowFields": ["r", "c"], "colFields": [], "valConfigs": [{"field": "v", "agg": "sum"}], "sorting": [],
         "filters": {}, "expanded": True},
        {"rowFields": ["r"], "colFields": ["k"], "valConfigs": [{"field": "v", "agg": "avg"}], "sorting": [{"id": "r", "desc": True}],
         "filters": {"n": {"operator": "AND", "conditions": [{"type": "gt", "value": 0}]}}, "expanded": {}},
        {"rowFields": ["r", "c"], "colFields": [], "valConfigs": [{"field": "v", "agg": "sum"}],
         "sorting": [{"id": "v_sum", "desc": True}], "filters": {}, "expanded": {"A": True}},
    ]:
        reply = client(state, table)
        assert "result" in reply, reply


def test_client_core_keys_are_canonical(client):
    """Rows carry the canonical typed identity (REQ-ID-001) next to the display path."""
    table = _table([{"r": None, "c": "1", "k": "K1", "v": 1.0, "n": 3}, {"r": "", "c": "1", "k": "K1", "v": 1.0, "n": 3}])
    reply = client({"rowFields": ["r", "n"], "colFields": [], "valConfigs": [{"field": "v", "agg": "sum"}],
                    "sorting": [], "filters": {}, "expanded": True}, table)
    from pivot_engine.core.identity import CanonicalPathCodec

    keys = [(CanonicalPathCodec.decode(r["_rowKey"]), r["_path"]) for r in reply["result"]["rows"]]
    assert keys == [(("",), ""), (("", 3), "|||3"), ((None,), ""), ((None, 3), "|||3")]


# --------------------------------------------------------------------------- the real transport


@pytest.fixture(scope="module")
def transport(tmp_path_factory):
    node = shutil.which("node")
    if node is None or not ESBUILD.exists():
        pytest.skip("node / esbuild unavailable")
    tmp = tmp_path_factory.mktemp("client_transport")
    out = tmp / "transport.mjs"
    src = FRONTEND / "src" / "lib" / "runtime" / "client" / "clientPivotTransport.ts"
    subprocess.run([str(ESBUILD), str(src), "--bundle", "--format=esm", "--platform=node", f"--outfile={out}",
                    "--external:xlsx*"], check=True, capture_output=True)
    proc = subprocess.Popen([node, str(pathlib.Path(__file__).with_name("client_transport_runner.mjs")), str(out)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8")
    files = tmp / "files"
    files.mkdir()

    def run(rows, request):
        con = duckdb.connect()
        con.execute(f"SET file_search_path = '{files.as_posix()}'")
        proc.stdin.write(json.dumps({"rows": rows, "request": request}) + "\n")
        proc.stdin.flush()
        while True:
            msg = json.loads(proc.stdout.readline())
            if "file" in msg:
                (files / msg["file"]).write_text(msg["text"], encoding="utf-8")
                reply = {"ok": True}
            elif "sql" in msg:
                try:
                    reply = {"rows": con.execute(msg["sql"]).fetch_arrow_table().to_pylist()}
                except duckdb.Error as exc:
                    reply = {"error": str(exc)}
            else:
                return msg
            proc.stdin.write(json.dumps(reply, default=str) + "\n")
            proc.stdin.flush()

    yield run
    proc.stdin.close()
    proc.wait(timeout=10)


def test_client_transport_serves_data_from_the_core(transport):
    rows = [{"r": "A", "c": "x", "v": 1.5}, {"r": "A", "c": "y", "v": 2.5}, {"r": None, "c": "x", "v": 4.0},
            {"r": "a", "c": "x", "v": 8.0}]
    request = {"kind": "data", "state": {"rowFields": ["r"], "colFields": [], "valConfigs": [{"field": "v", "agg": "sum"}],
                                         "expanded": True},
               "payload": {"start": 0, "end": 50, "include_grand_total": True}}
    reply = transport(rows, request)
    assert "response" in reply, reply.get("error")
    assert reply["telemetry"]["coreServed"] is True, reply["telemetry"]
    assert reply["telemetry"]["activeEngine"] == "duckdb-wasm"
    data = [r for r in reply["response"]["payload"]["data"] if r.get("_path") != "__color_scale_stats__"]
    body = [(r["_path"], r["v_sum"]) for r in data if not r.get("_isTotal")]
    assert body == [("A", 4.0), ("a", 8.0), ("", 4.0)]  # binary order, NULL last, case variants distinct
    gt = next(r for r in data if r.get("_path") == "__grand_total__")
    assert gt["v_sum"] == 16.0


def test_client_transport_falls_back_exactly_and_says_why(transport):
    rows = [{"r": "A", "v": 1.0}]
    request = {"kind": "data", "state": {"rowFields": ["r"], "colFields": [], "expanded": True,
                                         "valConfigs": [{"field": "v", "agg": "formula", "formula": "v * 2", "id": "f"}]},
               "payload": {"start": 0, "end": 50}}
    reply = transport(rows, request)
    assert "response" in reply, reply.get("error")
    assert reply["telemetry"]["coreServed"] is False
    assert "formula" in reply["telemetry"]["coreFallbackReason"]
