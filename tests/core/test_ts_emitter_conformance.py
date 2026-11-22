"""The TypeScript DuckDB emitter (client mode) equals the oracle (M3 gate).

``serverside_pivot/src/lib/runtime/core/duckdbSqlEmitter.ts`` is bundled with
esbuild and driven over a line protocol; its SQL runs on DuckDB (the engine
DuckDB-Wasm embeds) against the same generated data and queries as the Python
core, and must reproduce the oracle exactly, including every conformance vector.
"""

from __future__ import annotations

import json
import math
import pathlib
import shutil
import subprocess
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings

from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery, to_wire
from pivot_engine.core.registry import type_family

from ..oracle.ir_adapter import oracle_rows
from .test_core_semantics import _ARROW, RECORDS, VECTORS, _table, examples, queries

ROOT = pathlib.Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "serverside_pivot"
SOURCE = FRONTEND / "src" / "lib" / "runtime" / "core" / "duckdbSqlEmitter.ts"
ESBUILD = FRONTEND / "node_modules" / ".bin" / ("esbuild.cmd" if shutil.which("cmd") else "esbuild")


@pytest.fixture(scope="module")
def emitter(tmp_path_factory):
    node = shutil.which("node")
    if node is None or not ESBUILD.exists():
        pytest.skip("node / esbuild unavailable")
    out = tmp_path_factory.mktemp("emitter") / "emitter.mjs"
    subprocess.run([str(ESBUILD), str(SOURCE), "--bundle", "--format=esm", "--platform=node", f"--outfile={out}"],
                   check=True, capture_output=True)
    proc = subprocess.Popen([node, str(pathlib.Path(__file__).with_name("ts_emitter_runner.mjs")), str(out)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8")

    def emit(query: PivotQuery, table: pa.Table) -> str:
        columns = {}
        con = duckdb.connect()
        con.register("probe", table)
        sql_types = dict(con.execute("SELECT column_name, column_type FROM (DESCRIBE probe)").fetchall())
        import ibis

        schema = ibis.memtable(table).schema()
        for name in table.column_names:
            columns[name] = {"family": type_family(schema[name]), "sqlType": sql_types[name]}
        proc.stdin.write(json.dumps({"query": to_wire(query), "table": "t", "columns": columns}, default=str) + "\n")
        proc.stdin.flush()
        reply = json.loads(proc.stdout.readline())
        assert "sql" in reply, reply.get("error")
        return reply["sql"]

    yield emit
    proc.stdin.close()
    proc.wait(timeout=10)


def _run(emit, query: PivotQuery, table: pa.Table):
    sql = emit(query, table)
    con = duckdb.connect()
    con.register("t", table)
    result = con.execute(sql).fetch_arrow_table().to_pylist()
    out = []
    for rec in result:
        depth = int(rec.pop("__depth"))
        out.append((tuple(rec[k] for k in query.rows[:depth]), rec))
    return out


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    return a == b


def _assert_equal(got, expected):
    assert [p for p, _ in got] == [p for p, _ in expected], "paths/order differ"
    for (path, gv), (_, ev) in zip(got, expected, strict=True):
        bad = {k: (gv.get(k), v) for k, v in ev.items() if not _same(gv.get(k), v)}
        assert not bad, f"{path}: (emitted, oracle) {bad}"


@settings(max_examples=examples(200), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=RECORDS, query=queries())
def test_emitted_sql_equals_oracle(emitter, records, query):
    _assert_equal(_run(emitter, query, _table(records)), oracle_rows(query, records))


@pytest.mark.parametrize("vector", VECTORS, ids=[v["id"] for v in VECTORS])
def test_emitted_sql_conforms_to_every_vector(emitter, vector):
    cols = vector["data"]["columns"]
    table = pa.table({k: pa.array(c["values"], _ARROW[c["type"]]) for k, c in cols.items()})
    q = vector["query"]
    query = PivotQuery(
        dataset="t", rows=tuple(q["rows"]),
        measures=tuple(MeasureRef(m["alias"], m["agg"], m["field"]) for m in q["measures"]),
        measure_filters=tuple(MeasureFilter(f["level"], f["measure"], f["op"], f["value"]) for f in q["measureFilters"]),
        totals=q["totals"],
    )
    expected = [(tuple(r["path"]), r["values"]) for r in vector["expected"]["rows"]]
    _assert_equal(_run(emitter, query, table), expected)
