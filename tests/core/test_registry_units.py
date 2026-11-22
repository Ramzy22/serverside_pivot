"""Aggregation registry unit behaviour (REQ-AGG-001..004)."""

from __future__ import annotations

import json
import pathlib

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest

from pivot_engine.core import registry as reg

SPEC = pathlib.Path(reg.__file__).parent / "spec" / "aggregation-registry.json"
RAW = json.loads(SPEC.read_text(encoding="utf-8"))


def test_fresh_registry_mirrors_the_spec_exactly():
    r = reg.Registry(SPEC)
    assert r.version == RAW["version"] == 1
    assert r.names() == sorted(RAW["aggregations"]) == ["avg", "count", "count_distinct", "max", "median", "min", "sum"]
    for name, raw in RAW["aggregations"].items():
        e = r.get(name)
        assert (e.name, e.inputs, e.empty, e.merge, e.cost_class, e.sql) == (
            name, frozenset(raw["inputs"]), raw["empty"], raw["merge"], raw["costClass"], raw["sql"])
        assert e.lower is reg._LOWERINGS[name]


def test_module_registry_is_built_from_the_packaged_spec():
    assert reg._SPEC == SPEC and reg._SPEC.is_file()
    assert reg.REGISTRY.names() == reg.Registry().names()


def _write(tmp_path, aggregations):
    path = tmp_path / "spec.json"
    path.write_text(json.dumps({"version": 7, "aggregations": aggregations}), encoding="utf-8")
    return path


def test_spec_without_a_lowering_is_drift(tmp_path):
    aggs = dict(RAW["aggregations"], mode=RAW["aggregations"]["sum"])
    with pytest.raises(RuntimeError, match=r"drift: spec-only=\['mode'\] lowering-only=\[\]"):
        reg.Registry(_write(tmp_path, aggs))


def test_lowering_without_a_spec_entry_is_drift(tmp_path):
    aggs = {k: v for k, v in RAW["aggregations"].items() if k != "median"}
    with pytest.raises(RuntimeError, match=r"drift: spec-only=\[\] lowering-only=\['median'\]"):
        reg.Registry(_write(tmp_path, aggs))


def test_version_is_read_from_the_spec(tmp_path):
    assert reg.Registry(_write(tmp_path, RAW["aggregations"])).version == 7


def test_unknown_aggregation_is_a_typed_error_naming_it():
    with pytest.raises(reg.UnknownAggregationError, match=r"^unknown aggregation 'summ'$"):
        reg.REGISTRY.get("summ")
    assert issubclass(reg.UnknownAggregationError, ValueError)


@pytest.mark.parametrize("name, family, ok", [
    ("sum", "integer", True), ("sum", "floating", True), ("sum", "decimal", True), ("sum", "string", False),
    ("avg", "boolean", False), ("min", "string", True), ("max", "date", True), ("min", "other", False),
    ("count", "other", True), ("count", "string", True), ("count_distinct", "timestamp", True),
    ("median", "integer", True), ("median", "string", False), ("count", "star", False),
])
def test_accepts_matrix(name, family, ok):
    assert reg.REGISTRY.get(name).accepts(family) is ok


@pytest.mark.parametrize("dtype, family", [
    (dt.int8, "integer"), (dt.int64, "integer"), (dt.uint32, "integer"),
    (dt.float32, "floating"), (dt.float64, "floating"), (dt.Decimal(10, 2), "decimal"),
    (dt.string, "string"), (dt.boolean, "boolean"), (dt.timestamp, "timestamp"), (dt.date, "date"),
    (dt.binary, "other"), (dt.Array(dt.int64), "other"),
])
def test_type_family(dtype, family):
    assert reg.type_family(dtype) == family


def test_every_lowering_has_sql_semantics_with_nulls():
    con = ibis.duckdb.connect()
    t = con.create_table("t", pa.table({"v": pa.array([1, 2, 2, None, 5], pa.int64())}))
    got = {name: reg.REGISTRY.get(name).lower(t.v, t) for name in reg.REGISTRY.names()}
    got["count_star"] = reg.REGISTRY.get("count").lower(None, t)
    row = t.aggregate(**got).to_pyarrow().to_pylist()[0]
    assert row == {"avg": 2.5, "count": 4, "count_distinct": 3, "max": 5, "median": 2.0,
                   "min": 1, "sum": 10, "count_star": 5}


def test_lowerings_over_only_nulls_match_the_registry_empty_rule():
    con = ibis.duckdb.connect()
    t = con.create_table("t", pa.table({"v": pa.array([None, None], pa.int64())}))
    row = t.aggregate(**{n: reg.REGISTRY.get(n).lower(t.v, t) for n in reg.REGISTRY.names()}).to_pyarrow().to_pylist()[0]
    for name in reg.REGISTRY.names():
        assert row[name] == reg.REGISTRY.get(name).empty, name
