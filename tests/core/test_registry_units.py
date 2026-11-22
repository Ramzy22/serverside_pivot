"""Aggregation registry unit behaviour (REQ-AGG-001..004)."""

from __future__ import annotations

import json
import pathlib
import types

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest

from pivot_engine.core import registry as reg
from pivot_engine.core.physical import lowering

SPEC = pathlib.Path(reg.__file__).parent / "spec" / "aggregation-registry.json"
RAW = json.loads(SPEC.read_text(encoding="utf-8"))


def test_fresh_registry_mirrors_the_spec_exactly():
    r = reg.Registry(SPEC)
    assert r.version == RAW["version"] == 2
    assert r.names() == sorted(RAW["aggregations"]) == [
        "avg", "count", "count_distinct", "first", "last", "max", "median", "min", "percentile", "stddev", "sum",
        "variance", "weighted_avg"]
    for name, raw in RAW["aggregations"].items():
        e = r.get(name)
        assert (e.name, e.inputs, e.empty, e.merge, e.cost_class, e.sql, e.params) == (
            name, frozenset(raw["inputs"]), raw["empty"], raw["merge"], raw["costClass"], raw["sql"],
            frozenset(raw.get("params", ())))
        assert name in lowering.LOWERINGS
    assert r.get("percentile").params == {"q"} and r.get("weighted_avg").params == {"weight"}
    assert r.get("sum").params == frozenset()


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
        lowering.check_drift(reg.Registry(_write(tmp_path, aggs)))


def test_lowering_without_a_spec_entry_is_drift(tmp_path):
    aggs = {k: v for k, v in RAW["aggregations"].items() if k != "median"}
    with pytest.raises(RuntimeError, match=r"drift: spec-only=\[\] lowering-only=\['median'\]"):
        lowering.check_drift(reg.Registry(_write(tmp_path, aggs)))
    lowering.check_drift(reg.REGISTRY)  # the shipped pair agrees


def test_lower_rejects_an_unknown_aggregation():
    con = ibis.duckdb.connect()
    t = con.create_table("t", pa.table({"v": [1]}))
    with pytest.raises(reg.UnknownAggregationError):
        lowering.lower(types.SimpleNamespace(agg="summ", field="v"), t)
    assert lowering.lower(types.SimpleNamespace(agg="count", field=None), t).to_pyarrow().as_py() == 1


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
    ("stddev", "decimal", True), ("variance", "string", False), ("percentile", "floating", True),
    ("weighted_avg", "integer", True), ("weighted_avg", "boolean", False),
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


M = types.SimpleNamespace(weight="w", q=0.25, order_by="w")


def test_every_lowering_has_sql_semantics_with_nulls():
    con = ibis.duckdb.connect()
    t = con.create_table("t", pa.table({"v": pa.array([1, 2, 2, None, 5], pa.int64()),
                                        "w": pa.array([1, 0, 3, 2, None], pa.int64())}))
    got = {name: lowering.LOWERINGS[name](t.v, t, M) for name in reg.REGISTRY.names()}
    got["count_star"] = lowering.LOWERINGS["count"](None, t, M)
    row = t.aggregate(**got).to_pyarrow().to_pylist()[0]
    approx = {k: row.pop(k) for k in ("stddev", "variance", "weighted_avg", "percentile")}
    # first/last over w: complete (w, v) pairs are (1, 1), (0, 2), (3, 2)
    assert row == {"avg": 2.5, "count": 4, "count_distinct": 3, "max": 5, "median": 2.0,
                   "min": 1, "sum": 10, "count_star": 5, "first": 2, "last": 2}
    assert approx["variance"] == pytest.approx(3.0)  # sample variance of 1, 2, 2, 5
    assert approx["stddev"] == pytest.approx(3.0 ** 0.5)
    assert approx["percentile"] == pytest.approx(1.75)  # 25th of [1, 2, 2, 5], linear interpolation
    assert approx["weighted_avg"] == pytest.approx((1 * 1 + 2 * 0 + 2 * 3) / 4)  # (5, None) and (None, 2) drop


def test_lowerings_over_only_nulls_match_the_registry_empty_rule():
    con = ibis.duckdb.connect()
    t = con.create_table("t", pa.table({"v": pa.array([None, None], pa.int64()), "w": pa.array([1, 1], pa.int64())}))
    row = t.aggregate(**{n: lowering.LOWERINGS[n](t.v, t, M) for n in reg.REGISTRY.names()}).to_pyarrow()
    row = row.to_pylist()[0]
    for name in reg.REGISTRY.names():
        assert row[name] == reg.REGISTRY.get(name).empty, name


def test_first_last_take_the_extreme_order_and_break_ties_by_value():  # REQ-AGG-008
    con = ibis.duckdb.connect()
    t = con.create_table("t", pa.table({"v": pa.array([5, 3, 9, None, 4, 1], pa.int64()),
                                        "o": pa.array([2, 2, None, 0, 7, 7], pa.int64())}))
    m = types.SimpleNamespace(order_by="o")
    row = t.aggregate(first=lowering.LOWERINGS["first"](t.v, t, m),
                      last=lowering.LOWERINGS["last"](t.v, t, m)).to_pyarrow().to_pylist()[0]
    # (o, v) pairs: (2, 5), (2, 3), (7, 4), (7, 1); NULL v or NULL o never count
    assert row == {"first": 3, "last": 4}


def test_first_last_need_an_order_field():  # REQ-AGG-008: never an arbitrary record
    from pivot_engine.core.ir import IRValidationError, MeasureRef, PivotQuery

    for agg in ("first", "last"):
        with pytest.raises(IRValidationError, match="needs an order field"):
            PivotQuery(dataset="t", rows=(), measures=(MeasureRef("m", agg, "v"),)).validate({"v": "integer"})
    with pytest.raises(IRValidationError, match="does not take an order field"):
        PivotQuery(dataset="t", rows=(), measures=(MeasureRef("m", "sum", "v", order_by="v"),)).validate({"v": "integer"})


def test_sample_statistics_need_two_inputs_and_zero_weight_is_null():
    con = ibis.duckdb.connect()
    t = con.create_table("t", pa.table({"v": pa.array([7], pa.int64()), "w": pa.array([0], pa.int64())}))
    row = t.aggregate(**{n: lowering.LOWERINGS[n](t.v, t, M)
                         for n in ("stddev", "variance", "weighted_avg", "percentile")}).to_pyarrow().to_pylist()[0]
    assert row == {"stddev": None, "variance": None, "weighted_avg": None, "percentile": 7.0}
