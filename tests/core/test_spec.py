"""The language-neutral spec and the Python core agree."""

from __future__ import annotations

import json
import pathlib

import jsonschema
import pytest

from pivot_engine.core import registry as registry_module
from pivot_engine.core.ir import IRValidationError, MeasureFilter, MeasureRef, PivotQuery, SortKey, Window, from_wire

SPEC = pathlib.Path(registry_module.__file__).parent / "spec"
SCHEMA = json.loads((SPEC / "semantic-ir.schema.json").read_text())
REG = json.loads((SPEC / "aggregation-registry.json").read_text())
VECTORS = json.loads((pathlib.Path(__file__).parents[1] / "conformance" / "vectors.json").read_text())["vectors"]


@pytest.mark.parametrize("vector", VECTORS, ids=[v["id"] for v in VECTORS])
def test_every_vector_query_is_valid_wire_ir(vector):
    jsonschema.validate(vector["query"], SCHEMA)
    for m in vector["query"]["measures"]:
        assert m["agg"] in REG["aggregations"]
    from_wire(vector["query"], dataset="t")


def test_wire_round_trip_builds_the_same_query():
    wire = {
        "dataset": "sales", "rows": ["region", "city"],
        "measures": [{"alias": "s", "agg": "sum", "field": "amount"}, {"alias": "n", "agg": "count", "field": None}],
        "filters": [{"field": "year", "op": "in", "value": [2025, 2026]}],
        "measureFilters": [{"level": 2, "measure": "s", "op": ">", "value": 10}],
        "sort": [{"field": "region", "desc": True}], "totals": "all_data",
        "expanded": [["North"]], "window": {"offset": 5, "limit": 10},
    }
    jsonschema.validate(wire, SCHEMA)
    q = from_wire(wire)
    assert q == PivotQuery(
        dataset="sales", rows=("region", "city"),
        measures=(MeasureRef("s", "sum", "amount"), MeasureRef("n", "count", None)),
        filters=(q.filters[0],), measure_filters=(MeasureFilter(2, "s", ">", 10),),
        sort=(SortKey("region", True),), totals="all_data", expanded=frozenset({("North",)}), window=Window(5, 10),
    )
    assert q.filters[0].value == (2025, 2026)
    hash(q)  # frozen and hashable (usable in plan fingerprints)


@pytest.mark.parametrize("bad", [{}, {"rows": ["r"]}, {"rows": ["r"], "measures": [{"agg": "sum"}]}])
def test_malformed_wire_is_a_typed_error(bad):
    with pytest.raises(IRValidationError):
        from_wire(bad, dataset="t")


def test_schema_operator_enums_match_the_ir():
    from pivot_engine.core.ir import PREDICATE_OPS

    filt = SCHEMA["$defs"]["predicate"]["properties"]["op"]["enum"]
    assert set(filt) == set(PREDICATE_OPS)
    assert SCHEMA["$defs"]["group"]["properties"]["op"]["enum"] == ["and", "or"]


def test_nested_filter_groups_validate_against_the_schema():
    wire = {"rows": ["r"], "measures": [{"alias": "s", "agg": "sum", "field": "v"}],
            "filters": [{"op": "or", "conditions": [
                {"field": "r", "op": "contains", "value": "a", "caseSensitive": False},
                {"op": "and", "conditions": [{"field": "v", "op": "between", "value": [1, 3]}]}]}]}
    jsonschema.validate(wire, SCHEMA)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**wire, "filters": [{"op": "xor", "conditions": []}]}, SCHEMA)
