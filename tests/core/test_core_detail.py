"""Record-level reads through the core (REQ-DET-001)."""

from __future__ import annotations

import ibis
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pivot_engine.core.exec import detail
from pivot_engine.core.ir import DetailQuery, IRValidationError, Predicate, SortKey, Window

DIM = st.sampled_from(["A", "a", "B", "", "x|y", None])
RECORDS = st.lists(st.fixed_dictionaries({"r": DIM, "c": DIM, "v": st.one_of(st.none(), st.integers(-5, 5))}),
                   max_size=14)


def _con(records):
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({
        "r": pa.array([x["r"] for x in records], pa.string()),
        "c": pa.array([x["c"] for x in records], pa.string()),
        "v": pa.array([x["v"] for x in records], pa.int64()),
    }))
    return con


def _key(rec):  # the documented total order: every column ascending, NULLS LAST
    return tuple((rec[c] is None, rec[c] if rec[c] is not None else 0) for c in ("r", "c", "v"))


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(RECORDS, st.lists(DIM, min_size=0, max_size=2), st.integers(1, 4))
def test_pages_partition_the_node_records(records, path, size):  # REQ-DET-001
    fields = ("r", "c")[: len(path)]
    con = _con(records)
    expected = sorted((r for r in records if all(r[f] == v for f, v in zip(fields, path, strict=True))), key=_key)
    pages, offset, total = [], 0, None
    while True:
        table, total = detail(DetailQuery("t", path_fields=fields, path=tuple(path), window=Window(offset, size)),
                              con.table)
        rows = table.to_pylist()
        pages += rows
        offset += size
        if offset >= total:
            break
    assert total == len(expected)
    assert pages == expected  # NULL member selects the NULL group, never everything


def test_sort_search_and_filters():
    records = [{"r": "A", "c": "x", "v": 3}, {"r": "A", "c": "Y", "v": None}, {"r": "A", "c": "y", "v": 1},
               {"r": "B", "c": "y", "v": 2}]
    con = _con(records)
    table, total = detail(DetailQuery("t", path_fields=("r",), path=("A",), sort=(SortKey("v", desc=True),)),
                          con.table)
    assert [r["v"] for r in table.to_pylist()] == [3, 1, None] and total == 3  # NULLS LAST when descending too
    table, total = detail(DetailQuery("t", search="Y"), con.table)
    assert total == 3 and {r["c"] for r in table.to_pylist()} == {"Y", "y"}
    table, total = detail(DetailQuery("t", filters=(Predicate("v", ">", 1),)), con.table)
    assert sorted(r["v"] for r in table.to_pylist()) == [2, 3]


@pytest.mark.parametrize("kw, message", [
    (dict(path_fields=("r",)), "path [] does not match the fields ['r']."),
    (dict(path_fields=("r", "r"), path=("A", "A")), "path fields must be unique."),
    (dict(path_fields=("zz",), path=("A",)), "path field references unknown column 'zz'."),
    (dict(search=3), "search must be text."),
    (dict(sort=(SortKey("zz"),)), "sort key references unknown column 'zz'."),
    (dict(sort=(SortKey("v"), SortKey("v", True))), "each sort field may appear once."),
    (dict(filters=(Predicate("zz", "=", 1),)), "filter references unknown column 'zz'."),
    (dict(window=Window(-1)), "window offset/limit must be non-negative."),
])
def test_invalid_detail_queries(kw, message):
    with pytest.raises(IRValidationError, match="^" + message.replace("[", r"\[").replace("]", r"\]")
                       .replace("(", r"\(").replace(")", r"\)").replace(".", r"\.") + "$"):
        detail(DetailQuery("t", **kw), _con([{"r": "A", "c": "x", "v": 1}]).table)
