"""ResultFrame.records() builds canonical keys column by column: they equal the codec exactly (REQ-ID-001)."""

from __future__ import annotations

from datetime import date, datetime

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from pivot_engine.core.frame import DEPTH, ResultFrame
from pivot_engine.core.identity import CanonicalPathCodec

COLUMNS = {
    "s": (pa.string(), st.one_of(st.none(), st.text(max_size=6), st.sampled_from(["a", "a|||b", '"q"', "é", ""]))),
    "i": (pa.int64(), st.one_of(st.none(), st.integers(-(2**62), 2**62))),
    "f": (pa.float64(), st.one_of(st.none(), st.sampled_from([0.0, -0.0, 1.5, -2.25, 1e300]),
                                  st.floats(allow_nan=False, allow_infinity=False))),
    "b": (pa.bool_(), st.one_of(st.none(), st.booleans())),
    "d": (pa.date32(), st.one_of(st.none(), st.dates(min_value=date(1900, 1, 1), max_value=date(2100, 1, 1)))),
    "t": (pa.timestamp("us"), st.one_of(st.none(), st.datetimes(min_value=datetime(1990, 1, 1),
                                                                 max_value=datetime(2040, 1, 1)))),
}


@st.composite
def frames(draw):
    dims = draw(st.lists(st.sampled_from(sorted(COLUMNS)), min_size=1, max_size=3, unique=True))
    n = draw(st.integers(0, 12))
    pools = {d: draw(st.lists(COLUMNS[d][1], min_size=1, max_size=4)) for d in dims}  # repeats exercise the memo
    data = {d: pa.array([draw(st.sampled_from(pools[d])) for _ in range(n)], type=COLUMNS[d][0]) for d in dims}
    data["m"] = pa.array(list(range(n)), type=pa.int64())
    data[DEPTH] = pa.array([draw(st.integers(0, len(dims))) for _ in range(n)], type=pa.int16())
    return ResultFrame(table=pa.table(data), rows=tuple(dims), measures=("m",))


@settings(max_examples=300, deadline=None)
@given(frames())
def test_columnar_keys_equal_the_codec(frame):
    for record, raw in zip(frame.records(), frame.table.to_pylist()):
        depth = int(raw[DEPTH])
        path = tuple(raw[k] for k in frame.rows[:depth])
        assert record["path"] == path and record["depth"] == depth
        assert record["key"] == CanonicalPathCodec.encode(list(path))
        assert record["values"] == {"m": raw["m"]}
        assert record["dims"] == {k: raw[k] for k in frame.rows[:depth]}


def test_negative_zero_keeps_its_own_encoding():
    frame = ResultFrame(table=pa.table({"f": pa.array([0.0, -0.0, 0.0]), "m": [1, 2, 3],
                                        DEPTH: pa.array([1, 1, 1], type=pa.int16())}), rows=("f",), measures=("m",))
    keys = [r["key"] for r in frame.records()]
    assert keys == [CanonicalPathCodec.encode([0.0]), CanonicalPathCodec.encode([-0.0]), CanonicalPathCodec.encode([0.0])]
    assert keys[0] != keys[1]
