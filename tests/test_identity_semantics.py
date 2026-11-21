"""Row identity (REQ-ID-001, REQ-ID-002)."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from pivot_engine.streaming.exact_measure_engine import CanonicalPathCodec

VALUE = st.one_of(st.none(), st.text(max_size=6), st.integers(-5, 5))


def test_grand_total_and_null_member_are_distinct():  # REQ-ID-002
    grand, null_member = CanonicalPathCodec.encode([]), CanonicalPathCodec.encode([None])
    assert grand != null_member
    assert CanonicalPathCodec.decode(grand) == ()
    assert CanonicalPathCodec.decode(null_member) == (None,)
    assert CanonicalPathCodec.encode([""]) not in {grand, null_member}


@given(st.lists(VALUE, max_size=4), st.lists(VALUE, max_size=4))
def test_encoding_is_injective_and_round_trips(a, b):  # REQ-ID-001
    ea, eb = CanonicalPathCodec.encode(a), CanonicalPathCodec.encode(b)
    assert CanonicalPathCodec.decode(ea) == tuple(a)
    assert (ea == eb) == (list(a) == list(b) and [type(x) for x in a] == [type(x) for x in b])
