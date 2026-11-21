"""Core cache manager (REQ-SEC-004, F-CACHES)."""

from __future__ import annotations

import pytest

from pivot_engine.core.authority.scope import scoped_execution
from pivot_engine.core.cache import CacheManager, key_for
from pivot_engine.runtime.dataset_authorization import RowScopeGrant


def _grant(value):
    return RowScopeGrant(table="t", subject=value, field_values={"tenant": (value,)}, scope_fingerprint=value)


def test_keys_differ_per_tenant_so_reuse_across_tenants_is_impossible():
    cache = CacheManager()
    with scoped_execution(_grant("A")):
        cache.put(key_for("result", "t", "plan1"), "rows of A")
        assert cache.get(key_for("result", "t", "plan1")) == "rows of A"
    with scoped_execution(_grant("B")):
        assert cache.get(key_for("result", "t", "plan1")) is None
    assert cache.get(key_for("result", "t", "plan1")) is None  # unscoped is its own key


def test_revision_is_part_of_the_key_and_invalidation_keeps_the_current_one():
    cache = CacheManager()
    cache.put(key_for("result", "t", "p", revision="1"), "old")
    cache.put(key_for("result", "t", "p", revision="2"), "new")
    cache.put(key_for("result", "u", "p", revision="1"), "other dataset")
    assert cache.invalidate(dataset="t", keep_revision="2") == 1
    assert cache.get(key_for("result", "t", "p", revision="1")) is None
    assert cache.get(key_for("result", "t", "p", revision="2")) == "new"
    assert cache.get(key_for("result", "u", "p", revision="1")) == "other dataset"


def test_byte_budget_evicts_least_recently_used():
    cache = CacheManager(budgets={"result": 10}, sizeof=lambda v: len(v))
    a, b, c = (key_for("result", "t", p) for p in "abc")
    cache.put(a, "xxxx")
    cache.put(b, "yyyy")
    assert cache.get(a) == "xxxx"  # a is now most recent
    cache.put(c, "zzzz")  # 12 > 10: evict b
    assert cache.get(b) is None and cache.get(a) == "xxxx" and cache.get(c) == "zzzz"
    assert cache.used_bytes("result") == 8
    cache.put(key_for("result", "t", "huge"), "w" * 11)  # larger than the tier: not cached
    assert cache.get(key_for("result", "t", "huge")) is None


def test_unknown_tier_is_rejected():
    with pytest.raises(ValueError):
        key_for("everything", "t", "p")
