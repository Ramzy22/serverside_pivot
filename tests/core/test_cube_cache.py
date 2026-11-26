"""Scoped partial cubes: coverage extensions, generations and byte accounting."""

import gc
from dataclasses import dataclass, replace

import pyarrow as pa

from pivot_engine.core.authority.scope import scoped_execution
from pivot_engine.core.cache import CacheManager, key_for
from pivot_engine.core.cube_cache import CubeAccess, CubeEntry, cube_identity
from pivot_engine.core.ir import MeasureRef, PivotQuery, SortKey, Window
from pivot_engine.runtime.dataset_authorization import RowScopeGrant


def query():
    return PivotQuery(dataset="t", rows=("r", "c"), columns=("p",),
                      measures=(MeasureRef("s", "sum", "v"),))


def access(cache, revision="1", current=lambda: True):
    return CubeAccess(cache, revision, lambda k, v: cache.put(k, v) if current() else None, current)


def test_identity_ignores_presentation_but_keeps_input_semantics():
    q = query()
    assert cube_identity(q, ("r", "p")) == cube_identity(
        replace(q, sort=(SortKey("s", desc=True),), expanded=frozenset({("A",)}),
                window=Window(10, 20), column_members=(("Q2",),)), ("r", "p"))
    assert cube_identity(q, ("r", "p")) != cube_identity(q, ("r", "c", "p"))
    assert cube_identity(q, ("r", "p")) != cube_identity(replace(q, totals="all_data"), ("r", "p"))


def test_extensions_scan_only_new_parents_including_empty_and_null_groups():
    cache = CacheManager()
    scans = []

    def build(restriction):
        scans.append(restriction)
        paths = sorted(restriction[1], key=repr)
        return pa.table({"r": pa.array([p[0] for p in paths if p != ("empty",)], pa.string()),
                         "__s0": [1 for p in paths if p != ("empty",)]})

    for paths in [frozenset({("A",), (None,)}), frozenset({("B",), ("empty",)}),
                  frozenset({("A",), ("B",), (None,), ("empty",)})]:
        with access(cache) as a:
            table = a.get(query(), ("r", "c", "p"), (("r",), paths), build)
    assert len(scans) == 2
    assert scans[1][1] == frozenset({("B",), ("empty",)})
    assert table.num_rows == 3
    assert cache.used_bytes("cube") >= table.nbytes


def test_revision_scope_and_policy_cannot_reuse_another_cube():
    cache = CacheManager()
    calls = []

    def build(_restriction):
        calls.append(True)
        return pa.table({"__s0": [len(calls)]})

    @dataclass(frozen=True)
    class PolicyGrant(RowScopeGrant):
        policy_version: str = ""

    for tenant, policy, revision in [("A", "1", "1"), ("A", "1", "1"), ("B", "1", "1"),
                                     ("A", "2", "1"), ("A", "2", "2")]:
        grant = PolicyGrant(table="t", subject=tenant, field_values={"r": (tenant,)},
                              scope_fingerprint=tenant, policy_version=policy)
        with scoped_execution(grant), access(cache, revision) as a:
            a.get(query(), ("r", "p"), None, build)
    assert len(calls) == 4


def test_invalidation_during_build_cannot_publish_old_partials():
    cache = CacheManager()
    valid = [True]

    def build(_restriction):
        valid[0] = False
        cache.invalidate(dataset="t")
        return pa.table({"__s0": [1]})

    with access(cache, current=lambda: valid[0]) as a:
        a.get(query(), ("r", "p"), None, build)
    assert cache.used_bytes("cube") == 0


def test_cube_reservation_lives_until_last_reader_releases_entry():
    released = []

    class Lease:
        def release(self):
            released.append(True)

    cache = CacheManager()
    key = key_for("cube", "t", "p")
    entry = CubeEntry(pa.table({"v": [1]}), None)
    entry.lease = Lease()
    cache.put(key, entry)
    reader = cache.get(key)
    del entry
    cache.invalidate(dataset="t")
    gc.collect()
    assert not released
    del reader
    gc.collect()
    assert released == [True]
