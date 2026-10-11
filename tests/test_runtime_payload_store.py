from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import time

import fakeredis
import pytest

from pivot_engine.resource_governor import ResourceExhaustedError, ResourceGovernor, ResourceLimits
from pivot_engine.runtime.distributed import DistributedRuntimeSettings, RedisRuntimeState
from pivot_engine.runtime.payload_store import RedisPayloadBackend, RuntimePayload, RuntimePayloadStore


def test_payload_accounting_owner_cannot_be_set_by_metadata():
    store = RuntimePayloadStore(max_bytes=1024)
    reference = store.put_bytes(b"safe", metadata={"_accountingTenant": "forged"})
    item = store.get(reference["id"])
    assert item is not None
    assert "_accountingTenant" not in item.metadata
    assert "_accountingTenant" not in reference
    store.clear()


def test_payload_store_rejects_a_single_value_larger_than_total_budget():
    store = RuntimePayloadStore(max_entries=4, max_bytes=8)
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        store.put_bytes(b"123456789")
    assert store._size_snapshot() == {"entries": 0, "bytes": 0}


def test_local_payload_charges_survive_replace_eviction_expiry_delete_and_clear(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("pivot_engine.runtime.payload_store.time.time", lambda: now[0])
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    store = RuntimePayloadStore(max_entries=1, max_bytes=1000, governor=governor)
    first = store.put_bytes(b"first")
    first_charge = store._size_snapshot()["bytes"]
    assert first_charge >= 5
    assert governor.snapshot()["reserved_bytes"] == first_charge
    item = store.get(first["id"])
    store._store_item(replace(item, body=b"x", size=1), now[0])
    assert store._size_snapshot()["bytes"] == first_charge - 4
    second = store.put_bytes(b"second", ttl_seconds=1)
    assert store.get(first["id"]) is None
    assert governor.snapshot()["reserved_bytes"] == store._size_snapshot()["bytes"]
    now[0] = 102.0
    assert store.get(second["id"]) is None
    assert governor.snapshot()["reserved_bytes"] == 0
    third = store.put_bytes(b"third")
    store.delete(third["id"])
    store.delete(third["id"])
    assert governor.snapshot()["reserved_bytes"] == 0
    store.put_bytes(b"fourth")
    store.clear()
    assert governor.snapshot()["reserved_bytes"] == 0


def test_file_payload_charges_metadata_not_disk_contents(tmp_path):
    path = tmp_path / "large.bin"
    path.write_bytes(b"x" * 10000)
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    store = RuntimePayloadStore(max_bytes=1000, governor=governor)
    ref = store.put_file(str(path), metadata={"label": "small"})
    charge = store._size_snapshot()["bytes"]
    assert len(str(path)) <= charge < 1000
    assert ref["bytes"] == 10000
    assert governor.snapshot()["reserved_bytes"] == charge
    store.clear()
    assert governor.snapshot()["reserved_bytes"] == 0
    assert not path.exists()


def test_rejected_payload_does_not_evict_or_leak_capacity():
    governor = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    store = RuntimePayloadStore(max_bytes=1000, governor=governor)
    ref = store.put_bytes(b"valid")
    before = store._size_snapshot()
    with pytest.raises(ResourceExhaustedError):
        store.put_bytes(b"x" * 1001)
    assert store.get(ref["id"]).body == b"valid"
    assert store._size_snapshot() == before
    governor._rss_bytes_getter = lambda: 10000
    with pytest.raises(ResourceExhaustedError):
        store.put_bytes(b"rejected")
    assert governor.snapshot()["reserved_bytes"] == before["bytes"]


def redis_backends(total=400):
    server = fakeredis.FakeServer()
    settings = DistributedRuntimeSettings(enabled=True, key_prefix="budget", payload_max_bytes=1000,
                                          payload_total_max_bytes=total)
    return [RedisPayloadBackend(RedisRuntimeState(settings, client=fakeredis.FakeRedis(server=server)))
            for _ in range(2)]


def payload(token, expires=None):
    return RuntimePayload(token, b"x" * 200, "application/octet-stream", expires or time.time() + 60, {}, size=200)


def test_redis_aggregate_capacity_is_atomic_across_instances_and_released_on_delete():
    first, second = redis_backends()
    def put(index):
        try:
            [first, second][index].put(payload(str(index)))
            return True
        except ResourceExhaustedError:
            return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        admitted = list(pool.map(put, [0, 1]))
    assert sum(admitted) == 1
    winner = str(admitted.index(True))
    second.delete(winner)
    first.delete(winner)
    second.put(payload("replacement"))
    assert first.get("replacement").body == b"x" * 200
    first.clear()
    assert second.get("replacement") is None
    second.put(payload("after-clear"))


def test_redis_expiry_reconciliation_reclaims_budget_and_replacement_does_not_double_charge():
    first, second = redis_backends()
    first.put(payload("expired", expires=time.time() - 1))
    second.put(payload("current"))
    first.put(payload("current"))
    assert second.get("current") is not None
    assert first.get("expired") is None
    with pytest.raises(ResourceExhaustedError):
        second.put(payload("too-many"))


def test_distributed_total_limit_environment_validation():
    env = {"PIVOT_DISTRIBUTED_RUNTIME": "1", "PIVOT_RUNTIME_REDIS_URL": "rediss://u:p@localhost",
           "PIVOT_RUNTIME_REDIS_PREFIX": "budget", "PIVOT_RUNTIME_PAYLOAD_TOTAL_MAX_BYTES": "17"}
    assert DistributedRuntimeSettings.from_env(env).payload_total_max_bytes == 17


def test_distributed_file_rejects_oversize_before_reading_and_preserves_source(tmp_path, monkeypatch):
    path = tmp_path / "large.bin"
    path.write_bytes(b"x" * 1001)
    backend, _ = redis_backends()
    store = RuntimePayloadStore(backend=backend)
    def unexpected_read(*args, **kwargs):
        raise AssertionError("Oversized files must not be loaded into memory")
    monkeypatch.setattr("builtins.open", unexpected_read)
    with pytest.raises(ResourceExhaustedError):
        store.put_file(str(path))
    assert path.exists()


@pytest.mark.parametrize(
    "body_size,content_type,metadata",
    [
        (400, "application/octet-stream", {}),
        (200, "application/" + "x" * 200, {}),
        (200, "application/octet-stream", {"label": "\u00e9" * 40}),
        (0, "application/octet-stream", {"label": "x" * 400}),
    ],
)
def test_distributed_file_accounts_for_envelope_before_opening(
    tmp_path, monkeypatch, body_size, content_type, metadata
):
    path = tmp_path / "payload.bin"
    path.write_bytes(b"x" * body_size)
    backend, _ = redis_backends(total=400)
    store = RuntimePayloadStore(backend=backend)

    def unexpected_open(*args, **kwargs):
        raise AssertionError("Files whose envelope cannot fit must not be opened")

    monkeypatch.setattr("builtins.open", unexpected_open)
    with pytest.raises(ResourceExhaustedError):
        store.put_file(str(path), content_type=content_type, metadata=metadata)
    assert path.exists()
    assert backend.state.client.dbsize() == 0


def test_distributed_file_that_fits_preserves_payload_and_removes_source(tmp_path):
    path = tmp_path / "payload.bin"
    path.write_bytes(b"x" * 200)
    backend, _ = redis_backends(total=400)
    store = RuntimePayloadStore(backend=backend)
    ref = store.put_file(str(path), content_type="application/custom", metadata={"label": "small"})
    stored = store.get(ref["id"])
    assert stored.body == b"x" * 200
    assert stored.content_type == "application/custom"
    assert stored.metadata == {"label": "small"}
    assert ref["bytes"] == 200
    assert ref["format"] == "file"
    assert not path.exists()


def test_redis_native_expiry_and_clear_leave_exact_nonnegative_aggregate_counter():
    first, second = redis_backends()
    first.put(payload("one"))
    counter = first.state.key("payload-budget", "bytes")
    assert int(first.state.client.get(counter)) == len(first.state.client.get(first._key("one")))
    # Redis TTL removes the envelope independently of either Python backend.
    first.state.client.pexpire(first._key("one"), -1)
    second.put(payload("two"))
    assert int(first.state.client.get(counter)) == len(first.state.client.get(first._key("two")))
    first.delete("one")
    second.clear()
    first.delete("two")
    assert int(first.state.client.get(counter)) == 0


def test_redis_oversized_replacement_preserves_current_envelope_and_charge():
    first, second = redis_backends()
    first.put(payload("one"))
    counter = first.state.key("payload-budget", "bytes")
    previous = first.state.client.get(counter)
    with pytest.raises(ResourceExhaustedError):
        second.put(replace(payload("one"), body=b"x" * 500, size=500))
    assert first.get("one").body == b"x" * 200
    assert first.state.client.get(counter) == previous
