from __future__ import annotations

import logging
import os
import time

import fakeredis
import pytest
import redis

from pivot_engine.adapter_viewport_cache import AdapterViewportCache
from pivot_engine.cache.generation_guard import GenerationGuard, GenerationGuardedCache
from pivot_engine.cache.memory_cache import MemoryCache
from pivot_engine.cache_coordinator import (
    CacheCoordinator,
    CacheInvalidationEvent,
    CacheNamespaces,
)
from pivot_engine.runtime import DistributedRuntimeSettings as PublicSettings
from pivot_engine.runtime.distributed import (
    DistributedRuntimeConfigurationError,
    DistributedRuntimeUnavailable,
    DistributedRuntimeSettings,
    RedisRuntimeState,
)
from pivot_engine.runtime.payload_store import RedisPayloadBackend, RuntimePayloadStore
from pivot_engine.runtime.request_coordinator import RuntimeRequestCoordinator
from pivot_engine.runtime.service import PivotRuntimeService
from pivot_engine.runtime.session_gate import SessionRequestGate
from pivot_engine.runtime.models import PivotRequestContext, PivotViewState
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.tanstack_adapter import create_tanstack_adapter


class _RedisGenerationStub:
    """Small data-only Redis double for distributed-state unit contracts."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.published: list[tuple[str, bytes]] = []

    def ping(self) -> bool:
        return True

    def get(self, key: str):
        return self.values.get(key)

    def incr(self, key: str) -> int:
        current = int((self.values.get(key) or b"0").decode("ascii")) + 1
        self.values[key] = str(current).encode("ascii")
        return current

    def publish(self, channel: str, message: bytes) -> int:
        self.published.append((channel, message))
        return 0


class _RedisGateStub(_RedisGenerationStub):
    """Minimal shared implementation of the session-gate Lua transition."""

    def __init__(self, shared: dict[str, dict[str, str]]) -> None:
        super().__init__()
        self.shared = shared

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.shared.get(key, {}))

    def eval(self, _script, _key_count, key, state_epoch, window_seq, abort_generation, intent, now_ts, _ttl):
        current = self.shared.get(key, {})
        current_epoch = int(current.get("state_epoch", 0))
        current_window = int(current.get("window_seq", 0))
        current_structural = int(current.get("structural_seq", 0))
        current_chart = int(current.get("chart_seq", 0))
        current_abort = int(current.get("abort_generation", 0))
        state_epoch, window_seq, abort_generation = map(
            int, (state_epoch, window_seq, abort_generation)
        )
        if abort_generation < current_abort or state_epoch < current_epoch:
            return 0
        if state_epoch > current_epoch:
            next_state = {
                "state_epoch": str(state_epoch),
                "window_seq": str(window_seq),
                "structural_seq": "0",
                "chart_seq": str(window_seq if intent == "chart" else 0),
            }
        elif intent == "viewport":
            if window_seq <= current_window or window_seq <= current_structural:
                return 0
            next_state = {
                "state_epoch": str(current_epoch),
                "window_seq": str(window_seq),
                "structural_seq": str(current_structural),
                "chart_seq": str(current_chart),
            }
        elif intent == "chart":
            if window_seq <= current_chart:
                return 0
            next_state = {
                "state_epoch": str(current_epoch),
                "window_seq": str(current_window),
                "structural_seq": str(current_structural),
                "chart_seq": str(window_seq),
            }
        else:
            next_state = {
                "state_epoch": str(current_epoch),
                "window_seq": str(max(current_window, window_seq)),
                "structural_seq": str(max(current_structural, window_seq)),
                "chart_seq": str(current_chart),
            }
        next_state["abort_generation"] = str(max(current_abort, abort_generation))
        next_state["updated_at"] = str(now_ts)
        self.shared[key] = next_state
        return 1


class _FailingCleanupPubSub:
    """Pub/Sub double that proves shutdown cleans up each resource independently."""

    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.unsubscribe_calls = 0
        self.close_calls = 0

    def subscribe(self, channel: str) -> None:
        self.subscribed.append(channel)

    def get_message(self, timeout: float):
        time.sleep(min(timeout, 0.001))
        return None

    def unsubscribe(self, _channel: str) -> None:
        self.unsubscribe_calls += 1
        raise RuntimeError("simulated unsubscribe failure")

    def close(self) -> None:
        self.close_calls += 1
        raise RuntimeError("simulated close failure")


class _FailingCleanupRedis(_RedisGenerationStub):
    def __init__(self) -> None:
        super().__init__()
        self.pubsub_instance = _FailingCleanupPubSub()

    def pubsub(self, *, ignore_subscribe_messages: bool):
        assert ignore_subscribe_messages is True
        return self.pubsub_instance


def _payload_runtime_state(client):
    return RedisRuntimeState(
        DistributedRuntimeSettings(
            enabled=True,
            redis_url="rediss://user:pass@example/0",
            key_prefix="payload-test",
            payload_ttl_seconds=30,
            payload_max_bytes=1024 * 1024,
        ),
        client=client,
    )


def _wait_until(predicate, *, timeout_seconds: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_runtime_exports_distributed_configuration_contract():
    assert PublicSettings is DistributedRuntimeSettings


def test_export_job_store_factory_is_local_without_redis_and_namespaced_when_enabled():
    from pivot_engine.runtime.distributed import export_job_store_for_runtime

    local = export_job_store_for_runtime(None)
    from pivot_engine.runtime.export_jobs import ExportJobSnapshot

    snapshot = ExportJobSnapshot("job", "alice", "account", "sales")
    assert local.create(snapshot) == snapshot
    server = fakeredis.FakeServer()
    first_state = _payload_runtime_state(fakeredis.FakeRedis(server=server))
    second_state = _payload_runtime_state(fakeredis.FakeRedis(server=server))
    first = export_job_store_for_runtime(first_state)
    second = export_job_store_for_runtime(second_state)
    first.create(snapshot)
    assert second.request_cancel("job").phase == "cancelled"
    assert first.get("job", "alice", "sales").phase == "cancelled"
    other_state = RedisRuntimeState(
        DistributedRuntimeSettings(enabled=True, key_prefix="other-deployment"),
        client=fakeredis.FakeRedis(server=server),
    )
    assert export_job_store_for_runtime(other_state).get("job", "alice", "sales") is None


def test_distributed_runtime_rejects_missing_or_insecure_redis_configuration(
    monkeypatch,
):
    monkeypatch.setenv("PIVOT_DISTRIBUTED_RUNTIME", "1")
    monkeypatch.delenv("PIVOT_RUNTIME_REDIS_URL", raising=False)
    monkeypatch.delenv("PIVOT_RUNTIME_REDIS_PREFIX", raising=False)

    with pytest.raises(DistributedRuntimeConfigurationError):
        DistributedRuntimeSettings.from_env()

    monkeypatch.setenv("PIVOT_RUNTIME_REDIS_URL", "redis://localhost/0")
    monkeypatch.setenv("PIVOT_RUNTIME_REDIS_PREFIX", "test")
    with pytest.raises(DistributedRuntimeConfigurationError):
        DistributedRuntimeSettings.from_env()


def test_runtime_state_namespaces_keys_and_generations_with_injected_redis():
    client = _RedisGenerationStub()
    state = RedisRuntimeState(
        DistributedRuntimeSettings(
            enabled=True,
            redis_url="rediss://user:pass@example/0",
            key_prefix="release-a",
        ),
        client=client,
    )

    assert state.key("payload", "token") == "release-a:payload:token"
    assert state.generation() == 0
    assert (
        state.invalidate(
            CacheInvalidationEvent(
                1,
                "mutation",
                ("controller.query",),
                table="sales",
            )
        )
        == 1
    )
    assert state.generation() == 1
    assert client.published[0][0] == "release-a:cache:invalidations"


def test_independent_redis_runtime_states_share_generation_with_fakeredis():
    server = fakeredis.FakeServer()
    settings = DistributedRuntimeSettings(
        enabled=True,
        redis_url="rediss://user:pass@example/0",
        key_prefix="release-a",
    )
    first = RedisRuntimeState(
        settings,
        client=fakeredis.FakeRedis(server=server, decode_responses=False),
    )
    second = RedisRuntimeState(
        settings,
        client=fakeredis.FakeRedis(server=server, decode_responses=False),
    )

    assert first.invalidate(
        CacheInvalidationEvent(1, "mutation", ("controller.query",), table="sales")
    ) == 1
    assert second.generation() == 1


def test_redis_payload_store_survives_producer_worker_loss():
    server = fakeredis.FakeServer()
    producer = RuntimePayloadStore(
        backend=RedisPayloadBackend(
            _payload_runtime_state(
                fakeredis.FakeRedis(server=server, decode_responses=False)
            )
        ),
        default_ttl_seconds=30,
    )
    consumer = RuntimePayloadStore(
        backend=RedisPayloadBackend(
            _payload_runtime_state(
                fakeredis.FakeRedis(server=server, decode_responses=False)
            )
        ),
        default_ttl_seconds=30,
    )

    reference = producer.put_bytes(
        b"shared export",
        metadata={"subject": "alice", "filename": "x.csv"},
    )
    del producer

    item = consumer.get(reference["id"])
    assert item is not None
    assert item.body == b"shared export"
    assert item.file_path is None
    assert item.metadata["subject"] == "alice"


def test_redis_payload_store_copies_export_file_before_worker_path_disappears(tmp_path):
    server = fakeredis.FakeServer()
    export = tmp_path / "export.xlsx"
    export.write_bytes(b"workbook")
    producer = RuntimePayloadStore(
        backend=RedisPayloadBackend(
            _payload_runtime_state(
                fakeredis.FakeRedis(server=server, decode_responses=False)
            )
        )
    )
    consumer = RuntimePayloadStore(
        backend=RedisPayloadBackend(
            _payload_runtime_state(
                fakeredis.FakeRedis(server=server, decode_responses=False)
            )
        )
    )

    reference = producer.put_file(str(export), metadata={"subject": "alice"})
    assert not export.exists()

    item = consumer.get(reference["id"])
    assert item is not None
    assert item.body == b"workbook"
    assert item.file_path is None


def test_two_workers_share_causal_request_ordering():
    shared = {}
    first = SessionRequestGate(
        runtime_state=_payload_runtime_state(_RedisGateStub(shared))
    )
    second = SessionRequestGate(
        runtime_state=_payload_runtime_state(_RedisGateStub(shared))
    )

    assert first.register_request("alice", 4, 10, 1, "viewport", "mount")
    assert not second.register_request("alice", 4, 9, 1, "viewport", "mount")
    assert second.response_is_current("alice", 4, 10, 1, "viewport", "mount")


def test_distributed_request_gate_does_not_fall_back_to_process_memory():
    class _UnavailableRedis(_RedisGenerationStub):
        def eval(self, *_args):
            raise OSError("connection lost")

    gate = SessionRequestGate(runtime_state=_payload_runtime_state(_UnavailableRedis()))

    with pytest.raises(
        DistributedRuntimeUnavailable,
        match="Distributed runtime Redis is unavailable",
    ):
        gate.register_request("alice", 1, 1, 0, "viewport")


def test_runtime_service_threads_one_distributed_state_to_its_request_gate():
    state = _payload_runtime_state(_RedisGenerationStub())

    service = PivotRuntimeService(adapter_getter=lambda: object(), distributed_runtime=state)

    assert service._request_coordinator.session_gate._runtime_state is state


def test_runtime_service_rejects_a_distributed_coordinator_with_another_gate():
    state = _payload_runtime_state(_RedisGenerationStub())
    coordinator = RuntimeRequestCoordinator(SessionRequestGate())

    with pytest.raises(ValueError, match="shared Redis runtime state"):
        PivotRuntimeService(
            adapter_getter=lambda: object(),
            request_coordinator=coordinator,
            distributed_runtime=state,
        )


def test_worker_b_discards_l1_cache_after_worker_a_invalidation_without_pubsub():
    server = fakeredis.FakeServer()
    state_a = _payload_runtime_state(
        fakeredis.FakeRedis(server=server, decode_responses=False)
    )
    state_b = _payload_runtime_state(
        fakeredis.FakeRedis(server=server, decode_responses=False)
    )
    cache_b_backing = MemoryCache()
    cache_b = GenerationGuardedCache(
        cache_b_backing, GenerationGuard(state_b, cache_b_backing.clear)
    )
    cache_b.set("query", {"sales": 100})

    CacheCoordinator(
        generation_guard=GenerationGuard(state_a, lambda: None)
    ).invalidate(event="mutation", reason="test", table="sales")

    assert cache_b.get("query") is None


def test_remote_invalidation_pubsub_clears_viewport_cache_before_next_lookup():
    server = fakeredis.FakeServer()
    state_a = _payload_runtime_state(
        fakeredis.FakeRedis(server=server, decode_responses=False)
    )
    state_b = _payload_runtime_state(
        fakeredis.FakeRedis(server=server, decode_responses=False)
    )
    coordinator_a = CacheCoordinator(
        generation_guard=GenerationGuard(state_a, lambda: None)
    )
    coordinator_a.register_namespace(
        CacheNamespaces.ADAPTER_RESPONSE_WINDOW,
        owner="adapter",
    )
    coordinator_b = CacheCoordinator(
        generation_guard=GenerationGuard(state_b, lambda: None)
    )
    cache_b = AdapterViewportCache(
        ttl_seconds=60,
        pivot_catalog_size=8,
        response_window_size=8,
        row_block_cache_size=8,
        cache_coordinator=coordinator_b,
        generation_guard=coordinator_b.generation_guard,
    )
    cache_b.store(
        cache_b.response_window_cache,
        "window",
        {"rows": []},
        max_size=8,
        ttl_seconds=60,
    )

    coordinator_a.invalidate(
        [CacheNamespaces.ADAPTER_RESPONSE_WINDOW],
        event="mutation",
        reason="test",
        table="sales",
    )

    # Inspect the backing L1 collection directly: a guarded ``lookup`` would
    # also poll Redis and could mask a broken Pub/Sub delivery path.
    assert _wait_until(lambda: not cache_b.response_window_cache)
    coordinator_a.close()
    coordinator_b.close()


def test_pubsub_callback_failure_is_audited_and_does_not_stop_later_invalidations(caplog):
    """A bad optional callback must not hide failures or stop the subscriber."""

    server = fakeredis.FakeServer()
    publisher = _payload_runtime_state(
        fakeredis.FakeRedis(server=server, decode_responses=False)
    )
    subscriber = _payload_runtime_state(
        fakeredis.FakeRedis(server=server, decode_responses=False)
    )
    attempts: list[int] = []
    delivered: list[int] = []

    def callback(event: CacheInvalidationEvent) -> None:
        attempts.append(event.sequence)
        if len(attempts) == 1:
            raise RuntimeError("callback-secret-must-not-be-logged")
        delivered.append(event.sequence)

    with caplog.at_level(logging.WARNING, logger="pivot_engine.runtime.distributed"):
        subscriber.subscribe(callback)
        try:
            publisher.invalidate(
                CacheInvalidationEvent(1, "mutation", ("controller.query",))
            )
            assert _wait_until(lambda: attempts == [1])
            publisher.invalidate(
                CacheInvalidationEvent(2, "mutation", ("controller.query",))
            )
            assert _wait_until(lambda: delivered == [2])
        finally:
            publisher.close()
            subscriber.close()

    assert "event=distributed_invalidation_callback_failed" in caplog.text
    assert "callback-secret-must-not-be-logged" not in caplog.text


def test_pubsub_shutdown_audits_failures_and_attempts_every_cleanup_step(caplog):
    """An unsubscribe failure must not prevent pubsub.close during shutdown."""

    client = _FailingCleanupRedis()
    state = RedisRuntimeState(
        DistributedRuntimeSettings(
            enabled=True,
            redis_url="rediss://user:pass@example/0",
            key_prefix="shutdown-test",
        ),
        client=client,
    )

    with caplog.at_level(logging.WARNING, logger="pivot_engine.runtime.distributed"):
        state.subscribe(lambda _event: None)
        state.close()

    assert client.pubsub_instance.unsubscribe_calls == 1
    assert client.pubsub_instance.close_calls == 1
    assert "event=distributed_invalidation_unsubscribe_failed" in caplog.text
    assert "event=distributed_invalidation_close_failed" in caplog.text


def test_worker_b_adapter_cache_syncs_after_its_pubsub_subscription_is_closed():
    server = fakeredis.FakeServer()
    adapter_a = create_tanstack_adapter(
        distributed_runtime=_payload_runtime_state(
            fakeredis.FakeRedis(server=server, decode_responses=False)
        )
    )
    adapter_b = create_tanstack_adapter(
        distributed_runtime=_payload_runtime_state(
            fakeredis.FakeRedis(server=server, decode_responses=False)
        )
    )
    try:
        adapter_b._locked_cache_store(
            adapter_b._response_window_cache,
            "window",
            {"rows": []},
            max_size=8,
            ttl_seconds=60,
        )
        adapter_b.controller.cache_coordinator.close()

        adapter_a.controller._clear_mutation_caches("sales")

        assert (
            adapter_b._locked_cache_lookup(
                adapter_b._response_window_cache,
                "window",
            )
            is None
        )
    finally:
        adapter_a.controller.cache_coordinator.close()
        adapter_b.controller.cache_coordinator.close()


def test_worker_b_controller_query_l1_syncs_after_a_missed_notification():
    server = fakeredis.FakeServer()
    controller_a = ScalablePivotController(
        distributed_runtime=_payload_runtime_state(
            fakeredis.FakeRedis(server=server, decode_responses=False)
        )
    )
    controller_b = ScalablePivotController(
        distributed_runtime=_payload_runtime_state(
            fakeredis.FakeRedis(server=server, decode_responses=False)
        )
    )
    try:
        controller_b.cache.set("pivot_ibis:query:sales:cached", {"sales": 100})
        controller_b.cache_coordinator.close()

        controller_a._clear_mutation_caches("sales")

        assert controller_b.cache.get("pivot_ibis:query:sales:cached") is None
    finally:
        controller_a.cache_coordinator.close()
        controller_b.cache_coordinator.close()


@pytest.mark.parametrize(
    ("state", "trigger_prop"),
    [
        (PivotViewState(transaction_request={"action": "undo"}), None),
        (PivotViewState(transaction_request={"action": "redo"}), None),
        (PivotViewState(transaction_request={"action": "history"}), None),
        (PivotViewState(cell_update={"rowId": "North", "colId": "sales"}), "grid.cellUpdate"),
    ],
)
def test_distributed_editing_is_rejected_before_adapter_state_is_accessed(
    state,
    trigger_prop,
):
    accessed = False

    def adapter_getter():
        nonlocal accessed
        accessed = True
        raise AssertionError("editing must be rejected before adapter lookup")

    service = PivotRuntimeService(
        adapter_getter=adapter_getter,
        distributed_runtime=_payload_runtime_state(_RedisGenerationStub()),
    )

    response = service.process(
        state,
        PivotRequestContext(
            table="sales",
            trigger_prop=trigger_prop,
            session_id="alice",
            client_instance="grid",
        ),
    )

    assert response.status == "error"
    assert response.message == "editing_not_available_in_distributed_mode"
    assert not accessed


@pytest.mark.skipif(
    not os.environ.get("PIVOT_TEST_REDIS_URL"),
    reason="PIVOT_TEST_REDIS_URL is configured by Redis integration CI only.",
)
def test_independent_redis_runtime_states_share_generation_with_real_redis():
    settings = DistributedRuntimeSettings(
        enabled=True,
        redis_url="rediss://user:pass@example/0",
        key_prefix="pivot-test-distributed-runtime",
    )
    first = RedisRuntimeState(
        settings,
        client=redis.Redis.from_url(os.environ["PIVOT_TEST_REDIS_URL"], decode_responses=False),
    )
    second = RedisRuntimeState(
        settings,
        client=redis.Redis.from_url(os.environ["PIVOT_TEST_REDIS_URL"], decode_responses=False),
    )
    first.client.delete(first.key("cache", "generation"))

    assert first.invalidate(
        CacheInvalidationEvent(1, "mutation", ("controller.query",), table="sales")
    ) == 1
    assert second.generation() == 1
