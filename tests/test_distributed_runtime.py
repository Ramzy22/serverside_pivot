from __future__ import annotations

import os

import fakeredis
import pytest
import redis

from pivot_engine.cache_coordinator import CacheInvalidationEvent
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


def test_runtime_exports_distributed_configuration_contract():
    assert PublicSettings is DistributedRuntimeSettings


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
