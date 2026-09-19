from __future__ import annotations

import os

import fakeredis
import pytest
import redis

from pivot_engine.cache_coordinator import CacheInvalidationEvent
from pivot_engine.runtime import DistributedRuntimeSettings as PublicSettings
from pivot_engine.runtime.distributed import (
    DistributedRuntimeConfigurationError,
    DistributedRuntimeSettings,
    RedisRuntimeState,
)


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
