import pytest

from pivot_engine.config import ScalablePivotConfig


def test_config_from_env_mutates_and_returns_same_instance(monkeypatch):
    monkeypatch.setenv("BACKEND_URI", "env_uri")
    monkeypatch.delenv("TILE_SIZE", raising=False)

    config = ScalablePivotConfig(tile_size=50)
    returned = config.from_env()

    assert returned is config
    assert config.backend_uri == "env_uri"
    assert config.tile_size == 50


def test_config_from_env_populates_existing_redis_config(monkeypatch):
    monkeypatch.delenv("CACHE_TYPE", raising=False)
    monkeypatch.setenv("REDIS_HOST", "redis.local")
    monkeypatch.setenv("REDIS_PORT", "6380")

    config = ScalablePivotConfig(cache_type="redis")
    config.from_env()

    assert config.cache_type == "redis"
    assert config.redis_config["host"] == "redis.local"
    assert config.redis_config["port"] == 6380


def test_config_rejects_redis_cdc_state_without_validated_distributed_runtime(monkeypatch):
    monkeypatch.delenv("PIVOT_DISTRIBUTED_RUNTIME", raising=False)
    monkeypatch.delenv("PIVOT_RUNTIME_REDIS_URL", raising=False)
    monkeypatch.delenv("PIVOT_RUNTIME_REDIS_PREFIX", raising=False)

    config = ScalablePivotConfig()
    config.cdc_exact_state_store = "redis"

    with pytest.raises(ValueError, match="Redis CDC state requires"):
        config.validate()
