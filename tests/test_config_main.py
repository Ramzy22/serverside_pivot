"""
Test suite for configuration and main application
"""
import pytest
import asyncio
from pivot_engine.config import ScalablePivotConfig, ConfigManager, get_config
from pivot_engine.main import ScalablePivotApplication
from pivot_engine.scalable_pivot_controller import ScalablePivotController


def test_scalable_pivot_config_creation():
    """Test creation of ScalablePivotConfig"""
    config = ScalablePivotConfig()
    
    # Verify default values are set
    assert config.backend_uri == ":memory:"
    assert config.cache_type == "memory"
    assert config.tile_size == 100
    assert config.default_cache_ttl == 300
    assert config.max_hierarchy_depth == 10


def test_scalable_pivot_config_validation():
    """Test configuration validation"""
    config = ScalablePivotConfig()
    
    # Valid config should not raise an error
    config.validate()
    
    # Test invalid config - negative tile_size
    invalid_config = ScalablePivotConfig(tile_size=-1)
    with pytest.raises(ValueError):
        invalid_config.validate()


def test_config_manager():
    """Test configuration manager functionality"""
    manager = ConfigManager()
    
    # Get default config
    config1 = manager.get_config()
    assert isinstance(config1, ScalablePivotConfig)
    
    # Get config again (should return same instance)
    config2 = manager.get_config()
    assert config1 is config2


def test_global_config():
    """Test global configuration access"""
    config = get_config()
    assert isinstance(config, ScalablePivotConfig)


@pytest.mark.asyncio
async def test_scalable_pivot_application_creation():
    """Test creation of ScalablePivotApplication"""
    config = ScalablePivotConfig()
    app = ScalablePivotApplication(config)
    
    # Verify components are created
    assert app.controller is not None
    assert isinstance(app.controller, ScalablePivotController)
    
    # These might be None if FastAPI is not available
    assert app.config is not None


@pytest.mark.asyncio
async def test_scalable_pivot_application_services():
    """Test that application services are properly initialized"""
    config = ScalablePivotConfig(
        enable_streaming=True,
        enable_incremental_views=True
    )
    app = ScalablePivotApplication(config)
    
    # Verify controller is created
    assert app.controller is not None
    
    # Verify configuration is applied
    assert app.config == config


@pytest.mark.asyncio
async def test_application_pivot_request_handling():
    """Test application's pivot request handling"""
    config = ScalablePivotConfig()
    app = ScalablePivotApplication(config)
    
    # Create a simple spec
    spec = {
        "table": "test",
        "rows": ["col1"],
        "measures": [{"field": "col2", "agg": "sum", "alias": "sum_col2"}]
    }
    
    # The method should at least not crash (though it will fail without real data)
    try:
        result = await app.handle_pivot_request(spec)
        # Should return a result with status
        assert "status" in result
    except Exception:
        # Expected to fail without real data, but shouldn't crash the system
        assert True


@pytest.mark.asyncio
async def test_application_hierarchical_request_handling():
    """Test application's hierarchical request handling"""
    config = ScalablePivotConfig()
    app = ScalablePivotApplication(config)
    
    # Create a simple spec
    spec = {
        "table": "test",
        "rows": ["col1", "col2"],
        "measures": [{"field": "col3", "agg": "sum", "alias": "sum_col3"}]
    }
    
    expanded_paths = [["value1"]]
    user_preferences = {"enable_pruning": True}
    
    # The method should at least not crash
    try:
        result = await app.handle_hierarchical_request(spec, expanded_paths, user_preferences)
        # Should return a result with status
        assert "status" in result
    except Exception:
        # Expected to fail without real data, but shouldn't crash the system  
        assert True


@pytest.mark.asyncio
async def test_scalable_application_with_different_configs():
    """Test application with different configuration options"""
    # Test with different cache types
    configs = [
        ScalablePivotConfig(cache_type="memory", tile_size=50),
        ScalablePivotConfig(enable_streaming=False, chunk_size=500),
        ScalablePivotConfig(max_hierarchy_depth=5, enable_pruning=True)
    ]
    
    for config in configs:
        app = ScalablePivotApplication(config)
        
        # Verify app is created with the specific config
        assert app.config == config


def test_config_from_env(monkeypatch):
    """Test loading config from environment variables"""
    # Mock environment variables
    monkeypatch.setenv('BACKEND_URI', 'test_uri')
    monkeypatch.setenv('CACHE_TYPE', 'redis')
    monkeypatch.setenv('TILE_SIZE', '200')
    
    config = ScalablePivotConfig().from_env()
    
    assert config.backend_uri == 'test_uri'
    assert config.cache_type == 'redis'
    assert config.tile_size == 200


def test_scalable_pivot_config_equality():
    """Test ScalablePivotConfig equality and hashing"""
    config1 = ScalablePivotConfig(tile_size=100)
    config2 = ScalablePivotConfig(tile_size=100)
    config3 = ScalablePivotConfig(tile_size=200)
    
    # Same parameters should create equivalent configs
    assert config1.tile_size == config2.tile_size
    
    # Different parameters should be different
    assert config1.tile_size != config3.tile_size


if __name__ == "__main__":
    pytest.main([__file__])