"""
config.py - Configuration for the scalable pivot engine
"""
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()


@dataclass
class ScalablePivotConfig:
    """Configuration for the scalable pivot engine"""
    
    # Database configuration
    backend_uri: str = ":memory:"
    backend_type: str = "duckdb"
    
    # Caching configuration
    cache_type: str = "memory"  # memory or redis
    redis_config: Dict[str, Any] = field(default_factory=dict)
    default_cache_ttl: int = 300
    enable_l1_cache: bool = True
    l1_cache_ttl: int = 60
    
    # Performance configuration
    tile_size: int = 100
    enable_tiles: bool = True
    chunk_size: int = 1000
    enable_streaming: bool = True
    
    # Hierarchical configuration
    max_hierarchy_depth: int = 10
    enable_materialized_hierarchies: bool = True
    enable_intelligent_prefetch: bool = True
    enable_pruning: bool = True
    
    # Streaming & CDC configuration
    enable_cdc: bool = True
    cdc_batch_size: int = 100
    streaming_timeout: int = 30
    
    # Microservices configuration
    enable_microservices: bool = True
    service_discovery: str = "consul"  # consul, etcd, kubernetes
    load_balancer: str = "round_robin"
    
    # Performance optimization
    enable_delta_updates: bool = True
    enable_incremental_views: bool = True
    query_timeout: int = 300  # 5 minutes
    max_concurrent_queries: int = 10
    
    # UI configuration
    virtual_scroll_threshold: int = 1000  # When to enable virtual scrolling
    progressive_load_chunk_size: int = 100
    throttle_time: float = 0.1  # 100ms between UI updates

    def from_env(self) -> 'ScalablePivotConfig':
        """Load configuration from environment variables"""
        config = ScalablePivotConfig()
        
        # Database settings
        config.backend_uri = os.getenv('BACKEND_URI', config.backend_uri)
        config.backend_type = os.getenv('BACKEND_TYPE', config.backend_type)
        
        # Cache settings
        config.cache_type = os.getenv('CACHE_TYPE', config.cache_type)
        config.default_cache_ttl = int(os.getenv('CACHE_TTL', str(config.default_cache_ttl)))
        
        # Redis settings if enabled
        if os.getenv('CACHE_TYPE', 'memory') == 'redis':
            config.redis_config = {
                'host': os.getenv('REDIS_HOST', 'localhost'),
                'port': int(os.getenv('REDIS_PORT', '6379')),
                'db': int(os.getenv('REDIS_DB', '0')),
                'password': os.getenv('REDIS_PASSWORD', None)
            }
        
        # Performance settings
        config.tile_size = int(os.getenv('TILE_SIZE', str(config.tile_size)))
        config.chunk_size = int(os.getenv('CHUNK_SIZE', str(config.chunk_size)))
        
        # Hierarchical settings
        config.max_hierarchy_depth = int(os.getenv('MAX_HIERARCHY_DEPTH', str(config.max_hierarchy_depth)))
        
        # Performance limits
        config.query_timeout = int(os.getenv('QUERY_TIMEOUT', str(config.query_timeout)))
        config.max_concurrent_queries = int(os.getenv('MAX_CONCURRENT_QUERIES', str(config.max_concurrent_queries)))
        
        # UI settings
        config.virtual_scroll_threshold = int(os.getenv('VIRTUAL_SCROLL_THRESHOLD', str(config.virtual_scroll_threshold)))
        
        return config
    
    def validate(self) -> None:
        """Validate configuration settings"""
        errors = []
        
        if self.tile_size <= 0:
            errors.append("tile_size must be positive")
        
        if self.chunk_size <= 0:
            errors.append("chunk_size must be positive")
        
        if self.max_hierarchy_depth <= 0:
            errors.append("max_hierarchy_depth must be positive")
        
        if self.query_timeout <= 0:
            errors.append("query_timeout must be positive")
        
        if self.max_concurrent_queries <= 0:
            errors.append("max_concurrent_queries must be positive")
        
        if errors:
            raise ValueError(f"Configuration validation errors: {'; '.join(errors)}")


class ConfigManager:
    """Manager for configuration loading and validation"""
    
    def __init__(self):
        self.config: Optional[ScalablePivotConfig] = None
    
    def load_config(self, config_source: Optional[str] = None) -> ScalablePivotConfig:
        """Load configuration from various sources"""
        # Default to loading from env if no source specified or explicit 'env'
        if config_source == 'env' or config_source is None:
            self.config = ScalablePivotConfig().from_env()
        else:
            self.config = ScalablePivotConfig()
        
        self.config.validate()
        return self.config
    
    def get_config(self) -> ScalablePivotConfig:
        """Get the loaded configuration"""
        if self.config is None:
            self.config = self.load_config()
        return self.config


# Global configuration manager
config_manager = ConfigManager()


def get_config() -> ScalablePivotConfig:
    """Get the global configuration"""
    return config_manager.get_config()