"""
config.py - Configuration for the scalable pivot engine
"""
import os
from typing import Dict, Any, Literal, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv


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
    cdc_exact_state_store: Literal["memory", "redis"] = "memory"
    
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
        """Load configuration from environment variables into this instance."""
        # Fix L7: load .env lazily instead of at module import time
        load_dotenv()

        # Database settings
        self.backend_uri = os.getenv('BACKEND_URI', self.backend_uri)
        self.backend_type = os.getenv('BACKEND_TYPE', self.backend_type)
        
        # Cache settings
        self.cache_type = os.getenv('CACHE_TYPE', self.cache_type)
        self.default_cache_ttl = int(os.getenv('CACHE_TTL', str(self.default_cache_ttl)))
        
        # Redis settings if enabled
        if self.cache_type == 'redis':
            redis_url = str(os.getenv('REDIS_URL') or '').strip()
            if redis_url:
                self.redis_config = {
                    'url': redis_url,
                    'ssl_cert_reqs': os.getenv('REDIS_SSL_CERT_REQS', 'required'),
                }
            else:
                self.redis_config = {
                    'host': os.getenv('REDIS_HOST', 'localhost'),
                    'port': int(os.getenv('REDIS_PORT', '6379')),
                    'db': int(os.getenv('REDIS_DB', '0')),
                    'ssl_cert_reqs': os.getenv('REDIS_SSL_CERT_REQS', 'required'),
                }
                redis_username = os.getenv('REDIS_USERNAME')
                redis_password = os.getenv('REDIS_PASSWORD')
                if redis_username:
                    self.redis_config['username'] = redis_username
                if redis_password:
                    self.redis_config['password'] = redis_password
        
        # Performance settings
        self.tile_size = int(os.getenv('TILE_SIZE', str(self.tile_size)))
        self.chunk_size = int(os.getenv('CHUNK_SIZE', str(self.chunk_size)))
        self.cdc_exact_state_store = str(
            os.getenv('CDC_EXACT_STATE_STORE', self.cdc_exact_state_store)
        ).strip().lower()
        
        # Hierarchical settings
        self.max_hierarchy_depth = int(os.getenv('MAX_HIERARCHY_DEPTH', str(self.max_hierarchy_depth)))
        
        # Performance limits
        self.query_timeout = int(os.getenv('QUERY_TIMEOUT', str(self.query_timeout)))
        self.max_concurrent_queries = int(os.getenv('MAX_CONCURRENT_QUERIES', str(self.max_concurrent_queries)))
        
        # UI settings
        self.virtual_scroll_threshold = int(os.getenv('VIRTUAL_SCROLL_THRESHOLD', str(self.virtual_scroll_threshold)))
        
        return self
    
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

        if self.cdc_exact_state_store not in {"memory", "redis"}:
            errors.append("cdc_exact_state_store must be 'memory' or 'redis'")
        elif self.cdc_exact_state_store == "redis":
            try:
                from .runtime.distributed import distributed_runtime_from_env

                runtime_state = distributed_runtime_from_env()
                if runtime_state is None:
                    errors.append(
                        "Redis CDC state requires a validated distributed Redis runtime."
                    )
                else:
                    runtime_state.close()
            except Exception:
                errors.append(
                    "Redis CDC state requires a validated distributed Redis runtime."
                )

        if self.cache_type == "redis":
            try:
                from .production_security import ProductionSecuritySettings
                settings = ProductionSecuritySettings.from_env()
                settings.validate()
                if settings.is_production:
                    from urllib.parse import urlparse
                    redis_url = str(self.redis_config.get("url") or "")
                    parsed_url = urlparse(redis_url)
                    if (
                        parsed_url.scheme != "rediss"
                        or not parsed_url.username
                        or not parsed_url.password
                        or str(self.redis_config.get("ssl_cert_reqs") or "").lower()
                        not in {"required", "cert_required"}
                    ):
                        errors.append(
                            "Production Redis cache requires an authenticated rediss:// URL with certificate verification."
                        )
            except RuntimeError as exc:
                errors.append(str(exc))
        
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
