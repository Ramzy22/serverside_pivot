from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import json
import asyncio
from pivot_engine.cache.redis_cache import RedisCache

class StateStore(ABC):
    @abstractmethod
    async def get_state(self, job_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def save_state(self, job_id: str, state: Dict[str, Any]):
        pass

class MemoryStateStore(StateStore):
    def __init__(self):
        self._store = {}

    async def get_state(self, job_id: str) -> Dict[str, Any]:
        return self._store.get(job_id, {})

    async def save_state(self, job_id: str, state: Dict[str, Any]):
        self._store[job_id] = state

class RedisStateStore(StateStore):
    def __init__(self, redis_cache: RedisCache):
        self.redis = redis_cache

    async def get_state(self, job_id: str) -> Dict[str, Any]:
        # RedisCache.get returns deserialized object (dict)
        state = self.redis.get(f"stream_state:{job_id}")
        return state if state else {}

    async def save_state(self, job_id: str, state: Dict[str, Any]):
        # RedisCache.set handles serialization
        self.redis.set(f"stream_state:{job_id}", state)
