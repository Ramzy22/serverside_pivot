"""Monotonic generation guards for worker-local runtime cache state.

Redis is authoritative only for the generation counter.  Cached query and
viewport values remain process-local L1 data, so this module never attempts to
serialize arbitrary cache values across workers.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Optional, TYPE_CHECKING

from .memory_cache import CacheProtocol

if TYPE_CHECKING:
    from ..cache_coordinator import CacheInvalidationEvent


class GenerationGuard:
    """Synchronize one worker's L1 cache generation with shared runtime state."""

    def __init__(self, state: Optional[Any], clear: Callable[[], None]) -> None:
        self._state = state
        self._clear = clear
        self._lock = threading.RLock()
        self._generation = state.generation() if state is not None else 0

    @property
    def state(self) -> Optional[Any]:
        return self._state

    def set_clear(self, clear: Callable[[], None]) -> None:
        """Attach the local clear hook after a coordinator is constructed."""
        with self._lock:
            self._clear = clear

    def generation(self) -> int:
        return self.sync()

    def _apply_generation(self, generation: int) -> bool:
        with self._lock:
            if generation <= self._generation:
                return False
            self._generation = generation
            clear = self._clear
        clear()
        return True

    def sync(self) -> int:
        """Clear local state if Redis advanced while Pub/Sub was unavailable."""
        if self._state is None:
            with self._lock:
                return self._generation
        generation = self._state.generation()
        self._apply_generation(generation)
        return generation

    def invalidate(self, event: "CacheInvalidationEvent") -> int:
        """Advance the shared generation and immediately clear this worker's L1."""
        if self._state is None:
            with self._lock:
                generation = self._generation + 1
        else:
            generation = int(self._state.invalidate(event))
        self._apply_generation(generation)
        return generation

    def receive(self, event: "CacheInvalidationEvent") -> bool:
        """Apply a best-effort Pub/Sub invalidation event without republishing it."""
        return self._apply_generation(int(event.sequence))


class GenerationGuardedCache:
    """Cache proxy that synchronizes shared generation before every L1 access."""

    def __init__(self, backing: CacheProtocol, guard: GenerationGuard) -> None:
        self._backing = backing
        self._guard = guard

    @property
    def guard(self) -> GenerationGuard:
        return self._guard

    def get(self, key: str) -> Optional[Any]:
        self._guard.sync()
        return self._backing.get(key)

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self._guard.sync()
        self._backing.set(key, value, ttl=ttl)

    def delete(self, key: str) -> None:
        self._guard.sync()
        self._backing.delete(key)

    def get_all_keys(self) -> list[str]:
        self._guard.sync()
        return self._backing.get_all_keys()

    def clear(self) -> None:
        """Clear local L1 state without trying to advance the shared generation."""
        self._backing.clear()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backing, name)
