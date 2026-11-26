"""One cache manager for the core (replaces the ~20 ad-hoc caches, F-CACHES).

Keys are structural, never ``id()``-based:
    (tier, dataset, plan fingerprint, scope fingerprint, policy version, revision token)
The scope fingerprint makes cross-tenant reuse impossible by construction
(REQ-SEC-004); the revision token makes staleness explicit.  Each tier has its
own byte budget with LRU eviction.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .authority.scope import current_scope

__all__ = ["CacheKey", "CacheManager", "TIERS", "key_for"]

TIERS = ("plan", "result", "cube", "reactive", "snapshot")


@dataclass(frozen=True)
class CacheKey:
    tier: str
    dataset: str
    plan_fingerprint: str
    scope_fingerprint: str
    policy_version: str
    revision: str


def key_for(tier: str, dataset: str, plan_fingerprint: str, revision: str = "unversioned") -> CacheKey:
    """Build a key under the *active* scope (never pass a scope in by hand)."""
    if tier not in TIERS:
        raise ValueError(f"unknown cache tier {tier!r}")
    ctx = current_scope()
    grant = ctx.grant_for(dataset)
    scope_fp = str(getattr(grant, "scope_fingerprint", None) or "global")
    policy = str(getattr(grant, "policy_version", "") or "")
    return CacheKey(tier, dataset, plan_fingerprint, scope_fp, policy, revision)


def _default_size(value: Any) -> int:
    nbytes = getattr(value, "nbytes", None)
    if isinstance(nbytes, int):
        return nbytes
    table = getattr(value, "table", None)
    nbytes = getattr(table, "nbytes", None)
    return nbytes if isinstance(nbytes, int) else 1024


class CacheManager:
    def __init__(
        self,
        budgets: Optional[Dict[str, int]] = None,
        sizeof: Callable[[Any], int] = _default_size,
        on_evict: Optional[Callable[[CacheKey, Any], None]] = None,
    ):
        self._on_evict = on_evict
        self._budgets = {t: (budgets or {}).get(t, 256 * 1024 * 1024) for t in TIERS}
        self._sizeof = sizeof
        self._lock = threading.Lock()
        self._entries: Dict[str, OrderedDict[CacheKey, Tuple[Any, int]]] = {t: OrderedDict() for t in TIERS}
        self._used = {t: 0 for t in TIERS}
        self.hits = 0
        self.misses = 0

    def get(self, key: CacheKey) -> Optional[Any]:
        with self._lock:
            entries = self._entries[key.tier]
            item = entries.get(key)
            if item is None:
                self.misses += 1
                return None
            entries.move_to_end(key)
            self.hits += 1
            return item[0]

    def put(self, key: CacheKey, value: Any) -> None:
        size = max(0, int(self._sizeof(value)))
        with self._lock:
            entries = self._entries[key.tier]
            dropped: List[Tuple[CacheKey, Any]] = []
            if key in entries:
                old_value, old_size = entries.pop(key)
                self._used[key.tier] -= old_size
                if old_value is not value:
                    dropped.append((key, old_value))
            if size > self._budgets[key.tier]:
                dropped.append((key, value))  # larger than the whole tier: never cached
            else:
                entries[key] = (value, size)
                self._used[key.tier] += size
                while self._used[key.tier] > self._budgets[key.tier]:
                    evicted_key, (evicted_value, evicted_size) = entries.popitem(last=False)
                    self._used[key.tier] -= evicted_size
                    dropped.append((evicted_key, evicted_value))
        self._release(dropped)

    def get_or_compute(self, key: CacheKey, compute: Callable[[], Any]) -> Any:
        cached = self.get(key)
        if cached is not None:
            return cached
        value = compute()
        self.put(key, value)
        return value

    def invalidate(self, *, dataset: Optional[str] = None, tiers: Iterable[str] = TIERS,
                   keep_revision: Optional[str] = None) -> int:
        """Drop entries of ``dataset`` (all if None), except those at ``keep_revision``."""
        dropped: List[Tuple[CacheKey, Any]] = []
        with self._lock:
            for tier in tiers:
                entries = self._entries[tier]
                for key in [k for k in entries if (dataset is None or k.dataset == dataset)
                            and (keep_revision is None or k.revision != keep_revision)]:
                    value, size = entries.pop(key)
                    self._used[tier] -= size
                    dropped.append((key, value))
        self._release(dropped)
        return len(dropped)

    def discard(self, key: CacheKey) -> None:
        with self._lock:
            item = self._entries[key.tier].pop(key, None)
            if item is not None:
                self._used[key.tier] -= item[1]
        if item is not None:
            self._release([(key, item[0])])

    def _release(self, dropped: List[Tuple[CacheKey, Any]]) -> None:
        if self._on_evict is None:
            return
        for key, value in dropped:
            self._on_evict(key, value)

    def used_bytes(self, tier: str) -> int:
        return self._used[tier]
