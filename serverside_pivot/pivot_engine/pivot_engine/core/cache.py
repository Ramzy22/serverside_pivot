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
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from .authority.scope import current_scope

__all__ = ["CacheKey", "CacheManager", "TIERS", "key_for"]

TIERS = ("plan", "result", "reactive", "snapshot")


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
    def __init__(self, budgets: Optional[Dict[str, int]] = None, sizeof: Callable[[Any], int] = _default_size):
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
            if key in entries:
                self._used[key.tier] -= entries.pop(key)[1]
            if size > self._budgets[key.tier]:
                return  # larger than the whole tier: never cached
            entries[key] = (value, size)
            self._used[key.tier] += size
            while self._used[key.tier] > self._budgets[key.tier]:
                _, (_, evicted) = entries.popitem(last=False)
                self._used[key.tier] -= evicted

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
        removed = 0
        with self._lock:
            for tier in tiers:
                entries = self._entries[tier]
                for key in [k for k in entries if (dataset is None or k.dataset == dataset)
                            and (keep_revision is None or k.revision != keep_revision)]:
                    self._used[tier] -= entries.pop(key)[1]
                    removed += 1
        return removed

    def used_bytes(self, tier: str) -> int:
        return self._used[tier]
