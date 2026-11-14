from __future__ import annotations

from typing import Iterable, List, Optional, Set

from .models import ResolvedScopeTarget, ScopeLock
from .scope_index import scopes_overlap


class ConflictManager:
    @staticmethod
    def find_conflicts(
        existing_locks: List[ScopeLock],
        proposed_targets: List[ResolvedScopeTarget],
        *,
        exclude_owner_event_ids: Optional[Iterable[str]] = None,
    ) -> List[dict]:
        conflicts: List[dict] = []
        excluded_owner_ids: Set[str] = {
            str(event_id or "").strip()
            for event_id in (exclude_owner_event_ids or [])
            if str(event_id or "").strip()
        }
        for target in proposed_targets:
            for lock in existing_locks:
                if not isinstance(lock, ScopeLock):
                    continue
                if lock.owner_event_id in excluded_owner_ids:
                    continue
                if target.measure_id != lock.measure_id:
                    continue
                if scopes_overlap(lock.scope_id, lock.lock_mode, target.scope_id, target.lock_mode):
                    conflicts.append(
                        {
                            "scopeId": target.scope_id,
                            "measureId": target.measure_id,
                            "blockedByScopeId": lock.scope_id,
                            "blockedByEventId": lock.owner_event_id,
                        }
                    )
        return conflicts
