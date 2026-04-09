from __future__ import annotations

from typing import Dict, Iterable, List

from .scope_index import collect_impacted_scope_ids, is_ancestor_scope, normalize_scope_id


def build_impacted_scope_ids(scope_ids: Iterable[str]) -> List[str]:
    impacted: List[str] = []
    for scope_id in scope_ids:
        impacted.extend(collect_impacted_scope_ids(scope_id))
    return list(dict.fromkeys(scope for scope in impacted if scope))


def filter_visible_impacted_scope_ids(impacted_scope_ids: Iterable[str], visible_scope_ids: Iterable[str]) -> List[str]:
    visible = [normalize_scope_id(scope_id) for scope_id in visible_scope_ids if normalize_scope_id(scope_id)]
    impacted = [normalize_scope_id(scope_id) for scope_id in impacted_scope_ids if normalize_scope_id(scope_id)]
    matched: List[str] = []
    for visible_scope in visible:
        for impacted_scope in impacted:
            if visible_scope == impacted_scope or is_ancestor_scope(impacted_scope, visible_scope) or is_ancestor_scope(visible_scope, impacted_scope):
                matched.append(visible_scope)
                break
    return list(dict.fromkeys(matched))


def summarize_patch_scope_ids(impacted_scope_ids: Iterable[str], visible_scope_ids: Iterable[str]) -> Dict[str, List[str]]:
    impacted = list(dict.fromkeys([normalize_scope_id(scope_id) for scope_id in impacted_scope_ids if normalize_scope_id(scope_id)]))
    visible = list(dict.fromkeys([normalize_scope_id(scope_id) for scope_id in visible_scope_ids if normalize_scope_id(scope_id)]))
    return {
        "impactedScopeIds": impacted,
        "visibleImpactedScopeIds": filter_visible_impacted_scope_ids(impacted, visible),
    }
