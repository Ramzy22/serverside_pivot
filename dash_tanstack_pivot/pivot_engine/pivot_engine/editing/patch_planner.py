from __future__ import annotations

from typing import Dict, Iterable, List

from .scope_index import collect_impacted_scope_ids, normalize_scope_id


def build_impacted_scope_ids(scope_ids: Iterable[str]) -> List[str]:
    impacted: List[str] = []
    for scope_id in scope_ids:
        impacted.extend(collect_impacted_scope_ids(scope_id))
    return list(dict.fromkeys(scope for scope in impacted if scope))


def filter_visible_impacted_scope_ids(impacted_scope_ids: Iterable[str], visible_scope_ids: Iterable[str]) -> List[str]:
    visible = [
        normalized
        for scope_id in visible_scope_ids
        if (normalized := normalize_scope_id(scope_id))
    ]
    impacted = [
        normalized
        for scope_id in impacted_scope_ids
        if (normalized := normalize_scope_id(scope_id))
    ]
    impacted_lookup: set[str] = set()
    for impacted_scope in impacted:
        impacted_lookup.update(
            scope
            for scope in collect_impacted_scope_ids(impacted_scope)
            if scope != "__grand_total__"
        )
    if "__grand_total__" in impacted:
        impacted_lookup.add("__grand_total__")

    matched: List[str] = []
    seen: set[str] = set()
    for visible_scope in visible:
        if visible_scope in seen:
            continue
        visible_impacted_scopes = [
            scope
            for scope in collect_impacted_scope_ids(visible_scope)
            if scope != "__grand_total__" or visible_scope == "__grand_total__"
        ]
        if any(scope in impacted_lookup for scope in visible_impacted_scopes):
            seen.add(visible_scope)
            matched.append(visible_scope)
    return matched


def summarize_patch_scope_ids(impacted_scope_ids: Iterable[str], visible_scope_ids: Iterable[str]) -> Dict[str, List[str]]:
    impacted = list(dict.fromkeys(
        normalized
        for scope_id in impacted_scope_ids
        if (normalized := normalize_scope_id(scope_id))
    ))
    visible = list(dict.fromkeys(
        normalized
        for scope_id in visible_scope_ids
        if (normalized := normalize_scope_id(scope_id))
    ))
    return {
        "impactedScopeIds": impacted,
        "visibleImpactedScopeIds": filter_visible_impacted_scope_ids(impacted, visible),
    }
