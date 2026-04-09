from __future__ import annotations

from typing import Iterable, List

from .models import GRAND_TOTAL_SCOPE_ID


def normalize_scope_id(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text in {"Grand Total", GRAND_TOTAL_SCOPE_ID}:
        return GRAND_TOTAL_SCOPE_ID
    return text


def scope_depth(scope_id: str) -> int:
    normalized = normalize_scope_id(scope_id)
    if not normalized or normalized == GRAND_TOTAL_SCOPE_ID:
        return 0
    return len([part for part in normalized.split("|||") if part])


def is_grand_total_scope(scope_id: str) -> bool:
    return normalize_scope_id(scope_id) == GRAND_TOTAL_SCOPE_ID


def is_ancestor_scope(ancestor_scope_id: str, descendant_scope_id: str) -> bool:
    ancestor = normalize_scope_id(ancestor_scope_id)
    descendant = normalize_scope_id(descendant_scope_id)
    if not ancestor or not descendant or ancestor == descendant:
        return False
    if ancestor == GRAND_TOTAL_SCOPE_ID:
        return descendant != GRAND_TOTAL_SCOPE_ID
    return descendant.startswith(f"{ancestor}|||")


def scopes_overlap(left_scope_id: str, left_lock_mode: str, right_scope_id: str, right_lock_mode: str) -> bool:
    left = normalize_scope_id(left_scope_id)
    right = normalize_scope_id(right_scope_id)
    if not left or not right:
        return False
    if left == right:
        return True
    if is_grand_total_scope(left) or is_grand_total_scope(right):
        return True
    if left_lock_mode == "subtree" and is_ancestor_scope(left, right):
        return True
    if right_lock_mode == "subtree" and is_ancestor_scope(right, left):
        return True
    return False


def collect_impacted_scope_ids(scope_id: str) -> List[str]:
    normalized = normalize_scope_id(scope_id)
    if not normalized:
        return []
    if normalized == GRAND_TOTAL_SCOPE_ID:
        return [GRAND_TOTAL_SCOPE_ID]
    parts = normalized.split("|||")
    impacted = [normalized]
    for depth in range(len(parts) - 1, 0, -1):
        impacted.append("|||".join(parts[:depth]))
    impacted.append(GRAND_TOTAL_SCOPE_ID)
    return list(dict.fromkeys([scope for scope in impacted if scope]))


def visible_scope_ids_from_paths(paths: Iterable[str]) -> List[str]:
    return [normalize_scope_id(path) for path in paths if normalize_scope_id(path)]
