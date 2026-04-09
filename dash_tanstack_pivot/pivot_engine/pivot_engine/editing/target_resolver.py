from __future__ import annotations

from typing import Any, Dict, List

from .models import GRAND_TOTAL_SCOPE_ID, ResolvedScopeTarget
from .patch_planner import build_impacted_scope_ids
from .scope_index import collect_impacted_scope_ids, normalize_scope_id, scope_depth


def _grouping_depth(request: Any) -> int:
    grouping = getattr(request, "grouping", None)
    return len(grouping or []) if isinstance(grouping, list) else 0


def _scope_id_from_key_columns(request: Any, key_columns: Dict[str, Any]) -> str:
    grouping = getattr(request, "grouping", None) or []
    if not isinstance(key_columns, dict):
        return ""
    parts: List[str] = []
    for field in grouping:
        if field not in key_columns or key_columns.get(field) is None:
            break
        parts.append(str(key_columns.get(field)))
    return normalize_scope_id("|||".join(parts))


def resolve_scope_targets(request: Any, normalized_transaction: Dict[str, Any]) -> List[ResolvedScopeTarget]:
    targets: List[ResolvedScopeTarget] = []
    grouping_depth = _grouping_depth(request)
    for operation in list(normalized_transaction.get("update") or []):
        if not isinstance(operation, dict):
            continue
        aggregate_edit = operation.get("aggregate_edit") if isinstance(operation.get("aggregate_edit"), dict) else None
        edit_meta = operation.get("edit_meta") if isinstance(operation.get("edit_meta"), dict) else {}
        row_id = normalize_scope_id(
            edit_meta.get("rowPath")
            or edit_meta.get("rowId")
            or (aggregate_edit or {}).get("rowPath")
            or (aggregate_edit or {}).get("rowId")
            or _scope_id_from_key_columns(request, operation.get("key_columns") or {})
        )
        col_id = str(
            edit_meta.get("colId")
            or (aggregate_edit or {}).get("columnId")
            or next(iter((operation.get("updates") or {}).keys()), "")
            or ""
        ).strip()
        if not row_id or not col_id:
            continue
        depth = scope_depth(row_id)
        is_aggregate = aggregate_edit is not None
        lock_mode = "exact_scope"
        if row_id == GRAND_TOTAL_SCOPE_ID:
            lock_mode = "subtree"
        elif is_aggregate and grouping_depth > 0 and 0 < depth < grouping_depth:
            lock_mode = "subtree"
        targets.append(
            ResolvedScopeTarget(
                scope_id=row_id,
                measure_id=col_id,
                lock_mode=lock_mode,
                row_id=row_id,
                col_id=col_id,
                is_aggregate=is_aggregate,
                source=str(operation.get("source") or "update"),
            )
        )
    return targets


def build_affected_cells_payload(
    request: Any,
    transaction_payload: Dict[str, Any],
    normalized_transaction: Dict[str, Any],
) -> Dict[str, List[str]]:
    visible_paths = [
        normalize_scope_id(path)
        for path in (transaction_payload.get("visibleRowPaths") or [])
        if normalize_scope_id(path)
    ]
    direct: List[str] = []
    propagated: List[str] = []
    for target in resolve_scope_targets(request, normalized_transaction):
        direct_key = f"{target.scope_id}:::{target.measure_id}"
        if direct_key not in direct:
            direct.append(direct_key)
        for impacted_scope in collect_impacted_scope_ids(target.scope_id):
            impacted_key = f"{impacted_scope}:::{target.measure_id}"
            if impacted_key != direct_key and impacted_key not in propagated:
                propagated.append(impacted_key)
        if target.lock_mode == "subtree":
            prefix = f"{target.scope_id}|||"
            for visible_path in visible_paths:
                if visible_path == target.scope_id:
                    continue
                if target.scope_id == GRAND_TOTAL_SCOPE_ID or visible_path.startswith(prefix):
                    impacted_key = f"{visible_path}:::{target.measure_id}"
                    if impacted_key not in direct and impacted_key not in propagated:
                        propagated.append(impacted_key)
    return {"direct": direct, "propagated": propagated}


def build_impacted_scopes_payload(request: Any, normalized_transaction: Dict[str, Any]) -> List[str]:
    scope_ids = [target.scope_id for target in resolve_scope_targets(request, normalized_transaction)]
    return build_impacted_scope_ids(scope_ids)
