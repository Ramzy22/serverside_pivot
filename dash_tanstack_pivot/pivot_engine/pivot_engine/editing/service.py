from __future__ import annotations

import copy
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional

from .conflict_manager import ConflictManager
from .models import GRAND_TOTAL_SCOPE_ID, PreparedEventAction, ScopeLock, SessionEventRecord
from .patch_planner import summarize_patch_scope_ids
from .propagation_planner import validate_real_propagation_policy
from .session_manager import EditSessionManager
from .target_resolver import build_affected_cells_payload, build_impacted_scopes_payload, resolve_scope_targets

OVERLAP_BLOCK_WARNING = "Blocked edit because the target scope overlaps an active edit in this session."


def _clone_transaction(transaction: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(transaction, dict):
        return None
    return copy.deepcopy(transaction)


def _collect_requested_event_ids(transaction_payload: Dict[str, Any]) -> List[str]:
    raw_event_ids = transaction_payload.get("eventIds")
    if raw_event_ids is None:
        raw_event_ids = transaction_payload.get("event_ids")
    if raw_event_ids is None:
        raw_event_ids = [
            transaction_payload.get("eventId")
            if transaction_payload.get("eventId") is not None
            else transaction_payload.get("event_id")
        ]
    elif not isinstance(raw_event_ids, (list, tuple, set)):
        raw_event_ids = [raw_event_ids]
    return [
        str(value).strip()
        for value in raw_event_ids
        if value is not None and str(value).strip()
    ]


def _merge_history_transactions(transactions: List[Dict[str, Any]], *, source: str, refresh_mode: str) -> Dict[str, Any]:
    merged: Dict[str, Any] = {
        "add": [],
        "remove": [],
        "update": [],
        "upsert": [],
        "source": source,
        "refreshMode": refresh_mode,
    }
    for transaction in transactions:
        if not isinstance(transaction, dict):
            continue
        for key in ("add", "remove", "update", "upsert"):
            if isinstance(transaction.get(key), list):
                merged[key].extend(copy.deepcopy(transaction.get(key) or []))
    return merged


def _build_inverse_normalized_transaction(event: SessionEventRecord, *, source: str, refresh_mode: str) -> Dict[str, Any]:
    normalized = _clone_transaction(event.normalized_transaction) or {
        "add": [],
        "remove": [],
        "update": [],
        "upsert": [],
    }
    original_updates = list(event.original_updates or [])
    for index, operation in enumerate(list(normalized.get("update") or [])):
        if not isinstance(operation, dict):
            continue
        original_update = original_updates[index] if index < len(original_updates) and isinstance(original_updates[index], dict) else {}
        aggregate_edit = operation.get("aggregate_edit") if isinstance(operation.get("aggregate_edit"), dict) else None
        if aggregate_edit is not None:
            previous_value = original_update.get("oldValue", aggregate_edit.get("oldValue"))
            next_value = original_update.get("value", aggregate_edit.get("newValue"))
            aggregate_edit["newValue"] = previous_value
            aggregate_edit["oldValue"] = next_value
            continue
        updates = operation.get("updates") if isinstance(operation.get("updates"), dict) else None
        if not updates:
            continue
        if "oldValue" not in original_update:
            continue
        previous_value = original_update.get("oldValue")
        operation["updates"] = {
            key: previous_value
            for key in list(updates.keys())
        }
    normalized["source"] = source
    normalized["refreshMode"] = refresh_mode
    return normalized


def _build_redo_normalized_transaction(event: SessionEventRecord, *, source: str, refresh_mode: str) -> Dict[str, Any]:
    normalized = _clone_transaction(event.normalized_transaction) or {
        "add": [],
        "remove": [],
        "update": [],
        "upsert": [],
    }
    normalized["source"] = source
    normalized["refreshMode"] = refresh_mode
    return normalized


def _build_replacement_normalized_transaction(
    events: List[SessionEventRecord],
    *,
    propagation_policy: str,
    source: str,
    refresh_mode: str,
) -> Dict[str, Any]:
    replacement_updates: List[Dict[str, Any]] = []
    for event in events:
        normalized = _clone_transaction(event.normalized_transaction) or {}
        for operation in list(normalized.get("update") or []):
            if not isinstance(operation, dict):
                continue
            aggregate_edit = operation.get("aggregate_edit") if isinstance(operation.get("aggregate_edit"), dict) else None
            if aggregate_edit is not None:
                aggregate_edit["propagationStrategy"] = propagation_policy
            replacement_updates.append(operation)
    return _merge_history_transactions(
        [{"update": replacement_updates}],
        source=source,
        refresh_mode=refresh_mode,
    )


def _merge_scope_value_changes(scope_value_changes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    ordered_keys: List[str] = []
    for change in scope_value_changes:
        if not isinstance(change, dict):
            continue
        scope_id = str(change.get("scopeId") or "").strip()
        measure_id = str(change.get("measureId") or "").strip()
        if not scope_id or not measure_id:
            continue
        merge_key = f"{scope_id}:::{measure_id}"
        if merge_key not in merged:
            merged[merge_key] = copy.deepcopy(change)
            ordered_keys.append(merge_key)
            continue
        merged_change = merged[merge_key]
        merged_change["afterValue"] = copy.deepcopy(change.get("afterValue"))
        if str(change.get("role") or "") == "direct":
            merged_change["role"] = "direct"
        if change.get("aggregationFn"):
            merged_change["aggregationFn"] = change.get("aggregationFn")
    return [merged[key] for key in ordered_keys]


def _resolve_runtime_row_scope_id(row: Dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    if row.get("_isTotal") or row.get("_path") == GRAND_TOTAL_SCOPE_ID or row.get("_id") == "Grand Total":
        return GRAND_TOTAL_SCOPE_ID
    if row.get("_path") not in {None, ""}:
        return str(row.get("_path"))
    if row.get("id") not in {None, ""}:
        return str(row.get("id"))
    if row.get("_id") not in {None, ""}:
        return str(row.get("_id"))
    return ""


class EditDomainService:
    def __init__(self) -> None:
        self.sessions = EditSessionManager()

    @staticmethod
    def _payload_session_parts(request: Any, transaction_payload: Dict[str, Any]) -> tuple[str, str, str]:
        table = str(getattr(request, "table", "") or transaction_payload.get("table") or "")
        session_id = str(transaction_payload.get("session_id") or transaction_payload.get("sessionId") or "anonymous")
        client_instance = str(transaction_payload.get("client_instance") or transaction_payload.get("clientInstance") or "default")
        return table, session_id, client_instance

    def get_or_create_session(self, request: Any, transaction_payload: Dict[str, Any]):
        table, session_id, client_instance = self._payload_session_parts(request, transaction_payload)
        return self.sessions.get_or_create_session(table=table, session_id=session_id, client_instance=client_instance)

    def get_or_create_session_from_parts(self, *, table: str, session_id: str, client_instance: str):
        return self.sessions.get_or_create_session(table=table, session_id=session_id, client_instance=client_instance)

    @staticmethod
    def _normalize_event_ids(event_ids: Optional[Iterable[Any]]) -> List[str]:
        return [
            str(event_id or "").strip()
            for event_id in (event_ids or [])
            if str(event_id or "").strip()
        ]

    @staticmethod
    def _build_scope_locks_payload(session_locks: List[ScopeLock]) -> List[Dict[str, Any]]:
        return [
            {
                "scopeId": lock.scope_id,
                "measureId": lock.measure_id,
                "lockMode": lock.lock_mode,
                "ownerEventId": lock.owner_event_id,
            }
            for lock in session_locks
        ]

    def _resolve_session_events(
        self,
        session: Any,
        event_ids: Optional[Iterable[Any]],
        *,
        require_active: Optional[bool] = None,
    ) -> List[SessionEventRecord]:
        resolved_events: List[SessionEventRecord] = []
        for event_id in self._normalize_event_ids(event_ids):
            event = self.sessions.get_event(event_id)
            if event is None or event.session_key != session.session_key:
                continue
            if require_active is not None and bool(event.active) != require_active:
                continue
            resolved_events.append(event)
        return resolved_events

    def _resolve_undo_target_event(self, session: Any, event_id: str) -> Optional[SessionEventRecord]:
        normalized_event_id = str(event_id or "").strip()
        if normalized_event_id:
            events = self._resolve_session_events(session, [normalized_event_id], require_active=True)
            return events[0] if events else None
        if not session.active_event_ids:
            return None
        active_events = self._resolve_session_events(session, [session.active_event_ids[-1]], require_active=True)
        return active_events[0] if active_events else None

    def _validate_scope_targets(
        self,
        session: Any,
        proposed_targets: List[Any],
        *,
        exclude_owner_event_ids: Optional[Iterable[Any]] = None,
    ) -> Dict[str, Any]:
        conflicts = ConflictManager.find_conflicts(
            self.sessions.current_locks(session),
            proposed_targets,
            exclude_owner_event_ids=self._normalize_event_ids(exclude_owner_event_ids),
        )
        warnings: List[str] = []
        if conflicts:
            warnings.append(OVERLAP_BLOCK_WARNING)
        return {"session": session, "warnings": warnings, "conflicts": conflicts}

    def build_visible_edit_overlay(
        self,
        request: Any,
        *,
        session_id: str,
        client_instance: str,
        rows: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        table = str(getattr(request, "table", "") or "")
        if not table or not rows:
            return None
        session = self.get_or_create_session_from_parts(
            table=table,
            session_id=session_id or "anonymous",
            client_instance=client_instance or "default",
        )
        overlay_index = self.sessions.current_overlay_index(
            session,
            grouping_fields=list(getattr(request, "grouping", None) or []),
        )
        if not overlay_index:
            return None

        cells: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            scope_id = _resolve_runtime_row_scope_id(row)
            if not scope_id:
                continue
            row_overlay = overlay_index.get(scope_id) or {}
            if not row_overlay:
                continue
            for measure_id, cell_entry in row_overlay.items():
                if measure_id not in row or not isinstance(cell_entry, dict):
                    continue
                direct_event_ids = list(cell_entry.get("directEventIds") or [])
                propagated_event_ids = list(cell_entry.get("propagatedEventIds") or [])
                cells.append(
                    {
                        "rowId": scope_id,
                        "colId": measure_id,
                        "originalValue": copy.deepcopy(cell_entry.get("originalValue")),
                        "comparisonCount": len(direct_event_ids) + len(propagated_event_ids),
                        "directEventIds": direct_event_ids,
                        "propagatedEventIds": propagated_event_ids,
                    }
                )

        if not cells:
            return None
        cells.sort(key=lambda entry: f"{entry['rowId']}:::{entry['colId']}")
        return {
            "cells": cells,
            "sessionVersion": int(session.session_version or 0),
        }

    def validate_transaction(
        self,
        request: Any,
        transaction_payload: Dict[str, Any],
        normalized_transaction: Dict[str, Any],
    ) -> Dict[str, Any]:
        session = self.get_or_create_session(request, transaction_payload)
        warnings: List[str] = []
        proposed_targets = resolve_scope_targets(request, normalized_transaction)
        for operation in list(normalized_transaction.get("update") or []):
            aggregate_edit = operation.get("aggregate_edit") if isinstance(operation.get("aggregate_edit"), dict) else None
            if not aggregate_edit:
                continue
            operation_targets = resolve_scope_targets(
                request,
                {
                    "add": [],
                    "remove": [],
                    "update": [operation],
                    "upsert": [],
                },
            )
            target = operation_targets[0] if operation_targets else None
            if target is None or target.lock_mode != "subtree":
                continue
            propagation_policy = validate_real_propagation_policy(
                aggregate_edit.get("propagationStrategy") or aggregate_edit.get("propagationFormula")
            )
            if propagation_policy is None:
                warnings.append("Aggregate propagation policy 'none' is no longer supported for persisted edits.")
        if warnings:
            return {"session": session, "warnings": warnings, "conflicts": []}
        return self._validate_scope_targets(session, proposed_targets)

    def validate_prepared_event_action(
        self,
        request: Any,
        transaction_payload: Dict[str, Any],
        action: PreparedEventAction,
    ) -> Dict[str, Any]:
        session = self.get_or_create_session(request, transaction_payload)
        target_event_ids = self._normalize_event_ids(action.target_event_ids)
        warnings: List[str] = []

        if action.action == "undo":
            if not target_event_ids:
                warnings.append("There is no active edit to undo.")
        elif action.action == "redo":
            if not target_event_ids:
                warnings.append("There is no edit available to redo.")
        elif action.action == "revert":
            if not target_event_ids:
                warnings.append("Only active edits can be reverted.")
        elif action.action == "replace":
            if not target_event_ids:
                warnings.append("Only active edits can be replaced.")

        if warnings:
            return {"session": session, "warnings": warnings, "conflicts": []}

        proposed_targets = resolve_scope_targets(request, action.normalized_transaction or {})
        excluded_owner_event_ids = target_event_ids if action.action in {"undo", "revert", "replace"} else []
        return self._validate_scope_targets(
            session,
            proposed_targets,
            exclude_owner_event_ids=excluded_owner_event_ids,
        )

    def prepare_event_action(
        self,
        request: Any,
        transaction_payload: Dict[str, Any],
        normalized_transaction: Optional[Dict[str, Any]] = None,
    ) -> Optional[PreparedEventAction]:
        action = str(
            transaction_payload.get("eventAction")
            or transaction_payload.get("event_action")
            or ""
        ).strip().lower()
        if not action:
            return None
        session = self.get_or_create_session(request, transaction_payload)
        refresh_mode = str(transaction_payload.get("refreshMode") or transaction_payload.get("refresh_mode") or "smart")
        if action in {"undo", "redo"}:
            event_id = str(transaction_payload.get("eventId") or transaction_payload.get("event_id") or "").strip()
            if action == "undo":
                target_event = self._resolve_undo_target_event(session, event_id)
                if not target_event:
                    return PreparedEventAction(action=action, session_key=session.session_key, source=action, normalized_transaction={})
                return PreparedEventAction(
                    action=action,
                    session_key=session.session_key,
                    source=action,
                    normalized_transaction=_build_inverse_normalized_transaction(
                        target_event,
                        source=action,
                        refresh_mode=refresh_mode,
                    ),
                    target_event_ids=[target_event.event_id],
                )
            target_event = self.sessions.peek_redo_event(session, event_id=event_id)
            if target_event is not None and target_event.session_key != session.session_key:
                target_event = None
            if not target_event:
                return PreparedEventAction(action=action, session_key=session.session_key, source=action, normalized_transaction={})
            return PreparedEventAction(
                action=action,
                session_key=session.session_key,
                source=action,
                normalized_transaction=_build_redo_normalized_transaction(
                    target_event,
                    source=action,
                    refresh_mode=refresh_mode,
                ),
                target_event_ids=[target_event.event_id],
            )
        if action == "revert":
            event_ids = _collect_requested_event_ids(transaction_payload)
            events = self._resolve_session_events(session, event_ids, require_active=True)
            events = list(reversed(events))
            return PreparedEventAction(
                action=action,
                session_key=session.session_key,
                source=action,
                normalized_transaction=_merge_history_transactions(
                    [
                        _build_inverse_normalized_transaction(
                            event,
                            source=action,
                            refresh_mode=refresh_mode,
                        )
                        for event in events
                    ],
                    source=action,
                    refresh_mode=refresh_mode,
                ),
                target_event_ids=[event.event_id for event in events],
            )
        if action == "replace":
            event_ids = _collect_requested_event_ids(transaction_payload)
            target_events = [
                event
                for event in self._resolve_session_events(session, event_ids, require_active=True)
                if event.inverse_transaction
            ]
            if not target_events:
                return PreparedEventAction(action=action, session_key=session.session_key, source=action, normalized_transaction={})
            merged = _merge_history_transactions(
                [
                    _build_inverse_normalized_transaction(
                        event,
                        source=action,
                        refresh_mode=refresh_mode,
                    )
                    for event in reversed(target_events)
                ],
                source=action,
                refresh_mode=refresh_mode,
            )
            explicit_replacement_transaction = _clone_transaction(normalized_transaction)
            if explicit_replacement_transaction and any(
                explicit_replacement_transaction.get(kind)
                for kind in ("add", "remove", "update", "upsert")
            ):
                record_original_updates = copy.deepcopy(
                    explicit_replacement_transaction.get("update")
                    or transaction_payload.get("update")
                    or transaction_payload.get("updates")
                    or []
                )
                merged = _merge_history_transactions(
                    [merged, explicit_replacement_transaction],
                    source=action,
                    refresh_mode=refresh_mode,
                )
                replacement_updates = copy.deepcopy(record_original_updates)
                record_normalized_transaction = explicit_replacement_transaction
            else:
                propagation_policy = validate_real_propagation_policy(
                    transaction_payload.get("propagationStrategy") or transaction_payload.get("propagation_policy")
                )
                if propagation_policy is None:
                    return PreparedEventAction(action=action, session_key=session.session_key, source=action, normalized_transaction={})
                replacement_updates = []
                for target_event in target_events:
                    for update in list(target_event.original_updates or []):
                        if not isinstance(update, dict):
                            continue
                        replacement_updates.append(
                            {
                                **copy.deepcopy(update),
                                "propagationStrategy": propagation_policy,
                            }
                        )
                replacement_transaction = _build_replacement_normalized_transaction(
                    target_events,
                    propagation_policy=propagation_policy,
                    source=action,
                    refresh_mode=refresh_mode,
                )
                merged["update"] = list(merged.get("update") or []) + list(replacement_transaction.get("update") or [])
                record_normalized_transaction = replacement_transaction
                record_original_updates = copy.deepcopy(replacement_updates)
            return PreparedEventAction(
                action=action,
                session_key=session.session_key,
                source=action,
                normalized_transaction=merged,
                target_event_ids=[event.event_id for event in target_events],
                replacement_updates=replacement_updates,
                record_normalized_transaction=record_normalized_transaction,
                record_original_updates=record_original_updates,
            )
        return None

    def finalize_event_action(
        self,
        request: Any,
        transaction_payload: Dict[str, Any],
        action: PreparedEventAction,
        transaction_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        session = self.get_or_create_session(request, transaction_payload)
        if list(transaction_result.get("conflicts") or []):
            return transaction_result
        if action.action in {"revert", "replace"} and action.target_event_ids:
            self.sessions.deactivate_events(session, action.target_event_ids)
        if action.action == "undo" and action.target_event_ids:
            self.sessions.push_undone_events(session, action.target_event_ids)
        if action.action == "redo" and action.target_event_ids:
            self.sessions.activate_redo_events(session, action.target_event_ids)
        return transaction_result

    def enrich_transaction_result(
        self,
        request: Any,
        transaction_payload: Dict[str, Any],
        normalized_transaction: Dict[str, Any],
        transaction_result: Dict[str, Any],
        prepared_event_action: Optional[PreparedEventAction] = None,
    ) -> Dict[str, Any]:
        session = self.get_or_create_session(request, transaction_payload)
        applied = transaction_result.get("applied") if isinstance(transaction_result.get("applied"), dict) else {}
        did_apply = any(int(value or 0) > 0 for value in applied.values())
        response = dict(transaction_result)
        record_normalized_transaction = (
            _clone_transaction(prepared_event_action.record_normalized_transaction)
            if prepared_event_action and prepared_event_action.record_normalized_transaction
            else _clone_transaction(normalized_transaction)
        ) or {
            "add": [],
            "remove": [],
            "update": [],
            "upsert": [],
        }
        record_original_updates = copy.deepcopy(
            prepared_event_action.record_original_updates
            if prepared_event_action and prepared_event_action.record_original_updates
            else (transaction_payload.get("update") or transaction_payload.get("updates") or [])
        )
        response["editSession"] = {
            "sessionId": session.session_id,
            "clientInstance": session.client_instance,
            "sessionKey": session.session_key,
            "sessionVersion": int(session.session_version or 0),
        }
        response["affectedCells"] = build_affected_cells_payload(request, transaction_payload, record_normalized_transaction)
        impacted_scope_ids = build_impacted_scopes_payload(request, record_normalized_transaction)
        visible_scope_ids = transaction_payload.get("visibleRowPaths") or []
        response.update(summarize_patch_scope_ids(impacted_scope_ids, visible_scope_ids))
        if not did_apply:
            response["scopeLocks"] = self._build_scope_locks_payload(self.sessions.current_locks(session))
            return response
        history_result = response.get("history") if isinstance(response.get("history"), dict) else {}
        if not bool(history_result.get("captureable")):
            response["scopeLocks"] = self._build_scope_locks_payload(self.sessions.current_locks(session))
            return response
        event_id = f"evt_{uuid.uuid4().hex[:12]}"
        next_version = int(session.session_version or 0) + 1
        source = str(transaction_payload.get("source") or response.get("source") or "transaction")
        proposed_targets = resolve_scope_targets(request, record_normalized_transaction)
        scope_locks = [
            ScopeLock(
                scope_id=target.scope_id,
                measure_id=target.measure_id,
                lock_mode=target.lock_mode,
                owner_event_id=event_id,
            )
            for target in proposed_targets
        ]
        scope_value_changes = _merge_scope_value_changes(
            list(response.get("scopeValueChanges") or [])
        )
        record = SessionEventRecord(
            event_id=event_id,
            session_key=session.session_key,
            session_version=next_version,
            source=source,
            created_at=time.time(),
            normalized_transaction=record_normalized_transaction,
            inverse_transaction=_clone_transaction(response.get("inverseTransaction")),
            redo_transaction=_clone_transaction(response.get("redoTransaction")),
            original_updates=record_original_updates,
            affected_cells=copy.deepcopy(response.get("affectedCells") or {"direct": [], "propagated": []}),
            impacted_scope_ids=list(impacted_scope_ids),
            grouping_fields=list(getattr(request, "grouping", None) or []),
            scope_value_changes=scope_value_changes,
            scope_locks=scope_locks,
        )
        self.sessions.register_event(session, record)
        response["eventId"] = event_id
        response["sessionVersion"] = next_version
        response["editSession"] = {
            "sessionId": session.session_id,
            "clientInstance": session.client_instance,
            "sessionKey": session.session_key,
            "sessionVersion": next_version,
        }
        response["scopeLocks"] = self._build_scope_locks_payload(self.sessions.current_locks(session))
        return response
