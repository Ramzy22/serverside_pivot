import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import get_type_hints

import pyarrow as pa
import pytest

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "pivot_engine"))
sys.path.append(os.path.join(os.getcwd(), "dash_tanstack_pivot"))

from pivot_engine import create_tanstack_adapter
from pivot_engine.editing.models import EditSessionState, OverlayIndex, OverlayIndexByGrouping, PreparedEventAction, ScopeLock, SessionEventRecord
from pivot_engine.editing.scope_index import scopes_overlap
from pivot_engine.editing.session_manager import EditSessionManager
from pivot_engine.editing.service import EditDomainService, _build_inverse_normalized_transaction
from pivot_engine.editing.target_resolver import build_affected_cells_payload
from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState, SessionRequestGate


VISIBLE_ROW_PATHS = ["North", "North|||Canada", "North|||USA", "South", "South|||Brazil", "__grand_total__"]
OVERLAP_BLOCK_WARNING = "Blocked edit because the target scope overlaps an active edit in this session."


def _make_service():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "North", "South"],
            "country": ["USA", "USA", "Canada", "Brazil"],
            "sales": [100.0, 20.0, 80.0, 120.0],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)
    return PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())


def _make_context(session_id, client_instance, window_seq):
    return PivotRequestContext.from_frontend(
        table="sales_data",
        trigger_prop="pivot-grid.runtimeRequest",
        viewport={
            "start": 0,
            "end": 20,
            "window_seq": window_seq,
            "state_epoch": 1,
            "abort_generation": 1,
            "session_id": session_id,
            "client_instance": client_instance,
            "intent": "viewport",
            "needs_col_schema": False,
        },
    )


def _make_state(transaction_request, *, expanded=None):
    return PivotViewState(
        row_fields=["region", "country"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        expanded={"North": True, "South": True} if expanded is None else expanded,
        show_col_totals=False,
        transaction_request=transaction_request,
    )


def _process(service, transaction_request, *, session_id="sess-edit", client_instance="grid-edit", window_seq=1, expanded=None):
    return service.process(
        _make_state(transaction_request, expanded=expanded),
        _make_context(session_id, client_instance, window_seq),
    )


def _row_values_by_path(response):
    return {
        row.get("_path"): row
        for row in (response.data or [])
        if isinstance(row, dict) and row.get("_path")
    }


def test_runtime_service_rejects_parent_only_none_propagation():
    service = _make_service()

    response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North",
                    "rowPath": "North",
                    "colId": "sales_sum",
                    "value": 260,
                    "oldValue": 200,
                    "propagationStrategy": "none",
                }
            ],
        },
    )

    assert response.transaction_result is not None
    assert response.transaction_result["warnings"][0] == "Aggregate propagation policy 'none' is no longer supported for persisted edits."
    assert all(int(count or 0) == 0 for count in response.transaction_result["applied"].values())


def test_edit_domain_validates_none_propagation_when_earlier_update_has_no_resolved_target():
    domain = EditDomainService()
    request = SimpleNamespace(table="sales_data", grouping=["region", "country"])
    normalized_transaction = {
        "add": [],
        "remove": [],
        "update": [
            {
                "key_columns": {"region": "North"},
            },
            {
                "key_columns": {"region": "North"},
                "edit_meta": {
                    "rowId": "North",
                    "rowPath": "North",
                    "colId": "sales_sum",
                    "groupingFields": ["region", "country"],
                },
                "aggregate_edit": {
                    "rowId": "North",
                    "rowPath": "North",
                    "columnId": "sales_sum",
                    "propagationStrategy": "none",
                },
            },
        ],
        "upsert": [],
    }

    validation = domain.validate_transaction(
        request,
        {"session_id": "sess-index", "client_instance": "grid-index"},
        normalized_transaction,
    )

    assert validation["warnings"] == [
        "Aggregate propagation policy 'none' is no longer supported for persisted edits."
    ]


def test_build_inverse_normalized_transaction_restores_explicit_null_old_values():
    inverse = _build_inverse_normalized_transaction(
        SessionEventRecord(
            event_id="evt_null_undo",
            session_key="sales_data::sess-null::grid-null",
            session_version=1,
            source="apply",
            created_at=0.0,
            normalized_transaction={
                "add": [],
                "remove": [],
                "update": [
                    {
                        "key_columns": {"region": "North", "country": "USA"},
                        "updates": {"sales": 150},
                    }
                ],
                "upsert": [],
            },
            original_updates=[{"oldValue": None}],
        ),
        source="undo",
        refresh_mode="patch",
    )

    assert inverse["update"][0]["updates"] == {"sales": None}


def test_build_inverse_normalized_transaction_matches_original_updates_by_scope_identity():
    inverse = _build_inverse_normalized_transaction(
        SessionEventRecord(
            event_id="evt_identity_undo",
            session_key="sales_data::sess-identity::grid-identity",
            session_version=1,
            source="apply",
            created_at=0.0,
            normalized_transaction={
                "add": [],
                "remove": [],
                "update": [
                    {
                        "key_columns": {"region": "North"},
                        "edit_meta": {"rowId": "North", "rowPath": "North", "colId": "sales_sum"},
                        "aggregate_edit": {
                            "column": "sales",
                            "rowId": "North",
                            "rowPath": "North",
                            "columnId": "sales_sum",
                            "oldValue": 200,
                            "newValue": 260,
                        },
                    },
                    {
                        "key_columns": {"region": "North", "country": "USA"},
                        "edit_meta": {"rowId": "North|||USA", "rowPath": "North|||USA", "colId": "sales_sum"},
                        "updates": {"sales": 150},
                    },
                ],
                "upsert": [],
            },
            original_updates=[
                {"rowId": "South", "colId": "sales_sum", "oldValue": 120, "value": 150},
                {"rowId": "North|||USA", "colId": "sales_sum", "oldValue": 100, "value": 150},
                {"rowId": "North", "colId": "sales_sum", "oldValue": 200, "value": 260},
            ],
        ),
        source="undo",
        refresh_mode="patch",
    )

    assert inverse["update"][0]["aggregate_edit"]["newValue"] == 200
    assert inverse["update"][0]["aggregate_edit"]["oldValue"] == 260
    assert inverse["update"][1]["updates"] == {"sales": 100}


def test_edit_session_event_history_ids_are_read_only_snapshots():
    domain = EditDomainService()
    request = SimpleNamespace(table="sales_data", grouping=["region", "country"])
    session = domain.get_or_create_session(
        request,
        {"session_id": "sess-readonly", "client_instance": "grid-readonly"},
    )

    domain.sessions.register_event(
        session,
        SessionEventRecord(
            event_id="evt_readonly",
            session_key=session.session_key,
            session_version=1,
            source="apply",
            created_at=0.0,
            normalized_transaction={"add": [], "remove": [], "update": [], "upsert": []},
        ),
    )

    active_event_ids = session.active_event_ids
    assert active_event_ids == ("evt_readonly",)
    with pytest.raises(AttributeError):
        active_event_ids.append("evt_external")
    with pytest.raises(AttributeError):
        session.active_event_ids = ["evt_external"]
    assert domain.sessions.latest_active_event_id(session) == "evt_readonly"


def test_replace_action_keeps_replacement_record_transaction_independent_from_execution_merge():
    domain = EditDomainService()
    request = SimpleNamespace(table="sales_data", grouping=["region"])
    session = domain.get_or_create_session(
        request,
        {"session_id": "sess-replace-isolation", "client_instance": "grid-replace-isolation"},
    )
    event = SessionEventRecord(
        event_id="evt_replace_source",
        session_key=session.session_key,
        session_version=1,
        source="apply",
        created_at=0.0,
        normalized_transaction={
            "add": [],
            "remove": [],
            "update": [
                {
                    "key_columns": {"region": "North"},
                    "edit_meta": {"rowId": "North", "rowPath": "North", "colId": "sales_sum"},
                    "aggregate_edit": {
                        "column": "sales",
                        "rowId": "North",
                        "rowPath": "North",
                        "columnId": "sales_sum",
                        "oldValue": 200,
                        "newValue": 260,
                        "propagationStrategy": "equal",
                    },
                }
            ],
            "upsert": [],
        },
        inverse_transaction={"update": [{"rowId": "North", "colId": "sales_sum", "value": 200, "oldValue": 260}]},
        original_updates=[{"rowId": "North", "colId": "sales_sum", "oldValue": 200, "value": 260}],
    )
    domain.sessions.register_event(session, event)

    action = domain.prepare_event_action(
        request,
        {
            "session_id": "sess-replace-isolation",
            "client_instance": "grid-replace-isolation",
            "eventAction": "replace",
            "eventIds": ["evt_replace_source"],
            "propagationStrategy": "proportional",
        },
        {"add": [], "remove": [], "update": [], "upsert": []},
    )

    assert action is not None
    execution_replacement_update = action.normalized_transaction["update"][-1]
    record_replacement_update = action.record_normalized_transaction["update"][0]
    assert execution_replacement_update is not record_replacement_update
    execution_replacement_update["aggregate_edit"]["newValue"] = 999
    assert record_replacement_update["aggregate_edit"]["newValue"] == 260


def test_scope_overlap_treats_subtree_root_as_covered_scope():
    assert scopes_overlap("North", "subtree", "North", "exact_scope")
    assert scopes_overlap("North", "exact_scope", "North", "subtree")
    assert scopes_overlap("North", "subtree", "North|||USA", "exact_scope")
    assert scopes_overlap("North|||USA", "exact_scope", "North", "subtree")


def test_affected_cells_marks_subtree_root_direct_and_descendants_propagated():
    affected = build_affected_cells_payload(
        SimpleNamespace(grouping=["region", "country"]),
        {"visibleRowPaths": ["North", "North|||USA", "North|||Canada", "South"]},
        {
            "add": [],
            "remove": [],
            "update": [
                {
                    "key_columns": {"region": "North"},
                    "edit_meta": {
                        "rowPath": "North",
                        "colId": "sales_sum",
                    },
                    "aggregate_edit": {
                        "rowPath": "North",
                        "columnId": "sales_sum",
                    },
                }
            ],
            "upsert": [],
        },
    )

    assert affected["direct"] == ["North:::sales_sum"]
    assert "North:::sales_sum" not in affected["propagated"]
    assert "North|||USA:::sales_sum" in affected["propagated"]
    assert "North|||Canada:::sales_sum" in affected["propagated"]
    assert "South:::sales_sum" not in affected["propagated"]


def test_overlay_index_annotations_match_grouped_and_current_index_shapes():
    session_hints = get_type_hints(EditSessionState)
    manager_hints = get_type_hints(EditSessionManager.current_overlay_index)

    assert session_hints["overlay_index_by_grouping"] == OverlayIndexByGrouping
    assert manager_hints["return"] == OverlayIndex


def test_runtime_service_blocks_overlapping_parent_child_edit_scopes_in_same_session():
    service = _make_service()

    parent_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North",
                    "rowPath": "North",
                    "colId": "sales_sum",
                    "value": 260,
                    "oldValue": 200,
                    "propagationStrategy": "equal",
                }
            ],
        },
        session_id="sess-overlap",
        client_instance="grid-overlap",
        window_seq=11,
    )

    event_id = parent_response.transaction_result["eventId"]
    assert event_id

    child_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North|||USA",
                    "rowPath": "North|||USA",
                    "colId": "sales_sum",
                    "value": 190,
                    "oldValue": 120,
                }
            ],
        },
        session_id="sess-overlap",
        client_instance="grid-overlap",
        window_seq=12,
    )

    assert child_response.transaction_result is not None
    assert child_response.transaction_result["warnings"][0] == OVERLAP_BLOCK_WARNING
    assert all(int(count or 0) == 0 for count in child_response.transaction_result["applied"].values())
    assert any(lock["ownerEventId"] == event_id for lock in child_response.transaction_result["scopeLocks"])


def test_runtime_service_blocks_redo_when_undone_parent_now_conflicts_with_child_edit():
    service = _make_service()

    parent_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North",
                    "rowPath": "North",
                    "colId": "sales_sum",
                    "value": 260,
                    "oldValue": 200,
                    "propagationStrategy": "equal",
                }
            ],
        },
        session_id="sess-redo-conflict",
        client_instance="grid-redo-conflict",
        window_seq=41,
    )
    parent_event_id = parent_response.transaction_result["eventId"]

    undo_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "eventAction": "undo",
            "eventId": parent_event_id,
        },
        session_id="sess-redo-conflict",
        client_instance="grid-redo-conflict",
        window_seq=42,
    )
    assert undo_response.transaction_result is not None
    assert any(int(count or 0) > 0 for count in undo_response.transaction_result["applied"].values())

    child_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North|||USA",
                    "rowPath": "North|||USA",
                    "colId": "sales_sum",
                    "value": 190,
                    "oldValue": 120,
                }
            ],
        },
        session_id="sess-redo-conflict",
        client_instance="grid-redo-conflict",
        window_seq=43,
    )
    assert child_response.transaction_result is not None
    child_event_id = child_response.transaction_result["eventId"]
    assert child_event_id

    redo_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "eventAction": "redo",
            "eventId": parent_event_id,
        },
        session_id="sess-redo-conflict",
        client_instance="grid-redo-conflict",
        window_seq=44,
    )

    assert redo_response.transaction_result is not None
    assert redo_response.transaction_result["warnings"][0] == "There is no edit available to redo."
    assert all(int(count or 0) == 0 for count in redo_response.transaction_result["applied"].values())
    assert any(lock["ownerEventId"] == child_event_id for lock in redo_response.transaction_result["scopeLocks"])


def test_finalize_event_action_releases_locks_even_when_revert_applies_zero_rows():
    domain = EditDomainService()
    request = SimpleNamespace(table="sales_data", grouping=["region", "country"])
    transaction_payload = {"session_id": "sess-finalize", "client_instance": "grid-finalize"}
    session = domain.get_or_create_session(request, transaction_payload)
    record = SessionEventRecord(
        event_id="evt_finalize_zero",
        session_key=session.session_key,
        session_version=1,
        source="apply",
        created_at=0.0,
        normalized_transaction={"add": [], "remove": [], "update": [], "upsert": []},
        scope_locks=[
            ScopeLock(
                scope_id="North",
                measure_id="sales_sum",
                lock_mode="subtree",
                owner_event_id="evt_finalize_zero",
            )
        ],
    )
    domain.sessions.register_event(session, record)

    domain.finalize_event_action(
        request,
        transaction_payload,
        PreparedEventAction(
            action="revert",
            session_key=session.session_key,
            source="revert",
            normalized_transaction={"add": [], "remove": [], "update": [], "upsert": []},
            target_event_ids=["evt_finalize_zero"],
        ),
        {"applied": {"update": 0}, "warnings": []},
    )

    assert domain.sessions.current_locks(session) == []
    assert domain.sessions.get_event("evt_finalize_zero").active is False


def test_runtime_service_same_scope_reedit_requires_replace_and_revert_stays_exact():
    service = _make_service()

    first_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North|||USA",
                    "rowPath": "North|||USA",
                    "colId": "sales_sum",
                    "value": 150,
                    "oldValue": 120,
                }
            ],
        },
        session_id="sess-replace-direct",
        client_instance="grid-replace-direct",
        window_seq=51,
    )
    original_event_id = first_response.transaction_result["eventId"]
    assert original_event_id

    blocked_reedit_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North|||USA",
                    "rowPath": "North|||USA",
                    "colId": "sales_sum",
                    "value": 170,
                    "oldValue": 150,
                }
            ],
        },
        session_id="sess-replace-direct",
        client_instance="grid-replace-direct",
        window_seq=52,
    )
    assert blocked_reedit_response.transaction_result is not None
    assert blocked_reedit_response.transaction_result["warnings"][0] == OVERLAP_BLOCK_WARNING
    assert all(int(count or 0) == 0 for count in blocked_reedit_response.transaction_result["applied"].values())

    replace_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "eventAction": "replace",
            "eventIds": [original_event_id],
            "update": [
                {
                    "rowId": "North|||USA",
                    "rowPath": "North|||USA",
                    "colId": "sales_sum",
                    "value": 170,
                    "oldValue": 120,
                }
            ],
        },
        session_id="sess-replace-direct",
        client_instance="grid-replace-direct",
        window_seq=53,
    )
    assert replace_response.transaction_result is not None
    replacement_event_id = replace_response.transaction_result["eventId"]
    assert replacement_event_id
    assert replacement_event_id != original_event_id
    assert {lock["ownerEventId"] for lock in replace_response.transaction_result["scopeLocks"]} == {replacement_event_id}

    inactive_revert_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "eventAction": "revert",
            "eventIds": [original_event_id],
        },
        session_id="sess-replace-direct",
        client_instance="grid-replace-direct",
        window_seq=54,
    )
    assert inactive_revert_response.transaction_result is not None
    assert inactive_revert_response.transaction_result["warnings"][0] == "Only active edits can be reverted."
    assert all(int(count or 0) == 0 for count in inactive_revert_response.transaction_result["applied"].values())

    revert_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "eventAction": "revert",
            "eventIds": [replacement_event_id],
        },
        session_id="sess-replace-direct",
        client_instance="grid-replace-direct",
        window_seq=55,
    )
    assert revert_response.transaction_result is not None
    assert any(int(count or 0) > 0 for count in revert_response.transaction_result["applied"].values())

    hydrated_response = _process(
        service,
        None,
        session_id="sess-replace-direct",
        client_instance="grid-replace-direct",
        window_seq=56,
    )
    rows_by_path = _row_values_by_path(hydrated_response)
    assert rows_by_path["North|||USA"]["sales_sum"] == 120.0
    assert rows_by_path["North"]["sales_sum"] == 200.0


def test_runtime_service_replace_can_merge_multiple_active_events():
    service = _make_service()

    north_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North",
                    "rowPath": "North",
                    "colId": "sales_sum",
                    "value": 260,
                    "oldValue": 200,
                    "propagationStrategy": "equal",
                }
            ],
        },
        session_id="sess-replace",
        client_instance="grid-replace",
        window_seq=21,
    )
    south_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "South",
                    "rowPath": "South",
                    "colId": "sales_sum",
                    "value": 150,
                    "oldValue": 120,
                    "propagationStrategy": "equal",
                }
            ],
        },
        session_id="sess-replace",
        client_instance="grid-replace",
        window_seq=22,
    )

    north_event_id = north_response.transaction_result["eventId"]
    south_event_id = south_response.transaction_result["eventId"]

    replace_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": VISIBLE_ROW_PATHS,
            "visibleCenterColumnIds": ["sales_sum"],
            "eventAction": "replace",
            "eventIds": [north_event_id, south_event_id],
            "propagationStrategy": "proportional",
        },
        session_id="sess-replace",
        client_instance="grid-replace",
        window_seq=23,
    )

    assert replace_response.transaction_result is not None
    assert replace_response.transaction_result["eventId"]
    assert replace_response.transaction_result["eventId"] not in {north_event_id, south_event_id}
    lock_owner_ids = {lock["ownerEventId"] for lock in replace_response.transaction_result["scopeLocks"]}
    assert north_event_id not in lock_owner_ids
    assert south_event_id not in lock_owner_ids
    assert lock_owner_ids == {replace_response.transaction_result["eventId"]}


def test_runtime_service_rehydrates_child_overlay_after_parent_edit_expands_later():
    service = _make_service()

    edit_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": ["North", "South", "__grand_total__"],
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North",
                    "rowPath": "North",
                    "colId": "sales_sum",
                    "value": 260,
                    "oldValue": 200,
                    "propagationStrategy": "equal",
                }
            ],
        },
        session_id="sess-overlay",
        client_instance="grid-overlay",
        window_seq=31,
        expanded={"South": True},
    )

    assert edit_response.transaction_result is not None
    assert edit_response.transaction_result["eventId"]

    hydrated_response = _process(
        service,
        None,
        session_id="sess-overlay",
        client_instance="grid-overlay",
        window_seq=32,
        expanded={"North": True, "South": True},
    )

    assert hydrated_response.status == "data"
    assert hydrated_response.edit_overlay is not None
    overlay_cells = {
        f"{entry['rowId']}:::{entry['colId']}": entry
        for entry in (hydrated_response.edit_overlay.get("cells") or [])
        if isinstance(entry, dict)
    }
    assert overlay_cells["North:::sales_sum"]["originalValue"] == 200.0
    assert overlay_cells["North|||USA:::sales_sum"]["originalValue"] == 120.0
    assert overlay_cells["North|||Canada:::sales_sum"]["originalValue"] == 80.0
    assert overlay_cells["North|||USA:::sales_sum"]["propagatedEventIds"]
    assert overlay_cells["North|||Canada:::sales_sum"]["propagatedEventIds"]


def test_edit_domain_builds_overlay_for_flat_tables_without_grouping_fields():
    domain = EditDomainService()
    request = SimpleNamespace(table="sales_data", grouping=[])
    session = domain.get_or_create_session(
        request,
        {"session_id": "sess-flat", "client_instance": "grid-flat"},
    )
    domain.sessions.register_event(
        session,
        SessionEventRecord(
            event_id="evt_flat_overlay",
            session_key=session.session_key,
            session_version=1,
            source="apply",
            created_at=0.0,
            normalized_transaction={"add": [], "remove": [], "update": [], "upsert": []},
            grouping_fields=[],
            scope_value_changes=[
                {
                    "scopeId": "row-1",
                    "measureId": "sales",
                    "beforeValue": 10.0,
                    "afterValue": 12.0,
                    "role": "direct",
                }
            ],
        ),
    )

    overlay = domain.build_visible_edit_overlay(
        request,
        session_id="sess-flat",
        client_instance="grid-flat",
        rows=[{"id": "row-1", "sales": 12.0}],
    )

    assert overlay is not None
    assert overlay["cells"] == [
        {
            "rowId": "row-1",
            "colId": "sales",
            "originalValue": 10.0,
            "comparisonCount": 1,
            "directEventIds": ["evt_flat_overlay"],
            "propagatedEventIds": [],
        }
    ]


def test_runtime_service_clears_overlay_after_parent_edit_is_undone():
    service = _make_service()

    edit_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": ["North", "South", "__grand_total__"],
            "visibleCenterColumnIds": ["sales_sum"],
            "update": [
                {
                    "rowId": "North",
                    "rowPath": "North",
                    "colId": "sales_sum",
                    "value": 260,
                    "oldValue": 200,
                    "propagationStrategy": "equal",
                }
            ],
        },
        session_id="sess-overlay-undo",
        client_instance="grid-overlay-undo",
        window_seq=61,
        expanded={"South": True},
    )
    event_id = edit_response.transaction_result["eventId"]
    assert event_id

    undo_response = _process(
        service,
        {
            "refreshMode": "patch",
            "visibleRowPaths": ["North", "South", "__grand_total__"],
            "visibleCenterColumnIds": ["sales_sum"],
            "eventAction": "undo",
            "eventId": event_id,
        },
        session_id="sess-overlay-undo",
        client_instance="grid-overlay-undo",
        window_seq=62,
        expanded={"South": True},
    )

    assert undo_response.transaction_result is not None
    assert any(int(count or 0) > 0 for count in undo_response.transaction_result["applied"].values())

    hydrated_response = _process(
        service,
        None,
        session_id="sess-overlay-undo",
        client_instance="grid-overlay-undo",
        window_seq=63,
        expanded={"North": True, "South": True},
    )

    assert hydrated_response.status == "data"
    assert hydrated_response.edit_overlay is None


def test_frontend_edit_contract_uses_event_actions_and_removes_none_option():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    side_panel_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "EditSidePanel.js",
        )
    ).read_text(encoding="utf-8")

    assert "eventAction: 'undo'" in component_source
    assert "eventAction: 'redo'" in component_source
    assert "eventAction: 'revert'" in component_source
    assert "eventAction: 'replace'" in component_source
    assert "directEventIds" in component_source
    assert "resolveCellEditOwnershipState" in component_source
    assert "dispatchServerEditUpdates" in component_source
    assert "onEditBlocked" in component_source
    assert "{ value: 'none'" not in side_panel_source
