"""Export lifecycle invariants, independent of storage and execution."""

from dataclasses import FrozenInstanceError
import importlib

import pytest


def model():
    # An assertion gives RED a clear missing-feature failure before the module exists.
    spec = importlib.util.find_spec("pivot_engine.runtime.export_jobs")
    assert spec is not None, "The immutable export job lifecycle is missing"
    return importlib.import_module(spec.name)


def job(**overrides):
    fields = {"job_id": "opaque-job", "subject": "alice", "accounting_account_id": "trusted-account", "table": "sales"}
    fields.update(overrides)
    return model().ExportJobSnapshot(**fields)


def advance(snapshot, phase, **fields):
    return model().transition_export_job(snapshot, {"phase": phase, **fields})


def test_job_snapshot_is_immutable_and_public_progress_excludes_private_identity():
    snapshot = job()
    with pytest.raises(FrozenInstanceError):
        snapshot.phase = "completed"
    assert snapshot.to_payload() == {"jobId": "opaque-job", "phase": "queued", "rowsWritten": 0, "bytesWritten": 0, "totalRows": None}


def test_cancellation_lifecycle_retains_observed_counters_until_cleanup_acknowledgement():
    queued = job()
    running = advance(queued, "running", owner_epoch=1, rows_written=3, bytes_written=42)
    cancelling = advance(running, "cancelling")
    assert cancelling.phase == "cancelling"
    cancelled = advance(cancelling, "cancelled", rows_written=4, bytes_written=48)
    assert (cancelled.phase, cancelled.rows_written, cancelled.bytes_written, cancelled.total_rows) == ("cancelled", 4, 48, None)
    assert queued.phase == "queued"


def test_cancel_before_worker_claims_job_is_terminal():
    cancelled = advance(job(), "cancelled")
    assert cancelled.phase == "cancelled"
    with pytest.raises(ValueError):
        advance(cancelled, "running", owner_epoch=1)


@pytest.mark.parametrize("phase", ["failed", "interrupted", "completed"])
def test_running_can_finish_but_terminal_states_cannot_be_overwritten(phase):
    running = advance(job(), "running", owner_epoch=1)
    fields = {"payload_ref": {"token": "opaque", "url": "/payload/opaque"}} if phase == "completed" else {}
    terminal = advance(running, phase, **fields)
    assert terminal.phase == phase
    assert advance(terminal, phase) == terminal
    with pytest.raises(ValueError):
        advance(terminal, "interrupted" if phase != "interrupted" else "completed")
    with pytest.raises(ValueError):
        advance(terminal, phase, rows_written=1)


@pytest.mark.parametrize("fields", [{"rows_written": 2}, {"bytes_written": 41}, {"owner_epoch": 0}])
def test_observed_progress_and_owner_fence_never_move_backwards(fields):
    running = advance(job(), "running", owner_epoch=1, rows_written=3, bytes_written=42)
    with pytest.raises(ValueError):
        advance(running, "running", **fields)


@pytest.mark.parametrize("source,target", [("queued", "completed"), ("running", "cancelled"), ("cancelling", "completed"), ("cancelling", "running")])
def test_invalid_phase_transitions_cannot_publish_or_acknowledge_cleanup(source, target):
    with pytest.raises(ValueError):
        advance(job(phase=source), target, payload_ref={"token": "opaque"} if target == "completed" else None)


def test_progress_never_fabricates_an_unknown_total():
    running = advance(job(), "running", rows_written=10, bytes_written=100)
    assert running.total_rows is None
    assert advance(running, "running", rows_written=20, bytes_written=200).total_rows is None
    measured = advance(running, "running", total_rows=30)
    assert measured.to_payload()["totalRows"] == 30


def test_completion_requires_output_and_cancel_intent_fences_publication():
    running = advance(job(), "running")
    with pytest.raises(ValueError):
        advance(running, "completed")
    with pytest.raises(ValueError):
        advance(running, "cancelling", payload_ref={"token": "opaque"})


def test_completed_payload_reference_is_deeply_immutable_and_detached():
    reference = {"token": "opaque", "metadata": {"filename": "sales.csv"}}
    completed = advance(advance(job(), "running"), "completed", payload_ref=reference)
    reference["metadata"]["filename"] = "mutated.csv"
    payload = completed.to_payload()
    assert payload["payloadRef"]["metadata"]["filename"] == "sales.csv"
    payload["payloadRef"]["metadata"]["filename"] = "client-mutated.csv"
    assert completed.to_payload()["payloadRef"]["metadata"]["filename"] == "sales.csv"


@pytest.mark.parametrize("fields", [{"rows_written": -1}, {"bytes_written": 1.5}, {"total_rows": -1}, {"phase": "invented"}, {"job_id": ""}])
def test_invalid_snapshots_are_rejected(fields):
    with pytest.raises(ValueError):
        job(**fields)
