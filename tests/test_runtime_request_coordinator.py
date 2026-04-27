import asyncio
from pathlib import Path

import pytest

from pivot_engine.runtime import PivotRequestContext, RuntimeRequestCoordinator, SessionRequestGate


def _context(*, window_seq=1, intent="viewport", client_instance="grid-a"):
    return PivotRequestContext(
        table="sales",
        session_id="session-a",
        client_instance=client_instance,
        state_epoch=1,
        window_seq=window_seq,
        abort_generation=1,
        intent=intent,
        viewport_active=True,
        start_row=0,
        end_row=50,
    )


def test_runtime_request_coordinator_centralizes_session_gate_checks():
    coordinator = RuntimeRequestCoordinator(SessionRequestGate())

    assert coordinator.register_request(_context(window_seq=1))
    assert not coordinator.register_request(_context(window_seq=1))
    assert coordinator.register_request(_context(window_seq=2))
    assert coordinator.response_is_current(_context(window_seq=2))
    assert not coordinator.response_is_current(_context(window_seq=1))


def test_runtime_request_lifecycle_spec_documents_profile_contract():
    spec = Path("RUNTIME_REQUEST_LIFECYCLE.md").read_text(encoding="utf-8")

    for required in (
        "requestId",
        "sessionId",
        "clientInstance",
        "stateEpoch",
        "windowSeq",
        "abortGeneration",
        "cacheKey",
        "adapter.responseCacheKey",
        "lifecycleLane",
        "cancellationOutcome",
        "stale_registration_rejected",
        "superseded_cancelled",
        "stale_response_dropped",
    ):
        assert required in spec


@pytest.mark.asyncio
async def test_runtime_request_coordinator_cancels_superseded_same_lane_task():
    coordinator = RuntimeRequestCoordinator(SessionRequestGate())
    first_cancelled = asyncio.Event()
    release_first = asyncio.Event()

    async def first_request():
        coordinator.replace_active_request_task(_context(window_seq=1))
        try:
            await release_first.wait()
        except asyncio.CancelledError:
            assert coordinator.consume_superseded_cancel()
            first_cancelled.set()
            return "stale"
        finally:
            coordinator.release_active_request_task(_context(window_seq=1))

    async def second_request():
        coordinator.replace_active_request_task(_context(window_seq=2))
        coordinator.release_active_request_task(_context(window_seq=2))
        return "current"

    first_task = asyncio.create_task(first_request())
    await asyncio.sleep(0)
    assert await second_request() == "current"
    assert await first_task == "stale"
    assert first_cancelled.is_set()


@pytest.mark.asyncio
async def test_runtime_request_coordinator_keeps_client_instances_isolated():
    coordinator = RuntimeRequestCoordinator(SessionRequestGate())
    cancelled = False

    async def first_request():
        nonlocal cancelled
        coordinator.replace_active_request_task(_context(window_seq=1, client_instance="grid-a"))
        try:
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            coordinator.release_active_request_task(_context(window_seq=1, client_instance="grid-a"))

    task = asyncio.create_task(first_request())
    await asyncio.sleep(0)

    coordinator.replace_active_request_task(_context(window_seq=1, client_instance="grid-b"))
    coordinator.release_active_request_task(_context(window_seq=1, client_instance="grid-b"))

    await task
    assert cancelled is False
