import os
import sys
import asyncio

import pytest

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "pivot_engine"))
sys.path.append(os.path.join(os.getcwd(), "serverside_pivot"))

from pivot_engine.lifecycle import TaskManager


@pytest.mark.asyncio
async def test_create_task_returns_cancelled_task_during_shutdown():
    manager = TaskManager()
    manager._shutdown = True
    started = False

    async def rejected_work():
        nonlocal started
        started = True

    task = manager.create_task(rejected_work(), name="late-task")

    assert isinstance(task, asyncio.Task)
    assert task.get_name() == "late-task"
    assert task not in manager.active_tasks
    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled()
    assert started is False


@pytest.mark.asyncio
async def test_create_task_tracks_and_removes_active_task():
    manager = TaskManager()

    async def work():
        await asyncio.sleep(0)
        return "ok"

    task = manager.create_task(work(), name="tracked-task")

    assert task in manager.active_tasks
    assert await task == "ok"
    await asyncio.sleep(0)
    assert task not in manager.active_tasks


@pytest.mark.asyncio
async def test_cancel_task_defaults_to_one_matching_name():
    manager = TaskManager()
    release = asyncio.Event()

    async def work():
        await release.wait()
        return "ok"

    first = manager.create_task(work(), name="shared-name")
    second = manager.create_task(work(), name="shared-name")
    unrelated = manager.create_task(work(), name="other-name")
    await asyncio.sleep(0)

    assert manager.cancel_task("shared-name")
    await asyncio.sleep(0)
    assert sum(task.cancelled() for task in (first, second)) == 1
    assert not unrelated.done()

    release.set()
    results = await asyncio.gather(first, second, unrelated, return_exceptions=True)
    await asyncio.sleep(0)

    assert sum(isinstance(result, asyncio.CancelledError) for result in results[:2]) == 1
    assert results[:2].count("ok") == 1
    assert results[2] == "ok"
    assert not manager.active_tasks


@pytest.mark.asyncio
async def test_cancel_task_can_cancel_all_matching_names_explicitly():
    manager = TaskManager()
    release = asyncio.Event()

    async def work():
        await release.wait()
        return "ok"

    first = manager.create_task(work(), name="shared-name")
    second = manager.create_task(work(), name="shared-name")
    unrelated = manager.create_task(work(), name="other-name")
    await asyncio.sleep(0)

    assert manager.cancel_task("shared-name", cancel_all=True)
    await asyncio.sleep(0)
    assert all(task.cancelled() for task in (first, second))
    assert not unrelated.done()

    release.set()
    results = await asyncio.gather(first, second, unrelated, return_exceptions=True)
    await asyncio.sleep(0)

    assert all(isinstance(result, asyncio.CancelledError) for result in results[:2])
    assert results[2] == "ok"
    assert not manager.active_tasks


@pytest.mark.asyncio
async def test_critical_task_failure_is_recorded_and_reported():
    manager = TaskManager()
    reported = []

    async def fail():
        raise RuntimeError("critical boom")

    manager.set_critical_failure_handler(lambda name, exc: reported.append((name, str(exc))))
    task = manager.create_task(fail(), name="critical-task", critical=True)

    with pytest.raises(RuntimeError, match="critical boom"):
        await task
    await asyncio.sleep(0)

    assert "critical-task" in manager.critical_failures
    assert reported == [("critical-task", "critical boom")]
