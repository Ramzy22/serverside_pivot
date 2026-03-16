"""
lifecycle.py - Robust lifecycle management for background tasks
"""
import asyncio
import logging
import traceback
from typing import Dict, Set, Any, Coroutine, Optional, Callable

logger = logging.getLogger(__name__)

class TaskManager:
    """
    Manages background tasks with error handling and supervision.
    Prevents silent failures of critical background processes.
    """
    def __init__(self, max_concurrency: int = 50):
        self.active_tasks: Set[asyncio.Task] = set()
        self._shutdown = False
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def _run_bounded(self, coro: Coroutine[Any, Any, Any]) -> Any:
        async with self.semaphore:
            return await coro

    def create_task(self, coro: Coroutine[Any, Any, Any], name: str = "task") -> asyncio.Task:
        """
        Schedule a task for execution.
        """
        if self._shutdown:
            logger.warning(f"Attempted to schedule task '{name}' during shutdown.")
            return None

        # Wrap the coroutine with the semaphore
        task = asyncio.create_task(self._run_bounded(coro), name=name)
        self.active_tasks.add(task)
        task.add_done_callback(lambda t: self._handle_task_completion(t, name))
        return task

    def cancel_task(self, name: str) -> bool:
        """
        Cancel a task by name. Returns True if a task was found and cancelled.
        """
        cancelled = False
        for task in list(self.active_tasks):
            if task.get_name() == name:
                task.cancel()
                cancelled = True
                # Don't break, simpler to cancel all with same name or maybe just one?
                # Usually names should be unique for cancellation targeting.
                # Let's cancel all matching names to be safe for "category" cancellation.
        return cancelled

    def _handle_task_completion(self, task: asyncio.Task, name: str):
        """
        Handle task completion, log exceptions if any.
        """
        self.active_tasks.discard(task)
        try:
            # Check for exceptions
            if task.cancelled():
                logger.info(f"Task '{name}' was cancelled.")
                return
            
            exc = task.exception()
            if exc:
                logger.error(f"Task '{name}' failed with exception: {exc}")
                logger.error(traceback.format_exc())
            else:
                logger.debug(f"Task '{name}' completed successfully.")
        except Exception as e:
            logger.error(f"Error handling completion for task '{name}': {e}")

    async def shutdown(self, timeout: float = 5.0):
        """
        Gracefully cancel all active tasks.
        """
        self._shutdown = True
        if not self.active_tasks:
            return

        logger.info(f"Cancelling {len(self.active_tasks)} active tasks...")
        for task in self.active_tasks:
            task.cancel()
        
        # Wait for tasks to cancel
        results = await asyncio.gather(*self.active_tasks, return_exceptions=True)
        logger.info("All tasks shutdown.")

# Global instance for easy access if needed, though dependency injection is preferred
_global_task_manager = TaskManager()

def get_task_manager() -> TaskManager:
    return _global_task_manager
