from __future__ import annotations

import asyncio
import threading
from typing import Any, Awaitable


_background_loop: asyncio.AbstractEventLoop | None = None
_background_loop_lock = threading.Lock()


def _get_background_loop() -> asyncio.AbstractEventLoop:
    global _background_loop
    if _background_loop is not None and not _background_loop.is_closed():
        return _background_loop
    with _background_loop_lock:
        if _background_loop is None or _background_loop.is_closed():
            loop = asyncio.new_event_loop()
            t = threading.Thread(target=loop.run_forever, daemon=True, name="pivot-async-bridge")
            t.start()
            _background_loop = loop
    return _background_loop


def run_awaitable_sync(awaitable: Awaitable[Any]) -> Any:
    """Run an awaitable from sync code without re-entering a running event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop — safe to use asyncio.run directly.
        return asyncio.run(awaitable)

    # A running loop exists (e.g. Dash async context). Submit to the dedicated
    # background loop instead of blocking the shared ThreadPoolExecutor, which
    # can deadlock when all pool threads are waiting on each other.
    future = asyncio.run_coroutine_threadsafe(awaitable, _get_background_loop())
    return future.result()


def run_awaitable_in_worker_thread(awaitable: Awaitable[Any]) -> Any:
    """Run an awaitable on the background event loop from synchronous code."""
    future = asyncio.run_coroutine_threadsafe(awaitable, _get_background_loop())
    return future.result()
