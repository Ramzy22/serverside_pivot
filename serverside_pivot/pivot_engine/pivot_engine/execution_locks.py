"""Per-table execution locks with contention metrics.

DuckDB/Ibis expressions sharing a connection must not execute concurrently for
the same table. This registry makes that safety policy explicit and exposes
the wait time needed to distinguish database work from lock contention.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import threading
import time
from typing import Any, Callable, Dict, Optional, TypeVar


Result = TypeVar("Result")


@dataclass
class TableExecutionLockMetrics:
    acquisitions: int = 0
    wait_ms: float = 0.0
    max_wait_ms: float = 0.0


class TableExecutionLockRegistry:
    """Own per-table locks and contention metrics for backend execution."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: Dict[str, threading.RLock] = {}
        self._metrics: Dict[str, TableExecutionLockMetrics] = defaultdict(
            TableExecutionLockMetrics
        )

    @staticmethod
    def _key(table_name: Optional[str]) -> str:
        return str(table_name or "__default__")

    def lock_for(self, table_name: Optional[str]) -> threading.RLock:
        key = self._key(table_name)
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._locks[key] = lock
            return lock

    def run(self, table_name: Optional[str], operation: Callable[[], Result]) -> Result:
        """Run an operation under its table lock and record acquisition wait."""
        key = self._key(table_name)
        lock = self.lock_for(table_name)
        waiting_since = time.perf_counter()
        with lock:
            waited_ms = (time.perf_counter() - waiting_since) * 1000
            with self._guard:
                metrics = self._metrics[key]
                metrics.acquisitions += 1
                metrics.wait_ms += waited_ms
                metrics.max_wait_ms = max(metrics.max_wait_ms, waited_ms)
            return operation()

    def snapshot(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Return a stable metrics snapshot for one table or every table."""
        with self._guard:
            if table_name is not None:
                key = self._key(table_name)
                metrics = self._metrics.get(key, TableExecutionLockMetrics())
                return {
                    "table": key,
                    "acquisitions": metrics.acquisitions,
                    "waitMs": round(metrics.wait_ms, 3),
                    "maxWaitMs": round(metrics.max_wait_ms, 3),
                }
            return {
                key: {
                    "acquisitions": metrics.acquisitions,
                    "waitMs": round(metrics.wait_ms, 3),
                    "maxWaitMs": round(metrics.max_wait_ms, 3),
                }
                for key, metrics in self._metrics.items()
            }
