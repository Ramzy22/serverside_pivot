"""Pivot execution for ``ScalablePivotController.run_pivot_async``.

Normalise the spec, answer it from the semantic core (``core_pivot_table``:
one row per leaf group), then format and profile the result.  A request the
core does not express raises ``CoreUnsupported``.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional


def spec_fingerprint(spec: Any) -> str:
    """A stable digest of a normalized pivot spec (the result-tier key; the scope is keyed separately)."""
    import hashlib
    import json

    payload = spec.to_dict() if hasattr(spec, "to_dict") else spec
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


class PivotQueryExecutor:
    """Execute one normalized pivot specification through a controller port."""

    def __init__(self, controller: Any) -> None:
        self._controller = controller

    async def execute(
        self,
        spec: Any,
        *,
        return_format: str = "arrow",
        force_refresh: bool = False,
        profile_sink: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Answer a pivot from the core, then format and profile it."""
        controller = self._controller
        started_at = time.perf_counter()
        normalized_spec = controller._normalize_spec(spec)

        core_table = await self._core_table(normalized_spec, force_refresh=force_refresh)
        execute_finished_at = time.perf_counter()
        return self._format_result(
            core_table,
            normalized_spec,
            return_format=return_format,
            profile_sink=profile_sink,
            started_at=started_at,
            planning_started_at=None,
            planning_finished_at=None,
            execute_started_at=started_at,
            execute_finished_at=execute_finished_at,
            needs_column_discovery=False,
            query_count=1,
            execution_profile={"engine": "core"},
            path="core",
        )

    async def _core_table(self, spec: Any, *, force_refresh: bool = False) -> Any:
        """The pivot from the core result tier, else the core (an unsupported request raises
        ``CoreUnsupported``)."""
        from .transport.core_bridge import core_pivot_table, run_core_read

        controller = self._controller
        cached = None if force_refresh else controller.cached_core_result(spec)
        if cached is not None:
            return cached
        epoch = controller.core_epoch()  # before the read: a write during it voids the store
        table = await run_core_read(controller, spec.table, lambda resolve: core_pivot_table(resolve, spec))
        controller.store_core_result(spec, table, epoch)
        return table

    def _format_result(
        self,
        result_table: Any,
        spec: Any,
        *,
        return_format: str,
        profile_sink: Optional[Dict[str, Any]],
        started_at: float,
        planning_started_at: Optional[float],
        planning_finished_at: Optional[float],
        execute_started_at: float,
        execute_finished_at: float,
        needs_column_discovery: bool,
        query_count: int,
        execution_profile: Dict[str, Any],
        path: Optional[str] = None,
    ) -> Any:
        controller = self._controller
        conversion_started_at = conversion_finished_at = None
        result = result_table
        if return_format == "dict":
            conversion_started_at = time.perf_counter()
            result = controller.core_result_dict(spec, result_table)
            conversion_finished_at = time.perf_counter()
        if isinstance(profile_sink, dict):
            planner_profile: Dict[str, Any] = {
                "planningMs": (
                    controller._profile_ms(planning_started_at, planning_finished_at)
                    if planning_started_at is not None
                    else 0.0
                ),
                "executeMs": controller._profile_ms(
                    execute_started_at,
                    execute_finished_at,
                ),
                "convertMs": (
                    controller._profile_ms(
                        conversion_started_at,
                        conversion_finished_at,
                    )
                    if conversion_started_at is not None
                    else 0.0
                ),
                "totalMs": controller._profile_ms(started_at, time.perf_counter()),
                "needsColumnDiscovery": needs_column_discovery,
                "queryCount": query_count,
            }
            if path is not None:
                planner_profile["path"] = path
            lock_metrics = getattr(controller, "execution_lock_metrics_snapshot", None)
            if callable(lock_metrics):
                planner_profile["executionLock"] = lock_metrics(
                    getattr(spec, "table", None)
                )
            profile_sink.clear()
            profile_sink.update(
                {
                    "planner": planner_profile,
                    "plannerExecution": execution_profile,
                }
            )
        return result
