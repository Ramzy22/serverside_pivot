"""Planning and query execution ownership for scalable pivot requests.

The controller remains the stable public API, but does not need to own the
normalise -> plan -> execute -> profile sequence for every request. This
collaborator keeps that sequence outside ``ScalablePivotController`` so that
planner and backend execution can evolve independently.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Optional


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
        """Plan, execute, materialize when appropriate, and profile a pivot."""
        controller = self._controller
        started_wall_time = time.time()
        started_at = time.perf_counter()
        controller._request_count += 1
        normalized_spec = controller._normalize_spec(spec)

        if controller._can_use_sparse_materialized_pivot(normalized_spec):
            execution_profile: Dict[str, Any] = {}
            execute_started_at = time.perf_counter()
            result_table = await controller._execute_sparse_materialized_pivot_async(
                normalized_spec,
                force_refresh,
                profile_sink=execution_profile,
            )
            execute_finished_at = time.perf_counter()
            await self._schedule_materialization_if_needed(
                normalized_spec,
                started_wall_time,
            )
            return self._format_result(
                result_table,
                normalized_spec,
                return_format=return_format,
                profile_sink=profile_sink,
                started_at=started_at,
                planning_started_at=None,
                planning_finished_at=None,
                execute_started_at=execute_started_at,
                execute_finished_at=execute_finished_at,
                needs_column_discovery=False,
                query_count=1,
                execution_profile=execution_profile,
                path="sparse_materialized",
            )

        planning_started_at = time.perf_counter()

        def plan_with_lock() -> Dict[str, Any]:
            with controller.planning_lock:
                return controller.planner.plan(normalized_spec)

        plan_result = await asyncio.get_running_loop().run_in_executor(
            None,
            plan_with_lock,
        )
        planning_finished_at = time.perf_counter()
        metadata = plan_result.get("metadata", {})
        execution_profile: Dict[str, Any] = {}
        execute_started_at = time.perf_counter()
        if metadata.get("needs_column_discovery"):
            result_table = await controller._execute_topn_pivot_async(
                normalized_spec,
                plan_result,
                force_refresh,
                profile_sink=execution_profile,
            )
        else:
            result_table = await controller._execute_standard_pivot_async(
                normalized_spec,
                plan_result,
                force_refresh,
                profile_sink=execution_profile,
            )
        execute_finished_at = time.perf_counter()
        await self._schedule_materialization_if_needed(
            normalized_spec,
            started_wall_time,
        )
        return self._format_result(
            result_table,
            normalized_spec,
            return_format=return_format,
            profile_sink=profile_sink,
            started_at=started_at,
            planning_started_at=planning_started_at,
            planning_finished_at=planning_finished_at,
            execute_started_at=execute_started_at,
            execute_finished_at=execute_finished_at,
            needs_column_discovery=bool(metadata.get("needs_column_discovery")),
            query_count=len(plan_result.get("queries", []) or []),
            execution_profile=execution_profile,
        )

    async def _schedule_materialization_if_needed(
        self,
        spec: Any,
        started_wall_time: float,
    ) -> None:
        controller = self._controller
        if not controller.stats_tracker.record_query(
            spec,
            time.time() - started_wall_time,
        ):
            return
        # Shared DuckDB/Ibis connections or read-only cloud backends without DDL
        # materialization enabled must not run background materialization.
        caps = getattr(controller, "backend_capabilities", None)
        if caps is not None:
            if getattr(caps, "shared_connection_requires_lock", False) or not getattr(caps, "supports_ddl_materialization", False):
                return
        elif controller._uses_duckdb_ibis():
            return
        controller.task_manager.create_task(
            controller._trigger_materialization(spec),
            name=f"smart_materialization_{spec.table}",
        )

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
            result = controller._convert_table_to_dict(result_table, spec)
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
