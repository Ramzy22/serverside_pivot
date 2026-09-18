import asyncio
import threading

from pivot_engine.query_execution_service import PivotQueryExecutor


class _Planner:
    def __init__(self):
        self.called = False

    def plan(self, spec):
        self.called = True
        return {"metadata": {}, "queries": ["query"]}


class _Stats:
    def record_query(self, spec, duration):
        return False


class _Controller:
    def __init__(self):
        self._request_count = 0
        self.planning_lock = threading.Lock()
        self.planner = _Planner()
        self.stats_tracker = _Stats()
        self.standard_calls = []

    def _normalize_spec(self, spec):
        return {"normalized": spec}

    def _can_use_sparse_materialized_pivot(self, spec):
        return False

    async def _execute_standard_pivot_async(self, spec, plan, force_refresh, profile_sink):
        self.standard_calls.append((spec, plan, force_refresh))
        profile_sink.update({"mode": "standard"})
        return {"table": "result"}

    def _convert_table_to_dict(self, table, spec):
        return {"table": table, "spec": spec}

    def _uses_duckdb_ibis(self):
        return True

    @staticmethod
    def _profile_ms(start, end):
        return round((end - start) * 1000, 3) if start is not None and end is not None else None


def test_query_executor_owns_normalize_plan_execute_and_profile_sequence():
    async def run():
        controller = _Controller()
        profile = {}
        result = await PivotQueryExecutor(controller).execute(
            {"table": "sales"},
            return_format="dict",
            profile_sink=profile,
        )
        return controller, result, profile

    controller, result, profile = asyncio.run(run())

    assert controller._request_count == 1
    assert controller.planner.called is True
    assert controller.standard_calls[0][0] == {"normalized": {"table": "sales"}}
    assert result["table"] == {"table": "result"}
    assert profile["planner"]["queryCount"] == 1
    assert profile["plannerExecution"] == {"mode": "standard"}
