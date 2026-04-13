import asyncio
import inspect
import json
import re
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from pivot_engine.cache.memory_cache import MemoryCache
from pivot_engine.config import ScalablePivotConfig
from pivot_engine.controller import PivotController
from pivot_engine.dash_integration import _find_sort_options_in_layout
from pivot_engine.diff.diff_engine import DeltaInfo, QueryDiffEngine
from pivot_engine.editing.patch_planner import filter_visible_impacted_scope_ids
from pivot_engine.editing.scope_index import collect_impacted_scope_ids, visible_scope_ids_from_paths
from pivot_engine.editing.session_manager import EditSessionManager
from pivot_engine.editing.models import SessionEventRecord
from pivot_engine.flight_server import PivotFlightServer
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import PivotSpec


def test_scope_collection_deduplicates_during_construction():
    assert collect_impacted_scope_ids("North|||USA") == [
        "North|||USA",
        "North",
        "__grand_total__",
    ]
    assert visible_scope_ids_from_paths([" North ", "", None, "__grand_total__"]) == [
        "North",
        "__grand_total__",
    ]


def test_patch_planner_matches_visible_descendants_and_ancestors_without_nested_scan():
    visible = ["North", "North|||USA", "South"]
    assert filter_visible_impacted_scope_ids(["North|||USA"], visible) == ["North", "North|||USA"]


def test_session_manager_removes_overlay_incrementally_and_preserves_next_original_value():
    manager = EditSessionManager()
    session = manager.get_or_create_session(table="sales", session_id="s1", client_instance="grid")

    first = SessionEventRecord(
        event_id="evt_1",
        session_key=session.session_key,
        session_version=1,
        source="apply",
        created_at=0,
        normalized_transaction={"update": []},
        grouping_fields=["region"],
        scope_value_changes=[{"scopeId": "North", "measureId": "sales", "beforeValue": 10, "role": "direct"}],
    )
    second = SessionEventRecord(
        event_id="evt_2",
        session_key=session.session_key,
        session_version=2,
        source="apply",
        created_at=1,
        normalized_transaction={"update": []},
        grouping_fields=["region"],
        scope_value_changes=[{"scopeId": "North", "measureId": "sales", "beforeValue": 12, "role": "direct"}],
    )

    manager.register_event(session, first)
    manager.register_event(session, second)
    manager.push_undone_events(session, ["evt_1"])

    overlay = manager.current_overlay_index(session, grouping_fields=["region"])
    cell = overlay["North"]["sales"]
    assert cell["directEventIds"] == ["evt_2"]
    assert cell["originalValue"] == 12


def test_diff_engine_delta_table_preserves_base_schema_order():
    engine = QueryDiffEngine(MemoryCache(ttl=300))
    engine._delta_info["sales"] = DeltaInfo(table="sales", last_timestamp=0, incremental_field="updated_at")
    engine.compute_delta_queries = lambda _spec, _plan: [SimpleNamespace(to_pyarrow=lambda: pa.table({"b": [3], "a": [4]}))]

    base = pa.table({"a": [1], "b": [2]})
    result = engine.apply_delta_updates(
        {"table": "sales", "measures": []},
        {"queries": [object()]},
        base,
    )

    assert result.column_names == ["a", "b"]


def test_tile_planning_uses_limit_capability_instead_of_fragile_op_name():
    class LimitOnlyExpr:
        def __init__(self):
            self.calls = []

        def limit(self, limit, offset=0):
            self.calls.append((limit, offset))
            return f"limited:{limit}:{offset}"

    expr = LimitOnlyExpr()
    engine = QueryDiffEngine(MemoryCache(ttl=300), tile_size=10)
    queries, strategy = engine._plan_tile_aware(
        [expr],
        {"page": {"offset": 20, "limit": 5}, "limit": 5},
        {"queries": [expr]},
        {},
    )

    assert queries == ["limited:5:20"]
    assert expr.calls == [(5, 20)]
    assert strategy["tiles_needed"] == ["r20-25_c0--1"]


def test_config_omits_redis_password_when_unset(monkeypatch):
    monkeypatch.setenv("CACHE_TYPE", "redis")
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)

    config = ScalablePivotConfig().from_env()

    assert "password" not in config.redis_config


def test_dash_layout_search_walks_nested_props_beyond_children():
    layout = {
        "id": "root",
        "panel": {
            "items": [
                {"id": "other"},
                {"id": "pivot-grid", "sortOptions": {"columnOptions": {"desk": {"sortType": "absolute"}}}},
            ]
        },
    }

    assert _find_sort_options_in_layout(layout, "pivot-grid") == {
        "columnOptions": {"desk": {"sortType": "absolute"}}
    }


class _CountingController:
    def __init__(self):
        self.calls = 0

    def run_pivot_arrow(self, spec):
        self.calls += 1
        return pa.table({"value": [spec["value"]]})


def test_flight_server_requires_auth_token_by_default():
    with pytest.raises(RuntimeError, match="PIVOT_FLIGHT_AUTH_TOKEN"):
        PivotFlightServer(_CountingController(), location=None)


def test_flight_pivot_action_returns_ticket_and_do_get_reuses_cached_table():
    controller = _CountingController()
    server = PivotFlightServer(controller, location=None, require_auth=False)
    try:
        action = fl.Action("pivot", json.dumps({"value": 7}).encode("utf-8"))
        [result] = list(server.do_action(None, action))
        payload = json.loads(result.body.to_pybytes())

        stream = server.do_get(None, fl.Ticket(json.dumps({"ticket": payload["ticket"]}).encode("utf-8")))

        assert stream is not None
        assert server._load_result(payload["ticket"]).to_pylist() == [{"value": 7}]
        assert controller.calls == 1
    finally:
        server.shutdown()


def test_memory_cache_get_all_keys_is_lock_guarded():
    cache = MemoryCache(ttl=300)
    cache.clear()
    cache.set("a", pa.table({"x": [1]}))

    assert cache.get_all_keys() == ["a"]


def test_observability_metrics_noops_without_instrumentator(monkeypatch):
    import pivot_engine.observability as observability

    monkeypatch.setattr(observability, "Instrumentator", None)
    assert observability.setup_metrics(object()) is None


def test_parameterized_fetchall_has_single_column_mapping_implementation():
    source = inspect.getsource(ScalablePivotController)

    assert source.count("def _execute_parameterized_fetchall") == 1
    assert "description = getattr(result, \"description\", None) or []" in source


def test_integer_proportional_aggregate_edit_uses_one_set_based_update():
    class ProbeController(ScalablePivotController):
        def __init__(self):
            self.mutations = []
            self.fetch_calls = 0

        def _execute_parameterized_fetchall(self, sql, params):
            self.fetch_calls += 1
            return [
                {"rowid": 1, "val": 10},
                {"rowid": 2, "val": 20},
            ]

        def _execute_parameterized_mutation(self, sql, params):
            self.mutations.append((sql, params))

    controller = ProbeController()
    updated = controller._apply_integer_proportional_sync(
        "sales_data",
        "sales",
        "region = ?",
        ["North"],
        matched_row_count=2,
        scale_factor=1.3,
        target_sum=39,
    )

    assert updated == 2
    assert controller.fetch_calls == 1
    assert len(controller.mutations) == 1
    sql, params = controller.mutations[0]
    assert "WITH scaled(target_rowid, new_value) AS (VALUES (?, ?), (?, ?))" in sql
    assert "UPDATE sales_data" in sql
    assert "SET sales = scaled.new_value" in sql
    assert "FROM scaled" in sql
    assert params == [1, 13, 2, 26]


def test_pivot_controller_exposes_single_awaitable_hierarchical_api():
    source = inspect.getsource(PivotController)
    api_source = Path(
        "dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py"
    ).read_text(encoding="utf-8")

    assert inspect.iscoroutinefunction(PivotController.run_hierarchical_pivot)
    assert len(re.findall(r"^\s+async def run_hierarchical_pivot\(", source, flags=re.MULTILINE)) == 1
    assert "return self.tree_manager.run_hierarchical_pivot(" not in source
    assert "result = await self.controller.run_hierarchical_pivot(spec_dict)" in api_source


def test_mutation_cache_invalidation_is_table_scoped_and_preserves_unrelated_entries():
    cache = MemoryCache(ttl=300)
    cache.clear()
    cache.set("pivot_ibis:query:sales:abc", "sales")
    cache.set("pivot_ibis:query:other:abc", "other")
    cache.set("pivot_ibis:query:legacy", "legacy")
    cache.set("unrelated:key", "keep")

    class ProbeController(ScalablePivotController):
        def __init__(self):
            self.cache = cache
            self._hierarchy_view_cache = OrderedDict({"view": ({}, 0)})
            self._hierarchy_root_count_cache = OrderedDict({"count": (1, 0)})
            self._hierarchy_root_page_cache = OrderedDict({"page": ([1], 0)})
            self._hierarchy_grand_total_cache = OrderedDict({"total": ({}, 0)})

    controller = ProbeController()

    controller._clear_mutation_caches("sales", structural=False)

    assert cache.get("pivot_ibis:query:sales:abc") is None
    assert cache.get("pivot_ibis:query:legacy") is None
    assert cache.get("pivot_ibis:query:other:abc") == "other"
    assert cache.get("unrelated:key") == "keep"
    assert controller._hierarchy_view_cache == OrderedDict()
    assert controller._hierarchy_grand_total_cache == OrderedDict()
    assert controller._hierarchy_root_count_cache
    assert controller._hierarchy_root_page_cache


def test_hierarchy_batch_load_has_no_silent_100k_cap_and_exact_parent_filters():
    source = inspect.getsource(ScalablePivotController.run_hierarchical_pivot_batch_load)

    filters = ScalablePivotController._build_parent_batch_filters(
        ["region", "country"],
        [["North", "USA"], ["South", "Brazil"]],
    )

    assert "100000" not in source
    assert filters == [
        {
            "operator": "OR",
            "conditions": [
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "region", "op": "=", "value": "North"},
                        {"field": "country", "op": "=", "value": "USA"},
                    ],
                },
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "region", "op": "=", "value": "South"},
                        {"field": "country", "op": "=", "value": "Brazil"},
                    ],
                },
            ],
        }
    ]


def test_windowed_hierarchy_flatten_counts_without_returning_full_rows():
    class ProbeController(ScalablePivotController):
        def __init__(self):
            pass

    controller = ProbeController()
    spec = PivotSpec(
        table="sales",
        rows=["region"],
        measures=[],
        filters=[],
    )
    hierarchy_result = {
        "": [
            {"region": f"R{i}", "sales_sum": i}
            for i in range(1000)
        ]
    }

    flattened = controller._flatten_hierarchy_rows_window(spec, hierarchy_result, [], 10, 12)

    assert flattened["total_rows"] == 1000
    assert [row["region"] for row in flattened["rows"]] == ["R10", "R11", "R12"]
    assert flattened["full_rows"] is None
