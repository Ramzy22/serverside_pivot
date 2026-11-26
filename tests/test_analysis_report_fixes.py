import inspect
import json

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from pivot_engine.cache.memory_cache import MemoryCache
from pivot_engine.config import ScalablePivotConfig
from pivot_engine.dash_integration import _find_sort_options_in_layout
from pivot_engine.editing.patch_planner import filter_visible_impacted_scope_ids
from pivot_engine.editing.scope_index import collect_impacted_scope_ids, visible_scope_ids_from_paths
from pivot_engine.editing.session_manager import EditSessionManager
from pivot_engine.editing.models import SessionEventRecord
from pivot_engine.flight_server import PivotFlightServer
from pivot_engine.scalable_pivot_controller import ScalablePivotController


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


def test_flight_server_requires_principal_and_dataset_authorization_by_default():
    with pytest.raises(RuntimeError, match="principal_resolver and dataset_authorizer"):
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


def test_flight_ticket_and_query_are_bound_to_the_authenticated_row_scope():
    class Principal:
        def __init__(self, user_id, tenant):
            self.id = user_id
            self.role = "viewer"
            self.attributes = {"tenant": tenant}

    class Context:
        def __init__(self, principal):
            self._middleware = type("Middleware", (), {"principal": principal})()

        def get_middleware(self, key):
            assert key == "pivot_auth"
            return self._middleware

    class ScopedController:
        def run_pivot_arrow(self, spec):
            filters = {entry["field"]: entry["value"] for entry in spec["filters"]}
            return pa.table({"tenant": [filters["tenant"]]})

    from pivot_engine.runtime import RuntimeDatasetAuthorizer

    authorizer = RuntimeDatasetAuthorizer(
        {
            "sales": {
                "roles": {"viewer"},
                "operations": {"read"},
                "rowSecurity": {
                    "mode": "claims",
                    "claims": {"tenant": "tenant"},
                },
            }
        }
    )
    server = PivotFlightServer(
        ScopedController(),
        location=None,
        require_auth=True,
        principal_resolver=lambda token: Principal("resolved", token),
        dataset_authorizer=authorizer,
    )
    try:
        a_context = Context(Principal("tenant-a", "A"))
        b_context = Context(Principal("tenant-b", "B"))
        [result] = list(
            server.do_action(
                a_context,
                fl.Action("pivot", json.dumps({"table": "sales", "filters": []}).encode("utf-8")),
            )
        )
        ticket = json.loads(result.body.to_pybytes())["ticket"]

        assert server.do_get(
            a_context,
            fl.Ticket(json.dumps({"ticket": ticket}).encode("utf-8")),
        ) is not None
        with pytest.raises(fl.FlightUnauthorizedError):
            server.do_get(
                b_context,
                fl.Ticket(json.dumps({"ticket": ticket}).encode("utf-8")),
            )
    finally:
        server.shutdown()


def test_memory_cache_get_all_keys_is_lock_guarded():
    cache = MemoryCache(ttl=300)
    cache.clear()
    cache.set("a", pa.table({"x": [1]}))

    assert cache.get_all_keys() == ["a"]


def test_memory_cache_instances_are_isolated_by_default():
    first = MemoryCache(ttl=300)
    second = MemoryCache(ttl=300)
    first.clear()
    second.clear()

    first.set("shared", "first")

    assert first.get("shared") == "first"
    assert second.get("shared") is None


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




def _result_tier_controller(**kwargs):
    import pyarrow as pa

    controller = ScalablePivotController(backend_uri=":memory:", **kwargs)
    controller.load_data_from_arrow(
        "t", pa.table({"tenant": ["A", "A", "B"], "r": ["x", "y", "x"], "v": [1, 2, 100]}))
    spec = {"table": "t", "rows": ["r"], "measures": [{"field": "v", "agg": "sum", "alias": "s"}]}
    return controller, spec


def _sums(result):
    return {row[0]: row[1] for row in result["rows"]}


def test_the_core_result_tier_reuses_identical_reads():
    controller, spec = _result_tier_controller()
    first = controller.run_pivot(spec, return_format="dict")
    hits = controller.core_cache.hits
    assert controller.run_pivot(spec, return_format="dict") == first
    assert controller.core_cache.hits == hits + 1
    controller.run_pivot(spec, return_format="dict", force_refresh=True)
    assert controller.core_cache.hits == hits + 1  # a forced refresh never reads the tier
    controller.close()


def test_the_core_result_tier_is_dropped_on_load_and_mutation():
    import pyarrow as pa

    controller, spec = _result_tier_controller()
    assert _sums(controller.run_pivot(spec, return_format="dict")) == {"x": 101, "y": 2}
    controller.load_data_from_arrow("t", pa.table({"tenant": ["A"], "r": ["x"], "v": [5]}))
    assert _sums(controller.run_pivot(spec, return_format="dict")) == {"x": 5}
    controller.core_cache.put(
        __import__("pivot_engine.core.cache", fromlist=["key_for"]).key_for("result", "t", "probe"), (0.0, pa.table({})))
    controller._clear_mutation_caches("t", structural=False)
    assert controller.core_cache.used_bytes("result") == 0
    controller.close()


def test_the_core_result_tier_expires_after_the_ttl(monkeypatch):
    import pivot_engine.scalable_pivot_controller as module

    now = [1000.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])
    controller, spec = _result_tier_controller(cache_ttl=10)
    normalized = controller._normalize_spec(spec)
    controller.run_pivot(spec, return_format="dict")
    now[0] += 10
    assert controller.cached_core_result(normalized) is not None
    now[0] += 0.5
    assert controller.cached_core_result(normalized) is None
    controller.close()


def test_the_core_result_tier_never_crosses_tenants():  # REQ-SEC-004
    from pivot_engine.core.authority.scope import connections_of, scoped_execution
    from pivot_engine.runtime.dataset_authorization import RowScopeGrant

    controller, spec = _result_tier_controller()

    def grant(tenant):
        return RowScopeGrant(table="t", subject=tenant, field_values={"tenant": (tenant,)},
                             scope_fingerprint=f"tenant={tenant}")

    with scoped_execution(grant("A"), connections=connections_of(controller)):
        a = _sums(controller.run_pivot(spec, return_format="dict"))
    with scoped_execution(grant("B"), connections=connections_of(controller)):
        b = _sums(controller.run_pivot(spec, return_format="dict"))
    assert a == {"x": 1, "y": 2}
    assert b == {"x": 100}
    controller.close()


def test_a_read_overlapping_a_write_never_stores_its_stale_result(monkeypatch):
    import pyarrow as pa

    import pivot_engine.transport.core_bridge as bridge

    controller, spec = _result_tier_controller()
    real = bridge.core_pivot_table

    def read_then_write_lands(resolve, s):
        stale = real(resolve, s)  # computed on the old data ...
        controller.load_data_from_arrow("t", pa.table({"tenant": ["A"], "r": ["x"], "v": [5]}))  # ... then a write
        return stale

    monkeypatch.setattr(bridge, "core_pivot_table", read_then_write_lands)
    assert _sums(controller.run_pivot(spec, return_format="dict")) == {"x": 101, "y": 2}  # the in-flight answer
    monkeypatch.setattr(bridge, "core_pivot_table", real)
    assert _sums(controller.run_pivot(spec, return_format="dict")) == {"x": 5}  # not the stale one from the tier
    controller.close()
