"""
Tests for Cloud Database & Generic Ibis Backend Compatibility.

Validates:
1. BackendCapabilities detection & resolution across cloud/SQL dialects (Snowflake, BigQuery, Postgres, ClickHouse, Databricks, Trino, MotherDuck, SQLite, DuckDB).
2. Generic Ibis connection URI routing in IbisBackend._connect_ibis_uri.
3. Zero-copy lazy Ibis table registration via DataInputNormalizer & PivotController.register_ibis_table.
4. IbisTableCatalog lazy table resolution, qualified `database.schema.table` resolution, and TTL schema caching.
5. Dialect-portable Ibis expressions (case-insensitive LIKE fallback when `ilike` is unsupported, tenor sort fallback when `re_extract` is unsupported, skipping remote `COUNT(*)` during cost estimation).
6. Capability-gated DatabaseMutationExecutor (fail-closed on read-only remote backends, opt-in via `PIVOT_ALLOW_REMOTE_MUTATIONS=1`, `rowid`-free fallback for aggregate edits on cloud/RDBMS backends, and `%s` vs `?` parameter placeholder adaptation).
7. Bounded record batch fallback via paged `.limit(chunk_rows, offset=offset).to_pyarrow()` for cloud backends without `to_pyarrow_batches`.
8. Fail-closed connection error handling in PivotController for explicit cloud URIs.
"""
from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict, List
import ibis
import pyarrow as pa
import pytest

from pivot_engine.backends.capabilities import (
    BackendCapabilities,
    detect_ibis_backend_name,
    resolve_backend_capabilities,
)
from pivot_engine.backends.ibis_backend import IbisBackend
from pivot_engine.backends.table_catalog import IbisTableCatalog
from pivot_engine.common.ibis_expression_builder import IbisExpressionBuilder
from pivot_engine.config import ScalablePivotConfig
from pivot_engine.controller import PivotController
from pivot_engine.data_input import normalize_data_input
from pivot_engine.editing.mutation_executor import DatabaseMutationExecutor
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.planner.ibis_planner import IbisPlanner
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import Measure, PivotSpec


class _FakeCloudBackend:
    name = "snowflake"

    def __init__(self, inner_con):
        self._inner = inner_con
        self.table_calls = 0

    def table(self, name: str, **kwargs):
        self.table_calls += 1
        return self._inner.table(name)

    def compile(self, expr):
        return self._inner.compile(expr)


def test_capabilities_matrix_for_cloud_and_local_backends(monkeypatch):
    monkeypatch.delenv("PIVOT_ALLOW_REMOTE_DDL", raising=False)
    monkeypatch.delenv("PIVOT_ALLOW_REMOTE_MUTATIONS", raising=False)

    snowflake_caps = resolve_backend_capabilities(None, connection_uri="snowflake://user:pass@acct/db/public")
    assert snowflake_caps.backend_name == "snowflake"
    assert snowflake_caps.is_cloud_or_remote is True
    assert snowflake_caps.shared_connection_requires_lock is False
    assert snowflake_caps.supports_ddl_materialization is False
    assert snowflake_caps.supports_sql_mutations is False
    assert snowflake_caps.supports_rowid is False
    assert snowflake_caps.supports_ilike is True
    assert snowflake_caps.param_style == "pyformat"

    bigquery_caps = resolve_backend_capabilities(None, connection_uri="bigquery://my-project/my_dataset")
    assert bigquery_caps.backend_name == "bigquery"
    assert bigquery_caps.is_cloud_or_remote is True
    assert bigquery_caps.supports_ilike is False
    assert bigquery_caps.supports_rowid is False

    motherduck_caps = resolve_backend_capabilities(None, connection_uri="md:analytics_db")
    assert motherduck_caps.backend_name == "motherduck"
    assert motherduck_caps.is_cloud_or_remote is True
    assert motherduck_caps.shared_connection_requires_lock is True

    duckdb_caps = resolve_backend_capabilities(None, connection_uri="duckdb://:memory:")
    assert duckdb_caps.backend_name == "duckdb"
    assert duckdb_caps.is_cloud_or_remote is False
    assert duckdb_caps.shared_connection_requires_lock is True
    assert duckdb_caps.supports_ddl_materialization is True
    assert duckdb_caps.supports_sql_mutations is True
    assert duckdb_caps.supports_rowid is True

    # Opt-in remote DDL and mutations
    monkeypatch.setenv("PIVOT_ALLOW_REMOTE_DDL", "1")
    monkeypatch.setenv("PIVOT_ALLOW_REMOTE_MUTATIONS", "1")
    pg_caps = resolve_backend_capabilities(None, connection_uri="postgresql://user:pass@host:5432/db")
    assert pg_caps.backend_name == "postgres"
    assert pg_caps.supports_ddl_materialization is True
    assert pg_caps.supports_sql_mutations is True
    assert pg_caps.supports_rowid is False
    assert pg_caps.param_style == "pyformat"


def test_ibis_backend_uri_generic_routing(monkeypatch):
    called = {}

    def fake_snowflake_connect(**kwargs):
        called["kwargs"] = kwargs
        return ibis.duckdb.connect(":memory:")

    monkeypatch.setattr(ibis.snowflake, "connect", fake_snowflake_connect)
    backend = IbisBackend(connection_uri="snowflake://user:pass@acct/db/schema", warehouse="COMPUTE_WH")
    assert called["kwargs"] == {
        "user": "user",
        "password": "pass",
        "account": "acct",
        "database": "db/schema",
        "warehouse": "COMPUTE_WH",
    }
    assert backend.capabilities.backend_name == "duckdb" or backend.capabilities.is_cloud_or_remote


def test_zero_copy_lazy_ibis_table_registration_in_controller(monkeypatch):
    ext_con = ibis.duckdb.connect(":memory:")
    arrow_src = pa.table({
        "region": ["North", "North", "South"],
        "country": ["US", "CA", "BR"],
        "sales": [100.0, 150.0, 200.0],
    })
    ext_con.create_table("remote_sales", arrow_src, overwrite=True)
    lazy_table = ext_con.table("remote_sales")

    controller = ScalablePivotController(backend_uri=":memory:")
    # Spy on _ibis_to_arrow in data_input to prove normalize_data_input does NOT materialize the full table
    import pivot_engine.data_input as data_input_mod
    full_table_materialized = False
    orig_ibis_to_arrow = data_input_mod._ibis_to_arrow

    def guarded_ibis_to_arrow(expr):
        nonlocal full_table_materialized
        full_table_materialized = True
        return orig_ibis_to_arrow(expr)

    monkeypatch.setattr(data_input_mod, "_ibis_to_arrow", guarded_ibis_to_arrow)

    normalize_data_input(lazy_table, "cloud_sales", controller)
    assert full_table_materialized is False, "normalize_data_input should register Ibis table lazily without calling _ibis_to_arrow()"

    spec = PivotSpec(
        table="cloud_sales",
        rows=["region"],
        measures=[Measure(field="sales", agg="sum", alias="sales_sum")],
        totals=False,
    )
    result = controller.run_pivot(spec, return_format="dict")
    rows_by_region = dict(zip([r[0] for r in result["rows"]], [r[1] for r in result["rows"]]))
    assert rows_by_region == {"North": 250.0, "South": 200.0}


def test_table_catalog_qualified_names_and_ttl_schema_cache():
    con = ibis.duckdb.connect(":memory:")
    con.create_table("orders", pa.table({"id": [1, 2], "amount": [10.5, 20.0]}), overwrite=True)
    fake_cloud = _FakeCloudBackend(con)
    catalog = IbisTableCatalog(fake_cloud, schema_ttl_seconds=300.0)

    schema1, cols1 = catalog.get_schema_and_columns("orders")
    schema2, cols2 = catalog.get_schema_and_columns("orders")
    assert cols1 == ["id", "amount"]
    assert schema1 == schema2
    assert fake_cloud.table_calls == 1, "Second schema lookup should hit TTL cache"

    # Qualified database.schema.table fallback to table when backend is simple
    resolved = catalog.resolve_table("analytics.public.orders")
    assert list(resolved.columns) == ["id", "amount"]


def test_dialect_portable_ilike_and_tenor_fallbacks():
    con = ibis.duckdb.connect(":memory:")
    con.create_table(
        "items",
        pa.table({"name": ["Alpha", "beta", "GAMMA"], "tenor": ["1M", "3M", "1Y"], "val": [1, 2, 3]}),
        overwrite=True,
    )
    builder = IbisExpressionBuilder(con)
    # Simulate BigQuery/SQLite capability where ilike and regex_extract are False
    object.__setattr__(builder.capabilities, "supports_ilike", False)
    object.__setattr__(builder.capabilities, "supports_regex_extract", False)

    tbl = builder.resolve_table("items")
    f_expr = builder.build_filter_expression(
        tbl,
        [{"field": "name", "op": "contains", "value": "ALP", "caseSensitive": False}],
    )
    filtered = tbl.filter(f_expr).to_pyarrow()
    assert filtered.column("name").to_pylist() == ["Alpha"]

    sorts = builder.build_sort_expressions(
        tbl,
        [{"field": "tenor", "order": "asc", "semanticType": "tenor"}],
    )
    assert len(sorts) == 1
    sorted_tbl = tbl.order_by(sorts).to_pyarrow()
    assert set(sorted_tbl.column("tenor").to_pylist()) == {"1M", "3M", "1Y"}


def test_planner_skips_remote_row_count_query_by_default():
    con = ibis.duckdb.connect(":memory:")
    con.create_table("events", pa.table({"k": ["a", "b"], "v": [1, 2]}), overwrite=True)
    fake_cloud = _FakeCloudBackend(con)
    planner = IbisPlanner(con=fake_cloud)
    assert planner.capabilities.is_cloud_or_remote is True

    estimated = planner._estimate_table_row_count("events")
    assert estimated == 100000
    assert fake_cloud.table_calls == 0, "Remote cloud backend should not issue COUNT(*) during cost estimation"


def test_mutation_executor_blocks_readonly_cloud_and_supports_rowid_free_updates():
    controller = ScalablePivotController(backend_uri=":memory:")
    controller.load_data_from_arrow(
        "sales_tbl",
        pa.table({
            "region": ["North", "North", "South"],
            "country": ["US", "CA", "BR"],
            "sales": [100.0, 200.0, 50.0],
        }),
    )

    # 1. Simulate read-only remote backend: mutations must fail closed
    controller.backend_capabilities = resolve_backend_capabilities(
        None,
        connection_uri="snowflake://u:p@acct/db/schema",
        allow_remote_mutations=False,
    )
    with pytest.raises(NotImplementedError, match="SQL mutations are disabled"):
        asyncio.run(
            controller.apply_row_transaction(
                "sales_tbl",
                {"update": [{"key_columns": {"country": "US"}, "updates": {"sales": 125.0}}]},
            )
        )

    # 2. Simulate remote backend with mutations enabled but supports_rowid=False (e.g. Postgres/Snowflake)
    controller.backend_capabilities = BackendCapabilities(
        backend_name="postgres",
        is_cloud_or_remote=True,
        shared_connection_requires_lock=False,
        supports_statement_cancellation=True,
        supports_soft_query_timeout=True,
        supports_arrow_batches=True,
        supports_ddl_materialization=False,
        supports_sql_mutations=True,
        supports_rowid=False,
        supports_ilike=True,
        supports_regex_extract=True,
        supports_quantile=True,
        supports_filter_clause=True,
        supports_grouping_sets=True,
        param_style="qmark",
    )
    tx_res = asyncio.run(
        controller.apply_row_transaction(
            "sales_tbl",
            {
                "update": [
                    {
                        "key_columns": {"region": "North"},
                        "aggregate_edit": {
                            "rowId": "North",
                            "rowPath": "North",
                            "columnId": "sales_sum",
                            "column": "sales",
                            "aggregationFn": "sum",
                            "newValue": 600.0,
                            "propagationStrategy": "equal",
                        },
                        "edit_meta": {"groupingFields": ["region", "country"]},
                    }
                ]
            },
        )
    )
    assert tx_res["applied"]["update"] == 2
    updated_arrow = controller.resolve_table("sales_tbl").filter(
        controller.resolve_table("sales_tbl")["region"] == "North"
    ).to_pyarrow()
    assert sorted(updated_arrow.column("sales").to_pylist()) == [250.0, 350.0]


def test_ibis_backend_record_batches_paged_fallback():
    con = ibis.duckdb.connect(":memory:")
    con.create_table("nums", pa.table({"x": list(range(10))}), overwrite=True)
    expr = con.table("nums").order_by("x")

    class _NoBatchExprWrapper:
        def __init__(self, inner):
            self._inner = inner

        def limit(self, n, offset=0):
            return _NoBatchExprWrapper(self._inner.limit(n, offset=offset))

        def to_pyarrow(self):
            return self._inner.to_pyarrow()

    backend = IbisBackend(connection=con)
    batches = list(backend.execute_record_batches({"ibis_expr": _NoBatchExprWrapper(expr)}, chunk_rows=4))
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 10
    assert [b.num_rows for b in batches] == [4, 4, 2]


def test_materialized_hierarchy_manager_skips_ddl_on_readonly_cloud_and_avoids_disk_write(tmp_path):
    con = ibis.duckdb.connect(":memory:")
    con.create_table("t", pa.table({"a": ["x", "y"], "v": [1, 2]}), overwrite=True)
    fake_cloud = _FakeCloudBackend(con)
    reg_file = tmp_path / "should_not_exist.json"
    mgr = MaterializedHierarchyManager(fake_cloud, cache={}, registry_path=str(reg_file))
    spec = PivotSpec(
        table="t",
        rows=["a"],
        measures=[Measure(field="v", agg="sum", alias="v_sum")],
    )
    mgr.create_materialized_hierarchy(spec)
    assert not reg_file.exists(), "MaterializedHierarchyManager should not write registry file to disk by default"
    assert mgr.rollup_tables == {}


def test_config_cloud_settings_from_env(monkeypatch):
    monkeypatch.setenv("BACKEND_URI", "snowflake://u:p@acct/db/public")
    monkeypatch.setenv("BACKEND_DATABASE", "ANALYTICS")
    monkeypatch.setenv("BACKEND_SCHEMA", "PUBLIC")
    monkeypatch.setenv("PIVOT_ALLOW_REMOTE_DDL", "true")
    monkeypatch.setenv("PIVOT_ALLOW_REMOTE_MUTATIONS", "1")
    monkeypatch.setenv("PIVOT_SCHEMA_CACHE_TTL_SECONDS", "600")

    cfg = ScalablePivotConfig().from_env()
    assert cfg.backend_uri == "snowflake://u:p@acct/db/public"
    assert cfg.backend_database == "ANALYTICS"
    assert cfg.backend_schema == "PUBLIC"
    assert cfg.allow_remote_ddl is True
    assert cfg.allow_remote_mutations is True
    assert cfg.schema_cache_ttl == 600


def test_real_sqlite_ibis_backend_pivot_and_ilike_pushdown(tmp_path):
    """Verify end-to-end non-DuckDB SQL backend execution using Ibis SQLite."""
    db_file = tmp_path / "cloud_sim.sqlite"
    sqlite_uri = f"sqlite:///{db_file.as_posix()}"
    sqlite_con = ibis.sqlite.connect(str(db_file))
    sqlite_con.create_table(
        "warehouse_orders",
        pa.table({
            "region": ["AMER", "AMER", "EMEA", "APAC"],
            "tier": ["Enterprise", "Mid-Market", "Enterprise", "Enterprise"],
            "revenue": [500.0, 200.0, 400.0, 300.0],
        }),
        overwrite=True,
    )

    controller = ScalablePivotController(backend_uri=sqlite_uri)
    assert controller.backend_capabilities.backend_name == "sqlite"
    assert controller.backend_capabilities.supports_ilike is False

    spec = PivotSpec(
        table="warehouse_orders",
        rows=["region"],
        measures=[Measure(field="revenue", agg="sum", alias="revenue_sum")],
        filters=[{"field": "tier", "op": "contains", "value": "enter", "caseSensitive": False}],
        totals=True,
    )
    res = controller.run_pivot(spec, return_format="dict")
    rows_map = {r[0]: r[1] for r in res["rows"] if r[0] is not None}
    assert rows_map["AMER"] == 500.0
    assert rows_map["EMEA"] == 400.0
    assert rows_map["APAC"] == 300.0


def test_tanstack_adapter_and_runtime_service_over_lazy_ibis_table():
    """Verify full TanStackPivotAdapter + PivotRuntimeService virtual scroll & hierarchy over lazy ibis.Table."""
    from pivot_engine import (
        PivotRequestContext,
        PivotRuntimeService,
        PivotViewState,
        SessionRequestGate,
        create_tanstack_adapter,
    )

    ext_con = ibis.duckdb.connect(":memory:")
    ext_con.create_table(
        "cloud_accounts",
        pa.table({
            "region": ["AMER", "AMER", "EMEA", "APAC"],
            "segment": ["Enterprise", "SMB", "Enterprise", "Enterprise"],
            "quarter": ["2025-Q1", "2025-Q1", "2025-Q1", "2025-Q2"],
            "arr": [1200.0, 300.0, 900.0, 600.0],
        }),
        overwrite=True,
    )
    lazy_tbl = ext_con.table("cloud_accounts")

    adapter = create_tanstack_adapter(backend_uri=":memory:")
    normalize_data_input(lazy_tbl, "cloud_accounts", adapter.controller)

    service = PivotRuntimeService(
        adapter_getter=lambda: adapter,
        session_gate=SessionRequestGate(),
    )
    context = PivotRequestContext.from_frontend(
        table="cloud_accounts",
        trigger_prop="pivot.viewport",
        viewport={
            "start": 0,
            "end": 50,
            "window_seq": 1,
            "state_epoch": 1,
            "session_id": "sess-cloud-1",
            "client_instance": "client-1",
            "intent": "structural",
            "needs_col_schema": True,
        },
    )
    state = PivotViewState(
        row_fields=["region", "segment"],
        col_fields=["quarter"],
        val_configs=[{"field": "arr", "agg": "sum", "alias": "ARR"}],
        expanded={"AMER": True},
        show_row_totals=False,
        show_col_totals=True,
    )
    response = service.process(state, context)
    assert response.status == "data"
    assert response.total_rows is not None and response.total_rows >= 3
    assert isinstance(response.data, list) and len(response.data) >= 3


def test_mutation_executor_pyformat_placeholder_adaptation():
    """Verify DatabaseMutationExecutor adapts '?' to '%s' for pyformat cloud/RDBMS backends (Postgres/Snowflake)."""
    executed_queries: List[tuple] = []

    class _FakePyformatRawCon:
        def execute(self, sql: str, params=None):
            executed_queries.append((sql, tuple(params or ())))

            class _Res:
                rowcount = 1
            return _Res()

    class _FakePyformatIbisCon:
        name = "postgres"
        con = _FakePyformatRawCon()

    class _FakePlanner:
        con = _FakePyformatIbisCon()

    caps = resolve_backend_capabilities(
        None,
        connection_uri="postgresql://u:p@host:5432/db",
        allow_remote_mutations=True,
    )
    assert caps.param_style == "pyformat"

    class _FakeController:
        backend_capabilities = caps
        planner = _FakePlanner()

    executor = DatabaseMutationExecutor(_FakeController())
    executor.execute_parameterized_mutation(
        'UPDATE "sales" SET "amount" = ? WHERE "id" = ?',
        [250.0, "row-1"],
    )
    assert executed_queries == [
        ('UPDATE "sales" SET "amount" = %s WHERE "id" = %s', (250.0, "row-1"))
    ]


def test_demo_cloud_warehouse_showcase_smoke():
    """Smoke test for the commercial Cloud Data Warehouse & Ibis showcase (examples/demo_cloud_warehouse.py)."""
    from examples import demo_cloud_warehouse as demo

    adapter = demo.get_adapter()
    resolved_tbl = adapter.controller.resolve_table(demo.TABLE_NAME)
    assert resolved_tbl is not None
    assert "arr" in resolved_tbl.columns
    assert demo.app.layout is not None

    for profile_key in ("snowflake", "bigquery", "postgres", "motherduck", "sqlite", "duckdb"):
        sql = demo.compile_sample_pushdown_sql(profile_key)
        assert "SELECT" in sql.upper()
        bar, drawer_style, drawer_children = demo._update_cloud_telemetry(
            profile_key, ["mutations"], 1
        )
        assert bar is not None
        assert drawer_style.get("display") == "grid"
        assert len(drawer_children) == 2

    client = demo.app.server.test_client()
    index_res = client.get("/")
    assert index_res.status_code == 200
    drill_res = client.get(
        f"/api/drill-through?table={demo.TABLE_NAME}&row_fields=customer_segment&row_path=Growth+SMB&page=0&page_size=10"
    )
    assert drill_res.status_code == 200
    drill_payload = drill_res.get_json()
    assert drill_payload["total_rows"] > 0
    assert len(drill_payload["rows"]) == 10

