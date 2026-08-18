from types import SimpleNamespace

import duckdb
import pyarrow as pa

from pivot_engine.backends.ibis_backend import IbisBackend
from pivot_engine.controller import PivotController
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.util.sql_builder import quote_ident


INJECTED_COLUMN = 'x"); DROP TABLE victim; --'
INJECTED_TABLE = 'loaded; DROP TABLE victim; --'


class DuckDBRawSQL:
    def __init__(self, connection):
        self.connection = connection

    def raw_sql(self, sql):
        return self.connection.execute(sql)


def _table_exists(connection, table_name):
    return bool(
        connection.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
    )


def test_quote_ident_escapes_embedded_quotes():
    assert quote_ident(INJECTED_COLUMN) == '"x""); DROP TABLE victim; --"'


def test_controller_arrow_fallback_treats_table_name_as_identifier():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE victim(x INTEGER)")
    controller = PivotController.__new__(PivotController)
    controller.backend = SimpleNamespace(con=connection)
    controller.planner = SimpleNamespace(con=SimpleNamespace())

    controller.load_data_from_arrow(
        INJECTED_TABLE,
        pa.table({"x": [1]}),
        register_checkpoint=False,
    )

    assert _table_exists(connection, "victim")
    assert _table_exists(connection, INJECTED_TABLE)


def test_ibis_backend_create_index_cannot_execute_injected_column_sql():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE safe(x INTEGER)")
    connection.execute("CREATE TABLE victim(x INTEGER)")
    backend = IbisBackend.__new__(IbisBackend)
    backend.con = DuckDBRawSQL(connection)

    backend.create_index("safe", [INJECTED_COLUMN], "idx_safe")

    assert _table_exists(connection, "victim")


def test_materialized_index_builder_cannot_execute_injected_column_sql():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE safe(x INTEGER)")
    connection.execute("CREATE TABLE victim(x INTEGER)")
    manager = MaterializedHierarchyManager.__new__(MaterializedHierarchyManager)
    manager.backend = DuckDBRawSQL(connection)

    manager._create_index_safely("safe", [INJECTED_COLUMN])

    assert _table_exists(connection, "victim")
