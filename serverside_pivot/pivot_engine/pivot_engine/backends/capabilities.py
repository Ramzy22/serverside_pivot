"""Backend capability and dialect descriptor contract for Ibis and SQL backends.

Higher layers (runtime, adapter, controller, planner, editing, export) inspect
``BackendCapabilities`` rather than checking ``backend_name == "duckdb"`` or
probing driver internals directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional


_CLOUD_AND_CLIENT_SERVER_BACKENDS = frozenset({
    "motherduck",
    "postgres",
    "postgresql",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",
    "trino",
    "athena",
    "mysql",
    "mariadb",
    "mssql",
    "oracle",
    "impala",
    "pyspark",
    "risingwave",
    "exasol",
    "flink",
})

_EMBEDDED_SHARED_LOCK_BACKENDS = frozenset({
    "duckdb",
    "motherduck",
    "sqlite",
})

_ROWID_BACKENDS = frozenset({
    "duckdb",
    "sqlite",
})

_ILIKE_BACKENDS = frozenset({
    "duckdb",
    "motherduck",
    "postgres",
    "postgresql",
    "snowflake",
    "clickhouse",
    "databricks",
    "trino",
    "risingwave",
})

_REGEX_EXTRACT_BACKENDS = frozenset({
    "duckdb",
    "motherduck",
    "postgres",
    "postgresql",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",
    "trino",
    "mysql",
    "pyspark",
})

_QUANTILE_BACKENDS = frozenset({
    "duckdb",
    "motherduck",
    "postgres",
    "postgresql",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",
    "trino",
    "mysql",
    "mssql",
    "oracle",
})

_FILTER_CLAUSE_BACKENDS = frozenset({
    "duckdb",
    "motherduck",
    "postgres",
    "postgresql",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",
    "trino",
    "oracle",
})

_GROUPING_SETS_BACKENDS = frozenset({
    "duckdb",
    "motherduck",
    "postgres",
    "postgresql",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",
    "trino",
    "mysql",
    "mssql",
    "oracle",
})

_PYFORMAT_PARAM_BACKENDS = frozenset({
    "postgres",
    "postgresql",
    "mysql",
    "mariadb",
    "snowflake",
    "clickhouse",
    "pymssql",
})


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def detect_ibis_backend_name(
    con: Optional[Any],
    connection_uri: Optional[str] = None,
) -> str:
    """Detect canonical lowercase backend name from an Ibis connection or URI."""
    if con is not None:
        name = getattr(con, "name", None)
        if isinstance(name, str) and name.strip():
            normalized = name.strip().lower()
            return "postgres" if normalized == "postgresql" else normalized
        backend_attr = getattr(con, "_backend", None)
        if backend_attr is not None:
            cls_name = getattr(backend_attr, "__class__", type(con)).__name__.lower()
            if cls_name.endswith("backend"):
                cls_name = cls_name[: -len("backend")]
            return "postgres" if cls_name == "postgresql" else (cls_name or "unknown")

    if isinstance(connection_uri, str) and connection_uri.strip():
        uri = connection_uri.strip().lower()
        if uri == ":memory:" or uri.startswith(":memory:"):
            return "duckdb"
        if uri.startswith(("md:", "motherduck:", "duckdb://md:")):
            return "motherduck"
        if "://" in uri:
            scheme = uri.split("://", 1)[0].split("+", 1)[0]
            if scheme in {"postgresql", "postgres"}:
                return "postgres"
            if scheme in {"md", "motherduck"}:
                return "motherduck"
            if scheme == "duckdb":
                return "duckdb"
            return scheme
    return "unknown"


def _is_motherduck_uri(connection_uri: Optional[str], con: Optional[Any] = None) -> bool:
    for candidate in (
        connection_uri,
        getattr(con, "uri", None),
        getattr(getattr(con, "con", None), "database", None),
    ):
        if isinstance(candidate, str):
            lowered = candidate.strip().lower()
            if lowered.startswith(("md:", "md://", "motherduck:", "motherduck://")):
                return True
    return False


@dataclass(frozen=True)
class BackendCapabilities:
    """Immutable capability descriptor for an analytical or transactional backend."""

    backend_name: str = "unknown"
    is_cloud_or_remote: bool = False
    shared_connection_requires_lock: bool = False
    supports_statement_cancellation: bool = False
    supports_soft_query_timeout: bool = True
    supports_arrow_batches: bool = False
    supports_ddl_materialization: bool = False
    supports_sql_mutations: bool = False
    supports_rowid: bool = False
    supports_ilike: bool = True
    supports_regex_extract: bool = True
    supports_quantile: bool = True
    supports_filter_clause: bool = True
    supports_grouping_sets: bool = True
    param_style: str = "qmark"


def resolve_backend_capabilities(
    con: Optional[Any],
    *,
    connection_uri: Optional[str] = None,
    has_cancellable_pool: bool = False,
    allow_remote_ddl: Optional[bool] = None,
    allow_remote_mutations: Optional[bool] = None,
    read_only: Optional[bool] = None,
) -> BackendCapabilities:
    """Build deterministic ``BackendCapabilities`` for an Ibis or native connection."""
    backend_name = detect_ibis_backend_name(con, connection_uri=connection_uri)
    is_motherduck = _is_motherduck_uri(connection_uri, con)
    is_cloud_or_remote = (
        backend_name in _CLOUD_AND_CLIENT_SERVER_BACKENDS
        or is_motherduck
    )

    effective_read_only = (
        read_only
        if read_only is not None
        else _env_bool("BACKEND_READ_ONLY", default=False)
    )
    effective_allow_remote_ddl = (
        allow_remote_ddl
        if allow_remote_ddl is not None
        else _env_bool("PIVOT_ALLOW_REMOTE_DDL", default=False)
    )
    effective_allow_remote_mutations = (
        allow_remote_mutations
        if allow_remote_mutations is not None
        else _env_bool("PIVOT_ALLOW_REMOTE_MUTATIONS", default=False)
    )

    shared_connection_requires_lock = (
        backend_name in _EMBEDDED_SHARED_LOCK_BACKENDS or is_motherduck
    )

    has_parameterized_exec = bool(
        con is not None
        and (
            callable(getattr(getattr(con, "con", None), "execute", None))
            or callable(getattr(getattr(con, "_con", None), "execute", None))
            or callable(getattr(con, "execute", None))
            or callable(getattr(con, "raw_sql", None))
        )
    )

    if effective_read_only:
        supports_ddl = False
        supports_mutations = False
    elif is_cloud_or_remote:
        supports_ddl = bool(effective_allow_remote_ddl and (con is None or hasattr(con, "create_table")))
        supports_mutations = bool(effective_allow_remote_mutations and (con is None or has_parameterized_exec))
    else:
        # Local embedded DuckDB shares one connection across viewport threads;
        # background DDL materialization is disabled unless explicitly enabled,
        # while direct mutations and Arrow batching are supported.
        supports_ddl = bool(con is None or hasattr(con, "create_table"))
        supports_mutations = bool((con is None or has_parameterized_exec) and backend_name != "unknown")

    supports_arrow_batches = bool(
        has_cancellable_pool
        or (
            con is not None
            and backend_name != "unknown"
            and (
                callable(getattr(con, "to_pyarrow_batches", None))
                or callable(getattr(con, "to_pyarrow", None))
                or callable(getattr(con, "table", None))
            )
        )
    )

    param_style = (
        "pyformat"
        if backend_name in _PYFORMAT_PARAM_BACKENDS
        else "qmark"
    )

    return BackendCapabilities(
        backend_name=backend_name,
        is_cloud_or_remote=is_cloud_or_remote,
        shared_connection_requires_lock=shared_connection_requires_lock,
        supports_statement_cancellation=bool(has_cancellable_pool),
        supports_soft_query_timeout=True,
        supports_arrow_batches=supports_arrow_batches,
        supports_ddl_materialization=supports_ddl,
        supports_sql_mutations=supports_mutations,
        supports_rowid=backend_name in _ROWID_BACKENDS and not is_motherduck,
        supports_ilike=backend_name in _ILIKE_BACKENDS,
        supports_regex_extract=backend_name in _REGEX_EXTRACT_BACKENDS,
        supports_quantile=backend_name in _QUANTILE_BACKENDS,
        supports_filter_clause=backend_name in _FILTER_CLAUSE_BACKENDS,
        supports_grouping_sets=backend_name in _GROUPING_SETS_BACKENDS,
        param_style=param_style,
    )
