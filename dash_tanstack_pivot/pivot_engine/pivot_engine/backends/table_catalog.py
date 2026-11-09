"""Lazy Ibis table catalog and metadata cache for cloud and local backends.

Decouples logical pivot table identifiers from physical tables on a single
local connection so that:
1. Remote Ibis table expressions (e.g., Snowflake, BigQuery, Postgres,
   MotherDuck, or pre-filtered/joined Ibis views) can be registered lazily
   without materializing full tables into worker memory via ``to_pyarrow()``.
2. Qualified cloud table names (``schema.table`` or ``catalog.schema.table``)
   resolve cleanly across Ibis dialects.
3. Table column lists and schemas are cached with a bounded TTL to avoid
   redundant ``INFORMATION_SCHEMA`` / ``DESCRIBE`` round-trips on cloud warehouses.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


def _default_schema_cache_ttl() -> float:
    raw = os.environ.get("PIVOT_SCHEMA_CACHE_TTL_SECONDS", "300")
    try:
        val = float(raw)
        return max(0.0, val)
    except (TypeError, ValueError):
        return 300.0


class IbisTableCatalog:
    """Registry of lazy Ibis table expressions and cached schema metadata."""

    def __init__(
        self,
        con: Optional[Any] = None,
        *,
        default_database: Optional[str] = None,
        default_schema: Optional[str] = None,
        schema_cache_ttl: Optional[float] = None,
        schema_ttl_seconds: Optional[float] = None,
    ) -> None:
        self._con = con
        self.default_database = default_database or os.environ.get("BACKEND_DATABASE") or None
        self.default_schema = default_schema or os.environ.get("BACKEND_SCHEMA") or None
        effective_ttl = schema_cache_ttl if schema_cache_ttl is not None else schema_ttl_seconds
        self.schema_cache_ttl = (
            _default_schema_cache_ttl()
            if effective_ttl is None
            else max(0.0, float(effective_ttl))
        )
        self._lock = threading.RLock()
        self._registered_tables: Dict[str, Any] = {}
        self._registered_backends: Dict[str, Any] = {}
        self._schema_cache: Dict[str, Tuple[Any, List[str], float]] = {}

    def set_backend(self, con: Optional[Any]) -> None:
        """Update the underlying Ibis backend connection if changed."""
        with self._lock:
            if con is not None and con is not self._con:
                self._con = con
                self._schema_cache.clear()

    @property
    def con(self) -> Optional[Any]:
        return self._con

    @con.setter
    def con(self, value: Optional[Any]) -> None:
        with self._lock:
            self._con = value
            self._schema_cache.clear()

    def register_table(
        self,
        table_name: str,
        table_expr: Any,
        *,
        backend: Optional[Any] = None,
    ) -> None:
        """Register a lazy Ibis table expression under ``table_name``."""
        key = str(table_name or "").strip()
        if not key:
            raise ValueError("table_name must be a non-empty string")
        with self._lock:
            self._registered_tables[key] = table_expr
            if backend is not None:
                self._registered_backends[key] = backend
            self._schema_cache.pop(key, None)

    def unregister_table(self, table_name: str) -> None:
        """Remove a registered lazy table expression and its cached schema."""
        key = str(table_name or "").strip()
        with self._lock:
            self._registered_tables.pop(key, None)
            backend = self._registered_backends.pop(key, None)
            self._schema_cache.pop(key, None)
        if backend is not None and hasattr(backend, "close"):
            try:
                backend.close()
            except Exception:  # nosec B110
                pass

    def has_registered_table(self, table_name: str) -> bool:
        key = str(table_name or "").strip()
        with self._lock:
            return key in self._registered_tables

    def invalidate(self, table_name: Optional[str] = None) -> None:
        """Invalidate cached schema metadata for one table or all tables."""
        with self._lock:
            if table_name is None:
                self._schema_cache.clear()
            else:
                self._schema_cache.pop(str(table_name).strip(), None)

    def resolve_table(
        self,
        table_name: str,
        *,
        fallback_con: Optional[Any] = None,
    ) -> Any:
        """Resolve ``table_name`` to an Ibis ``Table`` expression."""
        key = str(table_name or "").strip()
        if not key:
            raise ValueError("table_name must be a non-empty string")

        with self._lock:
            registered = self._registered_tables.get(key)
        if registered is not None:
            return registered

        con = fallback_con if fallback_con is not None else self._con
        if con is None or not hasattr(con, "table"):
            raise ValueError(f"Cannot resolve table '{key}': no Ibis connection is configured")

        # Handle qualified names: schema.table or database.schema.table
        if "." in key:
            parts = [part.strip() for part in key.split(".") if part.strip()]
            if len(parts) == 2:
                schema_part, name_part = parts
                try:
                    return con.table(name_part, database=schema_part)
                except TypeError:
                    try:
                        return con.table(name_part, schema=schema_part)
                    except Exception:
                        pass
                except Exception:
                    pass
            elif len(parts) == 3:
                db_part, schema_part, name_part = parts
                try:
                    return con.table(name_part, database=(db_part, schema_part))
                except Exception:
                    try:
                        return con.table(name_part, database=f"{db_part}.{schema_part}")
                    except Exception:
                        pass

        # Apply configured default database / schema if set
        if self.default_database or self.default_schema:
            db_arg: Any = None
            if self.default_database and self.default_schema:
                db_arg = f"{self.default_database}.{self.default_schema}"
            else:
                db_arg = self.default_database or self.default_schema
            try:
                return con.table(key, database=db_arg)
            except Exception:
                pass

        return con.table(key)

    def get_schema_and_columns(
        self,
        table_name: str,
        *,
        fallback_con: Optional[Any] = None,
        force_refresh: bool = False,
    ) -> Tuple[Any, List[str]]:
        """Return ``(schema, columns)`` for ``table_name`` using the TTL cache."""
        key = str(table_name or "").strip()
        now = time.monotonic()
        if self.schema_cache_ttl > 0 and not force_refresh:
            with self._lock:
                cached = self._schema_cache.get(key)
                if cached is not None and cached[2] > now:
                    return cached[0], list(cached[1])

        table_expr = self.resolve_table(key, fallback_con=fallback_con)
        schema_fn = getattr(table_expr, "schema", None)
        schema = schema_fn() if callable(schema_fn) else None
        raw_cols = getattr(table_expr, "columns", None)
        if raw_cols is not None:
            columns = list(raw_cols)
        elif schema is not None and hasattr(schema, "names"):
            columns = list(schema.names)
        elif isinstance(schema, dict):
            columns = list(schema.keys())
        else:
            columns = []

        if self.schema_cache_ttl > 0:
            with self._lock:
                self._schema_cache[key] = (schema, columns, now + self.schema_cache_ttl)
        return schema, columns

    def close(self) -> None:
        with self._lock:
            backends = list(self._registered_backends.values())
            self._registered_backends.clear()
            self._registered_tables.clear()
            self._schema_cache.clear()
        for backend in backends:
            if hasattr(backend, "close"):
                try:
                    backend.close()
                except Exception:  # nosec B110
                    pass
