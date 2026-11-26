"""A core statement's result as Arrow: the one place results cross from a database.

Ibis 12's Oracle backend sets ``oracledb.defaults.fetch_decimals``, so every
Oracle ``NUMBER`` arrives as a ``Decimal`` - including results Ibis types as
``float64`` (an average, a ratio, a formula, a ``DOUBLE PRECISION`` cast),
which pyarrow refuses to convert ("tried to convert to double").  On Oracle
those floating columns are converted here; decimal and integer columns keep
their exact values.

SQLite's parser has a fixed stack: a core tree nests enough derived tables
(``FROM (SELECT ...)``) to overflow it ("parser stack overflow").  On SQLite
the derived tables are hoisted into top-level CTEs (sqlglot's
``eliminate_subqueries``, same meaning) before the statement runs, and the
result is converted exactly as Ibis converts it.  Every other backend uses
Ibis's ``to_pyarrow`` unchanged.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, List, Optional, Sequence, Tuple

import pyarrow as pa

__all__ = ["to_pyarrow"]


def _backend(expr: Any) -> Any:
    try:
        return expr._find_backend()
    except Exception:  # noqa: BLE001 - an unbound expression (memtables only)
        return None


def to_pyarrow(expr: Any, backend: Optional[Any] = None) -> Any:
    """``expr.to_pyarrow()`` on ``backend`` (default: the expression's own)."""
    backend = backend if backend is not None else _backend(expr)
    if backend is None:
        return expr.to_pyarrow()
    name = str(getattr(backend, "name", ""))
    if name == "sqlite":
        return _sqlite_to_pyarrow(backend, expr)
    if name != "oracle":
        return backend.to_pyarrow(expr)
    table_expr = expr.as_table()
    schema = table_expr.schema()
    floating = [i for i, dtype in enumerate(schema.types) if dtype.is_floating()]
    if not floating:
        return backend.to_pyarrow(expr)
    backend._run_pre_execute_hooks(table_expr)
    arrow_schema = schema.to_pyarrow()
    struct = schema.as_struct().to_pyarrow()

    def row(values: Sequence[Any]) -> Tuple[Any, ...]:
        out: List[Any] = list(values)
        for i in floating:
            if isinstance(out[i], Decimal):
                out[i] = float(out[i])
        return tuple(out)

    batches = [pa.RecordBatch.from_struct_array(pa.array([row(r) for r in batch], type=struct))
               for batch in backend._cursor_batches(table_expr, params=None, limit=None, chunk_size=1_000_000)]
    table = pa.Table.from_batches(batches, schema=arrow_schema)
    return expr.__pyarrow_result__(table.rename_columns(list(table_expr.columns)).cast(arrow_schema))


def _sqlite_to_pyarrow(backend: Any, expr: Any) -> Any:
    """``backend.to_pyarrow(expr)`` with the statement's derived tables flattened into CTEs."""
    import sqlglot as sg

    backend._run_pre_execute_hooks(expr)
    table_expr = expr.as_table()
    schema = table_expr.schema()
    sql = _flattened(backend.compiler.to_sqlglot(expr).sql(dialect=backend.dialect,
                                                            unsupported_level=sg.ErrorLevel.RAISE), backend.dialect)
    with backend._safe_raw_sql(sql) as cursor:
        frame = backend._fetch_from_cursor(cursor, schema)
    table = pa.Table.from_pandas(frame, schema=schema.to_pyarrow(), preserve_index=False)
    return expr.__pyarrow_result__(table)


def _flattened(sql: str, dialect: Any) -> str:
    """``sql`` with its derived tables as CTEs; re-parsed from text, as the optimizer expects a parsed tree
    (Ibis-built CTE nodes lack ``unwrap``).  A statement the optimizer cannot rewrite runs unflattened:
    the same meaning."""
    import logging

    import sqlglot as sg
    from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries

    try:
        return str(eliminate_subqueries(sg.parse_one(sql, read=dialect)).sql(dialect=dialect))
    except Exception:  # noqa: BLE001 - any optimizer failure keeps the original statement
        logging.getLogger(__name__).debug("SQLite statement not flattened", exc_info=True)
        return sql
