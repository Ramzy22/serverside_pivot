"""Request-owned backend registrations for Arrow partials (Ibis 12)."""

from __future__ import annotations

from typing import Any


def release(backend: Any, relation: Any) -> None:
    """Remove precisely this request's registration after all statements have materialized."""
    op = relation.op()
    if op not in backend._memtables:
        return
    if backend.name == "duckdb":
        backend.con.unregister(op.name)
    elif backend.name == "sqlite":
        backend.drop_table(op.name, database="temp", force=True)
    else:
        backend.drop_table(op.name, force=True)
    backend._memtables.discard(op)
