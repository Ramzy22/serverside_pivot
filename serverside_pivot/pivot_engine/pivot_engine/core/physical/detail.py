"""Record-level reads: drill-through / detail pages (REQ-DET-001).

The records under one node are the E0 records whose path dimensions equal
the node's typed path with null-safe equality (a NULL member selects the NULL
group, never "everything").  Pages follow a total order: the requested sort
keys (NULLS LAST), then every column ascending, so pages never overlap or
skip, whichever backend runs them.
"""

from __future__ import annotations

from typing import Any, List, Tuple

from ..ir import DetailQuery
from .union_all import _member_condition, filtered

__all__ = ["lower_detail"]


def _search(table: Any, needle: str) -> Any:
    """Case-insensitive substring over every column's text form; NULL never matches."""
    lowered = needle.lower()
    terms = [table[c].cast("string").lower().contains(lowered).fill_null(False) for c in table.columns]
    out = terms[0]
    for term in terms[1:]:
        out = out | term
    return out


def lower_detail(query: DetailQuery, relation: Any) -> Tuple[Any, Any]:
    """(page expression, count expression) for a detail query."""
    table = filtered(relation, query)
    if query.path_fields:
        table = table.filter(_member_condition(table, table.schema(), query.path_fields, query.path))
    if query.search:
        table = table.filter(_search(table, query.search))
    order: List[Any] = []
    sorted_fields = set()
    for key in query.sort:
        col = table[key.field].abs() if key.absolute else table[key.field]
        order += [col.isnull().cast("int8"), col.desc() if key.desc else col.asc()]
        sorted_fields.add(key.field)
    for name in table.columns:  # total order: deterministic pages
        if name not in sorted_fields:
            order += [table[name].isnull().cast("int8"), table[name].asc()]
    page = table.order_by(order)
    if query.window.limit is not None or query.window.offset:
        page = page.limit(query.window.limit, offset=query.window.offset)
    return page, table.count()

