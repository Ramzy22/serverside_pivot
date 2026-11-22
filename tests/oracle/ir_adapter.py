"""Evaluate a core ``PivotQuery`` with the independent oracle.

Duck-typed on purpose: the oracle never imports the core it referees.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from .oracle import Measure, MeasureFilter, Pred, PredGroup, Query, pivot


def _filter(f: Any) -> Any:
    if hasattr(f, "items") and not hasattr(f, "field"):
        return PredGroup(f.op, tuple(_filter(i) for i in f.items))
    return Pred(f.field, f.op, f.value, f.case_sensitive)


def to_oracle(query: Any) -> Query:
    return Query(
        rows=list(query.rows),
        measures=[Measure(m.alias, m.agg, m.field, m.weight, m.q) for m in query.measures],
        filters=[_filter(f) for f in query.filters],
        measure_filters=[MeasureFilter(f.level, f.measure, f.op, f.value) for f in query.measure_filters],
        totals=query.totals,
        subtotals=query.subtotals,
        expanded=None if query.expanded is None else list(query.expanded),
        sort=[(s.field, s.desc, s.absolute, s.by, s.member) for s in query.sort],
        columns=list(query.columns),
        column_members=list(query.column_members),
        derived=list(query.derived),
        categories=[(c.name, [(_filter(r.when), r.label) for r in c.rules], c.fallback)
                    for c in getattr(query, "categories", ())],
        unpivot=None if getattr(query, "unpivot", None) is None else (
            query.unpivot.label, query.unpivot.value, query.unpivot.key,
            [(m.label, m.field, m.order) for m in query.unpivot.members], query.unpivot.drop_nulls),
    )


def oracle_rows(query: Any, records: Sequence[Dict[str, Any]]) -> List[Tuple[Tuple[Any, ...], Dict[str, Any]]]:
    """The oracle's flattened tree (grand total first), windowed like the core."""
    rows = [(r.path, dict(r.values)) for r in pivot(to_oracle(query), records)]
    end = None if query.window.limit is None else query.window.offset + query.window.limit
    return rows[query.window.offset:end]
