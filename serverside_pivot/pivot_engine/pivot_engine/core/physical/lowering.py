"""Ibis lowering of every registry aggregation (REQ-AGG-001..004).

The registry (``core/registry.py``) holds engine-free metadata read from
``core/spec/aggregation-registry.json``; this module binds each entry to its
Ibis expression.  :func:`check_drift` runs at import, so a spec entry without
a lowering (or the reverse) can never ship.
"""

from __future__ import annotations

from typing import Any, Callable, Dict

import ibis

from ..registry import REGISTRY, Registry

__all__ = ["LOWERINGS", "check_drift", "lower"]

Lowering = Callable[[Any, Any, Any], Any]  # (column or None for count(*), table, measure) -> Ibis scalar


def _sum(col: Any, _t: Any, _m: Any) -> Any:
    return col.sum()


def _count(col: Any, t: Any, _m: Any) -> Any:
    return t.count() if col is None else col.count()


def _weighted_avg(col: Any, t: Any, m: Any, weight: Any = None) -> Any:
    weight = t[m.weight] if weight is None else weight
    valid = col.notnull() & weight.notnull()  # only complete (x, w) pairs count
    numerator = valid.ifelse(col.cast("float64") * weight.cast("float64"), ibis.null()).sum()
    denominator = valid.ifelse(weight.cast("float64"), ibis.null()).sum()
    return ibis.ifelse(denominator.coalesce(0) == 0, ibis.null(), numerator / denominator)


def _ordered(col: Any, t: Any, m: Any, last: bool) -> Any:
    """REQ-AGG-008: the value at the extreme order; ties resolve to the extreme value, so it is deterministic."""
    order = t[m.order_by]
    valid = col.notnull() & order.notnull()
    return (col.last if last else col.first)(where=valid, order_by=[order, col])


def _series(col: Any, t: Any, m: Any) -> Any:
    """REQ-AGG-009: the values as a list in a defined order: the order field, ties by value, else by value."""
    if m.order_by is None:
        return col.collect(where=col.notnull(), order_by=[col])
    order = t[m.order_by]
    return col.collect(where=col.notnull() & order.notnull(), order_by=[order, col])


# Every lowering ignores NULL inputs (SQL semantics) except count(*), matching
# the registry's "nulls" rules.
LOWERINGS: Dict[str, Lowering] = {
    "sum": _sum,
    "count": _count,
    "avg": lambda col, _t, _m: col.mean(),
    "min": lambda col, _t, _m: col.min(),
    "max": lambda col, _t, _m: col.max(),
    # exact, NULL ignored on every backend (REQ-AGG-003)
    "count_distinct": lambda col, _t, _m: col.nunique(where=col.notnull()),
    "median": lambda col, _t, _m: col.cast("float64").median(),
    "stddev": lambda col, _t, _m: col.cast("float64").std(how="sample"),
    "variance": lambda col, _t, _m: col.cast("float64").var(how="sample"),
    "percentile": lambda col, _t, m: col.cast("float64").quantile(float(m.q)),
    "weighted_avg": _weighted_avg,
    "first": lambda col, t, m: _ordered(col, t, m, last=False),
    "last": lambda col, t, m: _ordered(col, t, m, last=True),
    "array_agg": _series,
}


def check_drift(registry: Registry) -> None:
    names = set(registry.names())
    if names != set(LOWERINGS):
        raise RuntimeError(
            f"aggregation registry drift: spec-only={sorted(names - set(LOWERINGS))} "
            f"lowering-only={sorted(set(LOWERINGS) - names)}"
        )


def lower(measure: Any, table: Any, condition: Any = None) -> Any:
    """Aggregate ``measure``; with ``condition``, over the matching records only (a pivot cell)."""
    REGISTRY.get(measure.agg)  # unknown aggregation -> typed error
    if condition is None:
        column = None if measure.field is None else table[measure.field]
        return LOWERINGS[measure.agg](column, table, measure)
    if measure.field is None:  # count(*) of the matching records
        return ibis.ifelse(condition, 1, ibis.null()).count()
    column = condition.ifelse(table[measure.field], ibis.null())
    if measure.agg == "weighted_avg":
        return _weighted_avg(column, table, measure, condition.ifelse(table[measure.weight], ibis.null()))
    return LOWERINGS[measure.agg](column, table, measure)


check_drift(REGISTRY)
