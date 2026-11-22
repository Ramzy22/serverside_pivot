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


def _weighted_avg(col: Any, t: Any, m: Any) -> Any:
    weight = t[m.weight]
    valid = col.notnull() & weight.notnull()  # only complete (x, w) pairs count
    numerator = valid.ifelse(col.cast("float64") * weight.cast("float64"), ibis.null()).sum()
    denominator = valid.ifelse(weight.cast("float64"), ibis.null()).sum()
    return ibis.ifelse(denominator.coalesce(0) == 0, ibis.null(), numerator / denominator)


# Every lowering ignores NULL inputs (SQL semantics) except count(*), matching
# the registry's "nulls" rules.
LOWERINGS: Dict[str, Lowering] = {
    "sum": _sum,
    "count": _count,
    "avg": lambda col, _t, _m: col.mean(),
    "min": lambda col, _t, _m: col.min(),
    "max": lambda col, _t, _m: col.max(),
    "count_distinct": lambda col, _t, _m: col.nunique(),  # exact (REQ-AGG-003)
    "median": lambda col, _t, _m: col.cast("float64").median(),
    "stddev": lambda col, _t, _m: col.cast("float64").std(how="sample"),
    "variance": lambda col, _t, _m: col.cast("float64").var(how="sample"),
    "percentile": lambda col, _t, m: col.cast("float64").quantile(float(m.q)),
    "weighted_avg": _weighted_avg,
}


def check_drift(registry: Registry) -> None:
    names = set(registry.names())
    if names != set(LOWERINGS):
        raise RuntimeError(
            f"aggregation registry drift: spec-only={sorted(names - set(LOWERINGS))} "
            f"lowering-only={sorted(set(LOWERINGS) - names)}"
        )


def lower(measure: Any, table: Any) -> Any:
    REGISTRY.get(measure.agg)  # unknown aggregation -> typed error
    column = None if measure.field is None else table[measure.field]
    return LOWERINGS[measure.agg](column, table, measure)


check_drift(REGISTRY)
