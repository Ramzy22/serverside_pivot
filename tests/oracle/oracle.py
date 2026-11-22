"""Reference pivot oracle (docs/semantics/SEMANTICS.md).

Pure Python, record by record, deliberately naive: no SQL, no Ibis, no
pandas.  It is the definition of a correct answer, so it favours obviousness
over speed.  Do not import engine code from here.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

Record = Mapping[str, Any]


# ----------------------------------------------------------------- aggregations
# REQ-AGG-001/002: one definition per aggregation; NULLs ignored except count(*).


def _vals(records: Sequence[Record], field_name: Optional[str]) -> List[Any]:
    return [r.get(field_name) for r in records if r.get(field_name) is not None]


def _sum(values: List[Any]) -> Any:
    if not values:
        return None
    if any(isinstance(v, Decimal) for v in values):
        return sum((Decimal(str(v)) for v in values), Decimal(0))
    if all(isinstance(v, int) and not isinstance(v, bool) for v in values):
        return sum(values)
    return math.fsum(float(v) for v in values)


def _avg(values: List[Any]) -> Any:
    if not values:
        return None
    if any(isinstance(v, Decimal) for v in values):
        return _sum(values) / Decimal(len(values))
    return math.fsum(float(v) for v in values) / len(values)


def _median(values: List[Any]) -> Any:
    return statistics.median(values) if values else None


def _sample_variance(values: List[Any]) -> Any:
    return statistics.variance([float(v) for v in values]) if len(values) >= 2 else None


def _percentile(values: List[Any], q: float) -> Any:
    """Continuous percentile with linear interpolation (QUANTILE_CONT)."""
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    position = q * (len(ordered) - 1)
    low = math.floor(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def _weighted_avg(records: Sequence[Record], field_name: str, weight: str) -> Any:
    pairs = [(float(r[field_name]), float(r[weight])) for r in records
             if r.get(field_name) is not None and r.get(weight) is not None]
    total_weight = math.fsum(w for _, w in pairs)
    if not pairs or total_weight == 0:
        return None
    return math.fsum(x * w for x, w in pairs) / total_weight


AGGREGATIONS: Dict[str, Callable[[Sequence[Record], Optional[str]], Any]] = {
    "sum": lambda rs, f: _sum(_vals(rs, f)),
    "avg": lambda rs, f: _avg(_vals(rs, f)),
    "min": lambda rs, f: min(_vals(rs, f)) if _vals(rs, f) else None,
    "max": lambda rs, f: max(_vals(rs, f)) if _vals(rs, f) else None,
    "count": lambda rs, f: len(rs) if f in (None, "*") else len(_vals(rs, f)),
    "count_distinct": lambda rs, f: len(set(_vals(rs, f))),
    "median": lambda rs, f: _median(_vals(rs, f)),
    "variance": lambda rs, f: _sample_variance(_vals(rs, f)),
    "stddev": lambda rs, f: (lambda v: None if v is None else math.sqrt(v))(_sample_variance(_vals(rs, f))),
}


def aggregate(agg: str, records: Sequence[Record], field_name: Optional[str],
              weight: Optional[str] = None, q: Optional[float] = None) -> Any:
    if agg == "percentile":
        return _percentile(_vals(records, field_name), float(q))
    if agg == "weighted_avg":
        return _weighted_avg(records, field_name, weight)
    try:
        fn = AGGREGATIONS[agg]
    except KeyError:  # REQ-AGG-001: unknown aggregation is an error
        raise ValueError(f"unknown aggregation {agg!r}") from None
    return fn(records, field_name)


# ----------------------------------------------------------------- filters

_OPS: Dict[str, Callable[[Any, Any], bool]] = {
    "=": lambda a, b: a is not None and a == b,
    "!=": lambda a, b: a is not None and a != b,
    ">": lambda a, b: a is not None and a > b,
    ">=": lambda a, b: a is not None and a >= b,
    "<": lambda a, b: a is not None and a < b,
    "<=": lambda a, b: a is not None and a <= b,
    # Membership is set membership (REQ-NULL-003): None in the list selects NULL.
    "in": lambda a, b: a in set(b),
    "not_in": lambda a, b: a not in set(b),
    "between": lambda a, b: a is not None and None not in b and b[0] <= a <= b[1],
    "contains": lambda a, b: a is not None and b in a,
    "not_contains": lambda a, b: a is not None and b not in a,
    "starts_with": lambda a, b: a is not None and a.startswith(b),
    "ends_with": lambda a, b: a is not None and a.endswith(b),
    "is_null": lambda a, _b: a is None,
    "is_not_null": lambda a, _b: a is not None,
}


@dataclass(frozen=True)
class Pred:
    field: str
    op: str
    value: Any = None
    case_sensitive: bool = True

    def __call__(self, record: Record) -> bool:
        a, b = record.get(self.field), self.value
        folds = self.op in {"=", "!=", "contains", "not_contains", "starts_with", "ends_with"}
        if folds and not self.case_sensitive and isinstance(a, str) and isinstance(b, str):
            a, b = a.lower(), b.lower()
        return _OPS[self.op](a, b)


@dataclass(frozen=True)
class PredGroup:
    op: str  # "and" | "or"
    items: Tuple[Any, ...]

    def __call__(self, record: Record) -> bool:
        results = (item(record) for item in self.items)
        return all(results) if self.op == "and" else any(results)


@dataclass(frozen=True)
class MeasureFilter:
    """A HAVING-style filter at one hierarchy level (REQ-TOT-001)."""

    level: int  # 1-based depth in rows
    measure: str  # measure alias
    op: str
    value: Any


@dataclass(frozen=True)
class Measure:
    alias: str
    agg: str
    field: Optional[str]
    weight: Optional[str] = None
    q: Optional[float] = None


@dataclass
class Query:
    rows: List[str]
    measures: List[Measure]
    filters: List[Any] = field(default_factory=list)  # Pred | PredGroup, AND-ed
    measure_filters: List[MeasureFilter] = field(default_factory=list)
    scope: Mapping[str, Tuple[Any, ...]] = field(default_factory=dict)
    totals: str = "visible"  # REQ-TOT-003/004
    expanded: Optional[Iterable[Tuple[Any, ...]]] = None  # None = all expanded
    sort_desc: Mapping[str, bool] = field(default_factory=dict)  # sibling order per dimension


# ----------------------------------------------------------------- engine


def in_scope(record: Record, scope: Mapping[str, Tuple[Any, ...]]) -> bool:
    """REQ-SEC-003: exact typed equality; NULL grant value matches only NULL."""
    return all(any(_exact_equal(record.get(name), a) for a in allowed) for name, allowed in scope.items())


def _exact_equal(value: Any, allowed: Any) -> bool:
    if value is None or allowed is None:
        return value is None and allowed is None
    if isinstance(value, bool) or isinstance(allowed, bool):
        return type(value) is type(allowed) and value == allowed
    if isinstance(value, str) or isinstance(allowed, str):
        return isinstance(value, str) and isinstance(allowed, str) and value == allowed  # binary
    return value == allowed  # numbers compare by value


def _key(record: Record, rows: Sequence[str], depth: int) -> Tuple[Any, ...]:
    return tuple(record.get(f) for f in rows[:depth])


def _measures(query: Query, records: Sequence[Record]) -> Dict[str, Any]:
    return {m.alias: aggregate(m.agg, records, m.field, m.weight, m.q) for m in query.measures}


def eligible_sets(query: Query, data: Sequence[Record]) -> Tuple[List[Record], List[Record]]:
    """Return (E0, E) per SEMANTICS §2."""
    e0 = [r for r in data if in_scope(r, query.scope) and all(p(r) for p in query.filters)]
    passing: Dict[int, set] = {}
    for level in sorted({mf.level for mf in query.measure_filters}):
        groups: Dict[Tuple[Any, ...], List[Record]] = {}
        for r in e0:
            groups.setdefault(_key(r, query.rows, level), []).append(r)
        level_filters = [mf for mf in query.measure_filters if mf.level == level]
        passing[level] = {
            k
            for k, members in groups.items()
            if all(_OPS[mf.op](_measures(query, members)[mf.measure], mf.value) for mf in level_filters)
        }
    e = [r for r in e0 if all(_key(r, query.rows, lvl) in keys for lvl, keys in passing.items())]
    return e0, e


@dataclass(frozen=True)
class Row:
    path: Tuple[Any, ...]  # () = grand total
    values: Mapping[str, Any]

    @property
    def depth(self) -> int:
        return len(self.path)


def pivot(query: Query, data: Sequence[Record]) -> List[Row]:
    """Flattened visible tree in hierarchical order, grand total first."""
    e0, e = eligible_sets(query, data)
    totals_source = e if query.totals == "visible" else e0
    expanded = None if query.expanded is None else {tuple(p) for p in query.expanded}

    out = [Row((), _measures(query, totals_source))]

    def visit(prefix: Tuple[Any, ...]) -> None:
        depth = len(prefix) + 1
        if depth > len(query.rows):
            return
        candidates = {_key(r, query.rows, depth) for r in e if _key(r, query.rows, depth - 1) == prefix}
        non_null = sorted((k for k in candidates if k[-1] is not None), key=lambda k: k[-1],
                          reverse=bool(query.sort_desc.get(query.rows[depth - 1])))
        children = non_null + [k for k in candidates if k[-1] is None]  # NULLS LAST both ways
        for child in children:
            members = [r for r in totals_source if _key(r, query.rows, depth) == child]
            out.append(Row(child, _measures(query, members)))
            if expanded is None or child in expanded:
                visit(child)

    visit(())
    return out
