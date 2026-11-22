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


def _first_last(records: Sequence[Record], field_name: Optional[str], order_by: Optional[str], last: bool) -> Any:
    """REQ-AGG-008: among records with both values present, the value at the smallest (first) or largest
    (last) order; ties resolve to the smallest (first) or largest (last) value."""
    if order_by is None:
        raise ValueError("first/last needs an order field")
    pairs = [(r[order_by], r[field_name]) for r in records
             if r.get(field_name) is not None and r.get(order_by) is not None]
    if not pairs:
        return None
    return (max(pairs) if last else min(pairs))[1]


def aggregate(agg: str, records: Sequence[Record], field_name: Optional[str],
              weight: Optional[str] = None, q: Optional[float] = None, order_by: Optional[str] = None) -> Any:
    if agg in {"first", "last"}:
        return _first_last(records, field_name, order_by, agg == "last")
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
    # REQ-NULL-004: empty = NULL or a text of spaces only
    "is_empty": lambda a, _b: a is None or (isinstance(a, str) and a.strip(" ") == ""),
    "is_not_empty": lambda a, _b: not (a is None or (isinstance(a, str) and a.strip(" ") == "")),
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
    order_by: Optional[str] = None  # first / last (REQ-AGG-008)


@dataclass
class Query:
    rows: List[str]
    measures: List[Measure]
    filters: List[Any] = field(default_factory=list)  # Pred | PredGroup, AND-ed
    measure_filters: List[MeasureFilter] = field(default_factory=list)
    scope: Mapping[str, Tuple[Any, ...]] = field(default_factory=dict)
    totals: str = "visible"  # REQ-TOT-003/004
    subtotals: str = "before"  # REQ-LAYOUT-001: group row before (pre-order) or after (post-order) its descendants
    expanded: Optional[Iterable[Tuple[Any, ...]]] = None  # None = all expanded
    sort_desc: Mapping[str, bool] = field(default_factory=dict)  # shorthand: dimension -> descending
    # REQ-SORT-001..004: (field, descending[, absolute[, by[, member]]]), in precedence
    sort: Sequence[Tuple[Any, ...]] = ()
    columns: Sequence[str] = ()  # REQ-PIV-001
    column_members: Sequence[Tuple[Any, ...]] = ()
    derived: Sequence[Any] = ()  # objects with alias, func, of, of2, desc, frame, null_value (REQ-AGG-005/007)
    # REQ-DIM-001: (name, [(predicate, label), ...], fallback), evaluated in order on every record
    categories: Sequence[Tuple[str, Sequence[Tuple[Any, Any]], Any]] = ()
    # REQ-DIM-002: (label, value, key, [(member label, field, order), ...], drop_nulls), after the categories
    unpivot: Optional[Tuple[str, str, str, Sequence[Tuple[str, str, int]], bool]] = None


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
    out = {m.alias: aggregate(m.agg, records, m.field, m.weight, m.q, m.order_by) for m in query.measures}
    for j, member in enumerate(query.column_members):
        matching = [r for r in records if tuple(r.get(c) for c in query.columns) == tuple(member)]
        for m in query.measures:
            out[f"__c{j}__{m.alias}"] = aggregate(m.agg, matching, m.field, m.weight, m.q, m.order_by)
    return out


def with_categories(query: Query, data: Sequence[Record]) -> List[Record]:
    """REQ-DIM-001: each record gets, per category, the first matching rule's label, else the fallback."""
    out = []
    for record in data:
        r = dict(record)
        for name, rules, fallback in query.categories:
            r[name] = next((label for when, label in rules if when(r)), fallback)
        out.append(r)
    return out


def with_unpivot(query: Query, data: Sequence[Record]) -> List[Record]:
    """REQ-DIM-002: one record per (record, member); numeric values compare as floats."""
    if query.unpivot is None:
        return list(data)
    label, value, key, members, drop_nulls = query.unpivot
    out = []
    for member_label, source, order in members:
        for record in data:
            v = record.get(source)
            if v is None and drop_nulls:
                continue
            v = None if v is None else (float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v)
            out.append({**record, label: member_label, value: v, key: order})
    return out


def eligible_sets(query: Query, data: Sequence[Record]) -> Tuple[List[Record], List[Record]]:
    """Return (E0, E) per SEMANTICS §2."""
    data = with_unpivot(query, with_categories(query, data))
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


def _nulls_last(a: Any, b: Any, desc: bool) -> int:
    if a is None or b is None:
        return (a is None) - (b is None)  # NULL after any value, in both directions
    if a == b:
        return 0
    return (-1 if a > b else 1) if desc else (-1 if a < b else 1)


def _order_siblings(query: Query, depth: int, candidates: Iterable[Tuple[Any, ...]],
                    values: Mapping[Tuple[Any, ...], Mapping[str, Any]],
                    member_keys: Optional[Mapping[str, Mapping[Tuple[Any, ...], Any]]] = None) -> List[Tuple[Any, ...]]:
    dim = query.rows[depth - 1]
    entries = [(e[0], e[1], len(e) > 2 and bool(e[2]), e[3] if len(e) > 3 else None, e[4] if len(e) > 4 else None)
               for e in query.sort] or [(f, d, False, None, None) for f, d in query.sort_desc.items()]
    aliases = {m.alias for m in query.measures}

    def compare(x: Tuple[Any, ...], y: Tuple[Any, ...]) -> int:
        dim_sorted = False
        for f, desc, absolute, by, member in entries:
            mag = (lambda v: None if v is None else abs(v)) if absolute else (lambda v: v)
            if f in aliases and member is not None:  # REQ-SORT-004: the node's value in one column member
                cell = (member_keys or {})[("cell", f, tuple(member))]
                c = _nulls_last(mag(cell[x]), mag(cell[y]), desc)
            elif f in aliases:
                c = _nulls_last(mag(values[x][f]), mag(values[y][f]), desc)
            elif f == dim and by is not None:  # REQ-SORT-003: the member's key, then the member value
                c = _nulls_last((member_keys or {})[by][x], (member_keys or {})[by][y], desc)
            elif f == dim:
                dim_sorted = True
                c = _nulls_last(mag(x[-1]), mag(y[-1]), desc)
            else:
                continue
            if c:
                return c
        return 0 if dim_sorted else _nulls_last(x[-1], y[-1], False)

    import functools

    return sorted(candidates, key=functools.cmp_to_key(compare))


def _min_or_none(values: Sequence[Any]) -> Any:
    present = [v for v in values if v is not None]
    return min(present) if present else None


def _flat(query: Query, e: Sequence[Record], source: Sequence[Record]) -> List[Row]:
    """REQ-LAYOUT-002: the leaves, ordered by the sort entries in precedence, then every row dimension
    ascending (NULLS LAST throughout); the grand total last."""
    depth = len(query.rows)
    aliases = {m.alias for m in query.measures}
    leaves = sorted({_key(r, query.rows, depth) for r in e}, key=repr)
    values = {c: _measures(query, [r for r in source if _key(r, query.rows, depth) == c]) for c in leaves}

    def sort_value(c: Tuple[Any, ...], entry: Tuple[Any, ...]) -> Any:
        f = entry[0]
        absolute = len(entry) > 2 and bool(entry[2])
        by = entry[3] if len(entry) > 3 else None
        member = entry[4] if len(entry) > 4 else None
        if f in aliases and member is not None:
            m = next(m for m in query.measures if m.alias == f)
            v = aggregate(m.agg, [r for r in source if _key(r, query.rows, depth) == c
                                  and tuple(r.get(k) for k in query.columns) == tuple(member)], m.field, m.weight, m.q, m.order_by)
        elif f in aliases:
            v = values[c][f]
        elif by is not None:
            level = list(query.rows).index(f) + 1
            v = _min_or_none([r.get(by) for r in source if _key(r, query.rows, level) == c[:level]])
        else:
            v = c[list(query.rows).index(f)]
        return None if v is None else (abs(v) if absolute else v)

    def compare(x: Tuple[Any, ...], y: Tuple[Any, ...]) -> int:
        for entry in query.sort:
            c = _nulls_last(sort_value(x, entry), sort_value(y, entry), bool(entry[1]))
            if c:
                return c
        for i in range(depth):
            c = _nulls_last(x[i], y[i], False)
            if c:
                return c
        return 0

    import functools

    out = [Row(c, values[c]) for c in sorted(leaves, key=functools.cmp_to_key(compare))]
    out.append(Row((), _measures(query, source)))
    if query.derived:
        _derive(query, out)
    return out


def pivot(query: Query, data: Sequence[Record]) -> List[Row]:
    """Flattened visible tree in hierarchical order: grand total first, each group before its
    descendants; with ``subtotals="after"`` the reverse (post-order: grand total last)."""
    e0, e = eligible_sets(query, data)
    totals_source = e if query.totals == "visible" else e0
    expanded = None if query.expanded is None else {tuple(p) for p in query.expanded}

    if query.subtotals == "none" and query.rows:
        return _flat(query, e, totals_source)
    post_order = query.subtotals == "after"
    out: List[Row] = []

    def visit(prefix: Tuple[Any, ...]) -> None:
        depth = len(prefix) + 1
        if depth > len(query.rows):
            return
        candidates = {_key(r, query.rows, depth) for r in e if _key(r, query.rows, depth - 1) == prefix}
        values = {c: _measures(query, [r for r in totals_source if _key(r, query.rows, depth) == c])
                  for c in candidates}
        member_keys: Dict[Any, Dict[Tuple[Any, ...], Any]] = {}
        for entry in query.sort:
            by = entry[3] if len(entry) > 3 else None
            member = entry[4] if len(entry) > 4 else None
            if entry[0] == query.rows[depth - 1] and by is not None:
                member_keys[by] = {c: _min_or_none([r.get(by) for r in totals_source if _key(r, query.rows, depth) == c])
                                   for c in candidates}
            if member is not None:
                m = next(m for m in query.measures if m.alias == entry[0])
                member_keys[("cell", entry[0], tuple(member))] = {
                    c: aggregate(m.agg, [r for r in totals_source if _key(r, query.rows, depth) == c
                                         and tuple(r.get(k) for k in query.columns) == tuple(member)],
                                 m.field, m.weight, m.q, m.order_by)
                    for c in candidates}
        children = _order_siblings(query, depth, candidates, values, member_keys)
        for child in children:
            if not post_order:
                out.append(Row(child, values[child]))
            if expanded is None or child in expanded:
                visit(child)
            if post_order:
                out.append(Row(child, values[child]))

    grand_total = Row((), _measures(query, totals_source))
    if not post_order:
        out.append(grand_total)
    visit(())
    if post_order:
        out.append(grand_total)
    if query.derived:
        _derive(query, out)
    return out


def _derive(query: Query, out: List[Row]) -> None:
    """Derived measures, computed row by row from the finished visible tree."""
    siblings: Dict[Tuple[Any, ...], List[Row]] = {}
    for row in out:  # siblings appear in display order (pre- or post-order alike)
        siblings.setdefault(("p", len(row.path)) + tuple(row.path[:-1]), []).append(row)
    grand = next(r for r in out if r.path == ()).values
    for d in query.derived:
        for group in siblings.values():
            values = [r.values[d.of] for r in group]
            for i, row in enumerate(group):
                v = values[i]
                if d.func == "ratio":
                    den = row.values[d.of2]
                    row.values[d.alias] = d.null_value if den is None or den == 0 else (
                        None if v is None else float(v) / float(den))
                elif d.func == "percent_of_total":
                    total = grand[d.of]
                    row.values[d.alias] = None if total is None or total == 0 or v is None else float(v) / float(total)
                elif d.func in {"rank", "dense_rank"}:
                    if v is None:
                        row.values[d.alias] = None
                        continue
                    better = [x for x in values if x is not None and ((x > v) if d.desc else (x < v))]
                    row.values[d.alias] = 1 + (len(better) if d.func == "rank" else len(set(better)))
                else:
                    start, end = d.frame if d.frame is not None else (None, 0)
                    lo = 0 if start is None else max(0, i + start)
                    hi = len(group) - 1 if end is None else min(len(group) - 1, i + end)
                    window = [x for x in values[lo:hi + 1] if x is not None] if lo <= hi else []
                    if not window:
                        row.values[d.alias] = None
                    elif d.func == "cumulative":
                        row.values[d.alias] = sum(window)
                    else:
                        row.values[d.alias] = math.fsum(float(x) for x in window) / len(window)
