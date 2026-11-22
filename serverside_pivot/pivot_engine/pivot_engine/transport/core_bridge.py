"""Edge between the legacy request contract and the semantic core.

* :func:`spec_to_query` maps a legacy ``PivotSpec`` + expansion to a core
  :class:`~pivot_engine.core.ir.PivotQuery`, or raises :class:`CoreUnsupported`
  listing every feature the core does not express yet (never a silent
  approximation).
* :func:`core_hierarchy_view` answers the ``controller.run_hierarchy_view``
  contract from the core, so every adapter-based edge (Dash, HTTP gateway,
  Streamlit) is served through one execution boundary.

Row identity: ``_path`` / ``_id`` / ``_pathFields`` / ``depth`` keep the legacy
display contract; ``_key`` carries the canonical typed identity
(REQ-ID-001), which the frontend adopts in M3.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from ..core.authority.verifier import verify_scoped
from ..core.exec import execute
from ..core.identity import GRAND_TOTAL_DISPLAY_PATH, CanonicalPathCodec
from ..core.ir import (
    MeasureFilter, MeasureRef, PivotQuery, Predicate, PredicateGroup, SortKey, Window,
)
from ..core.physical import TREES, choose
from ..core.logical import plan as logical_plan
from ..core.registry import type_family

__all__ = ["CoreUnsupported", "core_hierarchy_view", "spec_to_query"]

GRAND_TOTAL_ID = "Grand Total"
ALL_EXPANDED = "__ALL__"

_AGG = {
    "sum": "sum", "avg": "avg", "mean": "avg", "min": "min", "max": "max", "count": "count",
    "count_distinct": "count_distinct", "distinct_count": "count_distinct", "median": "median",
    "stddev": "stddev", "variance": "variance", "percentile": "percentile",
    "weighted_avg": "weighted_avg", "wavg": "weighted_avg", "weighted_mean": "weighted_avg",
}
_OPS = {
    "=": "=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">=", "in": "in", "not in": "not_in",
    "not_in": "not_in", "between": "between", "contains": "contains", "not_contains": "not_contains",
    "starts_with": "starts_with", "ends_with": "ends_with", "is_null": "is_null", "is_not_null": "is_not_null",
}
_MEASURE_FILTER_OPS = {"=", "!=", "<", "<=", ">", ">="}


class CoreUnsupported(Exception):
    """The request uses features the core does not express (yet); serve it elsewhere."""

    def __init__(self, reasons: Sequence[str]) -> None:
        self.reasons: Tuple[str, ...] = tuple(sorted(set(reasons)))
        super().__init__("core does not support: " + ", ".join(self.reasons))


# --------------------------------------------------------------------- values


def _coerce(value: Any, family: str) -> Any:
    """Legacy UI values arrive as strings; coerce them exactly to the column family."""
    if value is None or not isinstance(value, str):
        return value
    text = value.strip()
    try:
        if family == "integer":
            return int(text)
        if family == "floating":
            return float(text)
        if family == "decimal":
            return Decimal(text)
    except (ValueError, InvalidOperation):
        raise CoreUnsupported([f"value {value!r} is not a {family}"]) from None
    if family == "boolean" and text.lower() in {"true", "false"}:
        return text.lower() == "true"
    return value


# --------------------------------------------------------------------- filters


def _flatten_fields(f: Mapping[str, Any]) -> List[str]:
    if "conditions" in f:
        return [x for c in f.get("conditions") or [] for x in _flatten_fields(c)]
    return [str(f.get("field"))]


def _dimension_filter(f: Mapping[str, Any], families: Mapping[str, str], reasons: List[str]) -> Any:
    if "conditions" in f:
        op = str(f.get("op") or "AND").lower()
        if op not in {"and", "or"}:
            reasons.append(f"filter group operator {op!r}")
            return None
        items = [_dimension_filter(c, families, reasons) for c in f.get("conditions") or []]
        items = [i for i in items if i is not None]
        return PredicateGroup(op, tuple(items)) if items else None
    field = f.get("field")
    if field not in families:
        reasons.append(f"filter on unknown field {field!r}")
        return None
    op = _OPS.get(str(f.get("op") or "="))
    if op is None:
        reasons.append(f"filter operator {f.get('op')!r}")
        return None
    family = families[field]
    value = f.get("value")
    if op in {"in", "not_in"}:
        value = tuple(_coerce(v, family) for v in (value if isinstance(value, (list, tuple, set)) else [value]))
    elif op == "between":
        if not (isinstance(value, (list, tuple)) and len(value) == 2):
            reasons.append("between without [low, high]")
            return None
        value = (_coerce(value[0], family), _coerce(value[1], family))
    elif op not in {"is_null", "is_not_null", "contains", "not_contains", "starts_with", "ends_with"}:
        value = _coerce(value, family)
    return Predicate(str(field), op, value, bool(f.get("caseSensitive", False)))


def _filters(spec: Any, aliases: FrozenSet[str], families: Mapping[str, str], depth: int,
             reasons: List[str]) -> Tuple[Tuple[Any, ...], Tuple[MeasureFilter, ...]]:
    rows: List[Any] = []
    measure_filters: List[MeasureFilter] = []
    for f in spec.filters or []:
        fields = set(_flatten_fields(f))
        if fields & aliases:
            # REQ-TOT-007: a level-less measure filter applies at the deepest row level.
            if "conditions" in f or not fields <= aliases:
                reasons.append("compound measure filter")
                continue
            op = str(f.get("op") or "=")
            if op not in _MEASURE_FILTER_OPS or depth == 0:
                reasons.append(f"measure filter {op!r}" if depth else "measure filter without row levels")
                continue
            measure_filters.append(MeasureFilter(depth, str(f["field"]), op, _coerce(f.get("value"), "floating")))
            continue
        mapped = _dimension_filter(f, families, reasons)
        if mapped is not None:
            rows.append(mapped)
    return tuple(rows), tuple(measure_filters)


# --------------------------------------------------------------------- spec -> IR


def _measure(m: Any, reasons: List[str]) -> Optional[MeasureRef]:
    agg = _AGG.get(str(m.agg or "sum").strip().lower())
    if agg is None:
        reasons.append(f"aggregation {m.agg!r}")
        return None
    for attr, label in (("expression", "measure expression"), ("separator", "string aggregation"),
                        ("filter_condition", "conditional measure"), ("ratio_numerator", "ratio measure"),
                        ("window_func", "window measure")):
        if getattr(m, attr, None):
            reasons.append(label)
            return None
    if getattr(m, "null_handling", None) not in (None, "ignore"):
        reasons.append(f"null handling {m.null_handling!r}")
        return None
    field = None if m.field in (None, "*") else str(m.field)
    weight = getattr(m, "weighted_field", None) if agg == "weighted_avg" else None
    q = getattr(m, "percentile", None) if agg == "percentile" else None
    return MeasureRef(str(m.alias), agg, field, weight, None if q is None else float(q))


def _expanded(target_paths: Any, rows: Sequence[str], families: Mapping[str, str]) -> Optional[FrozenSet[Tuple[Any, ...]]]:
    if target_paths is True or target_paths is None:
        return None
    paths = [list(p) for p in (target_paths or []) if isinstance(p, (list, tuple))]
    if any(p == [ALL_EXPANDED] for p in paths):
        return None
    out = set()
    for p in paths:
        if not 1 <= len(p) < len(rows):
            continue  # a leaf or over-long path expands nothing
        out.add(tuple(_coerce(v, families[rows[i]]) for i, v in enumerate(p)))
    return frozenset(out)


def spec_to_query(spec: Any, target_paths: Any, schema: Mapping[str, Any], *, start_row: int = 0,
                  end_row: Optional[int] = None) -> PivotQuery:
    """Map a legacy spec to the IR; raise :class:`CoreUnsupported` with every reason."""
    reasons: List[str] = []
    families = {name: type_family(dtype) for name, dtype in schema.items()}
    for attr, label in (("columns", "column pivot"), ("custom_dimensions", "custom dimensions"),
                        ("having", "having"), ("subtotals", "explicit subtotals"),
                        ("drill_paths", "drill paths"), ("cursor", "keyset cursor"),
                        ("grouping_config", "grouping sets")):
        if getattr(spec, attr, None):
            reasons.append(label)
    if getattr(spec, "group_allow_unbalanced", False):
        reasons.append("unbalanced hierarchy")
    axis = getattr(spec, "measure_axis", None)
    if axis is not None and getattr(axis, "placement", "none") != "none":
        reasons.append("measure axis")
    cfg = getattr(spec, "pivot_config", None)
    if cfg is not None and (cfg.top_n or cfg.order_by_measure or cfg.column_cursor or cfg.materialized_column_values):
        reasons.append("column pivot options")
    if getattr(spec, "offset", 0):
        reasons.append("spec offset")

    rows = [str(r) for r in spec.rows or []]
    measures = [m for m in (_measure(m, reasons) for m in spec.measures or []) if m is not None]
    aliases = frozenset(m.alias for m in measures)
    unknown = [r for r in rows if r not in families]
    if unknown:
        reasons.append(f"unknown row fields {unknown}")

    sort: List[SortKey] = []
    raw_sort = spec.sort if isinstance(spec.sort, list) else ([spec.sort] if spec.sort else [])
    for s in raw_sort:
        if s.get("sortKeyField"):
            reasons.append("custom member order")
        elif s.get("field") in rows or s.get("field") in aliases:
            sort.append(SortKey(str(s["field"]), str(s.get("order", "asc")).lower() == "desc"))
        else:
            reasons.append(f"sort on {s.get('field')!r}")

    filters, measure_filters = _filters(spec, aliases, families, len(rows), reasons)
    if reasons:
        raise CoreUnsupported(reasons)

    offset = max(0, int(start_row)) + 1  # the core orders the grand total first; legacy rows exclude it
    limit = None if end_row is None else max(0, int(end_row) - int(start_row) + 1)
    return PivotQuery(
        dataset=str(spec.table),
        rows=tuple(rows),
        measures=tuple(measures),
        filters=filters,
        measure_filters=measure_filters,
        sort=tuple(sort),
        expanded=_expanded(target_paths, rows, families) if rows else None,
        window=Window(offset, limit),
    )


# --------------------------------------------------------------------- frame -> legacy rows


def _legacy_row(record: Mapping[str, Any], rows: Sequence[str]) -> Dict[str, Any]:
    path = record["path"]
    if not path:
        out: Dict[str, Any] = {f: None for f in rows[:1]}
        out.update(record["values"])
        out.update({"_id": GRAND_TOTAL_ID, "_isTotal": True, "_path": GRAND_TOTAL_DISPLAY_PATH, "depth": 0,
                    "_key": record["key"]})
        return out
    out = dict(record["dims"])
    out.update(record["values"])
    last = path[-1]
    out.update({
        "_id": "" if last is None else str(last),
        "_path": CanonicalPathCodec.display_path(list(path)),
        "_pathFields": list(rows[: len(path)]),
        "depth": len(path) - 1,
        "_key": record["key"],
    })
    return out


def core_hierarchy_view(
    resolve: Callable[[str], Any],
    spec: Any,
    target_paths: Any,
    start_row: int,
    end_row: int,
    *,
    include_grand_total_row: bool = False,
    total_cache: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """The ``run_hierarchy_view`` contract, answered by the core.

    ``rows`` are the visible nodes in tree order, excluding the grand total,
    windowed by the inclusive ``[start_row, end_row]``; ``total_rows`` counts
    them plus the grand total; ``grand_total_row`` is returned separately.
    """
    relation = resolve(spec.table)
    query = spec_to_query(spec, target_paths, relation.schema(), start_row=start_row, end_row=end_row)
    resolver = lambda _name: relation  # noqa: E731 - one resolution per request (scope checked once)
    frame = execute(query, resolver)
    rows = [_legacy_row(r, query.rows) for r in frame.records()]
    grand_total = None
    if include_grand_total_row:
        gt = execute(replace(query, window=Window(0, 1)), resolver).records()
        grand_total = _legacy_row(gt[0], query.rows) if gt else None
    total = _visible_node_count(query, relation, total_cache)
    from ..hierarchy_rows import compute_hierarchy_color_scale_stats

    return {
        "rows": rows,
        "total_rows": total,
        "grand_total_row": grand_total,
        "grand_total_formula_source_rows": [],
        "color_scale_stats": compute_hierarchy_color_scale_stats(rows, list(query.rows), []),
        "profile": {"engine": "core"},
    }


def _visible_node_count(query: PivotQuery, relation: Any, cache: Optional[Dict[str, int]]) -> int:
    """Visible nodes including the grand total (the legacy ``total_rows``)."""
    from ..core.exec import fingerprint

    unwindowed = replace(query, window=Window())
    key = fingerprint(unwindowed)
    if cache is not None and key in cache:
        return cache[key]
    plan = logical_plan(unwindowed)
    union, _ = TREES[choose(plan, relation.schema())](plan, relation)
    counted = union.count()
    verify_scoped(counted)  # every core expression is proven scoped before it runs
    total = int(counted.to_pyarrow().as_py())
    if cache is not None:
        if len(cache) > 4096:
            cache.clear()
        cache[key] = total
    return total
