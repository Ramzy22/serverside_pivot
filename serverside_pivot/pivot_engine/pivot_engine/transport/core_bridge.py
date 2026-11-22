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
from ..core.exec import column_members, execute, fingerprint
from ..core.identity import GRAND_TOTAL_DISPLAY_PATH, CanonicalPathCodec
from ..core.ir import (
    DerivedMeasure, MeasureFilter, MeasureRef, PivotQuery, Predicate, PredicateGroup, SortKey, Window,
)
from ..core.physical import TREES, choose
from ..core.logical import plan as logical_plan
from ..core.registry import type_family

__all__ = ["CoreUnsupported", "core_hierarchy_view", "legacy_column_key", "spec_to_query"]

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


HIDDEN = "__base_"
_WINDOWS = {"rank": "rank", "dense_rank": "dense_rank", "cumulative": "cumulative", "running_avg": "moving_avg",
            "moving_avg": "moving_avg", "percent_of_total": "percent_of_total",
            "percent_of_grand_total": "percent_of_total"}


def _measures(spec: Any, reasons: List[str]) -> Tuple[List[MeasureRef], List[DerivedMeasure]]:
    """Base measures plus derived ones (ratios, window measures over a hidden base measure)."""
    base: List[MeasureRef] = []
    derived: List[DerivedMeasure] = []
    ratios = []
    for m in spec.measures or []:
        if getattr(m, "ratio_numerator", None):
            ratios.append(m)
            continue
        func = getattr(m, "window_func", None)
        if func:
            name = _WINDOWS.get(str(func).strip().lower())
            if name is None:
                reasons.append(f"window function {func!r}")
                continue
            if getattr(m, "window_group_by", None) is not None or getattr(m, "window_order_by", None):
                reasons.append("window partition or order override")
                continue
            ref = _measure(m, reasons, alias=HIDDEN + str(m.alias))
            if ref is None:
                continue
            base.append(ref)
            frame = None
            if str(func).strip().lower() == "moving_avg" and (
                    m.window_frame_start is not None or m.window_frame_end is not None):
                frame = (m.window_frame_start, 0 if m.window_frame_end is None else m.window_frame_end)
            derived.append(DerivedMeasure(str(m.alias), name, ref.alias, frame=frame))
            continue
        ref = _measure(m, reasons)
        if ref is not None:
            base.append(ref)
    plain = {b.alias for b in base}
    for m in ratios:
        num, den = str(m.ratio_numerator), str(m.ratio_denominator or "")
        if num not in plain or den not in plain:
            reasons.append("ratio of a derived or unknown measure")
            continue
        derived.append(DerivedMeasure(str(m.alias), "ratio", num, den,
                                      null_value=None if m.ratio_null_value is None else float(m.ratio_null_value)))
    return base, derived


def _measure(m: Any, reasons: List[str], *, alias: Optional[str] = None) -> Optional[MeasureRef]:
    agg = _AGG.get(str(m.agg or "sum").strip().lower())
    if agg is None:
        reasons.append(f"aggregation {m.agg!r}")
        return None
    for attr, label in (("expression", "measure expression"), ("separator", "string aggregation"),
                        ("filter_condition", "conditional measure")):
        if getattr(m, attr, None):
            reasons.append(label)
            return None
    if getattr(m, "null_handling", None) not in (None, "ignore"):
        reasons.append(f"null handling {m.null_handling!r}")
        return None
    field = None if m.field in (None, "*") else str(m.field)
    weight = getattr(m, "weighted_field", None) if agg == "weighted_avg" else None
    q = getattr(m, "percentile", None) if agg == "percentile" else None
    return MeasureRef(alias or str(m.alias), agg, field, weight, None if q is None else float(q))


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
    """Map a legacy spec to the IR; raise :class:`CoreUnsupported` with every reason.

    The query's window addresses the legacy flattened list (nodes in tree
    order; the core orders the grand total first, hence the +1).  Column
    members are left empty: :func:`core_hierarchy_view` resolves them.
    """
    reasons: List[str] = []
    families = {name: type_family(dtype) for name, dtype in schema.items()}
    for attr, label in (("custom_dimensions", "custom dimensions"),
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
    columns = [str(c) for c in getattr(spec, "columns", None) or []]
    cfg = getattr(spec, "pivot_config", None)
    if columns:
        if cfg is None or not cfg.enabled:
            reasons.append("column pivot without pivot_config")
        elif cfg.materialized_column_values is None and (cfg.top_n or cfg.order_by_measure or cfg.column_cursor):
            reasons.append("column pivot options")
        unknown_columns = [c for c in columns if c not in families]
        if unknown_columns:
            reasons.append(f"unknown column fields {unknown_columns}")
    if getattr(spec, "offset", 0):
        reasons.append("spec offset")

    rows = [str(r) for r in spec.rows or []]
    measures, derived = _measures(spec, reasons)
    aliases = frozenset(m.alias for m in measures if not m.alias.startswith(HIDDEN))
    if derived and columns:
        reasons.append("derived measure with column pivot")
    unknown = [r for r in rows if r not in families]
    if unknown:
        reasons.append(f"unknown row fields {unknown}")

    sort: List[SortKey] = []
    raw_sort = spec.sort if isinstance(spec.sort, list) else ([spec.sort] if spec.sort else [])
    for s in raw_sort:
        if s.get("sortKeyField"):
            reasons.append("custom member order")
        elif s.get("field") in rows or (s.get("field") in aliases and not columns):
            sort.append(SortKey(str(s["field"]), str(s.get("order", "asc")).lower() == "desc"))
        elif s.get("field") in aliases:
            reasons.append("sort by measure with column pivot")
        else:
            reasons.append(f"sort on {s.get('field')!r}")

    filters, measure_filters = _filters(spec, aliases, families, len(rows), reasons)
    if measure_filters and columns:
        reasons.append("measure filter with column pivot")
    if reasons:
        raise CoreUnsupported(reasons)

    offset = max(0, int(start_row)) + 1
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
        columns=tuple(columns),
        derived=tuple(derived),
    )


# --------------------------------------------------------------------- columns


def legacy_column_key(member: Sequence[Any]) -> str:
    """The legacy display key of a column member (``str`` of each value joined by ``|``)."""
    return str(member[0]) if len(member) == 1 else "|".join(str(v) for v in member)


def _resolve_members(query: PivotQuery, spec: Any, resolver: Callable[[str], Any]) -> Tuple[PivotQuery, List[str]]:
    """Bind the column window: every member, or the legacy-selected keys, in their order."""
    if not query.columns:
        return query, []
    members = column_members(query, resolver)
    by_key: Dict[str, Tuple[Any, ...]] = {}
    for member in members:
        key = legacy_column_key(member)
        if key in by_key:
            raise CoreUnsupported([f"ambiguous pivot column key {key!r}"])  # F-PIVOT-DELIM
        by_key[key] = member
    wanted = getattr(spec.pivot_config, "materialized_column_values", None)
    keys = list(by_key) if wanted is None else [str(k) for k in wanted if str(k) in by_key]
    return replace(query, column_members=tuple(by_key[k] for k in keys)), keys


# --------------------------------------------------------------------- frame -> legacy rows


def _legacy_values(record: Mapping[str, Any], query: PivotQuery, keys: Sequence[str],
                   totals_column: bool) -> Dict[str, Any]:
    values = record["values"]
    if not query.columns:
        return {k: v for k, v in values.items() if not k.startswith(HIDDEN)}
    out: Dict[str, Any] = {}
    for j, _member, m, name in query.cells():
        out[f"{keys[j]}_{m.alias}"] = values[name]
    if totals_column:
        for m in query.measures:
            out[f"__RowTotal__{m.alias}"] = values[m.alias]
    return out


def _legacy_row(record: Mapping[str, Any], query: PivotQuery, keys: Sequence[str] = (),
                totals_column: bool = False) -> Dict[str, Any]:
    rows = query.rows
    path = record["path"]
    values = _legacy_values(record, query, keys, totals_column)
    if not path:
        out: Dict[str, Any] = {f: None for f in rows[:1]}
        out.update(values)
        out.update({"_id": GRAND_TOTAL_ID, "_isTotal": True, "_path": GRAND_TOTAL_DISPLAY_PATH, "depth": 0,
                    "_key": record["key"]})
        return out
    out = dict(record["dims"])
    out.update(values)
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

    The flattened list is the visible nodes in tree order followed, when
    ``spec.totals``, by the grand total.  ``rows`` is its inclusive
    ``[start_row, end_row]`` window and ``total_rows`` its length;
    ``grand_total_row`` is returned only when requested and enabled.
    """
    relation = resolve(spec.table)
    query = spec_to_query(spec, target_paths, relation.schema(), start_row=start_row, end_row=end_row)
    resolver = lambda _name: relation  # noqa: E731 - one resolution per request (scope checked once)
    query, keys = _resolve_members(query, spec, resolver)
    totals = bool(getattr(spec, "totals", False))
    totals_column = bool(getattr(getattr(spec, "pivot_config", None), "include_totals_column", False))

    def legacy(record: Mapping[str, Any]) -> Dict[str, Any]:
        return _legacy_row(record, query, keys, totals_column)

    nodes = _visible_node_count(query, relation, total_cache) - 1
    rows = [legacy(r) for r in execute(query, resolver).records()]
    grand_total = None
    if totals and (include_grand_total_row or int(start_row) <= nodes <= int(end_row)):
        gt = execute(replace(query, window=Window(0, 1)), resolver).records()
        grand_total = legacy(gt[0])
        if int(start_row) <= nodes <= int(end_row):
            rows.append(dict(grand_total))
    formula_sources: List[Dict[str, Any]] = []
    if totals and include_grand_total_row and query.rows:
        top = execute(replace(query, expanded=frozenset(), window=Window(1, None)), resolver).records()
        formula_sources = [legacy(r) for r in top]
    from ..hierarchy_rows import compute_hierarchy_color_scale_stats

    return {
        "rows": rows,
        "total_rows": nodes + (1 if totals else 0),
        "grand_total_row": grand_total if include_grand_total_row else None,
        "grand_total_formula_source_rows": formula_sources if include_grand_total_row else None,
        "color_scale_stats": compute_hierarchy_color_scale_stats(rows, list(query.rows), list(query.columns)),
        "profile": {"engine": "core"},
    }


def _visible_node_count(query: PivotQuery, relation: Any, cache: Optional[Dict[str, int]]) -> int:
    """Rows of the core tree including its grand total."""
    unwindowed = replace(query, window=Window(), column_members=(), derived=())  # cells/derived never change it
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
