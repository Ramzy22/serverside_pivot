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

import time
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from ..core.authority.verifier import verify_scoped
from ..core.exec import column_members, detail, distinct_values, execute, fingerprint
from ..core.identity import GRAND_TOTAL_DISPLAY_PATH, CanonicalPathCodec
from ..core.ir import (
    DerivedMeasure, DetailQuery, DistinctQuery, MeasureFilter, to_wire, MeasureRef, PivotQuery, Predicate, PredicateGroup, SortKey, Window,
)
from ..core.physical import TREES, choose
from ..core.logical import plan as logical_plan
from ..core.registry import type_family

__all__ = ["CoreUnsupported", "core_drill_through", "core_hierarchy_view", "core_unique_values",
           "legacy_column_key", "spec_to_query"]

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
        if any(v is None or v == "" for v in p):
            # A display path cannot tell the NULL member from "" (F-EXPAND-EMPTY): not addressable.
            raise CoreUnsupported(["expanded path with an empty or NULL member (send rowKey)"])
        out.add(tuple(_coerce(v, families[rows[i]]) for i, v in enumerate(p)))
    return frozenset(out)


# Every PivotSpec / Measure / PivotConfig field the bridge interprets, or that
# has no effect on the hierarchy view's values (display-only).  Any other field
# with a non-default value is rejected by name, so the mapping is complete by
# construction rather than silently partial.
_SPEC_FIELDS = {"table", "rows", "columns", "measures", "filters", "custom_dimensions", "sort", "offset", "cursor",
                "totals", "having", "subtotals", "grouping_config", "pivot_config", "measure_axis", "drill_paths",
                "group_allow_unbalanced",
                "limit",  # the hierarchy view is windowed by start/end rows
                "column_sort_options"}  # orders the column catalog only; cells are keyed by member
_MEASURE_FIELDS = {"field", "agg", "alias", "weighted_field", "expression", "percentile", "separator",
                   "null_handling", "filter_condition", "ratio_numerator", "ratio_denominator", "ratio_null_value",
                   "window_func", "window_group_by", "window_order_by", "window_frame_start", "window_frame_end",
                   "ratio_format"}  # display formatting only
_PIVOT_CONFIG_FIELDS = {"enabled", "top_n", "order_by_measure", "include_totals_column", "column_cursor",
                        "materialized_column_values",
                        "null_column_label"}  # display only; cell keys use the legacy key
_SORT_KEYS = {"field", "order", "sortType", "absoluteSort", "nulls", "semanticType", "semantic", "sortSemantic",
              "sortKeyField"}
_FILTER_KEYS = {"field", "op", "value", "caseSensitive", "conditions"}


def _uninterpreted(obj: Any, known: FrozenSet[str], label: str, reasons: List[str]) -> None:
    import dataclasses

    if obj is None or not dataclasses.is_dataclass(obj):
        return
    for f in dataclasses.fields(obj):
        if f.name in known:
            continue
        if f.default is not dataclasses.MISSING:
            default: Any = f.default
        elif f.default_factory is not dataclasses.MISSING:
            default = f.default_factory()
        else:
            default = object()
        if getattr(obj, f.name) != default:
            reasons.append(f"{label} field {f.name!r}")


def _unknown_keys(f: Any, reasons: List[str]) -> None:
    if not isinstance(f, Mapping):
        reasons.append(f"filter {f!r}")
        return
    extra = set(f) - _FILTER_KEYS
    if extra:
        reasons.append(f"filter keys {sorted(extra)}")
    for c in f.get("conditions") or []:
        _unknown_keys(c, reasons)


def spec_to_query(spec: Any, target_paths: Any, schema: Mapping[str, Any], *, start_row: int = 0,
                  end_row: Optional[int] = None, subtotals: str = "before") -> PivotQuery:
    """Map a legacy spec to the IR; raise :class:`CoreUnsupported` with every reason.

    The query's window addresses the legacy flattened list (nodes in tree
    order; the core orders the grand total first, hence the +1).  Column
    members are left empty: :func:`core_hierarchy_view` resolves them.
    """
    reasons: List[str] = []
    families = {name: type_family(dtype) for name, dtype in schema.items()}
    _uninterpreted(spec, frozenset(_SPEC_FIELDS), "spec", reasons)
    _uninterpreted(getattr(spec, "pivot_config", None), frozenset(_PIVOT_CONFIG_FIELDS), "pivot_config", reasons)
    for m in spec.measures or []:
        _uninterpreted(m, frozenset(_MEASURE_FIELDS), "measure", reasons)
    for f in spec.filters or []:
        _unknown_keys(f, reasons)
    for s in (spec.sort if isinstance(spec.sort, list) else ([spec.sort] if spec.sort else [])):
        extra = set(s) - _SORT_KEYS if isinstance(s, Mapping) else {"<not a mapping>"}
        if extra:
            reasons.append(f"sort keys {sorted(extra)}")
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
        sort_type = str(s.get("sortType") or "").strip().lower()
        absolute = bool(s.get("absoluteSort")) or sort_type in {"absolute", "abs", "absolute_value", "absolute-value"}
        semantic = s.get("semanticType") or s.get("semantic") or s.get("sortSemantic")
        key_field = s.get("sortKeyField")
        if key_field and key_field in families and s.get("field") in rows:
            # REQ-SORT-003: members ordered by MIN(key) (legacy: a hidden sort key per member).
            if absolute or semantic or (sort_type and sort_type not in {"alphanumeric", "basic", "default", "natural"}):
                reasons.append("custom member order with a sort type")
            elif str(s.get("nulls") or "last").lower() != "last":
                reasons.append(f"nulls {s.get('nulls')!r}")
            else:
                sort.append(SortKey(str(s["field"]), str(s.get("order", "asc")).lower() == "desc", False, str(key_field)))
        elif key_field and key_field in families:
            reasons.append("custom member order")
        elif semantic:
            reasons.append(f"semantic sort {semantic!r}")
        elif str(s.get("nulls") or "last").lower() != "last":
            reasons.append(f"nulls {s.get('nulls')!r}")
        elif sort_type and not absolute and sort_type not in {"alphanumeric", "basic", "default", "natural"}:
            reasons.append(f"sort type {sort_type!r}")
        elif s.get("field") in rows or (s.get("field") in aliases and not columns):
            sort.append(SortKey(str(s["field"]), str(s.get("order", "asc")).lower() == "desc", absolute))
        elif s.get("field") in aliases:
            reasons.append("sort by measure with column pivot")
        else:
            reasons.append(f"sort on {s.get('field')!r}")

    filters, measure_filters = _filters(spec, aliases, families, len(rows), reasons)
    if measure_filters and columns:
        reasons.append("measure filter with column pivot")
    if reasons:
        raise CoreUnsupported(reasons)

    # Pre-order puts the grand total first (+1 skips it); post-order puts it last (REQ-LAYOUT-001).
    offset = max(0, int(start_row)) + (1 if subtotals == "before" else 0)
    limit = None if end_row is None else max(0, int(end_row) - int(start_row) + 1)
    return PivotQuery(
        dataset=str(spec.table),
        rows=tuple(rows),
        measures=tuple(measures),
        filters=filters,
        measure_filters=measure_filters,
        sort=tuple(sort),
        subtotals=subtotals,
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
                    "_rowKey": "[]"})
        return out
    out = dict(record["dims"])
    out.update(values)
    last = path[-1]
    out.update({
        "_id": "" if last is None else str(last),
        "_path": CanonicalPathCodec.display_path(list(path)),
        "_pathFields": list(rows[: len(path)]),
        "depth": len(path) - 1,
    })
    row_key = _canonical_row_key(path)
    if row_key is not None:
        out["_rowKey"] = row_key
    return out


def _canonical_row_key(path: Sequence[Any]) -> Optional[str]:
    """The typed identity the frontend keys rows by (REQ-ID-001); None when a member type has no encoding."""
    try:
        return CanonicalPathCodec.encode(list(path))
    except Exception:  # noqa: BLE001 - an unencodable member only loses the canonical key, never the row
        return None


def core_hierarchy_view(
    resolve: Callable[[str], Any],
    spec: Any,
    target_paths: Any,
    start_row: int,
    end_row: int,
    *,
    include_grand_total_row: bool = False,
    total_cache: Optional[Dict[str, int]] = None,
    capture: Optional[Dict[str, Any]] = None,
    tabular_subtotals: bool = False,
) -> Dict[str, Any]:
    """The ``run_hierarchy_view`` contract, answered by the core.

    ``tabular_subtotals``: the tabular subtotal layout (REQ-LAYOUT-001), the
    post-order of the fully expanded tree; group rows carry
    ``_isTabularSubtotal`` and the grand total comes last.

    ``capture`` (shadow referee, opt-in) receives the core query and, for
    datasets of at most ``CAPTURE_MAX_ROWS`` records, the scoped data.

    The flattened list is the visible nodes in tree order followed, when
    ``spec.totals``, by the grand total.  ``rows`` is its inclusive
    ``[start_row, end_row]`` window and ``total_rows`` its length;
    ``grand_total_row`` is returned only when requested and enabled.
    """
    started = time.perf_counter()
    relation = resolve(spec.table)
    if tabular_subtotals:
        target_paths = [["__ALL__"]]  # every group is open: collapse state does not apply to a flat layout
    query = spec_to_query(spec, target_paths, relation.schema(), start_row=start_row, end_row=end_row,
                          subtotals="after" if tabular_subtotals else "before")
    resolver = lambda _name: relation  # noqa: E731 - one resolution per request (scope checked once)
    query, keys = _resolve_members(query, spec, resolver)
    if capture is not None:
        _capture(capture, query, relation)
    totals = bool(getattr(spec, "totals", False))
    totals_column = bool(getattr(getattr(spec, "pivot_config", None), "include_totals_column", False))

    def legacy(record: Mapping[str, Any]) -> Dict[str, Any]:
        return _legacy_row(record, query, keys, totals_column)

    nodes = _visible_node_count(query, relation, total_cache) - 1
    if tabular_subtotals:
        return _post_order_view(query, resolver, legacy, nodes, int(start_row), int(end_row), totals,
                                include_grand_total_row, started)
    grand_total = None
    if totals and int(start_row) == 0:  # the core orders the grand total first: one query serves both
        limit = None if query.window.limit is None else query.window.limit + 1
        records = execute(replace(query, window=Window(0, limit)), resolver).records()
        grand_total, records = legacy(records[0]), records[1:]
    else:
        records = execute(query, resolver).records()
    rows = [legacy(r) for r in records]
    if totals and grand_total is None and (include_grand_total_row or int(start_row) <= nodes <= int(end_row)):
        grand_total = legacy(execute(replace(query, window=Window(0, 1)), resolver).records()[0])
    if grand_total is not None and int(start_row) <= nodes <= int(end_row):
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
        # Same profile layer as the legacy controller, so profile consumers keep working.
        "profile": {"controller": {"operation": "hierarchy_view", "engine": "core",
                                   "totalMs": round((time.perf_counter() - started) * 1000.0, 3)}},
    }


def _post_order_view(query: PivotQuery, resolver: Callable[[str], Any], legacy: Callable[[Mapping[str, Any]], Dict[str, Any]],
                     nodes: int, start_row: int, end_row: int, totals: bool, include_grand_total_row: bool,
                     started: float) -> Dict[str, Any]:
    """Tabular subtotals: the core's post-order is the legacy list (grand total last, at index ``nodes``)."""
    depth = len(query.rows)
    grand_total = None
    rows: List[Dict[str, Any]] = []
    for record in execute(query, resolver).records():
        row = legacy(record)
        if not record["path"]:
            grand_total = row
            continue
        if len(record["path"]) < depth:
            row["_isTabularSubtotal"] = True
        rows.append(row)
    if totals and grand_total is None and include_grand_total_row:
        grand_total = legacy(execute(replace(query, window=Window(nodes, 1)), resolver).records()[0])
    if totals and grand_total is not None and start_row <= nodes <= end_row:
        rows.append(dict(grand_total))
    formula_sources: List[Dict[str, Any]] = []
    if totals and include_grand_total_row and query.rows:  # the top-level groups, as in the pre-order view
        top = replace(query, subtotals="before", expanded=frozenset(), window=Window(1, None))
        formula_sources = [legacy(r) for r in execute(top, resolver).records()]
    from ..hierarchy_rows import compute_hierarchy_color_scale_stats

    return {
        "rows": rows,
        "total_rows": nodes + (1 if totals else 0),
        "grand_total_row": grand_total if include_grand_total_row else None,
        "grand_total_formula_source_rows": formula_sources if include_grand_total_row else None,
        "color_scale_stats": compute_hierarchy_color_scale_stats(rows, list(query.rows), list(query.columns)),
        "profile": {"controller": {"operation": "hierarchy_view", "engine": "core", "layout": "tabular_subtotals",
                                   "totalMs": round((time.perf_counter() - started) * 1000.0, 3)}},
    }


CAPTURE_MAX_ROWS = 2000


def _capture(capture: Dict[str, Any], query: PivotQuery, relation: Any) -> None:
    import base64

    import pyarrow as pa

    capture["query"] = to_wire(query)
    counted = relation.count()
    verify_scoped(counted)
    if int(counted.to_pyarrow().as_py()) > CAPTURE_MAX_ROWS:
        return
    verify_scoped(relation)
    table = relation.to_pyarrow()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    capture["data"] = base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")


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


# --------------------------------------------------------------------- drill-through


def _node_path(payload: Mapping[str, Any], fields: Sequence[str], families: Mapping[str, str]) -> Tuple[Any, ...]:
    """The typed node path: the canonical ``rowKey`` when sent, else the display path."""
    key = payload.get("rowKey") or payload.get("row_key")
    if key:
        path = CanonicalPathCodec.decode(str(key))
        if len(path) != len(fields):
            raise CoreUnsupported(["row key does not match the row fields"])
        return tuple(path)
    display = payload.get("row_path") or payload.get("rowPath") or ""
    parts = str(display).split("|||") if display else []
    if len(parts) != len(fields):
        raise CoreUnsupported(["display path does not match the row fields"])
    if any(part == "" for part in parts):
        raise CoreUnsupported(["display path with an empty or NULL member (send rowKey)"])
    return tuple(_coerce(part, families[f]) for part, f in zip(parts, fields, strict=True))


def core_drill_through(resolve: Callable[[str], Any], spec: Any, extra_filters: Sequence[Mapping[str, Any]],
                       payload: Mapping[str, Any], *, limit: int, offset: int, sort_col: Optional[str],
                       sort_dir: str, text_filter: str) -> Dict[str, Any]:
    """The ``get_drill_through_data`` contract, answered by the core (REQ-DET-001)."""
    relation = resolve(spec.table)
    families = {name: type_family(dtype) for name, dtype in relation.schema().items()}
    reasons: List[str] = []
    if getattr(spec, "custom_dimensions", None):
        reasons.append("custom dimensions")
    fields = payload.get("row_fields") or payload.get("pathFields") or payload.get("rowFields") or []
    fields = [str(f) for f in fields] if isinstance(fields, list) else []
    unknown = [f for f in fields if f not in families]
    if unknown:
        reasons.append(f"unknown row fields {unknown}")
    aliases = frozenset(str(m.alias) for m in spec.measures or [])
    filters: List[Any] = []
    for f in [*(spec.filters or []), *extra_filters]:
        if set(_flatten_fields(f)) & aliases:
            reasons.append("measure filter in drill-through")
            continue
        mapped = _dimension_filter(f, families, reasons)
        if mapped is not None:
            filters.append(mapped)
    if sort_col and sort_col not in families:
        reasons.append(f"sort on {sort_col!r}")
    if reasons:
        raise CoreUnsupported(reasons)
    path = _node_path(payload, fields, families)
    query = DetailQuery(
        dataset=str(spec.table),
        filters=tuple(filters),
        path_fields=tuple(fields),
        path=path,
        search=text_filter or None,
        sort=(SortKey(sort_col, sort_dir == "desc"),) if sort_col else (),
        window=Window(max(0, int(offset)), max(0, int(limit))),
    )
    table, total = detail(query, lambda _name: relation)
    return {"rows": table.to_pylist(), "total_rows": total}


# --------------------------------------------------------------------- filter options


def core_unique_values(resolve: Callable[[str], Any], spec: Any, column_id: str, *, search: str, limit: int,
                       offset: int) -> Tuple[List[Any], int]:
    """The ``get_unique_values`` contract, answered by the core (REQ-DET-002)."""
    relation = resolve(spec.table)
    families = {name: type_family(dtype) for name, dtype in relation.schema().items()}
    reasons: List[str] = []
    if getattr(spec, "custom_dimensions", None):
        reasons.append("custom dimensions")
    axis = getattr(spec, "measure_axis", None)
    if axis is not None and getattr(axis, "placement", "none") != "none":
        reasons.append("measure axis")
    if column_id not in families:
        reasons.append(f"unknown column {column_id!r}")
    aliases = frozenset(str(m.alias) for m in spec.measures or [])
    filters: List[Any] = []
    for f in spec.filters or []:
        _unknown_keys(f, reasons)
        if set(_flatten_fields(f)) & aliases:
            reasons.append("measure filter in filter options")
            continue
        mapped = _dimension_filter(f, families, reasons)
        if mapped is not None:
            filters.append(mapped)
    if reasons:
        raise CoreUnsupported(reasons)
    query = DistinctQuery(str(spec.table), column_id, tuple(filters), str(search or "").strip() or None,
                          Window(max(0, int(offset)), max(0, int(limit))))
    return distinct_values(query, lambda _name: relation)
