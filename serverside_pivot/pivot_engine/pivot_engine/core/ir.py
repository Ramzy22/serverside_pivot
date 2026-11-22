"""Semantic IR: what a pivot query *means*, independent of any engine.

Pure data, no Ibis.  Validated against a dataset schema (column -> type
family) before planning; every invalid input is a typed error, never a silent
fallback (REQ-AGG-001, F-SILENT).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple, Union

from .registry import REGISTRY, UnknownAggregationError

__all__ = [
    "IRValidationError",
    "MeasureFilter",
    "MeasureRef",
    "PivotQuery",
    "Predicate",
    "PredicateGroup",
    "SortKey",
    "Window",
    "PREDICATE_OPS",
    "from_wire",
]

STRING_OPS = frozenset({"contains", "not_contains", "starts_with", "ends_with"})
PREDICATE_OPS = frozenset(
    {"=", "!=", ">", ">=", "<", "<=", "in", "not_in", "is_null", "is_not_null", "between"} | STRING_OPS
)
GROUP_OPS = frozenset({"and", "or"})
# Operators where ``case_sensitive=False`` folds case (string columns only).
# Membership stays exact: case variants are distinct groups (REQ-NULL-002, F-CASE).
CASE_FOLDING_OPS = frozenset({"=", "!="}) | STRING_OPS
_COMPARISON_OPS = frozenset({"=", "!=", ">", ">=", "<", "<="})


class IRValidationError(ValueError):
    """The query is not meaningful for this dataset."""


@dataclass(frozen=True)
class MeasureRef:
    alias: str
    agg: str
    field: Optional[str]  # None only for count(*)
    weight: Optional[str] = None  # weighted_avg
    q: Optional[float] = None  # percentile, in [0, 1]


@dataclass(frozen=True)
class Predicate:
    """Row predicate evaluated before aggregation (part of E0).

    NULL rules (REQ-NULL-003): membership (``in`` / ``not_in``) is set
    membership, so a ``None`` in the list selects the NULL group and
    ``not_in`` keeps NULL rows unless ``None`` is listed; comparisons,
    ``between`` and string operators never match NULL.
    """

    field: str
    op: str
    value: Any = None
    case_sensitive: bool = True  # honoured by CASE_FOLDING_OPS on string columns


@dataclass(frozen=True)
class PredicateGroup:
    """``and`` / ``or`` over predicates or nested groups."""

    op: str
    items: Tuple[Filter, ...]


Filter = Union[Predicate, PredicateGroup]


@dataclass(frozen=True)
class MeasureFilter:
    """Group predicate at hierarchy ``level`` (1-based), evaluated on E0 (REQ-TOT-001)."""

    level: int
    measure: str
    op: str
    value: Any


@dataclass(frozen=True)
class SortKey:
    """One entry of the sibling order (REQ-SORT-001).

    ``field`` is a row dimension (orders its own level by value) or a measure
    alias (orders siblings at every level by the node's value).  NULLS LAST in
    both directions (REQ-BACK-001).  ``absolute`` orders a numeric value by its
    magnitude (REQ-SORT-002).
    """

    field: str
    desc: bool = False
    absolute: bool = False


@dataclass(frozen=True)
class Window:
    offset: int = 0
    limit: Optional[int] = None


DERIVED_FUNCS = frozenset({"ratio", "percent_of_total", "rank", "dense_rank", "cumulative", "moving_avg"})


@dataclass(frozen=True)
class DerivedMeasure:
    """A measure computed from base measures over the visible tree (REQ-AGG-005, REQ-AGG-007).

    * ``ratio``: ``of / of2``; a NULL or zero denominator gives ``null_value``.
    * ``percent_of_total``: ``of`` divided by the grand total's ``of``.
    * ``rank`` / ``dense_rank``: 1-based among siblings (same parent node) by
      ``of`` (largest first unless ``desc=False``); ties share a rank; a NULL
      ``of`` has no rank.
    * ``cumulative`` / ``moving_avg``: sum / average of ``of`` over siblings in
      display order, within the rows frame ``frame`` relative to the node
      (``None`` = unbounded; default from the first sibling to the node).
    """

    alias: str
    func: str
    of: str
    of2: Optional[str] = None
    desc: bool = True
    frame: Optional[Tuple[Optional[int], Optional[int]]] = None
    null_value: Optional[float] = None


@dataclass(frozen=True)
class PivotQuery:
    dataset: str
    rows: Tuple[str, ...]
    measures: Tuple[MeasureRef, ...]
    filters: Tuple[Filter, ...] = ()  # combined with AND
    measure_filters: Tuple[MeasureFilter, ...] = ()
    sort: Tuple[SortKey, ...] = ()
    totals: str = "visible"  # REQ-TOT-003 / REQ-TOT-004
    expanded: Optional[FrozenSet[Tuple[Any, ...]]] = None  # None = everything expanded
    window: Window = field(default_factory=Window)
    # Column pivot (REQ-PIV-001): the column dimensions and the explicit,
    # ordered column window of typed members (see exec.column_members).
    columns: Tuple[str, ...] = ()
    column_members: Tuple[Tuple[Any, ...], ...] = ()
    derived: Tuple[DerivedMeasure, ...] = ()

    def cells(self) -> List[Tuple[int, Tuple[Any, ...], MeasureRef, str]]:
        """(member index, member, measure, output column) for every pivot cell."""
        return [(j, member, m, cell_name(j, m.alias))
                for j, member in enumerate(self.column_members) for m in self.measures]

    def aggregate_columns(self) -> List[str]:
        """Columns produced by aggregation: one per measure (the row total), then one per cell."""
        return [*(m.alias for m in self.measures), *(c[3] for c in self.cells())]

    def value_columns(self) -> List[str]:
        """Output value columns: measures, derived measures, then cells."""
        return [*(m.alias for m in self.measures), *(d.alias for d in self.derived), *(c[3] for c in self.cells())]

    def validate(self, schema: Mapping[str, str]) -> PivotQuery:
        """``schema`` maps column -> type family ("string", "integer", "floating", "decimal", ...)."""
        columns = set(schema)

        def need(name: Optional[str], what: str) -> None:
            if name is None or name not in columns:
                raise IRValidationError(f"{what} references unknown column {name!r}.")

        if not self.dataset:
            raise IRValidationError("dataset is required.")
        if len(set(self.rows)) != len(self.rows):
            raise IRValidationError("row dimensions must be unique.")
        for name in self.rows:
            need(name, "row dimension")
        if not self.measures:
            raise IRValidationError("at least one measure is required.")
        aliases = [m.alias for m in self.measures]
        if len(set(aliases)) != len(aliases):
            raise IRValidationError("measure aliases must be unique.")
        overlap = set(aliases) & set(self.rows)
        if overlap:
            raise IRValidationError(f"measure alias collides with a dimension: {sorted(overlap)}")
        for m in self.measures:
            try:
                entry = REGISTRY.get(m.agg)
            except UnknownAggregationError as exc:
                raise IRValidationError(str(exc)) from None
            if m.field is None:
                if "star" not in entry.inputs:
                    raise IRValidationError(f"{m.agg} requires a field.")
            else:
                need(m.field, f"measure {m.alias!r}")
                if not entry.accepts(schema[m.field]):
                    raise IRValidationError(f"{m.agg} does not accept {schema[m.field]} column {m.field!r}.")
            if ("q" in entry.params) != (m.q is not None):
                raise IRValidationError(f"{m.agg} {'requires' if 'q' in entry.params else 'does not take'} q.")
            if m.q is not None and not (isinstance(m.q, (int, float)) and 0 <= m.q <= 1):
                raise IRValidationError(f"percentile q must be within [0, 1], not {m.q!r}.")
            if ("weight" in entry.params) != (m.weight is not None):
                raise IRValidationError(
                    f"{m.agg} {'requires' if 'weight' in entry.params else 'does not take'} a weight field.")
            if m.weight is not None:
                need(m.weight, f"weight of measure {m.alias!r}")
                if schema[m.weight] not in {"integer", "floating", "decimal"}:
                    raise IRValidationError(f"weight column {m.weight!r} must be numeric.")
        def check(f: Any) -> None:
            if isinstance(f, PredicateGroup):
                if f.op not in GROUP_OPS:
                    raise IRValidationError(f"unsupported filter group operator {f.op!r}.")
                if not f.items:
                    raise IRValidationError("a filter group needs at least one condition.")
                for item in f.items:
                    check(item)
                return
            if not isinstance(f, Predicate):
                raise IRValidationError(f"filter {f!r} is not a predicate or group.")
            need(f.field, "filter")
            if f.op not in PREDICATE_OPS:
                raise IRValidationError(f"unsupported filter operator {f.op!r}.")
            if f.op in {"in", "not_in"} and not isinstance(f.value, (list, tuple, frozenset, set)):
                raise IRValidationError(f"{f.op} requires a list value.")
            if f.op == "between" and not (isinstance(f.value, (list, tuple)) and len(f.value) == 2):
                raise IRValidationError("between requires [low, high].")
            if f.op in STRING_OPS:
                if schema[f.field] != "string":
                    raise IRValidationError(f"{f.op} requires a string column, not {schema[f.field]} {f.field!r}.")
                if not isinstance(f.value, str):
                    raise IRValidationError(f"{f.op} requires a string value.")

        for flt in self.filters:
            check(flt)
        for mf in self.measure_filters:
            if mf.measure not in aliases:
                raise IRValidationError(f"measure filter references unknown measure {mf.measure!r}.")
            if not 1 <= mf.level <= len(self.rows):
                raise IRValidationError(f"measure filter level {mf.level} outside 1..{len(self.rows)}.")
            if mf.op not in _COMPARISON_OPS:
                raise IRValidationError(f"unsupported measure filter operator {mf.op!r}.")
        if len({s.field for s in self.sort}) != len(self.sort):
            raise IRValidationError("each sort field may appear once.")
        for s in self.sort:
            if s.field not in self.rows and s.field not in aliases:
                raise IRValidationError(f"sort key {s.field!r} is neither a row dimension nor a measure.")
            if s.absolute and not _numeric_sort_target(s.field, self.rows, self.measures, schema):
                raise IRValidationError(f"absolute sort needs a numeric value, not {s.field!r}.")
        if self.totals not in {"visible", "all_data"}:
            raise IRValidationError(f"totals must be 'visible' or 'all_data', not {self.totals!r}.")
        if self.window.offset < 0 or (self.window.limit is not None and self.window.limit < 0):
            raise IRValidationError("window offset/limit must be non-negative.")
        if self.expanded is not None:
            for path in self.expanded:
                if not isinstance(path, tuple) or not 1 <= len(path) < max(1, len(self.rows)):
                    raise IRValidationError(f"expanded path {path!r} is not a parent path.")
        if len(set(self.columns)) != len(self.columns):
            raise IRValidationError("column dimensions must be unique.")
        for name in self.columns:
            need(name, "column dimension")
        both = set(self.columns) & set(self.rows)
        if both:
            raise IRValidationError(f"a dimension cannot be both a row and a column: {sorted(both)}")
        if self.column_members and not self.columns:
            raise IRValidationError("column members require column dimensions.")
        for member in self.column_members:
            if not isinstance(member, tuple) or len(member) != len(self.columns):
                raise IRValidationError(f"column member {member!r} does not match the columns {list(self.columns)}.")
        if len(set(self.column_members)) != len(self.column_members):
            raise IRValidationError("column members must be unique.")
        names = set(aliases) | set(self.rows) | set(self.columns)
        for d in self.derived:
            if d.alias in names:
                raise IRValidationError(f"derived measure alias {d.alias!r} collides with another name.")
            names.add(d.alias)
            if d.func not in DERIVED_FUNCS:
                raise IRValidationError(f"unknown derived measure function {d.func!r}.")
            if d.of not in aliases:
                raise IRValidationError(f"derived measure {d.alias!r} references unknown measure {d.of!r}.")
            if (d.func == "ratio") != (d.of2 is not None):
                verb = "requires" if d.func == "ratio" else "does not take"
                raise IRValidationError(f"{d.func} {verb} a denominator.")
            if d.of2 is not None and d.of2 not in aliases:
                raise IRValidationError(f"derived measure {d.alias!r} references unknown measure {d.of2!r}.")
            if d.null_value is not None and (d.func != "ratio" or not isinstance(d.null_value, (int, float))):
                raise IRValidationError("null_value applies to a ratio and must be a number.")
            if d.frame is not None:
                if d.func not in {"cumulative", "moving_avg"}:
                    raise IRValidationError(f"{d.func} does not take a frame.")
                if not (isinstance(d.frame, tuple) and len(d.frame) == 2
                        and all(b is None or isinstance(b, int) for b in d.frame)):
                    raise IRValidationError(f"frame {d.frame!r} must be (start, end) row offsets.")
                start, end = d.frame
                if start is not None and end is not None and start > end:
                    raise IRValidationError(f"frame start {start} is after its end {end}.")
        return self


def _numeric_sort_target(name: str, rows: Tuple[str, ...], measures: Tuple[MeasureRef, ...],
                         schema: Mapping[str, str]) -> bool:
    numeric = {"integer", "floating", "decimal"}
    if name in rows or name not in {m.alias for m in measures}:
        return schema.get(name) in numeric
    m = next(m for m in measures if m.alias == name)
    if m.agg in {"min", "max"}:
        return m.field is not None and schema.get(m.field) in numeric
    return True  # every other aggregation yields a number


def cell_name(member_index: int, alias: str) -> str:
    """Output column of one pivot cell; display names are an edge concern."""
    return f"__c{member_index}__{alias}"


def from_wire(obj: Mapping[str, Any], *, dataset: Optional[str] = None) -> PivotQuery:
    """Build a query from its wire form (``core/spec/semantic-ir.schema.json``).

    Structural errors are :class:`IRValidationError`; semantic validation
    against the dataset schema happens in :meth:`PivotQuery.validate`.
    """
    try:
        expanded = obj.get("expanded")
        window = obj.get("window") or {}
        return PivotQuery(
            dataset=str(dataset or obj["dataset"]),
            rows=tuple(obj["rows"]),
            measures=tuple(
                MeasureRef(m["alias"], m["agg"], m.get("field"), m.get("weight"), m.get("q")) for m in obj["measures"]
            ),
            filters=tuple(_filter_from_wire(f) for f in obj.get("filters", ())),
            measure_filters=tuple(
                MeasureFilter(int(f["level"]), f["measure"], f["op"], f["value"]) for f in obj.get("measureFilters", ())
            ),
            sort=tuple(SortKey(s["field"], bool(s.get("desc", False)), bool(s.get("absolute", False)))
                       for s in obj.get("sort", ())),
            totals=obj.get("totals", "visible"),
            expanded=None if expanded is None else frozenset(tuple(p) for p in expanded),
            window=Window(int(window.get("offset", 0)), window.get("limit")),
            columns=tuple(obj.get("columns", ())),
            column_members=tuple(tuple(m) for m in obj.get("columnMembers", ())),
            derived=tuple(
                DerivedMeasure(d["alias"], d["func"], d["of"], d.get("of2"), bool(d.get("desc", True)),
                               None if d.get("frame") is None else (d["frame"][0], d["frame"][1]),
                               d.get("nullValue"))
                for d in obj.get("derived", ())
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IRValidationError(f"malformed query: {exc!r}") from None


def to_wire(query: PivotQuery) -> Dict[str, Any]:
    """The wire form of a query (``core/spec/semantic-ir.schema.json``); ``from_wire`` inverts it."""

    def thaw(value: Any) -> Any:
        return list(value) if isinstance(value, (tuple, list, set, frozenset)) else value

    def filt(f: Filter) -> Dict[str, Any]:
        if isinstance(f, PredicateGroup):
            return {"op": f.op, "conditions": [filt(i) for i in f.items]}
        return {"field": f.field, "op": f.op, "value": thaw(f.value), "caseSensitive": f.case_sensitive}

    out: Dict[str, Any] = {
        "dataset": query.dataset,
        "rows": list(query.rows),
        "measures": [{"alias": m.alias, "agg": m.agg, "field": m.field, "weight": m.weight, "q": m.q}
                     for m in query.measures],
        "filters": [filt(f) for f in query.filters],
        "measureFilters": [{"level": f.level, "measure": f.measure, "op": f.op, "value": f.value}
                           for f in query.measure_filters],
        "sort": [{"field": k.field, "desc": k.desc, "absolute": k.absolute} for k in query.sort],
        "totals": query.totals,
        "window": {"offset": query.window.offset, "limit": query.window.limit},
        "columns": list(query.columns),
        "columnMembers": [list(m) for m in query.column_members],
        "derived": [{"alias": d.alias, "func": d.func, "of": d.of, "of2": d.of2, "desc": d.desc,
                     "frame": None if d.frame is None else list(d.frame), "nullValue": d.null_value}
                    for d in query.derived],
    }
    if query.expanded is not None:
        out["expanded"] = sorted((list(p) for p in query.expanded), key=repr)
    return out


def _freeze(value: Any) -> Any:
    return tuple(value) if isinstance(value, list) else value


def _filter_from_wire(obj: Mapping[str, Any]) -> Filter:
    if "conditions" in obj:
        return PredicateGroup(str(obj["op"]).lower(), tuple(_filter_from_wire(c) for c in obj["conditions"]))
    return Predicate(obj["field"], obj["op"], _freeze(obj.get("value")), bool(obj.get("caseSensitive", True)))


@dataclass(frozen=True)
class DetailQuery:
    """Records under one node (drill-through / detail), paged (REQ-DET-001)."""

    dataset: str
    filters: Tuple[Filter, ...] = ()
    path_fields: Tuple[str, ...] = ()
    path: Tuple[Any, ...] = ()  # typed values; None is the NULL member
    search: Optional[str] = None  # case-insensitive substring over every column
    sort: Tuple[SortKey, ...] = ()
    window: Window = field(default_factory=Window)

    def validate(self, schema: Mapping[str, str]) -> DetailQuery:
        if not self.dataset:
            raise IRValidationError("dataset is required.")
        probe = PivotQuery(dataset=self.dataset, rows=(), measures=(MeasureRef("__n", "count", None),),
                           filters=self.filters, window=self.window)
        probe.validate(schema)  # filters and window share the pivot rules
        if len(self.path) != len(self.path_fields):
            raise IRValidationError(f"path {list(self.path)!r} does not match the fields {list(self.path_fields)}.")
        if len(set(self.path_fields)) != len(self.path_fields):
            raise IRValidationError("path fields must be unique.")
        for name in self.path_fields:
            if name not in schema:
                raise IRValidationError(f"path field references unknown column {name!r}.")
        if self.search is not None and not isinstance(self.search, str):
            raise IRValidationError("search must be text.")
        if len({s.field for s in self.sort}) != len(self.sort):
            raise IRValidationError("each sort field may appear once.")
        for key in self.sort:
            if key.field not in schema:
                raise IRValidationError(f"sort key references unknown column {key.field!r}.")
            if key.absolute and schema[key.field] not in {"integer", "floating", "decimal"}:
                raise IRValidationError(f"absolute sort needs a numeric value, not {key.field!r}.")
        return self
