"""Semantic IR: what a pivot query *means*, independent of any engine.

Pure data, no Ibis.  Validated against a dataset schema (column -> type
family) before planning; every invalid input is a typed error, never a silent
fallback (REQ-AGG-001, F-SILENT).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, FrozenSet, Mapping, Optional, Tuple, Union

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
    both directions (REQ-BACK-001).
    """

    field: str
    desc: bool = False


@dataclass(frozen=True)
class Window:
    offset: int = 0
    limit: Optional[int] = None


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
        for s in self.sort:
            if s.field not in self.rows and s.field not in aliases:
                raise IRValidationError(f"sort key {s.field!r} is neither a row dimension nor a measure.")
        if self.totals not in {"visible", "all_data"}:
            raise IRValidationError(f"totals must be 'visible' or 'all_data', not {self.totals!r}.")
        if self.window.offset < 0 or (self.window.limit is not None and self.window.limit < 0):
            raise IRValidationError("window offset/limit must be non-negative.")
        if self.expanded is not None:
            for path in self.expanded:
                if not isinstance(path, tuple) or not 1 <= len(path) < max(1, len(self.rows)):
                    raise IRValidationError(f"expanded path {path!r} is not a parent path.")
        return self


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
            sort=tuple(SortKey(s["field"], bool(s.get("desc", False))) for s in obj.get("sort", ())),
            totals=obj.get("totals", "visible"),
            expanded=None if expanded is None else frozenset(tuple(p) for p in expanded),
            window=Window(int(window.get("offset", 0)), window.get("limit")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IRValidationError(f"malformed query: {exc!r}") from None


def _freeze(value: Any) -> Any:
    return tuple(value) if isinstance(value, list) else value


def _filter_from_wire(obj: Mapping[str, Any]) -> Filter:
    if "conditions" in obj:
        return PredicateGroup(str(obj["op"]).lower(), tuple(_filter_from_wire(c) for c in obj["conditions"]))
    return Predicate(obj["field"], obj["op"], _freeze(obj.get("value")), bool(obj.get("caseSensitive", True)))
