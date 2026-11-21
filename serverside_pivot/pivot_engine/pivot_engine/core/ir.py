"""Semantic IR: what a pivot query *means*, independent of any engine.

Pure data, no Ibis.  Validated against a dataset schema (column -> type
family) before planning; every invalid input is a typed error, never a silent
fallback (REQ-AGG-001, F-SILENT).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, FrozenSet, Mapping, Optional, Tuple

from .registry import REGISTRY, UnknownAggregationError

__all__ = [
    "IRValidationError",
    "MeasureFilter",
    "MeasureRef",
    "PivotQuery",
    "Predicate",
    "SortKey",
    "Window",
    "PREDICATE_OPS",
]

PREDICATE_OPS = frozenset({"=", "!=", ">", ">=", "<", "<=", "in", "not_in", "is_null", "is_not_null"})
_COMPARISON_OPS = frozenset({"=", "!=", ">", ">=", "<", "<="})


class IRValidationError(ValueError):
    """The query is not meaningful for this dataset."""


@dataclass(frozen=True)
class MeasureRef:
    alias: str
    agg: str
    field: Optional[str]  # None only for count(*)


@dataclass(frozen=True)
class Predicate:
    """Row predicate evaluated before aggregation (part of E0)."""

    field: str
    op: str
    value: Any = None


@dataclass(frozen=True)
class MeasureFilter:
    """Group predicate at hierarchy ``level`` (1-based), evaluated on E0 (REQ-TOT-001)."""

    level: int
    measure: str
    op: str
    value: Any


@dataclass(frozen=True)
class SortKey:
    """Sibling order for one row dimension. NULLS LAST in both directions (REQ-BACK-001)."""

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
    filters: Tuple[Predicate, ...] = ()
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
        for p in self.filters:
            need(p.field, "filter")
            if p.op not in PREDICATE_OPS:
                raise IRValidationError(f"unsupported filter operator {p.op!r}.")
            if p.op in {"in", "not_in"} and not isinstance(p.value, (list, tuple, frozenset, set)):
                raise IRValidationError(f"{p.op} requires a list value.")
        for mf in self.measure_filters:
            if mf.measure not in aliases:
                raise IRValidationError(f"measure filter references unknown measure {mf.measure!r}.")
            if not 1 <= mf.level <= len(self.rows):
                raise IRValidationError(f"measure filter level {mf.level} outside 1..{len(self.rows)}.")
            if mf.op not in _COMPARISON_OPS:
                raise IRValidationError(f"unsupported measure filter operator {mf.op!r}.")
        for s in self.sort:
            if s.field not in self.rows:
                raise IRValidationError(f"sort key {s.field!r} is not a row dimension.")
        if self.totals not in {"visible", "all_data"}:
            raise IRValidationError(f"totals must be 'visible' or 'all_data', not {self.totals!r}.")
        if self.window.offset < 0 or (self.window.limit is not None and self.window.limit < 0):
            raise IRValidationError("window offset/limit must be non-negative.")
        if self.expanded is not None:
            for path in self.expanded:
                if not isinstance(path, tuple) or not 1 <= len(path) < max(1, len(self.rows)):
                    raise IRValidationError(f"expanded path {path!r} is not a parent path.")
        return self
