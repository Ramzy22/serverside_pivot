"""Executable aggregation registry (REQ-AGG-001..004).

Metadata comes from ``core/spec/aggregation-registry.json``, the
language-neutral definition.  This module binds each entry to its Ibis
lowering; a JSON entry without a lowering (or the reverse) is an import-time
error, so the two cannot drift apart.
"""

from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, FrozenSet, List

__all__ = ["AggregationEntry", "REGISTRY", "Registry", "UnknownAggregationError", "type_family"]

_SPEC = pathlib.Path(__file__).resolve().parent / "spec" / "aggregation-registry.json"


class UnknownAggregationError(ValueError):
    pass


_FAMILIES = {
    "numeric": {"integer", "floating", "decimal"},
    "orderable": {"integer", "floating", "decimal", "string", "date", "timestamp", "boolean"},
    "any": {"integer", "floating", "decimal", "string", "date", "timestamp", "boolean", "other"},
}


def type_family(dtype: Any) -> str:
    """Map an Ibis dtype to the registry's type family."""
    if dtype.is_integer():
        return "integer"
    if dtype.is_floating():
        return "floating"
    if dtype.is_decimal():
        return "decimal"
    if dtype.is_string():
        return "string"
    if dtype.is_boolean():
        return "boolean"
    if dtype.is_timestamp():
        return "timestamp"
    if dtype.is_date():
        return "date"
    return "other"


@dataclass(frozen=True)
class AggregationEntry:
    name: str
    inputs: FrozenSet[str]
    empty: Any
    merge: str
    cost_class: str
    sql: str
    lower: Callable[[Any, Any], Any]  # (column or None for count(*), table) -> Ibis scalar

    def accepts(self, family: str) -> bool:
        return any(family in _FAMILIES.get(i, ()) for i in self.inputs if i != "star")


def _sum(col: Any, _t: Any) -> Any:
    return col.sum()


def _count(col: Any, t: Any) -> Any:
    return t.count() if col is None else col.count()


# Ibis lowering per aggregation.  Every lowering ignores NULL inputs (SQL
# semantics) except count(*), matching the registry's "nulls": "ignore".
_LOWERINGS: Dict[str, Callable[[Any, Any], Any]] = {
    "sum": _sum,
    "count": _count,
    "avg": lambda col, _t: col.mean(),
    "min": lambda col, _t: col.min(),
    "max": lambda col, _t: col.max(),
    "count_distinct": lambda col, _t: col.nunique(),  # exact (REQ-AGG-003)
    "median": lambda col, _t: col.cast("float64").median(),
}


class Registry:
    def __init__(self, spec_path: pathlib.Path = _SPEC) -> None:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        names = set(spec["aggregations"])
        if names != set(_LOWERINGS):
            raise RuntimeError(
                f"aggregation registry drift: spec-only={sorted(names - set(_LOWERINGS))} "
                f"lowering-only={sorted(set(_LOWERINGS) - names)}"
            )
        self.version = spec["version"]
        self._entries = {
            name: AggregationEntry(
                name=name,
                inputs=frozenset(raw["inputs"]),
                empty=raw["empty"],
                merge=raw["merge"],
                cost_class=raw["costClass"],
                sql=raw["sql"],
                lower=_LOWERINGS[name],
            )
            for name, raw in spec["aggregations"].items()
        }

    def get(self, name: str) -> AggregationEntry:
        try:
            return self._entries[name]
        except KeyError:
            raise UnknownAggregationError(f"unknown aggregation {name!r}") from None

    def names(self) -> List[str]:
        return sorted(self._entries)


REGISTRY = Registry()
