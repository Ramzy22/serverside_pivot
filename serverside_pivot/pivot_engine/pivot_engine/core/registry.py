"""Executable aggregation registry (REQ-AGG-001..004).

Engine-free metadata from ``core/spec/aggregation-registry.json``, the
language-neutral definition.  Ibis lowerings live in
``core/physical/lowering.py``, which fails at import if it drifts from this
registry.
"""

from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass
from typing import Any, FrozenSet, List

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
    params: FrozenSet[str]
    optional_params: FrozenSet[str] = frozenset()  # params a measure may omit (array_agg's orderBy)

    def accepts(self, family: str) -> bool:
        return any(family in _FAMILIES.get(i, ()) for i in self.inputs if i != "star")


class Registry:
    def __init__(self, spec_path: pathlib.Path = _SPEC) -> None:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        self.version = spec["version"]
        self._entries = {
            name: AggregationEntry(
                name=name,
                inputs=frozenset(raw["inputs"]),
                empty=raw["empty"],
                merge=raw["merge"],
                cost_class=raw["costClass"],
                sql=raw["sql"],
                params=frozenset(raw.get("params", ())),
                optional_params=frozenset(raw.get("optionalParams", ())),
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
