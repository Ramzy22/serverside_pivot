"""Logical planning: from IR to an engine-independent plan.

The plan states *which* nodes exist and *how* each value is defined; physical
strategies (``core.physical``) decide how to compute it.  Semantics follow
docs/semantics/SEMANTICS.md §2 and §5:

* E0 = scope ∧ row filters (scope is applied by the Authority on the relation);
* a level-ℓ group passes when its measures over E0 satisfy every filter at ℓ;
* E = records whose group passes at every filtered level;
* a node is shown iff E ∩ n ≠ ∅ and all its ancestors are expanded;
* values are agg(E ∩ n) ("visible") or agg(E0 ∩ n) ("all_data");
* order: grand total, then depth-first; siblings by the sort key, NULLS LAST.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Optional, Tuple

from .ir import MeasureFilter, PivotQuery

__all__ = ["Level", "LogicalPlan", "plan"]


@dataclass(frozen=True)
class Level:
    depth: int  # 0 = grand total
    keys: Tuple[str, ...]  # rows[:depth]
    # Parent paths (length depth-1) whose children are visible; None = all.
    visible_parents: Optional[FrozenSet[Tuple[Any, ...]]]


@dataclass(frozen=True)
class LogicalPlan:
    query: PivotQuery
    levels: Tuple[Level, ...]
    filters_by_level: Dict[int, Tuple[MeasureFilter, ...]]
    values_from: str  # "E" (visible) or "E0" (all_data)
    descending: Dict[str, bool]  # row dimension -> descending (its own level)
    sort: Tuple[Tuple[str, bool, bool, bool], ...] = ()  # (field, descending, is_measure, absolute), in precedence

    @property
    def rows(self) -> Tuple[str, ...]:
        return self.query.rows


def _visible_parents(
    expanded: Optional[FrozenSet[Tuple[Any, ...]]], depth: int
) -> Optional[FrozenSet[Tuple[Any, ...]]]:
    """Parents of length ``depth-1`` that are expanded with all their ancestors."""
    if expanded is None or depth <= 1:
        return None
    return frozenset(
        path for path in expanded
        if len(path) == depth - 1 and all(path[:k] in expanded for k in range(1, depth - 1))
    )


def plan(query: PivotQuery) -> LogicalPlan:
    candidates = (
        Level(depth=d, keys=tuple(query.rows[:d]), visible_parents=_visible_parents(query.expanded, d))
        for d in range(len(query.rows) + 1)
    )
    # A level with no expanded parent shows no node; pruning it is exact.
    levels = tuple(level for level in candidates if level.visible_parents != frozenset())
    by_level: Dict[int, Tuple[MeasureFilter, ...]] = {}
    for mf in query.measure_filters:
        by_level[mf.level] = by_level.get(mf.level, ()) + (mf,)
    return LogicalPlan(
        query=query,
        levels=levels,
        filters_by_level=by_level,
        values_from="E" if query.totals == "visible" else "E0",
        descending={s.field: s.desc for s in query.sort if s.field in query.rows},
        sort=tuple((s.field, s.desc, s.field not in query.rows, s.absolute) for s in query.sort),
    )
