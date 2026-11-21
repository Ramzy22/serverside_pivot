"""Physical strategies and the rule-based physical planner.

``union_all`` is the reference; every other strategy must be result-equivalent
to it (tests/core/test_strategy_equivalence.py).
"""

from __future__ import annotations

from typing import Any, Callable, Dict

from ..logical import LogicalPlan
from . import leaf_partials, union_all

__all__ = ["STRATEGIES", "choose", "leaf_partials", "union_all"]

STRATEGIES: Dict[str, Callable[[LogicalPlan, Any], Any]] = {
    "union_all": union_all.lower,
    "leaf_partials": leaf_partials.lower,
}


def choose(plan: LogicalPlan, schema: Any) -> str:
    """Rule-based choice; defaults are set from benchmarks (scripts/benchmark_core_strategies.py)."""
    if len(plan.rows) >= 2 and leaf_partials.supports(plan, schema):
        return "leaf_partials"
    return "union_all"
