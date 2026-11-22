"""Physical strategies and the rule-based physical planner.

``union_all`` is the reference; every other strategy must be result-equivalent
to it (tests/core/test_strategy_equivalence.py).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple

from ..logical import LogicalPlan
from . import leaf_partials, union_all

__all__ = ["STRATEGIES", "TREES", "choose", "leaf_partials", "union_all"]

STRATEGIES: Dict[str, Callable[[LogicalPlan, Any], Any]] = {
    "union_all": union_all.lower,
    "leaf_partials": leaf_partials.lower,
}

# Unordered tree + order key, for materialized pagination (core/pagination.py).
TREES: Dict[str, Callable[[LogicalPlan, Any], Tuple[Any, List[Any]]]] = {
    "union_all": union_all.tree_of,
    "leaf_partials": leaf_partials.tree_of,
}


# Measured with scripts/benchmark_core_strategies.py (DuckDB 1.5, 10M rows):
# leaf_partials is 1.13-1.27x faster while the partial grain stays well below
# the row count, and ~4x slower once the grain is near-unique (6 dims fully
# expanded).  Without cardinality statistics the grain depth is the proxy.
MAX_PARTIAL_GRAIN = 4


def choose(plan: LogicalPlan, schema: Any) -> str:
    """Rule-based physical choice (see the benchmark note above)."""
    grain = len(plan.levels[-1].keys) if plan.levels else 0
    if 1 <= grain <= MAX_PARTIAL_GRAIN and leaf_partials.supports(plan, schema):
        return "leaf_partials"
    return "union_all"
