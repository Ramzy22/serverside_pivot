"""Physical strategies and the rule-based physical planner.

``union_all`` is the reference; every other strategy must be result-equivalent
to it (tests/core/test_strategy_equivalence.py).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple

from ..logical import LogicalPlan
from . import leaf_partials, union_all
from .capabilities import BackendCapabilityError, check_capabilities

__all__ = ["STRATEGIES", "TREES", "BackendCapabilityError", "check_capabilities", "choose", "leaf_partials",
           "union_all"]

STRATEGIES: Dict[str, Callable[[LogicalPlan, Any], Any]] = {
    "union_all": union_all.lower,
    "leaf_partials": leaf_partials.lower,
}

# Unordered tree + order key, for materialized pagination (core/pagination.py).
TREES: Dict[str, Callable[[LogicalPlan, Any], Tuple[Any, List[Any]]]] = {
    "union_all": union_all.tree_of,
    "leaf_partials": leaf_partials.tree_of,
}


# Measured 2026-11-26 (DuckDB 1.5, the hierarchy benchmark set, 3 row levels):
# with two or more levels visible, one scan at the partial grain beats a scan
# per level (10M fully expanded: 304 vs 468 ms; 1M: even).  With one visible
# level it is a scan plus a merge (1M collapsed: 69 vs 61 ms), and with pivot
# columns the per-member merges cost more than per-level cells (1M, 48
# members: 1.2-1.5x slower in every view), so union_all serves those.  A
# near-unique grain (6 dims fully expanded) is ~4x slower: the grain depth,
# capped at MAX_PARTIAL_GRAIN, is the proxy without cardinality statistics.
MAX_PARTIAL_GRAIN = 4


def choose(plan: LogicalPlan, schema: Any) -> str:
    """Rule-based physical choice (see the measurement note above)."""
    grain = len(leaf_partials.grain(plan))
    if (not plan.query.columns and 2 <= grain <= MAX_PARTIAL_GRAIN
            and leaf_partials.supports(plan, schema)):
        return "leaf_partials"
    return "union_all"
