"""Leaf-partials strategy: one scan at leaf grain, parents merged from partial state.

Each parent is an algebraic merge of its leaves' sufficient statistics
(REQ-TOT-003 still holds: the partials are computed over the same E/E0 as the
reference).  Holistic aggregations (median, exact distinct) and DECIMAL
averages cannot be merged exactly this way, so :func:`supports` rejects them
and the planner uses ``union_all``.  Result-equivalence with ``union_all`` is
enforced by tests/core/test_strategy_equivalence.py.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

import ibis

from ..logical import LogicalPlan
from .union_all import LevelAggregate, build, tree

__all__ = ["lower", "supports", "tree_of"]

_MERGEABLE = {"sum", "count", "min", "max", "avg"}


def supports(plan: LogicalPlan, schema: Any) -> bool:
    if plan.query.columns:
        return False  # pivot cells aggregate conditionally; the reference strategy serves them
    if plan.query.ragged is not None:
        return False  # ragged levels end at different depths; the reference strategy serves them
    for m in plan.query.measures:
        if m.agg not in _MERGEABLE:
            return False
        if m.agg == "avg" and m.field is not None and not (
            schema[m.field].is_integer() or schema[m.field].is_floating()
        ):
            return False
    return bool(plan.rows)


def _partials(source: Any, plan: LogicalPlan) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i, m in enumerate(plan.query.measures):
        col: Any = None if m.field is None else source[m.field]
        if m.agg in {"sum", "avg"}:
            out[f"__s{i}"] = col.sum() if col is not None else source.count()
        if m.agg in {"count", "avg"}:
            out[f"__c{i}"] = source.count() if col is None else col.count()
        if m.agg == "min":
            out[f"__m{i}"] = col.min()
        if m.agg == "max":
            out[f"__m{i}"] = col.max()
    return out


def _merge(leaf: Any, plan: LogicalPlan) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i, m in enumerate(plan.query.measures):
        if m.agg == "sum":
            out[m.alias] = leaf[f"__s{i}"].sum()
        elif m.agg == "count":
            out[m.alias] = leaf[f"__c{i}"].sum().coalesce(0)  # count over no rows is 0
        elif m.agg == "min":
            out[m.alias] = leaf[f"__m{i}"].min()
        elif m.agg == "max":
            out[m.alias] = leaf[f"__m{i}"].max()
        else:  # avg = Σsum / Σcount, NULL when no non-null inputs
            total, count = leaf[f"__s{i}"].sum(), leaf[f"__c{i}"].sum()
            out[m.alias] = ibis.ifelse(count.coalesce(0) == 0, ibis.null(), total.cast("float64") / count)
    return out


def _from_leaf_partials(source: Any, plan: LogicalPlan) -> LevelAggregate:
    grain = list(plan.levels[-1].keys)  # deepest level that can show a node
    if not grain:
        return _direct_grand_total(source, plan)
    leaf = source.group_by(grain).aggregate(**_partials(source, plan))

    def aggregate(keys: Sequence[str]) -> Any:
        merged = _merge(leaf, plan)
        return leaf.group_by(list(keys)).aggregate(**merged) if keys else leaf.aggregate(**merged)

    return aggregate


def _direct_grand_total(source: Any, plan: LogicalPlan) -> LevelAggregate:
    leaf = source.aggregate(**_partials(source, plan))

    def aggregate(keys: Sequence[str]) -> Any:
        return leaf.aggregate(**_merge(leaf, plan))

    return aggregate


def lower(plan: LogicalPlan, relation: Any) -> Any:
    if not supports(plan, relation.schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    return build(plan, relation, _from_leaf_partials)


def tree_of(plan: LogicalPlan, relation: Any) -> Tuple[Any, List[Any]]:
    if not supports(plan, relation.schema()):
        raise ValueError("leaf_partials does not support this query; use union_all")
    return tree(plan, relation, _from_leaf_partials)
