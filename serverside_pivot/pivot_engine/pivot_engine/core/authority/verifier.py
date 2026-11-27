"""Plan verifier: prove an Ibis expression only reads protected data scoped.

Defence in depth behind the connection guard (REQ-SEC-001): before the
executor runs an expression, every physical table of a protected dataset must
be referenced *only* through a ``Filter`` whose predicates include the
canonical scope predicate for the active grant.  Anything else, such as an
unscoped self-join or a union with the raw table, is rejected.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set

import ibis.expr.operations as ops
from ibis.common.graph import Graph

from .level_binding import SCHEMA_PREFIX, accepts
from .scope import PathNotProvenError, compile_scope_predicate, current_scope, is_protecting

__all__ = ["PlanVerificationError", "verify_scoped"]


class PlanVerificationError(PathNotProvenError):
    """The expression reads a protected dataset outside its row scope."""


def _bare(name: str) -> str:
    return str(name).rsplit(".", 1)[-1].strip('"').lower()


def _subtree(node: Any) -> Set[Any]:
    return set(Graph.from_bfs(node).keys())


def verify_scoped(expr: Any, protected: Iterable[str] = ()) -> None:
    """Raise :class:`PlanVerificationError` unless ``expr`` reads protected tables scoped."""
    ctx = current_scope()
    tables = {_bare(t) for t in protected} | {t for t, g in ctx.grants.items() if is_protecting(g)}
    graph = Graph.from_bfs(expr.op())
    parents: Dict[Any, Set[Any]] = {}
    for node, children in graph.items():
        for child in children:
            parents.setdefault(child, set()).add(node)

    problems: List[str] = []
    for node in graph:
        if isinstance(node, ops.DatabaseTable) and str(node.namespace.database or "").startswith(SCHEMA_PREFIX):
            if not accepts(node):
                problems.append(f"{node.name}: level table read under a different scope binding")
            continue
        if not isinstance(node, ops.PhysicalTable) or _bare(node.name) not in tables:
            continue
        grant = ctx.grant_for(node.name)
        if not is_protecting(grant):
            problems.append(f"{node.name}: protected but no grant is active")
            continue
        canonical = compile_scope_predicate(node.to_expr(), grant).op()
        scope_filters = [
            p for p in parents.get(node, ())
            if isinstance(p, ops.Filter) and p.parent == node and canonical in p.predicates
        ]
        if not scope_filters:
            problems.append(f"{node.name}: read without its scope filter")
            continue
        allowed: Set[Any] = set(scope_filters)
        for f in scope_filters:
            for pred in f.predicates:
                allowed |= _subtree(pred)
        for p in parents.get(node, ()):
            if p in allowed:
                continue
            problems.append(f"{node.name}: referenced by {type(p).__name__} outside the scope filter")
    if problems:
        raise PlanVerificationError("Plan verification failed: " + "; ".join(sorted(set(problems))))
