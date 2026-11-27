"""Provenance of private native level tables, bound to the writing grant."""

from __future__ import annotations

from typing import Any, Tuple

from .scope import current_scope

SCHEMA_PREFIX = "__pivot_levels_"


def binding(dataset: str) -> Tuple[Any, ...]:
    grant = current_scope().grant_for(dataset)
    fields = getattr(grant, "field_values", {}) or {}
    return (dataset, str(getattr(grant, "scope_fingerprint", None) or "global"),
            str(getattr(grant, "policy_version", "") or ""),
            tuple(sorted((k, tuple(v)) for k, v in fields.items())))


def register(relation: Any, dataset: str) -> None:
    backend = relation._find_backend()
    if not hasattr(backend, "_pivot_level_bindings"):
        object.__setattr__(backend, "_pivot_level_bindings", {})
    op = relation.op()
    backend._pivot_level_bindings[(op.namespace.database, op.name)] = binding(dataset)


def unregister(relation: Any) -> None:
    op = relation.op()
    relation._find_backend()._pivot_level_bindings.pop((op.namespace.database, op.name), None)


def accepts(node: Any) -> bool:
    entries = getattr(getattr(node, "source", None), "_pivot_level_bindings", {})
    saved = entries.get((node.namespace.database, node.name))
    return saved is not None and saved == binding(saved[0])
