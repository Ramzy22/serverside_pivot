"""A spy on the semantic core's read entry points: proof that a request was answered by the core."""

from __future__ import annotations

from collections import Counter
from typing import Any

ENTRY_POINTS = ("core_hierarchy_view", "core_pivot_table", "core_drill_through", "core_unique_values")


def spy_core(monkeypatch: Any) -> Counter:
    """Count calls of each core entry point (by name) for the rest of the test."""
    from pivot_engine.transport import core_bridge

    calls: Counter = Counter()
    for name in ENTRY_POINTS:
        original = getattr(core_bridge, name)

        def wrapper(*args: Any, __name: str = name, __original: Any = original, **kwargs: Any) -> Any:
            calls[__name] += 1
            return __original(*args, **kwargs)

        monkeypatch.setattr(core_bridge, name, wrapper)
    return calls
