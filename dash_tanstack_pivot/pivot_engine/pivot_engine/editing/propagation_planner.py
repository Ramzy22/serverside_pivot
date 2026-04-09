from __future__ import annotations

from typing import Any, Optional


REAL_PROPAGATION_POLICIES = {"equal", "proportional"}


def normalize_real_propagation_policy(value: Any, fallback: Optional[str] = "equal") -> Optional[str]:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"equal", "even", "default", "delta", "uniform", "equal_delta", "balanced_delta", "uniform_shift", "balanced_shift"}:
        return "equal"
    if normalized in {"proportional", "ratio", "scale", "scaled"}:
        return "proportional"
    if normalized in {"none", "skip", "parent_only", "scenario_override"}:
        return None
    return fallback


def validate_real_propagation_policy(value: Any) -> Optional[str]:
    normalized = normalize_real_propagation_policy(value, fallback=None)
    return normalized if normalized in REAL_PROPAGATION_POLICIES else None
