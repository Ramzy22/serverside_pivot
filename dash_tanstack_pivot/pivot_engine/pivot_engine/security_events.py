"""Small, dependency-free audit events for security-sensitive denials.

These records deliberately contain only normalized control metadata.  They
must never serialize HTTP requests, JWTs, claims, row images, or exception
messages because logs have a broader audience than the protected data path.
"""

from __future__ import annotations

import logging
from typing import Any


_LOGGER = logging.getLogger(__name__)
_MAX_AUDIT_VALUE_LENGTH = 128


def _audit_value(value: Any, *, fallback: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return fallback
    normalized = "".join(character for character in normalized if character.isprintable())
    return normalized[:_MAX_AUDIT_VALUE_LENGTH] or fallback


def principal_subject(principal: Any) -> str:
    """Return the server-authenticated subject without serializing claims."""
    if isinstance(principal, dict):
        return _audit_value(principal.get("id") or principal.get("sub"), fallback="anonymous")
    return _audit_value(getattr(principal, "id", None), fallback="anonymous")


def log_authorization_denied(
    *,
    principal: Any,
    table: Any,
    operation: Any,
    reason: str = "policy_denied",
) -> None:
    """Emit a stable, non-sensitive authorization denial event."""
    _LOGGER.warning(
        "pivot_security_event event=authorization_denied subject=%s table=%s operation=%s reason=%s",
        principal_subject(principal),
        _audit_value(table, fallback="unknown"),
        _audit_value(operation, fallback="unknown"),
        _audit_value(reason, fallback="policy_denied"),
    )


__all__ = ["log_authorization_denied", "principal_subject"]
