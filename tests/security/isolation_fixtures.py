"""Shared fixtures for the tenant-isolation matrix (REQ-SEC-001..008).

The dataset is built so that any leak is unambiguous:

* ``tenant`` holds ``"A"`` (the principal's tenant), ``"a"`` (same letter,
  different case), ``"B"`` and ``None``.
* ``amount`` is a distinct power of ten per tenant, so any aggregate reveals
  exactly which tenants contributed (A rows sum to 3, anything >= 100 leaks).
* ``secret`` is a per-tenant text marker that must never be returned for any
  tenant except ``A``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable

import pyarrow as pa

from pivot_engine.runtime.dataset_authorization import RuntimeDatasetAuthorizer

TABLE = "sales"
ALLOWED_TENANT = "A"
FORBIDDEN_MARKERS = ("a-secret", "B-secret", "N-secret")
ALLOWED_AMOUNT_TOTAL = 3  # three tenant-A rows of amount 1


def tenant_table() -> pa.Table:
    rows = [
        ("A", "North", "x", 1, "A-secret"),
        ("A", "North", "y", 1, "A-secret"),
        ("A", "South", "x", 1, "A-secret"),
        ("a", "North", "x", 100, "a-secret"),
        ("a", "East", "z", 100, "a-secret"),
        ("B", "North", "x", 10_000, "B-secret"),
        ("B", "West", "w", 10_000, "B-secret"),
        (None, "North", "x", 1_000_000, "N-secret"),
    ]
    tenant, region, product, amount, secret = zip(*rows, strict=False)
    return pa.table(
        {
            "tenant": pa.array(tenant, type=pa.string()),
            "region": pa.array(region, type=pa.string()),
            "product": pa.array(product, type=pa.string()),
            "amount": pa.array(amount, type=pa.int64()),
            "secret": pa.array(secret, type=pa.string()),
            # tree-view columns (adjacency source)
            "id": pa.array([f"n{i}" for i in range(len(rows))], type=pa.string()),
            "parent_id": pa.array([None] * len(rows), type=pa.string()),
        }
    )


@dataclass(frozen=True)
class Principal:
    id: str = "tenant-a-user"
    role: str = "viewer"
    attributes: Dict[str, Any] = field(default_factory=lambda: {"tenant": ALLOWED_TENANT})


def claims_authorizer() -> RuntimeDatasetAuthorizer:
    return RuntimeDatasetAuthorizer(
        {
            TABLE: {
                "roles": {"viewer"},
                "operations": {"read", "export"},
                "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}},
            }
        }
    )


def _flatten(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return json.dumps(value, default=lambda v: v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v))


def leaks(payload: Any, rows: Iterable[Dict[str, Any]] = (), *, tenant_values: Iterable[Any] = ()) -> list[str]:
    """Describe every leak in a response.

    ``payload`` is searched as text for per-tenant secret markers.  ``rows``
    are the response's data records: every numeric non-metadata cell must be
    an aggregate over tenant-A rows only (all such aggregates are <= 3).
    ``tenant_values`` are distinct values a discovery endpoint returned.
    """
    found: list[str] = []
    text = _flatten(payload)
    for marker in FORBIDDEN_MARKERS:
        if marker in text:
            found.append(f"marker {marker!r} present")
    for row in rows:
        if not isinstance(row, dict):
            continue
        if "tenant" in row and row["tenant"] not in (ALLOWED_TENANT,) and not str(row.get("_id", "")).startswith("Grand"):
            if row["tenant"] is not None or "tenant" in (row.get("_pathFields") or []):
                found.append(f"row with tenant={row['tenant']!r}")
        for key, value in row.items():
            if key.startswith("_") or key in {"depth", "id", "parent_id"} or isinstance(value, bool):
                continue
            if isinstance(value, (int, float)) and value > ALLOWED_AMOUNT_TOTAL:
                found.append(f"cell {key}={value} aggregates forbidden tenants")
    for value in tenant_values:
        if value != ALLOWED_TENANT:
            found.append(f"distinct tenant value {value!r} disclosed")
    return found
