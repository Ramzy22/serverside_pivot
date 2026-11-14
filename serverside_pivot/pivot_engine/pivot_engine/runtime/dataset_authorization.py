"""Server-owned dataset authorization for runtime and legacy API reads."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, FrozenSet, Mapping, NoReturn, Optional

from ..security_events import log_authorization_denied
from .contracts import RuntimeContractError


class RuntimeDatasetAuthorizationError(RuntimeContractError, PermissionError):
    """Raised when a principal is not allowed to access a dataset."""

    def __init__(self, message: str = "Dataset access is not authorized.") -> None:
        super().__init__(message, code="dataset_not_authorized")


class RowScopeAuthorizationError(RuntimeDatasetAuthorizationError):
    """Raised when a tenant-scoped dataset lacks a valid trusted claim."""

    def __init__(self) -> None:
        # Keep this deliberately generic: neither a table nor a missing claim
        # should become an oracle for an unauthorised caller.
        super().__init__("Row-level access is not authorized.")


_ALL_DATASET_OPERATIONS = frozenset(
    {"read", "export", "mutate", "cdc", "schema", "subscribe"}
)
_DEFAULT_DATASET_ROLES = frozenset({"admin", "editor", "viewer"})
_ROW_SECURITY_CLAIMS = "claims"
_ROW_SECURITY_GLOBAL = "global"


def _strings(value: Any) -> FrozenSet[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(
        str(item).strip().lower() for item in value if str(item).strip()
    )


def _scope_values(value: Any) -> tuple[Any, ...]:
    """Normalise a trusted claim to a deterministic non-empty scalar tuple."""
    if isinstance(value, (list, tuple, set, frozenset)):
        values = list(value)
        if isinstance(value, (set, frozenset)):
            values.sort(key=lambda item: str(item))
    else:
        values = [value]
    if not values or any(
        item is None or isinstance(item, (dict, list, tuple, set, frozenset))
        for item in values
    ):
        raise RowScopeAuthorizationError()
    return tuple(values)


def _scope_fingerprint(table: str, field_values: Mapping[str, tuple[Any, ...]]) -> str:
    """Create a stable cache/ticket scope identity without exposing values."""
    canonical = {
        "table": str(table),
        "fields": [
            [field, list(field_values[field])]
            for field in sorted(field_values)
        ],
    }
    encoded = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class RowSecurityPolicy:
    """Explicit policy deciding whether a dataset is global or claim-scoped."""

    mode: str
    claim_to_field: Mapping[str, str]

    @classmethod
    def from_value(cls, value: Any) -> "RowSecurityPolicy":
        if not isinstance(value, Mapping):
            raise ValueError("Dataset permissions require an explicit rowSecurity mapping.")
        mode = str(value.get("mode") or "").strip().lower()
        raw_claims = value.get("claims")
        if mode == _ROW_SECURITY_GLOBAL:
            if raw_claims not in (None, {}, []):
                raise ValueError("Global rowSecurity policies cannot declare claims.")
            return cls(mode=mode, claim_to_field=MappingProxyType({}))
        if mode != _ROW_SECURITY_CLAIMS or not isinstance(raw_claims, Mapping):
            raise ValueError(
                "rowSecurity must declare mode 'claims' with a non-empty claims mapping or mode 'global'."
            )
        claim_to_field: dict[str, str] = {}
        destination_fields: set[str] = set()
        for claim, field in raw_claims.items():
            claim_name = str(claim or "").strip()
            field_name = str(field or "").strip()
            if not claim_name or not field_name or field_name in destination_fields:
                raise ValueError("rowSecurity claims must map unique non-empty claim and field names.")
            claim_to_field[claim_name] = field_name
            destination_fields.add(field_name)
        if not claim_to_field:
            raise ValueError("Claim-scoped rowSecurity policies require at least one claim.")
        return cls(
            mode=mode,
            claim_to_field=MappingProxyType(dict(claim_to_field)),
        )

    def grant(self, *, table: str, principal: Any) -> "RowScopeGrant":
        subject = str(getattr(principal, "id", None) or "anonymous")
        if self.mode == _ROW_SECURITY_GLOBAL:
            field_values: Mapping[str, tuple[Any, ...]] = MappingProxyType({})
        else:
            attributes = getattr(principal, "attributes", None)
            if not isinstance(attributes, Mapping):
                raise RowScopeAuthorizationError()
            values: dict[str, tuple[Any, ...]] = {}
            for claim, field in self.claim_to_field.items():
                if claim not in attributes:
                    raise RowScopeAuthorizationError()
                values[field] = _scope_values(attributes[claim])
            field_values = MappingProxyType(values)
        return RowScopeGrant(
            table=str(table),
            subject=subject,
            field_values=field_values,
            scope_fingerprint=_scope_fingerprint(str(table), field_values),
        )


@dataclass(frozen=True)
class RowScopeGrant:
    """Immutable server-derived row predicates for one principal and table."""

    table: str
    subject: str
    field_values: Mapping[str, tuple[Any, ...]]
    scope_fingerprint: str

    def apply_to_filters(self, filters: Optional[Mapping[str, Any]]) -> dict[str, Any]:
        scoped = dict(filters or {})
        for field, values in self.field_values.items():
            scoped[field] = {
                "type": "eq" if len(values) == 1 else "in",
                "value": values[0] if len(values) == 1 else list(values),
            }
        return scoped


@dataclass(frozen=True)
class DatasetTableAccess:
    """Static table permissions configured at the host boundary."""

    roles: FrozenSet[str] = _DEFAULT_DATASET_ROLES
    operations: FrozenSet[str] = _ALL_DATASET_OPERATIONS
    row_security: RowSecurityPolicy = RowSecurityPolicy(
        mode=_ROW_SECURITY_GLOBAL,
        claim_to_field=MappingProxyType({}),
    )

    @classmethod
    def from_value(cls, value: Any) -> "DatasetTableAccess":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("Dataset permissions must be mappings.")

        raw_roles = value.get("roles")
        raw_operations = value.get("operations")
        if "rowSecurity" not in value:
            raise ValueError(
                "Dataset permissions require explicit rowSecurity; declare claims or global."
            )
        roles = _strings(_DEFAULT_DATASET_ROLES if raw_roles is None else raw_roles)
        operations = _strings(
            _ALL_DATASET_OPERATIONS if raw_operations is None else raw_operations
        )
        if not roles or not operations:
            raise ValueError("Dataset permissions require roles and operations.")
        unsupported = operations - _ALL_DATASET_OPERATIONS
        if unsupported:
            raise ValueError(
                "Unsupported dataset operation permission: "
                + ", ".join(sorted(unsupported))
            )
        return cls(
            roles=roles,
            operations=operations,
            row_security=RowSecurityPolicy.from_value(value.get("rowSecurity")),
        )


class RuntimeDatasetAuthorizer:
    """Default-deny table authorization for externally reachable datasets.

    Policies are supplied only by the server during application construction::

        RuntimeDatasetAuthorizer({
            "sales": {"roles": {"viewer", "editor"}, "operations": {"read", "export"}},
            "audit": {"roles": {"admin"}},
        })

    A missing table entry (including an entirely empty policy) is denied.
    ``"*"`` may be used deliberately for a server-wide rule.
    """

    def __init__(self, table_permissions: Optional[Mapping[str, Any]] = None) -> None:
        self._table_permissions = {
            str(table).strip(): DatasetTableAccess.from_value(access)
            for table, access in (table_permissions or {}).items()
            if str(table).strip()
        }

    @staticmethod
    def _role(principal: Any) -> str:
        return str(getattr(principal, "role", "") or "").strip().lower()

    @staticmethod
    def _deny(
        *,
        principal: Any,
        table: str,
        operation: str,
        reason: str,
        message: str | None = None,
    ) -> NoReturn:
        """Log a generic denial at the common server-owned policy boundary."""
        log_authorization_denied(
            principal=principal,
            table=table,
            operation=operation,
            reason=reason,
        )
        raise RuntimeDatasetAuthorizationError(message or "Dataset access is not authorized.")

    def authorize(
        self, *, principal: Any, table: str, operation: str = "read"
    ) -> RowScopeGrant:
        normalized_table = str(table or "").strip()
        normalized_operation = str(operation or "read").strip().lower()
        if normalized_operation not in _ALL_DATASET_OPERATIONS:
            self._deny(
                principal=principal,
                table=normalized_table,
                operation=normalized_operation,
                reason="unsupported_operation",
            )
        access = self._table_permissions.get(normalized_table) or self._table_permissions.get("*")
        if access is None:
            self._deny(
                principal=principal,
                table=normalized_table,
                operation=normalized_operation,
                reason="dataset_not_configured",
                message="This dataset is not authorized for the current principal.",
            )
        if self._role(principal) not in access.roles:
            self._deny(
                principal=principal,
                table=normalized_table,
                operation=normalized_operation,
                reason="role_not_permitted",
            )
        if normalized_operation not in access.operations:
            self._deny(
                principal=principal,
                table=normalized_table,
                operation=normalized_operation,
                reason="operation_not_permitted",
            )
        try:
            return access.row_security.grant(table=normalized_table, principal=principal)
        except RuntimeDatasetAuthorizationError:
            log_authorization_denied(
                principal=principal,
                table=normalized_table,
                operation=normalized_operation,
                reason="row_scope_denied",
            )
            raise


__all__ = [
    "DatasetTableAccess",
    "RowScopeAuthorizationError",
    "RowScopeGrant",
    "RowSecurityPolicy",
    "RuntimeDatasetAuthorizationError",
    "RuntimeDatasetAuthorizer",
]
