"""Server-owned authorization for mutating Pivot Runtime commands.

The runtime protocol is intentionally host-neutral, but a browser envelope is
never an authority to change a dataset.  Hosts authenticate a principal and
install one of these authorizers at their server boundary.  The resulting
``MutationGrant`` is immutable and is the only mutation capability the router
passes to an adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Dict, FrozenSet, Mapping, Optional

from ..security import MutationAuthorizationError
from .contracts import RuntimeContractError


class RuntimeMutationAuthorizationError(
    RuntimeContractError, MutationAuthorizationError
):
    """Raised when an authenticated caller is not allowed to mutate data."""

    def __init__(self, message: str = "Mutation is not authorized.") -> None:
        super().__init__(message, code="mutation_not_authorized")


_ALL_MUTATION_OPERATIONS = frozenset(
    {"add", "remove", "update", "upsert", "aggregate", "history"}
)


def _strings(value: Any) -> FrozenSet[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(
        str(item).strip().lower() for item in value if str(item).strip()
    )


def _constraint_values(value: Any) -> tuple[Any, ...]:
    if isinstance(value, (list, tuple, set, frozenset)):
        values = tuple(item for item in value)
    else:
        values = (value,)
    if not values:
        raise RuntimeMutationAuthorizationError(
            "Mutation is not authorized for an empty row-security scope."
        )
    if any(isinstance(item, (dict, list, tuple, set, frozenset)) for item in values):
        raise RuntimeMutationAuthorizationError(
            "Mutation row-security constraints must be scalar values."
        )
    return values


@dataclass(frozen=True)
class MutationTableAccess:
    """Static server policy for one dataset.

    ``columns=None`` authorizes every physical column in the configured table;
    an explicit collection is safer for applications that expose only a small
    editable surface.  A table must be listed (or matched by ``"*"``) before
    any mutation is allowed.
    """

    roles: FrozenSet[str] = frozenset({"admin", "editor"})
    columns: Optional[FrozenSet[str]] = None
    operations: FrozenSet[str] = _ALL_MUTATION_OPERATIONS

    @classmethod
    def from_value(cls, value: Any) -> "MutationTableAccess":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("Mutation table permissions must be mappings.")
        raw_columns = value.get("columns")
        columns = (
            None
            if raw_columns is None or raw_columns == "*"
            else frozenset(
                str(column).strip()
                for column in raw_columns
                if str(column).strip()
            )
        )
        # Only an omitted field receives the documented default.  Treating an
        # explicitly empty list as false and replacing it with a broad default
        # turns a deny policy into a write capability.
        raw_roles = value.get("roles")
        raw_operations = value.get("operations")
        roles = _strings({"admin", "editor"} if raw_roles is None else raw_roles)
        operations = _strings(
            _ALL_MUTATION_OPERATIONS
            if raw_operations is None
            else raw_operations
        )
        if not roles or not operations:
            raise ValueError("Mutation table permissions require roles and operations.")
        unsupported = operations - _ALL_MUTATION_OPERATIONS
        if unsupported:
            raise ValueError(
                "Unsupported mutation operation permission: "
                + ", ".join(sorted(unsupported))
            )
        return cls(roles=roles, columns=columns, operations=operations)


@dataclass(frozen=True)
class MutationGrant:
    """An immutable, server-derived capability for one runtime request."""

    table: str
    subject: str
    row_constraints: Mapping[str, tuple[Any, ...]]
    allowed_columns: Optional[FrozenSet[str]]
    allowed_operations: FrozenSet[str]


class RuntimeMutationAuthorizer:
    """Default-deny authorization for HTTP ``update`` and ``transaction``.

    Configure it during server startup, never from a request payload::

        RuntimeMutationAuthorizer({
            "sales": {
                "roles": {"editor", "admin"},
                "columns": {"tenant", "region", "sales"},
                "operations": {"update", "aggregate", "history"},
            }
        })

    A dataset boundary supplies an immutable ``RowScopeGrant``. Its
    policy-declared field values are carried into every controller predicate
    and inserts/upserts must include matching values. An empty policy
    therefore makes an HTTP deployment read-only by construction.
    """

    def __init__(self, table_permissions: Optional[Mapping[str, Any]] = None) -> None:
        self._table_permissions = {
            str(table).strip(): MutationTableAccess.from_value(access)
            for table, access in (table_permissions or {}).items()
            if str(table).strip()
        }

    @staticmethod
    def _subject(principal: Any) -> str:
        return str(getattr(principal, "id", None) or "anonymous")

    @staticmethod
    def _role(principal: Any) -> str:
        return str(getattr(principal, "role", "") or "").strip().lower()

    @staticmethod
    def _requested_operations(kind: str, payload: Mapping[str, Any]) -> FrozenSet[str]:
        if kind == "update":
            return frozenset({"update"})
        event_action = str(
            payload.get("eventAction") or payload.get("event_action") or ""
        ).strip()
        if event_action:
            return frozenset({"history"})
        aliases = {
            "add": ("add", "insert"),
            "remove": ("remove", "delete"),
            "update": ("update", "updates"),
            "upsert": ("upsert", "merge"),
        }
        requested = {
            operation
            for operation, keys in aliases.items()
            if any(payload.get(key) is not None for key in keys)
        }
        if isinstance(payload.get("operations"), list):
            requested.update(
                str(item.get("kind") or "").strip().lower()
                for item in payload["operations"]
                if isinstance(item, Mapping)
            )
        return frozenset(requested or {"update"})

    def authorize(
        self,
        *,
        principal: Any,
        table: str,
        kind: str,
        payload: Mapping[str, Any],
        row_scope: Any,
    ) -> MutationGrant:
        access = self._table_permissions.get(table) or self._table_permissions.get("*")
        if access is None:
            raise RuntimeMutationAuthorizationError(
                "Mutations are disabled for this dataset."
            )
        role = self._role(principal)
        if not role or role not in access.roles:
            raise RuntimeMutationAuthorizationError()
        requested_operations = self._requested_operations(kind, payload)
        if not requested_operations.issubset(access.operations):
            raise RuntimeMutationAuthorizationError()
        scope_table = str(getattr(row_scope, "table", "") or "").strip()
        scope_subject = str(getattr(row_scope, "subject", "") or "").strip()
        scope_values = getattr(row_scope, "field_values", None)
        if (
            scope_table != str(table).strip()
            or scope_subject != self._subject(principal)
            or not isinstance(scope_values, Mapping)
        ):
            raise RuntimeMutationAuthorizationError()
        try:
            constraints = MappingProxyType(
                {
                    str(field): _constraint_values(values)
                    for field, values in scope_values.items()
                    if str(field).strip()
                }
            )
        except RuntimeMutationAuthorizationError:
            raise
        except Exception as exc:
            raise RuntimeMutationAuthorizationError() from exc
        return MutationGrant(
            table=table,
            subject=self._subject(principal),
            row_constraints=constraints,
            allowed_columns=access.columns,
            allowed_operations=access.operations,
        )


__all__ = [
    "MutationGrant",
    "MutationTableAccess",
    "RuntimeMutationAuthorizationError",
    "RuntimeMutationAuthorizer",
]
