"""
Apache Arrow Flight server for the pivot engine.
"""
import hmac
import json
import os
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Generator, Mapping, Optional, Protocol

import pyarrow as pa
import pyarrow.flight as fl

from .controller import PivotController
from .production_security import resolve_runtime_environment
from .runtime.dataset_authorization import (
    RuntimeDatasetAuthorizationError,
    RuntimeDatasetAuthorizer,
)


class FlightPrincipalResolver(Protocol):
    """Host-neutral resolver for one Flight request's normalized metadata."""

    def resolve(self, headers: Mapping[str, tuple[str, ...]]) -> Any: ...


class _PrincipalMiddleware(fl.ServerMiddleware):
    """Carries the host-resolved principal for the lifetime of one call."""

    def __init__(self, principal: Any):
        self.principal = principal


class _PrincipalMiddlewareFactory(fl.ServerMiddlewareFactory):
    """Authenticate Flight metadata through a host-provided principal resolver."""

    def __init__(
        self,
        principal_resolver: FlightPrincipalResolver | Callable[[str], Any],
    ):
        self.principal_resolver = principal_resolver

    def start_call(self, info, headers):
        normalized_headers: dict[str, tuple[str, ...]] = {}
        for raw_name, raw_values in (headers or {}).items():
            name = (
                raw_name.decode("utf-8")
                if isinstance(raw_name, bytes)
                else str(raw_name)
            ).strip().lower()
            if not name:
                continue
            values = raw_values if isinstance(raw_values, (list, tuple)) else [raw_values]
            normalized_headers[name] = tuple(
                value.decode("utf-8") if isinstance(value, bytes) else str(value)
                for value in values
                if value is not None
            )

        resolver_method = getattr(self.principal_resolver, "resolve", None)
        if callable(resolver_method):
            try:
                principal = resolver_method(normalized_headers)
            except Exception as exc:
                raise fl.FlightUnauthorizedError("Invalid or missing Flight credentials.") from exc
            if principal is not None:
                return _PrincipalMiddleware(principal=principal)
            raise fl.FlightUnauthorizedError("Invalid or missing Flight credentials.")

        # Retain a narrow adapter for existing Python hosts while new hosts use
        # ``FlightPrincipalResolver.resolve(headers)``. A shared token is never
        # accepted by PivotFlightServer itself.
        candidates = (
            *normalized_headers.get("authorization", ()),
            *normalized_headers.get("x-pivot-flight-token", ()),
        )
        for candidate in candidates:
            token = candidate[7:].strip() if candidate.lower().startswith("bearer ") else candidate.strip()
            if not token:
                continue
            try:
                principal = self.principal_resolver(token)
            except Exception:  # nosec B112
                continue
            if principal is not None:
                return _PrincipalMiddleware(principal=principal)
        raise fl.FlightUnauthorizedError("Invalid or missing Flight credentials.")


@dataclass(frozen=True)
class _FlightResultCacheEntry:
    table: pa.Table
    expires_at: float
    table_name: Optional[str] = None
    owner_subject: Optional[str] = None
    scope_fingerprint: Optional[str] = None


class PivotFlightServer(fl.FlightServerBase):
    """
    A Flight server that exposes the pivot engine's functionality.
    """

    def __init__(
        self,
        controller: PivotController,
        location: str = "grpc://0.0.0.0:8080",
        *,
        auth_token: Optional[str] = None,
        principal_resolver: Optional[Callable[[str], Any]] = None,
        dataset_authorizer: Optional[RuntimeDatasetAuthorizer] = None,
        require_auth: Optional[bool] = None,
        result_ttl_seconds: float = 300.0,
        max_cached_results: int = 64,
        **kwargs
    ):
        """
        Initialize the server.
        
        Args:
            controller: A PivotController instance.
            location: The location to host the server on.
        """
        production = resolve_runtime_environment() == "production"
        if require_auth is None:
            require_auth = production or (
                os.environ.get("PIVOT_FLIGHT_ALLOW_UNAUTHENTICATED", "").lower()
                not in {"1", "true", "yes"}
            )
        if production and not require_auth:
            raise RuntimeError("Flight authentication cannot be disabled in production.")
        if production and not str(location or "").startswith("grpc+tls://"):
            raise RuntimeError("Production Flight requires a grpc+tls:// location.")
        if production and not kwargs.get("tls_certificates"):
            raise RuntimeError("Production Flight requires TLS certificates.")
        if auth_token is not None:
            raise RuntimeError(
                "Shared PIVOT_FLIGHT_AUTH_TOKEN authentication is not supported. "
                "Configure a per-principal Flight principal_resolver instead."
            )
        if require_auth and (principal_resolver is None or dataset_authorizer is None):
            raise RuntimeError(
                "Flight authentication requires both principal_resolver and "
                "dataset_authorizer. Set require_auth=False only for local development."
            )

        middleware = dict(kwargs.pop("middleware", {}) or {})
        if require_auth:
            middleware["pivot_auth"] = _PrincipalMiddlewareFactory(principal_resolver)

        super().__init__(location, middleware=middleware or None, **kwargs)
        self._location = location
        self._controller = controller
        self._require_auth = bool(require_auth)
        self._dataset_authorizer = dataset_authorizer
        self._result_ttl_seconds = float(result_ttl_seconds)
        self._max_cached_results = int(max_cached_results)
        self._result_cache: OrderedDict[str, _FlightResultCacheEntry] = OrderedDict()
        self._result_cache_lock = threading.RLock()

    def _evict_expired_results(self) -> None:
        now = time.time()
        for ticket_id, entry in list(self._result_cache.items()):
            if entry.expires_at <= now:
                self._result_cache.pop(ticket_id, None)
        while len(self._result_cache) > self._max_cached_results:
            self._result_cache.popitem(last=False)

    @staticmethod
    def _scope_owner(row_scope: Any) -> tuple[str, str]:
        """Return only opaque cache ownership values, never row-claim data."""
        subject = str(getattr(row_scope, "subject", "") or "").strip()
        scope_fingerprint = str(
            getattr(row_scope, "scope_fingerprint", "") or ""
        ).strip()
        if not subject or not scope_fingerprint:
            raise fl.FlightUnauthorizedError("Flight row scope is invalid.")
        return subject, scope_fingerprint

    def _principal_from_context(self, context: fl.ServerCallContext) -> Any:
        if not self._require_auth:
            return None
        middleware_getter = getattr(context, "get_middleware", None)
        middleware = middleware_getter("pivot_auth") if callable(middleware_getter) else None
        principal = getattr(middleware, "principal", None)
        if principal is None:
            raise fl.FlightUnauthorizedError("Flight authentication is required.")
        return principal

    def _authorize_read(self, principal: Any, table_name: str) -> Any:
        if not self._require_auth:
            return None
        try:
            return self._dataset_authorizer.authorize(
                principal=principal,
                table=table_name,
                operation="read",
            )
        except RuntimeDatasetAuthorizationError as exc:
            raise fl.FlightUnauthorizedError("Flight dataset access is not authorized.") from exc

    @staticmethod
    def _filter_references_fields(filter_spec: Any, protected_fields: set[str]) -> bool:
        if not isinstance(filter_spec, dict):
            return False
        if str(filter_spec.get("field") or "") in protected_fields:
            return True
        return any(
            isinstance(condition, dict)
            and str(condition.get("field") or "") in protected_fields
            for condition in (filter_spec.get("conditions") or [])
        )

    def _scope_pivot_spec(self, spec: Any, row_scope: Any) -> tuple[str, dict[str, Any]]:
        if not isinstance(spec, Mapping):
            raise ValueError("Flight pivot action requires an object spec.")
        table_name = str(spec.get("table") or "").strip()
        if self._require_auth and not table_name:
            raise fl.FlightUnauthorizedError("Flight pivot action requires an authorized dataset table.")
        scoped_spec = json.loads(json.dumps(dict(spec), default=str))
        filters = scoped_spec.get("filters") or []
        if not isinstance(filters, list):
            raise ValueError("Flight pivot filters must be a list.")
        if row_scope is None:
            return table_name, scoped_spec
        field_values = getattr(row_scope, "field_values", None)
        if not isinstance(field_values, Mapping):
            raise fl.FlightUnauthorizedError("Flight row scope is invalid.")
        protected_fields = {str(field) for field in field_values}
        scoped_spec["filters"] = [
            filter_spec
            for filter_spec in filters
            if not self._filter_references_fields(filter_spec, protected_fields)
        ]
        for field, values in field_values.items():
            values = list(values)
            scoped_spec["filters"].append(
                {
                    "field": str(field),
                    "op": "in" if len(values) > 1 else "=",
                    "value": values if len(values) > 1 else values[0],
                }
            )
        return table_name, scoped_spec

    def _store_result(
        self,
        table: pa.Table,
        *,
        table_name: Optional[str] = None,
        row_scope: Any = None,
    ) -> str:
        ticket_id = f"pivot-{uuid.uuid4().hex}"
        owner_subject = None
        scope_fingerprint = None
        if row_scope is not None:
            owner_subject, scope_fingerprint = self._scope_owner(row_scope)
        with self._result_cache_lock:
            self._evict_expired_results()
            self._result_cache[ticket_id] = _FlightResultCacheEntry(
                table=table,
                expires_at=time.time() + self._result_ttl_seconds,
                table_name=table_name,
                owner_subject=owner_subject,
                scope_fingerprint=scope_fingerprint,
            )
            self._result_cache.move_to_end(ticket_id)
            self._evict_expired_results()
        return ticket_id

    def _load_result_entry(self, ticket_id: str) -> Optional[_FlightResultCacheEntry]:
        with self._result_cache_lock:
            self._evict_expired_results()
            cached = self._result_cache.get(ticket_id)
            if cached is None:
                return None
            if cached.expires_at <= time.time():
                self._result_cache.pop(ticket_id, None)
                return None
            self._result_cache.move_to_end(ticket_id)
            return cached

    def _load_result(self, ticket_id: str) -> Optional[pa.Table]:
        entry = self._load_result_entry(ticket_id)
        return entry.table if entry is not None else None

    def _load_authorized_result(self, ticket_id: str, principal: Any) -> Optional[pa.Table]:
        entry = self._load_result_entry(ticket_id)
        if entry is None:
            return None
        if entry.table_name is None:
            return entry.table
        current_scope = self._authorize_read(principal, entry.table_name)
        current_subject, current_scope_fingerprint = self._scope_owner(current_scope)
        if (
            not hmac.compare_digest(current_subject, str(entry.owner_subject or ""))
            or not hmac.compare_digest(
                current_scope_fingerprint, str(entry.scope_fingerprint or "")
            )
        ):
            raise fl.FlightUnauthorizedError("Flight ticket is not authorized for this principal.")
        return entry.table

    def do_action(self, context: fl.ServerCallContext, action: fl.Action) -> Generator[fl.Result, None, None]:
        """
        Perform a custom action. We use this to trigger a pivot query.
        """
        if action.type == "pivot":
            spec = json.loads(action.body.to_pybytes())
            principal = self._principal_from_context(context)
            table_name, scoped_spec = self._scope_pivot_spec(spec, None)
            row_scope = self._authorize_read(principal, table_name)
            _table_name, scoped_spec = self._scope_pivot_spec(spec, row_scope)
            
            # The result is a PyArrow Table
            table = self._controller.run_pivot_arrow(scoped_spec)
            ticket_id = self._store_result(
                table,
                table_name=table_name if self._require_auth else None,
                row_scope=row_scope,
            )
            
            payload = {
                "ticket": ticket_id,
                "rows": table.num_rows,
                "columns": table.column_names,
            }
            yield fl.Result(pa.py_buffer(json.dumps(payload).encode("utf-8")))
        
        elif action.type == "clear_cache":
            principal = self._principal_from_context(context)
            if self._require_auth and str(getattr(principal, "role", "")).strip().lower() != "admin":
                raise fl.FlightUnauthorizedError("Flight cache administration is not authorized.")
            # Clear the controller's cache
            if hasattr(self._controller, 'clear_cache'):
                self._controller.clear_cache()
                yield fl.Result(pa.py_buffer(b'{"status": "success", "message": "Cache cleared"}'))
            else:
                 yield fl.Result(pa.py_buffer(b'{"status": "error", "message": "Cache clearing not supported by controller"}'))
        
        elif action.type == "status":
            # Return server status
            yield fl.Result(pa.py_buffer(json.dumps({"status": "running", "location": self._location}).encode("utf-8")))
            
        else:
            raise NotImplementedError(f"Action {action.type} not implemented.")

    def do_get(self, context: fl.ServerCallContext, ticket: fl.Ticket) -> fl.FlightDataStream:
        """
        Get a Flight data stream. This is used by the client to fetch the
        full result of an action.
        """
        ticket_body = ticket.ticket
        try:
            payload = json.loads(ticket_body)
        except Exception:
            payload = ticket_body.decode("utf-8") if isinstance(ticket_body, bytes) else str(ticket_body)

        principal = self._principal_from_context(context)
        table = None
        if isinstance(payload, dict) and payload.get("ticket"):
            table = self._load_authorized_result(str(payload["ticket"]), principal)
        elif isinstance(payload, str):
            table = self._load_authorized_result(payload, principal)

        if table is None:
            raise KeyError("Flight result ticket was not found or has expired.")
        
        # Return the table as a RecordBatchStream
        return fl.RecordBatchStream(table)

    def list_actions(self, context: fl.ServerCallContext) -> Generator[tuple[str, str], None, None]:
        """List available actions."""
        return [
            ("pivot", "Run a pivot query."),
            ("clear_cache", "Clear the query cache."),
            ("status", "Get server status."),
        ]

    def serve(self):
        """
        Start the server. This is a blocking call.
        """
        print(f"Starting Flight server on {self._location}")
        super().serve()

    def shutdown(self):
        """
        Shutdown the server.
        """
        print("Shutting down Flight server.")
        super().shutdown()


__all__ = ["FlightPrincipalResolver", "PivotFlightServer"]
