"""
Apache Arrow Flight server for the pivot engine.
"""
import json
import os
import threading
import time
import uuid
from collections import OrderedDict
from typing import Generator, Optional

import pyarrow as pa
import pyarrow.flight as fl

from .controller import PivotController


class _BearerTokenMiddleware(fl.ServerMiddleware):
    def __init__(self, authorized: bool):
        self.authorized = authorized


class _BearerTokenMiddlewareFactory(fl.ServerMiddlewareFactory):
    def __init__(self, expected_token: str):
        self.expected_token = expected_token

    def start_call(self, info, headers):
        candidates = []
        for name in ("authorization", "Authorization"):
            value = headers.get(name)
            if isinstance(value, list):
                candidates.extend(value)
            elif value is not None:
                candidates.append(value)
        for name in ("x-pivot-flight-token", "X-Pivot-Flight-Token"):
            value = headers.get(name)
            if isinstance(value, list):
                candidates.extend(value)
            elif value is not None:
                candidates.append(value)

        normalized_candidates = [
            token.decode("utf-8") if isinstance(token, bytes) else str(token)
            for token in candidates
        ]
        expected_bearer = f"Bearer {self.expected_token}"
        authorized = any(token in {self.expected_token, expected_bearer} for token in normalized_candidates)
        if not authorized:
            raise fl.FlightUnauthorizedError("Invalid or missing Flight auth token.")
        return _BearerTokenMiddleware(authorized=True)


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
        if auth_token is None:
            auth_token = os.environ.get("PIVOT_FLIGHT_AUTH_TOKEN")
        if require_auth is None:
            require_auth = os.environ.get("PIVOT_FLIGHT_ALLOW_UNAUTHENTICATED", "").lower() not in {"1", "true", "yes"}
        if require_auth and not auth_token:
            raise RuntimeError(
                "PIVOT_FLIGHT_AUTH_TOKEN must be set for Flight server authentication. "
                "Set PIVOT_FLIGHT_ALLOW_UNAUTHENTICATED=1 only for local development."
            )

        middleware = dict(kwargs.pop("middleware", {}) or {})
        if require_auth:
            middleware["pivot_auth"] = _BearerTokenMiddlewareFactory(str(auth_token))

        super().__init__(location, middleware=middleware or None, **kwargs)
        self._location = location
        self._controller = controller
        self._result_ttl_seconds = float(result_ttl_seconds)
        self._max_cached_results = int(max_cached_results)
        self._result_cache: OrderedDict[str, tuple[pa.Table, float]] = OrderedDict()
        self._result_cache_lock = threading.RLock()

    def _evict_expired_results(self) -> None:
        now = time.time()
        for ticket_id, (_table, expires_at) in list(self._result_cache.items()):
            if expires_at <= now:
                self._result_cache.pop(ticket_id, None)
        while len(self._result_cache) > self._max_cached_results:
            self._result_cache.popitem(last=False)

    def _store_result(self, table: pa.Table) -> str:
        ticket_id = f"pivot-{uuid.uuid4().hex}"
        with self._result_cache_lock:
            self._evict_expired_results()
            self._result_cache[ticket_id] = (table, time.time() + self._result_ttl_seconds)
            self._result_cache.move_to_end(ticket_id)
            self._evict_expired_results()
        return ticket_id

    def _load_result(self, ticket_id: str) -> Optional[pa.Table]:
        with self._result_cache_lock:
            self._evict_expired_results()
            cached = self._result_cache.get(ticket_id)
            if cached is None:
                return None
            table, expires_at = cached
            if expires_at <= time.time():
                self._result_cache.pop(ticket_id, None)
                return None
            self._result_cache.move_to_end(ticket_id)
            return table

    def do_action(self, context: fl.ServerCallContext, action: fl.Action) -> Generator[fl.Result, None, None]:
        """
        Perform a custom action. We use this to trigger a pivot query.
        """
        if action.type == "pivot":
            spec = json.loads(action.body.to_pybytes())
            
            # The result is a PyArrow Table
            table = self._controller.run_pivot_arrow(spec)
            ticket_id = self._store_result(table)
            
            payload = {
                "ticket": ticket_id,
                "rows": table.num_rows,
                "columns": table.column_names,
            }
            yield fl.Result(pa.py_buffer(json.dumps(payload).encode("utf-8")))
        
        elif action.type == "clear_cache":
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

        table = None
        if isinstance(payload, dict) and payload.get("ticket"):
            table = self._load_result(str(payload["ticket"]))
        elif isinstance(payload, str):
            table = self._load_result(payload)

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
