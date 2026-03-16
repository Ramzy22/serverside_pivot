"""
Apache Arrow Flight server for the pivot engine.
"""
import json
import threading
import time
from typing import Generator

import pyarrow as pa
import pyarrow.flight as fl

from .controller import PivotController

class PivotFlightServer(fl.FlightServerBase):
    """
    A Flight server that exposes the pivot engine's functionality.
    """

    def __init__(self, controller: PivotController, location: str = "grpc://0.0.0.0:8080", **kwargs):
        """
        Initialize the server.
        
        Args:
            controller: A PivotController instance.
            location: The location to host the server on.
        """
        super().__init__(location, **kwargs)
        self._location = location
        self._controller = controller

    def do_action(self, context: fl.ServerCallContext, action: fl.Action) -> Generator[fl.Result, None, None]:
        """
        Perform a custom action. We use this to trigger a pivot query.
        """
        if action.type == "pivot":
            spec = json.loads(action.body.to_pybytes())
            
            # The result is a PyArrow Table
            table = self._controller.run_pivot_arrow(spec)
            
            # Send the result back to the client as a single-item stream.
            # The body is a buffer containing the table's schema.
            yield fl.Result(pa.py_buffer(table.schema.serialize()))
        
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
        # The ticket body is expected to be the serialized spec
        spec = json.loads(ticket.ticket)
        
        # Re-run the pivot query to get the Arrow table
        table = self._controller.run_pivot_arrow(spec)
        
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