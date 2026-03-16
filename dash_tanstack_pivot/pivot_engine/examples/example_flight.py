"""
Example usage of the Arrow Flight server and client.

This script starts a server, runs a client to query it, and then shuts down.
"""
import os
import json
import time
import threading

import pyarrow as pa
import pyarrow.flight as fl

from pivot_engine.controller import PivotController
from pivot_engine.flight_server import PivotFlightServer

def run_client(location: str, spec: dict):
    """
    Connects to the Flight server, runs a pivot query, and prints the result.
    """
    # Give the server a moment to start up
    time.sleep(1)
    
    try:
        # Connect to the server
        client = fl.connect(location)
        print("\nClient connected to server.")

        # Check available actions
        print("Available actions:", [action.type for action in client.list_actions()])

        # Prepare the pivot action
        action_body = json.dumps(spec).encode('utf-8')
        action = fl.Action("pivot", action_body)

        print("\nClient: Sending 'pivot' action with spec:")
        print(json.dumps(spec, indent=2))

        # Trigger the action. The result is not the data itself, but a "ticket"
        # that can be used to retrieve the data stream.
        # In our simple implementation, the result stream is empty, but we could
        # pass back metadata like the schema in a real app.
        for result in client.do_action(action):
             print(f"Client: Received result from action: {result.body.to_pybytes()}")


        # Now, create a ticket to get the actual data
        # The ticket is just the spec itself, serialized
        ticket = fl.Ticket(json.dumps(spec).encode('utf-8'))

        # Use do_get with the ticket to get the data stream
        reader = client.do_get(ticket)
        
        print("\nClient: Reading data stream...")
        table = reader.read_all()
        
        print("Client: Received table:")
        print(table)
        print(f"\nTotal rows received: {table.num_rows}")

    except Exception as e:
        print(f"An error occurred in the client: {e}")


def main():
    # === Server Setup ===
    db_path = os.path.join(os.path.dirname(__file__), "sales.duckdb")
    controller = PivotController(backend_uri=db_path, planner_name="ibis")
    
    host = "localhost"
    port = 8080
    location = f"grpc://{host}:{port}"
    server = PivotFlightServer(controller, location=location)
    
    # Run the server in a background thread
    server_thread = threading.Thread(target=server.serve)
    server_thread.daemon = True
    server_thread.start()
    print(f"Server started in background thread on {location}")

    # === Client Logic ===
    try:
        spec = {
            "table": "sales",
            "rows": ["region", "product"],
            "measures": [{"field": "sales", "agg": "sum", "alias": "total_sales"}],
            "filters": [{"field": "year", "op": "=", "value": 2024}],
            "sort": [{"field": "total_sales", "order": "desc"}]
        }
        run_client(location, spec)
    finally:
        # === Server Shutdown ===
        print("\nShutting down server...")
        server.shutdown()
        # Wait for the server thread to finish
        server_thread.join(timeout=2.0)
        print("Server shut down.")

if __name__ == "__main__":
    main()
