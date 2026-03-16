"""
main.py - Main entry point for the Scalable Pivot Engine
"""
import uvicorn
import os
from pivot_engine.config import get_config
from pivot_engine.complete_rest_api import create_complete_api, create_realtime_api

def main():
    """
    Start the Scalable Pivot Engine server.
    """
    config = get_config()
    
    # Initialize the application with real-time support
    api = create_realtime_api()
    app = api.get_app()
    
    print(f"Starting Pivot Engine on port {8000}...")
    print(f"Backend: {config.backend_type} ({config.backend_uri})")
    print(f"Features: Streaming={config.enable_streaming}, CDC={config.enable_cdc}")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
