"""
observability.py - Structured logging and metrics configuration
"""
import structlog
import logging
import sys
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi import FastAPI

def setup_logging():
    """Configure structured JSON logging"""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ],
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Redirect standard logging to structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO
    )

def setup_metrics(app: FastAPI):
    """Setup Prometheus metrics"""
    Instrumentator().instrument(app).expose(app)
