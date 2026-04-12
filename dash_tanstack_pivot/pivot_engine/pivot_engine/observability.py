"""
observability.py - Structured logging and metrics configuration
"""
import logging
import sys

try:
    import structlog
    import structlog.stdlib
except ImportError:  # pragma: no cover - optional dependency
    structlog = None

try:
    from fastapi import FastAPI
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = object

try:
    from prometheus_fastapi_instrumentator import Instrumentator
except ImportError:  # pragma: no cover - optional dependency
    Instrumentator = None

def setup_logging():
    """Configure structured JSON logging"""
    if structlog is None:
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=logging.INFO,
        )
        return None

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO
    )

def setup_metrics(app: FastAPI):
    """Setup Prometheus metrics"""
    if Instrumentator is None:
        return None
    Instrumentator().instrument(app).expose(app)
