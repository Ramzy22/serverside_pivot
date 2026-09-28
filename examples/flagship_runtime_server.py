"""Host-neutral Pivot Runtime Protocol V1 service for the published demos.

Run this before the React and Streamlit flagship examples:
``python -m uvicorn examples.flagship_runtime_server:app --port 8765``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pivot_engine import (
    PivotRuntimeService,
    RuntimeDatasetAuthorizer,
    register_runtime_http_transport,
)

from examples.flagship_demo_data import (
    DEMO_ROW_COUNT,
    TABLE_NAME,
    get_demo_adapter,
    workspace_config,
)


@dataclass(frozen=True)
class DemoPrincipal:
    """Explicit local-only identity used by this unauthenticated sample."""

    id: str = "flagship-demo"
    role: str = "viewer"
    attributes: dict[str, Any] = field(default_factory=dict)


def demo_principal() -> DemoPrincipal:
    return DemoPrincipal()


runtime_service = PivotRuntimeService(adapter_getter=get_demo_adapter)
app = FastAPI(title="serverside-pivot flagship runtime", version="1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5174",
        "http://localhost:5174",
        "http://127.0.0.1:8502",
        "http://localhost:8502",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Keep demonstration row windows inline. Production applications can use the
# default payload references, which are user-scoped and ETag-aware.
register_runtime_http_transport(
    app,
    lambda: runtime_service,
    current_user_dependency=demo_principal,
    dataset_authorizer=RuntimeDatasetAuthorizer(
        {
            TABLE_NAME: {
                "roles": {"viewer"},
                "operations": {"read", "export"},
                "rowSecurity": {"mode": "global"},
            }
        }
    ),
    inline_payload_bytes=5_000_000,
)


@app.get("/v1/workspace-config")
async def react_workspace_config() -> dict[str, Any]:
    """Deliver the same public workspace contract used by the other demos."""
    return workspace_config("react")


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "protocol": "Pivot Runtime Protocol V1",
        "table": TABLE_NAME,
        "rows": DEMO_ROW_COUNT,
    }
