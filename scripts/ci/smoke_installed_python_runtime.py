"""Exercise an installed wheel's runtime route and packaged Streamlit assets."""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine.runtime import (
    PivotServiceResponse,
    RuntimeDatasetAuthorizer,
    register_runtime_http_transport,
)
from streamlit_pivot import _component_script


class _InstalledRuntimeService:
    async def process_async(self, *_args, **_kwargs) -> PivotServiceResponse:
        return PivotServiceResponse(
            status="data",
            data=[{"region": "North", "sales": 100}],
            total_rows=1,
        )


async def _principal() -> SimpleNamespace:
    return SimpleNamespace(id="wheel-smoke", role="viewer", attributes={})


def main() -> int:
    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: _InstalledRuntimeService(),
        current_user_dependency=_principal,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "sales": {
                    "roles": {"viewer"},
                    "operations": {"read"},
                    "rowSecurity": {"mode": "global"},
                }
            }
        ),
    )
    response = TestClient(app).post(
        "/v1/runtime",
        json={
            "schemaVersion": 1,
            "kind": "data",
            "requestId": "installed-wheel-request",
            "table": "sales",
            "payload": {"intent": "viewport", "start": 0, "end": 0},
        },
    )
    if response.status_code != 200 or response.json().get("payload", {}).get("data") != [
        {"region": "North", "sales": 100}
    ]:
        raise RuntimeError(f"Installed wheel runtime request failed: {response.status_code} {response.text}")
    entry = _component_script()
    if not entry.startswith("pivot-workspace-") or not entry.endswith(".js"):
        raise RuntimeError(f"Installed Streamlit asset entry is invalid: {entry!r}")
    print("installed-python-runtime-smoke: runtime request and Streamlit assets OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
