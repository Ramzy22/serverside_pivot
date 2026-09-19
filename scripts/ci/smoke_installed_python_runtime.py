"""Exercise an installed wheel's runtime route and packaged Streamlit assets."""

from __future__ import annotations

import os
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine.runtime import (
    PivotServiceResponse,
    RuntimeDatasetAuthorizer,
    register_runtime_http_transport,
)
from pivot_engine.runtime.distributed import DistributedRuntimeSettings, RedisRuntimeState
from pivot_engine.runtime.payload_store import RedisPayloadBackend, RuntimePayloadStore
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


def _dataset_policy() -> RuntimeDatasetAuthorizer:
    return RuntimeDatasetAuthorizer(
        {
            "sales": {
                "roles": {"viewer"},
                "operations": {"read"},
                "rowSecurity": {"mode": "global"},
            }
        }
    )


def _runtime_request(request_id: str) -> dict:
    return {
        "schemaVersion": 1,
        "kind": "data",
        "requestId": request_id,
        "table": "sales",
        "payload": {"intent": "viewport", "start": 0, "end": 0},
    }


def _distributed_payload_handoff_smoke() -> bool:
    redis_url = os.environ.get("PIVOT_TEST_REDIS_URL")
    if not redis_url:
        print("installed-python-runtime-smoke: cross-worker Redis handoff skipped")
        return False

    import redis

    prefix = f"wheel-smoke:{os.getpid()}"
    settings = DistributedRuntimeSettings(
        enabled=True,
        # The integration client is injected below. Production configuration
        # remains rediss-only and is validated before creating a client.
        redis_url="rediss://wheel-smoke:placeholder@redis.invalid/15",
        key_prefix=prefix,
        payload_ttl_seconds=30,
        payload_max_bytes=1024 * 1024,
    )
    producer_client = redis.Redis.from_url(redis_url, decode_responses=False)
    consumer_client = redis.Redis.from_url(redis_url, decode_responses=False)
    producer_state = RedisRuntimeState(settings, client=producer_client)
    consumer_state = RedisRuntimeState(settings, client=consumer_client)
    producer_store = RuntimePayloadStore(backend=RedisPayloadBackend(producer_state))
    consumer_store = RuntimePayloadStore(backend=RedisPayloadBackend(consumer_state))
    active_principal = {"value": SimpleNamespace(id="wheel-smoke", role="viewer", attributes={})}

    async def current_principal() -> SimpleNamespace:
        return active_principal["value"]

    producer_app = FastAPI()
    register_runtime_http_transport(
        producer_app,
        lambda: _InstalledRuntimeService(),
        current_user_dependency=current_principal,
        dataset_authorizer=_dataset_policy(),
        payload_store=producer_store,
        inline_payload_bytes=1,
    )
    consumer_app = FastAPI()
    register_runtime_http_transport(
        consumer_app,
        lambda: _InstalledRuntimeService(),
        current_user_dependency=current_principal,
        dataset_authorizer=_dataset_policy(),
        payload_store=consumer_store,
        inline_payload_bytes=1,
    )

    producer_response = TestClient(producer_app).post(
        "/v1/runtime", json=_runtime_request("installed-wheel-shared-payload")
    )
    if producer_response.status_code != 200:
        raise RuntimeError(
            "Installed wheel producer request failed: "
            f"{producer_response.status_code} {producer_response.text}"
        )
    reference = producer_response.json().get("payload", {}).get("payloadRef", {})
    payload_url = reference.get("url") if isinstance(reference, dict) else None
    if not isinstance(payload_url, str) or not payload_url:
        raise RuntimeError("Installed wheel did not externalize the shared payload.")
    consumer_response = TestClient(consumer_app).get(payload_url)
    if consumer_response.status_code != 200:
        raise RuntimeError(
            "Installed wheel consumer could not retrieve the producer payload: "
            f"{consumer_response.status_code} {consumer_response.text}"
        )
    active_principal["value"] = SimpleNamespace(
        id="wheel-smoke-other", role="viewer", attributes={}
    )
    if TestClient(consumer_app).get(payload_url).status_code != 404:
        raise RuntimeError("Installed wheel shared payload was not principal-bound.")

    for client in (producer_client, consumer_client):
        keys = list(client.scan_iter(match=f"{prefix}:*"))
        if keys:
            client.delete(*keys)
    producer_state.close()
    consumer_state.close()
    return True


def main() -> int:
    app = FastAPI()
    register_runtime_http_transport(
        app,
        lambda: _InstalledRuntimeService(),
        current_user_dependency=_principal,
        dataset_authorizer=_dataset_policy(),
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
    handoff_verified = _distributed_payload_handoff_smoke()
    message = "installed-python-runtime-smoke: runtime request and Streamlit assets OK"
    if handoff_verified:
        message += "; cross-worker Redis payload handoff OK"
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
