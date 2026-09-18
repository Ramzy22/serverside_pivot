"""Security regressions for the real-time pivot WebSocket boundary."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from pivot_engine.complete_rest_api import RealTimePivotAPI, _RealtimeConnection
from pivot_engine.runtime import RuntimeDatasetAuthorizer
from pivot_engine.security import User


class _Socket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_text(self, message: str) -> None:
        self.messages.append(message)


def _api() -> RealTimePivotAPI:
    return RealTimePivotAPI(
        MagicMock(),
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "sales": {
                    "roles": {"admin", "viewer"},
                    "operations": {"read", "subscribe", "cdc"},
                    "rowSecurity": {
                        "mode": "claims",
                        "claims": {"tenant": "tenant"},
                    },
                }
            }
        ),
    )


def _user(*, user_id: str, tenant: str, role: str = "viewer") -> User:
    return User(
        id=user_id,
        username=user_id,
        role=role,
        attributes={"tenant": tenant},
    )


def test_websocket_rejects_unauthenticated_connections():
    api = _api()
    client = TestClient(api.get_app())

    with pytest.raises(WebSocketDisconnect) as disconnect:
        with client.websocket_connect("/ws/pivot/anonymous"):
            pass

    assert getattr(disconnect.value, "code", None) in {4401, None}
    assert getattr(disconnect.value, "status_code", None) in {None, 401}
    assert api.active_connections == {}


def test_authenticated_websocket_can_subscribe_only_after_handshake_auth(monkeypatch):
    monkeypatch.setenv("PIVOT_API_KEY", "realtime-test-key")
    api = RealTimePivotAPI(
        MagicMock(),
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "sales": {
                    "roles": {"admin"},
                    "operations": {"subscribe"},
                    "rowSecurity": {"mode": "global"},
                }
            }
        ),
    )
    client = TestClient(api.get_app())

    with client.websocket_connect(
        "/ws/pivot/admin",
        headers={"X-API-Key": "realtime-test-key"},
    ) as socket:
        socket.send_json({"type": "subscribe", "table_name": "sales", "subscription_id": "sales"})
        assert socket.receive_json() == {
            "type": "subscription_ack",
            "subscription_id": "sales",
            "table": "sales",
            "status": "subscribed",
        }


def test_subscription_requires_explicit_dataset_subscribe_permission():
    api = RealTimePivotAPI(
        MagicMock(),
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
    socket = _Socket()
    connection = _RealtimeConnection(
        websocket=socket,
        principal=_user(user_id="tenant-a", tenant="A"),
    )

    asyncio.run(
        api._handle_subscription(
            connection,
            {"type": "subscribe", "table_name": "sales", "subscription_id": "sales-a"},
        )
    )

    assert connection.subscriptions == set()
    message = json.loads(socket.messages[-1])
    assert message["type"] == "subscription_error"
    assert message["status"] == "denied"


def test_broadcasts_are_subscription_and_tenant_scoped_and_never_include_rows():
    api = _api()
    a_socket = _Socket()
    b_socket = _Socket()
    unrelated_socket = _Socket()
    a_principal = _user(user_id="tenant-a", tenant="A")
    b_principal = _user(user_id="tenant-b", tenant="B")
    api.active_connections = {
        "tenant-a": _RealtimeConnection(
            websocket=a_socket,
            principal=a_principal,
            subscriptions={"sales"},
            subscription_scopes={"sales": api._subscription_scope(a_principal, "sales")},
        ),
        "tenant-b": _RealtimeConnection(
            websocket=b_socket,
            principal=b_principal,
            subscriptions={"sales"},
            subscription_scopes={"sales": api._subscription_scope(b_principal, "sales")},
        ),
        "other-table": _RealtimeConnection(
            websocket=unrelated_socket,
            principal=_user(user_id="other", tenant="A"),
            subscriptions={"inventory"},
        ),
    }

    asyncio.run(
        api.broadcast_data_update(
            "sales",
            {
                "type": "change",
                "change": {
                    "type": "UPDATE",
                    "old_row": {"tenant": "A", "region": "North", "sales": 100},
                    "new_row": {"tenant": "A", "region": "North", "sales": 999},
                },
            },
        )
    )

    assert len(a_socket.messages) == 1
    assert b_socket.messages == []
    assert unrelated_socket.messages == []
    message = json.loads(a_socket.messages[0])
    assert message["type"] == "data_update"
    assert message["table"] == "sales"
    assert message["data"] == {"type": "invalidated"}
    assert "new_row" not in a_socket.messages[0]
    assert "old_row" not in a_socket.messages[0]
