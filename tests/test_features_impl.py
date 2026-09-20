import pytest
import asyncio
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
import os

# Set API Key for testing
os.environ["PIVOT_API_KEY"] = "test-secret-key"

from pivot_engine.complete_rest_api import create_complete_api, CompletePivotAPI
from pivot_engine.runtime import RuntimeDatasetAuthorizer
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import PivotSpec

@pytest.fixture
def mock_controller():
    controller = MagicMock(spec=ScalablePivotController)
    controller.run_pivot_async = AsyncMock(return_value={"columns": [], "rows": []})
    controller.run_pivot_export = MagicMock()
    controller.setup_push_cdc = AsyncMock()
    controller.push_change_event = AsyncMock()
    return controller

@pytest.fixture
def api_client(mock_controller):
    api = CompletePivotAPI(
        mock_controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {
                "*": {
                    "roles": {"admin"},
                    "operations": {"read", "export", "mutate", "cdc", "schema", "subscribe"},
                    "rowSecurity": {"mode": "global"},
                }
            }
        ),
    )
    return TestClient(api.get_app())

def test_security_missing_key(api_client):
    """Test that requests without API key are rejected"""
    # Temporarily unset env var or just don't send header
    # Since we set env var above, the server expects it.
    # Client doesn't send it.
    response = api_client.get("/health")
    # If env var is set, get_api_key checks header.
    assert response.status_code == 401 # Unauthorized

def test_security_valid_key(api_client):
    """Test that requests with valid API key are accepted"""
    response = api_client.get("/health", headers={"X-API-Key": "test-secret-key"})
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_export_endpoint(api_client, mock_controller):
    """Test the data export endpoint"""
    async def mock_stream():
        yield b"col1,col2\n"
        yield b"1,2"

    mock_controller.run_pivot_export.return_value = mock_stream()
    
    payload = {
        "spec": {
            "table": "test_table",
            "measures": [{"field": "amount", "agg": "sum", "alias": "total"}]
        },
        "format": "csv"
    }
    
    response = api_client.post(
        "/pivot/export", 
        json=payload,
        headers={"X-API-Key": "test-secret-key"}
    )
    
    assert response.status_code == 200
    assert response.content == b"col1,col2\n1,2"
    assert "text/csv" in response.headers["content-type"]
    mock_controller.run_pivot_export.assert_called_once()


def test_export_endpoint_streams_parquet_with_binary_media_type(api_client, mock_controller):
    async def mock_stream():
        yield b"PAR1"

    mock_controller.run_pivot_export.return_value = mock_stream()
    payload = {
        "spec": {
            "table": "test_table",
            "measures": [{"field": "amount", "agg": "sum", "alias": "total"}],
        },
        "format": "PARQUET",
    }

    response = api_client.post(
        "/pivot/export",
        json=payload,
        headers={"X-API-Key": "test-secret-key"},
    )

    assert response.status_code == 200
    assert response.content == b"PAR1"
    assert response.headers["content-type"] == "application/vnd.apache.parquet"
    assert response.headers["content-disposition"] == "attachment; filename=export.parquet"
    mock_controller.run_pivot_export.assert_called_once()


@pytest.mark.parametrize("export_format", ["xlsx", "html", "CSVV", ""])
def test_export_endpoint_rejects_unsupported_formats(api_client, mock_controller, export_format):
    payload = {
        "spec": {
            "table": "test_table",
            "measures": [{"field": "amount", "agg": "sum", "alias": "total"}],
        },
        "format": export_format,
    }

    response = api_client.post(
        "/pivot/export",
        json=payload,
        headers={"X-API-Key": "test-secret-key"},
    )

    assert response.status_code == 400
    assert "csv" in response.json()["detail"]
    assert "parquet" in response.json()["detail"]
    mock_controller.run_pivot_export.assert_not_called()

def test_push_cdc_setup(api_client, mock_controller):
    """Test setting up push CDC"""
    payload = {"table_name": "test_table"}
    response = api_client.post(
        "/pivot/cdc/push-setup",
        json=payload,
        headers={"X-API-Key": "test-secret-key"}
    )
    
    assert response.status_code == 200
    mock_controller.setup_push_cdc.assert_called_with("test_table")

def test_push_cdc_event(api_client, mock_controller):
    """Test pushing a change event"""
    payload = {
        "type": "INSERT",
        "new_row": {"id": 1, "val": "test"}
    }
    
    response = api_client.post(
        "/pivot/cdc/push/test_table",
        json=payload,
        headers={"X-API-Key": "test-secret-key"}
    )
    
    assert response.status_code == 200
    mock_controller.push_change_event.assert_called_once()


def test_incomplete_push_is_not_advertised_as_exact_cdc(api_client, mock_controller):
    mock_controller.push_change_event.return_value = "accepted_invalidating"

    response = api_client.post(
        "/pivot/cdc/push/test_table",
        json={"type": "DELETE", "old_row": {"id": 1}},
        headers={"X-API-Key": "test-secret-key"},
    )

    assert response.status_code == 200
    assert response.json()["data"]["mode"] == "accepted_invalidating"

