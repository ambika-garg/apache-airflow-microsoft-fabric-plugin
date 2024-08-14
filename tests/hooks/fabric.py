from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
import requests

from airflow.models.connection import Connection
from apache_airflow_microsoft_fabric_plugin.hooks.fabric import (
    FabricAsyncHook,
    FabricHook,
    FabricRunItemException,
    FabricRunItemStatus,
)

DEFAULT_FABRIC_CONNECTION = "fabric_default"
ITEM_RUN_LOCATION = "https://api.fabric.microsoft.com/v1/workspaces/4b218778-e7a5-4d73-8187-f10824047715/items/431e8d7b-4a95-4c02-8ccd-6faef5ba1bd7/jobs/instances/f2d65699-dd22-4889-980c-15226deb0e1b"
WORKSPACE_ID = "workspace_id"
ITEM_ID = "item_id"
ITEM_RUN_ID = "item_run_id"
BASE_URL = "https://api.fabric.microsoft.com"
API_VERSION = "v1"
JOB_TYPE = "RunNotebook"
MODULE = "apache_airflow_microsoft_fabric_plugin.hooks.fabric"


@pytest.fixture(autouse=True)
def setup_connections(create_mock_connection):
    create_mock_connection(
        Connection(
            conn_id=DEFAULT_FABRIC_CONNECTION,
            conn_type="generic",
            login="clientId",
            password="userRefreshToken",
            extra={
                "tenantId": "tenantId",
            },
        )
    )


@pytest.fixture
def fabric_hook():
    client = FabricHook(fabric_conn_id=DEFAULT_FABRIC_CONNECTION)
    return client


@pytest.fixture
def get_token(fabric_hook):
    fabric_hook._get_token = MagicMock(return_value="access_token")
    return fabric_hook._get_token


def test_get_headers(get_token, fabric_hook):
    headers = fabric_hook.get_headers()
    assert isinstance(headers, dict)
    assert "Authorization" in headers
    assert headers["Authorization"] == "Bearer access_token"


def test_get_item_run_details_success(fabric_hook, get_token, mocker):
    # Mock response for successful response from _send_request
    response = MagicMock()
    response.ok = True
    response.json.return_value = {"status": "Completed"}

    mocker.patch.object(fabric_hook, "_send_request", return_value=response)
    mocker.patch.object(fabric_hook, "get_headers", return_value={"Authorization": "Bearer access_token"})

    result = fabric_hook.get_item_run_details(location=ITEM_RUN_LOCATION)

    assert result == {"status": "Completed"}
    fabric_hook.get_headers.assert_called_once()
    fabric_hook._send_request.assert_called_once_with(
        "GET", ITEM_RUN_LOCATION, headers={"Authorization": f"Bearer {get_token.return_value}"}
    )


def test_get_item_run_details_failure(fabric_hook, get_token, mocker):
    # Mock response for failed response from _send_request
    response = MagicMock()
    response.ok = False
    response.raise_for_status.side_effect = requests.exceptions.HTTPError("Error")

    mocker.patch.object(fabric_hook, "_send_request", return_value=response)
    mocker.patch.object(fabric_hook, "get_headers", return_value={"Authorization": "Bearer access_token"})

    with pytest.raises(requests.exceptions.HTTPError):
        fabric_hook.get_item_run_details(location=ITEM_RUN_LOCATION)

    fabric_hook.get_headers.assert_called_once()
    fabric_hook._send_request.assert_called_once_with(
        "GET", ITEM_RUN_LOCATION, headers={"Authorization": f"Bearer {get_token.return_value}"}
    )


@patch(f"{MODULE}.FabricHook._send_request")
def test_get_item_details(mock_send_request, fabric_hook, get_token):
    fabric_hook.get_item_details(WORKSPACE_ID, ITEM_ID)
    expected_url = f"{BASE_URL}/{API_VERSION}/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}"
    mock_send_request.assert_called_once_with(
        "GET", expected_url, headers={"Authorization": f"Bearer {get_token.return_value}"}
    )


@patch(f"{MODULE}.FabricHook._send_request")
def test_run_fabric_item(mock_send_request, fabric_hook, get_token):
    fabric_hook.run_fabric_item(WORKSPACE_ID, ITEM_ID, JOB_TYPE)
    expected_url = f"{BASE_URL}/{API_VERSION}/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/jobs/instances?jobType={JOB_TYPE}"
    mock_send_request.assert_called_once_with(
        "POST", expected_url, headers={"Authorization": f"Bearer {get_token.return_value}"}
    )

_wait_for_item_run_status_test_args = [
    (FabricRunItemStatus.COMPLETED, FabricRunItemStatus.COMPLETED, True),
    (FabricRunItemStatus.FAILED, FabricRunItemStatus.COMPLETED, False),
    (FabricRunItemStatus.IN_PROGRESS, FabricRunItemStatus.COMPLETED, "timeout"),
    (FabricRunItemStatus.NOT_STARTED, FabricRunItemStatus.COMPLETED, "timeout"),
    (FabricRunItemStatus.CANCELLED, FabricRunItemStatus.COMPLETED, False)
]

@pytest.mark.parametrize(
    argnames=("item_run_status", "expected_status", "expected_result"),
    argvalues=_wait_for_item_run_status_test_args,
    ids=[
        f"run_status_{argval[0]}_expected_{argval[1]}"
        if isinstance(argval[1], str)
        else f"run_status_{argval[0]}_expected_AnyTerminalStatus"
        for argval in _wait_for_item_run_status_test_args
    ]
)
def test_wait_for_item_run_status(fabric_hook, item_run_status, expected_status, expected_result):
    config = {
        "location": ITEM_RUN_LOCATION,
        "timeout": 3,
        "check_interval": 1,
        "target_status": expected_status,
    }

    with patch.object(FabricHook, "get_item_run_details") as mock_item_run:
        mock_item_run.return_value = {"status": item_run_status}

        if expected_result != "timeout":
            assert fabric_hook.wait_for_item_run_status(**config) == expected_result
        else:
            with pytest.raises(FabricRunItemException):
                fabric_hook.wait_for_item_run_status(**config)

@patch(f"{MODULE}.FabricHook._send_request")
def test_send_request(mock_send_request, fabric_hook: FabricHook):
    request_type = "GET"
    url = "https://api.fabric.microsoft.com/test"
    fabric_hook._send_request(request_type, url)
    mock_send_request.assert_called_once_with(request_type, url)

@patch(f"{MODULE}.FabricHook._send_request")
def test_send_request_with_custom_headers(mock_send_request, get_token, fabric_hook):
    request_type = "GET"
    url = "https://api.fabric.microsoft.com/test"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {get_token.return_value}"}
    fabric_hook._send_request(request_type, url, headers=headers)
    mock_send_request.assert_called_once_with(
        request_type, url, headers={"Content-Type": "application/json", "Authorization": "Bearer access_token"}
    )

@pytest.fixture
def fabric_async_hook():
    client = FabricAsyncHook(fabric_conn_id=DEFAULT_FABRIC_CONNECTION)
    return client

@pytest.mark.asyncio
@mock.patch(f"{MODULE}.FabricAsyncHook._get_token", return_value="access_token")
async def test_async_get_headers(mock_get_token, fabric_async_hook):
    headers = await fabric_async_hook.get_headers()
    assert isinstance(headers, dict)
    assert "Authorization" in headers
    assert headers["Authorization"] == "Bearer access_token"


@pytest.mark.asyncio
@mock.patch(f"{MODULE}.FabricAsyncHook._get_token", return_value="access_token")
async def test_async_get_item_run_details_success(get_token, fabric_async_hook, mocker):
    # Mock response for successful response from _send_request
    response = MagicMock()
    response.ok = True
    response.json.return_value = {"status": "Completed"}

    mocker.patch.object(fabric_async_hook, "get_headers", return_value={"Authorization": f"Bearer {get_token.return_value}"})
    mocker.patch.object(fabric_async_hook, "_send_request", return_value=response.json.return_value)

    expected_url = f"{BASE_URL}/{API_VERSION}/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/jobs/instances/{ITEM_RUN_ID}"
    result = await fabric_async_hook.get_item_run_details(workspace_id=WORKSPACE_ID, item_id=ITEM_ID, item_run_id=ITEM_RUN_ID)

    assert result == {"status": "Completed"}
    fabric_async_hook.get_headers.assert_called_once()
    fabric_async_hook._send_request.assert_called_once_with(
        "GET", expected_url, headers={"Authorization": "Bearer access_token"}
    )

@pytest.mark.asyncio
@patch(f"{MODULE}.FabricAsyncHook._send_request")
@mock.patch(f"{MODULE}.FabricAsyncHook._get_token", return_value="access_token")
async def test_async_cancel_item_run(get_token, mock_send_request, fabric_async_hook):
    await fabric_async_hook.cancel_item_run(WORKSPACE_ID, ITEM_ID, ITEM_RUN_ID)
    expected_url = f"{BASE_URL}/{API_VERSION}/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/jobs/instances/{ITEM_RUN_ID}/cancel"
    mock_send_request.assert_called_once_with(
        "POST", expected_url, headers={"Authorization": "Bearer access_token"}
    )
