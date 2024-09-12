from __future__ import annotations

import time
from typing import Any, Callable

import aiohttp
import requests
from asgiref.sync import sync_to_async
from tenacity import retry, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils.session import provide_session

FABRIC_SCOPES = "https://api.fabric.microsoft.com/Item.Execute.All https://api.fabric.microsoft.com/Item.ReadWrite.All offline_access openid profile"


@provide_session
def update_conn(conn_id, refresh_token: str, session=None):
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).one()
    conn.password = refresh_token
    session.add(conn)
    session.commit()


class FabricRunItemStatus:
    """Fabric item run operation statuses."""

    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    NOT_STARTED = "NotStarted"
    DEDUPED = "Deduped"

    TERMINAL_STATUSES = {CANCELLED, FAILED, COMPLETED}
    INTERMEDIATE_STATES = {IN_PROGRESS}
    FAILURE_STATES = {FAILED, CANCELLED, DEDUPED}


class FabricRunItemException(AirflowException):
    """An exception that indicates a item run failed to complete."""


class FabricHook(BaseHook):
    """
    A hook to interact with Microsoft Fabric.
    This hook uses OAuth token generated from the refresh token, client ID and tenant ID specified in the connection.

    :param fabric_conn_id: Airflow Connection ID that contains the connection
        information for the Fabric account used for authentication.
    """  # noqa: D205

    conn_type: str = "fabric"
    conn_name_attr: str = "fabric_conn_id"
    default_conn_name: str = "fabric_default"
    hook_name: str = "MS Fabric"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "clientId": StringField(lazy_gettext("Client ID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Refresh Token",
            },
        }

    def __init__(
        self,
        *,
        fabric_conn_id: str = default_conn_name
    ):
        self.conn_id = fabric_conn_id
        self._api_version = "v1"
        self._base_url = "https://api.fabric.microsoft.com"
        self.cached_access_token: dict[str, str | None | int] = {"access_token": None, "expiry_time": 0}
        super().__init__()

    def _get_token(self) -> str:
        """
        If cached access token isn't expired, return it.

        Generate OAuth access token using refresh token in connection details and cache it.
        Update the connection with the new refresh token.

        :return: The access token.
        """
        access_token = self.cached_access_token.get("access_token")
        expiry_time = self.cached_access_token.get("expiry_time")

        if access_token and expiry_time > time.time():
            return str(access_token)

        connection = self.get_connection(self.conn_id)
        tenant_id = connection.extra_dejson.get("tenantId")
        client_id = connection.login
        client_secret = connection.extra_dejson.get("clientSecret")
        scopes = connection.extra_dejson.get("scopes", FABRIC_SCOPES)
        refresh_token = connection.password

        data = {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": refresh_token,
            "scope": scopes,
        }
        if client_secret:
            data["client_secret"] = client_secret

        response = self._send_request(
            "POST",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data=data,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            msg = f"Response: {e.response.content.decode()} Status Code: {e.response.status_code}"
            raise AirflowException(msg)

        response_data = response.json()

        api_access_token: str | None = response_data.get("access_token")
        api_refresh_token: str | None = response_data.get("refresh_token")

        if not api_access_token or not api_refresh_token:
            raise AirflowException("Failed to obtain access or refresh token from API.")

        update_conn(self.conn_id, api_refresh_token)

        self.cached_access_token = {
            "access_token": api_access_token,
            "expiry_time": time.time() + response_data.get("expires_in"),
        }

        return api_access_token

    def get_headers(self) -> dict[str, str]:
        """
        Form of auth headers based on OAuth token.

        :return: dict: Headers with the authorization token.
        """
        return {
            "Authorization": f"Bearer {self._get_token()}",
        }

    def get_item_run_details(self, location: str) -> None:
        """
        Get details of the item run instance.

        :param location: The location of the item instance.
        """

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=self.retry_delay, max=10)
        )
        def _internal_get_item_run_details():
            headers = self.get_headers()
            response = self._send_request("GET", location, headers=headers)
            response.raise_for_status()

            item_run_details = response.json()
            item_failure_reason = item_run_details.get("failureReason", dict())
            if item_failure_reason is not None and item_failure_reason.get("errorCode") in ["RequestExecutionFailed", "NotFound"]:
                raise FabricRunItemException("Unable to get item run details.")
            return item_run_details

        return _internal_get_item_run_details()

    def get_item_details(self, workspace_id: str, item_id: str) -> dict:
        """
        Get details of the item.

        :param workspace_id: The ID of the workspace in which the item is located.
        :param item_id: The ID of the item.

        :return: The details of the item.
        """
        url = f"{self._base_url}/{self._api_version}/workspaces/{workspace_id}/items/{item_id}"

        headers = self.get_headers()
        response = self._send_request("GET", url, headers=headers)

        if response.ok:
            return response.json()

        raise AirflowException(f"Failed to get item details for item {item_id} in workspace {workspace_id}.")

    def run_fabric_item(self, workspace_id: str, item_id: str, job_type: str, job_params: dict | None) -> str:
        """
        Run a Fabric item.

        :param workspace_id: The workspace Id in which the item is located.
        :param item_id: The item Id. To check available items, Refer to: https://learn.microsoft.com/rest/api/fabric/admin/items/list-items?tabs=HTTP#itemtype.
        :param job_type: The type of job to run. For running a notebook, this should be "RunNotebook".
        :param job_params: An optional dictionary of parameters to pass to the job.

        :return: The run Id of item.
        """
        url = f"{self._base_url}/{self._api_version}/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}"

        headers = self.get_headers()

        data = {"executionData": {"parameters": job_params}} if job_params else {}

        response = self._send_request("POST", url, headers=headers, json=data)
        response.raise_for_status()

        location_header = response.headers.get("Location")
        if location_header is None:
            raise AirflowException("Location header not found in run on demand item response.")

        return location_header

    # TODO: output value from notebook should be available in xcom - not available in API yet

    def wait_for_item_run_status(
        self,
        location: str,
        target_status: str,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Wait for the item run to reach a target status.

        :param location: The location of the item instance retrieved from the header of item run API.
        :param target_status: The status to wait for.
        :param check_interval: The interval at which to check the status.
        :param timeout: The maximum time to wait for the status.

        :return: True if the item run reached the target status, False otherwise.
        """
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            item_run_details = self.get_item_run_details(location)
            item_run_status = item_run_details["status"]
            if item_run_status in FabricRunItemStatus.TERMINAL_STATUSES:
                return item_run_status == target_status
            self.log.info("Sleeping for %s. The pipeline state is %s.", check_interval, item_run_status)
            time.sleep(check_interval)
        raise FabricRunItemException(
            f"Item run did not reach the target status {target_status} within the {timeout} seconds."
        )

    def _send_request(self, request_type: str, url: str, **kwargs) -> requests.Response:
        """
        Send a request to the REST API.

        :param request_type: The type of the request (GET, POST, PUT, etc.).
        :param url: The URL against which the request needs to be made.
        :param kwargs: Additional keyword arguments to be passed to the request function.
        :return: The response object returned by the request.
        :raises requests.HTTPError: If the request fails (e.g., non-2xx status code).
        """
        request_funcs: dict[str, Callable[..., requests.Response]] = {
            "GET": requests.get,
            "POST": requests.post,
        }

        func: Callable[..., requests.Response] = request_funcs[request_type.upper()]

        response = func(url=url, **kwargs)

        return response


class FabricAsyncHook(FabricHook):
    """
    Interact with Microsoft Fabric asynchronously.

    :param fabric_conn_id: Airflow Connection ID that contains the connection
    """

    default_conn_name: str = "fabric_default"

    def __init__(self, *, fabric_conn_id: str = default_conn_name):
        super().__init__(fabric_conn_id=fabric_conn_id)

    async def _async_send_request(self, request_type: str, url: str, **kwargs) -> Any:
        """
        Asynchronously sends a HTTP request and returns the response.

        :param request_type: The HTTP method to use ('GET', 'POST', etc.).
        :param url: The URL to send the request to.
        :param kwargs: Additional arguments to pass to the request method.
        :return: The response from the server.
        """
        async with aiohttp.ClientSession() as session:
            if request_type.upper() == "GET":
                request_func = session.get
            elif request_type.upper() == "POST":
                request_func = session.post
            else:
                raise AirflowException(f"Unsupported request type: {request_type}")

            try:
                response = await request_func(url, **kwargs)

                content_type = response.headers.get('Content-Type', '').lower()
                if 'application/json' in content_type:
                    return await response.json()
                elif 'application/octet-stream' in content_type:
                    return response # Returns the raw bytes
                else:
                    raise AirflowException(f"Unsupported Content-Type: {content_type}")

            except aiohttp.ClientResponseError as e:
                raise AirflowException("Request to %s failed with error %s", (url, str(e)))

    async def _async_get_token(self) -> str:
        """
        Get the access token from the refresh token.

        :return: The access token.
        """
        cached_token = self.cached_access_token.get("access_token")
        expiry_time = self.cached_access_token.get("expiry_time")

        if isinstance(cached_token, str) and isinstance(expiry_time, float) and expiry_time > time.time():
            return str(cached_token)

        connection = await sync_to_async(self.get_connection)(self.conn_id)
        tenant_id = connection.extra_dejson.get("tenantId")
        client_id = connection.login
        client_secret = connection.extra_dejson.get("clientSecret")
        refresh_token = connection.password
        scopes = connection.extra_dejson.get("scopes", FABRIC_SCOPES)

        data = {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": refresh_token,
            "scope": scopes,
        }
        if client_secret:
            data["client_secret"] = client_secret

        response = await self._async_send_request(
            "POST",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data=data,
        )
        api_access_token: str | None = response.get("access_token")
        api_refresh_token: str | None = response.get("refresh_token")

        if not api_access_token or not api_refresh_token:
            raise AirflowException("Failed to obtain access or refresh token from API.")

        await sync_to_async(update_conn)(self.conn_id, api_refresh_token)

        self.cached_access_token = {
            "access_token": api_access_token,
            "expiry_time": time.time() + response.get("expires_in"),
        }

        return api_access_token

    async def async_get_headers(self) -> dict[str, str]:
        """
        Form of auth headers based on OAuth token.

        :return: dict: Headers with the authorization token.
        """
        access_token = await self._async_get_token()

        return {
            "Authorization": f"Bearer {access_token}",
        }

    async def async_get_item_run_details(self, workspace_id: str, item_id: str, item_run_id: str) -> None:
        """
        Get run details of the item instance.

        :param location: The location of the item instance.
        """
        url = f"{self._base_url}/{self._api_version}/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{item_run_id}"
        headers = await self.async_get_headers()
        response = await self._async_send_request("GET", url, headers=headers)

        return response

    async def cancel_item_run(self, workspace_id: str, item_id: str, item_run_id: str):
        """
        Cancel the item run.

        :param workspace_id: The workspace Id in which the item is located.
        :param item_id: The item Id.
        :param item_run_id: The Id of the item run.

        """
        url = f"{self._base_url}/{self._api_version}/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{item_run_id}/cancel"
        headers = await self.async_get_headers()
        response = await self._async_send_request("POST", url, headers=headers)
        response.raise_for_status()

        return response
