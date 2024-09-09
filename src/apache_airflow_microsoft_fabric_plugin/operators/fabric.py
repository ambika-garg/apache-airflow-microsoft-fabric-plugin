# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import time
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.configuration import conf
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from apache_airflow_microsoft_fabric_plugin.hooks.fabric import (
    FabricHook,
    FabricRunItemException,
    FabricRunItemStatus,
)
from apache_airflow_microsoft_fabric_plugin.triggers.fabric import FabricTrigger
from airflow.utils.decorators import apply_defaults

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class FabricRunItemLink(BaseOperatorLink):
    """
    Link to the Fabric item run details page.

    :param run_id: The item run ID.
    :type run_id: str
    """

    name = "Monitor Item Run"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ):
        self.item_run_id = XCom.get_value(key="run_id", ti_key=ti_key)
        self.workspace_id = operator.workspace_id
        self.item_id = operator.item_id
        self.base_url = "https://app.fabric.microsoft.com"
        conn_id = operator.fabric_conn_id  # type: ignore
        self.hook = FabricHook(fabric_conn_id=conn_id)

        item_details = self.hook.get_item_details(self.workspace_id, self.item_id)
        self.item_name = item_details.get("displayName")
        url = None

        if operator.job_type == "RunNotebook":
            url = f"{self.base_url}/groups/{self.workspace_id}/synapsenotebooks/{self.item_id}?experience=data-factory"

        elif operator.job_type == "Pipeline":
            url = f"{self.base_url}/workloads/data-pipeline/monitoring/workspaces/{self.workspace_id}/pipelines/{self.item_name}/{self.item_run_id}?experience=data-factory"

        return url


class FabricRunItemOperator(BaseOperator):
    """Operator to run a Fabric item (e.g. a notebook) in a workspace."""

    template_fields: Sequence[str] = (
        "workspace_id",
        "item_id",
        "job_type",
        "fabric_conn_id",
        "job_params",
    )
    template_fields_renderers = {"parameters": "json"}

    operator_extra_links = (FabricRunItemLink(),)

    @apply_defaults
    def __init__(
        self,
        *,
        workspace_id: str,
        item_id: str,
        fabric_conn_id: str,
        job_type: str,
        wait_for_termination: bool = True,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        job_params: dict = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.job_params = job_params

    @cached_property
    def hook(self) -> FabricHook:
        """Create and return the FabricHook (cached)."""
        return FabricHook(fabric_conn_id=self.fabric_conn_id)

    def execute(self, context: Context) -> None:
        # Execute the item run
        self.location = self.hook.run_fabric_item(
            workspace_id=self.workspace_id, item_id=self.item_id, job_type=self.job_type, job_params=self.job_params
        )
        item_run_details = self.hook.get_item_run_details(self.location)
                
        self.item_run_status = item_run_details["status"]
        self.item_run_id = item_run_details["id"]

        # Push the run id to XCom regardless of what happen during execution
        context["ti"].xcom_push(key="run_id", value=self.item_run_id)
        context["ti"].xcom_push(key="location", value=self.location)         

        if self.wait_for_termination:
            if self.deferrable is False:
                self.log.info("Waiting for item run %s to terminate.", self.item_run_id)

                if self.hook.wait_for_item_run_status(
                    self.location,
                    FabricRunItemStatus.COMPLETED,
                    check_interval=self.check_interval,
                    timeout=self.timeout,
                ):
                    self.log.info("Item run %s has completed successfully.", self.item_run_id)
                else:
                    raise FabricRunItemException(
                        f"Item run {self.item_run_id} has failed with status {self.item_run_status}."
                    )
            else:
                end_time = time.monotonic() + self.timeout

                if self.item_run_status not in FabricRunItemStatus.TERMINAL_STATUSES:
                    self.log.info("Deferring the task to wait for item run to complete.")

                    self.defer(
                        trigger=FabricTrigger(
                            fabric_conn_id=self.fabric_conn_id,
                            item_run_id=self.item_run_id,
                            wait_for_termination=self.wait_for_termination,
                            workspace_id=self.workspace_id,
                            item_id=self.item_id,
                            job_type=self.job_type,
                            check_interval=self.check_interval,
                            end_time=end_time,
                        ),
                        method_name="execute_complete",
                    )
                elif self.item_run_status == FabricRunItemStatus.COMPLETED:
                    self.log.info("Item run %s has completed successfully.", self.item_run_id)
                elif self.item_run_status in FabricRunItemStatus.FAILURE_STATES:
                    raise FabricRunItemException(
                        f"Item run {self.item_run_id} has failed with status {self.item_run_status}."
                    )

            # Update the status of Item run in Xcom
            item_run_details = self.hook.get_item_run_details(self.location)
            self.item_run_status = item_run_details["status"]
            context["ti"].xcom_push(key="run_status", value=self.item_run_status)
        else:
            if self.deferrable is True:
                warnings.warn(
                    "Argument `wait_for_termination` is False and `deferrable` is True , hence "
                    "`deferrable` parameter doesn't have any effect",
                    UserWarning,
                    stacklevel=2,
                )
        return

    def execute_complete(self, context: Context, event) -> None:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            self.log.info(event["message"])
            context["ti"].xcom_push(key="run_status", value=event["item_run_status"])
            if event["status"] == "error":
                raise FabricRunItemException(str(event["message"]))
