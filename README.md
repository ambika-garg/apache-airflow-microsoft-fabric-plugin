# Apache Airflow Plugin for Microsoft Fabric Plugin. 🚀

## Introduction
A Python package that helps Data and Analytics engineers trigger run on demand job items of [Microsoft Fabric](https://www.microsoft.com/en-us/microsoft-fabric) in Apache Airflow DAGs.

[Microsoft Fabric](https://www.microsoft.com/microsoft-fabric) is an end-to-end analytics and data platform designed for enterprises that require a unified solution. It encompasses data movement, processing, ingestion, transformation, real-time event routing, and report building. It offers a comprehensive suite of services including Data Engineering, Data Factory, Data Science, Real-Time Analytics, Data Warehouse, and Databases.

## How to Use

### Install the Plugin 
Pypi package: https://pypi.org/project/apache-airflow-microsoft-fabric-plugin/
```bash 
pip install apache-airflow-microsoft-fabric-plugin
```

### Prerequisities
Before diving in,
* The plugin supports the <strong>authentication using user tokens</strong>. Tenant level admin account must enable the setting <strong>Allow user consent for apps</strong>. Refer to: [Configure user consent](https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/configure-user-consent?pivots=portal)
* [Create a Microsoft Entra Id app](https://learn.microsoft.com/entra/identity-platform/quickstart-register-app?tabs=certificate) if you don’t have one.
* You must have [Refresh token](https://learn.microsoft.com/entra/identity-platform/v2-oauth2-auth-code-flow#refresh-the-access-token).


Since custom connection forms aren't feasible in Apache Airflow plugins, use can use `Generic` connection type. Here's what you need to store:
1. `Connection Id`: Name of the connection Id
2. `Connection Type`: Generic
3. `Login`: The Client ID of your service principal.
4. `Password`: The refresh token fetched using Microsoft OAuth.
5. `Extra`: {
    "tenantId": "The Tenant Id of your service principal",
    "clientSecret": "(optional) The Client Secret for your Entra ID App"
    "scopes": "(optional) Scopes you used to fetch the refresh token" 
}
> **_NOTE:_** Default scopes applied are: https://api.fabric.microsoft.com/Item.Execute.All, https://api.fabric.microsoft.com/Item.ReadWrite.All, offline_access, openid, profile


## Operators
### FabricRunItemOperator
This operator composes the logic for this plugin. It triggers the Fabric item run and pushes the details in Xcom. It can accept the following parameters:

* `workspace_id`: The workspace Id.
* `item_id`: The Item Id. i.e Notebook and Pipeline.
* `fabric_conn_id`: Connection Id for Fabric.
* `job_type`: "RunNotebook" or "Pipeline".
* `wait_for_termination`: (Default value: True) Wait until the run item.
* `timeout`: int (Default value: 60 * 60 * 24 * 7). Time in seconds to wait for the pipeline or notebook. Used only if `wait_for_termination` is True.
* `check_interval`: int (Default value: 60s). Time in seconds to wait before rechecking the refresh status.
* `max_retries`: int (Default value: 5 retries). Max number of times to poll the API for a valid response after starting a job.
* `retry_delay`: int (Default value: 1s). Polling retry delay.
* `deferrable`: Boolean. Use the operator in deferrable mode.
* `job_params`: Dict. Parameters to pass into the job.

## Features
* #### Refresh token rotation:
  Refresh token rotation is a security mechanism that involves replacing the refresh token each time it is used to obtain a new access token.
  This process enhances security by reducing the risk of stolen tokens being reused indefinitely.

* #### Xcom Integration:
  The Fabric run item enriches the Xcom with essential fields for downstream tasks:
    1. `run_id`: Run Id of the Fabric item.
    2. `run_status`: Fabric item run status.
        * `In Progress`: Item run is in progress.
        * `Completed`: Item run successfully completed.
        * `Failed`: Item run failed.
        * `Disabled`: Item run is disabled by a selective refresh.
    3. `run_location`: The location of item run status.

* #### External Monitoring link:
  The operator conveniently provides a redirect link to the Microsoft Fabric item run.

* ### Deferable Mode:
  The operator runs in deferrable mode. The operator is deferred until the target status of the item run is achieved.

## Sample DAG to use the plugin.

Ready to give it a spin? Check out the sample DAG code below:

```python
from __future__ import annotations

from airflow import DAG
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="fabric_items_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_notebook = FabricRunItemOperator(
        task_id="run_fabric_notebook",
        workspace_id="<workspace_id>",
        item_id="<item_id>",
        fabric_conn_id="fabric_conn_id",
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True,
    )

    run_notebook

```
Feel free to tweak and tailor this DAG to suit your needs!

## Contributing
We welcome any contributions:

- Report all enhancements, bugs, and tasks as [GitHub issues](https://github.com/ambika-garg/apache-airflow-microsoft-fabric-plugin/issues)
- Provide fixes or enhancements by opening pull requests in GitHub.
