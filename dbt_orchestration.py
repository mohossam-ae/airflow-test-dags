"""Live-test DAG: dbt runs orchestrated via Airflow.

Seed anti-patterns:
  - ``dbt run`` without ``--select`` (R-orch-08)
  - No ``dbt test`` downstream of ``dbt run`` (R-orch-09)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "analytics-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def _pre_check(**context):
    return "ok"


def _post_notify(**context):
    return "notified"


with DAG(
    dag_id="dbt_orchestration",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    pre_check = PythonOperator(
        task_id="pre_check",
        python_callable=_pre_check,
        sla=timedelta(minutes=30),
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run",
    )
    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command="dbt docs generate",
    )
    post_notify = PythonOperator(
        task_id="post_notify",
        python_callable=_post_notify,
        sla=timedelta(hours=1),
    )

    pre_check >> dbt_run >> dbt_docs >> post_notify
