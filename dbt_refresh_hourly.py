"""Hourly dbt refresh DAG — runs `dbt run` with --select but never tests.

Targets R-orch-09: dbt run with a notify downstream but no dbt test
anywhere in the DAG. R-orch-08 is suppressed via the --select flag so
the finding isolates to the missing-test rule.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "analytics-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _notify(**context):
    return "notified"


with DAG(
    dag_id="dbt_refresh_hourly",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    dbt_run_customers = BashOperator(
        task_id="dbt_run_customers",
        bash_command="dbt run --select customers",
    )
    notify = PythonOperator(
        task_id="notify_ops",
        python_callable=_notify,
        sla=timedelta(minutes=45),
    )

    dbt_run_customers >> notify
