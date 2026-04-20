"""Daily reporting DAG — no catchup kwarg (Airflow silently defaults True).

Intentionally demonstrates R-orch-03: a scheduled DAG that would
backfill every missed interval on pause/resume or on deploy with a
past start_date.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def _build_report(**context):
    return "built"


def _publish_report(**context):
    return "published"


with DAG(
    dag_id="reports_daily",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    build = PythonOperator(
        task_id="build_report",
        python_callable=_build_report,
    )
    publish = PythonOperator(
        task_id="publish_report",
        python_callable=_publish_report,
        sla=timedelta(hours=2),
    )

    build >> publish
