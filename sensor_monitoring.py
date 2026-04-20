"""Live-test DAG: multi-sensor dependency monitoring.

Seed anti-patterns:
  - ExternalTaskSensor poke mode without timeout (R-orch-02)
  - Leaf PythonOperator without SLA (R-orch-05)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _finalize(**context):
    return "done"


with DAG(
    dag_id="sensor_monitoring",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    wait_upstream_a = ExternalTaskSensor(
        task_id="wait_upstream_a",
        external_dag_id="upstream_a",
        external_task_id="finalize",
        mode="poke",
    )
    wait_upstream_b = ExternalTaskSensor(
        task_id="wait_upstream_b",
        external_dag_id="upstream_b",
        external_task_id="finalize",
        mode="reschedule",
        timeout=60 * 60 * 2,
    )
    wait_s3 = S3KeySensor(
        task_id="wait_s3",
        bucket_key="s3://raw/landing/{{ ds }}",
        mode="reschedule",
        timeout=60 * 60 * 4,
    )
    wait_upstream_c = ExternalTaskSensor(
        task_id="wait_upstream_c",
        external_dag_id="upstream_c",
        external_task_id="finalize",
        mode="poke",
    )
    finalize = PythonOperator(
        task_id="finalize_monitoring",
        python_callable=_finalize,
    )

    [wait_upstream_a, wait_upstream_b, wait_s3, wait_upstream_c] >> finalize
