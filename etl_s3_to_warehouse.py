"""Live-test DAG: S3 → warehouse ETL.

Seed anti-patterns:
  - HttpOperator with retries=0 (R-orch-01)
  - PythonOperator without execution_timeout (R-orch-04 info)
  - PostgresOperator with INSERT and retries=3, no ON CONFLICT (R-orch-07)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retry_delay": timedelta(minutes=5),
}


def _transform(**context):
    return "transformed"


def _cleanup(**context):
    return "cleaned"


with DAG(
    dag_id="etl_s3_to_warehouse",
    schedule="@daily",
    catchup=False,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    wait_s3 = S3KeySensor(
        task_id="wait_for_s3",
        bucket_key="s3://raw/incoming/{{ ds }}.parquet",
        mode="reschedule",
        timeout=60 * 60 * 4,
    )
    transform = PythonOperator(
        task_id="transform_records",
        python_callable=_transform,
    )
    load = PostgresOperator(
        task_id="load_warehouse",
        postgres_conn_id="warehouse",
        sql="INSERT INTO staging.records SELECT * FROM tmp_landing",
        retries=3,
    )
    notify = HttpOperator(
        task_id="notify_api",
        http_conn_id="api_default",
        endpoint="/v1/etl/complete",
        method="POST",
        retries=0,
    )
    health_check = HttpOperator(
        task_id="post_run_health_check",
        http_conn_id="api_default",
        endpoint="/v1/etl/health",
        method="GET",
        retries=0,
    )
    cleanup = PythonOperator(
        task_id="cleanup_tmp",
        python_callable=_cleanup,
    )

    wait_s3 >> transform >> load >> notify >> health_check >> cleanup
