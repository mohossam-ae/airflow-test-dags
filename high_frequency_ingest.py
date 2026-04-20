"""Live-test DAG: every-5-minute streaming ingest.

Seed anti-patterns:
  - ``schedule=*/5 * * * *`` without ``max_active_runs`` (R-orch-10)
  - No ``catchup`` kwarg → Airflow defaults True (R-orch-03)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}


def _parse(**context):
    return "parsed"


with DAG(
    dag_id="high_frequency_ingest",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    fetch = HttpOperator(
        task_id="fetch_stream",
        http_conn_id="stream_api",
        endpoint="/v1/latest",
        method="GET",
    )
    parse = PythonOperator(
        task_id="parse_batch",
        python_callable=_parse,
        sla=timedelta(minutes=3),
    )
    persist = PostgresOperator(
        task_id="persist_batch",
        postgres_conn_id="warehouse",
        sql="INSERT INTO stream.events VALUES (1, 'x') ON CONFLICT (event_id) DO NOTHING",
        sla=timedelta(minutes=4),
    )

    fetch >> parse >> persist
