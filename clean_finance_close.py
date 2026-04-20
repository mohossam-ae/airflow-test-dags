"""Finance close DAG — zero findings expected.

Follows every Agent 2 best practice:
  - default_args provides retries + retry_delay + execution_timeout
  - catchup=False explicit, max_active_runs=1
  - sensor in reschedule mode with timeout
  - leaf task carries SLA
  - no dbt run (so dbt rules don't apply)
  - writes wrapped in PythonOperator (caller owns idempotency), so
    R-orch-07 stays silent
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "finance-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}


def _ingest_ledger(**context):
    return "ingested"


def _reconcile(**context):
    return "reconciled"


def _publish_close(**context):
    return "published"


with DAG(
    dag_id="clean_finance_close",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    wait_ledger = S3KeySensor(
        task_id="wait_ledger_export",
        bucket_key="s3://finance/ledger/{{ ds }}/_SUCCESS",
        mode="reschedule",
        timeout=60 * 60 * 6,
    )
    ingest = PythonOperator(
        task_id="ingest_ledger",
        python_callable=_ingest_ledger,
    )
    reconcile = PythonOperator(
        task_id="reconcile_accounts",
        python_callable=_reconcile,
    )
    publish = PythonOperator(
        task_id="publish_close",
        python_callable=_publish_close,
        sla=timedelta(hours=3),
    )

    wait_ledger >> ingest >> reconcile >> publish
