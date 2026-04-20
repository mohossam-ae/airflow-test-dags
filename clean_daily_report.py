"""Live-test DAG: clean daily reporting pipeline — zero findings expected.

Validates no false positives from the full 9-rule engine. All tasks
have explicit retries (via default_args), execution_timeout inherited,
catchup=False, max_active_runs=1, and leaf task carries SLA. The Slack
notification is sent via a PythonOperator wrapping slack-sdk rather
than HttpOperator+POST, so R-orch-07 (non-idempotent retry) stays
silent — the real-world pattern for Slack webhook posts.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def _format(**context):
    return "formatted"


def _send_to_slack(**context):
    # In real life: slack_sdk.WebClient(...).chat_postMessage(...) with
    # dedup via idempotency key. Wrapped in a PythonOperator so the
    # non-idempotent-retry rule doesn't flag it — the caller owns the
    # idempotency guard here.
    return "sent"


def _log_completion(**context):
    return "logged"


with DAG(
    dag_id="clean_daily_report",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    query = PostgresOperator(
        task_id="query_reporting_warehouse",
        postgres_conn_id="warehouse",
        sql="SELECT * FROM marts.daily_kpis WHERE date = '{{ ds }}'",
    )
    format_output = PythonOperator(
        task_id="format_report",
        python_callable=_format,
    )
    send_slack = PythonOperator(
        task_id="send_to_slack",
        python_callable=_send_to_slack,
        sla=timedelta(hours=2),
    )
    log_completion = PythonOperator(
        task_id="log_completion",
        python_callable=_log_completion,
        sla=timedelta(hours=2),
    )

    query >> format_output >> send_slack >> log_completion
