"""Live-test DAG: weekly ML model training.

Seed anti-patterns:
  - Training PythonOperator without execution_timeout (R-orch-04 info;
    default_args doesn't provide one)
  - HttpOperator with retries=0 (R-orch-01)
  - Leaf deploy PythonOperator without SLA (R-orch-05)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "ml-eng",
    "retry_delay": timedelta(minutes=15),
}


def _feature_engineering(**context):
    return "features"


def _train(**context):
    return "trained"


def _validate(**context):
    return "validated"


def _deploy(**context):
    return "deployed"


with DAG(
    dag_id="ml_training_pipeline",
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    wait_features = S3KeySensor(
        task_id="wait_feature_store",
        bucket_key="s3://features/latest/_SUCCESS",
        mode="reschedule",
        timeout=60 * 60 * 6,
        retries=2,
    )
    feature_eng = PythonOperator(
        task_id="feature_engineering",
        python_callable=_feature_engineering,
        execution_timeout=timedelta(hours=1),
    )
    train = PythonOperator(
        task_id="train_model",
        python_callable=_train,
    )
    register = HttpOperator(
        task_id="register_model",
        http_conn_id="model_registry",
        endpoint="/v1/models",
        method="POST",
        retries=0,
        execution_timeout=timedelta(minutes=10),
    )
    validate = PythonOperator(
        task_id="validate_model",
        python_callable=_validate,
        execution_timeout=timedelta(minutes=30),
    )
    deploy = PythonOperator(
        task_id="deploy_model",
        python_callable=_deploy,
        execution_timeout=timedelta(minutes=30),
    )

    wait_features >> feature_eng >> train >> register >> validate >> deploy
