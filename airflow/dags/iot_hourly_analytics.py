"""Hourly device, regional, and fleet telemetry analytics."""

from datetime import timedelta

import pendulum
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2 compatibility path
    from airflow.operators.python import PythonOperator

from airflow_analytics_common import ensure_analytics_tables, notify_failure, run_hourly_aggregation

DEFAULT_ARGS = {
    "owner": "iot-data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="iot_hourly_analytics",
    description="Aggregate processed IoT telemetry into device, regional, and fleet analytics.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["iot", "analytics", "hourly"],
) as dag:
    ensure_tables = PythonOperator(
        task_id="ensure_analytics_tables",
        python_callable=ensure_analytics_tables,
    )
    aggregate_hour = PythonOperator(
        task_id="aggregate_closed_hour",
        python_callable=run_hourly_aggregation,
    )

    ensure_tables >> aggregate_hour

