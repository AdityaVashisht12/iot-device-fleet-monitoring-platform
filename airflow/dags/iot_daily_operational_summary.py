"""Daily UTC operational summary built from idempotent hourly analytics."""

from datetime import timedelta

import pendulum
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2 compatibility path
    from airflow.operators.python import PythonOperator

from airflow_analytics_common import ensure_analytics_tables, notify_failure, run_daily_summary

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
    dag_id="iot_daily_operational_summary",
    description="Roll up the prior UTC calendar day of hourly IoT analytics.",
    schedule="20 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["iot", "analytics", "daily", "operations"],
) as dag:
    ensure_tables = PythonOperator(
        task_id="ensure_analytics_tables",
        python_callable=ensure_analytics_tables,
    )
    summarize_day = PythonOperator(
        task_id="summarize_previous_utc_day",
        python_callable=run_daily_summary,
    )

    ensure_tables >> summarize_day

