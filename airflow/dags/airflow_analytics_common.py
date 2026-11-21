"""Shared SQL, scheduling helpers, and notifications for IoT analytics DAGs."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

LOGGER = logging.getLogger(__name__)
POSTGRES_CONN_ID = "iot_postgres"
EXPECTED_INTERVAL_SECONDS = 2


def _make_upsert(table: str, columns: tuple[str, ...], keys: tuple[str, ...]) -> str:
    placeholders = ", ".join(["%s"] * len(columns))
    col_sql = ", ".join(columns)
    update_cols = [column for column in columns if column not in keys]
    update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_cols)
    key_sql = ", ".join(keys)
    return (
        f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT ({key_sql}) DO UPDATE SET {update_sql}, calculated_at = CURRENT_TIMESTAMP"
    )

ANALYTICS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS iot_hourly_analytics (
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('device', 'region', 'global')),
    scope_id TEXT NOT NULL,
    region TEXT,
    device_count BIGINT NOT NULL,
    telemetry_count BIGINT NOT NULL,
    avg_temperature_c DOUBLE PRECISION,
    avg_cpu_usage_pct DOUBLE PRECISION,
    avg_memory_usage_pct DOUBLE PRECISION,
    avg_network_latency_ms DOUBLE PRECISION,
    avg_packet_loss_pct DOUBLE PRECISION,
    avg_battery_level_pct DOUBLE PRECISION,
    avg_device_health_score DOUBLE PRECISION,
    healthy_event_count BIGINT NOT NULL,
    degraded_event_count BIGINT NOT NULL,
    critical_event_count BIGINT NOT NULL,
    expected_event_count BIGINT NOT NULL,
    received_event_count BIGINT NOT NULL,
    uptime_pct DOUBLE PRECISION NOT NULL,
    failure_count BIGINT NOT NULL,
    failure_rate_pct DOUBLE PRECISION NOT NULL,
    anomaly_low_count BIGINT NOT NULL,
    anomaly_medium_count BIGINT NOT NULL,
    anomaly_high_count BIGINT NOT NULL,
    anomaly_critical_count BIGINT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (period_start, scope_type, scope_id),
    CHECK (period_end > period_start)
);
CREATE INDEX IF NOT EXISTS idx_iot_hourly_scope_region
    ON iot_hourly_analytics (scope_type, region, period_start DESC);

CREATE TABLE IF NOT EXISTS iot_daily_operational_summary (
    summary_date DATE NOT NULL,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('device', 'region', 'global')),
    scope_id TEXT NOT NULL,
    region TEXT,
    device_count BIGINT NOT NULL,
    telemetry_count BIGINT NOT NULL,
    avg_temperature_c DOUBLE PRECISION,
    avg_cpu_usage_pct DOUBLE PRECISION,
    avg_memory_usage_pct DOUBLE PRECISION,
    avg_network_latency_ms DOUBLE PRECISION,
    avg_packet_loss_pct DOUBLE PRECISION,
    avg_battery_level_pct DOUBLE PRECISION,
    avg_device_health_score DOUBLE PRECISION,
    healthy_event_count BIGINT NOT NULL,
    degraded_event_count BIGINT NOT NULL,
    critical_event_count BIGINT NOT NULL,
    expected_event_count BIGINT NOT NULL,
    received_event_count BIGINT NOT NULL,
    uptime_pct DOUBLE PRECISION NOT NULL,
    failure_count BIGINT NOT NULL,
    failure_rate_pct DOUBLE PRECISION NOT NULL,
    anomaly_low_count BIGINT NOT NULL,
    anomaly_medium_count BIGINT NOT NULL,
    anomaly_high_count BIGINT NOT NULL,
    anomaly_critical_count BIGINT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (summary_date, scope_type, scope_id)
);
CREATE INDEX IF NOT EXISTS idx_iot_daily_region_date
    ON iot_daily_operational_summary (region, summary_date DESC);
"""

HOURLY_AGGREGATION_SQL = """
WITH bounds AS (
    SELECT %s::timestamptz AS period_start, %s::timestamptz AS period_end
), event_metrics AS (
    SELECT
        p.device_id,
        COUNT(*)::BIGINT AS telemetry_count,
        SUM(p.temperature_c) AS temperature_sum, COUNT(p.temperature_c)::BIGINT AS temperature_n,
        SUM(p.cpu_usage_pct) AS cpu_sum, COUNT(p.cpu_usage_pct)::BIGINT AS cpu_n,
        SUM(p.memory_usage_pct) AS memory_sum, COUNT(p.memory_usage_pct)::BIGINT AS memory_n,
        SUM(p.network_latency_ms) AS latency_sum, COUNT(p.network_latency_ms)::BIGINT AS latency_n,
        SUM(p.packet_loss_pct) AS packet_loss_sum, COUNT(p.packet_loss_pct)::BIGINT AS packet_loss_n,
        SUM(p.battery_level_pct) AS battery_sum, COUNT(p.battery_level_pct)::BIGINT AS battery_n,
        SUM(p.device_health_score) AS health_sum,
        COUNT(*) FILTER (WHERE p.performance_status = 'healthy')::BIGINT AS healthy_count,
        COUNT(*) FILTER (WHERE p.performance_status = 'degraded')::BIGINT AS degraded_count,
        COUNT(*) FILTER (WHERE p.performance_status = 'critical')::BIGINT AS critical_count,
        COUNT(*) FILTER (WHERE p.is_anomaly AND p.anomaly_severity = 'LOW')::BIGINT AS anomaly_low,
        COUNT(*) FILTER (WHERE p.is_anomaly AND p.anomaly_severity = 'MEDIUM')::BIGINT AS anomaly_medium,
        COUNT(*) FILTER (WHERE p.is_anomaly AND p.anomaly_severity = 'HIGH')::BIGINT AS anomaly_high,
        COUNT(*) FILTER (WHERE p.is_anomaly AND p.anomaly_severity = 'CRITICAL')::BIGINT AS anomaly_critical
    FROM processed_telemetry AS p CROSS JOIN bounds AS b
    WHERE p.event_timestamp >= b.period_start AND p.event_timestamp < b.period_end
    GROUP BY p.device_id
), alert_metrics AS (
    SELECT
        a.device_id,
        COUNT(*)::BIGINT AS failure_count
    FROM device_alerts AS a CROSS JOIN bounds AS b
    WHERE a.alert_type IN ('telemetry_gap', 'health_critical')
      AND a.alert_timestamp >= b.period_start AND a.alert_timestamp < b.period_end
    GROUP BY a.device_id
), per_device AS (
    SELECT
        d.device_id, d.region,
        COALESCE(e.telemetry_count, 0)::BIGINT AS telemetry_count,
        COALESCE(e.temperature_sum, 0) AS temperature_sum,
        COALESCE(e.temperature_n, 0)::BIGINT AS temperature_n,
        COALESCE(e.cpu_sum, 0) AS cpu_sum, COALESCE(e.cpu_n, 0)::BIGINT AS cpu_n,
        COALESCE(e.memory_sum, 0) AS memory_sum, COALESCE(e.memory_n, 0)::BIGINT AS memory_n,
        COALESCE(e.latency_sum, 0) AS latency_sum, COALESCE(e.latency_n, 0)::BIGINT AS latency_n,
        COALESCE(e.packet_loss_sum, 0) AS packet_loss_sum, COALESCE(e.packet_loss_n, 0)::BIGINT AS packet_loss_n,
        COALESCE(e.battery_sum, 0) AS battery_sum, COALESCE(e.battery_n, 0)::BIGINT AS battery_n,
        COALESCE(e.health_sum, 0) AS health_sum,
        COALESCE(e.healthy_count, 0)::BIGINT AS healthy_count,
        COALESCE(e.degraded_count, 0)::BIGINT AS degraded_count,
        COALESCE(e.critical_count, 0)::BIGINT AS critical_count,
        COALESCE(e.anomaly_low, 0)::BIGINT AS anomaly_low,
        COALESCE(e.anomaly_medium, 0)::BIGINT AS anomaly_medium,
        COALESCE(e.anomaly_high, 0)::BIGINT AS anomaly_high,
        COALESCE(e.anomaly_critical, 0)::BIGINT AS anomaly_critical,
        COALESCE(a.failure_count, 0)::BIGINT AS failure_count,
        GREATEST(1, CEIL(EXTRACT(EPOCH FROM
            (b.period_end - GREATEST(b.period_start, d.first_seen_at))) / %(interval_seconds)s)::BIGINT
        ) AS expected_event_count
    FROM devices AS d CROSS JOIN bounds AS b
    LEFT JOIN event_metrics AS e ON e.device_id = d.device_id
    LEFT JOIN alert_metrics AS a ON a.device_id = d.device_id
    WHERE d.first_seen_at < b.period_end
), rolled AS (
    SELECT
        b.period_start, b.period_end,
        CASE WHEN GROUPING(x.device_id) = 0 THEN 'device'
             WHEN GROUPING(x.region) = 0 THEN 'region' ELSE 'global' END AS scope_type,
        CASE WHEN GROUPING(x.device_id) = 0 THEN x.device_id
             WHEN GROUPING(x.region) = 0 THEN x.region ELSE 'all' END AS scope_id,
        CASE WHEN GROUPING(x.region) = 0 THEN x.region ELSE NULL END AS region,
        COUNT(*)::BIGINT AS device_count,
        SUM(x.telemetry_count)::BIGINT AS telemetry_count,
        SUM(x.temperature_sum) / NULLIF(SUM(x.temperature_n), 0) AS avg_temperature_c,
        SUM(x.cpu_sum) / NULLIF(SUM(x.cpu_n), 0) AS avg_cpu_usage_pct,
        SUM(x.memory_sum) / NULLIF(SUM(x.memory_n), 0) AS avg_memory_usage_pct,
        SUM(x.latency_sum) / NULLIF(SUM(x.latency_n), 0) AS avg_network_latency_ms,
        SUM(x.packet_loss_sum) / NULLIF(SUM(x.packet_loss_n), 0) AS avg_packet_loss_pct,
        SUM(x.battery_sum) / NULLIF(SUM(x.battery_n), 0) AS avg_battery_level_pct,
        SUM(x.health_sum) / NULLIF(SUM(x.telemetry_count), 0) AS avg_device_health_score,
        SUM(x.healthy_count)::BIGINT AS healthy_event_count,
        SUM(x.degraded_count)::BIGINT AS degraded_event_count,
        SUM(x.critical_count)::BIGINT AS critical_event_count,
        SUM(x.expected_event_count)::BIGINT AS expected_event_count,
        SUM(x.telemetry_count)::BIGINT AS received_event_count,
        100.0 * SUM(LEAST(x.telemetry_count, x.expected_event_count))
            / NULLIF(SUM(x.expected_event_count), 0) AS uptime_pct,
        SUM(x.failure_count)::BIGINT AS failure_count,
        100.0 * SUM(x.failure_count) / NULLIF(COUNT(*), 0) AS failure_rate_pct,
        SUM(x.anomaly_low)::BIGINT AS anomaly_low_count,
        SUM(x.anomaly_medium)::BIGINT AS anomaly_medium_count,
        SUM(x.anomaly_high)::BIGINT AS anomaly_high_count,
        SUM(x.anomaly_critical)::BIGINT AS anomaly_critical_count
    FROM per_device AS x CROSS JOIN bounds AS b
    GROUP BY b.period_start, b.period_end,
        GROUPING SETS ((x.device_id, x.region), (x.region), ())
    HAVING COUNT(*) > 0
)
SELECT period_start, period_end, scope_type, scope_id, region, device_count, telemetry_count,
       avg_temperature_c, avg_cpu_usage_pct, avg_memory_usage_pct, avg_network_latency_ms,
       avg_packet_loss_pct, avg_battery_level_pct, avg_device_health_score,
       healthy_event_count, degraded_event_count, critical_event_count,
       expected_event_count, received_event_count, uptime_pct, failure_count, failure_rate_pct,
       anomaly_low_count, anomaly_medium_count, anomaly_high_count, anomaly_critical_count
FROM rolled
"""

HOURLY_COLUMNS = (
    "period_start", "period_end", "scope_type", "scope_id", "region", "device_count",
    "telemetry_count", "avg_temperature_c", "avg_cpu_usage_pct", "avg_memory_usage_pct",
    "avg_network_latency_ms", "avg_packet_loss_pct", "avg_battery_level_pct",
    "avg_device_health_score", "healthy_event_count", "degraded_event_count",
    "critical_event_count", "expected_event_count", "received_event_count", "uptime_pct",
    "failure_count", "failure_rate_pct", "anomaly_low_count", "anomaly_medium_count",
    "anomaly_high_count", "anomaly_critical_count",
)

DAILY_COLUMNS = (
    "summary_date", "scope_type", "scope_id", "region", "device_count", "telemetry_count",
    "avg_temperature_c", "avg_cpu_usage_pct", "avg_memory_usage_pct", "avg_network_latency_ms",
    "avg_packet_loss_pct", "avg_battery_level_pct", "avg_device_health_score",
    "healthy_event_count", "degraded_event_count", "critical_event_count",
    "expected_event_count", "received_event_count", "uptime_pct", "failure_count",
    "failure_rate_pct", "anomaly_low_count", "anomaly_medium_count", "anomaly_high_count",
    "anomaly_critical_count",
)

HOURLY_UPSERT_SQL = _make_upsert(
    "iot_hourly_analytics", HOURLY_COLUMNS, ("period_start", "scope_type", "scope_id")
)
DAILY_UPSERT_SQL = _make_upsert(
    "iot_daily_operational_summary", DAILY_COLUMNS, ("summary_date", "scope_type", "scope_id")
)

DAILY_AGGREGATION_SQL = """
SELECT
    %s::date AS summary_date,
    scope_type, scope_id, region,
    MAX(device_count)::BIGINT AS device_count,
    SUM(telemetry_count)::BIGINT AS telemetry_count,
    SUM(avg_temperature_c * telemetry_count) / NULLIF(SUM(telemetry_count), 0) AS avg_temperature_c,
    SUM(avg_cpu_usage_pct * telemetry_count) / NULLIF(SUM(telemetry_count), 0) AS avg_cpu_usage_pct,
    SUM(avg_memory_usage_pct * telemetry_count) / NULLIF(SUM(telemetry_count), 0) AS avg_memory_usage_pct,
    SUM(avg_network_latency_ms * telemetry_count) / NULLIF(SUM(telemetry_count), 0) AS avg_network_latency_ms,
    SUM(avg_packet_loss_pct * telemetry_count) / NULLIF(SUM(telemetry_count), 0) AS avg_packet_loss_pct,
    SUM(avg_battery_level_pct * telemetry_count) / NULLIF(SUM(telemetry_count), 0) AS avg_battery_level_pct,
    SUM(avg_device_health_score * telemetry_count) / NULLIF(SUM(telemetry_count), 0) AS avg_device_health_score,
    SUM(healthy_event_count)::BIGINT AS healthy_event_count,
    SUM(degraded_event_count)::BIGINT AS degraded_event_count,
    SUM(critical_event_count)::BIGINT AS critical_event_count,
    SUM(expected_event_count)::BIGINT AS expected_event_count,
    SUM(received_event_count)::BIGINT AS received_event_count,
    SUM(uptime_pct * expected_event_count) / NULLIF(SUM(expected_event_count), 0) AS uptime_pct,
    SUM(failure_count)::BIGINT AS failure_count,
    100.0 * SUM(failure_count) / NULLIF(SUM(device_count), 0) AS failure_rate_pct,
    SUM(anomaly_low_count)::BIGINT AS anomaly_low_count,
    SUM(anomaly_medium_count)::BIGINT AS anomaly_medium_count,
    SUM(anomaly_high_count)::BIGINT AS anomaly_high_count,
    SUM(anomaly_critical_count)::BIGINT AS anomaly_critical_count
FROM iot_hourly_analytics
WHERE period_start >= %s::timestamptz AND period_start < %s::timestamptz
GROUP BY scope_type, scope_id, region
"""


def ensure_analytics_tables() -> None:
    """Create analytics tables and indexes idempotently."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    connection = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with connection.cursor() as cursor:
            for statement in ANALYTICS_SCHEMA_SQL.split(";"):
                if statement.strip():
                    cursor.execute(statement)
        connection.commit()
        LOGGER.info("Hourly and daily analytics tables are ready")
    except Exception:
        connection.rollback()
        LOGGER.exception("Could not create analytics tables")
        raise
    finally:
        connection.close()


def run_hourly_aggregation(**context: Any) -> None:
    """Aggregate one closed UTC hour and idempotently upsert device/region/global rows."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    start, end = _hour_bounds(context)
    connection = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                HOURLY_AGGREGATION_SQL.replace("%(interval_seconds)s", "%s"),
                (start, end, EXPECTED_INTERVAL_SECONDS),
            )
            rows = cursor.fetchall()
            if rows:
                cursor.executemany(HOURLY_UPSERT_SQL, rows)
        connection.commit()
        LOGGER.info(
            "Hourly analytics upserted",
            extra={"period_start": start.isoformat(), "period_end": end.isoformat(), "rows": len(rows)},
        )
    except Exception:
        connection.rollback()
        LOGGER.exception("Hourly analytics aggregation failed")
        raise
    finally:
        connection.close()


def run_daily_summary(**context: Any) -> None:
    """Roll up hourly aggregates for one UTC calendar day and idempotently upsert."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    summary_day = _summary_date(context)
    day_start = datetime.combine(summary_day, time.min, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    connection = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(DAILY_AGGREGATION_SQL, (summary_day, day_start, day_end))
            rows = cursor.fetchall()
            if rows:
                cursor.executemany(DAILY_UPSERT_SQL, rows)
        connection.commit()
        LOGGER.info(
            "Daily operational summary upserted",
            extra={"summary_date": summary_day.isoformat(), "rows": len(rows)},
        )
    except Exception:
        connection.rollback()
        LOGGER.exception("Daily operational summary failed")
        raise
    finally:
        connection.close()


def notify_failure(context: dict[str, Any]) -> None:
    """Log task failure and email configured operators through Airflow's email backend."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = getattr(task_instance, "task_id", "unknown")
    exception = context.get("exception")
    LOGGER.error("Airflow analytics task failed", extra={"dag_id": dag_id, "task_id": task_id, "error": str(exception)})
    recipients = os.getenv("AIRFLOW_ALERT_EMAIL")
    if not recipients:
        try:
            from airflow.models import Variable

            recipients = Variable.get("AIRFLOW_ALERT_EMAIL", default_var=None)
        except Exception:
            LOGGER.exception("Could not read AIRFLOW_ALERT_EMAIL Airflow Variable")
    if not recipients:
        LOGGER.error("Failure notification email not sent; configure AIRFLOW_ALERT_EMAIL")
        return
    try:
        from airflow.utils.email import send_email

        send_email(
            to=[address.strip() for address in recipients.split(",")],
            subject=f"Airflow task failed: {dag_id}.{task_id}",
            html_content=(
                f"<p>DAG <b>{dag_id}</b>, task <b>{task_id}</b> failed.</p>"
                f"<p>Run: {context.get('run_id', 'unknown')}</p><pre>{exception}</pre>"
            ),
        )
        LOGGER.info("Failure notification email sent", extra={"dag_id": dag_id, "task_id": task_id})
    except Exception:
        LOGGER.exception("Could not send Airflow failure notification email")


def _hour_bounds(context: dict[str, Any]) -> tuple[datetime, datetime]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    if conf.get("period_start") and conf.get("period_end"):
        start = _parse_datetime(conf["period_start"])
        end = _parse_datetime(conf["period_end"])
    else:
        start = _parse_datetime(context["data_interval_start"])
        end = _parse_datetime(context["data_interval_end"])
        if end - start != timedelta(hours=1):
            now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            end, start = now, now - timedelta(hours=1)
    if end - start != timedelta(hours=1):
        raise ValueError("hourly aggregation period must be exactly one hour")
    return start, end


def _summary_date(context: dict[str, Any]) -> date:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    if conf.get("summary_date"):
        return date.fromisoformat(str(conf["summary_date"]))
    interval_end = context.get("data_interval_end")
    if interval_end is None:
        interval_end = datetime.now(timezone.utc)
    return _parse_datetime(interval_end).date() - timedelta(days=1)


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).replace("Z", "+00:00")
        result = datetime.fromisoformat(text)
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)

