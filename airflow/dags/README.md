# IoT analytics DAGs

## DAGs and outputs

- `iot_hourly_analytics` runs at the top of each UTC hour. It reads processed telemetry for the just-closed hour and writes device, region, and global rows to `iot_hourly_analytics`.
- `iot_daily_operational_summary` runs at 00:20 UTC and rolls the previous UTC calendar day into `iot_daily_operational_summary`. The 20-minute offset gives the midnight hourly run time to finish.

Hourly rows include weighted sensor/health averages, healthy/degraded/critical event counts, regional/device uptime, operational incident count and rate, and anomaly counts for LOW/MEDIUM/HIGH/CRITICAL. Uptime assumes one expected reading every two seconds and is capped at 100%. Failure rate is operational incidents per 100 device-hours; it counts `health_critical` and `telemetry_gap` alerts, not predicted hardware failures. Daily averages are weighted by telemetry event counts and daily uptime by expected event counts.

Each table has a composite primary key for its time period and scope. Writes use PostgreSQL `ON CONFLICT ... DO UPDATE`, so task retries and manual reruns replace the aggregate for that key rather than duplicate it. The tables and indexes are created idempotently by each DAG before aggregation.

## Airflow setup and UI

The Compose deployment builds the pinned `apache/airflow:2.11.1-python3.12` image with `airflow/requirements.txt`, mounts `airflow/dags/` at `/opt/airflow/dags`, and provides the `iot_postgres` connection from environment variables. For a standalone Airflow install, install the listed requirements, mount the DAG folder, and create the connection manually.

Open **http://localhost:8080**. Sign in with `AIRFLOW_USER` and `AIRFLOW_PASSWORD`. Compose configures `iot_postgres` automatically. The connection fields below are for standalone Airflow installs.

- **Connection Id:** `iot_postgres`
- **Connection Type:** `Postgres`
- **Host:** `postgres` when Airflow and PostgreSQL share a Compose network; use the reachable database hostname otherwise
- **Schema/Database:** `iot_telemetry`
- **Login/Password:** the configured PostgreSQL user/password
- **Port:** `5432`

For email failure notices, configure the Airflow SMTP backend and set `AIRFLOW_ALERT_EMAIL` to one or more comma-separated recipients (or create an Airflow Variable with that key). If no recipient is configured, the callback still emits a structured task failure log.

## Manual trigger

In the Airflow UI, open **DAGs**, enable the DAG, click its name, then select **Trigger DAG**. Optional JSON run configuration can choose the target period:

Hourly, with an explicit UTC one-hour window:

```json
{"period_start": "2026-11-21T09:00:00Z", "period_end": "2026-11-21T10:00:00Z"}
```

Daily, with an explicit UTC calendar date:

```json
{"summary_date": "2026-11-20"}
```

Without hourly configuration, the task uses the Airflow data interval; if a manual run has no one-hour interval, it falls back to the most recently completed UTC hour. Without daily configuration, it summarizes the prior UTC date. Tasks retry three times with exponential backoff up to 30 minutes. A final task failure invokes the email/log notification callback.

