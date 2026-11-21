# PostgreSQL schema and relationships

Apply `schema.sql` to the configured PostgreSQL database. The Kafka consumer also applies this DDL at startup before it subscribes. Accepted event persistence is transactional: device registration, raw event insertion, processed event insertion, and any generated alert commit together. The `invalid_telemetry_events` table retains rejected input separately.

## Relationship model

```text
devices (1) ------< raw_telemetry (many)
   |                       |
   |                       | one raw event
   |                       v
   +-------------------> processed_telemetry (one per raw event)
   |                            |
   +----------------------------+----< device_alerts (incidents)
```

- `devices.device_id` is the primary key. A device is registered automatically on its first valid telemetry event. `region` and the latest operational status are updated as new events arrive.
- `raw_telemetry.raw_telemetry_id` is the primary key; `event_id` is globally unique and `device_id` references `devices`. It stores the source JSONB payload and source/ingestion timestamps.
- `processed_telemetry.raw_telemetry_id` is both its primary key and a foreign key to the raw event, enforcing at most one processed row per source event. Its `event_id` is unique and references the raw event; `device_id` also references the registered device. Sensor constraints mirror consumer validation.
- `device_alerts` references both a device and a processed `event_id`. A partial unique index allows one unresolved incident per `(device_id, alert_type)`; repeated anomalous readings update its latest event, severity, last-seen timestamp, and occurrence count. A normal reading resolves it, and a later anomaly can open a new incident. The consumer generates `anomaly` incidents for Isolation Forest or simulator-marked anomalies, `health_critical` incidents for critical health scores, and closed `telemetry_gap` incidents when a returning device has a gap of at least six seconds (three default two-second ticks). Isolation Forest anomalies are statistical unusualness; neither they nor these telemetry-derived operational incidents are hardware-failure predictions.
- `invalid_telemetry_events` is intentionally not linked to `devices`: malformed records may not contain a usable device ID. Its SHA-256 `record_hash` primary key makes identical rejected payload writes idempotent.

Foreign keys restrict deleting referenced devices; raw-event deletion cascades to its processed row, and processed-event deletion cascades to its alerts. The consumer does not delete telemetry. `event_id` uniqueness plus transactional `ON CONFLICT DO NOTHING` insertion protects valid records from Kafka replays.

## Indexing and analytical views

Primary/unique constraints create indexes on identifiers. Additional indexes cover device activity by `last_seen_at`, `(device_id, event_timestamp)` for raw and processed telemetry, timestamp-only scans, and `(device_id, alert_timestamp)` for alert history.

| View | Meaning |
|---|---|
| `v_device_activity_summary` | Total devices and active/inactive counts; active means a reading in the last five minutes. |
| `v_average_temperature` | Fleet average temperature over the previous 24 hours. |
| `v_cpu_utilization` | Average and peak CPU per region for the previous hour. |
| `v_device_uptime_24h` | Received readings divided by expected readings over each device's observable 24-hour window. Assumes one reading every two seconds. |
| `v_device_failure_frequency_24h` | Critical-health and recovered telemetry-gap operational incident count and count per hour over the previous day. This is not a prediction of hardware failure. |
| `v_device_anomaly_counts_24h` | Anomaly and telemetry counts per device over the previous day. |
| `v_regional_performance_24h` | Regional health, CPU, latency, anomaly and failure summaries over the previous day. |
| `v_regional_anomaly_counts_24h` | Regional anomaly and telemetry counts over the previous day. |

These are ordinary PostgreSQL views, so their rolling time windows use query-time `CURRENT_TIMESTAMP` and do not require refresh jobs. See [`queries/sample_queries.sql`](queries/sample_queries.sql) for examples.

Airflow also creates and maintains `iot_hourly_analytics` and `iot_daily_operational_summary`; their cadence, aggregation definitions, and idempotency keys are documented in [`../airflow/dags/README.md`](../airflow/dags/README.md).

