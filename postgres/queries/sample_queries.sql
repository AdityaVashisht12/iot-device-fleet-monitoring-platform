-- 1) Fleet totals and current activity (active = seen within five minutes).
SELECT * FROM v_device_activity_summary;

-- 2) All-time latest readings and current state for each device.
SELECT
    d.device_id,
    d.region,
    d.operational_status,
    d.last_seen_at,
    p.temperature_c,
    p.cpu_usage_pct,
    p.device_health_score,
    p.performance_status
FROM devices AS d
LEFT JOIN LATERAL (
    SELECT *
    FROM processed_telemetry AS p
    WHERE p.device_id = d.device_id
    ORDER BY p.event_timestamp DESC
    LIMIT 1
) AS p ON TRUE
ORDER BY d.region, d.device_id;

-- 3) Average fleet temperature during the last day.
SELECT * FROM v_average_temperature;

-- 4) CPU utilization per region during the last hour.
SELECT * FROM v_cpu_utilization ORDER BY average_cpu_usage_pct DESC;

-- 5) Device uptime estimates for the last 24 hours (two-second expected cadence).
SELECT * FROM v_device_uptime_24h ORDER BY uptime_pct ASC, device_id;

-- 6) Failure frequency and anomaly counts by device.
SELECT
    d.device_id,
    d.region,
    f.failures_24h,
    f.failures_per_hour,
    a.anomaly_count_24h,
    a.telemetry_count_24h
FROM devices AS d
JOIN v_device_failure_frequency_24h AS f USING (device_id)
JOIN v_device_anomaly_counts_24h AS a USING (device_id)
ORDER BY f.failures_24h DESC, a.anomaly_count_24h DESC;

-- 7) Regional performance and anomaly/failure summary.
SELECT * FROM v_regional_performance_24h ORDER BY average_health_score DESC;

-- 8) Inspect recent severe alerts.
SELECT alert_timestamp, last_seen_at, resolved_at, occurrence_count,
       device_id, event_id, alert_type, severity, details
FROM device_alerts
WHERE severity = 'CRITICAL'
ORDER BY alert_timestamp DESC
LIMIT 100;

-- 9) Inspect recent rejected Kafka records.
SELECT received_at, event_id, validation_errors, raw_payload
FROM invalid_telemetry_events
ORDER BY received_at DESC
LIMIT 100;
