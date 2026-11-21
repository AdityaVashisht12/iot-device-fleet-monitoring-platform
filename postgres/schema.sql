-- IoT telemetry relational model and analytical views.
-- Devices are registered on first accepted event by the Kafka consumer.

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    region TEXT NOT NULL,
    operational_status TEXT NOT NULL DEFAULT 'online'
        CHECK (operational_status IN ('online', 'degraded', 'critical', 'offline')),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (last_seen_at >= first_seen_at)
);

CREATE TABLE IF NOT EXISTS raw_telemetry (
    raw_telemetry_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (device_id, event_id)
);

CREATE TABLE IF NOT EXISTS processed_telemetry (
    raw_telemetry_id BIGINT PRIMARY KEY
        REFERENCES raw_telemetry(raw_telemetry_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL UNIQUE
        REFERENCES raw_telemetry(event_id) ON UPDATE CASCADE ON DELETE CASCADE,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION CHECK (temperature_c IS NULL OR temperature_c BETWEEN -40 AND 125),
    cpu_usage_pct DOUBLE PRECISION CHECK (cpu_usage_pct IS NULL OR cpu_usage_pct BETWEEN 0 AND 100),
    memory_usage_pct DOUBLE PRECISION CHECK (memory_usage_pct IS NULL OR memory_usage_pct BETWEEN 0 AND 100),
    network_latency_ms DOUBLE PRECISION CHECK (network_latency_ms IS NULL OR network_latency_ms BETWEEN 0 AND 60000),
    packet_loss_pct DOUBLE PRECISION CHECK (packet_loss_pct IS NULL OR packet_loss_pct BETWEEN 0 AND 100),
    battery_level_pct DOUBLE PRECISION CHECK (battery_level_pct IS NULL OR battery_level_pct BETWEEN 0 AND 100),
    device_health_score DOUBLE PRECISION NOT NULL CHECK (device_health_score BETWEEN 0 AND 100),
    performance_status TEXT NOT NULL CHECK (performance_status IN ('healthy', 'degraded', 'critical')),
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_score DOUBLE PRECISION,
    anomaly_severity TEXT CHECK (anomaly_severity IS NULL OR anomaly_severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS device_alerts (
    alert_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    event_id TEXT NOT NULL REFERENCES processed_telemetry(event_id) ON DELETE CASCADE,
    alert_type TEXT NOT NULL CHECK (alert_type IN ('anomaly', 'telemetry_gap', 'health_critical')),
    severity TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    alert_timestamp TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    occurrence_count BIGINT NOT NULL DEFAULT 1 CHECK (occurrence_count > 0),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT device_alerts_resolved_at_check
        CHECK (resolved_at IS NULL OR resolved_at >= alert_timestamp)
);

-- Upgrade existing development schemas created by earlier pipeline iterations.
ALTER TABLE processed_telemetry ADD COLUMN IF NOT EXISTS anomaly_score DOUBLE PRECISION;
ALTER TABLE processed_telemetry ADD COLUMN IF NOT EXISTS anomaly_severity TEXT;
ALTER TABLE processed_telemetry DROP CONSTRAINT IF EXISTS processed_telemetry_anomaly_severity_check;
ALTER TABLE processed_telemetry ADD CONSTRAINT processed_telemetry_anomaly_severity_check
    CHECK (anomaly_severity IS NULL OR anomaly_severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'));

ALTER TABLE device_alerts ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
UPDATE device_alerts SET last_seen_at = alert_timestamp WHERE last_seen_at IS NULL;
ALTER TABLE device_alerts ALTER COLUMN last_seen_at SET NOT NULL;
ALTER TABLE device_alerts ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
ALTER TABLE device_alerts ADD COLUMN IF NOT EXISTS occurrence_count BIGINT NOT NULL DEFAULT 1;
ALTER TABLE device_alerts DROP CONSTRAINT IF EXISTS device_alerts_occurrence_count_check;
ALTER TABLE device_alerts ADD CONSTRAINT device_alerts_occurrence_count_check CHECK (occurrence_count > 0);
ALTER TABLE device_alerts DROP CONSTRAINT IF EXISTS device_alerts_resolved_at_check;
ALTER TABLE device_alerts ADD CONSTRAINT device_alerts_resolved_at_check
    CHECK (resolved_at IS NULL OR resolved_at >= alert_timestamp);
ALTER TABLE device_alerts DROP CONSTRAINT IF EXISTS device_alerts_severity_check;
UPDATE device_alerts SET severity = CASE severity
    WHEN 'warning' THEN 'MEDIUM'
    WHEN 'critical' THEN 'CRITICAL'
    ELSE severity
END;
ALTER TABLE device_alerts ADD CONSTRAINT device_alerts_severity_check
    CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'));
ALTER TABLE device_alerts DROP CONSTRAINT IF EXISTS device_alerts_alert_type_check;
UPDATE device_alerts SET alert_type = CASE
    WHEN alert_type = 'device_failure' AND details->>'failure_reason' = 'telemetry_gap' THEN 'telemetry_gap'
    WHEN alert_type = 'device_failure' THEN 'health_critical'
    ELSE alert_type
END;
ALTER TABLE device_alerts ADD CONSTRAINT device_alerts_alert_type_check
    CHECK (alert_type IN ('anomaly', 'telemetry_gap', 'health_critical'));
ALTER TABLE device_alerts DROP CONSTRAINT IF EXISTS device_alerts_event_id_alert_type_key;
WITH ranked_open_alerts AS (
    SELECT alert_id,
           ROW_NUMBER() OVER (PARTITION BY device_id, alert_type ORDER BY alert_timestamp DESC, alert_id DESC) AS rn
    FROM device_alerts
    WHERE resolved_at IS NULL
)
UPDATE device_alerts AS a
SET resolved_at = a.alert_timestamp
FROM ranked_open_alerts AS r
WHERE a.alert_id = r.alert_id AND r.rn > 1;
CREATE UNIQUE INDEX IF NOT EXISTS uq_device_alerts_open_incident
    ON device_alerts (device_id, alert_type) WHERE resolved_at IS NULL;

-- Invalid source records are retained separately and deduplicated by payload hash.
CREATE TABLE IF NOT EXISTS invalid_telemetry_events (
    record_hash TEXT PRIMARY KEY,
    event_id TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_payload TEXT NOT NULL,
    validation_errors JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_telemetry_device_time
    ON raw_telemetry (device_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_devices_last_seen
    ON devices (last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_telemetry_event_time
    ON raw_telemetry (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_processed_telemetry_device_time
    ON processed_telemetry (device_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_processed_telemetry_event_time
    ON processed_telemetry (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_processed_telemetry_region_time
    ON processed_telemetry (event_timestamp DESC, device_id);
CREATE INDEX IF NOT EXISTS idx_device_alerts_device_time
    ON device_alerts (device_id, alert_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_device_alerts_time
    ON device_alerts (alert_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_invalid_telemetry_received
    ON invalid_telemetry_events (received_at DESC);

-- Activity is based on the last five minutes, independent of the stored status.
CREATE OR REPLACE VIEW v_device_activity_summary AS
SELECT
    COUNT(*)::BIGINT AS total_devices,
    COUNT(*) FILTER (WHERE last_seen_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes')::BIGINT AS active_devices,
    COUNT(*) FILTER (WHERE last_seen_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes')::BIGINT AS inactive_devices
FROM devices;

CREATE OR REPLACE VIEW v_average_temperature AS
SELECT
    AVG(temperature_c) AS average_temperature_c,
    COUNT(temperature_c)::BIGINT AS sample_count,
    MAX(event_timestamp) AS latest_sample_at
FROM processed_telemetry
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

CREATE OR REPLACE VIEW v_cpu_utilization AS
SELECT
    d.region,
    AVG(p.cpu_usage_pct) AS average_cpu_usage_pct,
    MAX(p.cpu_usage_pct) AS peak_cpu_usage_pct,
    COUNT(p.cpu_usage_pct)::BIGINT AS sample_count
FROM processed_telemetry AS p
JOIN devices AS d ON d.device_id = p.device_id
WHERE p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY d.region;

-- Assumes a configured/default simulator cadence of one event every two seconds.
CREATE OR REPLACE VIEW v_device_uptime_24h AS
WITH windows AS (
    SELECT
        d.device_id,
        GREATEST(d.first_seen_at, CURRENT_TIMESTAMP - INTERVAL '24 hours') AS window_start,
        LEAST(CURRENT_TIMESTAMP, d.first_seen_at + INTERVAL '24 hours') AS window_end
    FROM devices AS d
), counts AS (
    SELECT
        w.device_id,
        COUNT(p.raw_telemetry_id)::BIGINT AS received_events,
        GREATEST(1, CEIL(EXTRACT(EPOCH FROM (w.window_end - w.window_start)) / 2.0)) AS expected_events
    FROM windows AS w
    LEFT JOIN processed_telemetry AS p
        ON p.device_id = w.device_id
       AND p.event_timestamp >= w.window_start
       AND p.event_timestamp <= w.window_end
    GROUP BY w.device_id, w.window_start, w.window_end
)
SELECT
    device_id,
    received_events,
    expected_events::BIGINT AS expected_events,
    LEAST(100.0, 100.0 * received_events / expected_events) AS uptime_pct
FROM counts;

CREATE OR REPLACE VIEW v_device_failure_frequency_24h AS
SELECT
    d.device_id,
    COUNT(a.alert_id) FILTER (
        WHERE a.alert_type IN ('telemetry_gap', 'health_critical')
          AND a.alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    )::BIGINT AS failures_24h,
    COUNT(a.alert_id) FILTER (
        WHERE a.alert_type IN ('telemetry_gap', 'health_critical')
          AND a.alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    ) / 24.0 AS failures_per_hour
FROM devices AS d
LEFT JOIN device_alerts AS a ON a.device_id = d.device_id
GROUP BY d.device_id;

CREATE OR REPLACE VIEW v_device_anomaly_counts_24h AS
SELECT
    d.device_id,
    COUNT(*) FILTER (WHERE p.is_anomaly)::BIGINT AS anomaly_count_24h,
    COUNT(*)::BIGINT AS telemetry_count_24h
FROM devices AS d
LEFT JOIN processed_telemetry AS p
    ON p.device_id = d.device_id
   AND p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY d.device_id;

CREATE OR REPLACE VIEW v_regional_performance_24h AS
WITH telemetry AS (
    SELECT
        d.region,
        AVG(p.device_health_score) AS average_health_score,
        AVG(p.cpu_usage_pct) AS average_cpu_usage_pct,
        AVG(p.network_latency_ms) AS average_network_latency_ms,
        COUNT(*) FILTER (WHERE p.is_anomaly)::BIGINT AS anomaly_count,
        COUNT(*)::BIGINT AS telemetry_count
    FROM processed_telemetry AS p
    JOIN devices AS d ON d.device_id = p.device_id
    WHERE p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    GROUP BY d.region
), failures AS (
    SELECT d.region, COUNT(a.alert_id)::BIGINT AS failure_count
    FROM device_alerts AS a
    JOIN devices AS d ON d.device_id = a.device_id
    WHERE a.alert_type IN ('telemetry_gap', 'health_critical')
      AND a.alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    GROUP BY d.region
)
SELECT
    t.region,
    t.average_health_score,
    t.average_cpu_usage_pct,
    t.average_network_latency_ms,
    t.anomaly_count,
    COALESCE(f.failure_count, 0)::BIGINT AS failure_count_24h,
    t.telemetry_count
FROM telemetry AS t
LEFT JOIN failures AS f ON f.region = t.region;

CREATE OR REPLACE VIEW v_regional_anomaly_counts_24h AS
SELECT
    d.region,
    COUNT(*) FILTER (WHERE p.is_anomaly)::BIGINT AS anomaly_count_24h,
    COUNT(*)::BIGINT AS telemetry_count_24h
FROM processed_telemetry AS p
JOIN devices AS d ON d.device_id = p.device_id
WHERE p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY d.region;
