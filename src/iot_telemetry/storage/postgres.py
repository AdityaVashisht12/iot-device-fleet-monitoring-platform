"""PostgreSQL persistence for raw, processed, invalid telemetry, and alerts."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)
SCHEMA_PATHS = (
    Path(__file__).resolve().parents[3] / "postgres" / "schema.sql",
    Path.cwd() / "postgres" / "schema.sql",
)
FAILURE_GAP_THRESHOLD_SECONDS = 6.0


class PostgresRepository:
    """Transactional writes with event-ID uniqueness for replay safety."""

    def __init__(self, connection: Any | None = None, *, connect_kwargs: dict[str, Any] | None = None) -> None:
        if connection is None:
            try:
                import psycopg
            except ImportError as exc:
                raise RuntimeError("Install psycopg[binary] to use PostgreSQL storage") from exc
            connection = psycopg.connect(**(connect_kwargs or {}))
        self.connection = connection

    @classmethod
    def from_config(cls, config: Any) -> "PostgresRepository":
        return cls(
            connect_kwargs={
                "host": config.postgres_host,
                "port": config.postgres_port,
                "dbname": config.postgres_db,
                "user": config.postgres_user,
                "password": config.postgres_password,
                "application_name": "iot-telemetry-consumer",
            }
        )

    def initialize(self) -> None:
        """Apply the checked-in schema before consuming records."""
        schema_path = next((path for path in SCHEMA_PATHS if path.is_file()), None)
        if schema_path is None:
            raise RuntimeError(f"Could not locate postgres/schema.sql; checked {SCHEMA_PATHS}")
        schema_sql = schema_path.read_text(encoding="utf-8")
        with self.connection.cursor() as cursor:
            for statement in schema_sql.split(";"):
                if statement.strip():
                    cursor.execute(statement)
        self.connection.commit()
        LOGGER.info("PostgreSQL telemetry schema is ready", extra={"schema_path": str(schema_path)})

    def persist_valid(self, event: dict[str, Any], raw_payload: str | None = None) -> bool:
        """Atomically register a device, store raw/processed data, and create alerts.

        Returns False for an event ID already present in raw_telemetry.
        """
        event_timestamp = event["timestamp"]
        operational_status = "online" if event["performance_status"] == "healthy" else event["performance_status"]
        source_json = raw_payload if raw_payload is not None else json.dumps(event, allow_nan=False)
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXTRACT(EPOCH FROM (%s::timestamptz - last_seen_at))
                    FROM devices WHERE device_id = %s FOR UPDATE
                    """,
                    (event_timestamp, event["device_id"]),
                )
                previous = cursor.fetchone()
                gap_seconds = float(previous[0]) if previous and previous[0] is not None else None
                if gap_seconds is not None and gap_seconds < 0:
                    gap_seconds = None
                cursor.execute(
                    """
                    INSERT INTO devices (
                        device_id, region, operational_status, first_seen_at, last_seen_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (device_id) DO UPDATE SET
                        region = EXCLUDED.region,
                        first_seen_at = LEAST(devices.first_seen_at, EXCLUDED.first_seen_at),
                        last_seen_at = GREATEST(devices.last_seen_at, EXCLUDED.last_seen_at),
                        operational_status = CASE
                            WHEN EXCLUDED.last_seen_at >= devices.last_seen_at
                            THEN EXCLUDED.operational_status
                            ELSE devices.operational_status
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        event["device_id"], event["region"], operational_status,
                        event_timestamp, event_timestamp,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO raw_telemetry (
                        event_id, device_id, event_timestamp, ingestion_timestamp, raw_payload
                    ) VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING raw_telemetry_id
                    """,
                    (
                        event["event_id"], event["device_id"], event_timestamp,
                        event["ingestion_timestamp"], source_json,
                    ),
                )
                raw_row = cursor.fetchone()
                if raw_row is None:
                    return False
                raw_telemetry_id = raw_row[0]
                cursor.execute(
                    """
                    INSERT INTO processed_telemetry (
                        raw_telemetry_id, event_id, device_id, event_timestamp, ingestion_timestamp,
                        temperature_c, cpu_usage_pct, memory_usage_pct, network_latency_ms,
                        packet_loss_pct, battery_level_pct, device_health_score,
                        performance_status, is_anomaly, anomaly_score, anomaly_severity
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        raw_telemetry_id, event["event_id"], event["device_id"],
                        event_timestamp, event["ingestion_timestamp"],
                        event.get("temperature_c"), event.get("cpu_usage_pct"),
                        event.get("memory_usage_pct"), event.get("network_latency_ms"),
                        event.get("packet_loss_pct"), event.get("battery_level_pct"),
                        event["device_health_score"], event["performance_status"],
                        event["is_anomaly"], event["anomaly_score"], event.get("anomaly_severity"),
                    ),
                )
                self._insert_alerts(cursor, event, gap_seconds)
        return True

    @classmethod
    def _insert_alerts(
        cls,
        cursor: Any,
        event: dict[str, Any],
        gap_seconds: float | None = None,
    ) -> None:
        alert_timestamp = event["timestamp"]
        base_details: dict[str, Any] = {
            "device_health_score": event["device_health_score"],
            "performance_status": event["performance_status"],
            "anomaly_score": event["anomaly_score"],
        }
        if event["is_anomaly"]:
            cls._upsert_open_incident(
                cursor,
                event,
                alert_type="anomaly",
                severity=event["anomaly_severity"] or "LOW",
                details={**base_details, "source_anomaly": event.get("anomaly")},
            )
        else:
            cls._resolve_open_incident(cursor, event["device_id"], "anomaly", alert_timestamp)

        if event["performance_status"] == "critical":
            cls._upsert_open_incident(
                cursor,
                event,
                alert_type="health_critical",
                severity="CRITICAL",
                details={**base_details, "incident_type": "critical_health"},
            )
        else:
            cls._resolve_open_incident(cursor, event["device_id"], "health_critical", alert_timestamp)

        gap_failure = gap_seconds is not None and gap_seconds >= FAILURE_GAP_THRESHOLD_SECONDS
        if gap_failure:
            # A gap alert is a completed connectivity incident: this event is the recovery.
            gap_details = {
                **base_details,
                "incident_type": "telemetry_gap",
                "telemetry_gap_seconds": gap_seconds,
            }
            cursor.execute(
                """
                INSERT INTO device_alerts (
                    device_id, event_id, alert_type, severity, alert_timestamp,
                    last_seen_at, resolved_at, occurrence_count, details
                ) VALUES (
                    %s, %s, 'telemetry_gap', 'HIGH',
                    %s::timestamptz - (%s * INTERVAL '1 second') + (%s * INTERVAL '1 second'),
                    %s, %s, 1, %s::jsonb
                )
                """,
                (
                    event["device_id"], event["event_id"], event["timestamp"],
                    gap_seconds, FAILURE_GAP_THRESHOLD_SECONDS,
                    event["timestamp"], event["timestamp"], json.dumps(gap_details),
                ),
            )

    @staticmethod
    def _upsert_open_incident(
        cursor: Any,
        event: dict[str, Any],
        *,
        alert_type: str,
        severity: str,
        details: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            INSERT INTO device_alerts (
                device_id, event_id, alert_type, severity, alert_timestamp,
                last_seen_at, occurrence_count, details
            ) VALUES (%s, %s, %s, %s, %s, %s, 1, %s::jsonb)
            ON CONFLICT (device_id, alert_type) WHERE resolved_at IS NULL
            DO UPDATE SET
                event_id = EXCLUDED.event_id,
                last_seen_at = GREATEST(device_alerts.last_seen_at, EXCLUDED.last_seen_at),
                severity = CASE
                    WHEN CASE device_alerts.severity
                        WHEN 'LOW' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'HIGH' THEN 3 ELSE 4 END
                      >= CASE EXCLUDED.severity
                        WHEN 'LOW' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'HIGH' THEN 3 ELSE 4 END
                    THEN device_alerts.severity ELSE EXCLUDED.severity
                END,
                occurrence_count = device_alerts.occurrence_count + 1,
                details = EXCLUDED.details
            """,
            (
                event["device_id"], event["event_id"], alert_type, severity,
                event["timestamp"], event["timestamp"], json.dumps(details),
            ),
        )

    @staticmethod
    def _resolve_open_incident(cursor: Any, device_id: str, alert_type: str, timestamp: str) -> None:
        cursor.execute(
            """
            UPDATE device_alerts
            SET resolved_at = GREATEST(%s::timestamptz, alert_timestamp)
            WHERE device_id = %s AND alert_type = %s AND resolved_at IS NULL
              AND alert_timestamp <= %s::timestamptz
            """,
            (timestamp, device_id, alert_type, timestamp),
        )

    def persist_invalid(self, raw_payload: str, errors: tuple[str, ...], event_id: str | None = None) -> bool:
        """Store a rejected source record once using a content hash."""
        digest = hashlib.sha256(raw_payload.encode("utf-8", errors="replace")).hexdigest()
        sql = """
            INSERT INTO invalid_telemetry_events (record_hash, event_id, raw_payload, validation_errors)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (record_hash) DO NOTHING
        """
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (digest, event_id, raw_payload, json.dumps(errors)))
                inserted = cursor.rowcount == 1
        return inserted

    def close(self) -> None:
        self.connection.close()

