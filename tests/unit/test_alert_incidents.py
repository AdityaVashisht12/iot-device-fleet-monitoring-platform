"""Tests for database alert incident upsert and resolution SQL behavior."""

from iot_telemetry.storage.postgres import PostgresRepository


class RecordingCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.parameters: list[tuple] = []

    def execute(self, statement: str, parameters: tuple) -> None:
        self.statements.append(statement)
        self.parameters.append(parameters)


def _event(*, is_anomaly: bool = True, status: str = "degraded") -> dict:
    return {
        "device_id": "device-1",
        "event_id": "event-1",
        "timestamp": "2026-11-21T00:00:00+00:00",
        "device_health_score": 70.0,
        "performance_status": status,
        "anomaly_score": 0.72,
        "anomaly_severity": "HIGH" if is_anomaly else None,
        "anomaly": "temperature_c" if is_anomaly else None,
        "is_anomaly": is_anomaly,
    }


def test_repeated_anomaly_uses_open_incident_upsert_and_escalates_occurrence() -> None:
    cursor = RecordingCursor()

    PostgresRepository._insert_alerts(cursor, _event())

    assert len(cursor.statements) == 2  # anomaly upsert plus resolution check for health incidents
    assert "ON CONFLICT (device_id, alert_type) WHERE resolved_at IS NULL" in cursor.statements[0]
    assert "occurrence_count = device_alerts.occurrence_count + 1" in cursor.statements[0]
    assert cursor.parameters[0][3] == "HIGH"


def test_normal_reading_resolves_existing_anomaly_incident() -> None:
    cursor = RecordingCursor()

    PostgresRepository._insert_alerts(cursor, _event(is_anomaly=False, status="healthy"))

    assert any("UPDATE device_alerts" in statement for statement in cursor.statements)
    assert cursor.parameters[0][2] == "anomaly"
