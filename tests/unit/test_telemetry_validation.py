"""Tests for Kafka telemetry schema validation and UTC normalization."""

from datetime import UTC, datetime

import pytest

from iot_telemetry.processing.validation import SENSOR_RANGES, validate_event


def valid_payload() -> dict:
    return {
        "event_id": "evt-1",
        "device_id": "device-1",
        "region": "europe",
        "timestamp": "2026-11-21T10:00:00+05:30",
        "temperature_c": 22.5,
        "cpu_usage_pct": 35,
        "memory_usage_pct": 50,
        "network_latency_ms": 18,
        "packet_loss_pct": 0.1,
        "battery_level_pct": 90,
    }


def test_valid_event_normalizes_timestamp_to_utc_and_sets_ingestion_time() -> None:
    result = validate_event(valid_payload(), ingestion_time=datetime(2026, 11, 21, tzinfo=UTC))

    assert result.is_valid
    assert result.event["timestamp"] == "2026-11-21T04:30:00+00:00"
    assert result.event["ingestion_timestamp"] == "2026-11-21T00:00:00+00:00"


def test_missing_sensor_metrics_are_nullable_and_reported() -> None:
    payload = valid_payload()
    payload.pop("battery_level_pct")

    result = validate_event(payload)

    assert result.is_valid
    assert result.event["battery_level_pct"] is None
    assert result.missing_metrics == ("battery_level_pct",)


@pytest.mark.parametrize("field", list(SENSOR_RANGES))
def test_out_of_range_reading_is_rejected(field: str) -> None:
    payload = valid_payload()
    payload[field] = SENSOR_RANGES[field][1] + 1

    result = validate_event(payload)

    assert not result.is_valid
    assert any(field in error and "range" in error for error in result.errors)


def test_bool_is_not_accepted_as_numeric_sensor_reading() -> None:
    payload = valid_payload()
    payload["cpu_usage_pct"] = True

    result = validate_event(payload)

    assert not result.is_valid
    assert any("cpu_usage_pct must be a number" in error for error in result.errors)


def test_required_field_and_timestamp_errors_are_reported_together() -> None:
    payload = {"event_id": "", "device_id": "device-1", "region": "", "timestamp": "bad"}

    result = validate_event(payload)

    assert not result.is_valid
    assert len(result.errors) >= 3

