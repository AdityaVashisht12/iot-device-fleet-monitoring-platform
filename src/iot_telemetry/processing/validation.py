"""Telemetry schema validation and UTC normalization."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SENSOR_RANGES: dict[str, tuple[float, float]] = {
    "temperature_c": (-40.0, 125.0),
    "cpu_usage_pct": (0.0, 100.0),
    "memory_usage_pct": (0.0, 100.0),
    "network_latency_ms": (0.0, 60_000.0),
    "packet_loss_pct": (0.0, 100.0),
    "battery_level_pct": (0.0, 100.0),
}
REQUIRED_STRING_FIELDS = ("event_id", "device_id", "region")


@dataclass(frozen=True, slots=True)
class ValidationResult:
    event: dict[str, Any] | None
    errors: tuple[str, ...] = ()
    missing_metrics: tuple[str, ...] = ()

    @property
    def is_valid(self) -> bool:
        return self.event is not None and not self.errors


def validate_event(raw_event: Any, *, ingestion_time: datetime | None = None) -> ValidationResult:
    """Validate one decoded JSON value and normalize its timestamp and optional metrics."""
    if not isinstance(raw_event, dict):
        return ValidationResult(None, ("event must be a JSON object",))

    errors: list[str] = []
    for field in REQUIRED_STRING_FIELDS:
        value = raw_event.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field} must be a non-empty string")
    if "status" in raw_event and raw_event["status"] is not None and not isinstance(raw_event["status"], str):
        errors.append("status must be a string or null")
    if "anomaly" in raw_event and raw_event["anomaly"] is not None and not isinstance(raw_event["anomaly"], str):
        errors.append("anomaly must be a string or null")

    event_time = _parse_timestamp(raw_event.get("timestamp"), errors)
    normalized: dict[str, Any] = dict(raw_event)
    missing_metrics: list[str] = []
    for field, (lower, upper) in SENSOR_RANGES.items():
        value = raw_event.get(field)
        if value is None:
            normalized[field] = None
            missing_metrics.append(field)
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"{field} must be a number or null")
            continue
        try:
            numeric = float(value)
        except OverflowError:
            errors.append(f"{field} must be a finite number")
            continue
        if not math.isfinite(numeric):
            errors.append(f"{field} must be finite")
        elif not lower <= numeric <= upper:
            errors.append(f"{field} must be in range [{lower}, {upper}]")
        else:
            normalized[field] = numeric

    if errors:
        return ValidationResult(None, tuple(errors), tuple(missing_metrics))

    received_at = ingestion_time or datetime.now(UTC)
    if received_at.tzinfo is None:
        return ValidationResult(None, ("ingestion_time must be timezone-aware",))
    normalized["timestamp"] = event_time.isoformat()
    normalized["ingestion_timestamp"] = received_at.astimezone(UTC).isoformat()
    return ValidationResult(normalized, (), tuple(missing_metrics))


def _parse_timestamp(value: Any, errors: list[str]) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        errors.append("timestamp must be a non-empty ISO-8601 string")
        return None
    if "T" not in value and "t" not in value and " " not in value:
        errors.append("timestamp must include a date and time")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        errors.append("timestamp must be a valid ISO-8601 datetime")
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)

