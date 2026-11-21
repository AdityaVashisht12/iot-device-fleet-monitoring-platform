"""Device health and performance status calculations."""

from __future__ import annotations

from typing import Any

SENSOR_FIELDS = (
    "temperature_c",
    "cpu_usage_pct",
    "memory_usage_pct",
    "network_latency_ms",
    "packet_loss_pct",
    "battery_level_pct",
)


def calculate_health(event: dict[str, Any], *, anomalous: bool = False) -> tuple[float, str]:
    """Return a 0-100 health score and healthy/degraded/critical status.

    Missing metrics are excluded from the mean, rather than silently treated as zero.
    """
    scores: list[float] = []
    temp = event.get("temperature_c")
    if temp is not None:
        scores.append(_range_score(float(temp), ideal_low=18, ideal_high=35, hard_low=-20, hard_high=85))
    for field in ("cpu_usage_pct", "memory_usage_pct"):
        value = event.get(field)
        if value is not None:
            scores.append(100.0 - float(value))
    latency = event.get("network_latency_ms")
    if latency is not None:
        scores.append(max(0.0, 100.0 - float(latency) / 10.0))
    loss = event.get("packet_loss_pct")
    if loss is not None:
        scores.append(max(0.0, 100.0 - float(loss) * 5.0))
    battery = event.get("battery_level_pct")
    if battery is not None:
        scores.append(float(battery))

    score = round(sum(scores) / len(scores), 2) if scores else 0.0
    if score < 50.0:
        status = "critical"
    elif score < 80.0 or anomalous:
        status = "degraded"
    else:
        status = "healthy"
    return score, status


def _range_score(value: float, *, ideal_low: float, ideal_high: float, hard_low: float, hard_high: float) -> float:
    if ideal_low <= value <= ideal_high:
        return 100.0
    if value < ideal_low:
        return max(0.0, 100.0 * (value - hard_low) / (ideal_low - hard_low))
    return max(0.0, 100.0 * (hard_high - value) / (hard_high - ideal_high))

