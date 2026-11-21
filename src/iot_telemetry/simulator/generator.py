"""Stateful, correlated telemetry generation for a simulated device fleet."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from iot_telemetry.simulator.config import SimulatorConfig

REGIONS = ("north_america", "south_america", "europe", "asia_pacific", "africa", "middle_east")
METRICS = ("temperature_c", "cpu_usage_pct", "memory_usage_pct", "network_latency_ms", "packet_loss_pct", "battery_level_pct")


@dataclass(slots=True)
class DeviceState:
    device_id: str
    region: str
    temperature_c: float
    cpu_usage_pct: float
    memory_usage_pct: float
    network_latency_ms: float
    packet_loss_pct: float
    battery_level_pct: float
    failure_ticks_remaining: int = 0


class TelemetryEvent(dict[str, Any]):
    """Dictionary event with a stable JSON serializer."""

    def to_json(self) -> str:
        return json.dumps(self, separators=(",", ":"), allow_nan=False)


class DeviceSimulator:
    """Maintains unique devices and generates temporally correlated readings."""

    def __init__(self, config: SimulatorConfig, *, rng: random.Random | None = None) -> None:
        self.config = config
        self.rng = rng or random.Random(config.random_seed)
        self.devices = [self._new_device(i) for i in range(1, config.device_count + 1)]

    def _new_device(self, number: int) -> DeviceState:
        return DeviceState(
            device_id=f"device-{number:06d}",
            region=REGIONS[(number - 1) % len(REGIONS)],
            temperature_c=self.rng.uniform(18.0, 30.0),
            cpu_usage_pct=self.rng.uniform(15.0, 55.0),
            memory_usage_pct=self.rng.uniform(25.0, 65.0),
            network_latency_ms=self.rng.uniform(15.0, 100.0),
            packet_loss_pct=self.rng.uniform(0.0, 0.5),
            battery_level_pct=self.rng.uniform(55.0, 100.0),
        )

    def generate_event(self, device: DeviceState, *, now: datetime | None = None) -> TelemetryEvent:
        """Advance one device and return its event, or mark an outage with no event."""
        if device.failure_ticks_remaining > 0:
            device.failure_ticks_remaining -= 1
            return TelemetryEvent()
        if self.rng.random() < self.config.failure_probability:
            device.failure_ticks_remaining = self.rng.randint(
                self.config.min_failure_ticks, self.config.max_failure_ticks
            ) - 1
            return TelemetryEvent()

        # A shared load factor couples CPU, temperature, latency, and packet loss.
        load = self.rng.gauss(0.0, 3.0)
        device.cpu_usage_pct = self._bounded(device.cpu_usage_pct + load, 1.0, 98.0)
        device.temperature_c = self._bounded(
            device.temperature_c + 0.12 * load + self.rng.gauss(0.0, 0.25), 8.0, 75.0
        )
        device.memory_usage_pct = self._bounded(
            device.memory_usage_pct + 0.08 * load + self.rng.gauss(0.0, 0.4), 5.0, 99.0
        )
        device.network_latency_ms = self._bounded(
            device.network_latency_ms + max(0.0, load) * 1.6 + self.rng.gauss(0.0, 4.0), 2.0, 2500.0
        )
        device.packet_loss_pct = self._bounded(
            device.packet_loss_pct + max(0.0, load) * 0.025 + self.rng.gauss(0.0, 0.04), 0.0, 100.0
        )
        device.battery_level_pct = self._bounded(
            device.battery_level_pct - self.rng.uniform(0.0, 0.025), 0.0, 100.0
        )

        abnormal_metric: str | None = None
        if self.rng.random() < self.config.abnormal_probability:
            abnormal_metric = self.rng.choice(METRICS)
            self._inject_abnormal(device, abnormal_metric)

        timestamp = now or datetime.now(UTC)
        if timestamp.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        return TelemetryEvent(
            event_id=f"{device.device_id}-{timestamp.timestamp():.6f}",
            timestamp=timestamp.astimezone(UTC).isoformat(),
            device_id=device.device_id,
            region=device.region,
            status="degraded" if abnormal_metric else "online",
            anomaly=abnormal_metric,
            temperature_c=round(device.temperature_c, 2),
            cpu_usage_pct=round(device.cpu_usage_pct, 2),
            memory_usage_pct=round(device.memory_usage_pct, 2),
            network_latency_ms=round(device.network_latency_ms, 2),
            packet_loss_pct=round(device.packet_loss_pct, 3),
            battery_level_pct=round(device.battery_level_pct, 2),
        )

    def events_for_tick(self, *, now: datetime | None = None) -> list[TelemetryEvent]:
        """Generate all available device events for one fleet tick."""
        return [event for device in self.devices if (event := self.generate_event(device, now=now))]

    @staticmethod
    def _bounded(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))

    def _inject_abnormal(self, device: DeviceState, metric: str) -> None:
        if metric == "temperature_c":
            device.temperature_c = self.rng.choice((-1.0, 80.0))
        elif metric == "cpu_usage_pct":
            device.cpu_usage_pct = self.rng.choice((0.0, 100.0))
        elif metric == "memory_usage_pct":
            device.memory_usage_pct = self.rng.choice((0.0, 100.0))
        elif metric == "network_latency_ms":
            device.network_latency_ms = self.rng.uniform(1500.0, 3000.0)
        elif metric == "packet_loss_pct":
            device.packet_loss_pct = self.rng.uniform(20.0, 100.0)
        else:
            device.battery_level_pct = self.rng.uniform(0.0, 5.0)

