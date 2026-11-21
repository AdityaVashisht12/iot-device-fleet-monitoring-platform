"""Unit tests for simulator configuration and event generation."""

from datetime import UTC, datetime
import json
import random

import pytest

from iot_telemetry.simulator import DeviceSimulator, SimulatorConfig
from iot_telemetry.simulator.generator import REGIONS


def test_default_fleet_has_500_unique_devices_distributed_across_regions() -> None:
    simulator = DeviceSimulator(SimulatorConfig(random_seed=7))

    assert len(simulator.devices) == 500
    assert len({device.device_id for device in simulator.devices}) == 500
    assert {device.region for device in simulator.devices} == set(REGIONS)


def test_event_has_expected_metrics_and_json_serializes() -> None:
    simulator = DeviceSimulator(SimulatorConfig(device_count=1, random_seed=11))
    now = datetime(2026, 1, 1, tzinfo=UTC)

    event = simulator.generate_event(simulator.devices[0], now=now)
    decoded = json.loads(event.to_json())

    assert decoded["device_id"] == "device-000001"
    assert decoded["timestamp"] == now.isoformat()
    assert decoded["region"] in REGIONS
    assert {"temperature_c", "cpu_usage_pct", "memory_usage_pct", "network_latency_ms", "packet_loss_pct", "battery_level_pct"} <= decoded.keys()
    assert decoded["status"] == "online"


def test_abnormal_readings_are_marked() -> None:
    simulator = DeviceSimulator(
        SimulatorConfig(device_count=1, abnormal_probability=1.0, random_seed=3),
        rng=random.Random(3),
    )

    event = simulator.generate_event(simulator.devices[0])

    assert event["status"] == "degraded"
    assert event["anomaly"] in {"temperature_c", "cpu_usage_pct", "memory_usage_pct", "network_latency_ms", "packet_loss_pct", "battery_level_pct"}


def test_failed_device_is_temporarily_silent() -> None:
    config = SimulatorConfig(
        device_count=1,
        failure_probability=1.0,
        min_failure_ticks=3,
        max_failure_ticks=3,
        random_seed=9,
    )
    simulator = DeviceSimulator(config)
    device = simulator.devices[0]

    assert simulator.generate_event(device) == {}
    assert simulator.generate_event(device) == {}
    assert simulator.generate_event(device) == {}
    # It retries the failure decision at the fourth tick; probability 1 keeps it offline.
    assert simulator.generate_event(device) == {}


def test_configuration_rejects_invalid_probabilities() -> None:
    with pytest.raises(ValueError, match="abnormal_probability"):
        SimulatorConfig(abnormal_probability=1.1)


def test_environment_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIMULATOR_DEVICE_COUNT", "12")
    monkeypatch.setenv("SIMULATOR_SPEED_MULTIPLIER", "2")
    monkeypatch.setenv("MQTT_HOST", "broker")

    config = SimulatorConfig.from_env()

    assert config.device_count == 12
    assert config.tick_interval_seconds == 1
    assert config.mqtt_host == "broker"

