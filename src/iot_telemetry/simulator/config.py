"""Validated environment-backed simulator settings."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _positive_int(name: str, value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _positive_float(name: str, value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


@dataclass(frozen=True, slots=True)
class SimulatorConfig:
    """Runtime settings; defaults target local Mosquitto."""

    device_count: int = 500
    interval_seconds: float = 2.0
    speed_multiplier: float = 1.0
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_keepalive: int = 60
    mqtt_qos: int = 1
    abnormal_probability: float = 0.01
    failure_probability: float = 0.0005
    min_failure_ticks: int = 3
    max_failure_ticks: int = 15
    random_seed: int | None = None

    def __post_init__(self) -> None:
        if self.device_count <= 0:
            raise ValueError("device_count must be greater than zero")
        if self.interval_seconds <= 0 or self.speed_multiplier <= 0:
            raise ValueError("interval_seconds and speed_multiplier must be greater than zero")
        if not 1 <= self.mqtt_port <= 65535:
            raise ValueError("mqtt_port must be between 1 and 65535")
        if self.mqtt_qos not in (0, 1, 2):
            raise ValueError("mqtt_qos must be 0, 1, or 2")
        if not 0 <= self.abnormal_probability <= 1:
            raise ValueError("abnormal_probability must be between 0 and 1")
        if not 0 <= self.failure_probability <= 1:
            raise ValueError("failure_probability must be between 0 and 1")
        if self.min_failure_ticks <= 0 or self.max_failure_ticks < self.min_failure_ticks:
            raise ValueError("failure tick range is invalid")
        if not self.mqtt_host.strip():
            raise ValueError("mqtt_host cannot be empty")

    @property
    def tick_interval_seconds(self) -> float:
        """Wall-clock interval for a complete fleet tick."""
        return self.interval_seconds / self.speed_multiplier

    @classmethod
    def from_env(cls) -> "SimulatorConfig":
        """Read simulator settings from environment variables."""
        env = os.environ
        count = _positive_int("SIMULATOR_DEVICE_COUNT", env.get("SIMULATOR_DEVICE_COUNT", "500"))
        interval = _positive_float("SIMULATOR_INTERVAL_SECONDS", env.get("SIMULATOR_INTERVAL_SECONDS", "2"))
        speed = _positive_float("SIMULATOR_SPEED_MULTIPLIER", env.get("SIMULATOR_SPEED_MULTIPLIER", "1"))
        port = _positive_int("MQTT_PORT", env.get("MQTT_PORT", "1883"))
        try:
            qos = int(env.get("SIMULATOR_MQTT_QOS", "1"))
        except ValueError as exc:
            raise ValueError("SIMULATOR_MQTT_QOS must be an integer") from exc
        abnormal = float(env.get("SIMULATOR_ABNORMAL_PROBABILITY", "0.01"))
        failure = float(env.get("SIMULATOR_FAILURE_PROBABILITY", "0.0005"))
        seed_raw = env.get("SIMULATOR_RANDOM_SEED")
        seed = int(seed_raw) if seed_raw else None
        return cls(
            device_count=count,
            interval_seconds=interval,
            speed_multiplier=speed,
            mqtt_host=env.get("MQTT_HOST", "localhost"),
            mqtt_port=port,
            mqtt_qos=qos,
            abnormal_probability=abnormal,
            failure_probability=failure,
            min_failure_ticks=_positive_int("SIMULATOR_MIN_FAILURE_TICKS", env.get("SIMULATOR_MIN_FAILURE_TICKS", "3")),
            max_failure_ticks=_positive_int("SIMULATOR_MAX_FAILURE_TICKS", env.get("SIMULATOR_MAX_FAILURE_TICKS", "15")),
            random_seed=seed,
        )

