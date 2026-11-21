"""Configuration for the MQTT-to-Kafka ingestion service."""

from __future__ import annotations

import os
import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MqttKafkaConfig:
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_client_id: str = "iot-telemetry-ingestor"
    mqtt_keepalive: int = 60
    mqtt_topic: str = "iot/devices/+/telemetry"
    mqtt_qos: int = 1
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "iot-telemetry"
    kafka_client_id: str = "iot-mqtt-kafka-bridge"
    kafka_delivery_timeout_seconds: float = 10.0
    kafka_max_retries: int = 3
    kafka_retry_backoff_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not self.mqtt_host.strip() or not self.mqtt_client_id.strip():
            raise ValueError("MQTT host and client ID must be non-empty")
        if not 1 <= self.mqtt_port <= 65535:
            raise ValueError("MQTT_PORT must be between 1 and 65535")
        if self.mqtt_qos not in (1, 2):
            raise ValueError("MQTT_INGEST_QOS must be 1 or 2 to support delivery acknowledgements")
        if not self.kafka_bootstrap_servers.strip() or not self.kafka_topic.strip():
            raise ValueError("Kafka bootstrap servers and topic must be non-empty")
        if self.kafka_max_retries < 0:
            raise ValueError("KAFKA_MAX_RETRIES must be zero or greater")
        if (
            not math.isfinite(self.kafka_delivery_timeout_seconds)
            or not math.isfinite(self.kafka_retry_backoff_seconds)
            or self.kafka_delivery_timeout_seconds <= 0
            or self.kafka_retry_backoff_seconds < 0
        ):
            raise ValueError("Kafka delivery timeout must be positive and retry backoff non-negative")

    @classmethod
    def from_env(cls) -> "MqttKafkaConfig":
        """Build settings from the process environment."""
        return cls(
            mqtt_host=os.getenv("MQTT_HOST", "localhost"),
            mqtt_port=_int_env("MQTT_PORT", 1883),
            mqtt_client_id=os.getenv("MQTT_INGEST_CLIENT_ID", "iot-telemetry-ingestor"),
            mqtt_keepalive=_positive_int_env("MQTT_KEEPALIVE", 60),
            mqtt_topic=os.getenv("MQTT_INGEST_TOPIC", "iot/devices/+/telemetry"),
            mqtt_qos=_int_env("MQTT_INGEST_QOS", 1),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TELEMETRY_TOPIC", "iot-telemetry"),
            kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "iot-mqtt-kafka-bridge"),
            kafka_delivery_timeout_seconds=_float_env("KAFKA_DELIVERY_TIMEOUT_SECONDS", 10.0),
            kafka_max_retries=_int_env("KAFKA_MAX_RETRIES", 3),
            kafka_retry_backoff_seconds=_float_env("KAFKA_RETRY_BACKOFF_SECONDS", 1.0),
        )


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _positive_int_env(name: str, default: int) -> int:
    value = _int_env(name, default)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc

