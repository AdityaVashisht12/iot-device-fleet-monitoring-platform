"""Environment-backed settings for the PostgreSQL consumer."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TelemetryConsumerConfig:
    bootstrap_servers: str = "localhost:9092"
    topic: str = "iot-telemetry"
    group_id: str = "iot-telemetry-postgres"
    client_id: str = "iot-telemetry-consumer"
    auto_offset_reset: str = "earliest"
    poll_timeout_seconds: float = 1.0
    anomaly_model_path: str = "models/telemetry_isolation_forest.joblib"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "iot_telemetry"
    postgres_user: str = "iot_app"
    postgres_password: str = "change-me-local"

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip() or not self.topic.strip() or not self.group_id.strip():
            raise ValueError("Kafka bootstrap servers, topic, and group ID must not be empty")
        if self.auto_offset_reset not in {"earliest", "latest", "error"}:
            raise ValueError("KAFKA_AUTO_OFFSET_RESET must be earliest, latest, or error")
        if not math.isfinite(self.poll_timeout_seconds) or self.poll_timeout_seconds <= 0:
            raise ValueError("KAFKA_POLL_TIMEOUT_SECONDS must be positive")
        if not self.anomaly_model_path.strip():
            raise ValueError("ANOMALY_MODEL_PATH must not be empty")
        if not 1 <= self.postgres_port <= 65535:
            raise ValueError("POSTGRES_PORT must be between 1 and 65535")

    @classmethod
    def from_env(cls) -> "TelemetryConsumerConfig":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TELEMETRY_TOPIC", "iot-telemetry"),
            group_id=os.getenv("KAFKA_CONSUMER_GROUP_ID", "iot-telemetry-postgres"),
            client_id=os.getenv("KAFKA_CONSUMER_CLIENT_ID", "iot-telemetry-consumer"),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            poll_timeout_seconds=_float_env("KAFKA_POLL_TIMEOUT_SECONDS", 1.0),
            anomaly_model_path=os.getenv("ANOMALY_MODEL_PATH", "models/telemetry_isolation_forest.joblib"),
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=_int_env("POSTGRES_PORT", 5432),
            postgres_db=os.getenv("POSTGRES_DB", "iot_telemetry"),
            postgres_user=os.getenv("POSTGRES_USER", "iot_app"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "change-me-local"),
        )


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc

