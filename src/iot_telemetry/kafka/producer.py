"""Kafka producer with bounded retries and delivery acknowledgements."""

from __future__ import annotations

import logging
import time
from typing import Any

from iot_telemetry.mqtt.config import MqttKafkaConfig

LOGGER = logging.getLogger(__name__)


class KafkaEventProducer:
    """Serialize events as JSON and wait for Kafka's delivery report."""

    def __init__(self, config: MqttKafkaConfig, *, producer: Any | None = None) -> None:
        self.config = config
        if producer is None:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:
                raise RuntimeError("Install the ingestion extra to use Kafka (confluent-kafka)") from exc
            producer = Producer(
                {
                    "bootstrap.servers": config.kafka_bootstrap_servers,
                    "client.id": config.kafka_client_id,
                    "enable.idempotence": True,
                    "acks": "all",
                    "retries": 5,
                    "request.timeout.ms": int(config.kafka_delivery_timeout_seconds * 1000),
                }
            )
        self._producer = producer

    def publish(self, device_id: str, serialized_event: str) -> None:
        """Publish a keyed event and return only after a successful delivery report."""
        last_error: Exception | None = None
        for attempt in range(1, self.config.kafka_max_retries + 2):
            delivered = False
            delivery_error: Exception | None = None

            def on_delivery(error: Any, message: Any) -> None:
                nonlocal delivered, delivery_error
                if error is not None:
                    delivery_error = RuntimeError(str(error))
                else:
                    delivered = True
                    LOGGER.info(
                        "Kafka delivery acknowledged",
                        extra={"device_id": device_id, "topic": message.topic(), "partition": message.partition()},
                    )

            try:
                self._producer.produce(
                    self.config.kafka_topic,
                    key=device_id.encode("utf-8"),
                    value=serialized_event.encode("utf-8"),
                    on_delivery=on_delivery,
                )
                deadline = time.monotonic() + self.config.kafka_delivery_timeout_seconds
                while not delivered and delivery_error is None and time.monotonic() < deadline:
                    self._producer.poll(0.1)
                if delivered:
                    return
                last_error = delivery_error or TimeoutError("Kafka delivery acknowledgement timed out")
            except Exception as exc:
                last_error = exc

            LOGGER.warning(
                "Kafka publish attempt failed",
                extra={"device_id": device_id, "attempt": attempt, "error": str(last_error)},
            )
            if attempt <= self.config.kafka_max_retries and self.config.kafka_retry_backoff_seconds:
                time.sleep(self.config.kafka_retry_backoff_seconds * attempt)

        raise RuntimeError(
            f"Kafka publish failed for device {device_id} after {self.config.kafka_max_retries + 1} attempts"
        ) from last_error

    def flush(self, timeout: float = 10.0) -> None:
        """Wait for queued messages during graceful shutdown."""
        remaining = self._producer.flush(timeout)
        if remaining:
            LOGGER.error("Kafka flush timed out", extra={"undelivered_messages": remaining})

