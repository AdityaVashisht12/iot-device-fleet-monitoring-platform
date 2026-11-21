"""Offset-controlled Kafka telemetry consumer."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from iot_telemetry.kafka.consumer_config import TelemetryConsumerConfig
from iot_telemetry.processing.anomaly import IsolationForestDetector
from iot_telemetry.processing.health import calculate_health
from iot_telemetry.processing.validation import validate_event

LOGGER = logging.getLogger(__name__)


class TelemetryConsumer:
    """Validate, enrich, persist, then commit each Kafka record."""

    def __init__(
        self,
        config: TelemetryConsumerConfig,
        repository: Any,
        *,
        consumer: Any | None = None,
        anomaly_detector: Any | None = None,
    ) -> None:
        self.config = config
        self.repository = repository
        if anomaly_detector is None:
            model_path = Path(config.anomaly_model_path)
            if not model_path.is_file():
                raise RuntimeError(
                    f"Anomaly model not found at {model_path}; train it with "
                    "python -m iot_telemetry.processing.train_anomaly_model"
                )
            anomaly_detector = IsolationForestDetector.load(model_path)
        self.anomaly_detector = anomaly_detector
        if consumer is None:
            try:
                from confluent_kafka import Consumer
            except ImportError as exc:
                raise RuntimeError("Install confluent-kafka to run the telemetry consumer") from exc
            consumer = Consumer(
                {
                    "bootstrap.servers": config.bootstrap_servers,
                    "group.id": config.group_id,
                    "client.id": config.client_id,
                    "auto.offset.reset": config.auto_offset_reset,
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                }
            )
        self.consumer = consumer

    def start(self) -> None:
        self.repository.initialize()
        self.consumer.subscribe([self.config.topic])
        LOGGER.info("Kafka telemetry consumer subscribed", extra={"kafka_topic": self.config.topic})

    def process_message(self, message: Any) -> str:
        """Persist one event and synchronously commit its Kafka offset.

        Returns ``stored``, ``duplicate``, or ``invalid``. Persistence and offset
        commit exceptions propagate so the process can restart and replay safely.
        """
        raw_bytes = message.value()
        if raw_bytes is None:
            raw_payload = ""
            decoded: Any = None
            parse_error = "Kafka message value is null"
        else:
            try:
                raw_payload = raw_bytes.decode("utf-8")
                decoded = json.loads(raw_payload, parse_constant=_reject_json_constant)
                parse_error = ""
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError, RecursionError) as exc:
                raw_payload = raw_bytes.decode("utf-8", errors="replace")
                decoded = None
                parse_error = f"invalid JSON: {exc}"

        event_id = decoded.get("event_id") if isinstance(decoded, dict) and isinstance(decoded.get("event_id"), str) else None
        if parse_error:
            errors = (parse_error,)
            self.repository.persist_invalid(raw_payload, errors, event_id)
            self._commit(message)
            LOGGER.warning(
                "Invalid telemetry persisted",
                extra={"event_id": event_id, "kafka_partition": message.partition(), "kafka_offset": message.offset(), "errors": errors},
            )
            return "invalid"

        validation = validate_event(decoded, ingestion_time=datetime.now(UTC))
        if not validation.is_valid:
            self.repository.persist_invalid(raw_payload, validation.errors, event_id)
            self._commit(message)
            LOGGER.warning(
                "Invalid telemetry persisted",
                extra={"event_id": event_id, "kafka_partition": message.partition(), "kafka_offset": message.offset(), "errors": validation.errors},
            )
            return "invalid"

        event = validation.event
        assert event is not None
        model_result = self.anomaly_detector.detect(event)
        source_marked_anomaly = bool(event.get("anomaly"))
        is_anomaly = model_result.is_anomaly or source_marked_anomaly
        anomaly_severity = model_result.severity or ("LOW" if source_marked_anomaly else None)
        health_score, performance_status = calculate_health(event, anomalous=is_anomaly)
        event["is_anomaly"] = is_anomaly
        event["anomaly_score"] = model_result.anomaly_score
        event["anomaly_severity"] = anomaly_severity
        event["device_health_score"] = health_score
        event["performance_status"] = performance_status

        inserted = self.repository.persist_valid(event, raw_payload)
        self._commit(message)
        if not inserted:
            LOGGER.info(
                "Duplicate telemetry event ignored",
                extra={"event_id": event["event_id"], "device_id": event["device_id"]},
            )
            return "duplicate"
        LOGGER.info(
            "Telemetry persisted",
            extra={
                "event_id": event["event_id"],
                "device_id": event["device_id"],
                "performance_status": performance_status,
                "is_anomaly": is_anomaly,
                "anomaly_severity": anomaly_severity,
                "anomaly_score": model_result.anomaly_score,
            },
        )
        return "stored"

    def run(self, stop_event: Any) -> None:
        """Poll until stopped. Persistence/commit failures terminate for safe replay."""
        try:
            self.start()
            while not stop_event.is_set():
                message = self.consumer.poll(self.config.poll_timeout_seconds)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    # Kafka partition EOF is informational; all other errors abort this run.
                    try:
                        from confluent_kafka import KafkaError
                    except ImportError:
                        KafkaError = None
                    if KafkaError is not None and error.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consume error: {error}")
                self.process_message(message)
        finally:
            try:
                self.consumer.close()
            finally:
                self.repository.close()
            LOGGER.info("Kafka telemetry consumer stopped")

    def _commit(self, message: Any) -> None:
        result = self.consumer.commit(message=message, asynchronous=False)
        if result:
            errors = [partition.error for partition in result if getattr(partition, "error", None)]
            if errors:
                raise RuntimeError(f"Kafka offset commit failed: {errors}")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON numeric constant {value} is not allowed")

