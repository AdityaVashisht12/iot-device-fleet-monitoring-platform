"""CLI entry point for Kafka-to-PostgreSQL ingestion."""

from __future__ import annotations

import logging
import signal
import threading

from iot_telemetry.kafka.consumer_config import TelemetryConsumerConfig
from iot_telemetry.kafka.telemetry_consumer import TelemetryConsumer
from iot_telemetry.processing.anomaly import IsolationForestDetector
from iot_telemetry.storage.postgres import PostgresRepository
from iot_telemetry.observability.logging import configure_json_logging


def configure_logging() -> None:
    configure_json_logging()


def main() -> None:
    configure_logging()
    config = TelemetryConsumerConfig.from_env()
    stop_event = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        logging.getLogger(__name__).info("Shutdown requested", extra={"signal": signum})
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    detector = IsolationForestDetector.load(config.anomaly_model_path)
    consumer = TelemetryConsumer(
        config,
        PostgresRepository.from_config(config),
        anomaly_detector=detector,
    )
    consumer.run(stop_event)


if __name__ == "__main__":
    main()

