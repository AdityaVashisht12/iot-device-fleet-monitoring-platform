"""Executable entry point for the MQTT-to-Kafka ingestion bridge."""

from __future__ import annotations

import logging
import signal
import threading

from iot_telemetry.mqtt.bridge import MqttKafkaBridge
from iot_telemetry.mqtt.config import MqttKafkaConfig
from iot_telemetry.observability.logging import configure_json_logging


def configure_logging() -> None:
    configure_json_logging()


def main() -> None:
    configure_logging()
    config = MqttKafkaConfig.from_env()
    stop_event = threading.Event()
    bridge = MqttKafkaBridge(config)

    def request_stop(signum: int, _frame: object) -> None:
        logging.getLogger(__name__).info("Shutdown requested", extra={"signal": signum})
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    bridge.start()
    try:
        stop_event.wait()
    finally:
        bridge.stop()


if __name__ == "__main__":
    main()

