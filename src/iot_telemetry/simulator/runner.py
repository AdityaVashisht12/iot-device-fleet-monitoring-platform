"""Long-running simulator process and command-line entry point."""

from __future__ import annotations

import logging
import signal
import threading
import time

from iot_telemetry.simulator.config import SimulatorConfig
from iot_telemetry.simulator.generator import DeviceSimulator
from iot_telemetry.simulator.mqtt_publisher import MqttPublisher

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def run(
    config: SimulatorConfig,
    *,
    stop_event: threading.Event | None = None,
    publisher: MqttPublisher | None = None,
) -> None:
    """Publish fleet ticks until stopped; per-device failures do not stop the fleet."""
    stop = stop_event or threading.Event()
    mqtt_publisher = publisher or MqttPublisher(config)
    simulator = DeviceSimulator(config)
    try:
        mqtt_publisher.connect()
        LOGGER.info(
            "Starting simulator devices=%d interval=%.2fs speed=%.2fx",
            config.device_count,
            config.interval_seconds,
            config.speed_multiplier,
        )
        while not stop.is_set():
            tick_started = time.monotonic()
            sent = 0
            for event in simulator.events_for_tick():
                try:
                    mqtt_publisher.publish(event)
                    sent += 1
                except Exception:
                    # Continue other devices; a later MQTT tick may recover.
                    continue
            LOGGER.info("Fleet tick complete published=%d unavailable=%d", sent, config.device_count - sent)
            delay = max(0.0, config.tick_interval_seconds - (time.monotonic() - tick_started))
            if stop.wait(delay):
                break
    finally:
        try:
            mqtt_publisher.close()
        except Exception:
            LOGGER.exception("Error while closing MQTT publisher")
        LOGGER.info("Simulator stopped")


def main() -> None:
    configure_logging()
    config = SimulatorConfig.from_env()
    stop = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        LOGGER.info("Received signal %s; shutting down", signum)
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    run(config, stop_event=stop)

