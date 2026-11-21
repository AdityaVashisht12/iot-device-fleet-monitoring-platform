"""MQTT publishing adapter for simulator events."""

from __future__ import annotations

import logging

from iot_telemetry.simulator.config import SimulatorConfig
from iot_telemetry.simulator.generator import TelemetryEvent

LOGGER = logging.getLogger(__name__)


class MqttPublisher:
    """Small paho-mqtt wrapper; import stays lazy for generator-only use and tests."""

    def __init__(self, config: SimulatorConfig) -> None:
        self.config = config
        self._client = None

    def connect(self) -> None:
        try:
            import paho.mqtt.client as mqtt

            self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            self._client.connect(self.config.mqtt_host, self.config.mqtt_port, self.config.mqtt_keepalive)
            self._client.loop_start()
            LOGGER.info("Connected to MQTT broker %s:%s", self.config.mqtt_host, self.config.mqtt_port)
        except Exception:
            LOGGER.exception("Could not connect to MQTT broker %s:%s", self.config.mqtt_host, self.config.mqtt_port)
            raise

    def publish(self, event: TelemetryEvent) -> None:
        if self._client is None:
            raise RuntimeError("MQTT publisher is not connected")
        topic = f"iot/devices/{event['device_id']}/telemetry"
        try:
            info = self._client.publish(topic, event.to_json(), qos=self.config.mqtt_qos)
            info.wait_for_publish(timeout=10)
            if info.rc != 0:
                raise RuntimeError(f"MQTT publish failed with result code {info.rc}")
        except Exception:
            LOGGER.exception("Failed to publish telemetry for %s", event.get("device_id", "unknown"))
            raise

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            finally:
                self._client = None
            LOGGER.info("MQTT publisher disconnected")

