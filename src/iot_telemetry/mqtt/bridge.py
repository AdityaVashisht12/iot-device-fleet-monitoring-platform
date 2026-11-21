"""Paho subscriber that forwards validated telemetry messages into Kafka."""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

from iot_telemetry.kafka.producer import KafkaEventProducer
from iot_telemetry.mqtt.config import MqttKafkaConfig

LOGGER = logging.getLogger(__name__)


class MqttKafkaBridge:
    """Subscribe to device telemetry and acknowledge MQTT only after Kafka delivery."""

    def __init__(self, config: MqttKafkaConfig, *, kafka_producer: Any | None = None) -> None:
        self.config = config
        self.kafka = kafka_producer or KafkaEventProducer(config)
        self._client: Any | None = None
        self.subscribed = threading.Event()

    def start(self) -> None:
        """Connect and start Paho's background network loop."""
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("Install paho-mqtt to run the MQTT subscriber") from exc

        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.config.mqtt_client_id,
            clean_session=False,
        )
        client.manual_ack_set(True)
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_subscribe = self._on_subscribe
        client.on_message = self._on_message
        self._client = client
        try:
            client.connect(self.config.mqtt_host, self.config.mqtt_port, self.config.mqtt_keepalive)
            client.loop_start()
            LOGGER.info(
                "MQTT subscriber started",
                extra={"mqtt_host": self.config.mqtt_host, "mqtt_port": self.config.mqtt_port},
            )
        except Exception:
            LOGGER.exception("Unable to connect MQTT subscriber")
            client.loop_stop()
            self._client = None
            raise

    def stop(self) -> None:
        """Stop MQTT intake and flush Kafka before returning."""
        client, self._client = self._client, None
        if client is not None:
            try:
                client.disconnect()
                client.loop_stop()
            finally:
                self.subscribed.clear()
        self.kafka.flush()
        LOGGER.info("MQTT-to-Kafka bridge stopped")

    def _on_connect(self, client: Any, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
        if getattr(reason_code, "is_failure", False):
            LOGGER.error("MQTT connection rejected", extra={"reason": str(reason_code)})
            return
        LOGGER.info("MQTT connection established", extra={"reason": str(reason_code)})
        client.subscribe(self.config.mqtt_topic, qos=self.config.mqtt_qos)

    def _on_disconnect(self, _client: Any, _userdata: Any, disconnect_flags: Any, reason_code: Any, _properties: Any) -> None:
        self.subscribed.clear()
        LOGGER.warning(
            "MQTT disconnected; Paho will reconnect",
            extra={"reason": str(reason_code), "disconnect_flags": str(disconnect_flags)},
        )

    def _on_subscribe(self, _client: Any, _userdata: Any, _mid: int, reason_codes: Any, _properties: Any) -> None:
        if any(getattr(reason, "is_failure", False) for reason in reason_codes):
            LOGGER.error("MQTT telemetry subscription rejected", extra={"reason_codes": str(reason_codes)})
            return
        self.subscribed.set()
        LOGGER.info("MQTT telemetry subscription active", extra={"mqtt_topic": self.config.mqtt_topic})

    def _on_message(self, client: Any, _userdata: Any, message: Any) -> None:
        topic = message.topic
        try:
            event = json.loads(message.payload.decode("utf-8"))
            if not isinstance(event, dict):
                raise ValueError("telemetry payload must be a JSON object")
            device_id = event.get("device_id")
            if not isinstance(device_id, str) or not device_id.strip():
                raise ValueError("telemetry event requires a non-empty device_id")
            expected_topic = f"iot/devices/{device_id}/telemetry"
            if topic != expected_topic:
                raise ValueError("MQTT topic device ID does not match payload device_id")
            serialized = json.dumps(event, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            LOGGER.warning(
                "Malformed MQTT telemetry discarded",
                extra={"mqtt_topic": topic, "mqtt_mid": message.mid, "error": str(exc)},
            )
            try:
                self._ack(client, message)
            except Exception:
                LOGGER.exception("Could not acknowledge rejected MQTT message", extra={"mqtt_mid": message.mid})
                try:
                    client.disconnect()
                except Exception:
                    LOGGER.exception("Could not disconnect after MQTT acknowledgement failure")
            return

        try:
            self.kafka.publish(device_id, serialized)
            self._ack(client, message)
            LOGGER.info(
                "Telemetry forwarded to Kafka",
                extra={"device_id": device_id, "mqtt_topic": topic, "kafka_topic": self.config.kafka_topic},
            )
        except Exception:
            LOGGER.exception(
                "Kafka forwarding failed; leaving MQTT message unacknowledged",
                extra={"device_id": device_id, "mqtt_topic": topic, "mqtt_mid": message.mid},
            )
            # Force a reconnect so the persistent MQTT session can redeliver the unacked QoS 1 message.
            try:
                client.disconnect()
            except Exception:
                LOGGER.exception("Could not disconnect MQTT client after Kafka failure")

    @staticmethod
    def _ack(client: Any, message: Any) -> None:
        if message.qos > 0:
            result = client.ack(message.mid, message.qos)
            if result != 0:
                raise RuntimeError(f"MQTT acknowledgement failed with result code {result}")

