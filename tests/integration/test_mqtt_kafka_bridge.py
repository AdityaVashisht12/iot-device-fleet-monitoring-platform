"""Real Mosquitto/Kafka integration test; opt in with RUN_MQTT_KAFKA_INTEGRATION=1."""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_MQTT_KAFKA_INTEGRATION") != "1",
    reason="set RUN_MQTT_KAFKA_INTEGRATION=1 and run Mosquitto plus Kafka",
)


def test_mqtt_message_is_delivered_to_kafka_with_device_key() -> None:
    mqtt = pytest.importorskip("paho.mqtt.client")
    confluent = pytest.importorskip("confluent_kafka")

    from iot_telemetry.mqtt.bridge import MqttKafkaBridge
    from iot_telemetry.mqtt.config import MqttKafkaConfig

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    mqtt_host = os.getenv("MQTT_HOST", "localhost")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    topic = os.getenv("KAFKA_TELEMETRY_TOPIC", "iot-telemetry")
    unique_id = f"integration-{uuid.uuid4().hex}"
    config = MqttKafkaConfig(
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_client_id=f"bridge-{uuid.uuid4().hex[:12]}",
        kafka_bootstrap_servers=bootstrap,
        kafka_topic=topic,
        kafka_delivery_timeout_seconds=15,
        kafka_retry_backoff_seconds=0.2,
    )
    consumer = confluent.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"integration-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    bridge = MqttKafkaBridge(config)
    publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"source-{uuid.uuid4().hex[:12]}")
    try:
        consumer.subscribe([topic])
        bridge.start()
        assert bridge.subscribed.wait(timeout=15), "bridge did not subscribe to Mosquitto"
        publisher.connect(mqtt_host, mqtt_port, 30)
        publisher.loop_start()
        event = {"device_id": unique_id, "temperature_c": 22.5, "region": "integration"}
        info = publisher.publish(f"iot/devices/{unique_id}/telemetry", json.dumps(event), qos=1)
        info.wait_for_publish(timeout=10)
        assert info.rc == mqtt.MQTT_ERR_SUCCESS

        deadline = time.monotonic() + 30
        received = None
        while time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            assert message.error() is None, str(message.error())
            if message.key().decode() == unique_id:
                received = json.loads(message.value().decode())
                break

        assert received == event
    finally:
        publisher.disconnect()
        publisher.loop_stop()
        bridge.stop()
        consumer.close()

