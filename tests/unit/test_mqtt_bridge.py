"""Fast tests for MQTT ingestion validation and acknowledgement behavior."""

import json
from types import SimpleNamespace

from iot_telemetry.mqtt.bridge import MqttKafkaBridge
from iot_telemetry.mqtt.config import MqttKafkaConfig


class FakeKafka:
    def __init__(self, should_fail: bool = False) -> None:
        self.messages: list[tuple[str, str]] = []
        self.should_fail = should_fail

    def publish(self, device_id: str, serialized: str) -> None:
        if self.should_fail:
            raise RuntimeError("broker unavailable")
        self.messages.append((device_id, serialized))


class FakeMqttClient:
    def __init__(self) -> None:
        self.acks: list[tuple[int, int]] = []
        self.disconnects = 0

    def ack(self, mid: int, qos: int) -> int:
        self.acks.append((mid, qos))
        return 0

    def disconnect(self) -> None:
        self.disconnects += 1


def _message(payload: bytes, *, topic: str = "iot/devices/device-1/telemetry", qos: int = 1):
    return SimpleNamespace(topic=topic, payload=payload, mid=17, qos=qos)


def test_valid_message_is_keyed_by_device_and_acked_after_publish() -> None:
    kafka = FakeKafka()
    bridge = MqttKafkaBridge(MqttKafkaConfig(), kafka_producer=kafka)
    client = FakeMqttClient()
    payload = json.dumps({"device_id": "device-1", "temperature_c": 21.4}).encode()

    bridge._on_message(client, None, _message(payload))

    assert kafka.messages[0][0] == "device-1"
    assert json.loads(kafka.messages[0][1])["temperature_c"] == 21.4
    assert client.acks == [(17, 1)]
    assert client.disconnects == 0


def test_malformed_message_is_dropped_and_acked() -> None:
    kafka = FakeKafka()
    bridge = MqttKafkaBridge(MqttKafkaConfig(), kafka_producer=kafka)
    client = FakeMqttClient()

    bridge._on_message(client, None, _message(b"not-json"))

    assert kafka.messages == []
    assert client.acks == [(17, 1)]


def test_topic_and_payload_device_ids_must_match() -> None:
    kafka = FakeKafka()
    bridge = MqttKafkaBridge(MqttKafkaConfig(), kafka_producer=kafka)
    client = FakeMqttClient()

    bridge._on_message(client, None, _message(b'{"device_id":"other"}'))

    assert kafka.messages == []
    assert client.acks == [(17, 1)]


def test_kafka_failure_leaves_mqtt_message_unacked_and_disconnects() -> None:
    bridge = MqttKafkaBridge(MqttKafkaConfig(), kafka_producer=FakeKafka(should_fail=True))
    client = FakeMqttClient()

    bridge._on_message(client, None, _message(b'{"device_id":"device-1"}'))

    assert client.acks == []
    assert client.disconnects == 1

