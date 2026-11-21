"""Tests for persistence and Kafka offset commit ordering."""

import json
from types import SimpleNamespace

import pytest

from iot_telemetry.kafka.consumer_config import TelemetryConsumerConfig
from iot_telemetry.kafka.telemetry_consumer import TelemetryConsumer
from iot_telemetry.processing.anomaly import AnomalyResult


class FakeRepository:
    def __init__(self, *, fail: bool = False, inserted: bool = True) -> None:
        self.persisted: list[tuple[str, object]] = []
        self.fail = fail
        self.inserted = inserted

    def persist_valid(self, event, raw_payload=None):
        if self.fail:
            raise RuntimeError("database unavailable")
        self.persisted.append(("valid", event))
        return self.inserted

    def persist_invalid(self, payload, errors, event_id=None):
        if self.fail:
            raise RuntimeError("database unavailable")
        self.persisted.append(("invalid", payload))
        return True


class FakeConsumer:
    def __init__(self, repo: FakeRepository) -> None:
        self.repo = repo
        self.commits = 0

    def commit(self, *, message, asynchronous):
        assert self.repo.persisted, "offset commit must follow persistence"
        assert asynchronous is False
        self.commits += 1
        return []


class NoAnomalies:
    def detect(self, _event):
        return AnomalyResult(False, None, 0.01)


def _kafka_message(value: bytes):
    return SimpleNamespace(value=lambda: value, partition=lambda: 0, offset=lambda: 7)


def _event() -> dict:
    return {
        "event_id": "evt-1",
        "device_id": "device-1",
        "region": "europe",
        "timestamp": "2026-11-21T10:00:00Z",
        "temperature_c": 22.5,
        "cpu_usage_pct": 35,
        "memory_usage_pct": 50,
        "network_latency_ms": 18,
        "packet_loss_pct": 0.1,
        "battery_level_pct": 90,
    }


def test_valid_event_is_enriched_persisted_then_committed() -> None:
    repo = FakeRepository()
    kafka = FakeConsumer(repo)
    worker = TelemetryConsumer(
        TelemetryConsumerConfig(), repo, consumer=kafka, anomaly_detector=NoAnomalies()
    )

    result = worker.process_message(_kafka_message(json.dumps(_event()).encode()))

    assert result == "stored"
    assert kafka.commits == 1
    stored = repo.persisted[0][1]
    assert stored["device_health_score"] > 0
    assert stored["performance_status"] in {"healthy", "degraded", "critical"}
    assert stored["ingestion_timestamp"].endswith("+00:00")


def test_invalid_event_is_persisted_to_reject_store_before_commit() -> None:
    repo = FakeRepository()
    kafka = FakeConsumer(repo)
    worker = TelemetryConsumer(
        TelemetryConsumerConfig(), repo, consumer=kafka, anomaly_detector=NoAnomalies()
    )

    result = worker.process_message(_kafka_message(b'{"event_id":"bad"}'))

    assert result == "invalid"
    assert repo.persisted[0][0] == "invalid"
    assert kafka.commits == 1


def test_non_standard_json_numbers_are_rejected_and_stored() -> None:
    repo = FakeRepository()
    kafka = FakeConsumer(repo)
    worker = TelemetryConsumer(
        TelemetryConsumerConfig(), repo, consumer=kafka, anomaly_detector=NoAnomalies()
    )

    result = worker.process_message(
        _kafka_message(b'{"event_id":"bad","cpu_usage_pct":NaN}')
    )

    assert result == "invalid"
    assert repo.persisted[0][0] == "invalid"
    assert kafka.commits == 1


def test_database_failure_prevents_offset_commit() -> None:
    repo = FakeRepository(fail=True)
    kafka = FakeConsumer(repo)
    worker = TelemetryConsumer(
        TelemetryConsumerConfig(), repo, consumer=kafka, anomaly_detector=NoAnomalies()
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        worker.process_message(_kafka_message(json.dumps(_event()).encode()))

    assert kafka.commits == 0


def test_duplicate_event_is_idempotently_ignored_and_committed() -> None:
    repo = FakeRepository(inserted=False)
    kafka = FakeConsumer(repo)
    worker = TelemetryConsumer(
        TelemetryConsumerConfig(), repo, consumer=kafka, anomaly_detector=NoAnomalies()
    )

    result = worker.process_message(_kafka_message(json.dumps(_event()).encode()))

    assert result == "duplicate"
    assert kafka.commits == 1


def test_source_marked_anomaly_sets_degraded_health_even_during_model_warmup() -> None:
    repo = FakeRepository()
    kafka = FakeConsumer(repo)
    worker = TelemetryConsumer(
        TelemetryConsumerConfig(), repo, consumer=kafka, anomaly_detector=NoAnomalies()
    )
    event = _event()
    event["anomaly"] = "temperature_c"

    worker.process_message(_kafka_message(json.dumps(event).encode()))

    stored = repo.persisted[0][1]
    assert stored["is_anomaly"] is True
    assert stored["anomaly_severity"] == "LOW"
    assert stored["performance_status"] == "degraded"
