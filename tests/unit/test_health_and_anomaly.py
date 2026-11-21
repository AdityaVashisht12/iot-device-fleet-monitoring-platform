"""Health score and trained Isolation Forest evaluation tests."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from iot_telemetry.processing.anomaly import IsolationForestDetector, evaluate_labeled
from iot_telemetry.processing.health import calculate_health
from iot_telemetry.processing.train_anomaly_model import load_csv

ROOT = Path(__file__).resolve().parents[2]


def test_health_skips_missing_metrics_and_marks_low_score_critical() -> None:
    score, status = calculate_health(
        {
            "temperature_c": None,
            "cpu_usage_pct": 95,
            "memory_usage_pct": 98,
            "network_latency_ms": 2000,
            "packet_loss_pct": 20,
            "battery_level_pct": 5,
        }
    )

    assert score < 50
    assert status == "critical"


def test_model_trains_only_on_normal_rows_and_evaluates_labeled_test_set() -> None:
    normal = load_csv(ROOT / "ml_data" / "normal_telemetry.csv", labeled=False)
    labeled_test = load_csv(ROOT / "ml_data" / "labeled_test_telemetry.csv", labeled=True)
    detector = IsolationForestDetector(random_state=7).fit(normal)

    metrics = evaluate_labeled(detector, labeled_test)

    assert metrics["test_samples"] == len(labeled_test)
    assert 0 <= metrics["precision"] <= 1
    assert 0 <= metrics["recall"] <= 1
    assert 0 <= metrics["f1_score"] <= 1
    assert len(metrics["confusion_matrix"]) == 2
    assert set(metrics["severity_counts"]) == {"LOW", "MEDIUM", "HIGH", "CRITICAL"}


def test_model_artifact_round_trip_preserves_predictions(tmp_path: Path) -> None:
    normal = load_csv(ROOT / "ml_data" / "normal_telemetry.csv", labeled=False)
    detector = IsolationForestDetector(random_state=9).fit(normal)
    event = {
        "temperature_c": 80,
        "cpu_usage_pct": 98,
        "memory_usage_pct": 97,
        "network_latency_ms": 5000,
        "packet_loss_pct": 70,
    }
    expected = detector.detect(event)
    artifact = tmp_path / "iforest.joblib"

    detector.save(artifact)
    actual = IsolationForestDetector.load(artifact).detect(event)

    assert actual == expected


def test_model_assigns_all_four_severity_bands() -> None:
    detector = IsolationForestDetector()
    detector._pipeline = SimpleNamespace(score_samples=lambda _matrix: [-1.5])
    detector._thresholds = {"anomaly": 1.0, "medium": 2.0, "high": 3.0, "critical": 4.0}
    event = {
        "temperature_c": 22,
        "cpu_usage_pct": 30,
        "memory_usage_pct": 45,
        "network_latency_ms": 40,
        "packet_loss_pct": 0.2,
    }

    assert detector.detect(event).severity == "LOW"
    detector._pipeline = SimpleNamespace(score_samples=lambda _matrix: [-2.5])
    assert detector.detect(event).severity == "MEDIUM"
    detector._pipeline = SimpleNamespace(score_samples=lambda _matrix: [-3.5])
    assert detector.detect(event).severity == "HIGH"
    detector._pipeline = SimpleNamespace(score_samples=lambda _matrix: [-4.5])
    assert detector.detect(event).severity == "CRITICAL"

