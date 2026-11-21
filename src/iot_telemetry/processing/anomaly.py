"""Trainable Isolation Forest anomaly detection and severity calibration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

FEATURES = (
    "temperature_c",
    "cpu_usage_pct",
    "memory_usage_pct",
    "network_latency_ms",
    "packet_loss_pct",
)
SEVERITIES = ("LOW", "MEDIUM", "HIGH", "CRITICAL")


@dataclass(frozen=True, slots=True)
class AnomalyResult:
    is_anomaly: bool
    severity: str | None
    anomaly_score: float


class IsolationForestDetector:
    """Fit on normal telemetry and score unusual multivariate observations.

    The anomaly decision threshold and severity boundaries are calibrated from
    Isolation Forest scores for the normal training set. The detector is an
    anomaly classifier; it does not predict or prove a hardware failure.
    """

    def __init__(self, *, contamination: float = 0.025, random_state: int = 42, n_estimators: int = 300) -> None:
        if not 0.001 <= contamination < 0.5:
            raise ValueError("contamination must be in [0.001, 0.5)")
        if n_estimators < 50:
            raise ValueError("n_estimators must be at least 50")
        self.contamination = contamination
        self.random_state = random_state
        self.n_estimators = n_estimators
        self._pipeline: Any | None = None
        self._thresholds: dict[str, float] | None = None

    @property
    def is_fitted(self) -> bool:
        return self._pipeline is not None

    @property
    def thresholds(self) -> dict[str, float]:
        if self._thresholds is None:
            raise RuntimeError("anomaly model has not been trained")
        return dict(self._thresholds)

    def fit(self, normal_events: Iterable[dict[str, Any]]) -> "IsolationForestDetector":
        """Fit only on normal events and calibrate four severity bands."""
        try:
            import numpy as np
            from sklearn.ensemble import IsolationForest
            from sklearn.impute import SimpleImputer
            from sklearn.pipeline import make_pipeline
            from sklearn.preprocessing import StandardScaler
        except ImportError as exc:
            raise RuntimeError("Install scikit-learn to train the Isolation Forest model") from exc

        rows = list(normal_events)
        if len(rows) < 20:
            raise ValueError("at least 20 normal telemetry events are required for training")
        matrix = self._matrix(rows)
        self._pipeline = make_pipeline(
            SimpleImputer(strategy="median", add_indicator=True),
            StandardScaler(),
            IsolationForest(
                n_estimators=self.n_estimators,
                contamination="auto",
                random_state=self.random_state,
                n_jobs=-1,
            ),
        )
        self._pipeline.fit(matrix)
        normal_scores = -self._pipeline.score_samples(matrix)
        anomaly_quantile = 1.0 - self.contamination
        medium_quantile = min(0.999, max(0.99, anomaly_quantile + (1.0 - anomaly_quantile) * 0.2))
        high_quantile = min(0.9995, max(0.995, medium_quantile + (1.0 - medium_quantile) * 0.5))
        critical_quantile = min(0.9999, max(0.999, high_quantile + (1.0 - high_quantile) * 0.5))
        self._thresholds = {
            "anomaly": float(np.quantile(normal_scores, anomaly_quantile)),
            "medium": float(np.quantile(normal_scores, medium_quantile)),
            "high": float(np.quantile(normal_scores, min(0.9995, high_quantile))),
            "critical": float(np.quantile(normal_scores, min(0.9999, critical_quantile))),
        }
        return self

    def detect(self, event: dict[str, Any]) -> AnomalyResult:
        """Score an event, assigning severity only when it crosses the anomaly threshold."""
        if self._pipeline is None or self._thresholds is None:
            raise RuntimeError("anomaly model has not been trained or loaded")
        score = float(-self._pipeline.score_samples(self._matrix([event]))[0])
        bounds = self._thresholds
        if score < bounds["anomaly"]:
            return AnomalyResult(False, None, score)
        if score >= bounds["critical"]:
            severity = "CRITICAL"
        elif score >= bounds["high"]:
            severity = "HIGH"
        elif score >= bounds["medium"]:
            severity = "MEDIUM"
        else:
            severity = "LOW"
        return AnomalyResult(True, severity, score)

    def save(self, path: str | Path) -> None:
        if not self.is_fitted:
            raise RuntimeError("cannot save an untrained anomaly model")
        try:
            import joblib
        except ImportError as exc:
            raise RuntimeError("Install scikit-learn to save the anomaly model") from exc
        model_path = Path(path)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "pipeline": self._pipeline,
                "thresholds": self._thresholds,
                "contamination": self.contamination,
                "random_state": self.random_state,
                "n_estimators": self.n_estimators,
                "features": FEATURES,
                "format_version": 1,
            },
            model_path,
        )

    @classmethod
    def load(cls, path: str | Path) -> "IsolationForestDetector":
        try:
            import joblib
        except ImportError as exc:
            raise RuntimeError("Install scikit-learn to load the anomaly model") from exc
        artifact = joblib.load(Path(path))
        if artifact.get("format_version") != 1 or tuple(artifact.get("features", ())) != FEATURES:
            raise ValueError("model artifact version or telemetry feature schema is incompatible")
        detector = cls(
            contamination=artifact["contamination"],
            random_state=artifact["random_state"],
            n_estimators=artifact["n_estimators"],
        )
        detector._pipeline = artifact["pipeline"]
        detector._thresholds = artifact["thresholds"]
        return detector

    @staticmethod
    def _matrix(events: list[dict[str, Any]]) -> Any:
        import numpy as np

        rows: list[list[float]] = []
        for event in events:
            row = []
            for name in FEATURES:
                value = event.get(name)
                if value is None:
                    row.append(np.nan)
                elif isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"{name} must be numeric or null for anomaly scoring")
                elif not np.isfinite(float(value)):
                    raise ValueError(f"{name} must be finite for anomaly scoring")
                else:
                    row.append(float(value))
            rows.append(row)
        return np.asarray(rows, dtype=float)


def evaluate_labeled(
    detector: IsolationForestDetector,
    labeled_events: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Calculate binary precision/recall/F1 and a [[TN, FP], [FN, TP]] matrix."""
    try:
        from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score
    except ImportError as exc:
        raise RuntimeError("Install scikit-learn to evaluate anomaly detection") from exc

    rows = list(labeled_events)
    if not rows:
        raise ValueError("labeled test data cannot be empty")
    labels: list[int] = []
    predictions: list[int] = []
    for row in rows:
        label = row.get("label")
        if isinstance(label, bool) or label not in (0, 1):
            raise ValueError("test data label must be 0 (normal) or 1 (anomaly)")
        labels.append(int(label))
        predictions.append(int(detector.detect(row).is_anomaly))
    matrix = confusion_matrix(labels, predictions, labels=[0, 1])
    return {
        "test_samples": len(rows),
        "precision": float(precision_score(labels, predictions, zero_division=0)),
        "recall": float(recall_score(labels, predictions, zero_division=0)),
        "f1_score": float(f1_score(labels, predictions, zero_division=0)),
        "confusion_matrix": matrix.tolist(),
        "confusion_matrix_labels": ["normal", "anomaly"],
        "severity_counts": _severity_counts(detector, rows),
    }


def _severity_counts(detector: IsolationForestDetector, rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {severity: 0 for severity in SEVERITIES}
    for row in rows:
        result = detector.detect(row)
        if result.severity is not None:
            counts[result.severity] += 1
    return counts
