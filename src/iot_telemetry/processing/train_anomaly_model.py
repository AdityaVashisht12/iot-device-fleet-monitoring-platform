"""Train/evaluate an Isolation Forest from normal and labeled CSV files."""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any

from iot_telemetry.processing.anomaly import FEATURES, IsolationForestDetector, evaluate_labeled

LOGGER = logging.getLogger(__name__)


def load_csv(path: str | Path, *, labeled: bool) -> list[dict[str, Any]]:
    """Load feature rows; labeled files must contain a binary `label` column."""
    rows: list[dict[str, Any]] = []
    with Path(path).open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        expected = set(FEATURES) | ({"label"} if labeled else set())
        missing = expected - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path} is missing columns: {', '.join(sorted(missing))}")
        has_labels = "label" in set(reader.fieldnames or ())
        for line_number, source in enumerate(reader, start=2):
            row: dict[str, Any] = {}
            for name in FEATURES:
                value = source.get(name, "").strip()
                try:
                    row[name] = float(value) if value else None
                except ValueError as exc:
                    raise ValueError(f"{path}:{line_number}: {name} must be numeric or blank") from exc
            if labeled:
                label = source.get("label", "").strip()
                if label not in {"0", "1"}:
                    raise ValueError(f"{path}:{line_number}: label must be 0 or 1")
                row["label"] = int(label)
            elif has_labels and source.get("label", "").strip() != "0":
                raise ValueError(
                    f"{path}:{line_number}: training data must contain normal rows only (label=0)"
                )
            rows.append(row)
    if not rows:
        raise ValueError(f"{path} contains no telemetry rows")
    return rows


def train_and_evaluate(
    normal_data: str | Path,
    labeled_test_data: str | Path,
    model_output: str | Path,
    *,
    contamination: float = 0.025,
    random_state: int = 42,
) -> dict[str, Any]:
    """Fit on normal-only data, evaluate labeled holdout data, and save artifact."""
    normal_rows = load_csv(normal_data, labeled=False)
    test_rows = load_csv(labeled_test_data, labeled=True)
    detector = IsolationForestDetector(contamination=contamination, random_state=random_state)
    detector.fit(normal_rows)
    metrics = evaluate_labeled(detector, test_rows)
    detector.save(model_output)
    metrics.update(
        {
            "training_normal_samples": len(normal_rows),
            "model_output": str(model_output),
            "anomaly_score_threshold": detector.thresholds["anomaly"],
            "severity_score_thresholds": {
                key: detector.thresholds[key] for key in ("medium", "high", "critical")
            },
        }
    )
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--normal-data", default="ml_data/normal_telemetry.csv")
    parser.add_argument("--test-data", default="ml_data/labeled_test_telemetry.csv")
    parser.add_argument("--model-output", default="models/telemetry_isolation_forest.joblib")
    parser.add_argument("--contamination", type=float, default=0.025)
    parser.add_argument("--random-state", type=int, default=42)
    args = parser.parse_args()
    metrics = train_and_evaluate(
        args.normal_data,
        args.test_data,
        args.model_output,
        contamination=args.contamination,
        random_state=args.random_state,
    )
    print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

