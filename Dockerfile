FROM python:3.12-slim

WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md ./
COPY src ./src
COPY postgres ./postgres
COPY ml_data ./ml_data
RUN pip install --no-cache-dir .
RUN python -m iot_telemetry.processing.train_anomaly_model \
    --normal-data ml_data/normal_telemetry.csv \
    --test-data ml_data/labeled_test_telemetry.csv \
    --model-output models/telemetry_isolation_forest.joblib

CMD ["python", "-m", "iot_telemetry.simulator"]

