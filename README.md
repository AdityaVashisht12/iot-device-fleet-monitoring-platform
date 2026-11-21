# IoT Device Telemetry Pipeline

An enterprise-style local reference architecture for collecting simulated device readings, streaming and processing events, persisting telemetry, and exposing operational dashboards.

> The repository includes the simulator, MQTT-to-Kafka bridge, Kafka processor, PostgreSQL schema, Airflow DAGs, Grafana dashboards, and Docker Compose deployment.

## Architecture

```text
Device simulator -> Mosquitto (MQTT) -> Kafka -> consumer/validation -> PostgreSQL
                                                     ^
                                      Airflow orchestrates batch tasks and health checks
PostgreSQL -> Grafana dashboards
```

Python services are organized by responsibility: device simulation and MQTT publishing, MQTT/Kafka bridging, Kafka consumption, validation and transformation, anomaly scoring, and persistence. Airflow owns scheduled analytics rather than sitting in the streaming path. PostgreSQL is the durable query store; Kafka is the event buffer; MQTT is the device-facing protocol; Grafana visualizes operational and device metrics.

## Docker Compose platform

Compose brings up Mosquitto, Kafka in single-node KRaft mode, PostgreSQL, the Python 3.12 simulator, MQTT-to-Kafka bridge, Kafka-to-PostgreSQL consumer, Apache Airflow, and Grafana on a private bridge network. Brokers and the database use persistent named volumes. Local plaintext/anonymous broker settings and sample passwords are for development only.

### Start the platform

Install Docker Desktop (or Docker Engine) with Docker Compose v2.20 or later, copy `.env.example` to `.env`, and change the example passwords and Airflow keys. Then run one command from the project root:

```powershell
docker compose up --build -d
```

Compose builds the Python and Airflow images, initializes PostgreSQL schema and service roles on a new database volume, migrates Airflow metadata, creates the Kafka topic, then starts dependent services after their broker/database checks pass. The Python image trains and packages the Isolation Forest artifact during its build so the consumer can start immediately.

| Service | Host URL / port | Purpose |
|---|---|---|
| Mosquitto | `localhost:1883` | Device MQTT events |
| Kafka | `localhost:9092` | Optional host-side Kafka clients; container clients use `kafka:9092` |
| PostgreSQL | `localhost:5432` | Telemetry and Airflow metadata |
| Airflow | [http://localhost:8080](http://localhost:8080) | Scheduled analytics UI |
| Grafana | [http://localhost:3000](http://localhost:3000) | Four provisioned dashboards |

The simulator defaults to 500 devices at a two-second interval. Airflow and Grafana credentials and all broker/database ports are configurable in `.env`; the complete variable list is in [.env.example](.env.example). See [the Grafana guide](grafana/README.md) for dashboard filters and [the Airflow guide](airflow/dags/README.md) for DAG use.

### Shutdown and persistence

Stop containers while keeping data with `docker compose down`. Start again with `docker compose up -d`. To remove all stored broker, telemetry, dashboard, and Airflow logs, use `docker compose down -v`; this permanently deletes the named volume data. Rebuilding application code without restarting other services can use `docker compose build simulator mqtt-kafka-ingestor kafka-postgres-consumer` followed by `docker compose up -d`.

### Troubleshooting

- Check readiness and exit state with `docker compose ps -a`; inspect a service with `docker compose logs -f kafka` (replace `kafka` with `postgres`, `mosquitto`, `airflow-init`, `airflow-scheduler`, or another service).
- If an app starts before a dependency is ready, Compose retries it under `unless-stopped`; confirm Mosquitto, Kafka, and PostgreSQL show healthy and that `kafka-init` and `airflow-init` exited successfully.
- If a published host port is busy, change the matching `*_PORT` value in `.env`, then run `docker compose up -d` again.
- PostgreSQL's schema init script runs on a new data directory; the Kafka consumer also applies the schema idempotently at startup. `postgres-bootstrap` synchronizes Grafana and Airflow role passwords from `.env` when it initializes or is recreated. Do not delete the volume unless its data can be discarded.
- Airflow UI or tasks failing to connect: check `airflow-init` and scheduler logs, confirm `AIRFLOW_DB_*` and `POSTGRES_*` values match, and allow the first database migration to complete.
- Kafka startup failures: check its logs and ensure the persisted `kafka-data` volume belongs to this KRaft cluster configuration. Changing cluster identity or incompatible broker settings may require a deliberate data-volume reset.
- For a clean service restart without deleting data, use `docker compose restart <service>`; for a full configuration refresh, use `docker compose up -d --build`.

### Resource requirements

The full stack is intended for local development. Allocate at least 4 CPU cores, 8 GB RAM, and 10 GB free disk to Docker; 6+ cores and 12 GB RAM give more room for Airflow, image builds, and the 500-device simulator. Persistent Kafka and PostgreSQL storage grows as events accumulate. This is a single Kafka broker with replication factor 1, so it is not highly available.

## Project layout

```text
src/iot_telemetry/     Python package, split by pipeline responsibility
tests/                 Unit and integration test suites
mosquitto/             MQTT broker configuration
airflow/dags/          Workflow definitions
grafana/               Four dashboards, datasource/provider provisioning, and setup guide
postgres/              Database schema, relationship guide, and sample queries
ml_data/               Normal training and labeled anomaly test telemetry
```

## Device simulator

The simulator is a standalone producer under `src/iot_telemetry/simulator/`. It creates unique devices assigned round-robin across six regions and emits correlated readings for temperature, CPU, memory, latency, packet loss, and battery. Readings drift over time; abnormal values are marked with `status=degraded` and an `anomaly` metric. Simulated outages temporarily suppress that device's event while the rest of the fleet continues.

Install the package with `python -m pip install -e ".[dev]"`, copy `.env.example` to `.env`, and run `python -m iot_telemetry.simulator` from a shell where the environment is loaded. For example, in PowerShell:

```powershell
$env:MQTT_HOST = "localhost"
$env:SIMULATOR_DEVICE_COUNT = "500"
$env:SIMULATOR_INTERVAL_SECONDS = "2"
$env:SIMULATOR_SPEED_MULTIPLIER = "1"
python -m iot_telemetry.simulator
```

Each fleet tick targets one event per available device. Speed multiplier `2` runs at twice the configured rate (a 1-second fleet interval for the 2-second default); `0.5` runs at half speed. Topics use `iot/devices/{device_id}/telemetry`. Stop with Ctrl+C; SIGINT/SIGTERM trigger MQTT cleanup.

Simulator environment settings:

| Variable | Default | Purpose |
|---|---:|---|
| `SIMULATOR_DEVICE_COUNT` | `500` | Number of unique devices |
| `SIMULATOR_INTERVAL_SECONDS` | `2` | Target interval between fleet ticks |
| `SIMULATOR_SPEED_MULTIPLIER` | `1` | Multiply or slow the tick rate |
| `SIMULATOR_ABNORMAL_PROBABILITY` | `0.01` | Chance per device tick of an anomalous metric |
| `SIMULATOR_FAILURE_PROBABILITY` | `0.0005` | Chance per available device tick to enter a temporary outage |
| `SIMULATOR_MIN_FAILURE_TICKS` / `SIMULATOR_MAX_FAILURE_TICKS` | `3` / `15` | Outage duration range in fleet ticks |
| `SIMULATOR_RANDOM_SEED` | unset | Optional repeatable random sequence |
| `MQTT_HOST` / `MQTT_PORT` | `localhost` / `1883` | Mosquitto connection |
| `SIMULATOR_MQTT_QOS` | `1` | MQTT QoS (`0`, `1`, or `2`) |

`SIMULATOR_DEVICE_COUNT` controls fleet size; the default represents the requested 500-device fleet. All settings are validated on startup. A broker outage is logged and fails startup; publish errors are logged per device and the next fleet tick continues.

## MQTT-to-Kafka ingestion flow

Run the bridge with `python -m iot_telemetry.mqtt.runner` (or `iot-telemetry-ingestor`) after installing the project dependencies and starting Mosquitto and Kafka. It subscribes to `iot/devices/+/telemetry` at QoS 1, decodes and validates each JSON object, verifies that its `device_id` matches the topic, serializes compact JSON, and writes it to Kafka topic `iot-telemetry` with the UTF-8 `device_id` as the record key. The Kafka producer uses `acks=all` and idempotence, and the bridge waits for the delivery report before acknowledging MQTT.

Paho reconnects with increasing delay and restores the subscription using a stable client ID and persistent MQTT session. Transient Kafka delivery failures get bounded retries; after exhaustion the MQTT message stays unacknowledged and the subscriber disconnects so Mosquitto can redeliver it after reconnect. This is at-least-once across the MQTT/Kafka boundary: a Kafka record can be duplicated if Kafka accepted it but the acknowledgement was lost before MQTT ack. Downstream consumers should be idempotent. Invalid JSON, non-object payloads, missing/empty `device_id`, topic/key mismatch, and non-JSON numeric values are logged and acknowledged as rejected to avoid poison-message loops.

The bridge logs one JSON object per line with contextual fields (device ID, MQTT topic, Kafka topic, retry attempt, and delivery destination). Environment settings are in `.env.example`; the bridge uses `MQTT_HOST`, `MQTT_PORT`, `MQTT_INGEST_*`, and `KAFKA_*` values. Run the optional real-service integration test with `RUN_MQTT_KAFKA_INTEGRATION=1` after both services and the Kafka topic are available:

```powershell
$env:RUN_MQTT_KAFKA_INTEGRATION = "1"
python -m pytest tests/integration/test_mqtt_kafka_bridge.py
```

## Kafka-to-PostgreSQL consumer flow

Train an anomaly model first (details below), then start the consumer with `python -m iot_telemetry.kafka.consumer_runner` or `iot-telemetry-consumer`. It consumes `iot-telemetry` with auto-commit disabled, validates events, normalizes timestamps to UTC, stores missing sensor readings as SQL `NULL`, calculates `device_health_score`, `performance_status`, and `ingestion_timestamp`, and records the Isolation Forest score and severity. Sensor bounds are temperature `[-40, 125] °C`, CPU and memory `[0, 100]%`, latency `[0, 60000] ms`, packet loss `[0, 100]%`, and battery `[0, 100]%`. Required fields are `event_id`, `device_id`, `region`, and `timestamp`.

PostgreSQL creates `devices`, `raw_telemetry`, `processed_telemetry`, `device_alerts`, and `invalid_telemetry_events` on startup. A valid message transactionally registers/updates its device, stores the source payload and processed measurements, and creates or updates incidents. `raw_telemetry.event_id` is unique; identical rejected payloads are deduplicated by SHA-256. Invalid JSON and invalid schema/range values are stored with their raw payload and validation errors. Kafka offsets are synchronously committed only after the corresponding valid or rejected record is persisted. If persistence or offset commit fails, the consumer exits so the record can be replayed idempotently. See the [database relationship guide](postgres/README.md), [schema](postgres/schema.sql), and [sample queries](postgres/queries/sample_queries.sql).

## Isolation Forest training and evaluation

Train using only normal telemetry, evaluate on a separate labeled holdout, and save the artifact consumed by the Kafka worker:

```powershell
python -m iot_telemetry.processing.train_anomaly_model
python -m iot_telemetry.kafka.consumer_runner
```

The default training file is [normal_telemetry.csv](ml_data/normal_telemetry.csv); the labeled evaluation file is [labeled_test_telemetry.csv](ml_data/labeled_test_telemetry.csv), where `label=0` means normal and `label=1` means anomalous. The command prints precision, recall, F1-score, a `[[TN, FP], [FN, TP]]` confusion matrix, severity counts, and calibration thresholds, then writes `models/telemetry_isolation_forest.joblib`. Use `--normal-data`, `--test-data`, and `--model-output` to select other files and locations. The default model path is configurable with `ANOMALY_MODEL_PATH`.

The model fits only on normal samples and evaluates five correlated sensor features: temperature, CPU, memory, network latency, and packet loss. Its anomaly-score threshold and `LOW`, `MEDIUM`, `HIGH`, and `CRITICAL` bands are calibrated from normal-training score quantiles. These metrics describe statistical telemetry anomaly classification on the labeled holdout; they do not measure hardware-failure prediction. An anomaly is an unusual sensor pattern, not evidence that hardware failed. Operational alerts for critical health and telemetry gaps are stored as separate `health_critical` and `telemetry_gap` incidents. One unresolved anomaly incident is maintained per device; repeated anomalous readings update its severity and occurrence count, normal telemetry resolves it, and later anomalies can open a new incident.

Consumer settings use `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TELEMETRY_TOPIC`, `KAFKA_CONSUMER_GROUP_ID`, `KAFKA_AUTO_OFFSET_RESET`, `POSTGRES_*`, `KAFKA_POLL_TIMEOUT_SECONDS`, and `ANOMALY_MODEL_PATH`. Defaults are in `.env.example`. Install dependencies with `python -m pip install -e ".[dev]"`; unit tests are in `tests/unit/`.

## Scheduled analytics with Airflow

The hourly `iot_hourly_analytics` DAG writes device-, region-, and fleet-level performance, uptime, operational incident rates, and severity counts. The daily `iot_daily_operational_summary` DAG rolls up the prior UTC date. Both use retrying, idempotent upserts into their analytics tables. See [the Airflow guide](airflow/dags/README.md) for provider setup, PostgreSQL connection configuration, UI access at `http://localhost:8080`, manual trigger examples, and failure email setup.

## Grafana dashboards

Grafana is provisioned with a read-only PostgreSQL datasource and four dashboards: Executive Overview, Device Performance, Anomaly Monitoring, and Regional Analysis. They include fleet KPIs, telemetry trends, severity tables with critical alerts highlighted, and Region and Device ID filters. Dashboard files are provisioned from JSON and refresh every 30 seconds. See the [Grafana setup guide](grafana/README.md) for startup, login, and existing-database setup.

## Initial setup

Copy `.env.example` to `.env`, update local credentials, then start the complete platform with `docker compose up --build -d`. Install development dependencies with `python -m pip install -e ".[dev]"`. Unit tests are in `tests/unit/`.

