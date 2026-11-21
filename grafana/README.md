# Grafana dashboards

Grafana is provisioned with the project's PostgreSQL database as a read-only data source and four JSON dashboards:

| Dashboard | Purpose |
|---|---|
| Executive Overview | Fleet KPIs, health and event trends, severity distribution, and unresolved critical incidents. |
| Device Performance | Health, temperature, CPU, memory, network, battery, uptime, and latest readings. |
| Anomaly Monitoring | Anomaly volume and severity, affected devices, recent anomalous samples, and open critical alerts. |
| Regional Analysis | Regional health, uptime, utilization, latency, anomaly volume, and summary table. |

All dashboards default to the last 24 hours, refresh every 30 seconds, and provide `Region` and `Device ID` dropdown filters. Panels that show active critical incidents are scoped by those filters and sort newest activity first. Critical alert tables include severity fields to make the highest severity easy to identify.

## Start locally

1. Copy `.env.example` to `.env` in the project root. Set unique values for `POSTGRES_PASSWORD`, `GRAFANA_DB_PASSWORD`, and `GRAFANA_PASSWORD`.
2. Start PostgreSQL and Grafana:

   ```powershell
   docker compose up -d postgres grafana
   ```

   On a new or existing volume, the `postgres-bootstrap` service synchronizes the `grafana_reader` login and grants SELECT access. The schema is initialized on first database creation (and idempotently by the Kafka consumer). The reader password comes from `GRAFANA_DB_PASSWORD`.
3. Open `http://localhost:3000` (or the configured `GRAFANA_PORT`) and sign in with `GRAFANA_USER` and `GRAFANA_PASSWORD`.
4. Open **Dashboards → IoT Observability**. Grafana loads the datasource and dashboards from the mounted provisioning files.

The PostgreSQL datasource is named **IoT PostgreSQL** and has the stable UID `iot-postgres`. Its connection uses the Compose DNS name `postgres:5432`, database `POSTGRES_DB`, and environment-substituted credentials. The account is read-only so dashboard queries cannot change telemetry records. Datasource credentials are provided to Grafana through `$__env{...}` substitutions, following Grafana's [PostgreSQL datasource provisioning](https://grafana.com/docs/grafana/latest/datasources/postgres/configure/) guidance.

## Existing PostgreSQL volumes

PostgreSQL's `/docker-entrypoint-initdb.d` scripts only run when its data directory is first initialized. The separate `postgres-bootstrap` service applies service-role SQL after PostgreSQL is healthy, including when using an existing volume. If you change `GRAFANA_DB_USER` or `GRAFANA_DB_PASSWORD`, recreate the bootstrap and Grafana services with `docker compose up -d --force-recreate postgres-bootstrap grafana`.

## Provisioning files

- [`provisioning/datasources/iot-postgres.yaml`](provisioning/datasources/iot-postgres.yaml) configures the PostgreSQL datasource.
- [`provisioning/dashboards/provider.yaml`](provisioning/dashboards/provider.yaml) loads JSON dashboards from `/var/lib/grafana/dashboards` into the **IoT Observability** folder.
- [`dashboards/`](dashboards/) contains the four provisioned JSON dashboards. Regenerate them with `python grafana/generate_dashboards.py` after changing the dashboard definitions in `generate_dashboards.py`.

Provisioning is file-backed; dashboard edits in the UI are disabled to keep the JSON files as the source of truth. Grafana checks the directory every 30 seconds and updates provisioned dashboards. See Grafana's [file provisioning documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/) for provider behavior.

The SQL uses the project tables and rolling analytical views documented in [`../postgres/README.md`](../postgres/README.md). Device and region dropdowns are populated from `devices`; panels use the dashboard time range where event-level trends are shown. Some KPI panels intentionally show rolling 24-hour or current incident status, indicated in their titles or descriptions.
