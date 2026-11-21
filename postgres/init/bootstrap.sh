#!/bin/sh
set -eu

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set ON_ERROR_STOP=1 \
  --set db_name="$POSTGRES_DB" \
  --set app_user="$POSTGRES_USER" \
  --set grafana_user="$GRAFANA_DB_USER" \
  --set grafana_password="$GRAFANA_DB_PASSWORD" \
  --file /bootstrap/02-grafana-reader.sql

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set ON_ERROR_STOP=1 \
  --set db_name="$POSTGRES_DB" \
  --set airflow_user="$AIRFLOW_DB_USER" \
  --set airflow_password="$AIRFLOW_DB_PASSWORD" \
  --file /bootstrap/03-airflow-role.sql
