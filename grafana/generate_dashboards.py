"""Generate the provisioned Grafana dashboard JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "dashboards"
DATASOURCE = {"type": "postgres", "uid": "iot-postgres"}
REGION_VAR = "${region:sqlstring}"
DEVICE_VAR = "${device_id:sqlstring}"


def scoped(device_alias: str, region_alias: str) -> str:
    return (
        f" AND ({REGION_VAR} = 'All' OR {region_alias}.region = {REGION_VAR})"
        f" AND ({DEVICE_VAR} = 'All' OR {device_alias}.device_id = {DEVICE_VAR})"
    )


def target(sql: str, *, fmt: str = "table", ref: str = "A") -> dict[str, Any]:
    return {"datasource": DATASOURCE, "format": fmt, "rawQuery": True, "rawSql": sql, "refId": ref}


def panel(
    panel_id: int,
    title: str,
    viz: str,
    x: int,
    y: int,
    w: int,
    h: int,
    sql: str,
    *,
    fmt: str = "table",
    unit: str = "short",
    description: str = "",
    color_mode: str = "palette-classic",
    mappings: list[dict[str, Any]] | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
    x_field: str = "",
) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "color": {"mode": color_mode},
        "mappings": mappings or [],
        "thresholds": {
            "mode": "absolute",
            "steps": [
                {"color": "green", "value": None},
                {"color": "yellow", "value": 60},
                {"color": "orange", "value": 80},
                {"color": "red", "value": 95},
            ],
        },
        "unit": unit,
    }
    if minimum is not None:
        defaults["min"] = minimum
    if maximum is not None:
        defaults["max"] = maximum
    if viz == "timeseries":
        defaults["custom"] = {
            "axisBorderShow": False,
            "axisCenteredZero": False,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "drawStyle": "line",
            "fillOpacity": 14,
            "gradientMode": "opacity",
            "lineInterpolation": "smooth",
            "lineWidth": 2,
            "pointSize": 4,
            "showPoints": "never",
            "spanNulls": False,
            "stacking": {"group": "A", "mode": "none"},
            "thresholdsStyle": {"mode": "off"},
        }
    elif viz == "table":
        defaults["custom"] = {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False}

    options: dict[str, Any]
    if viz == "stat":
        options = {
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "showPercentChange": False,
            "textMode": "auto",
            "wideLayout": True,
        }
    elif viz == "timeseries":
        options = {
            "legend": {"calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        }
    elif viz == "barchart":
        options = {
            "barWidth": 0.85,
            "groupWidth": 0.72,
            "legend": {"showLegend": False},
            "orientation": "horizontal",
            "showValue": "always",
            "stacking": {"group": "A", "mode": "none"},
            "tooltip": {"mode": "single"},
            "xField": x_field,
        }
    else:
        options = {"cellHeight": "sm", "showHeader": True, "sortBy": []}

    return {
        "datasource": DATASOURCE,
        "description": description,
        "fieldConfig": {
            "defaults": defaults,
            "overrides": (
                [
                    {
                        "matcher": {"id": "byName", "options": "severity"},
                        "properties": [
                            {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                            {"id": "color", "value": {"mode": "palette-classic"}},
                        ],
                    }
                ]
                if viz == "table" and mappings
                else []
            ),
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": options,
        "targets": [target(sql, fmt=fmt)],
        "title": title,
        "type": viz,
    }


def severity_mappings() -> list[dict[str, Any]]:
    return [
        {
            "type": "value",
            "options": {
                "LOW": {"color": "blue", "index": 0, "text": "LOW"},
                "MEDIUM": {"color": "yellow", "index": 1, "text": "MEDIUM"},
                "HIGH": {"color": "orange", "index": 2, "text": "HIGH"},
                "CRITICAL": {"color": "red", "index": 3, "text": "CRITICAL"},
            },
        }
    ]


def variables() -> list[dict[str, Any]]:
    region_query = (
        "SELECT 'All' AS \"__text\", 'All' AS \"__value\" "
        "UNION SELECT DISTINCT region AS \"__text\", region AS \"__value\" FROM devices "
        "ORDER BY 1"
    )
    device_query = (
        "SELECT 'All' AS \"__text\", 'All' AS \"__value\" "
        "UNION SELECT DISTINCT device_id AS \"__text\", device_id AS \"__value\" FROM devices "
        f"WHERE ({REGION_VAR} = 'All' OR region = {REGION_VAR}) ORDER BY 1"
    )
    common = {
        "current": {"selected": True, "text": "All", "value": "All"},
        "datasource": DATASOURCE,
        "definition": "",
        "hide": 0,
        "includeAll": False,
        "label": "",
        "multi": False,
        "options": [],
        "refresh": 1,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "type": "query",
    }
    region = {
        **common,
        "definition": region_query,
        "label": "Region",
        "name": "region",
        "query": region_query,
        "refresh": 1,
    }
    device = {
        **common,
        "definition": device_query,
        "label": "Device ID",
        "name": "device_id",
        "query": device_query,
        "refresh": 1,
    }
    return [region, device]


def dashboard(title: str, uid: str, description: str, panels: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                    "enable": True,
                    "hide": True,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard",
                }
            ]
        },
        "description": description,
        "editable": False,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": [],
        "liveNow": False,
        "panels": panels,
        "refresh": "30s",
        "schemaVersion": 39,
        "tags": ["iot", "telemetry", "provisioned"],
        "templating": {"list": variables()},
        "time": {"from": "now-24h", "to": "now"},
        "timepicker": {
            "refresh_intervals": ["10s", "30s", "1m", "5m", "15m", "1h"],
            "time_options": ["5m", "15m", "1h", "6h", "12h", "24h", "7d", "30d"],
        },
        "timezone": "utc",
        "title": title,
        "uid": uid,
        "version": 1,
        "weekStart": "monday",
    }


def executive_overview() -> dict[str, Any]:
    f = scoped("d", "d")
    panels = [
        panel(1, "Total Devices", "stat", 0, 0, 6, 4,
              f"SELECT COUNT(*)::double precision AS value FROM devices d WHERE TRUE{f}",
              description="Registered devices in the selected region/device scope."),
        panel(2, "Active Devices · 5m", "stat", 6, 0, 6, 4,
              f"SELECT COUNT(*)::double precision AS value FROM devices d WHERE d.last_seen_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'{f}",
              description="Devices reporting within the previous five minutes."),
        panel(3, "Average Health Score", "stat", 12, 0, 6, 4,
              f"SELECT AVG(p.device_health_score) AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE $__timeFilter(p.event_timestamp){f}",
              unit="percent", minimum=0, maximum=100),
        panel(4, "Open Critical Alerts", "stat", 18, 0, 6, 4,
              f"SELECT COUNT(*)::double precision AS value FROM device_alerts a JOIN devices d USING (device_id) WHERE a.resolved_at IS NULL AND a.severity = 'CRITICAL'{f}",
              unit="short", color_mode="thresholds", description="Unresolved critical incidents. Red indicates immediate attention."),
        panel(5, "Fleet Health Trend", "timeseries", 0, 4, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '5m'), AVG(p.device_health_score) AS \"Health score\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE $__timeFilter(p.event_timestamp){f} GROUP BY 1 ORDER BY 1",
              fmt="time_series", unit="percent", minimum=0, maximum=100),
        panel(6, "Telemetry Events", "timeseries", 12, 4, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '5m'), COUNT(*) AS \"Events\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE $__timeFilter(p.event_timestamp){f} GROUP BY 1 ORDER BY 1",
              fmt="time_series", unit="short"),
        panel(7, "Regional Health · 24h", "barchart", 0, 12, 12, 8,
              f"SELECT d.region, AVG(p.device_health_score) AS \"Health score\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'{f} GROUP BY d.region ORDER BY 2",
              unit="percent", minimum=0, maximum=100),
        panel(8, "Anomalies by Severity", "barchart", 12, 12, 12, 8,
              f"SELECT p.anomaly_severity AS severity, COUNT(*) AS anomaly_count FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.is_anomaly AND $__timeFilter(p.event_timestamp){f} GROUP BY p.anomaly_severity ORDER BY CASE p.anomaly_severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END",
              description="Anomalous telemetry events in the selected time range.", x_field="severity"),
        panel(9, "Open Critical Incidents", "table", 0, 20, 24, 9,
              f"SELECT a.alert_timestamp, a.last_seen_at, d.device_id, d.region, a.alert_type, a.severity, a.occurrence_count, a.details FROM device_alerts a JOIN devices d USING (device_id) WHERE a.resolved_at IS NULL AND a.severity = 'CRITICAL'{f} ORDER BY a.last_seen_at DESC LIMIT 100",
              mappings=severity_mappings(), description="Unresolved critical alerts, highlighted red."),
    ]
    return dashboard("Executive Overview", "iot-executive-overview", "Fleet health, activity, event volume, anomalies, and open critical incidents.", panels)


def device_performance() -> dict[str, Any]:
    f = scoped("d", "d")
    tf = "$__timeFilter(p.event_timestamp)"
    panels = [
        panel(1, "Health Score", "stat", 0, 0, 6, 4,
              f"SELECT AVG(p.device_health_score) AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE {tf}{f}",
              unit="percent", minimum=0, maximum=100),
        panel(2, "Average CPU", "stat", 6, 0, 6, 4,
              f"SELECT AVG(p.cpu_usage_pct) AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE {tf}{f}",
              unit="percent", minimum=0, maximum=100),
        panel(3, "Average Temperature", "stat", 12, 0, 6, 4,
              f"SELECT AVG(p.temperature_c) AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE {tf}{f}",
              unit="celsius"),
        panel(4, "24h Uptime", "stat", 18, 0, 6, 4,
              f"SELECT AVG(u.uptime_pct) AS value FROM v_device_uptime_24h u JOIN devices d USING (device_id) WHERE TRUE{f}",
              unit="percent", minimum=0, maximum=100),
        panel(5, "Temperature", "timeseries", 0, 4, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '5m'), AVG(p.temperature_c) AS \"Temperature\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE {tf}{f} GROUP BY 1 ORDER BY 1",
              fmt="time_series", unit="celsius"),
        panel(6, "CPU and Memory Utilization", "timeseries", 12, 4, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '5m'), AVG(p.cpu_usage_pct) AS \"CPU\", AVG(p.memory_usage_pct) AS \"Memory\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE {tf}{f} GROUP BY 1 ORDER BY 1",
              fmt="time_series", unit="percent", minimum=0, maximum=100),
        panel(7, "Network Latency and Packet Loss", "timeseries", 0, 12, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '5m'), AVG(p.network_latency_ms) AS \"Latency ms\", AVG(p.packet_loss_pct) AS \"Packet loss %\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE {tf}{f} GROUP BY 1 ORDER BY 1",
              fmt="time_series", unit="short"),
        panel(8, "Battery Level", "timeseries", 12, 12, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '5m'), AVG(p.battery_level_pct) AS \"Battery %\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE {tf}{f} GROUP BY 1 ORDER BY 1",
              fmt="time_series", unit="percent", minimum=0, maximum=100),
        panel(9, "Latest Device Readings", "table", 0, 20, 24, 10,
              f"SELECT DISTINCT ON (p.device_id) p.event_timestamp, d.device_id, d.region, d.operational_status, p.temperature_c, p.cpu_usage_pct, p.memory_usage_pct, p.network_latency_ms, p.packet_loss_pct, p.battery_level_pct, p.device_health_score, p.performance_status FROM processed_telemetry p JOIN devices d USING (device_id) WHERE $__timeFilter(p.event_timestamp){f} ORDER BY p.device_id, p.event_timestamp DESC",
              description="Most recent sample within the selected time range for each device."),
    ]
    return dashboard("Device Performance", "iot-device-performance", "Device-level sensor trends and latest operational measurements.", panels)


def anomaly_monitoring() -> dict[str, Any]:
    f = scoped("d", "d")
    panels = [
        panel(1, "Anomaly Events", "stat", 0, 0, 6, 4,
              f"SELECT COUNT(*)::double precision AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.is_anomaly AND $__timeFilter(p.event_timestamp){f}"),
        panel(2, "Critical Anomaly Events", "stat", 6, 0, 6, 4,
              f"SELECT COUNT(*)::double precision AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.is_anomaly AND p.anomaly_severity = 'CRITICAL' AND $__timeFilter(p.event_timestamp){f}",
              color_mode="thresholds"),
        panel(3, "Affected Devices", "stat", 12, 0, 6, 4,
              f"SELECT COUNT(DISTINCT p.device_id)::double precision AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.is_anomaly AND $__timeFilter(p.event_timestamp){f}"),
        panel(4, "Open Critical Incidents", "stat", 18, 0, 6, 4,
              f"SELECT COUNT(*)::double precision AS value FROM device_alerts a JOIN devices d USING (device_id) WHERE a.resolved_at IS NULL AND a.severity = 'CRITICAL'{f}",
              color_mode="thresholds"),
        panel(5, "Anomaly Events by Severity", "timeseries", 0, 4, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '5m'), COUNT(*) FILTER (WHERE p.anomaly_severity = 'LOW') AS \"LOW\", COUNT(*) FILTER (WHERE p.anomaly_severity = 'MEDIUM') AS \"MEDIUM\", COUNT(*) FILTER (WHERE p.anomaly_severity = 'HIGH') AS \"HIGH\", COUNT(*) FILTER (WHERE p.anomaly_severity = 'CRITICAL') AS \"CRITICAL\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.is_anomaly AND $__timeFilter(p.event_timestamp){f} GROUP BY 1 ORDER BY 1",
              fmt="time_series", description="Counts are events; repeated samples in one ongoing incident remain visible."),
        panel(6, "Severity Distribution", "barchart", 12, 4, 12, 8,
              f"SELECT p.anomaly_severity AS severity, COUNT(*) AS anomaly_count FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.is_anomaly AND $__timeFilter(p.event_timestamp){f} GROUP BY p.anomaly_severity ORDER BY CASE p.anomaly_severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END",
              mappings=severity_mappings(), x_field="severity"),
        panel(7, "Recent Anomalous Telemetry", "table", 0, 12, 24, 10,
              f"SELECT p.event_timestamp, d.device_id, d.region, p.anomaly_severity, p.anomaly_score, p.temperature_c, p.cpu_usage_pct, p.memory_usage_pct, p.network_latency_ms, p.packet_loss_pct, p.performance_status FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.is_anomaly AND $__timeFilter(p.event_timestamp){f} ORDER BY p.event_timestamp DESC LIMIT 500",
              mappings=severity_mappings()),
        panel(8, "Open Critical Alerts", "table", 0, 22, 24, 8,
              f"SELECT a.alert_timestamp, a.last_seen_at, d.device_id, d.region, a.alert_type, a.severity, a.occurrence_count, a.details FROM device_alerts a JOIN devices d USING (device_id) WHERE a.resolved_at IS NULL AND a.severity = 'CRITICAL'{f} ORDER BY a.last_seen_at DESC LIMIT 200",
              mappings=severity_mappings()),
    ]
    return dashboard("Anomaly Monitoring", "iot-anomaly-monitoring", "Statistical telemetry anomalies, severity distribution, and active critical incidents.", panels)


def regional_analysis() -> dict[str, Any]:
    f = scoped("d", "d")
    panels = [
        panel(1, "Regions Reporting", "stat", 0, 0, 6, 4,
              f"SELECT COUNT(DISTINCT d.region)::double precision AS value FROM devices d WHERE TRUE{f}"),
        panel(2, "Average Regional Health", "stat", 6, 0, 6, 4,
              f"SELECT AVG(p.device_health_score) AS value FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'{f}",
              unit="percent", minimum=0, maximum=100),
        panel(3, "Average Uptime · 24h", "stat", 12, 0, 6, 4,
              f"SELECT AVG(u.uptime_pct) AS value FROM v_device_uptime_24h u JOIN devices d USING (device_id) WHERE TRUE{f}",
              unit="percent", minimum=0, maximum=100),
        panel(4, "Operational Incidents · 24h", "stat", 18, 0, 6, 4,
              f"SELECT COUNT(*)::double precision AS value FROM device_alerts a JOIN devices d USING (device_id) WHERE a.alert_type IN ('telemetry_gap', 'health_critical') AND a.alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'{f}"),
        panel(5, "Regional Health Score", "barchart", 0, 4, 12, 8,
              f"SELECT d.region, AVG(p.device_health_score) AS \"Health score\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'{f} GROUP BY d.region ORDER BY 2",
              unit="percent", minimum=0, maximum=100, x_field="region"),
        panel(6, "Regional Uptime", "barchart", 12, 4, 12, 8,
              f"SELECT d.region, AVG(u.uptime_pct) AS \"Uptime %\" FROM v_device_uptime_24h u JOIN devices d USING (device_id) WHERE TRUE{f} GROUP BY d.region ORDER BY 2",
              unit="percent", minimum=0, maximum=100, x_field="region"),
        panel(7, "CPU Utilization by Region", "timeseries", 0, 12, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '15m'), d.region AS metric, AVG(p.cpu_usage_pct) AS \"CPU %\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE $__timeFilter(p.event_timestamp){f} GROUP BY 1, d.region ORDER BY 1",
              fmt="time_series", unit="percent", minimum=0, maximum=100),
        panel(8, "Network Latency by Region", "timeseries", 12, 12, 12, 8,
              f"SELECT $__timeGroupAlias(p.event_timestamp, '15m'), d.region AS metric, AVG(p.network_latency_ms) AS \"Latency ms\" FROM processed_telemetry p JOIN devices d USING (device_id) WHERE $__timeFilter(p.event_timestamp){f} GROUP BY 1, d.region ORDER BY 1",
              fmt="time_series", unit="ms"),
        panel(9, "Regional Anomaly Counts", "barchart", 0, 20, 12, 8,
              f"SELECT d.region, COUNT(*) FILTER (WHERE p.is_anomaly) AS anomaly_count FROM processed_telemetry p JOIN devices d USING (device_id) WHERE p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'{f} GROUP BY d.region ORDER BY 2 DESC",
              x_field="region"),
        panel(10, "Regional Performance Summary", "table", 12, 20, 12, 8,
              f"SELECT d.region, COUNT(DISTINCT d.device_id) AS devices, COUNT(p.event_id) AS telemetry_events, AVG(p.device_health_score) AS avg_health_score, AVG(p.cpu_usage_pct) AS avg_cpu_pct, AVG(p.network_latency_ms) AS avg_latency_ms, COUNT(*) FILTER (WHERE p.is_anomaly) AS anomalies FROM devices d LEFT JOIN processed_telemetry p ON p.device_id = d.device_id AND p.event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours' WHERE TRUE{f} GROUP BY d.region ORDER BY avg_health_score DESC"),
    ]
    return dashboard("Regional Analysis", "iot-regional-analysis", "Compare regional health, uptime, utilization, latency, and anomalies.", panels)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    dashboards = [executive_overview(), device_performance(), anomaly_monitoring(), regional_analysis()]
    for item in dashboards:
        path = OUTPUT / f"{item['uid']}.json"
        path.write_text(json.dumps(item, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()

