# Device simulator

Run from the repository root after installing the project:

```powershell
python -m pip install -e ".[dev]"
$env:MQTT_HOST = "localhost"
python -m iot_telemetry.simulator
```

The default is 500 unique devices, grouped round-robin across six geographical regions. Every fleet tick (2 seconds by default), each available device publishes one JSON reading to `iot/devices/{device_id}/telemetry`. Readings use correlated load trends across CPU, temperature, memory, latency, and packet loss; battery drains gradually. Rare anomalies are explicitly marked. A simulated outage suppresses a device for a configurable number of ticks; unaffected devices continue publishing.

| Setting | Default | Description |
|---|---:|---|
| `SIMULATOR_DEVICE_COUNT` | `500` | Fleet size |
| `SIMULATOR_INTERVAL_SECONDS` | `2` | Target fleet tick interval |
| `SIMULATOR_SPEED_MULTIPLIER` | `1` | Rate multiplier; `2` means twice as fast |
| `SIMULATOR_ABNORMAL_PROBABILITY` | `0.01` | Per available device tick probability of an abnormal metric |
| `SIMULATOR_FAILURE_PROBABILITY` | `0.0005` | Per available device tick probability of an outage |
| `SIMULATOR_MIN_FAILURE_TICKS` / `SIMULATOR_MAX_FAILURE_TICKS` | `3` / `15` | Outage duration bounds |
| `SIMULATOR_RANDOM_SEED` | unset | Optional seed for repeatable generated values |
| `MQTT_HOST` / `MQTT_PORT` | `localhost` / `1883` | Broker endpoint |
| `SIMULATOR_MQTT_QOS` | `1` | MQTT delivery QoS |

SIGINT and SIGTERM stop the loop and close MQTT cleanly. Broker connection errors fail startup; individual publish errors are logged and do not prevent other device events from being attempted. Unit tests cover fleet identity/regions, event serialization, anomaly and outage behavior, and configuration.

