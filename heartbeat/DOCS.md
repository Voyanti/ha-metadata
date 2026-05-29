# Heartbeat

Periodic network connectivity checks, buffered in SQLite and published to MQTT.

## How it works

On each `heartbeat_interval` the app runs three checks:

| Check | What it does | MQTT topic |
|-------|--------------|------------|
| Gateway ping | Discovers the default gateway (`ip route show default 0.0.0.0/0`) and runs `ping -c 2 -W 2 <gw>` | `<prefix>/gateway/default_gateway` |
| Public DNS ping | `ping -c 2 -W 2 <host>` for each configured target | `<prefix>/public_dns/<name>` |
| Broker DNS resolution | `getent hosts <host> \|\| nslookup <host>` and parses the resolved IPs | `<prefix>/mqtt_broker/mqtt_broker_endpoint` |

Each result is a JSON document:

```json
{
  "observed_at": "2026-05-29T14:00:00+00:00",
  "check": "ping",
  "target": { "type": "public_dns", "name": "cloudflare", "host": "1.1.1.1" },
  "success": true,
  "rc": 0,
  "error": null,
  "details": { "stdout": "...", "stderr": "" }
}
```

Results are first written to a SQLite outbox at `/data/heartbeat.db`, then
published to MQTT with QoS 1. **Rows are deleted once published.** When the
broker is unreachable, rows stay queued and are retried on later intervals
(store-and-forward). A backlog cap (`max_backlog_rows`, default 100000) drops the
oldest rows during a very long outage so the database cannot grow without bound.

## Configuration

```yaml
heartbeat_interval: 60
check_gateway: true
ping_targets:
  - name: cloudflare
    host: 1.1.1.1
  - name: google
    host: 8.8.8.8
broker_dns_target: ""        # empty -> resolve the MQTT broker host below
log_level: info
mqtt:
  host: ""                   # REQUIRED to publish; empty -> results just queue
  port: 1883
  username: ""
  password: ""
  topic_prefix: heartbeat
  tls: false
```

| Option | Description |
|--------|-------------|
| `heartbeat_interval` | Seconds between check rounds (min 5). |
| `check_gateway` | Enable/disable the default-gateway ping. |
| `ping_targets` | List of `{name, host}` to ping each round. |
| `broker_dns_target` | Hostname to DNS-resolve as a check. Empty → uses `mqtt.host`. |
| `log_level` | `debug` / `info` / `warning` / `error`. |
| `mqtt.host` | MQTT broker hostname/IP. **Empty disables publishing** (results queue). |
| `mqtt.port` | Broker port (default 1883, or 8883 for TLS). |
| `mqtt.username` / `mqtt.password` | Optional broker credentials. |
| `mqtt.topic_prefix` | Topic prefix; results go to `<prefix>/<type>/<name>`. |
| `mqtt.tls` | Use TLS for the broker connection. |

## Permissions

This add-on runs with `host_network: true` (so the gateway check sees the real
LAN default route, not the Docker bridge) and the `NET_RAW` capability (so ICMP
`ping` works). No AppArmor profile is shipped in this version; the network and
subprocess footprint is broad by design (it spawns `ip`/`ping`/`getent`/
`nslookup`), and a too-tight profile would silently break the very checks this
app performs. Hardening with an AppArmor profile is a planned follow-up.

## Running standalone (outside Home Assistant)

The same image runs as a plain container; configuration then comes from
`HEARTBEAT_*` environment variables instead of `/data/options.json`:

```bash
docker run --rm --network host --cap-add NET_RAW \
  -e HEARTBEAT_INTERVAL=30 \
  -e HEARTBEAT_MQTT_HOST=192.168.1.10 \
  -e HEARTBEAT_MQTT_USERNAME=hb -e HEARTBEAT_MQTT_PASSWORD=secret \
  -e HEARTBEAT_PING_TARGETS='[{"name":"cloudflare","host":"1.1.1.1"}]' \
  -e HEARTBEAT_BROKER_DNS_TARGET=192.168.1.10 \
  -e HEARTBEAT_DB_PATH=/data/heartbeat.db \
  -v hb-data:/data \
  <image>
```

Precedence is **defaults < `/data/options.json` (if present) < `HEARTBEAT_*` env vars**.
