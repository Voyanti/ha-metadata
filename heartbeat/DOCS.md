# Heartbeat

Periodic network connectivity checks, buffered in SQLite and published to MQTT.

## How it works

On each `heartbeat_interval` the app runs three checks:

| Check | What it does | MQTT topic |
|-------|--------------|------------|
| Gateway ping | Discovers the default gateway (`ip route show default 0.0.0.0/0`) and runs `ping -c 2 -W 2 <gw>` | `heartbeat/gateway/default_gateway` |
| Public DNS ping | `ping -c 2 -W 2 <host>` for each configured target | `heartbeat/public_dns/<name>` |
| Broker DNS resolution | `getent hosts <host> \|\| nslookup <host>` and parses the resolved IPs | `heartbeat/mqtt_broker/mqtt_broker_endpoint` |

The full topic is `[<topic_prefix>/]heartbeat/<type>/<name>`. Set `mqtt.topic_prefix`
to a custom namespace (e.g. a site name or AWS IoT thing name) and every topic is
prefixed with it — `mysite/heartbeat/gateway/default_gateway`. Leave it empty
(the default) to get the topics above. `mqtt.client_id` is not part of the topic.

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
  topic_prefix: ""           # optional custom namespace: <topic_prefix>/heartbeat/...
  client_id: ""              # MQTT connection id (e.g. AWS IoT thing name); not in the topic
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
| `mqtt.topic_prefix` | Optional custom namespace. Topics are `[<topic_prefix>/]heartbeat/<type>/<name>`. Empty → `heartbeat/<type>/<name>`. |
| `mqtt.client_id` | MQTT connection identifier (e.g. an AWS IoT thing name). **Not** part of the topic. Empty → the broker auto-generates one. |
| `mqtt.tls` | Use TLS for the broker connection. |
| `mqtt.ca_cert` | Path to a CA certificate (PEM) to verify the broker. Empty → system CA bundle. |
| `mqtt.client_cert` | Path to the client certificate (PEM) for mutual TLS. |
| `mqtt.client_key` | Path to the client private key (PEM) for mutual TLS. |
| `mqtt.tls_insecure` | Skip broker hostname/cert verification (testing only — leave `false`). |

Setting any of `ca_cert` / `client_cert` enables TLS even if `tls` is left `false`.

## Connecting to AWS IoT Core (mutual TLS)

AWS IoT Core authenticates clients with an X.509 **client certificate + private
key** over TLS on port **8883** — there is no username/password. In AWS IoT,
create a Thing, generate/attach a certificate, attach a policy allowing
`iot:Connect` / `iot:Publish` (on your topics), and note your account's
**ATS data endpoint** (`xxxx-ats.iot.<region>.amazonaws.com`).

1. Copy the three PEM files to the Home Assistant `/ssl` directory (this app maps
   `/ssl` read-only):
   - the Amazon root CA, e.g. `AmazonRootCA1.pem`
   - the device certificate, e.g. `xxxx-certificate.pem.crt`
   - the device private key, e.g. `xxxx-private.pem.key`
2. Configure MQTT:

```yaml
mqtt:
  host: "xxxx-ats.iot.eu-north-1.amazonaws.com"
  port: 8883
  tls: true
  ca_cert: /ssl/AmazonRootCA1.pem
  client_cert: /ssl/xxxx-certificate.pem.crt
  client_key: /ssl/xxxx-private.pem.key
  client_id: my-thing-name    # connection id = the AWS IoT thing name
  topic_prefix: my-thing-name # publish under <thing-name>/heartbeat/... per the policy
```

Notes:
- Leave `username`/`password` empty for AWS IoT.
- AWS IoT supports QoS 0 and 1 only — the default QoS 1 is fine.
- Set `client_id` to the **thing name** (used for `iot:Connect`), and set
  `topic_prefix` to the same thing name so topics become
  `my-thing-name/heartbeat/gateway/default_gateway`, matching policies that
  restrict publishing to thing-name-prefixed topics.
- The certificate must be **active** and its policy must allow `iot:Connect` for
  the `client_id` and `iot:Publish` on those topics.

Standalone (outside Home Assistant), mount the certs and point the env vars at them:

```bash
docker run --rm --network host --cap-add NET_RAW \
  -e HEARTBEAT_MQTT_HOST=xxxx-ats.iot.eu-north-1.amazonaws.com \
  -e HEARTBEAT_MQTT_PORT=8883 -e HEARTBEAT_MQTT_TLS=true \
  -e HEARTBEAT_MQTT_CA_CERT=/certs/AmazonRootCA1.pem \
  -e HEARTBEAT_MQTT_CLIENT_CERT=/certs/device.pem.crt \
  -e HEARTBEAT_MQTT_CLIENT_KEY=/certs/private.pem.key \
  -v /path/to/certs:/certs:ro -v hb-data:/data \
  <image>
```

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
