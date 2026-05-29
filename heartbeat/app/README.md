# heartbeat (application core)

The Python application behind the **Heartbeat** Home Assistant app. It is
deliberately Home-Assistant-agnostic so the same code/image runs as a plain
Docker container on any Linux host.

## What it does

On each interval it runs three checks (ported from a Node-RED flow):

| Check | Command | Target type |
|-------|---------|-------------|
| Default gateway ping | `ip route show default ...` then `ping -c 2 -W 2 <gw>` | `gateway` |
| Public DNS ping | `ping -c 2 -W 2 <host>` per configured target | `public_dns` |
| Broker DNS resolution | `getent hosts <host> \|\| nslookup <host>` | `mqtt_broker` |

Each result is written to a SQLite **outbox** and published to MQTT. Rows are
deleted once published; if the broker is unreachable they stay queued and are
retried (store-and-forward). A backlog cap bounds the DB during long outages.

## Configuration

Config precedence (low → high): **defaults < `/data/options.json` < `HEARTBEAT_*` env vars**.

Common env vars: `HEARTBEAT_INTERVAL`, `HEARTBEAT_CHECK_GATEWAY`,
`HEARTBEAT_PING_TARGETS` (JSON `[{"name","host"}]` or `name=host,name=host`),
`HEARTBEAT_BROKER_DNS_TARGET`, `HEARTBEAT_DB_PATH`, `HEARTBEAT_LOG_LEVEL`,
`HEARTBEAT_MQTT_HOST|PORT|USERNAME|PASSWORD|TLS|TOPIC_PREFIX|QOS|RETAIN`.

## Develop

```bash
uv sync                # create the venv + install deps (uv-managed Python 3.14)
uv run pytest          # run the test suite
uv run heartbeat       # run locally (set HEARTBEAT_* env vars first)
```
