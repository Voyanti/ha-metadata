# Home Assistant App: Heartbeat

Periodic network heartbeat checker. Each interval it:

1. Pings the host's **default gateway** (discovered via `ip route`).
2. Pings a configurable list of **public DNS** targets (default `1.1.1.1`, `8.8.8.8`).
3. **Resolves the MQTT broker hostname** via `getent`/`nslookup`.

Every result is written to a SQLite **store-and-forward outbox** and published to
an MQTT broker. If the broker (or network) is down, results stay queued and are
published once connectivity returns — so a heartbeat is never lost during the
outage it is meant to detect.

The application core is Home-Assistant-agnostic, so the same image also runs as
a plain Docker container on any Linux host (see [DOCS.md](./DOCS.md)).

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]

See [DOCS.md](./DOCS.md) for configuration and standalone usage.

[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg
