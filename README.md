# Voyanti Home Assistant apps

Home Assistant add-on repository maintained by [Voyanti](https://voyanti.com).

[![Open your Home Assistant instance and show the add-on store with a specific repository URL pre-filled.](https://my.home-assistant.io/badges/supervisor_store.svg)](https://my.home-assistant.io/redirect/supervisor_store/?repository_url=https%3A%2F%2Fgithub.com%2FVoyanti%2Fha-metadata)

## Installation

Add this repository to Home Assistant (Settings → Add-ons → Add-on Store → ⋮ →
Repositories) using the URL `https://github.com/Voyanti/ha-metadata`, or click
the badge above.

## Add-ons

### [Heartbeat](./heartbeat)

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]

_Network heartbeat checker (gateway/DNS ping, broker DNS resolution) buffered to
SQLite store-and-forward and published over MQTT — so a heartbeat is never lost
during the outage it is meant to detect._

See the [Heartbeat README](./heartbeat/README.md) and [DOCS](./heartbeat/DOCS.md)
for configuration and standalone Docker usage.

## License

[Apache License 2.0](./LICENSE)

[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg
