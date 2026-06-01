## 1.1.0

- Support mutual-TLS MQTT brokers (e.g. AWS IoT Core): `mqtt.ca_cert`,
  `mqtt.client_cert`, `mqtt.client_key`, and `mqtt.tls_insecure` options.
- Map `/ssl` read-only so certificate/key files can be referenced from there.

## 1.0.0

- Initial release.
- Periodic heartbeat checks ported from the Node-RED flow: default-gateway ping,
  public-DNS ping, and MQTT-broker DNS resolution.
- SQLite store-and-forward outbox (delete-after-publish) with a backlog cap.
- MQTT publishing (paho-mqtt) to a configurable external broker.
- uv-managed Python 3.14; runnable both as a Home Assistant app and as a plain
  Docker container on a Linux host.
