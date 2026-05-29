## 1.0.0

- Initial release.
- Periodic heartbeat checks ported from the Node-RED flow: default-gateway ping,
  public-DNS ping, and MQTT-broker DNS resolution.
- SQLite store-and-forward outbox (delete-after-publish) with a backlog cap.
- MQTT publishing (paho-mqtt) to a configurable external broker.
- uv-managed Python 3.14; runnable both as a Home Assistant app and as a plain
  Docker container on a Linux host.
