## 1.3.0

- Topics are now `[<topic_prefix>/]heartbeat/<type>/<name>`. `topic_prefix` is an
  optional custom namespace (default empty → `heartbeat/...` as before);
  `client_id` is no longer part of the topic — it is only the MQTT connection id.
- Log MQTT connectivity loudly: warn (not debug) when the broker is not connected
  and results are queued, warn on unconfirmed publishes, surface paho's own
  connection errors, and include `mqtt_connected` in the per-tick log line.

## 1.2.0

- Add `mqtt.client_id` option. When set, published topics are namespaced with it
  (`<client_id>/<topic_prefix>/<type>/<name>`) so multiple instances can share a
  broker and work with brokers that restrict publishing to client-id-prefixed
  topics. Default empty (topics and connection id unchanged).

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
