"""MQTT publisher (paho-mqtt v2), tolerant of a down broker.

Connection is asynchronous and runs in paho's background network thread, so the
service loop never blocks on it. When the broker is unreachable, ``flush()``
publishes nothing and the rows stay queued in the outbox; paho auto-reconnects
and the backlog drains (oldest first) once it is back.
"""

from __future__ import annotations

import logging
import threading

import paho.mqtt.client as mqtt

from .config import MqttConfig
from .storage import Outbox

_LOGGER = logging.getLogger(__name__)


class MqttPublisher:
    def __init__(self, cfg: MqttConfig) -> None:
        self.cfg = cfg
        self._connected = threading.Event()
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=cfg.client_id or None,
        )
        if cfg.username:
            self._client.username_pw_set(cfg.username, cfg.password or None)
        # Enable TLS when explicitly requested, or implicitly when any cert is
        # provided (mutual TLS, e.g. AWS IoT Core). ca_cert/client_cert/
        # client_key default to the system CA bundle / no client cert when None.
        if cfg.tls or cfg.ca_cert or cfg.client_cert:
            self._client.tls_set(
                ca_certs=cfg.ca_cert,
                certfile=cfg.client_cert,
                keyfile=cfg.client_key,
            )
            if cfg.tls_insecure:
                self._client.tls_insecure_set(True)
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        # Surface paho's own connection diagnostics (socket errors, retries).
        self._client.enable_logger(logging.getLogger("heartbeat.mqtt"))

    # -- callbacks ---------------------------------------------------------- #
    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        failure = getattr(reason_code, "is_failure", None)
        ok = (failure is False) or (failure is None and reason_code == 0)
        if ok:
            self._connected.set()
            _LOGGER.info("Connected to MQTT broker %s:%s", self.cfg.host, self.cfg.port)
        else:
            self._connected.clear()
            _LOGGER.warning("MQTT connect failed: %s", reason_code)

    def _on_disconnect(self, client, userdata, *args) -> None:
        self._connected.clear()
        _LOGGER.warning("Disconnected from MQTT broker")

    # -- public API --------------------------------------------------------- #
    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def connect(self) -> None:
        if not self.cfg.host:
            _LOGGER.warning(
                "No MQTT host configured; publishing disabled (results will queue)."
            )
            return
        try:
            self._client.connect_async(self.cfg.host, self.cfg.port, keepalive=60)
            self._client.loop_start()
        except (OSError, ValueError) as exc:
            _LOGGER.warning("MQTT connect_async failed: %s", exc)

    def _topic(self, target_type: str, target_name: str) -> str:
        # Topic: [<topic_prefix>/]heartbeat/<type>/<name>. topic_prefix is an
        # optional custom namespace (e.g. a site or AWS IoT thing name); when
        # empty the topic is just heartbeat/<type>/<name>.
        base = f"heartbeat/{target_type}/{target_name}"
        return f"{self.cfg.topic_prefix}/{base}" if self.cfg.topic_prefix else base

    def publish_result(self, payload_json: str, target_type: str, target_name: str) -> bool:
        if not self.connected:
            return False
        topic = self._topic(target_type, target_name)
        try:
            info = self._client.publish(
                topic, payload_json, qos=self.cfg.qos, retain=self.cfg.retain
            )
        except (ValueError, RuntimeError) as exc:
            _LOGGER.warning("MQTT publish error on %s: %s", topic, exc)
            return False
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            _LOGGER.warning("MQTT publish to %s returned rc=%s", topic, info.rc)
            return False
        if self.cfg.qos > 0:
            try:
                info.wait_for_publish(timeout=5.0)
            except (ValueError, RuntimeError) as exc:
                _LOGGER.warning("MQTT publish to %s not confirmed: %s", topic, exc)
                return False
            if not info.is_published():
                _LOGGER.warning("MQTT publish to %s not confirmed (timeout)", topic)
                return False
            return True
        return True

    def flush(self, outbox: Outbox, limit: int = 200) -> tuple[int, int]:
        """Publish pending rows in id order, deleting each on success. Stops at
        the first failure so the rest retry next tick. Returns
        ``(published, remaining_in_batch)``."""
        rows = outbox.fetch_pending(limit)
        if not rows:
            return (0, 0)
        if not self.connected:
            _LOGGER.warning(
                "MQTT not connected to %s:%s; %d result(s) queued (will retry)",
                self.cfg.host or "<unset>", self.cfg.port, outbox.count(),
            )
            return (0, len(rows))

        published = 0
        for row in rows:
            if self.publish_result(row.payload_json, row.target_type, row.target_name):
                outbox.delete([row.id])
                published += 1
            else:
                outbox.increment_attempts([row.id])
                break

        remaining = len(rows) - published
        if published:
            _LOGGER.info(
                "Published %d result(s) to MQTT; %d remaining in batch",
                published, remaining,
            )
        return (published, remaining)

    def close(self) -> None:
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
