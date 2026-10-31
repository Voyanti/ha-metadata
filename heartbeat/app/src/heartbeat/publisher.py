"""MQTT publisher (paho-mqtt v2), tolerant of a down broker.

Connection is asynchronous and runs in paho's background network thread, so the
service loop never blocks on it. When the broker is unreachable, ``flush()``
publishes nothing and the rows stay queued in the outbox; paho auto-reconnects
and the backlog drains (newest first) once it is back, paced to
``max_publish_rate`` so a large backlog never bursts past the broker's limit.
"""

from __future__ import annotations

import logging
import math
import ssl
import threading
import time
from collections.abc import Callable

import paho.mqtt.client as mqtt

from .config import MqttConfig
from .storage import Outbox

_LOGGER = logging.getLogger(__name__)


class _Pacer:
    """Spaces successive calls to ``wait()`` at least ``1/rate`` seconds apart,
    so no one-second window ever holds more than ``rate`` calls. Time is kept in
    integer nanoseconds so that bound is exact rather than subject to float drift."""

    def __init__(
        self,
        rate: float,
        *,
        clock: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._gap_ns = math.ceil(1e9 / rate)
        self._clock = clock
        self._sleep = sleep
        self._next_ns: int | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._next_ns is not None:
            while now < self._next_ns:
                self._sleep((self._next_ns - now) / 1e9)
                now = self._clock()
        self._next_ns = now + self._gap_ns


class MqttPublisher:
    def __init__(
        self,
        cfg: MqttConfig,
        client: mqtt.Client | None = None,
        *,
        pacer: _Pacer | None = None,
    ) -> None:
        # ``client`` and ``pacer`` are injectable so tests can substitute a fake
        # MQTT client and a virtual clock and exercise the real flush logic;
        # production builds the paho client and paces on the monotonic clock.
        self.cfg = cfg
        self._pacer = pacer if pacer is not None else _Pacer(cfg.max_publish_rate)
        self._connected = threading.Event()
        self._client = client if client is not None else self._build_client(cfg)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

    def _build_client(self, cfg: MqttConfig) -> mqtt.Client:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=cfg.client_id or None,
        )
        if cfg.username:
            client.username_pw_set(cfg.username, cfg.password or None)
        # Enable TLS when explicitly requested, or implicitly when any cert is
        # provided (mutual TLS, e.g. AWS IoT Core). ca_cert/client_cert/
        # client_key default to the system CA bundle / no client cert when None.
        # ALPN (AWS IoT Core on port 443) needs a hand-built SSLContext, which
        # tls_set() cannot provide.
        if cfg.alpn:
            client.tls_set_context(_alpn_context(cfg))
        elif cfg.tls or cfg.ca_cert or cfg.client_cert:
            client.tls_set(
                ca_certs=cfg.ca_cert,
                certfile=cfg.client_cert,
                keyfile=cfg.client_key,
            )
            if cfg.tls_insecure:
                client.tls_insecure_set(True)
        _LOGGER.info("MQTT TLS: %s", _describe_tls(cfg))
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        # Allow many qos>0 messages in flight at once so a backlog flush
        # pipelines instead of blocking on each PUBACK in turn (default caps at
        # AWS IoT Core's per-connection in-flight limit).
        client.max_inflight_messages_set(cfg.max_inflight)
        # Surface paho's own connection diagnostics (socket errors, retries).
        client.enable_logger(logging.getLogger("heartbeat.mqtt"))
        return client

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

    def _send(self, topic: str, payload_json: str):
        """Issue a publish without blocking on its PUBACK. Returns paho's
        message info, or ``None`` if the client rejected the send outright."""
        try:
            info = self._client.publish(
                topic, payload_json, qos=self.cfg.qos, retain=self.cfg.retain
            )
        except (ValueError, RuntimeError) as exc:
            _LOGGER.warning("MQTT publish error on %s: %s", topic, exc)
            return None
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            _LOGGER.warning("MQTT publish to %s returned rc=%s", topic, info.rc)
            return None
        return info

    def _confirm(self, info, topic: str) -> bool:
        """Block until the broker acknowledges a qos>0 publish. qos 0 is
        fire-and-forget and confirms immediately."""
        if self.cfg.qos == 0:
            return True
        try:
            info.wait_for_publish(timeout=5.0)
        except (ValueError, RuntimeError) as exc:
            _LOGGER.warning("MQTT publish to %s not confirmed: %s", topic, exc)
            return False
        if not info.is_published():
            _LOGGER.warning("MQTT publish to %s not confirmed (timeout)", topic)
            return False
        return True

    def publish_result(self, payload_json: str, target_type: str, target_name: str) -> bool:
        if not self.connected:
            return False
        topic = self._topic(target_type, target_name)
        info = self._send(topic, payload_json)
        return info is not None and self._confirm(info, topic)

    def flush(self, outbox: Outbox, limit: int = 200) -> tuple[int, int]:
        """Publish pending rows newest-first. Every publish is issued first, at
        most ``max_publish_rate`` per second, so the messages go in flight
        concurrently on paho's network thread (bounded by
        max_inflight_messages); then each PUBACK is awaited independently.
        Every confirmed row is deleted and every unconfirmed one is left queued
        for retry — a delivered message is never republished just because an
        earlier or later one in the batch failed. Returns
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

        # Phase 1: issue the publishes without waiting for PUBACKs, paced to
        # max_publish_rate (AWS IoT Core drops a connection that exceeds its
        # per-connection limit). A row the client refuses to send stays queued
        # (unconfirmed) and retries next cycle. If the broker drops mid-batch,
        # stop sending: the unsent rows stay queued for the next cycle.
        inflight = []  # (row, info) for accepted sends
        unconfirmed_ids = []
        for i, row in enumerate(rows):
            self._pacer.wait()
            if not self.connected:
                unconfirmed_ids.extend(r.id for r in rows[i:])
                break
            info = self._send(self._topic(row.target_type, row.target_name), row.payload_json)
            if info is None:
                unconfirmed_ids.append(row.id)
            else:
                inflight.append((row, info))

        # Phase 2: confirm each independently — no stop-at-first-failure. If the
        # broker drops mid-batch, stop waiting: each unacked confirm would block
        # for the full timeout, so leave the rest queued to retry next cycle.
        confirmed_ids = []
        for i, (row, info) in enumerate(inflight):
            if not self.connected:
                unconfirmed_ids.extend(r.id for r, _ in inflight[i:])
                break
            if self._confirm(info, self._topic(row.target_type, row.target_name)):
                confirmed_ids.append(row.id)
            else:
                unconfirmed_ids.append(row.id)

        if confirmed_ids:
            outbox.delete(confirmed_ids)
        if unconfirmed_ids:
            outbox.increment_attempts(unconfirmed_ids)

        published = len(confirmed_ids)
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


def _alpn_context(cfg: MqttConfig) -> ssl.SSLContext:
    """TLS context for brokers that select the protocol by ALPN, e.g. AWS IoT
    Core on port 443 (``x-amzn-mqtt-ca``), which also requires a client
    certificate. Unset ca_cert means the system CA bundle, as with tls_set()."""
    ctx = ssl.create_default_context(cafile=cfg.ca_cert)
    if cfg.client_cert:
        ctx.load_cert_chain(cfg.client_cert, cfg.client_key)
    ctx.set_alpn_protocols([cfg.alpn])
    if cfg.tls_insecure:
        # Same meaning as tls_insecure_set(True) on the tls_set() path: skip
        # the hostname check, still verify the certificate chain.
        ctx.check_hostname = False
    return ctx


def _describe_tls(cfg: MqttConfig) -> str:
    """One-line TLS mode for the log. Names settings only, never key material."""
    if not (cfg.alpn or cfg.tls or cfg.ca_cert or cfg.client_cert):
        return "off"
    parts = ["on"]
    if cfg.alpn:
        parts.append(f"ALPN {cfg.alpn!r}")
    parts.append("client certificate " + ("set" if cfg.client_cert else "not set"))
    parts.append("CA " + ("from ca_cert" if cfg.ca_cert else "system bundle"))
    if cfg.tls_insecure:
        parts.append("hostname check DISABLED (tls_insecure)")
    return ", ".join(parts)
