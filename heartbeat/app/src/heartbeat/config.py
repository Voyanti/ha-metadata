"""Typed configuration and the single config-loading seam.

Precedence (lowest to highest):

    built-in defaults  <  /data/options.json (if present)  <  HEARTBEAT_* env vars

``/data/options.json`` is written by the Home Assistant supervisor from the
add-on's ``options:`` schema. When running as a plain container there is no such
file and configuration comes entirely from environment variables. This is the
*only* Home-Assistant-aware behaviour in the whole package: "read a JSON file if
it happens to exist".
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace

_LOGGER = logging.getLogger(__name__)

DEFAULT_OPTIONS_FILE = "/data/options.json"


@dataclass(frozen=True)
class PingTarget:
    name: str
    host: str


@dataclass(frozen=True)
class MqttConfig:
    host: str = ""
    port: int = 1883
    username: str | None = None
    password: str | None = None
    tls: bool = False
    topic_prefix: str = ""  # optional custom namespace -> <topic_prefix>/heartbeat/<type>/<name>
    qos: int = 1
    retain: bool = False
    max_inflight: int = 100  # IoT Core per-connection in-flight limit
    client_id: str = ""  # empty -> no topic namespace + broker auto-generates the connection id
    # Mutual TLS (e.g. AWS IoT Core): paths to PEM files.
    ca_cert: str | None = None
    client_cert: str | None = None
    client_key: str | None = None
    tls_insecure: bool = False


@dataclass(frozen=True)
class Config:
    heartbeat_interval: int = 60
    check_gateway: bool = True
    ping_targets: tuple[PingTarget, ...] = (
        PingTarget("cloudflare", "1.1.1.1"),
        PingTarget("google", "8.8.8.8"),
    )
    broker_dns_target: str = ""
    db_path: str = "/data/heartbeat.db"
    log_level: str = "info"
    ping_count: int = 2
    ping_timeout: int = 2
    cmd_timeout: float = 10.0
    max_backlog_rows: int = 100_000
    flush_batch: int = 200
    flush_interval: float = 15.0  # seconds between flush cycles (independent of checks)
    concurrent_pings: bool = True
    mqtt: MqttConfig = field(default_factory=MqttConfig)

    @property
    def effective_broker_dns_target(self) -> str:
        """Hostname to DNS-resolve for the broker check; falls back to the
        broker we publish to when not explicitly set."""
        return self.broker_dns_target or self.mqtt.host


# --------------------------------------------------------------------------- #
# coercion helpers
# --------------------------------------------------------------------------- #
def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _clean_cred(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def parse_ping_targets(value: object) -> tuple[PingTarget, ...]:
    """Accept a list of dicts, a JSON array string, or a ``name=host,name=host``
    CSV string. Dict items may use ``host`` or (flow-style) ``ip``."""
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        if text.startswith("["):
            value = json.loads(text)
        else:
            items: list[dict] = []
            for pair in text.split(","):
                pair = pair.strip()
                if not pair:
                    continue
                name, _, host = pair.partition("=")
                items.append({"name": name.strip(), "host": host.strip()})
            value = items

    targets: list[PingTarget] = []
    for item in value:  # type: ignore[union-attr]
        if isinstance(item, PingTarget):
            targets.append(item)
            continue
        host = str(item.get("host", item.get("ip", ""))).strip()
        name = str(item.get("name", "")).strip()
        if host:
            targets.append(PingTarget(name or host, host))
    return tuple(targets)


# --------------------------------------------------------------------------- #
# loader
# --------------------------------------------------------------------------- #
def load_config(environ: Mapping[str, str] | None = None) -> Config:
    environ = os.environ if environ is None else environ
    cfg = Config()
    mqtt = cfg.mqtt

    # 1) Home Assistant options.json overlay -------------------------------- #
    options_path = environ.get("HEARTBEAT_OPTIONS_FILE", DEFAULT_OPTIONS_FILE)
    options: dict = {}
    if options_path and os.path.isfile(options_path):
        try:
            with open(options_path, encoding="utf-8") as fh:
                options = json.load(fh) or {}
            _LOGGER.info("Loaded options from %s", options_path)
        except (OSError, json.JSONDecodeError) as exc:
            _LOGGER.warning("Could not read options file %s: %s", options_path, exc)

    def opt(key: str, default):
        return options[key] if key in options else default

    cfg = replace(
        cfg,
        heartbeat_interval=int(opt("heartbeat_interval", cfg.heartbeat_interval)),
        check_gateway=_as_bool(opt("check_gateway", cfg.check_gateway)),
        broker_dns_target=str(opt("broker_dns_target", cfg.broker_dns_target)),
        db_path=str(opt("db_path", cfg.db_path)),
        log_level=str(opt("log_level", cfg.log_level)),
    )
    if "ping_targets" in options:
        cfg = replace(cfg, ping_targets=parse_ping_targets(options["ping_targets"]))

    mqtt_opts = options.get("mqtt") or {}
    mqtt = replace(
        mqtt,
        host=str(mqtt_opts.get("host", mqtt.host)),
        port=int(mqtt_opts.get("port", mqtt.port)),
        username=_clean_cred(mqtt_opts.get("username", mqtt.username)),
        password=_clean_cred(mqtt_opts.get("password", mqtt.password)),
        tls=_as_bool(mqtt_opts.get("tls", mqtt.tls)),
        topic_prefix=str(mqtt_opts.get("topic_prefix", mqtt.topic_prefix)),
        qos=int(mqtt_opts.get("qos", mqtt.qos)),
        retain=_as_bool(mqtt_opts.get("retain", mqtt.retain)),
        max_inflight=int(mqtt_opts.get("max_inflight", mqtt.max_inflight)),
        client_id=str(mqtt_opts.get("client_id", mqtt.client_id)),
        ca_cert=_clean_cred(mqtt_opts.get("ca_cert", mqtt.ca_cert)),
        client_cert=_clean_cred(mqtt_opts.get("client_cert", mqtt.client_cert)),
        client_key=_clean_cred(mqtt_opts.get("client_key", mqtt.client_key)),
        tls_insecure=_as_bool(mqtt_opts.get("tls_insecure", mqtt.tls_insecure)),
    )

    # 2) environment variable overrides ------------------------------------- #
    e = environ
    if "HEARTBEAT_INTERVAL" in e:
        cfg = replace(cfg, heartbeat_interval=int(e["HEARTBEAT_INTERVAL"]))
    if "HEARTBEAT_CHECK_GATEWAY" in e:
        cfg = replace(cfg, check_gateway=_as_bool(e["HEARTBEAT_CHECK_GATEWAY"]))
    if "HEARTBEAT_PING_TARGETS" in e:
        cfg = replace(cfg, ping_targets=parse_ping_targets(e["HEARTBEAT_PING_TARGETS"]))
    if "HEARTBEAT_BROKER_DNS_TARGET" in e:
        cfg = replace(cfg, broker_dns_target=e["HEARTBEAT_BROKER_DNS_TARGET"])
    if "HEARTBEAT_DB_PATH" in e:
        cfg = replace(cfg, db_path=e["HEARTBEAT_DB_PATH"])
    if "HEARTBEAT_LOG_LEVEL" in e:
        cfg = replace(cfg, log_level=e["HEARTBEAT_LOG_LEVEL"])
    if "HEARTBEAT_PING_COUNT" in e:
        cfg = replace(cfg, ping_count=int(e["HEARTBEAT_PING_COUNT"]))
    if "HEARTBEAT_PING_TIMEOUT" in e:
        cfg = replace(cfg, ping_timeout=int(e["HEARTBEAT_PING_TIMEOUT"]))
    if "HEARTBEAT_CMD_TIMEOUT" in e:
        cfg = replace(cfg, cmd_timeout=float(e["HEARTBEAT_CMD_TIMEOUT"]))
    if "HEARTBEAT_MAX_BACKLOG_ROWS" in e:
        cfg = replace(cfg, max_backlog_rows=int(e["HEARTBEAT_MAX_BACKLOG_ROWS"]))
    if "HEARTBEAT_FLUSH_BATCH" in e:
        cfg = replace(cfg, flush_batch=int(e["HEARTBEAT_FLUSH_BATCH"]))
    if "HEARTBEAT_FLUSH_INTERVAL" in e:
        cfg = replace(cfg, flush_interval=float(e["HEARTBEAT_FLUSH_INTERVAL"]))
    if "HEARTBEAT_CONCURRENT_PINGS" in e:
        cfg = replace(cfg, concurrent_pings=_as_bool(e["HEARTBEAT_CONCURRENT_PINGS"]))

    if "HEARTBEAT_MQTT_HOST" in e:
        mqtt = replace(mqtt, host=e["HEARTBEAT_MQTT_HOST"])
    if "HEARTBEAT_MQTT_PORT" in e:
        mqtt = replace(mqtt, port=int(e["HEARTBEAT_MQTT_PORT"]))
    if "HEARTBEAT_MQTT_USERNAME" in e:
        mqtt = replace(mqtt, username=_clean_cred(e["HEARTBEAT_MQTT_USERNAME"]))
    if "HEARTBEAT_MQTT_PASSWORD" in e:
        mqtt = replace(mqtt, password=_clean_cred(e["HEARTBEAT_MQTT_PASSWORD"]))
    if "HEARTBEAT_MQTT_TLS" in e:
        mqtt = replace(mqtt, tls=_as_bool(e["HEARTBEAT_MQTT_TLS"]))
    if "HEARTBEAT_MQTT_TOPIC_PREFIX" in e:
        mqtt = replace(mqtt, topic_prefix=e["HEARTBEAT_MQTT_TOPIC_PREFIX"])
    if "HEARTBEAT_MQTT_QOS" in e:
        mqtt = replace(mqtt, qos=int(e["HEARTBEAT_MQTT_QOS"]))
    if "HEARTBEAT_MQTT_MAX_INFLIGHT" in e:
        mqtt = replace(mqtt, max_inflight=int(e["HEARTBEAT_MQTT_MAX_INFLIGHT"]))
    if "HEARTBEAT_MQTT_RETAIN" in e:
        mqtt = replace(mqtt, retain=_as_bool(e["HEARTBEAT_MQTT_RETAIN"]))
    if "HEARTBEAT_MQTT_CLIENT_ID" in e:
        mqtt = replace(mqtt, client_id=e["HEARTBEAT_MQTT_CLIENT_ID"])
    if "HEARTBEAT_MQTT_CA_CERT" in e:
        mqtt = replace(mqtt, ca_cert=_clean_cred(e["HEARTBEAT_MQTT_CA_CERT"]))
    if "HEARTBEAT_MQTT_CLIENT_CERT" in e:
        mqtt = replace(mqtt, client_cert=_clean_cred(e["HEARTBEAT_MQTT_CLIENT_CERT"]))
    if "HEARTBEAT_MQTT_CLIENT_KEY" in e:
        mqtt = replace(mqtt, client_key=_clean_cred(e["HEARTBEAT_MQTT_CLIENT_KEY"]))
    if "HEARTBEAT_MQTT_TLS_INSECURE" in e:
        mqtt = replace(mqtt, tls_insecure=_as_bool(e["HEARTBEAT_MQTT_TLS_INSECURE"]))

    return replace(cfg, mqtt=mqtt)
