"""Entry point. Wires config -> storage -> publisher -> service loop, and
handles SIGTERM/SIGINT (s6 sends SIGTERM on shutdown) for a clean flush+close."""

from __future__ import annotations

import logging
import signal
import threading

from .config import load_config
from .logging_setup import configure_logging
from .publisher import MqttPublisher
from .runner import SubprocessRunner
from .service import HeartbeatService
from .storage import Outbox

_LOGGER = logging.getLogger(__name__)


def main() -> int:
    cfg = load_config()
    configure_logging(cfg.log_level)
    _LOGGER.info(
        "Starting heartbeat (interval=%ss, db=%s, mqtt=%s:%s, broker_dns=%s)",
        cfg.heartbeat_interval,
        cfg.db_path,
        cfg.mqtt.host or "<unset>",
        cfg.mqtt.port,
        cfg.effective_broker_dns_target or "<unset>",
    )

    outbox = Outbox(cfg.db_path)
    outbox.init()
    publisher = MqttPublisher(cfg.mqtt)
    publisher.connect()
    service = HeartbeatService(cfg, SubprocessRunner(), outbox, publisher)

    stop = threading.Event()

    def _handle(signum, _frame):
        _LOGGER.info("Received signal %s; shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    # The check writer and the MQTT flusher run independently: a publish backlog
    # never delays a check, and flushing is not tied to the heartbeat interval.
    writer = threading.Thread(target=service.run_writer, args=(stop,), name="writer")
    flusher = threading.Thread(target=service.run_flusher, args=(stop,), name="flusher")
    writer.start()
    flusher.start()

    try:
        while not stop.wait(1.0):  # main thread parks here, wakes to run signal handlers
            pass
    finally:
        stop.set()
        writer.join(timeout=10)
        flusher.join(timeout=10)
        _LOGGER.info("Final flush before exit")
        try:
            publisher.flush(outbox, limit=cfg.flush_batch)
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Final flush failed")
        publisher.close()
        outbox.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
