"""The heartbeat service: two independent loops. The writer runs checks and
persists every result to the outbox on the heartbeat interval; the flusher
drains the outbox to MQTT on its own interval. Decoupling them means a publish
backlog never delays a check, and flushing is not rate-limited to one batch per
heartbeat."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .checks import check_broker_dns, check_gateway, check_public_dns_target
from .config import Config
from .models import CheckResult
from .publisher import MqttPublisher
from .runner import CommandRunner
from .storage import Outbox

_LOGGER = logging.getLogger(__name__)


class HeartbeatService:
    def __init__(
        self,
        cfg: Config,
        runner: CommandRunner,
        outbox: Outbox,
        publisher: MqttPublisher,
    ) -> None:
        self.cfg = cfg
        self.runner = runner
        self.outbox = outbox
        self.publisher = publisher

    def run_checks(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        if self.cfg.check_gateway:
            results.extend(check_gateway(self.runner, self.cfg))
        results.extend(self._run_public_dns())
        results.extend(check_broker_dns(self.runner, self.cfg))
        return results

    def _run_public_dns(self) -> list[CheckResult]:
        targets = self.cfg.ping_targets
        if not targets:
            return []
        if self.cfg.concurrent_pings and len(targets) > 1:
            with ThreadPoolExecutor(max_workers=min(len(targets), 8)) as pool:
                return list(
                    pool.map(
                        lambda t: check_public_dns_target(self.runner, self.cfg, t),
                        targets,
                    )
                )
        return [check_public_dns_target(self.runner, self.cfg, t) for t in targets]

    def write_once(self) -> None:
        """Run the checks and persist every result to the outbox. Does not
        publish — the flusher drains the outbox independently."""
        results = self.run_checks()
        for result in results:
            self.outbox.enqueue(result)
        failures = sum(1 for r in results if not r.success)
        _LOGGER.info(
            "Ran %d check(s); %d failed; queue=%d; mqtt_connected=%s",
            len(results), failures, self.outbox.count(), self.publisher.connected,
        )
        self.outbox.trim_backlog(self.cfg.max_backlog_rows)

    def flush_once(self) -> tuple[int, int]:
        """Publish one batch from the outbox. Does not run checks."""
        return self.publisher.flush(self.outbox, limit=self.cfg.flush_batch)

    def run_writer(
        self, stop: threading.Event, *, now: Callable[[], float] | None = None
    ) -> None:
        """Loop ``write_once`` on the heartbeat interval. ``now`` is injectable
        so tests can drive the clock; production uses the monotonic clock."""
        now = now or time.monotonic
        _LOGGER.info("Check loop started; interval=%ss", self.cfg.heartbeat_interval)
        next_at = now()
        while not stop.is_set():
            try:
                self.write_once()
            except Exception:  # noqa: BLE001 - one bad tick must not kill the loop
                _LOGGER.exception("Check tick failed")
            # Fixed-cadence scheduling: the sleep shrinks by however long the
            # tick took, so a slow check never pushes back the next one. A tick
            # that overruns the interval makes the next one fire immediately; the
            # max() drops missed slots so a long overrun never bursts to catch up.
            next_at = max(next_at + self.cfg.heartbeat_interval, now())
            stop.wait(max(0.0, next_at - now()))

    def run_flusher(self, stop: threading.Event) -> None:
        """Loop ``flush_once``, sleeping ``flush_interval`` between cycles."""
        _LOGGER.info("Flush loop started; interval=%ss", self.cfg.flush_interval)
        while not stop.is_set():
            try:
                self.flush_once()
            except Exception:  # noqa: BLE001 - one bad cycle must not kill the loop
                _LOGGER.exception("Flush cycle failed")
            stop.wait(self.cfg.flush_interval)
