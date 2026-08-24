"""Intended behaviour for decoupling the check writer from the MQTT flusher.

These tests pin down the upcoming change: a configurable paho in-flight window,
a flush that confirms every message independently (no stop-at-first-failure),
a separate ``flush_interval`` config, and split ``write_once``/``flush_once``
loops on HeartbeatService. They are expected to fail until that lands.
"""

from __future__ import annotations

import json

import paho.mqtt.client as mqtt

from heartbeat.config import Config, MqttConfig, PingTarget, load_config
from heartbeat.models import CheckResult
from heartbeat.publisher import MqttPublisher
from heartbeat.service import HeartbeatService
from heartbeat.storage import Outbox

from test_service import _full_runner  # reuse the canned full-run runner

NO_FILE = {"HEARTBEAT_OPTIONS_FILE": "/nonexistent/options.json"}


def _outbox() -> Outbox:
    ob = Outbox(":memory:")
    ob.init()
    return ob


def _result(name: str) -> CheckResult:
    return CheckResult(
        observed_at="2026-08-24T09:00:00+00:00",
        check="ping",
        target={"type": "public_dns", "name": name, "host": "1.1.1.1"},
        success=True,
        rc=0,
        error=None,
        details={},
    )


def _cfg(**kw) -> Config:
    base = dict(
        ping_targets=(PingTarget("cf", "1.1.1.1"),),
        broker_dns_target="b.com",
        concurrent_pings=False,
        mqtt=MqttConfig(host="x"),
    )
    base.update(kw)
    return Config(**base)


# --------------------------------------------------------------------------- #
# A. configurable MQTT in-flight window
# --------------------------------------------------------------------------- #
def test_mqtt_max_inflight_default_and_overrides(tmp_path):
    assert load_config(environ=NO_FILE).mqtt.max_inflight == 100

    f = tmp_path / "options.json"
    f.write_text(json.dumps({"mqtt": {"max_inflight": 50}}))
    assert load_config(environ={"HEARTBEAT_OPTIONS_FILE": str(f)}).mqtt.max_inflight == 50

    cfg = load_config(environ={**NO_FILE, "HEARTBEAT_MQTT_MAX_INFLIGHT": "25"})
    assert cfg.mqtt.max_inflight == 25


def test_real_client_gets_configured_max_inflight(monkeypatch):
    calls = []
    monkeypatch.setattr(
        mqtt.Client, "max_inflight_messages_set", lambda self, n: calls.append(n)
    )
    MqttPublisher(MqttConfig(host="x", max_inflight=42))
    assert calls == [42]


# --------------------------------------------------------------------------- #
# B. flush confirms each message independently
# --------------------------------------------------------------------------- #
class _FakeInfo:
    rc = 0  # mqtt.MQTT_ERR_SUCCESS

    def __init__(self, published: bool) -> None:
        self._published = published

    def wait_for_publish(self, timeout=None) -> None:
        return None

    def is_published(self) -> bool:
        return self._published


class _FakeClient:
    """Records publish order; the confirm of the publish at each index in
    ``fail_at`` reports unpublished, all others confirm."""

    def __init__(self, fail_at: set[int] = frozenset()) -> None:
        self._fail_at = fail_at
        self.published: list[str] = []  # topics, in publish order

    def publish(self, topic, payload, qos=0, retain=False) -> _FakeInfo:
        idx = len(self.published)
        self.published.append(topic)
        return _FakeInfo(published=idx not in self._fail_at)


def test_flush_middle_failure_still_deletes_later_confirmed_rows():
    ob = _outbox()
    for i in range(3):
        ob.enqueue(_result(f"t{i}"))  # t0 oldest ... t2 newest
    # LIFO fetch: publish order is t2, t1, t0 — index 1 (t1) fails to confirm.
    pub = MqttPublisher(MqttConfig(host="x", qos=1), client=_FakeClient(fail_at={1}))
    pub._connected.set()

    published, remaining = pub.flush(ob, limit=10)

    assert published == 2
    assert remaining == 1
    assert ob.count() == 1
    leftover = ob.fetch_pending(10)
    assert [r.target_name for r in leftover] == ["t1"]  # only the middle one retries


# --------------------------------------------------------------------------- #
# C. flush_interval config
# --------------------------------------------------------------------------- #
def test_flush_interval_default_and_env_override():
    assert load_config(environ=NO_FILE).flush_interval == 15.0
    cfg = load_config(environ={**NO_FILE, "HEARTBEAT_FLUSH_INTERVAL": "2.5"})
    assert cfg.flush_interval == 2.5


# --------------------------------------------------------------------------- #
# D. decoupled writer and flusher loops
# --------------------------------------------------------------------------- #
def test_write_once_enqueues_without_publishing(runner_factory, fake_publisher_factory):
    ob = _outbox()
    pub = fake_publisher_factory(connected=True)
    svc = HeartbeatService(_cfg(), _full_runner(runner_factory), ob, pub)

    svc.write_once()

    assert ob.count() > 0  # results queued
    assert pub.published == 0  # but nothing flushed — writing is decoupled


def test_flush_once_publishes_without_running_checks(fake_publisher_factory):
    class NoRunRunner:
        def run(self, *a, **k):
            raise AssertionError("flush_once must not run checks")

    ob = _outbox()
    for i in range(3):
        ob.enqueue(_result(f"t{i}"))
    pub = fake_publisher_factory(connected=True)
    svc = HeartbeatService(_cfg(), NoRunRunner(), ob, pub)

    svc.flush_once()

    assert pub.published == 3
    assert ob.count() == 0


class VirtualClock:
    def __init__(self) -> None:
        self.t = 0.0

    def advance(self, dt: float) -> None:
        self.t += dt


def test_run_writer_fixed_cadence_independent_of_backlog(fake_publisher_factory):
    """Even with a 500-row backlog and slow checks, the writer ticks exactly on
    the interval — it never flushes and the slow tick shrinks the sleep."""
    INTERVAL = 60
    clock = VirtualClock()

    class SlowRunner:
        """Every check command consumes virtual time."""

        def run(self, command, *, timeout, shell=False):
            clock.advance(7.0)
            from heartbeat.runner import CommandResult

            return CommandResult(0, "192.168.0.1", "")

    ob = _outbox()
    for i in range(500):
        ob.enqueue(_result(f"backlog{i}"))
    pub = fake_publisher_factory(connected=True)
    svc = HeartbeatService(
        _cfg(heartbeat_interval=INTERVAL), SlowRunner(), ob, pub
    )

    tick_starts: list[float] = []

    class Stop:
        def is_set(self) -> bool:
            tick_starts.append(clock.t)  # recorded at the top of every iteration
            return len(tick_starts) > 5

        def wait(self, delay: float) -> None:
            clock.advance(delay)  # fast-forward the deadline-computed sleep

    svc.run_writer(Stop(), now=lambda: clock.t)

    periods = [b - a for a, b in zip(tick_starts, tick_starts[1:])]
    assert periods
    assert all(p == INTERVAL for p in periods)
    assert pub.published == 0  # the writer never flushed the backlog
