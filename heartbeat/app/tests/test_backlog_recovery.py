"""Intended behaviour for recovering from a large backlog (the 2026-08-23
incident: the add-on came back after an outage with thousands of queued rows).

These drive the *real* ``MqttPublisher.flush`` and ``HeartbeatService.run_forever``.
The only things faked are the external MQTT client (paho) and the clock, both
injected through production seams — no flush or loop logic is re-implemented here.
"""

from __future__ import annotations

from heartbeat.config import Config, MqttConfig, PingTarget
from heartbeat.models import CheckResult
from heartbeat.publisher import MqttPublisher
from heartbeat.service import HeartbeatService
from heartbeat.storage import Outbox

from test_service import _full_runner  # reuse the canned full-run runner

PER_MSG = 0.22  # virtual seconds one qos=1 publish blocks on its PUBACK
INTERVAL = 60
FLUSH_BATCH = 200
BACKLOG = 500


class VirtualClock:
    def __init__(self) -> None:
        self.t = 0.0

    def advance(self, dt: float) -> None:
        self.t += dt


class FakeInfo:
    """Stand-in for paho's MQTTMessageInfo. ``wait_for_publish`` is where the
    real client blocks on the broker round-trip, so that is where the virtual
    clock advances."""

    rc = 0  # mqtt.MQTT_ERR_SUCCESS

    def __init__(self, clock: VirtualClock | None, per_msg: float) -> None:
        self._clock = clock
        self._per_msg = per_msg

    def wait_for_publish(self, timeout=None) -> None:
        if self._clock is not None:
            self._clock.advance(self._per_msg)

    def is_published(self) -> bool:
        return True


class FakeClient:
    """Minimal stand-in for paho's Client: records the order messages were
    published in and hands back a FakeInfo."""

    def __init__(self, clock: VirtualClock | None = None, per_msg: float = 0.0) -> None:
        self._clock = clock
        self._per_msg = per_msg
        self.published: list[str] = []  # topics, in publish order

    def publish(self, topic, payload, qos=0, retain=False) -> FakeInfo:
        self.published.append(topic)
        return FakeInfo(self._clock, self._per_msg)


def _outbox() -> Outbox:
    ob = Outbox(":memory:")
    ob.init()
    return ob


def _result(name: str) -> CheckResult:
    return CheckResult(
        observed_at="2026-08-23T09:00:00+00:00",
        check="ping",
        target={"type": "public_dns", "name": name, "host": "1.1.1.1"},
        success=True,
        rc=0,
        error=None,
        details={},
    )


def _svc_cfg() -> Config:
    return Config(
        heartbeat_interval=INTERVAL,
        ping_targets=(PingTarget("cf", "1.1.1.1"),),
        broker_dns_target="b.com",
        concurrent_pings=False,
        flush_batch=FLUSH_BATCH,
        mqtt=MqttConfig(host="x"),
    )


def test_flush_outbox_happens_lifo():
    """flush publishes the newest queued rows first."""
    ob = _outbox()
    for i in range(3):
        ob.enqueue(_result(f"t{i}"))  # t0 oldest ... t2 newest
    pub = MqttPublisher(MqttConfig(host="x", qos=0), client=FakeClient())
    pub._connected.set()

    pub.flush(ob, limit=10)

    assert pub._client.published == [
        "heartbeat/public_dns/t2",
        "heartbeat/public_dns/t1",
        "heartbeat/public_dns/t0",
    ]
    assert ob.count() == 0


def test_flush_long_queue_does_not_push_back_next_tick(runner_factory):
    """With a 500-row backlog, each flush burns ~200 * 0.22s of publishing, yet
    every tick still starts exactly one interval after the previous one — the
    slow flush is absorbed by a shorter sleep, not added on top of it."""
    clock = VirtualClock()
    ob = _outbox()
    for i in range(BACKLOG):
        ob.enqueue(_result(f"backlog{i}"))
    pub = MqttPublisher(MqttConfig(host="x"), client=FakeClient(clock, PER_MSG))
    pub._connected.set()
    svc = HeartbeatService(_svc_cfg(), _full_runner(runner_factory), ob, pub)

    tick_starts: list[float] = []

    class Stop:
        def is_set(self) -> bool:
            tick_starts.append(clock.t)  # recorded at the top of every iteration
            return ob.count() == 0 or len(tick_starts) > 50

        def wait(self, delay: float) -> None:
            clock.advance(delay)  # fast-forward the deadline-computed sleep

    svc.run_forever(Stop(), now=lambda: clock.t)

    periods = [b - a for a, b in zip(tick_starts, tick_starts[1:])]
    assert periods  # the loop ran several ticks to drain the backlog
    assert all(p == INTERVAL for p in periods)
