"""Backlog flush pacing and no-loss guarantees.

AWS IoT Core allows at most 100 publishes per second per connection and may
drop a connection that exceeds it. ``flush()`` paces its publishes to
``max_publish_rate`` (default 50/s), deletes a row only once its PUBACK arrives,
and leaves every other row queued for a later cycle.
"""

from __future__ import annotations

from collections import deque

from heartbeat.config import Config, MqttConfig, PingTarget
from heartbeat.models import CheckResult
from heartbeat.publisher import MqttPublisher, _Pacer
from heartbeat.service import HeartbeatService
from heartbeat.storage import Outbox

NS = 1_000_000_000


class VirtualClock:
    """Integer-nanosecond clock; ``sleep`` advances it instead of blocking."""

    def __init__(self) -> None:
        self.ns = 0
        self.sleeps: list[float] = []

    def now(self) -> int:
        return self.ns

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.ns += round(seconds * NS)


class FakeInfo:
    rc = 0  # mqtt.MQTT_ERR_SUCCESS

    def __init__(self, published: bool = True, on_wait=None) -> None:
        self._published = published
        self._on_wait = on_wait

    def wait_for_publish(self, timeout=None) -> None:
        if self._on_wait is not None:
            self._on_wait()

    def is_published(self) -> bool:
        return self._published


class FakeClient:
    """Records each publish with the virtual time it was issued at."""

    def __init__(self, clock: VirtualClock, make_info=None) -> None:
        self._clock = clock
        self._make_info = make_info or (lambda idx: FakeInfo())
        self.sent_at: list[int] = []
        self.topics: list[str] = []

    def publish(self, topic, payload, qos=0, retain=False) -> FakeInfo:
        idx = len(self.topics)
        self.topics.append(topic)
        self.sent_at.append(self._clock.now())
        return self._make_info(idx)


def _outbox(n: int) -> Outbox:
    ob = Outbox(":memory:")
    ob.init()
    for i in range(n):
        ob.enqueue(CheckResult(
            observed_at="2026-10-01T00:00:00+00:00",
            check="ping",
            target={"type": "public_dns", "name": f"t{i}", "host": "192.0.2.1"},
            success=True,
            rc=0,
            error=None,
            details={},
        ))
    return ob


def _publisher(clock: VirtualClock, rate: float = 50.0, make_info=None) -> MqttPublisher:
    cfg = MqttConfig(host="broker.example", qos=1, max_publish_rate=rate)
    pacer = _Pacer(rate, clock=clock.now, sleep=clock.sleep)
    pub = MqttPublisher(cfg, client=FakeClient(clock, make_info), pacer=pacer)
    pub._connected.set()
    return pub


def _max_in_any_second(times: list[int]) -> int:
    """Largest number of events in any half-open one-second window."""
    window: deque[int] = deque()
    best = 0
    for t in sorted(times):
        window.append(t)
        while window[0] <= t - NS:
            window.popleft()
        best = max(best, len(window))
    return best


def _attempts(ob: Outbox) -> dict[str, int]:
    rows = ob.conn.execute("SELECT target_name, attempts FROM outbox").fetchall()
    return {r["target_name"]: r["attempts"] for r in rows}


# --------------------------------------------------------------------------- #
# pacing
# --------------------------------------------------------------------------- #
def test_default_rate_is_50_per_second():
    assert MqttConfig().max_publish_rate == 50.0


def test_large_backlog_never_exceeds_cap_in_any_second():
    clock = VirtualClock()
    ob = _outbox(500)
    pub = _publisher(clock)

    published, remaining = pub.flush(ob, limit=500)

    sent = pub._client.sent_at
    assert (published, remaining) == (500, 0) and ob.count() == 0
    assert _max_in_any_second(sent) == 50
    # Sustained rate: 500 publishes take ~10 s, not one burst.
    assert 9.9 * NS <= sent[-1] - sent[0] < 10 * NS


def test_cap_holds_across_back_to_back_flush_cycles():
    """The pacer outlives one flush() call, so a cycle that starts right after
    the previous one ended cannot burst at the seam."""
    clock = VirtualClock()
    ob = _outbox(300)
    pub = _publisher(clock)

    for _ in range(3):
        pub.flush(ob, limit=100)

    assert ob.count() == 0
    assert _max_in_any_second(pub._client.sent_at) == 50


def test_configured_rate_is_honoured():
    clock = VirtualClock()
    ob = _outbox(100)
    pub = _publisher(clock, rate=20)

    pub.flush(ob, limit=100)

    assert _max_in_any_second(pub._client.sent_at) == 20


def test_pacer_does_not_delay_first_publish_or_catch_up_after_idle():
    clock = VirtualClock()
    pacer = _Pacer(50, clock=clock.now, sleep=clock.sleep)
    pacer.wait()
    assert clock.sleeps == []  # the first call goes straight through
    clock.ns += 60 * NS  # idle for a minute
    pacer.wait()
    pacer.wait()
    # Idle time is not banked as credit: the next pair is still spaced.
    assert clock.sleeps == [0.02]


# --------------------------------------------------------------------------- #
# no loss
# --------------------------------------------------------------------------- #
def test_only_acknowledged_rows_are_deleted():
    clock = VirtualClock()
    ob = _outbox(5)  # fetched newest-first: t4, t3, t2, t1, t0
    pub = _publisher(clock, make_info=lambda idx: FakeInfo(published=idx not in {1, 3}))

    published, remaining = pub.flush(ob, limit=10)

    assert (published, remaining) == (3, 2)
    assert _attempts(ob) == {"t3": 1, "t1": 1}  # no PUBACK -> still queued, retried later


def test_disconnect_while_sending_stops_and_keeps_unsent_rows():
    clock = VirtualClock()
    ob = _outbox(6)
    pub = None

    def make_info(idx):
        if idx == 2:
            pub._connected.clear()  # broker drops the connection after the 3rd send
        return FakeInfo()

    pub = _publisher(clock, make_info=make_info)

    published, remaining = pub.flush(ob, limit=10)

    assert len(pub._client.topics) == 3  # stopped sending once disconnected
    assert (published, remaining) == (0, 6)
    assert ob.count() == 6  # nothing lost; resent (at-least-once) after reconnect
    assert set(_attempts(ob).values()) == {1}


def test_disconnect_while_confirming_keeps_acked_deletes_and_queues_rest():
    clock = VirtualClock()
    ob = _outbox(5)  # publish order t4, t3, t2, t1, t0
    pub = None

    def make_info(idx):
        if idx == 2:
            return FakeInfo(published=False, on_wait=pub._connected.clear)
        return FakeInfo()

    pub = _publisher(clock, make_info=make_info)

    published, remaining = pub.flush(ob, limit=10)

    assert (published, remaining) == (2, 3)
    assert sorted(_attempts(ob)) == ["t0", "t1", "t2"]  # t4, t3 acked and deleted


def test_flusher_does_not_spin_while_disconnected():
    """With the broker down, every flush cycle is followed by the full
    flush_interval wait, so retries never spin."""
    clock = VirtualClock()
    ob = _outbox(10)
    pub = _publisher(clock)
    pub._connected.clear()
    cfg = Config(
        ping_targets=(PingTarget("cf", "192.0.2.1"),),
        mqtt=pub.cfg,
        flush_interval=15.0,
    )
    svc = HeartbeatService(cfg, runner=None, outbox=ob, publisher=pub)

    waits: list[float] = []

    class Stop:
        def is_set(self) -> bool:
            return len(waits) >= 3

        def wait(self, delay: float) -> None:
            waits.append(delay)

    svc.run_flusher(Stop())

    assert waits == [15.0, 15.0, 15.0]
    assert pub._client.topics == [] and ob.count() == 10
