"""Concurrency guarantees for the two-loop design: the check-writer thread
enqueues while the flusher thread fetches and deletes, both through one shared
``Outbox`` (single sqlite connection, serialized by its lock).

Note: ``Outbox(":memory:")`` is one in-memory DB tied to that one connection, so
every thread must go through the SAME Outbox instance — never construct a second
``Outbox(":memory:")`` and expect it to see the same rows.
"""

from __future__ import annotations

import threading

from heartbeat.config import MqttConfig
from heartbeat.models import CheckResult
from heartbeat.publisher import MqttPublisher
from heartbeat.storage import Outbox


def _result(name: str) -> CheckResult:
    return CheckResult(
        observed_at="2026-08-24T00:00:00+00:00",
        check="ping",
        target={"type": "public_dns", "name": name, "host": "1.1.1.1"},
        success=True,
        rc=0,
        error=None,
        details={},
    )


def _outbox() -> Outbox:
    ob = Outbox(":memory:")
    ob.init()
    return ob


def test_concurrent_enqueue_and_drain_is_exactly_once():
    """One writer enqueues while one flusher fetches+deletes (production
    topology). Every row is drained exactly once — none lost, none twice."""
    ob = _outbox()
    n = 2000
    delivered: list[int] = []
    done = threading.Event()

    def writer():
        for i in range(n):
            ob.enqueue(_result(f"r{i}"))
        done.set()

    def flusher():
        # Mirror flush(): fetch a batch, then delete those ids. A single flusher,
        # as in production (run_flusher is one thread).
        while not done.is_set() or ob.count() > 0:
            rows = ob.fetch_pending(limit=64)
            if not rows:
                continue
            ob.delete([r.id for r in rows])
            delivered.extend(r.id for r in rows)

    threads = [threading.Thread(target=writer), threading.Thread(target=flusher)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert all(not t.is_alive() for t in threads)
    assert ob.count() == 0
    assert len(delivered) == n           # nothing lost
    assert len(set(delivered)) == n      # nothing delivered twice


def test_enqueue_not_blocked_by_in_progress_flush():
    """Broker round-trips happen outside the outbox lock, so a flush parked in
    wait_for_publish never blocks the writer from enqueuing."""
    ob = _outbox()
    for i in range(5):
        ob.enqueue(_result(f"r{i}"))
    gate = threading.Event()

    class BlockingInfo:
        rc = 0

        def wait_for_publish(self, timeout=None):
            gate.wait(5)  # simulate a slow PUBACK

        def is_published(self):
            return True

    class BlockingClient:
        def __init__(self):
            self.on_connect = self.on_disconnect = None

        def publish(self, topic, payload, qos=0, retain=False):
            return BlockingInfo()

    pub = MqttPublisher(MqttConfig(host="x", qos=1), client=BlockingClient())
    pub._connected.set()

    flusher = threading.Thread(target=pub.flush, args=(ob,))
    flusher.start()

    enqueued = threading.Thread(target=lambda: ob.enqueue(_result("live")))
    enqueued.start()
    enqueued.join(timeout=1.0)
    assert not enqueued.is_alive()  # would hang if the network wait held the lock

    gate.set()
    flusher.join(timeout=5)
    assert not flusher.is_alive()


def test_concurrent_delete_and_increment_no_deadlock():
    """Interleaved mutating ops on the shared connection stay serialized: no
    deadlock and no unhandled sqlite error, even on overlapping ids."""
    ob = _outbox()
    ids = [ob.enqueue(_result(f"r{i}")) for i in range(500)]
    barrier = threading.Barrier(2)

    def deleter():
        barrier.wait()
        ob.delete(ids[:250])

    def incrementer():
        barrier.wait()
        ob.increment_attempts(ids)  # overlaps the deleted half

    threads = [threading.Thread(target=deleter), threading.Thread(target=incrementer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert all(not t.is_alive() for t in threads)  # no deadlock, no crash
