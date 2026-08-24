import threading
import time

from heartbeat.config import Config, MqttConfig, PingTarget
from heartbeat.runner import CommandResult
from heartbeat.service import HeartbeatService
from heartbeat.storage import Outbox


def _full_runner(runner_factory):
    r = runner_factory()
    r.add("ip route", CommandResult(0, "192.168.0.1", ""))
    r.add("ping", CommandResult(0, "ok", ""))
    r.add("getent", CommandResult(0, "1.1.1.1 broker", ""))
    return r


def _cfg(**kw) -> Config:
    base = dict(
        ping_targets=(PingTarget("cf", "1.1.1.1"),),
        broker_dns_target="b.com",
        concurrent_pings=False,
        mqtt=MqttConfig(host="x"),
    )
    base.update(kw)
    return Config(**base)


def make_outbox() -> Outbox:
    ob = Outbox(":memory:")
    ob.init()
    return ob


def test_write_then_flush(runner_factory, fake_publisher_factory):
    r = _full_runner(runner_factory)
    ob = make_outbox()
    pub = fake_publisher_factory(connected=True)
    svc = HeartbeatService(_cfg(), r, ob, pub)
    svc.write_once()
    svc.flush_once()
    # gateway + 1 public-dns ping + broker dns = 3 results, all published
    assert pub.published == 3
    assert ob.count() == 0


def test_results_persist_when_broker_down(runner_factory, fake_publisher_factory):
    r = _full_runner(runner_factory)
    ob = make_outbox()
    pub = fake_publisher_factory(connected=False)
    svc = HeartbeatService(_cfg(), r, ob, pub)
    svc.write_once()
    svc.flush_once()
    assert pub.published == 0
    assert ob.count() == 3  # nothing lost; queued for next cycle


def test_check_gateway_disabled(runner_factory, fake_publisher_factory):
    r = _full_runner(runner_factory)
    ob = make_outbox()
    pub = fake_publisher_factory(connected=True)
    svc = HeartbeatService(_cfg(check_gateway=False), r, ob, pub)
    results = svc.run_checks()
    # 1 public-dns + 1 broker dns, no gateway
    assert len(results) == 2
    assert not any(x.target["type"] == "gateway" for x in results)


def test_run_writer_exits_on_stop(runner_factory, fake_publisher_factory):
    ob = make_outbox()
    pub = fake_publisher_factory(connected=True)
    svc = HeartbeatService(
        _cfg(heartbeat_interval=0.02), _full_runner(runner_factory), ob, pub
    )
    stop = threading.Event()
    t = threading.Thread(target=svc.run_writer, args=(stop,))
    t.start()
    time.sleep(0.1)
    stop.set()
    t.join(timeout=2)
    assert not t.is_alive()
    assert ob.count() > 0  # it wrote at least once


def test_writer_exception_does_not_kill_loop(fake_publisher_factory):
    class BoomRunner:
        def run(self, *a, **k):
            raise RuntimeError("boom")

    ob = make_outbox()
    pub = fake_publisher_factory(connected=True)
    svc = HeartbeatService(_cfg(heartbeat_interval=0.02), BoomRunner(), ob, pub)
    stop = threading.Event()
    t = threading.Thread(target=svc.run_writer, args=(stop,))
    t.start()
    time.sleep(0.1)
    stop.set()
    t.join(timeout=2)
    assert not t.is_alive()  # survived the exceptions
