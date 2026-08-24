"""Outbox drain ordering (the 2026-08-23 incident: the add-on came back after
an outage with a large backlog). The real ``flush`` runs against a fake MQTT
client so the newest-first (LIFO) drain order is observable directly."""

from __future__ import annotations

from heartbeat.config import MqttConfig
from heartbeat.models import CheckResult
from heartbeat.publisher import MqttPublisher
from heartbeat.storage import Outbox


class FakeInfo:
    rc = 0  # mqtt.MQTT_ERR_SUCCESS

    def wait_for_publish(self, timeout=None) -> None:
        return None

    def is_published(self) -> bool:
        return True


class FakeClient:
    """Records the order messages were published in."""

    def __init__(self) -> None:
        self.published: list[str] = []  # topics, in publish order

    def publish(self, topic, payload, qos=0, retain=False) -> FakeInfo:
        self.published.append(topic)
        return FakeInfo()


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
