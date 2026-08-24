import paho.mqtt.client as mqtt

from heartbeat.config import MqttConfig
from heartbeat.models import CheckResult
from heartbeat.publisher import MqttPublisher
from heartbeat.storage import Outbox


def _result(name="cloudflare") -> CheckResult:
    return CheckResult(
        observed_at="2026-01-01T00:00:00+00:00",
        check="ping",
        target={"type": "public_dns", "name": name, "host": "1.1.1.1"},
        success=True,
        rc=0,
        error=None,
        details={"stdout": "", "stderr": ""},
    )


def make_outbox() -> Outbox:
    ob = Outbox(":memory:")
    ob.init()
    return ob


class _FakeInfo:
    rc = 0  # mqtt.MQTT_ERR_SUCCESS

    def __init__(self, events=None, published=True):
        self._events = events
        self._published = published

    def is_published(self):
        return self._published

    def wait_for_publish(self, timeout=None):
        if self._events is not None:
            self._events.append("wait")
        return None


class _FakeClient:
    """Records publish order and hands back a _FakeInfo. ``fail_at`` is a set of
    publish indices whose confirm reports unpublished (all others confirm).
    ``events``, if given, records the interleaving of publishes and confirm-waits."""

    def __init__(self, events=None, fail_at=frozenset()):
        self._events = events
        self._fail_at = fail_at
        self.published = []  # topics, in publish order
        self.on_connect = None
        self.on_disconnect = None

    def publish(self, topic, payload, qos=0, retain=False):
        idx = len(self.published)
        self.published.append(topic)
        if self._events is not None:
            self._events.append("publish")
        return _FakeInfo(events=self._events, published=idx not in self._fail_at)


def test_mutual_tls_passes_cert_paths(monkeypatch):
    calls = {}
    monkeypatch.setattr(
        mqtt.Client, "tls_set",
        lambda self, ca_certs=None, certfile=None, keyfile=None, **kw: calls.update(
            ca=ca_certs, cert=certfile, key=keyfile
        ),
    )
    MqttPublisher(MqttConfig(
        host="x", tls=True,
        ca_cert="/ssl/AmazonRootCA1.pem",
        client_cert="/ssl/device.pem.crt",
        client_key="/ssl/private.pem.key",
    ))
    assert calls == {
        "ca": "/ssl/AmazonRootCA1.pem",
        "cert": "/ssl/device.pem.crt",
        "key": "/ssl/private.pem.key",
    }


def test_certs_enable_tls_without_explicit_flag(monkeypatch):
    count = {"n": 0}
    monkeypatch.setattr(mqtt.Client, "tls_set", lambda self, **kw: count.__setitem__("n", count["n"] + 1))
    MqttPublisher(MqttConfig(host="x", client_cert="/ssl/device.pem.crt"))  # tls flag stays False
    assert count["n"] == 1


def test_no_tls_no_cert_calls(monkeypatch):
    count = {"n": 0}
    monkeypatch.setattr(mqtt.Client, "tls_set", lambda self, **kw: count.__setitem__("n", count["n"] + 1))
    MqttPublisher(MqttConfig(host="x"))
    assert count["n"] == 0


def _capture_topic(monkeypatch, cfg):
    pub = MqttPublisher(cfg)
    pub._connected.set()
    captured = {}

    def fake_publish(topic, payload, qos, retain):
        captured["topic"] = topic
        captured["payload"] = payload
        return _FakeInfo()

    monkeypatch.setattr(pub._client, "publish", fake_publish)
    pub.publish_result('{"x":1}', "gateway", "default_gateway")
    return captured


def test_topic_default_no_prefix(monkeypatch):
    captured = _capture_topic(monkeypatch, MqttConfig(host="x", qos=0))  # topic_prefix ""
    assert captured["topic"] == "heartbeat/gateway/default_gateway"
    assert captured["payload"] == '{"x":1}'


def test_topic_with_custom_prefix(monkeypatch):
    captured = _capture_topic(monkeypatch, MqttConfig(host="x", qos=0, topic_prefix="mysite"))
    assert captured["topic"] == "mysite/heartbeat/gateway/default_gateway"


def test_topic_excludes_client_id(monkeypatch):
    # client_id must NOT appear in the topic
    captured = _capture_topic(monkeypatch, MqttConfig(host="x", qos=0, client_id="thing-1"))
    assert captured["topic"] == "heartbeat/gateway/default_gateway"


def test_flush_warns_when_not_connected(caplog):
    import logging

    ob = make_outbox()
    ob.enqueue(_result())
    pub = MqttPublisher(MqttConfig(host="broker.example", port=1883))  # not connected
    with caplog.at_level(logging.WARNING):
        published, remaining = pub.flush(ob)
    assert published == 0 and remaining == 1
    assert any("not connected" in r.getMessage() for r in caplog.records)


def test_flush_publishes_and_deletes():
    ob = make_outbox()
    for i in range(3):
        ob.enqueue(_result(name=f"t{i}"))
    pub = MqttPublisher(MqttConfig(host="x", qos=1), client=_FakeClient())
    pub._connected.set()
    n, remaining = pub.flush(ob, limit=10)
    assert n == 3
    assert remaining == 0
    assert ob.count() == 0
    assert pub._client.published[0] == "heartbeat/public_dns/t2"  # newest first (LIFO)


def test_flush_pipelines_all_publishes_before_confirming():
    """Every publish is issued before any PUBACK is awaited — the messages go
    in flight concurrently rather than one round-trip at a time."""
    ob = make_outbox()
    for i in range(3):
        ob.enqueue(_result(name=f"t{i}"))
    events = []
    pub = MqttPublisher(MqttConfig(host="x", qos=1), client=_FakeClient(events=events))
    pub._connected.set()
    pub.flush(ob, limit=10)
    assert events == ["publish", "publish", "publish", "wait", "wait", "wait"]


def test_flush_broker_down_keeps_rows():
    ob = make_outbox()
    ob.enqueue(_result())
    pub = MqttPublisher(MqttConfig(host="x"))
    # not connected
    n, remaining = pub.flush(ob)
    assert n == 0
    assert remaining == 1
    assert ob.count() == 1


def test_flush_confirms_each_independently():
    ob = make_outbox()
    for i in range(3):
        ob.enqueue(_result(name=f"t{i}"))
    # Publish order is LIFO (t2, t1, t0); the FIRST publish fails to confirm.
    # The later two must still be delivered and deleted — no stop-at-first.
    pub = MqttPublisher(MqttConfig(host="x", qos=1), client=_FakeClient(fail_at={0}))
    pub._connected.set()
    n, remaining = pub.flush(ob, limit=10)
    assert n == 2
    assert remaining == 1
    assert [r.target_name for r in ob.fetch_pending(10)] == ["t2"]  # only the failure retries
