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

    def is_published(self):
        return True

    def wait_for_publish(self, timeout=None):
        return None


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


def test_topic_format(monkeypatch):
    pub = MqttPublisher(MqttConfig(host="x", topic_prefix="hb", qos=0))
    pub._connected.set()
    captured = {}

    def fake_publish(topic, payload, qos, retain):
        captured["topic"] = topic
        captured["payload"] = payload
        return _FakeInfo()

    monkeypatch.setattr(pub._client, "publish", fake_publish)
    assert pub.publish_result('{"x":1}', "gateway", "default_gateway") is True
    assert captured["topic"] == "hb/gateway/default_gateway"
    assert captured["payload"] == '{"x":1}'


def test_flush_publishes_and_deletes(monkeypatch):
    ob = make_outbox()
    for i in range(3):
        ob.enqueue(_result(name=f"t{i}"))
    pub = MqttPublisher(MqttConfig(host="x"))
    pub._connected.set()
    published = []
    monkeypatch.setattr(
        pub, "publish_result",
        lambda payload, ttype, tname: (published.append((ttype, tname)), True)[1],
    )
    n, remaining = pub.flush(ob, limit=10)
    assert n == 3
    assert remaining == 0
    assert ob.count() == 0
    assert published[0] == ("public_dns", "t0")


def test_flush_broker_down_keeps_rows():
    ob = make_outbox()
    ob.enqueue(_result())
    pub = MqttPublisher(MqttConfig(host="x"))
    # not connected
    n, remaining = pub.flush(ob)
    assert n == 0
    assert remaining == 1
    assert ob.count() == 1


def test_flush_partial_failure_stops(monkeypatch):
    ob = make_outbox()
    for i in range(3):
        ob.enqueue(_result(name=f"t{i}"))
    pub = MqttPublisher(MqttConfig(host="x"))
    pub._connected.set()
    state = {"n": 0}

    def fake_pub(payload, ttype, tname):
        state["n"] += 1
        return state["n"] == 1  # first succeeds, second fails

    monkeypatch.setattr(pub, "publish_result", fake_pub)
    n, remaining = pub.flush(ob, limit=10)
    assert n == 1
    assert ob.count() == 2  # two rows remain queued
