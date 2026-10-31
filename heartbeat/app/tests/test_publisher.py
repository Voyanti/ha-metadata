import shutil
import ssl
import subprocess

import paho.mqtt.client as mqtt
import pytest

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


def test_no_alpn_never_builds_a_context(monkeypatch):
    """Without alpn the TLS path is the original tls_set() call, unchanged."""
    calls = []
    monkeypatch.setattr(mqtt.Client, "tls_set", lambda self, **kw: calls.append(("tls_set", kw)))
    monkeypatch.setattr(
        mqtt.Client, "tls_set_context", lambda self, ctx=None: calls.append(("tls_set_context", ctx))
    )
    MqttPublisher(MqttConfig(
        host="x", port=8883, tls=True, ca_cert="/ssl/ca.pem",
        client_cert="/ssl/device.pem.crt", client_key="/ssl/private.pem.key",
    ))
    assert calls == [("tls_set", {
        "ca_certs": "/ssl/ca.pem",
        "certfile": "/ssl/device.pem.crt",
        "keyfile": "/ssl/private.pem.key",
    })]


# --------------------------------------------------------------------------- #
# ALPN (AWS IoT Core on port 443): verified with a real in-memory TLS handshake
# against synthetic certificates generated for the test.
# --------------------------------------------------------------------------- #
ALPN = "x-amzn-mqtt-ca"


def _openssl(*args, cwd):
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def pki(tmp_path):
    """A throwaway CA and one leaf cert (CN/SAN localhost) signed by it. The
    leaf serves as both the broker's and the device's certificate."""
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available to generate test certificates")
    ec = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "1"]
    _openssl(
        "req", "-x509", *ec, "-keyout", "ca.key", "-out", "ca.pem", "-subj", "/CN=test-ca",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        cwd=tmp_path,
    )
    _openssl(
        "req", "-x509", *ec, "-CA", "ca.pem", "-CAkey", "ca.key",
        "-keyout", "leaf.key", "-out", "leaf.pem", "-subj", "/CN=localhost",
        "-addext", "subjectAltName=DNS:localhost",
        "-addext", "basicConstraints=critical,CA:FALSE",
        "-addext", "keyUsage=critical,digitalSignature",
        "-addext", "extendedKeyUsage=serverAuth,clientAuth",
        cwd=tmp_path,
    )
    return {name: str(tmp_path / name) for name in ("ca.pem", "leaf.pem", "leaf.key")}


def _captured_context(monkeypatch, cfg):
    captured = []
    monkeypatch.setattr(mqtt.Client, "tls_set_context", lambda self, ctx=None: captured.append(ctx))
    monkeypatch.setattr(
        mqtt.Client, "tls_set", lambda self, **kw: pytest.fail("tls_set must not be used with alpn")
    )
    MqttPublisher(cfg)
    assert len(captured) == 1
    return captured[0]


def _handshake(client_ctx, pki, server_alpn=(ALPN,)):
    """Run a full TLS handshake over memory BIOs against a broker that requires
    a client certificate. Returns (client_sslobj, server_sslobj)."""
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(pki["leaf.pem"], pki["leaf.key"])
    server_ctx.load_verify_locations(pki["ca.pem"])
    server_ctx.verify_mode = ssl.CERT_REQUIRED
    server_ctx.set_alpn_protocols(list(server_alpn))

    c_in, c_out, s_in, s_out = (ssl.MemoryBIO() for _ in range(4))
    client = client_ctx.wrap_bio(c_in, c_out, server_hostname="localhost")
    server = server_ctx.wrap_bio(s_in, s_out, server_side=True)
    done = {"client": False, "server": False}
    for _ in range(20):
        for name, obj in (("client", client), ("server", server)):
            if not done[name]:
                try:
                    obj.do_handshake()
                    done[name] = True
                except ssl.SSLWantReadError:
                    pass
        s_in.write(c_out.read())
        c_in.write(s_out.read())
        if all(done.values()):
            return client, server
    raise AssertionError("TLS handshake did not complete")


def test_alpn_uses_context_with_protocol_and_client_cert(monkeypatch, pki):
    ctx = _captured_context(monkeypatch, MqttConfig(
        host="localhost", port=443, alpn=ALPN,
        ca_cert=pki["ca.pem"], client_cert=pki["leaf.pem"], client_key=pki["leaf.key"],
    ))
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname is True

    client, server = _handshake(ctx, pki)

    assert client.selected_alpn_protocol() == ALPN
    assert server.selected_alpn_protocol() == ALPN
    assert server.getpeercert()["subject"] == ((("commonName", "localhost"),),)  # mTLS cert presented


def test_alpn_without_ca_cert_uses_system_bundle(monkeypatch, pki):
    ctx = _captured_context(monkeypatch, MqttConfig(
        host="localhost", port=443, alpn=ALPN,
        client_cert=pki["leaf.pem"], client_key=pki["leaf.key"],
    ))
    # The synthetic CA is not in the system bundle, so verification must fail.
    with pytest.raises(ssl.SSLCertVerificationError):
        _handshake(ctx, pki)


def test_alpn_tls_insecure_skips_hostname_check_only(monkeypatch, pki):
    ctx = _captured_context(monkeypatch, MqttConfig(
        host="localhost", port=443, alpn=ALPN, tls_insecure=True,
        ca_cert=pki["ca.pem"], client_cert=pki["leaf.pem"], client_key=pki["leaf.key"],
    ))
    # Same meaning as tls_insecure_set(True) on the tls_set() path.
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_alpn_rejected_by_broker_is_not_negotiated(monkeypatch, pki):
    ctx = _captured_context(monkeypatch, MqttConfig(
        host="localhost", port=443, alpn=ALPN,
        ca_cert=pki["ca.pem"], client_cert=pki["leaf.pem"], client_key=pki["leaf.key"],
    ))
    client, _ = _handshake(ctx, pki, server_alpn=("mqtt",))
    assert client.selected_alpn_protocol() is None


def test_tls_mode_log_names_no_secrets(monkeypatch, pki, caplog):
    import logging

    monkeypatch.setattr(mqtt.Client, "tls_set_context", lambda self, ctx=None: None)
    key_pem = open(pki["leaf.key"]).read()
    with caplog.at_level(logging.INFO, logger="heartbeat.publisher"):
        MqttPublisher(MqttConfig(
            host="localhost", port=443, alpn=ALPN, password="synthetic-secret",
            ca_cert=pki["ca.pem"], client_cert=pki["leaf.pem"], client_key=pki["leaf.key"],
        ))
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "MQTT TLS: on, ALPN 'x-amzn-mqtt-ca'" in text
    assert "synthetic-secret" not in text
    assert "PRIVATE KEY" not in text and key_pem.strip().splitlines()[1] not in text
