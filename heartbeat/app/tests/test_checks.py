from heartbeat.checks import check_broker_dns, check_gateway, check_public_dns
from heartbeat.config import Config, PingTarget
from heartbeat.runner import CommandResult


def cfg(**kw) -> Config:
    return Config(**kw)


# -- gateway ---------------------------------------------------------------- #
def test_gateway_success(runner_factory):
    r = runner_factory()
    r.add("ip route", CommandResult(0, "192.168.1.1\n", ""))
    r.add("ping", CommandResult(0, "2 received", ""))
    [res] = check_gateway(r, cfg())
    assert res.success is True
    assert res.error is None
    assert res.check == "ping"
    assert res.target == {"type": "gateway", "name": "default_gateway", "host": "192.168.1.1"}


def test_gateway_no_route(runner_factory):
    r = runner_factory()
    r.add("ip route", CommandResult(0, "\n", ""))
    [res] = check_gateway(r, cfg())
    assert res.success is False
    assert res.error == "no_default_gateway_found"
    assert res.target["host"] is None


def test_gateway_invalid_value(runner_factory):
    r = runner_factory()
    r.add("ip route", CommandResult(0, "not_an_ip\n", ""))
    [res] = check_gateway(r, cfg())
    assert res.success is False
    assert res.error == "invalid_gateway_value"
    # never attempts the ping
    assert not any(c.startswith("ping") for c in r.calls)


def test_gateway_ping_failed(runner_factory):
    r = runner_factory()
    r.add("ip route", CommandResult(0, "10.0.0.1\n", ""))
    r.add("ping", CommandResult(1, "", "100% packet loss"))
    [res] = check_gateway(r, cfg())
    assert res.success is False
    assert res.error == "ping_failed"
    assert res.target["host"] == "10.0.0.1"


# -- public dns ------------------------------------------------------------- #
def test_public_dns_one_result_per_target(runner_factory):
    r = runner_factory()
    r.add("ping", CommandResult(0, "ok", ""))
    targets = (PingTarget("cf", "1.1.1.1"), PingTarget("g", "8.8.8.8"))
    results = check_public_dns(r, cfg(ping_targets=targets, concurrent_pings=False))
    assert len(results) == 2
    assert all(x.success for x in results)
    assert results[0].target == {"type": "public_dns", "name": "cf", "host": "1.1.1.1"}
    assert results[1].target["host"] == "8.8.8.8"


def test_public_dns_failure(runner_factory):
    r = runner_factory(default=CommandResult(1, "", "loss"))
    results = check_public_dns(r, cfg(ping_targets=(PingTarget("cf", "1.1.1.1"),)))
    assert results[0].error == "ping_failed"
    assert results[0].success is False


# -- broker dns ------------------------------------------------------------- #
def test_broker_dns_success(runner_factory):
    r = runner_factory()
    r.add("getent", CommandResult(0, "1.1.1.1 broker\n1.0.0.1 broker", ""))
    [res] = check_broker_dns(r, cfg(broker_dns_target="broker.example.com"))
    assert res.success is True
    assert res.check == "dns_resolution"
    assert res.error is None
    assert res.details["resolved_ips"] == ["1.1.1.1", "1.0.0.1"]
    assert res.target["type"] == "mqtt_broker"
    assert res.target["name"] == "mqtt_broker_endpoint"


def test_broker_dns_no_ips(runner_factory):
    r = runner_factory()
    r.add("getent", CommandResult(0, "no addresses here", ""))
    [res] = check_broker_dns(r, cfg(broker_dns_target="broker.example.com"))
    assert res.success is False
    assert res.error == "dns_resolution_failed"


def test_broker_dns_rc_nonzero(runner_factory):
    r = runner_factory()
    r.add("getent", CommandResult(2, "1.1.1.1", ""))
    [res] = check_broker_dns(r, cfg(broker_dns_target="broker.example.com"))
    assert res.success is False
    assert res.error == "dns_resolution_failed"


def test_broker_dns_invalid_hostname(runner_factory):
    r = runner_factory()
    [res] = check_broker_dns(r, cfg(broker_dns_target="bad host!"))
    assert res.success is False
    assert res.error == "invalid_hostname"
    assert res.rc is None
    assert res.details["resolved_ips"] == []
    assert r.calls == []  # never shelled out


def test_broker_dns_falls_back_to_mqtt_host(runner_factory):
    from heartbeat.config import MqttConfig

    r = runner_factory()
    r.add("getent", CommandResult(0, "1.2.3.4 broker", ""))
    c = cfg(broker_dns_target="", mqtt=MqttConfig(host="mybroker.local"))
    [res] = check_broker_dns(r, c)
    assert res.target["host"] == "mybroker.local"
    assert res.success is True
