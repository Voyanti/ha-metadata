from heartbeat.checks import GATEWAY_VALUE_RE, HOSTNAME_RE, extract_ips


def test_extract_ipv4_in_order():
    out = "broker has address 52.1.2.3\nbroker has address 52.1.2.4"
    assert extract_ips(out) == ["52.1.2.3", "52.1.2.4"]


def test_extract_ipv6():
    out = "broker has address 2606:4700:4700::1111"
    ips = extract_ips(out)
    assert ips, "expected at least one address parsed"
    assert any(ip.startswith("2606:4700") for ip in ips)


def test_extract_dedup():
    assert extract_ips("1.2.3.4 1.2.3.4 1.2.3.4") == ["1.2.3.4"]


def test_extract_none():
    assert extract_ips("no addresses here") == []


def test_gateway_regex_accepts_ips():
    assert GATEWAY_VALUE_RE.match("192.168.1.1")
    assert GATEWAY_VALUE_RE.match("fe80::1")


def test_gateway_regex_rejects_garbage():
    assert not GATEWAY_VALUE_RE.match("not_an_ip")
    assert not GATEWAY_VALUE_RE.match("bad; rm -rf /")


def test_hostname_regex():
    assert HOSTNAME_RE.match("broker.example.com")
    assert HOSTNAME_RE.match("xxxxxxxxxxxxxx-ats.iot.eu-north-1.amazonaws.com")
    assert not HOSTNAME_RE.match("bad host")
    assert not HOSTNAME_RE.match("a/b$c")
