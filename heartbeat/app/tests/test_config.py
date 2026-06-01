import json

from heartbeat.config import load_config

# Always point the loader at a non-existent options file unless a test supplies
# one, so a real /data/options.json on the test host can't leak in.
NO_FILE = {"HEARTBEAT_OPTIONS_FILE": "/nonexistent/options.json"}


def test_defaults():
    cfg = load_config(environ=NO_FILE)
    assert cfg.heartbeat_interval == 60
    assert cfg.check_gateway is True
    assert [t.host for t in cfg.ping_targets] == ["1.1.1.1", "8.8.8.8"]
    assert cfg.mqtt.host == ""
    assert cfg.mqtt.port == 1883
    assert cfg.mqtt.username is None


def test_options_file_overlay(tmp_path):
    opts = {
        "heartbeat_interval": 30,
        "check_gateway": False,
        "ping_targets": [{"name": "quad9", "host": "9.9.9.9"}],
        "broker_dns_target": "broker.local",
        "log_level": "debug",
        "mqtt": {
            "host": "10.0.0.5", "port": 8883, "username": "u", "password": "p",
            "tls": True, "topic_prefix": "hb",
        },
    }
    f = tmp_path / "options.json"
    f.write_text(json.dumps(opts))
    cfg = load_config(environ={"HEARTBEAT_OPTIONS_FILE": str(f)})
    assert cfg.heartbeat_interval == 30
    assert cfg.check_gateway is False
    assert [t.host for t in cfg.ping_targets] == ["9.9.9.9"]
    assert cfg.broker_dns_target == "broker.local"
    assert cfg.log_level == "debug"
    assert cfg.mqtt.host == "10.0.0.5"
    assert cfg.mqtt.port == 8883
    assert cfg.mqtt.username == "u"
    assert cfg.mqtt.password == "p"
    assert cfg.mqtt.tls is True
    assert cfg.mqtt.topic_prefix == "hb"


def test_env_overrides_options(tmp_path):
    f = tmp_path / "options.json"
    f.write_text(json.dumps({"heartbeat_interval": 30, "mqtt": {"host": "fromfile", "password": "filepw"}}))
    env = {
        "HEARTBEAT_OPTIONS_FILE": str(f),
        "HEARTBEAT_INTERVAL": "5",
        "HEARTBEAT_MQTT_HOST": "fromenv",
        "HEARTBEAT_MQTT_PORT": "1884",
    }
    cfg = load_config(environ=env)
    assert cfg.heartbeat_interval == 5
    assert cfg.mqtt.host == "fromenv"
    assert cfg.mqtt.port == 1884
    # untouched-by-env field keeps the options.json value
    assert cfg.mqtt.password == "filepw"


def test_missing_options_file_is_noop():
    cfg = load_config(environ=NO_FILE)
    assert cfg.heartbeat_interval == 60


def test_empty_creds_become_none(tmp_path):
    f = tmp_path / "options.json"
    f.write_text(json.dumps({"mqtt": {"username": "", "password": ""}}))
    cfg = load_config(environ={"HEARTBEAT_OPTIONS_FILE": str(f)})
    assert cfg.mqtt.username is None
    assert cfg.mqtt.password is None


def test_ping_targets_env_json():
    env = {**NO_FILE, "HEARTBEAT_PING_TARGETS": '[{"name":"a","host":"1.2.3.4"}]'}
    cfg = load_config(environ=env)
    assert [(t.name, t.host) for t in cfg.ping_targets] == [("a", "1.2.3.4")]


def test_ping_targets_env_csv():
    env = {**NO_FILE, "HEARTBEAT_PING_TARGETS": "a=1.2.3.4, b=5.6.7.8"}
    cfg = load_config(environ=env)
    assert [(t.name, t.host) for t in cfg.ping_targets] == [("a", "1.2.3.4"), ("b", "5.6.7.8")]


def test_ping_targets_accepts_flow_ip_key(tmp_path):
    f = tmp_path / "options.json"
    f.write_text(json.dumps({"ping_targets": [{"name": "cf", "ip": "1.1.1.1"}]}))
    cfg = load_config(environ={"HEARTBEAT_OPTIONS_FILE": str(f)})
    assert [(t.name, t.host) for t in cfg.ping_targets] == [("cf", "1.1.1.1")]


def test_bool_coercion():
    cfg = load_config(environ={**NO_FILE, "HEARTBEAT_CHECK_GATEWAY": "false"})
    assert cfg.check_gateway is False
    cfg = load_config(environ={**NO_FILE, "HEARTBEAT_CHECK_GATEWAY": "yes"})
    assert cfg.check_gateway is True


def test_mqtt_tls_certs_from_options(tmp_path):
    f = tmp_path / "options.json"
    f.write_text(json.dumps({"mqtt": {
        "tls": True,
        "ca_cert": "/ssl/AmazonRootCA1.pem",
        "client_cert": "/ssl/device.pem.crt",
        "client_key": "/ssl/private.pem.key",
    }}))
    cfg = load_config(environ={"HEARTBEAT_OPTIONS_FILE": str(f)})
    assert cfg.mqtt.tls is True
    assert cfg.mqtt.ca_cert == "/ssl/AmazonRootCA1.pem"
    assert cfg.mqtt.client_cert == "/ssl/device.pem.crt"
    assert cfg.mqtt.client_key == "/ssl/private.pem.key"


def test_mqtt_tls_certs_default_none_and_env_override():
    cfg = load_config(environ=NO_FILE)
    assert cfg.mqtt.ca_cert is None
    assert cfg.mqtt.client_cert is None
    cfg = load_config(environ={**NO_FILE, "HEARTBEAT_MQTT_CLIENT_CERT": "/ssl/c.crt",
                               "HEARTBEAT_MQTT_CA_CERT": ""})
    assert cfg.mqtt.client_cert == "/ssl/c.crt"
    assert cfg.mqtt.ca_cert is None  # empty string -> None


def test_effective_broker_dns_target():
    cfg = load_config(environ={**NO_FILE, "HEARTBEAT_MQTT_HOST": "mybroker"})
    assert cfg.effective_broker_dns_target == "mybroker"
    cfg = load_config(environ={**NO_FILE, "HEARTBEAT_MQTT_HOST": "mybroker", "HEARTBEAT_BROKER_DNS_TARGET": "other"})
    assert cfg.effective_broker_dns_target == "other"
