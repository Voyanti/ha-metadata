"""The three heartbeat checks, ported verbatim from the Node-RED flow.

Every error string and the result shape match the original flow exactly so a
downstream consumer sees identical data. The runner is injected, so checks are
pure and testable without touching the network.
"""

from __future__ import annotations

import logging
import re

from .config import Config, PingTarget
from .models import CheckResult, utc_now_iso
from .runner import CommandRunner

_LOGGER = logging.getLogger(__name__)

# Exact commands from the flow.
GATEWAY_CMD = "ip route show default 0.0.0.0/0 | awk '{print $3; exit}'"
DNS_CMD_TEMPLATE = "getent hosts {host} || nslookup {host}"

# Exact regexes from the flow.
GATEWAY_VALUE_RE = re.compile(r"^[0-9a-fA-F:.]+$")
HOSTNAME_RE = re.compile(r"^[a-zA-Z0-9.-]+$")
IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
IPV6_RE = re.compile(r"\b(?:[a-fA-F0-9]{0,4}:){2,}[a-fA-F0-9]{0,4}\b")


def extract_ips(stdout: str) -> list[str]:
    """Pull resolved IPv4/IPv6 addresses out of getent/nslookup output,
    preserving order and de-duplicating."""
    ips: list[str] = []
    for ip in IPV4_RE.findall(stdout) + IPV6_RE.findall(stdout):
        if ip not in ips:
            ips.append(ip)
    return ips


def _ping_cmd(cfg: Config, host: str) -> list[str]:
    return ["ping", "-c", str(cfg.ping_count), "-W", str(cfg.ping_timeout), host]


def check_gateway(runner: CommandRunner, cfg: Config) -> list[CheckResult]:
    """Discover the default gateway and ping it."""
    observed_at = utc_now_iso()
    res = runner.run(GATEWAY_CMD, timeout=cfg.cmd_timeout, shell=True)
    gateway = (res.stdout or "").strip()
    target = {"type": "gateway", "name": "default_gateway", "host": gateway or None}

    if not gateway:
        return [CheckResult(observed_at, "ping", target, False, res.rc,
                            "no_default_gateway_found",
                            {"stdout": res.stdout, "stderr": res.stderr})]
    if not GATEWAY_VALUE_RE.match(gateway):
        return [CheckResult(observed_at, "ping", target, False, res.rc,
                            "invalid_gateway_value",
                            {"stdout": res.stdout, "stderr": res.stderr})]

    ping = runner.run(_ping_cmd(cfg, gateway), timeout=cfg.cmd_timeout)
    success = ping.rc == 0
    return [CheckResult(observed_at, "ping", target, success, ping.rc,
                        None if success else "ping_failed",
                        {"stdout": ping.stdout, "stderr": ping.stderr})]


def check_public_dns_target(runner: CommandRunner, cfg: Config, target: PingTarget) -> CheckResult:
    """Ping a single public-DNS target."""
    observed_at = utc_now_iso()
    ping = runner.run(_ping_cmd(cfg, target.host), timeout=cfg.cmd_timeout)
    success = ping.rc == 0
    return CheckResult(
        observed_at, "ping",
        {"type": "public_dns", "name": target.name, "host": target.host},
        success, ping.rc,
        None if success else "ping_failed",
        {"stdout": ping.stdout, "stderr": ping.stderr},
    )


def check_public_dns(runner: CommandRunner, cfg: Config) -> list[CheckResult]:
    return [check_public_dns_target(runner, cfg, t) for t in cfg.ping_targets]


def check_broker_dns(runner: CommandRunner, cfg: Config) -> list[CheckResult]:
    """Resolve the MQTT broker hostname via getent/nslookup."""
    observed_at = utc_now_iso()
    host = cfg.effective_broker_dns_target
    target = {"type": "mqtt_broker", "name": "mqtt_broker_endpoint", "host": host}

    if not host or not HOSTNAME_RE.match(host):
        return [CheckResult(observed_at, "dns_resolution", target, False, None,
                            "invalid_hostname",
                            {"stdout": "", "stderr": "", "resolved_ips": []})]

    res = runner.run(DNS_CMD_TEMPLATE.format(host=host), timeout=cfg.cmd_timeout, shell=True)
    ips = extract_ips(res.stdout or "")
    success = res.rc == 0 and len(ips) > 0
    return [CheckResult(observed_at, "dns_resolution", target, success, res.rc,
                        None if success else "dns_resolution_failed",
                        {"stdout": res.stdout, "stderr": res.stderr, "resolved_ips": ips})]
