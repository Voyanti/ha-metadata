"""The normalized check result: one shape for every check, identical to the
Node-RED flow's payload, and the JSON published to MQTT."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class CheckResult:
    observed_at: str  # ISO8601 UTC
    check: str  # "ping" | "dns_resolution"
    target: dict  # {"type", "name", "host"}
    success: bool
    rc: int | None
    error: str | None
    details: dict  # {"stdout", "stderr"[, "resolved_ips"]}

    def to_dict(self) -> dict:
        return {
            "observed_at": self.observed_at,
            "check": self.check,
            "target": self.target,
            "success": self.success,
            "rc": self.rc,
            "error": self.error,
            "details": self.details,
        }

    def to_payload(self) -> str:
        """Compact JSON string published to MQTT and stored in the outbox."""
        return json.dumps(self.to_dict(), separators=(",", ":"))

    def to_record(self) -> dict:
        """Flatten to SQLite columns. ``check`` becomes ``check_kind`` to avoid
        the SQL reserved word, but stays ``"check"`` in the JSON payload."""
        return {
            "observed_at": self.observed_at,
            "check_kind": self.check,
            "target_type": self.target.get("type", ""),
            "target_name": self.target.get("name", ""),
            "target_host": self.target.get("host") or "",
            "success": 1 if self.success else 0,
            "rc": self.rc,
            "error": self.error,
            "payload_json": self.to_payload(),
        }
