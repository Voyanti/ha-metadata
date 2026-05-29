"""Shared test fixtures and fakes."""

from __future__ import annotations

import pytest

from heartbeat.runner import CommandResult


class FakeRunner:
    """A CommandRunner that returns canned results by substring match.

    ``add(needle, result)`` registers a rule; the first ``needle`` found in the
    command string wins. Unmatched commands return ``default``.
    """

    def __init__(self, responses=None, default: CommandResult | None = None) -> None:
        self.responses: list[tuple[str, CommandResult]] = list(responses or [])
        self.default = default if default is not None else CommandResult(0, "", "")
        self.calls: list[str] = []

    def add(self, needle: str, result: CommandResult) -> "FakeRunner":
        self.responses.append((needle, result))
        return self

    def run(self, command, *, timeout, shell=False) -> CommandResult:
        cmd = command if isinstance(command, str) else " ".join(command)
        self.calls.append(cmd)
        for needle, result in self.responses:
            if needle in cmd:
                return result
        return self.default


class FakePublisher:
    """Stand-in for MqttPublisher used in service tests."""

    def __init__(self, connected: bool = True) -> None:
        self._connected = connected
        self.published = 0

    @property
    def connected(self) -> bool:
        return self._connected

    def flush(self, outbox, limit: int = 200):
        rows = outbox.fetch_pending(limit)
        if not self._connected:
            return (0, len(rows))
        outbox.delete([r.id for r in rows])
        self.published += len(rows)
        return (len(rows), 0)

    def close(self) -> None:
        pass


@pytest.fixture
def runner_factory():
    return FakeRunner


@pytest.fixture
def fake_publisher_factory():
    return FakePublisher
