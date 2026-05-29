"""The single shell-out chokepoint. Wrapping every subprocess call here keeps
the check logic pure and trivially mockable in tests."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class CommandResult:
    rc: int | None  # None when the command timed out or could not be launched
    stdout: str
    stderr: str


class CommandRunner(Protocol):
    def run(
        self,
        command: Sequence[str] | str,
        *,
        timeout: float,
        shell: bool = False,
    ) -> CommandResult: ...


class SubprocessRunner:
    """Runs a command via :mod:`subprocess`, capturing rc/stdout/stderr.

    Use a list with ``shell=False`` for simple commands (``ping``), or a string
    with ``shell=True`` for the flow's pipelines (``ip ... | awk ...`` and
    ``getent ... || nslookup ...``). Values interpolated into the shell strings
    are regex-validated by the callers before they get here.
    """

    def run(self, command, *, timeout, shell=False) -> CommandResult:
        try:
            proc = subprocess.run(
                command,
                shell=shell,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            return CommandResult(rc=None, stdout=exc.stdout or "", stderr="timeout")
        except (OSError, ValueError) as exc:
            return CommandResult(rc=None, stdout="", stderr=str(exc))
        return CommandResult(
            rc=proc.returncode,
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
        )
