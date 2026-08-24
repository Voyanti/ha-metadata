"""SQLite store-and-forward outbox.

Every check result is enqueued here first, then flushed to MQTT. Rows are
*deleted* once successfully published (no history is kept). A backlog cap drops
the oldest rows if the broker stays unreachable long enough to exceed it, so a
prolonged outage can never grow the database without bound.

The connection is owned by the service loop thread; the paho network thread
never touches SQLite.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .models import CheckResult

_LOGGER = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS outbox (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at  TEXT    NOT NULL,
    check_kind   TEXT    NOT NULL,
    target_type  TEXT    NOT NULL,
    target_name  TEXT    NOT NULL,
    target_host  TEXT    NOT NULL,
    success      INTEGER NOT NULL,
    rc           INTEGER,
    error        TEXT,
    payload_json TEXT    NOT NULL,
    created_at   TEXT    NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_outbox_id ON outbox(id);
"""


@dataclass(frozen=True)
class PendingRow:
    id: int
    payload_json: str
    target_type: str
    target_name: str


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Outbox:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def init(self) -> None:
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None -> autocommit; fine for a single writer.
        # check_same_thread=False lets the service loop run on a thread other
        # than the one that called init(). Access is still single-threaded by
        # discipline: only the loop thread touches the outbox (the public-DNS
        # thread pool only returns results, and paho's network thread never
        # touches SQLite), so this stays safe.
        self._conn = sqlite3.connect(
            self.db_path, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_DDL)

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Outbox not initialised; call init() first")
        return self._conn

    def enqueue(self, result: CheckResult) -> int:
        rec = result.to_record()
        cur = self.conn.execute(
            """INSERT INTO outbox
               (observed_at, check_kind, target_type, target_name, target_host,
                success, rc, error, payload_json, created_at, attempts)
               VALUES (:observed_at, :check_kind, :target_type, :target_name,
                       :target_host, :success, :rc, :error, :payload_json,
                       :created_at, 0)""",
            {**rec, "created_at": _now()},
        )
        return int(cur.lastrowid)

    def fetch_pending(self, limit: int = 200) -> list[PendingRow]:
        """Return up to ``limit`` rows newest-first (LIFO). After an outage the
        most recent checks publish before the older backlog, so live status
        recovers first. Delivery order is not the source of truth: every payload
        carries ``observed_at``, so consumers order by real observation time."""
        rows = self.conn.execute(
            "SELECT id, payload_json, target_type, target_name "
            "FROM outbox ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            PendingRow(r["id"], r["payload_json"], r["target_type"], r["target_name"])
            for r in rows
        ]

    def delete(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        self.conn.execute(f"DELETE FROM outbox WHERE id IN ({placeholders})", tuple(ids))

    def increment_attempts(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        self.conn.execute(
            f"UPDATE outbox SET attempts = attempts + 1 WHERE id IN ({placeholders})",
            tuple(ids),
        )

    def count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

    def trim_backlog(self, max_rows: int) -> int:
        """Drop the oldest rows beyond ``max_rows``. Returns the number dropped
        and logs loudly — truncation is never silent."""
        if max_rows <= 0:
            return 0
        total = self.count()
        if total <= max_rows:
            return 0
        to_drop = total - max_rows
        self.conn.execute(
            "DELETE FROM outbox WHERE id IN "
            "(SELECT id FROM outbox ORDER BY id LIMIT ?)",
            (to_drop,),
        )
        _LOGGER.warning(
            "Outbox backlog %d exceeded cap %d; dropped %d oldest row(s)",
            total, max_rows, to_drop,
        )
        return to_drop

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
