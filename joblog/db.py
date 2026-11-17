"""SQLite connection handling for the job-log source database."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

# Every connection uses WAL so that a consistent snapshot can be read while
# writers keep appending.  synchronous=FULL makes an acknowledged commit durable
# across a crash, which is what "committed log" means for recovery purposes.
_SETUP_PRAGMAS = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=FULL",
    "PRAGMA busy_timeout=10000",
    "PRAGMA foreign_keys=ON",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS job_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT    NOT NULL,
    message     TEXT    NOT NULL,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS ix_job_logs_job_id ON job_logs(job_id, id);

-- One row per backup attempt.  A row is only ever exposed through the API
-- once status == 'ready'; rows in other states are implementation detail.
CREATE TABLE IF NOT EXISTS backups (
    id             TEXT PRIMARY KEY,
    status         TEXT NOT NULL CHECK (status IN ('staging','ready','failed')),
    root           TEXT NOT NULL,
    snapshot_count INTEGER,
    snapshot_max_id INTEGER,
    db_file        TEXT NOT NULL,
    db_sha256      TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    failure_reason TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at   TEXT
);
"""


class Database:
    def __init__(self, path: Path):
        self.path = Path(path)

    def connect(self, *, timeout: float = 10.0) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path.as_posix(),
            timeout=timeout,
            isolation_level=None,  # explicit transaction control
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        for pragma in _SETUP_PRAGMAS:
            conn.execute(pragma)
        return conn

    def connect_readonly_snapshot(self) -> sqlite3.Connection:
        """Connection used to take a backup snapshot.

        The caller must issue ``BEGIN`` and keep the transaction open for the
        duration of the online backup so the copy reflects one committed state.
        """
        conn = self.connect()
        conn.execute("PRAGMA query_only=ON")
        return conn

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    @contextmanager
    def writer(self) -> Iterator[sqlite3.Connection]:
        """Yield a connection inside a BEGIN IMMEDIATE transaction.

        Concurrent appends serialise on SQLite's write lock rather than failing,
        so a burst of appends is durable and ordered by commit.
        """
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()
