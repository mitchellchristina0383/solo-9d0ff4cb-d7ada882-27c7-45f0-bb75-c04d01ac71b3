"""Prefix archiving of the source job log onto published, verified backups.

Guarantees implemented here
---------------------------
Only a fully covered contiguous prefix is archived
    The named backup must be a published, freshly re-verified backup whose
    rows form a continuous id range starting exactly at the first id still
    present in the source table.  Every row of that prefix is compared
    (order-independent SHA-256 digest over id/job_id/message/created_at plus
    count and id bounds) against the backup database before anything is
    deleted.  A damaged backup or a backup that does not continue the archive
    chain is rejected and the source table is left completely untouched.

Registration and source deletion are atomic
    The ``archive_segments`` row and the ``DELETE`` of the covered rows run in
    a single ``BEGIN IMMEDIATE`` transaction.  A crash or failure before commit
    leaves neither a segment nor a missing source row; after commit both are
    visible together.  There is therefore never a window in which a log row is
    neither queryable from the source nor registered in an archive.

Later backups continue the chain
    Each segment stores its inclusive [start_id, end_id] range.  The next
    archive must start at ``previous end_id + 1``, which is also the smallest
    id remaining in the source, so any later backup can archive the next
    prefix once its rows are still present.

Reads never silently lose or repeat a row
    ``GET /logs`` is served from one pinned read transaction.  The segment
    chain and the remaining source rows are validated for contiguity, each
    touched segment is re-verified against its manifest and content hashes
    before rows are returned, and results are streamed in strict id order
    with one ``LIMIT`` across every segment and the source table.  A corrupt
    segment, a broken chain or a hole in the source raise an explicit error;
    the endpoint never returns a result set with missing rows.
"""

from __future__ import annotations

import hashlib
import re
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .backup import (
    DB_NAME,
    MANIFEST_NAME,
    BackupManager,
    BackupNotFound,
    BackupVerificationError,
    verify_backup,
)
from .db import Database

ARCHIVE_ID_RE = re.compile(r"^arc-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$")


class ArchiveError(Exception):
    """Base class for expected archiving failures."""


class ArchiveNotFound(ArchiveError):
    pass


class ArchiveConflict(ArchiveError):
    """The request is well-formed but cannot archive this backup (HTTP 409)."""


class ArchiveIntegrityError(ArchiveError):
    """The stored archive/source state itself is broken (HTTP 500)."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _new_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"arc-{stamp}-{secrets.token_hex(6)}"


def _digest_rows(rows: list[sqlite3.Row] | list[tuple[Any, ...]]) -> str:
    """Order-independent content digest of a set of job_logs rows.

    The same deterministic per-row hash is computed in the source database and
    in the backup file; XOR-folding makes the comparison insensitive to row
    order while still detecting any changed, duplicated or missing row.
    """
    digest = 0
    for row in rows:
        rid, job_id, message, created_at = row[0], row[1], row[2], row[3]
        line = f"{rid}\x1f{job_id}\x1f{message}\x1f{created_at}\n".encode("utf-8")
        digest ^= int.from_bytes(hashlib.sha256(line).digest(), "big")
    return f"{digest:064x}"


class ArchiveManager:
    def __init__(self, database: Database, backup_manager: BackupManager):
        self.database = database
        self.backups = backup_manager

    # ------------------------------------------------------------- helpers
    def _backup_db_path(self, backup_id: str) -> Path:
        return self.backups._backup_dir(backup_id) / DB_NAME

    def _backup_read_conn(self, db_path: Path) -> sqlite3.Connection:
        # The file is a published, immutable backup; open it strictly read-only
        # so archiving can never modify backup content.
        uri = f"file:{db_path.as_posix()}?mode=ro&immutable=1"
        return sqlite3.connect(uri, uri=True, timeout=10)

    def _ready_segments(self, conn: sqlite3.Connection) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM archive_segments WHERE status='ready'"
            " ORDER BY sequence ASC"
        ).fetchall()

    # ------------------------------------------------------------- writing
    def archive_by_backup(
        self,
        backup_id: str,
        *,
        before_commit: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """Archive the earliest source prefix covered by *backup_id*.

        ``before_commit`` is a test hook invoked after every check has passed
        but inside the write transaction, just before registration + deletion
        are committed.  Raising from it rolls the whole archive back.
        """
        # 1. The backup must exist, be published and still match its manifest,
        #    file hashes and SQLite integrity check.  A damaged backup is
        #    rejected before the source database is opened for writing.
        try:
            row = self.backups._verified_ready_row(backup_id)
        except BackupNotFound as exc:
            raise ArchiveNotFound(str(exc)) from exc
        except BackupVerificationError as exc:
            raise ArchiveConflict(f"backup verification failed: {exc}") from exc

        bmax = row["snapshot_max_id"]
        bcount = row["snapshot_count"]
        if bmax is None or not bcount:
            raise ArchiveConflict("backup contains no log rows and cannot archive a prefix")

        db_path = self._backup_db_path(backup_id)

        # Read-only preparation work happens outside the write transaction so
        # appends are not blocked while backup files are hashed and compared.
        with self.database.reader() as conn:
            segments = self._ready_segments(conn)
            expected_start = 1
            prev_end = 0
            if segments:
                prev_end = int(segments[-1]["end_id"])
                expected_start = prev_end + 1
            if any(seg["backup_id"] == backup_id for seg in segments):
                raise ArchiveConflict(
                    f"backup {backup_id} has already been archived"
                )
            src_min, src_max = conn.execute(
                "SELECT MIN(id), MAX(id) FROM job_logs"
            ).fetchone()
            if src_min is None:
                raise ArchiveConflict("source table has no rows left to archive")
            src_min = int(src_min)
            if src_min != expected_start:
                raise ArchiveIntegrityError(
                    f"source table has a hole: expected earliest id {expected_start},"
                    f" found {src_min}"
                )
            if bmax < src_min:
                raise ArchiveConflict(
                    "backup only covers rows already archived; it cannot extend the chain"
                )
            if bmax > int(src_max):
                raise ArchiveConflict(
                    "backup reaches past the rows present in the source table"
                )
            source_rows = conn.execute(
                "SELECT id, job_id, message, created_at FROM job_logs"
                " WHERE id BETWEEN ? AND ? ORDER BY id",
                (expected_start, bmax),
            ).fetchall()
            src_count = len(source_rows)
            src_digest = _digest_rows(source_rows)
            src_first, src_last = int(source_rows[0][0]), int(source_rows[-1][0])

        if src_first != expected_start or src_last != int(bmax):
            raise ArchiveConflict("the backup does not cover a contiguous source prefix")

        # 2. Compare the backup's contents row for row with the source prefix.
        with closing(self._backup_read_conn(db_path)) as bconn:
            brows = bconn.execute(
                "SELECT id, job_id, message, created_at FROM job_logs"
                " WHERE id BETWEEN ? AND ? ORDER BY id",
                (expected_start, bmax),
            ).fetchall()
            b_min, b_max, b_total = bconn.execute(
                "SELECT MIN(id), MAX(id), COUNT(*) FROM job_logs"
            ).fetchone()
        if b_min != expected_start or b_max != bmax or int(b_total) != int(bcount):
            raise ArchiveConflict(
                "backup snapshot bounds do not match its recorded manifest"
            )
        if len(brows) != src_count:
            raise ArchiveConflict(
                "backup contents differ from the source prefix; refusing to archive"
            )
        backup_ids = [int(r[0]) for r in brows]
        source_ids = [int(r[0]) for r in source_rows]
        if backup_ids != source_ids or _digest_rows(brows) != src_digest:
            raise ArchiveConflict(
                "backup contents differ from the source prefix; refusing to archive"
            )

        segment_id = _new_id()
        created_at = _utc_now()

        # 3. Registration and source deletion in one atomic transaction.
        with self.database.writer() as conn:
            # Re-check the invariants under the write lock: nothing archived in
            # between, and the prefix is still exactly what was verified.
            current = self._ready_segments(conn)
            if current:
                if int(current[-1]["end_id"]) != prev_end:
                    raise ArchiveConflict(
                        "another archive ran concurrently; retry the request"
                    )
            if any(seg["backup_id"] == backup_id for seg in current):
                raise ArchiveConflict(
                    f"backup {backup_id} has already been archived"
                )
            recheck = conn.execute(
                "SELECT MIN(id), MAX(id), COUNT(*) FROM job_logs"
            ).fetchone()
            if recheck[0] is None or int(recheck[0]) != expected_start:
                raise ArchiveConflict(
                    "source prefix changed between verification and archiving"
                )
            if int(recheck[1]) < int(bmax):
                raise ArchiveConflict(
                    "source no longer contains the full backup prefix"
                )
            sequence = (len(current) + 1)
            conn.execute(
                "INSERT INTO archive_segments"
                " (id, sequence, backup_id, start_id, end_id, row_count, status, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, 'ready', ?)",
                (
                    segment_id,
                    sequence,
                    backup_id,
                    expected_start,
                    int(bmax),
                    src_count,
                    created_at,
                ),
            )
            cur = conn.execute(
                "DELETE FROM job_logs WHERE id BETWEEN ? AND ?",
                (expected_start, bmax),
            )
            if cur.rowcount != src_count:
                # Defensive: the whole transaction rolls back on this exception.
                raise ArchiveIntegrityError(
                    f"deleted {cur.rowcount} rows but expected {src_count}"
                )
            if before_commit is not None:
                before_commit()

        return {
            "id": segment_id,
            "sequence": sequence,
            "backup_id": backup_id,
            "start_id": expected_start,
            "end_id": int(bmax),
            "row_count": src_count,
            "status": "ready",
            "created_at": created_at,
        }

    # ------------------------------------------------------------- reading
    def list_segments(self) -> list[dict[str, Any]]:
        with self.database.reader() as conn:
            rows = self._ready_segments(conn)
        return [self._segment_view(r) for r in rows]

    def get_segment(self, segment_id: str) -> dict[str, Any]:
        if not ARCHIVE_ID_RE.match(segment_id):
            raise ArchiveNotFound(f"archive segment {segment_id!r} not found")
        with self.database.reader() as conn:
            row = conn.execute(
                "SELECT * FROM archive_segments WHERE id=? AND status='ready'",
                (segment_id,),
            ).fetchone()
        if row is None:
            raise ArchiveNotFound(f"archive segment {segment_id!r} not found")
        return self._segment_view(row)

    @staticmethod
    def _segment_view(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "sequence": int(row["sequence"]),
            "backup_id": row["backup_id"],
            "start_id": int(row["start_id"]),
            "end_id": int(row["end_id"]),
            "row_count": int(row["row_count"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "failure_reason": row["failure_reason"],
        }

    def _verify_segment_files(
        self, segment: sqlite3.Row, db_path: Path
    ) -> sqlite3.Connection:
        """Verify a segment's backup on disk; return an open read connection."""
        backup_dir = db_path.parent
        if not (backup_dir / MANIFEST_NAME).is_file() or not db_path.is_file():
            raise ArchiveIntegrityError(
                f"archive segment {segment['id']}: backup files are missing"
            )
        try:
            manifest = verify_backup(backup_dir)
        except BackupVerificationError as exc:
            raise ArchiveIntegrityError(
                f"archive segment {segment['id']} is corrupt: {exc}"
            ) from exc
        if manifest.get("backup_id") != segment["backup_id"]:
            raise ArchiveIntegrityError(
                f"archive segment {segment['id']}: manifest backup id mismatch"
            )
        conn = self._backup_read_conn(db_path)
        try:
            count, lo, hi = conn.execute(
                "SELECT COUNT(*), MIN(id), MAX(id) FROM job_logs"
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            conn.close()
            raise ArchiveIntegrityError(
                f"archive segment {segment['id']} database is unreadable: {exc}"
            ) from exc
        if (
            int(count) != int(segment["row_count"])
            or lo != segment["start_id"]
            or hi != segment["end_id"]
        ):
            conn.close()
            raise ArchiveIntegrityError(
                f"archive segment {segment['id']} contents do not match its"
                " registered id range"
            )
        return conn

    def query_logs(
        self,
        *,
        job_id: str | None,
        after_id: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return up to *limit* logs with id > *after_id* in id order.

        The merge runs in a single pinned read transaction over the segment
        chain followed by the remaining source rows, so the result is exactly
        one continuous, deduplicated id sequence or an explicit error.
        """
        items: list[dict[str, Any]] = []
        remaining = limit

        with self.database.read_transaction() as conn:
            segments = self._ready_segments(conn)

            # Structural chain check: contiguous ranges starting at id 1.
            expected_start = 1
            for segment in segments:
                if int(segment["start_id"]) != expected_start:
                    raise ArchiveIntegrityError(
                        "archive segment chain has a gap or overlap at sequence "
                        f"{segment['sequence']}"
                    )
                expected_start = int(segment["end_id"]) + 1

            src_min, src_max = conn.execute(
                "SELECT MIN(id), MAX(id) FROM job_logs"
            ).fetchone()
            if src_min is not None and int(src_min) != expected_start:
                raise ArchiveIntegrityError(
                    f"source table begins at id {src_min} but the archive chain"
                    f" expects {expected_start}"
                )

            # 1. Segments first (they hold the lowest ids), in sequence order.
            for segment in segments:
                if remaining <= 0:
                    break
                if int(segment["end_id"]) <= after_id:
                    continue  # whole segment is behind the pagination cursor
                db_path = self._backup_db_path(segment["backup_id"])
                bconn = self._verify_segment_files(segment, db_path)
                try:
                    params: list[Any] = [after_id]
                    sql = (
                        "SELECT id, job_id, message, created_at FROM job_logs"
                        " WHERE id > ?"
                    )
                    if job_id is not None:
                        sql += " AND job_id = ?"
                        params.append(job_id)
                    sql += " ORDER BY id LIMIT ?"
                    params.append(remaining)
                    rows = bconn.execute(sql, params).fetchall()
                finally:
                    bconn.close()
                items.extend(
                    {
                        "id": int(r[0]),
                        "job_id": r[1],
                        "message": r[2],
                        "created_at": r[3],
                    }
                    for r in rows
                )
                remaining = limit - len(items)

            # 2. Then the still-live rows of the source database.
            if remaining > 0:
                params = [after_id]
                sql = (
                    "SELECT id, job_id, message, created_at FROM job_logs"
                    " WHERE id > ?"
                )
                if job_id is not None:
                    sql += " AND job_id = ?"
                    params.append(job_id)
                sql += " ORDER BY id LIMIT ?"
                params.append(remaining)
                rows = conn.execute(sql, params).fetchall()
                items.extend(dict(r) for r in rows)

        return items

    # ------------------------------------------------------------ recovery
    def startup_check(self) -> dict[str, Any]:
        """Lightweight structural check at startup.

        Segment *files* are verified lazily on first read so that a damaged
        segment never blocks the whole service from starting; queries that
        touch it then fail explicitly instead of returning missing rows.
        Structural chain problems are reported via the returned status rather
        than raised, for the same reason: the service stays up and the error
        surfaces on the affected queries.
        """
        healthy = True
        reason: str | None = None
        segment_count = 0
        next_start_id: int | None = None
        try:
            with self.database.reader() as conn:
                segments = self._ready_segments(conn)
                segment_count = len(segments)
                src_min = conn.execute(
                    "SELECT MIN(id) FROM job_logs"
                ).fetchone()[0]
            expected_start = 1
            for segment in segments:
                if int(segment["start_id"]) != expected_start:
                    raise ArchiveIntegrityError(
                        "archive segment chain is broken at sequence "
                        f"{segment['sequence']}"
                    )
                expected_start = int(segment["end_id"]) + 1
            if src_min is not None and int(src_min) != expected_start:
                raise ArchiveIntegrityError(
                    "source table is not contiguous with the archive chain"
                )
            next_start_id = expected_start
        except ArchiveIntegrityError as exc:
            healthy = False
            reason = str(exc)
        return {
            "segments": segment_count,
            "next_start_id": next_start_id,
            "healthy": healthy,
            "reason": reason,
        }
