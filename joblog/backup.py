"""Consistent backup, verification and restore for the job-log database.

Guarantees implemented here
---------------------------
Consistent snapshot
    A backup runs SQLite's online-backup API against a connection holding an
    open read transaction.  That read transaction pins exactly one committed
    state of the WAL database, so concurrent appends either happen entirely
    before it (and are included) or entirely after it (and are not).

Publish only after verification
    The copy is written into a private staging directory, the source row count
    is cross-checked against the copy, ``PRAGMA integrity_check`` is run, and a
    ``manifest.json`` with SHA-256 digests is produced and itself verified from
    disk.  Only then is the directory atomically renamed to ``backups/<id>``
    and the database row flipped to ``ready``.  Only ``ready`` rows are ever
    exposed by the API.

Interruption safety
    An interrupted backup leaves a ``staging`` row and/or a staging directory.
    On startup :meth:`BackupManager.recover_incomplete` marks non-ready rows
    ``failed`` and deletes their partial files.  A ``ready`` row whose files are
    missing is downgraded to ``failed``.  A failed/partial backup can therefore
    never be returned by the API as a restorable version.

Immutability of published backups
    Published files are chmod-ed read-only and are never touched by later
    appends; later source writes cannot change them.

Verified restore to an independent target
    Restore re-verifies manifest + file hashes + integrity before touching the
    target, materialises a fresh copy in a temporary directory, verifies the
    copy again, and only then swaps it into place.  The previous target is moved
    aside first and restored on failure, so a corrupt backup or a restore error
    leaves the existing target byte-identical.  The source database is never
    opened for writing during restore and remains writable throughout.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .db import Database

BACKUP_ID_RE = re.compile(r"^bkp-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$")
STAGING_PREFIX = ".staging-"
MANIFEST_NAME = "manifest.json"
DB_NAME = "joblogs.db"
FILE_SUFFIXES = ("-wal", "-shm", "-journal")


class BackupError(Exception):
    """Base class for expected backup/restore failures."""


class BackupNotFound(BackupError):
    pass


class BackupVerificationError(BackupError):
    pass


class RestoreError(BackupError):
    pass


class TargetExistsError(RestoreError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_dir(path: Path) -> None:
    """Best-effort durability for renames inside *path*."""
    try:
        fd = os.open(path.as_posix(), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _remove_sidecars(db_path: Path) -> None:
    for suffix in FILE_SUFFIXES:
        try:
            db_path.with_name(db_path.name + suffix).unlink()
        except FileNotFoundError:
            pass


def integrity_check(db_path: Path) -> str:
    """Run SQLite's integrity check on a static database file."""
    uri = f"file:{db_path.as_posix()}?mode=ro&immutable=1"
    with closing(sqlite3.connect(uri, uri=True, timeout=10)) as conn:
        row = conn.execute("PRAGMA integrity_check").fetchone()
    return str(row[0]) if row else "unknown"


def _read_manifest(backup_dir: Path) -> dict[str, Any]:
    manifest_path = backup_dir / MANIFEST_NAME
    try:
        raw = manifest_path.read_bytes()
    except FileNotFoundError as exc:
        raise BackupVerificationError("manifest.json missing") from exc
    try:
        manifest = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupVerificationError("manifest.json is not valid JSON") from exc
    if not isinstance(manifest, dict):
        raise BackupVerificationError("manifest.json is not an object")
    return manifest


def verify_backup(backup_dir: Path) -> dict[str, Any]:
    """Verify a backup directory: file hashes, manifest, SQLite integrity."""
    manifest = _read_manifest(backup_dir)
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise BackupVerificationError("manifest has no file entries")
    seen_db = False
    for entry in files:
        if not isinstance(entry, dict):
            raise BackupVerificationError("manifest file entry malformed")
        name = entry.get("name")
        expected = entry.get("sha256")
        if (
            not isinstance(name, str)
            or "/" in name
            or "\\" in name
            or name in ("", ".", "..")
            or not isinstance(expected, str)
            or not re.fullmatch(r"[0-9a-f]{64}", expected)
        ):
            raise BackupVerificationError("manifest contains an invalid file entry")
        path = backup_dir / name
        if not path.is_file():
            raise BackupVerificationError(f"{name} missing")
        if sha256_file(path) != expected:
            raise BackupVerificationError(f"{name} checksum does not match manifest")
        if name == DB_NAME:
            seen_db = True
            result = integrity_check(path)
            if result != "ok":
                raise BackupVerificationError(
                    f"database integrity check failed: {result}"
                )
    if not seen_db:
        raise BackupVerificationError(f"manifest has no entry for {DB_NAME}")
    return manifest


@dataclass
class BackupResult:
    id: str
    status: str
    created_at: str
    completed_at: str
    sha256: dict[str, str]
    manifest_sha256: str
    snapshot_row_count: int
    snapshot_max_id: int | None


@dataclass
class RestoreResult:
    backup_id: str
    target_path: str
    verified_files: list[str]
    row_count: int
    max_id: int | None


class BackupManager:
    def __init__(self, database: Database, backups_root: Path):
        self.database = database
        self.root = Path(backups_root)
        self.staging = self.root / ".staging"

    def ensure_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.staging.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ paths
    def _backup_dir(self, backup_id: str) -> Path:
        return self.root / backup_id

    def _new_id(self) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"bkp-{stamp}-{secrets.token_hex(6)}"

    def _row(self, backup_id: str) -> sqlite3.Row | None:
        if not BACKUP_ID_RE.match(backup_id):
            return None
        with self.database.reader() as conn:
            return conn.execute(
                "SELECT * FROM backups WHERE id = ?", (backup_id,)
            ).fetchone()

    # --------------------------------------------------------------- recovery
    def recover_incomplete(self) -> dict[str, int]:
        """Make every backup that was not fully published permanently failed.

        Called at startup, before requests are served.  It is intentionally not
        safe to run concurrently with an in-flight backup in the same process.
        """
        stats = {"staging_removed": 0, "failed_marked": 0, "ready_missing": 0}
        self.ensure_dirs()

        for entry in self.staging.iterdir():
            if entry.is_dir() and entry.name.startswith(STAGING_PREFIX):
                shutil.rmtree(entry, ignore_errors=True)
                stats["staging_removed"] += 1
            elif entry.name.startswith(STAGING_PREFIX):
                entry.unlink(missing_ok=True)
                stats["staging_removed"] += 1

        with self.database.reader() as conn:
            unfinished = conn.execute(
                "SELECT id FROM backups WHERE status != 'ready'"
            ).fetchall()
        for row in unfinished:
            shutil.rmtree(self._backup_dir(row["id"]), ignore_errors=True)
            with self.database.writer() as conn:
                conn.execute(
                    "UPDATE backups SET status='failed',"
                    " failure_reason=COALESCE(failure_reason,'interrupted'),"
                    " completed_at=COALESCE(completed_at, ?)"
                    " WHERE id = ? AND status != 'ready'",
                    (utc_now(), row["id"]),
                )
            stats["failed_marked"] += 1

        with self.database.reader() as conn:
            ready = conn.execute(
                "SELECT id FROM backups WHERE status = 'ready'"
            ).fetchall()
        for row in ready:
            backup_dir = self._backup_dir(row["id"])
            if (
                not (backup_dir / MANIFEST_NAME).is_file()
                or not (backup_dir / DB_NAME).is_file()
            ):
                shutil.rmtree(backup_dir, ignore_errors=True)
                with self.database.writer() as conn:
                    conn.execute(
                        "UPDATE backups SET status='failed',"
                        " failure_reason='published files missing',"
                        " completed_at=COALESCE(completed_at, ?)"
                        " WHERE id = ? AND status = 'ready'",
                        (utc_now(), row["id"]),
                    )
                stats["ready_missing"] += 1
        return stats

    # --------------------------------------------------------------- creation
    def create(
        self,
        *,
        sleep_seconds: float = 0.0,
        after_freeze: Callable[[], None] | None = None,
    ) -> BackupResult:
        """Take a consistent snapshot and publish it after verification.

        ``after_freeze`` is an internal test hook invoked once the source read
        transaction has been established, i.e. at the moment the snapshot is
        fixed.  It has no effect on the produced backup.
        """
        backup_id = self._new_id()
        staging_dir = self.staging / f"{STAGING_PREFIX}{backup_id}"
        created_at = utc_now()
        self.ensure_dirs()

        # Reserve the row before work starts so an interruption leaves a
        # durable trace which startup recovery turns into a hard "failed".
        with self.database.writer() as conn:
            conn.execute(
                "INSERT INTO backups"
                " (id, status, root, db_file, db_sha256, manifest_sha256, created_at)"
                " VALUES (?, 'staging', ?, ?, '', '', ?)",
                (backup_id, staging_dir.as_posix(), DB_NAME, created_at),
            )

        try:
            staging_dir.mkdir(parents=True, exist_ok=False)
            snapshot_count, snapshot_max_id = self._copy_snapshot(
                staging_dir / DB_NAME,
                sleep_seconds=sleep_seconds,
                after_freeze=after_freeze,
            )
            self._finalize_snapshot_db(staging_dir / DB_NAME)

            # Cross-check that the copy is exactly the committed state that was
            # frozen before the copy started.
            with closing(sqlite3.connect(staging_dir / DB_NAME, timeout=10)) as conn:
                copied_count, copied_max = conn.execute(
                    "SELECT COUNT(*), MAX(id) FROM job_logs"
                ).fetchone()
            if int(copied_count) != snapshot_count or copied_max != snapshot_max_id:
                raise BackupVerificationError(
                    "snapshot copy does not match the frozen committed state"
                )

            result = integrity_check(staging_dir / DB_NAME)
            if result != "ok":
                raise BackupVerificationError(
                    f"snapshot integrity check failed: {result}"
                )

            db_sha = sha256_file(staging_dir / DB_NAME)
            manifest = {
                "format": "joblog-backup/1",
                "backup_id": backup_id,
                "created_at": created_at,
                "database": DB_NAME,
                "snapshot": {
                    "journal_mode": "delete",
                    "integrity_check": "ok",
                    "row_count": snapshot_count,
                    "max_id": snapshot_max_id,
                },
                "files": [
                    {
                        "name": DB_NAME,
                        "sha256": db_sha,
                        "size": (staging_dir / DB_NAME).stat().st_size,
                    }
                ],
            }
            manifest_path = staging_dir / MANIFEST_NAME
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            manifest_sha = sha256_file(manifest_path)

            # Verify exactly what will be published, reading it back from disk.
            verify_backup(staging_dir)

            completed_at = utc_now()
            final_dir = self._backup_dir(backup_id)
            if final_dir.exists():
                raise BackupError(f"backup directory already exists: {backup_id}")
            os.replace(staging_dir, final_dir)
            _fsync_dir(self.root)
            for path in final_dir.iterdir():
                try:
                    path.chmod(0o444)
                except OSError:
                    pass

            with self.database.writer() as conn:
                cur = conn.execute(
                    "UPDATE backups SET status='ready', root=?, snapshot_count=?,"
                    " snapshot_max_id=?, db_file=?, db_sha256=?, manifest_sha256=?,"
                    " completed_at=?, failure_reason=NULL"
                    " WHERE id=? AND status='staging'",
                    (
                        final_dir.as_posix(),
                        snapshot_count,
                        snapshot_max_id,
                        DB_NAME,
                        db_sha,
                        manifest_sha,
                        completed_at,
                        backup_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise BackupError(
                        "backup row was not in staging state at publish time"
                    )
        except BaseException as exc:
            shutil.rmtree(staging_dir, ignore_errors=True)
            shutil.rmtree(self._backup_dir(backup_id), ignore_errors=True)
            reason = f"{type(exc).__name__}: {exc}"[:500]
            try:
                with self.database.writer() as conn:
                    conn.execute(
                        "UPDATE backups SET status='failed', failure_reason=?,"
                        " completed_at=COALESCE(completed_at, ?)"
                        " WHERE id = ? AND status != 'ready'",
                        (reason, utc_now(), backup_id),
                    )
            except Exception:
                pass
            raise

        return BackupResult(
            id=backup_id,
            status="ready",
            created_at=created_at,
            completed_at=completed_at,
            sha256={DB_NAME: db_sha},
            manifest_sha256=manifest_sha,
            snapshot_row_count=snapshot_count,
            snapshot_max_id=snapshot_max_id,
        )

    def _copy_snapshot(
        self,
        dest_path: Path,
        *,
        sleep_seconds: float = 0.0,
        after_freeze: Callable[[], None] | None = None,
    ) -> tuple[int, int | None]:
        source = self.database.connect_readonly_snapshot()
        dest: sqlite3.Connection | None = None
        try:
            dest = sqlite3.connect(
                dest_path.as_posix(), isolation_level=None, timeout=10
            )
            # A standalone .db file must not depend on a -wal sidecar.
            dest.execute("PRAGMA journal_mode=DELETE")
            source.execute("BEGIN")
            # This SELECT starts the read transaction: the snapshot is fixed
            # here.  Every later commit on the source is invisible to it.
            row = source.execute("SELECT COUNT(*), MAX(id) FROM job_logs").fetchone()
            snapshot_count = int(row[0])
            snapshot_max_id = row[1]
            if after_freeze is not None:
                after_freeze()
            source.backup(dest, pages=256, sleep=sleep_seconds)
            source.execute("ROLLBACK")
        except BaseException:
            if source.in_transaction:
                source.execute("ROLLBACK")
            raise
        finally:
            if dest is not None:
                dest.close()
            source.close()
        return snapshot_count, snapshot_max_id

    @staticmethod
    def _finalize_snapshot_db(db_path: Path) -> None:
        with closing(
            sqlite3.connect(db_path.as_posix(), isolation_level=None, timeout=10)
        ) as conn:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        _remove_sidecars(db_path)

    # ---------------------------------------------------------------- reading
    def get(self, backup_id: str) -> dict[str, Any]:
        return self._public_view(backup_id)

    def list_ready(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self.database.reader() as conn:
            rows = conn.execute(
                "SELECT id FROM backups WHERE status='ready'"
                " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        out = []
        for row in rows:
            try:
                out.append(self._public_view(row["id"]))
            except BackupNotFound:
                continue
        return out

    def _public_view(self, backup_id: str) -> dict[str, Any]:
        row = self._row(backup_id)
        if row is None or row["status"] != "ready":
            raise BackupNotFound(f"backup {backup_id!r} not found")
        backup_dir = self._backup_dir(backup_id)
        if not (backup_dir / MANIFEST_NAME).is_file():
            raise BackupNotFound(f"backup {backup_id!r} not found")
        return {
            "id": row["id"],
            "status": row["status"],
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "sha256": {DB_NAME: row["db_sha256"]},
            "manifest_sha256": row["manifest_sha256"],
            "snapshot_row_count": row["snapshot_count"],
            "snapshot_max_id": row["snapshot_max_id"],
            "download_url": f"/backups/{row['id']}/download",
        }

    def _verified_ready_row(self, backup_id: str) -> sqlite3.Row:
        row = self._row(backup_id)
        if row is None or row["status"] != "ready":
            raise BackupNotFound(f"backup {backup_id!r} not found")
        backup_dir = self._backup_dir(backup_id)
        if not (backup_dir / MANIFEST_NAME).is_file():
            raise BackupNotFound(f"backup {backup_id!r} not found")
        manifest = verify_backup(backup_dir)
        if manifest.get("backup_id") != backup_id:
            raise BackupVerificationError("manifest backup_id mismatch")
        db_path = backup_dir / DB_NAME
        if sha256_file(db_path) != row["db_sha256"]:
            raise BackupVerificationError(
                "database hash no longer matches recorded hash"
            )
        if sha256_file(backup_dir / MANIFEST_NAME) != row["manifest_sha256"]:
            raise BackupVerificationError(
                "manifest hash no longer matches recorded hash"
            )
        return row

    def verify(self, backup_id: str) -> dict[str, Any]:
        try:
            row = self._verified_ready_row(backup_id)
        except BackupNotFound:
            raise
        return {
            "id": backup_id,
            "verified": True,
            "integrity_check": "ok",
            "sha256": row["db_sha256"],
            "manifest_sha256": row["manifest_sha256"],
            "snapshot_row_count": row["snapshot_count"],
            "snapshot_max_id": row["snapshot_max_id"],
        }

    def backup_file(self, backup_id: str) -> Path:
        """Return the database path of a verified, published backup."""
        self._verified_ready_row(backup_id)
        return self._backup_dir(backup_id) / DB_NAME

    # -------------------------------------------------------------- restoring
    def restore(
        self, backup_id: str, target_path: Path, *, overwrite: bool = False
    ) -> RestoreResult:
        """Restore a published backup into an independent target database."""
        target_path = Path(target_path)
        if not target_path.is_absolute():
            raise RestoreError("target path must be absolute")
        if target_path.exists() and target_path.is_dir():
            raise RestoreError("target path is an existing directory")
        if target_path.exists() and not overwrite:
            raise TargetExistsError(f"target already exists: {target_path}")

        row = self._verified_ready_row(backup_id)
        db_path = self._backup_dir(backup_id) / DB_NAME

        target_path.parent.mkdir(parents=True, exist_ok=True)
        work_dir = target_path.parent / (f".restore-{backup_id}-{secrets.token_hex(8)}")
        work_dir.mkdir(parents=True, exist_ok=False)
        new_db = work_dir / "restored.db"
        moved_aside: list[tuple[Path, Path]] = []

        try:
            shutil.copyfile(db_path, new_db)
            if sha256_file(new_db) != row["db_sha256"]:
                raise RestoreError("copied database hash mismatch")
            integrity = integrity_check(new_db)
            if integrity != "ok":
                raise RestoreError(
                    f"restored database integrity check failed: {integrity}"
                )

            # Normalise to a standalone rollback-journal database.
            with closing(
                sqlite3.connect(new_db.as_posix(), isolation_level=None, timeout=10)
            ) as conn:
                conn.execute("PRAGMA journal_mode=DELETE")
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            _remove_sidecars(new_db)

            # Final verification of the exact file that will become the target.
            if sha256_file(new_db) != row["db_sha256"]:
                raise RestoreError("prepared database hash mismatch")

            # Move an existing target (and any sidecars) aside first so that a
            # failure after this point can restore the original exactly.
            targets = [target_path] + [
                target_path.with_name(target_path.name + suffix)
                for suffix in FILE_SUFFIXES
            ]
            for existing in targets:
                if existing.exists():
                    aside = work_dir / f"old-{existing.name}"
                    os.replace(existing, aside)
                    moved_aside.append((aside, existing))

            os.replace(new_db, target_path)
            _fsync_dir(target_path.parent)

            # The old copy is no longer needed once the swap succeeded.
            for aside, _ in moved_aside:
                try:
                    aside.unlink(missing_ok=True)
                except OSError:
                    pass
            moved_aside = []
            _remove_sidecars(target_path)
            _fsync_dir(target_path.parent)
        except BaseException:
            # Put the previous target back, bit for bit.
            if target_path.exists() and moved_aside:
                try:
                    target_path.unlink()
                except OSError:
                    pass
            restored_old = True
            for aside, original in moved_aside:
                try:
                    os.replace(aside, original)
                except OSError:
                    restored_old = False
            if restored_old:
                _fsync_dir(target_path.parent)
            shutil.rmtree(work_dir, ignore_errors=True)
            raise
        else:
            shutil.rmtree(work_dir, ignore_errors=True)

        with closing(
            sqlite3.connect(f"file:{target_path.as_posix()}?mode=ro", uri=True)
        ) as conn:
            count, max_id = conn.execute(
                "SELECT COUNT(*), MAX(id) FROM job_logs"
            ).fetchone()
        return RestoreResult(
            backup_id=backup_id,
            target_path=target_path.as_posix(),
            verified_files=[DB_NAME, MANIFEST_NAME],
            row_count=int(count),
            max_id=max_id,
        )
