"""FastAPI service exposing append/query logs plus backup and restore."""

from __future__ import annotations

import os
import sqlite3
from contextlib import asynccontextmanager, closing
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .backup import (
    BackupError,
    BackupManager,
    BackupNotFound,
    BackupVerificationError,
    RestoreError,
    TargetExistsError,
)
from .db import Database

DEFAULT_DATA_DIR = Path(os.environ.get("JOBLOG_DATA_DIR", "/data"))


# --------------------------------------------------------------------- models
class LogAppend(BaseModel):
    job_id: str = Field(min_length=1, max_length=200)
    message: str = Field(max_length=100_000)


class LogEntry(BaseModel):
    id: int
    job_id: str
    message: str
    created_at: str


class LogList(BaseModel):
    items: list[LogEntry]
    next_after_id: int | None


class BackupView(BaseModel):
    id: str
    status: Literal["ready"]
    created_at: str
    completed_at: str | None
    sha256: dict[str, str]
    manifest_sha256: str
    snapshot_row_count: int | None
    snapshot_max_id: int | None
    download_url: str


class BackupList(BaseModel):
    items: list[BackupView]


class VerifyResult(BaseModel):
    id: str
    verified: bool
    integrity_check: str
    sha256: str
    manifest_sha256: str
    snapshot_row_count: int | None
    snapshot_max_id: int | None


class RestoreRequest(BaseModel):
    target_name: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}\.db$",
        description="New independent database file, created under the restore directory.",
    )
    overwrite: bool = Field(
        default=False,
        description="Atomically replace an existing target file (the old file is kept on failure).",
    )


class RestoreResultView(BaseModel):
    backup_id: str
    target_path: str
    verified_files: list[str]
    row_count: int
    max_id: int | None


class RestoredLogView(BaseModel):
    target_name: str
    items: list[LogEntry]
    row_count: int


class Health(BaseModel):
    status: str
    database: str
    backups_dir: str
    restores_dir: str


def create_app(
    *,
    data_dir: Path | None = None,
    db_path: Path | None = None,
    backups_dir: Path | None = None,
    restore_dir: Path | None = None,
) -> FastAPI:
    root = (
        Path(data_dir)
        if data_dir is not None
        else Path(os.environ.get("JOBLOG_DATA_DIR", DEFAULT_DATA_DIR))
    )
    db_file = (
        Path(db_path)
        if db_path is not None
        else Path(os.environ.get("JOBLOG_DB_PATH", root / "joblogs" / "source.db"))
    )
    backups = (
        Path(backups_dir)
        if backups_dir is not None
        else Path(os.environ.get("JOBLOG_BACKUPS_DIR", root / "backups"))
    )
    restores = (
        Path(restore_dir)
        if restore_dir is not None
        else Path(os.environ.get("JOBLOG_RESTORE_DIR", root / "restores"))
    )

    database = Database(db_file)
    manager = BackupManager(database, backups)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.initialize()
        manager.ensure_dirs()
        restores.mkdir(parents=True, exist_ok=True)
        manager.recover_incomplete()
        yield

    app = FastAPI(
        title="Job Log Service",
        version="1.0.0",
        description=(
            "Append-only job logs with consistent, immutable backups and verified restore.\n\n"
            "Backups are published only after the snapshot and its SHA-256 manifest have "
            "been verified. Restores target an independent database file and leave the "
            "source database writable."
        ),
        lifespan=lifespan,
    )
    app.state.database = database
    app.state.manager = manager
    app.state.restore_dir = restores
    app.state.paths = {
        "database": db_file,
        "backups": backups,
        "restores": restores,
    }

    @app.get("/health", response_model=Health, tags=["ops"])
    def health() -> Health:
        return Health(
            status="ok",
            database=db_file.as_posix(),
            backups_dir=backups.as_posix(),
            restores_dir=restores.as_posix(),
        )

    @app.post(
        "/logs",
        response_model=LogEntry,
        status_code=status.HTTP_201_CREATED,
        tags=["logs"],
        summary="Append a log entry (committed before returning)",
    )
    def append_log(entry: LogAppend) -> LogEntry:
        with database.writer() as conn:
            cur = conn.execute(
                "INSERT INTO job_logs (job_id, message) VALUES (?, ?)",
                (entry.job_id, entry.message),
            )
            row = conn.execute(
                "SELECT id, job_id, message, created_at FROM job_logs WHERE id = ?",
                (cur.lastrowid,),
            ).fetchone()
        return LogEntry(**dict(row))

    @app.get(
        "/logs", response_model=LogList, tags=["logs"], summary="Query committed logs"
    )
    def list_logs(
        job_id: str | None = Query(default=None),
        after_id: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> LogList:
        sql = "SELECT id, job_id, message, created_at FROM job_logs WHERE id > ?"
        params: list[object] = [after_id]
        if job_id is not None:
            sql += " AND job_id = ?"
            params.append(job_id)
        sql += " ORDER BY id LIMIT ?"
        params.append(limit)
        with database.reader() as conn:
            rows = conn.execute(sql, params).fetchall()
        items = [LogEntry(**dict(r)) for r in rows]
        return LogList(items=items, next_after_id=items[-1].id if items else None)

    @app.post(
        "/backups",
        response_model=BackupView,
        status_code=status.HTTP_201_CREATED,
        tags=["backups"],
        summary="Create and publish a verified backup",
        description=(
            "Snapshots one committed state, writes the database file and a SHA-256 "
            "manifest, verifies both, and only then publishes a backup id. Concurrent "
            "appends neither leak into the snapshot nor block it."
        ),
    )
    def create_backup() -> BackupView:
        try:
            result = manager.create()
        except (BackupError, sqlite3.Error, OSError) as exc:
            raise HTTPException(
                status_code=500, detail=f"backup failed: {exc}"
            ) from exc
        return BackupView(**manager.get(result.id))

    @app.get(
        "/backups",
        response_model=BackupList,
        tags=["backups"],
        summary="List published backups",
    )
    def list_backups(
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> BackupList:
        return BackupList(
            items=[
                BackupView(**v) for v in manager.list_ready(limit=limit, offset=offset)
            ]
        )

    @app.get(
        "/backups/{backup_id}",
        response_model=BackupView,
        tags=["backups"],
        summary="Get a published backup",
    )
    def get_backup(backup_id: str) -> BackupView:
        try:
            return BackupView(**manager.get(backup_id))
        except BackupNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get(
        "/backups/{backup_id}/verify",
        response_model=VerifyResult,
        tags=["backups"],
        summary="Re-verify a published backup against its manifest",
    )
    def verify_backup_endpoint(backup_id: str) -> VerifyResult:
        try:
            return VerifyResult(**manager.verify(backup_id))
        except BackupNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except BackupVerificationError as exc:
            raise HTTPException(
                status_code=409, detail=f"verification failed: {exc}"
            ) from exc

    @app.get(
        "/backups/{backup_id}/download",
        tags=["backups"],
        summary="Download the verified database file of a published backup",
        response_class=FileResponse,
    )
    def download_backup(backup_id: str) -> FileResponse:
        try:
            path = manager.backup_file(backup_id)
        except BackupNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except BackupVerificationError as exc:
            raise HTTPException(
                status_code=409, detail=f"verification failed: {exc}"
            ) from exc
        return FileResponse(
            path, media_type="application/vnd.sqlite3", filename=f"{backup_id}.db"
        )

    @app.post(
        "/backups/{backup_id}/restore",
        response_model=RestoreResultView,
        tags=["backups"],
        summary="Restore a backup into an independent target database",
        description=(
            "Verifies manifest, SHA-256 and SQLite integrity before touching the target. "
            "The target is swapped in atomically only after the restored copy verifies; "
            "on any failure the existing target is preserved and the source stays writable."
        ),
    )
    def restore_backup(backup_id: str, request: RestoreRequest) -> RestoreResultView:
        target = restores / request.target_name
        try:
            result = manager.restore(backup_id, target, overwrite=request.overwrite)
        except BackupNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TargetExistsError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except BackupVerificationError as exc:
            raise HTTPException(
                status_code=409,
                detail=f"backup verification failed, target untouched: {exc}",
            ) from exc
        except RestoreError as exc:
            raise HTTPException(
                status_code=422, detail=f"restore failed, target untouched: {exc}"
            ) from exc
        return RestoreResultView(
            backup_id=result.backup_id,
            target_path=result.target_path,
            verified_files=result.verified_files,
            row_count=result.row_count,
            max_id=result.max_id,
        )

    @app.get(
        "/restores/{target_name}/logs",
        response_model=RestoredLogView,
        tags=["restore"],
        summary="Query logs from a restored target database (read-only)",
    )
    def read_restored_logs(
        target_name: str,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> RestoredLogView:
        if "/" in target_name or "\\" in target_name or target_name in ("", ".", ".."):
            raise HTTPException(status_code=400, detail="invalid target name")
        target = restores / target_name
        if not target.is_file():
            raise HTTPException(status_code=404, detail="restored target not found")
        uri = f"file:{target.as_posix()}?mode=ro"
        try:
            with closing(sqlite3.connect(uri, uri=True, timeout=10)) as conn:
                total = conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0]
                rows = conn.execute(
                    "SELECT id, job_id, message, created_at FROM job_logs"
                    " ORDER BY id LIMIT ?",
                    (limit,),
                ).fetchall()
        except sqlite3.DatabaseError as exc:
            raise HTTPException(
                status_code=422, detail=f"target database unreadable: {exc}"
            ) from exc
        return RestoredLogView(
            target_name=target_name,
            items=[
                LogEntry(id=r[0], job_id=r[1], message=r[2], created_at=r[3])
                for r in rows
            ],
            row_count=int(total),
        )

    @app.get("/", include_in_schema=False)
    def root() -> dict[str, str]:
        return {"service": "job-log", "docs": "/docs"}

    return app


app = create_app()
