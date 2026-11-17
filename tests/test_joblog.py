import hashlib
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from joblog.main import create_app


@pytest.fixture()
def env(tmp_path: Path):
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    with client:
        yield {
            "app": app,
            "client": client,
            "root": tmp_path,
            "db": app.state.database,
            "manager": app.state.manager,
            "restores": app.state.restore_dir,
        }


def append(client: TestClient, job_id: str, message: str) -> dict:
    response = client.post("/logs", json={"job_id": job_id, "message": message})
    assert response.status_code == 201, response.text
    return response.json()


def make_backup(client: TestClient) -> dict:
    response = client.post("/backups")
    assert response.status_code == 201, response.text
    return response.json()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_append_and_query(env):
    client = env["client"]
    first = append(client, "job-a", "first")
    second = append(client, "job-b", "second")
    assert second["id"] > first["id"]

    page = client.get("/logs", params={"job_id": "job-a"}).json()
    assert [item["message"] for item in page["items"]] == ["first"]

    after = client.get("/logs", params={"after_id": second["id"]}).json()
    assert after["items"] == []


def test_backup_publishes_verified_manifest_and_download(env):
    client = env["client"]
    append(client, "job-a", "before backup")
    backup = make_backup(client)

    backup_dir = env["root"] / "backups" / backup["id"]
    manifest = backup_dir / "manifest.json"
    database = backup_dir / "joblogs.db"
    assert manifest.is_file() and database.is_file()
    assert sha256(database) == backup["sha256"]["joblogs.db"]
    assert sha256(manifest) == backup["manifest_sha256"]
    # Published files must be read-only.
    assert database.stat().st_mode & 0o222 == 0

    verify = client.get(f"/backups/{backup['id']}/verify").json()
    assert verify["verified"] is True
    assert verify["snapshot_max_id"] == 1

    download = client.get(f"/backups/{backup['id']}/download")
    assert download.status_code == 200
    assert (
        hashlib.sha256(download.content).hexdigest() == backup["sha256"]["joblogs.db"]
    )

    listing = client.get("/backups").json()["items"]
    assert [item["id"] for item in listing] == [backup["id"]]


def test_snapshot_is_frozen_against_concurrent_appends(env):
    """Appends committed after the snapshot point must not enter the backup."""
    client = env["client"]
    manager = env["manager"]
    db = env["db"]
    for i in range(20):
        append(client, "job-a", f"pre-{i}")

    def write_after_freeze():
        # Runs synchronously inside create() after BEGIN fixed the snapshot.
        with db.writer() as conn:
            conn.execute(
                "INSERT INTO job_logs(job_id, message) VALUES ('job-a', 'during-backup')"
            )

    result = manager.create(after_freeze=write_after_freeze)
    assert result.snapshot_row_count == 20
    assert result.snapshot_max_id == 20

    append(client, "job-a", "after-backup")
    assert client.get("/logs").json()["next_after_id"] == 22

    restored = client.post(
        f"/backups/{result.id}/restore", json={"target_name": "frozen.db"}
    )
    assert restored.status_code == 200, restored.text
    assert restored.json()["row_count"] == 20
    logs = client.get("/restores/frozen.db/logs").json()["items"]
    assert len(logs) == 20
    assert all(item["id"] <= 20 for item in logs)
    assert "during-backup" not in {item["message"] for item in logs}
    assert "after-backup" not in {item["message"] for item in logs}


def test_concurrent_appends_do_not_corrupt_backup(env):
    client = env["client"]
    manager = env["manager"]
    db = env["db"]
    append(client, "job-a", "seed")

    stop = threading.Event()
    errors: list[BaseException] = []

    def writer(n: int):
        try:
            while not stop.is_set():
                with db.writer() as conn:
                    conn.execute(
                        "INSERT INTO job_logs(job_id, message) VALUES (?, ?)",
                        ("load", f"w{n}"),
                    )
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    try:
        result = manager.create(sleep_seconds=0.001)
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert not errors, errors
    assert manager.verify(result.id)["verified"] is True

    with sqlite3.connect(env["root"] / "backups" / result.id / "joblogs.db") as conn:
        rows = conn.execute("SELECT COUNT(*), MAX(id) FROM job_logs").fetchone()
    assert rows[0] == result.snapshot_row_count
    assert rows[1] == result.snapshot_max_id
    # Source is still writable after the backup.
    append(client, "job-a", "still-writable")


def test_published_backup_does_not_change_with_source_writes(env):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "v1")
    backup = make_backup(client)
    db_path = env["root"] / "backups" / backup["id"] / "joblogs.db"
    before = sha256(db_path)

    for i in range(5):
        append(client, "job-a", f"later-{i}")
    assert manager.verify(backup["id"])["verified"] is True
    assert sha256(db_path) == before

    client.post(f"/backups/{backup['id']}/restore", json={"target_name": "old.db"})
    logs = client.get("/restores/old.db/logs").json()["items"]
    assert [item["message"] for item in logs] == ["v1"]


def test_restore_is_independent_and_keeps_source_writable(env):
    client = env["client"]
    append(client, "job-a", "committed-before")
    backup = make_backup(client)
    append(client, "job-a", "committed-after")

    result = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "target.db"}
    )
    assert result.status_code == 200, result.text
    body = result.json()
    assert body["row_count"] == 1
    assert body["max_id"] == 1

    # Append after restore: source and restored target must stay independent.
    append(client, "job-a", "source-only")
    target = env["restores"] / "target.db"
    with sqlite3.connect(target) as conn:
        assert conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0] == 1
        conn.execute(
            "INSERT INTO job_logs(job_id, message) VALUES ('x', 'target-only')"
        )
        conn.commit()
    source = client.get("/logs").json()["items"]
    assert "target-only" not in {item["message"] for item in source}
    assert client.get("/restores/target.db/logs").json()["row_count"] == 2

    # Existing target is protected unless overwrite is requested.
    conflict = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "target.db"}
    )
    assert conflict.status_code == 409
    assert client.get("/restores/target.db/logs").json()["row_count"] == 2


def test_corrupt_backup_cannot_be_restored_and_target_is_preserved(env):
    client = env["client"]
    append(client, "job-a", "good")
    backup = make_backup(client)
    target = env["restores"] / "keep.db"
    target.write_bytes(b"original-target-bytes")
    original = sha256(target)

    db_path = env["root"] / "backups" / backup["id"] / "joblogs.db"
    db_path.chmod(0o644)
    db_path.write_bytes(db_path.read_bytes()[:4096])  # truncate -> corrupt

    verify = client.get(f"/backups/{backup['id']}/verify")
    assert verify.status_code == 409

    restore = client.post(
        f"/backups/{backup['id']}/restore",
        json={"target_name": "keep.db", "overwrite": True},
    )
    assert restore.status_code == 409, restore.text
    assert sha256(target) == original
    # Source keeps accepting writes.
    append(client, "job-a", "after-corruption")


def test_interrupted_backup_is_never_restorable(env):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "seed")
    backup_id = manager._new_id()
    staging = manager.staging / f".staging-{backup_id}"
    staging.mkdir()
    (staging / "joblogs.db").write_bytes(b"partial")
    with env["db"].writer() as conn:
        conn.execute(
            "INSERT INTO backups (id, status, root, db_file, db_sha256, manifest_sha256)"
            " VALUES (?, 'staging', ?, 'joblogs.db', '', '')",
            (backup_id, staging.as_posix()),
        )

    # Simulated restart.
    stats = manager.recover_incomplete()
    assert stats["failed_marked"] >= 1
    assert not staging.exists()
    assert client.get(f"/backups/{backup_id}").status_code == 404
    assert client.get("/backups").json()["items"] == []
    restore = client.post(
        f"/backups/{backup_id}/restore", json={"target_name": "nope.db"}
    )
    assert restore.status_code == 404

    with env["db"].reader() as conn:
        row = conn.execute(
            "SELECT status FROM backups WHERE id=?", (backup_id,)
        ).fetchone()
    assert row["status"] == "failed"
    append(client, "job-a", "source-still-writable")


def test_ready_row_with_missing_files_is_demoted(env):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "seed")
    backup = make_backup(client)
    backup_dir = env["root"] / "backups" / backup["id"]
    for file in backup_dir.iterdir():
        file.chmod(0o644)
    (backup_dir / "manifest.json").unlink()

    manager.recover_incomplete()
    assert client.get(f"/backups/{backup['id']}").status_code == 404
    assert client.get("/backups").json()["items"] == []
    append(client, "job-a", "writable-after-recovery")


def test_backup_failure_leaves_no_published_version(env, monkeypatch):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "seed")

    def explode(path):
        raise OSError("disk full")

    monkeypatch.setattr(manager, "_finalize_snapshot_db", explode)
    response = client.post("/backups")
    assert response.status_code == 500
    assert client.get("/backups").json()["items"] == []
    assert list(manager.staging.iterdir()) == []
    append(client, "job-a", "still-writable")

    # After the fault is cleared a new backup succeeds and restores cleanly.
    monkeypatch.undo()
    backup = make_backup(client)
    restored = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "ok.db"}
    )
    assert restored.status_code == 200
    assert restored.json()["row_count"] == 2


def test_backups_survive_service_restart(env):
    client = env["client"]
    append(client, "job-a", "before-restart")
    backup = make_backup(client)
    root = env["root"]

    restarted = create_app(data_dir=root)
    with TestClient(restarted) as client2:
        assert client2.get(f"/backups/{backup['id']}/verify").json()["verified"] is True
        assert client2.get("/backups").json()["items"][0]["id"] == backup["id"]
        restored = client2.post(
            f"/backups/{backup['id']}/restore", json={"target_name": "after.db"}
        )
        assert restored.status_code == 200
        assert restored.json()["max_id"] == 1
        append(client2, "job-a", "after-restart")


def test_docs_and_openapi_available(env):
    client = env["client"]
    assert client.get("/docs").status_code == 200
    schema = client.get("/openapi.json").json()
    for path in (
        "/logs",
        "/backups",
        "/backups/{backup_id}",
        "/backups/{backup_id}/restore",
    ):
        assert path in schema["paths"]
