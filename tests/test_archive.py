"""Tests for prefix archiving onto published backups."""

from __future__ import annotations

import hashlib
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from joblog.archive import ARCHIVE_ID_RE
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
            "archives": app.state.archives,
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


def all_log_ids(client: TestClient, **params) -> list[int]:
    ids: list[int] = []
    after = 0
    while True:
        page = client.get("/logs", params={"after_id": after, "limit": 3, **params})
        assert page.status_code == 200, page.text
        body = page.json()
        ids.extend(item["id"] for item in body["items"])
        if body["next_after_id"] is None:
            return ids
        after = body["next_after_id"]


def all_logs(client: TestClient, **params) -> list[dict]:
    out: list[dict] = []
    after = 0
    while True:
        page = client.get("/logs", params={"after_id": after, "limit": 2, **params})
        assert page.status_code == 200, page.text
        body = page.json()
        out.extend(body["items"])
        if body["next_after_id"] is None:
            return out
        after = body["next_after_id"]


def archive(client: TestClient, backup_id: str) -> dict:
    response = client.post(f"/backups/{backup_id}/archive")
    assert response.status_code == 200, response.text
    return response.json()


def seed(client, n, prefix="job-a"):
    return [append(client, prefix, f"msg-{i}")["id"] for i in range(n)]


# ----------------------------------------------------------------- happy path
def test_archive_moves_prefix_and_logs_remain_queryable(env):
    client = env["client"]
    ids = seed(client, 6)
    backup = make_backup(client)
    # rows appended after the backup stay in the source
    later = [append(client, "job-b", f"late-{i}")["id"] for i in range(2)]

    result = archive(client, backup["id"])
    assert ARCHIVE_ID_RE.match(result["id"])
    assert result["sequence"] == 1
    assert result["backup_id"] == backup["id"]
    assert (result["start_id"], result["end_id"]) == (ids[0], ids[-1])
    assert result["row_count"] == 6
    assert result["source_remaining_min_id"] == later[0]
    assert result["source_remaining_count"] == 2

    # The live table really shrank.
    with env["db"].reader() as conn:
        live = conn.execute("SELECT MIN(id), COUNT(*) FROM job_logs").fetchone()
    assert tuple(live) == (later[0], 2)

    # /logs still returns the full ordered, gap-free sequence.
    assert all_log_ids(client) == ids + later
    listing = client.get("/archive/segments").json()["items"]
    assert len(listing) == 1
    one = client.get(f"/archive/segments/{result['id']}").json()
    assert one["end_id"] == ids[-1]


def test_logs_pagination_and_job_filter_span_archive_boundary(env):
    client = env["client"]
    seed(client, 4, "job-a")
    b1 = make_backup(client)
    append(client, "job-b", "b1")
    append(client, "job-a", "a5")
    archive(client, b1["id"])

    # job_id filter merges segment rows and live rows with no gaps/duplicates.
    job_a = [item["id"] for item in all_logs(client, job_id="job-a")]
    assert job_a == [1, 2, 3, 4, 6]
    job_b = [item["id"] for item in all_logs(client, job_id="job-b")]
    assert job_b == [5]

    # Pagination cursor placed inside the archived range still walks all rows.
    page = client.get("/logs", params={"after_id": 3, "limit": 2}).json()
    assert [item["id"] for item in page["items"]] == [4, 5]
    assert page["next_after_id"] == 5
    page = client.get("/logs", params={"after_id": 5, "limit": 10}).json()
    assert [item["id"] for item in page["items"]] == [6]


def test_chain_continues_with_later_backups(env):
    client = env["client"]
    seed(client, 3)
    b1 = make_backup(client)
    archive(client, b1["id"])

    seed(client, 3)  # ids 4..6
    b2 = make_backup(client)
    seed(client, 2)  # ids 7..8 stay live
    result2 = archive(client, b2["id"])
    assert result2["sequence"] == 2
    assert (result2["start_id"], result2["end_id"]) == (4, 6)

    segments = client.get("/archive/segments").json()["items"]
    assert [(s["sequence"], s["start_id"], s["end_id"]) for s in segments] == [
        (1, 1, 3),
        (2, 4, 6),
    ]
    assert all_log_ids(client) == [1, 2, 3, 4, 5, 6, 7, 8]

    # A third archive after more appends.
    b3 = make_backup(client)
    result3 = archive(client, b3["id"])
    assert (result3["start_id"], result3["end_id"]) == (7, 8)
    assert all_log_ids(client) == list(range(1, 9))
    with env["db"].reader() as conn:
        assert conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0] == 0


def test_archive_empty_or_stale_backup_is_rejected(env):
    client = env["client"]
    empty = make_backup(client)  # no rows at all
    resp = client.post(f"/backups/{empty['id']}/archive")
    assert resp.status_code == 409

    seed(client, 3)
    b1 = make_backup(client)
    seed(client, 2)
    archive(client, b1["id"])

    # Re-archiving the same backup is a conflict.
    again = client.post(f"/backups/{b1['id']}/archive")
    assert again.status_code == 409

    # A backup that only contains already-archived rows cannot extend the chain.
    stale = client.post(f"/backups/{b1['id']}/archive")
    assert stale.status_code == 409

    # Unknown id -> 404.
    missing = client.post(
        "/backups/bkp-20000101T000000Z-000000000000/archive"
    )
    assert missing.status_code == 404

    # Source untouched after every rejection.
    with env["db"].reader() as conn:
        assert conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0] == 2
    assert all_log_ids(client) == [1, 2, 3, 4, 5]


# ------------------------------------------------------------------ failures
def test_corrupt_backup_cannot_be_archived_and_source_preserved(env):
    client = env["client"]
    seed(client, 5)
    backup = make_backup(client)
    db_path = env["root"] / "backups" / backup["id"] / "joblogs.db"
    db_path.chmod(0o644)
    db_path.write_bytes(db_path.read_bytes()[:2048])  # truncate -> corrupt

    resp = client.post(f"/backups/{backup['id']}/archive")
    assert resp.status_code == 409
    with env["db"].reader() as conn:
        assert conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0] == 5
    assert client.get("/archive/segments").json()["items"] == []
    # All source logs remain queryable.
    assert all_log_ids(client) == [1, 2, 3, 4, 5]
    # The service still accepts appends.
    append(client, "job-a", "after-failure")


def test_failure_inside_transaction_rolls_back_everything(env):
    client = env["client"]
    archives = env["archives"]
    seed(client, 4)
    backup = make_backup(client)

    def explode():
        raise RuntimeError("simulated crash before commit")

    with pytest.raises(RuntimeError):
        archives.archive_by_backup(backup["id"], before_commit=explode)

    # No segment registered, no row deleted.
    with env["db"].reader() as conn:
        assert conn.execute("SELECT COUNT(*) FROM archive_segments").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0] == 4
    assert all_log_ids(client) == [1, 2, 3, 4]

    # A retry after the fault clears completes normally.
    result = archive(client, backup["id"])
    assert result["row_count"] == 4
    assert all_log_ids(client) == [1, 2, 3, 4]


def test_backup_whose_contents_differ_from_source_is_rejected(env, monkeypatch):
    client = env["client"]
    archives = env["archives"]
    seed(client, 4)
    backup = make_backup(client)

    # Tamper with the source prefix *before* archiving by bypassing the API:
    # update a message so digest comparison must detect the divergence.
    with env["db"].writer() as conn:
        conn.execute("UPDATE job_logs SET message='tampered' WHERE id=2")

    resp = client.post(f"/backups/{backup['id']}/archive")
    assert resp.status_code == 409
    assert "differ" in resp.json()["detail"]
    with env["db"].reader() as conn:
        assert conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0] == 4
        assert conn.execute("SELECT COUNT(*) FROM archive_segments").fetchone()[0] == 0


# ------------------------------------------------------------- interleaving
def test_appends_during_archive_are_never_lost_or_duplicated(env):
    client = env["client"]
    db = env["db"]
    archives = env["archives"]
    seed(client, 10)
    backup = make_backup(client)

    appended: list[int] = []
    started = threading.Event()

    def writer():
        # Make sure the archive's read-side checks are already underway.
        started.wait(2)
        for i in range(10):
            with db.writer() as conn:
                cur = conn.execute(
                    "INSERT INTO job_logs(job_id, message) VALUES (?, ?)",
                    ("concurrent", f"c-{i}"),
                )
                appended.append(cur.lastrowid)

    original = archives.archive_by_backup

    def wrapped(backup_id, *, before_commit=None):
        started.set()
        return original(backup_id, before_commit=before_commit)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(archives, "archive_by_backup", wrapped)
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        result = archive(client, backup["id"])
    finally:
        thread.join()
        monkeypatch.undo()

    assert result["end_id"] == 10
    expected = list(range(1, 21))
    assert sorted(appended) == list(range(11, 21))
    assert all_log_ids(client) == expected
    # Every archived row is queryable exactly once.
    logs = all_logs(client)
    assert len(logs) == len({row["id"] for row in logs}) == 20


def test_concurrent_archives_serialize_without_gap(env):
    client = env["client"]
    archives = env["archives"]
    seed(client, 4)
    b1 = make_backup(client)
    seed(client, 4)
    b2 = make_backup(client)

    results: list[dict | BaseException] = []

    def run(bid):
        try:
            results.append(archives.archive_by_backup(bid))
        except BaseException as exc:  # noqa: BLE001 - recorded for assertion
            results.append(exc)

    t1 = threading.Thread(target=run, args=(b1["id"],))
    t2 = threading.Thread(target=run, args=(b2["id"],))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # One of the two must be rejected because the second backup does not start
    # at the source's first row once the first archive landed.
    ok = [r for r in results if not isinstance(r, BaseException)]
    conflicts = [r for r in results if isinstance(r, BaseException)]
    assert len(ok) == 1 and len(conflicts) == 1
    assert all_log_ids(client) == list(range(1, 9))
    with env["db"].reader() as conn:
        assert conn.execute("SELECT COUNT(*) FROM archive_segments").fetchone()[0] == 1


def test_queries_concurrent_with_archive_see_consistent_states(env):
    client = env["client"]
    db = env["db"]
    seed(client, 8)
    backup = make_backup(client)

    seen: list[list[int]] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            with db.read_transaction() as conn:
                rows = conn.execute(
                    "SELECT id FROM job_logs ORDER BY id"
                ).fetchall()
                seen.append([r[0] for r in rows])

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        archive(client, backup["id"])
    finally:
        stop.set()
        thread.join()

    # Every snapshot must be either the full pre-archive prefix or the
    # post-archive remainder, never a half-deleted mix.
    for snapshot in seen:
        assert snapshot == list(range(1, 9)) or snapshot == []
    assert all_log_ids(client) == list(range(1, 9))


# ------------------------------------------------------- corrupt segment reads
def test_corrupt_archive_segment_makes_logs_fail_explicitly(env):
    client = env["client"]
    seed(client, 4)
    b1 = make_backup(client)
    segment = archive(client, b1["id"])
    seed(client, 2)

    db_path = env["root"] / "backups" / b1["id"] / "joblogs.db"
    db_path.chmod(0o644)
    db_path.write_bytes(db_path.read_bytes()[:1024])  # corrupt the segment payload

    # A query touching the segment errors instead of returning missing rows.
    resp = client.get("/logs")
    assert resp.status_code == 500
    assert "archive" in resp.json()["detail"].lower()

    # A cursor past the damaged segment still serves live rows explicitly.
    ok = client.get("/logs", params={"after_id": 4})
    assert ok.status_code == 200
    assert [item["id"] for item in ok.json()["items"]] == [5, 6]

    # Segment listing (metadata only) still works and names the segment.
    assert client.get("/archive/segments").json()["items"][0]["id"] == segment["id"]


def test_missing_segment_backup_files_are_reported(env):
    client = env["client"]
    seed(client, 3)
    b1 = make_backup(client)
    archive(client, b1["id"])

    backup_dir = env["root"] / "backups" / b1["id"]
    for f in backup_dir.iterdir():
        f.chmod(0o644)
    (backup_dir / "joblogs.db").unlink()

    resp = client.get("/logs")
    assert resp.status_code == 500
    assert "missing" in resp.json()["detail"].lower()


# ----------------------------------------------------- coexistence / restart
def test_verify_download_restore_still_work_after_archiving(env):
    client = env["client"]
    seed(client, 5)
    backup = make_backup(client)
    archive(client, backup["id"])
    seed(client, 2)

    assert client.get(f"/backups/{backup['id']}/verify").json()["verified"] is True
    download = client.get(f"/backups/{backup['id']}/download")
    assert download.status_code == 200
    assert hashlib.sha256(download.content).hexdigest() == backup["sha256"]["joblogs.db"]

    restored = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "incident.db"}
    )
    assert restored.status_code == 200
    assert restored.json()["row_count"] == 5
    rows = client.get("/restores/incident.db/logs").json()["items"]
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5]

    # A backup taken after archiving only contains the remaining live rows and
    # can itself be archived as the next segment.
    b2 = make_backup(client)
    assert b2["snapshot_max_id"] == 7
    archive(client, b2["id"])
    assert all_log_ids(client) == list(range(1, 8))


def test_archive_survives_restart_and_health_reports_count(env):
    client = env["client"]
    seed(client, 3)
    b1 = make_backup(client)
    archive(client, b1["id"])
    seed(client, 2)
    root = env["root"]

    restarted = create_app(data_dir=root)
    with TestClient(restarted) as client2:
        assert client2.get("/health").json()["archive_segments"] == 1
        assert [r["id"] for r in all_logs(client2)] == [1, 2, 3, 4, 5]
        segments = client2.get("/archive/segments").json()["items"]
        assert segments[0]["backup_id"] == b1["id"]
        # Continue the chain after restart.
        b2 = client2.post("/backups").json()
        result = client2.post(f"/backups/{b2['id']}/archive").json()
        assert (result["start_id"], result["end_id"]) == (4, 5)
        assert [r["id"] for r in all_logs(client2)] == [1, 2, 3, 4, 5]


def test_archive_endpoints_documented(env):
    schema = env["client"].get("/openapi.json").json()
    assert "/backups/{backup_id}/archive" in schema["paths"]
    assert "/archive/segments" in schema["paths"]
    assert env["client"].get("/docs").status_code == 200
