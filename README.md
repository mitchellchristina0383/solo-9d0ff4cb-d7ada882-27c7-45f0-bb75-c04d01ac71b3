# Job Log Service

FastAPI + SQLite service for an append-only job log with crash-safe backup,
verified restore and prefix archiving.  All key flows are usable from the
interactive docs at `/docs`.

## Run

```bash
docker compose up --build
```

The API listens on `http://localhost:8000` (`JOBLOG_PORT` overrides the port),
docs at `http://localhost:8000/docs`.  State lives in the `joblog-data` volume:
source database `/data/joblogs/source.db`, backups `/data/backups`, restored
targets `/data/restores`.

Without Docker (Python 3.11+):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
JOBLOG_DATA_DIR=./data uvicorn joblog.main:app --port 8000
```

## API

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `POST` | `/logs` | Append a log entry; returns only after the commit is durable |
| `GET`  | `/logs` | Query committed logs across archive segments and the live table (`job_id`, `after_id`, `limit`) |
| `POST` | `/backups` | Create, verify and publish a backup; returns its backup id |
| `GET`  | `/backups` | List published backups |
| `GET`  | `/backups/{id}` | Get one published backup (id, SHA-256s, snapshot row count) |
| `GET`  | `/backups/{id}/verify` | Re-check manifest, SHA-256 and SQLite integrity |
| `GET`  | `/backups/{id}/download` | Download the verified `.db` file |
| `POST` | `/backups/{id}/restore` | Restore into an independent target (`target_name`, `overwrite`) |
| `POST` | `/backups/{id}/archive` | Archive the earliest contiguous source prefix covered by this backup |
| `GET`  | `/archive/segments` | List registered archive segments in chain order |
| `GET`  | `/archive/segments/{id}` | Get one archive segment registration |
| `GET`  | `/restores/{target_name}/logs` | Read logs from a restored target (read-only) |
| `GET`  | `/health` | Liveness/readiness (includes the number of archive segments) |

### Typical flow (`/docs` order)

1. `POST /logs` a few entries.
2. `POST /backups` — the response contains the published `id`, the database
   SHA-256 and the manifest SHA-256.
3. `POST /logs` more entries (they must not appear in the backup).
4. `GET /backups/{id}/verify` — confirms the manifest still matches.
5. `POST /backups/{id}/restore` with `{"target_name": "incident-2026.db"}`.
6. `GET /restores/incident-2026.db/logs` — shows the logs committed before the
   backup and none appended afterwards.

### Archiving the oldest prefix (`/docs` "archive" tag)

1. `POST /logs` enough entries, then `POST /backups` to publish backup **b1**.
2. Keep appending; those new rows stay in the source and out of b1.
3. `POST /backups/b1/archive` — the backup is re-verified and its rows are
   compared one-for-one with the source's earliest contiguous prefix.  The
   archive segment is registered and those source rows are deleted **in the
   same transaction**.  The response reports the covered id range and the
   remaining live rows.
4. `GET /logs` still returns every log id in order, transparently merging the
   archived segment with the live table; `job_id`, `after_id` and `limit`
   behave exactly as before.
5. Publish a later backup **b2** and archive it: its segment starts exactly at
   the previous segment's `end_id + 1`, so the chain continues prefix by
   prefix.  Original verify/download/restore on b1 and b2 keep working.
6. If a backup is corrupt, does not continue the chain, or differs from the
   source prefix, archiving returns `409` and deletes nothing; all
   not-yet-archived source logs remain queryable.  If an already-registered
   segment's files later become corrupt, `GET /logs` that would touch it
   returns `500` with an explicit reason instead of a result set with gaps
   (a cursor past the damaged segment still serves the live rows).

## Guarantees and how they are achieved

* **Consistent snapshot under concurrent appends.**  A backup opens a
  read-only connection, starts a read transaction (`BEGIN` + a `SELECT`, which
  pins one committed WAL snapshot) and copies with SQLite's online-backup API.
  Writers keep appending on `BEGIN IMMEDIATE` transactions; rows committed
  after the snapshot are excluded, rows committed before are included.  The
  copy's `COUNT(*)`/`MAX(id)` are cross-checked against the frozen snapshot.
* **Backup id published only after verification.**  The copy is staged under
  `backups/.staging/`, checked with `PRAGMA integrity_check`, a
  `manifest.json` with SHA-256 digests is written, and the whole directory is
  re-verified from disk.  Only then is it atomically renamed to
  `backups/<id>` and the database row flipped to `ready`.  The API only ever
  exposes `ready` backups.
* **Interruption / restart safety.**  A backup row is inserted as `staging`
  before any copying.  Startup runs `recover_incomplete()`: staging directories
  are deleted, non-`ready` rows become `failed`, and `ready` rows whose files
  vanished are demoted to `failed`.  A partial or interrupted backup can never
  be listed, downloaded or restored.
* **Published backups are immutable.**  Backup files are made read-only
  (`0444`), stored outside the source database, and never touched by later
  appends.  The source is a normal WAL database and stays writable.
* **Verified restore.**  `POST /backups/{id}/restore` re-verifies the manifest
  hash, the database hash and `integrity_check` *before* touching the target.
  The restored copy is prepared and hashed in a private temp directory, then
  renamed into place.  An existing target (when `overwrite=true`) is moved
  aside first and put back on any failure, so the previous target survives
  bit-for-bit.
* **Independent target.**  Restore writes a separate database file under
  `/data/restores`; later appends to the source and later writes to the target
  are isolated from each other.  The restore endpoint never opens the source
  for writing.
* **Only a fully covered contiguous prefix is archived.**  An archive first
  re-runs the full backup verification (manifest, SHA-256, integrity), then
  checks that the backup's id range begins exactly at the first id still in the
  source and continues without holes.  Every covered row is compared against
  the source using an explicit id-sequence check plus a per-row SHA-256 digest
  (folded over the whole prefix).  A damaged, stale or mismatched backup is
  rejected with `409` before any source row is deleted.
* **Segment registration and source deletion commit together.**  The
  `archive_segments` row and the `DELETE ... WHERE id BETWEEN start AND end`
  run in one `BEGIN IMMEDIATE` transaction.  A crash or any error before commit
  leaves neither a segment nor a missing row; after commit both are visible.
  There is no state in which a log is neither in the source nor registered in
  an archive, so a failed archive never loses queryable logs.
* **Backups continue the chain; the live table actually shrinks.**  Each
  segment stores its inclusive `[start_id, end_id]`; the next archive must
  start at `end_id + 1`, which is also the smallest remaining source id.
  Re-archiving the same backup or archiving a stale prefix is a `409`
  conflict.
* **No missing or duplicated rows under interleaved append/query.**  `GET
  /logs` runs inside one pinned read transaction and merges the segment chain
  followed by the live table with a single `LIMIT` across both.  The chain and
  the source are validated for contiguity and each touched segment is
  re-verified from its manifest before rows are returned.  A broken chain, a
  source hole or a corrupt segment raises an explicit `500`; the endpoint never
  silently returns a partial sequence.
* **Archiving reuses immutable backups and does not impair them.**  A segment
  points at the published, read-only backup directory; verify, download and
  restore keep working unchanged, and archiving only ever opens backup files
  read-only.

## Tests

```bash
pip install -r requirements.txt pytest httpx
python -m pytest
```

The suite covers concurrent appends during a backup, frozen-snapshot
behaviour, immutable published backups, independent restore, corrupt-backup
restore with the target preserved, interrupted backups after restart,
`ready`-row demotion, backup failure, restart persistence and `/docs`.

`tests/test_archive.py` additionally covers: archiving a prefix while later
appends stay live, `job_id`/`after_id`/pagination across the archive
boundary, chain continuation over several later backups, rejection of empty,
stale, duplicate, corrupt or content-diverging backups with the source
untouched, full rollback when the archive transaction fails, concurrent
appends/queries/archives with no lost or duplicated row, explicit `500` on a
corrupt or missing segment instead of gap-filled results, coexistence with
verify/download/restore, and restart persistence of the archive chain.

## Notes / limits

* The service is a single process.  `recover_incomplete()` assumes it is the
  only writer to the backup directory at startup; do not run several API
  instances against the same `/data` volume.
* Backup files are hash-checked on every verify/download/restore, which is
  O(backup size).  Fine for job-log volumes; for very large databases this
  should be cached.
* Restore takes an explicit `target_name` (a filename under `/data/restores`);
  absolute host paths are deliberately not accepted through the API.
* Archiving deletes rows to shrink the *logical* table and the WAL; it does not
  run `VACUUM`, so the on-disk file size is reclaimed lazily by SQLite rather
  than immediately.
* Archive segments are stored as references to the immutable published backup
  files, not as extra copies: deleting a backup directory out-of-band would
  break the segment.  Such damage is detected (manifest/hash check) and
  reported as an explicit error on the next query that touches the segment.
* Like verify/download/restore, reading an archived segment re-hashes its
  backup file, which is O(segment size); this is fine for job-log volumes but
  could be cached for very large databases.
