# Deploying Interchained Vision

This document is the short, opinionated path to a working production
instance. For full architectural details see `README.md`.

---

## 0. What you need

- An **ITC Core node** with `txindex=1` and JSON-RPC enabled.
- A Linux server with **Python 3.11+** and **Node 20+** (or just Docker).
- ~5 GB of free disk for the local address index DB (grows with chain).

ElectrumX is **optional** — Vision builds and serves its own UTXO and
per-address tx index from the node's RPC, so the explorer is fully
functional without one.

---

## 1. Configure

```bash
cp .env.example .env
$EDITOR .env   # fill in ITC_RPC_HOST / ITC_RPC_USER / ITC_RPC_PASS
```

If your node is behind a reverse-proxied URL, just put the full URL
in `ITC_RPC_HOST` and leave `ITC_RPC_PORT` blank.

---

## 2. Run — pick one

### Option A — Docker (recommended)

```bash
docker compose up -d --build
```

This builds and starts:

- `backend` on port **8080** (FastAPI + indexers)
- `web` on port **5000** (Next.js)

State lives in the named volume `vision-data` (persists across restarts).

### Option B — Bare metal

Two terminals:

```bash
# Terminal 1 — backend
cd backend
pip install -r requirements.txt
bash start.sh
```

```bash
# Terminal 2 — frontend
cd web
npm install
npm run build
npm start
```

---

## 3. First run — what to expect

On first start the backend opens the embedded NEDB store
(`data/vision-embedded/`) and begins **two indexers in parallel**:

1. **Block indexer** — caches block headers and recent tx records. Fast.
2. **Address indexer** — walks every block at verbosity=2 and builds a
   local UTXO + per-address-tx history table. **One-time backfill** of
   ~85 minutes on a healthy LAN node, ~85 blocks/sec sustained.

Watch progress:

```bash
curl http://localhost:8080/api/address-index/status
# {"phase":"backfilling","last_height":12345,"tip":510123}
```

Once `phase` reads `"live"` you're caught up. **Restarts always resume
from the last persisted height** — the address index is never rebuilt
unless you explicitly delete `data/vision-embedded/`.

---

## 4. Reverse proxy

Vision listens on `:8080` (API) and `:5000` (web). A typical Nginx
front-door:

```nginx
server {
  listen 443 ssl http2;
  server_name vision.example.com;

  location /api/ { proxy_pass http://127.0.0.1:8080; }
  location /sse/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
  }
  location /ws/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
  }
  location / { proxy_pass http://127.0.0.1:5000; }
}
```

If you put the API on a different origin from the web, set the three
`NEXT_PUBLIC_*_BASE` values in `.env` to the public API origin.

---

## 5. Operational notes

- **Backups**: snapshot the `data/vision-embedded/` directory (the
  live store). `data/vision.db` is a **cold backup only** — the
  migrated-from SQLite file; the backend never opens it (see §7).
- **Restarts are cheap.** The address indexer resumes from the last
  persisted height; no re-walking, no reindex.
- **Reorgs are automatic** within `REORG_DEPTH` (default 6 blocks). The
  indexer self-detects drift via stored per-height hashes and atomically
  rolls back the canonical UTXO set using its undo log.
- **ElectrumX is optional.** If you point `ELECTRUMX_*` at a server, it
  is used only to top up unconfirmed (mempool) balance. Failures are
  cached for 30 s so a slow upstream cannot block address pages.
- **Schema migrations.** New tables use `CREATE TABLE IF NOT EXISTS` —
  upgrades are non-destructive. Any future incompatible schema change
  will be flagged in the release notes.

---

## 6. Updating

```bash
git pull
docker compose up -d --build   # or repeat the bare-metal step 2
```

Database is preserved. The address index resumes where it left off.

---

## 7. Production: embedded NEDB only (cutover runbook)

The live store is the **embedded NEDB engine** — KV, UTXO ledger,
address history, pools and snapshots all live in
`data/vision-embedded/`, in-process. There is **no SQLite fallback**:
if the embedded store cannot open, the backend refuses to start
rather than silently serving a stale file. SQLite (`vision.db`)
remains on disk as a **cold backup / migration source only**.

**Required `.env` (production):**

```bash
STORE_MODE=embedded      # the only accepted value; anything else fails startup
DISABLE_SQLITE=1         # kill switch: constructing SQLiteStore raises RuntimeError
NEDB_EMBEDDED_DIR=../data/vision-embedded
# NEDB_URL=http://127.0.0.1:7070   # optional: legacy nedbd as a secondary
#                                  # history-sync source (never for serving)
```

**One-time cutover from an existing SQLite-backed deployment**
(run from `backend/`, backend stopped):

```bash
# 1. (Optional, speeds the first history sync) copy the KV layer to
#    the legacy nedbd store; the backfill can then read block docs
#    from it as a secondary source. Skippable — RPC works alone.
python scripts/migrate_sqlite_to_nedb.py --sqlite ../data/vision.db

# 2. Copy the DOMAIN tables into the embedded engine dir (UTXOs,
#    address tx index + its control keys, undo log, pools,
#    snapshots, entries, reward blocks; id counters seeded).
#    Streaming + resumable; safe to re-run.
python scripts/migrate_domain_sqlite_to_nedb.py \
    --sqlite ../data/vision.db --embedded-dir ../data/vision-embedded

# 3. Set the .env values above, then start the backend.
bash start.sh
```

**One-time bounded-open conversion (existing data dirs):** the
engine's stock open replays + re-verifies the entire op history on
every start — at production scale that is minutes of CPU and
multiple GB of RAM. Convert each existing embedded dir ONCE,
backend stopped, before starting:

```bash
python scripts/checkpoint_embedded.py --dir ../data/vision-embedded
```

It performs the slow audited open (full chain verify included),
records a checkpoint + `vision-open.json` sidecar, and proves the
bounded reopen. After that, opens are O(live state + new ops):
the backend checkpoints again on every clean shutdown, so the
fast path maintains itself. Re-running the script with
`--verify-only` is the full integrity audit.

**Verify:**

```bash
curl http://localhost:8080/api/health
# → "db": true, "sync": {"active": true, "store": "embedded", ...}
#   and "indexer_height" advancing. /api/stats/indexer should leave
#   the "starting" phase as the address indexer resumes from the
#   migrated height (not from genesis).
```

**After cutover:** leave `data/vision.db` (and its `-wal`/`-shm`)
exactly where it is — untouched cold backup. Do not delete it in
the same change that ships this cutover; it is the rollback path
(revert the deploy + remove `DISABLE_SQLITE` to return to the
pre-cutover code). The backwards history sync fills any block-KV
gaps from RPC (plus nedbd if `NEDB_URL` is set); when
`"sync": {"complete": true}` appears in `/api/health`, the embedded
store holds the full history on its own.
