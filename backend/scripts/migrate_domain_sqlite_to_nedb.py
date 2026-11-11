#!/usr/bin/env python3
"""migrate_domain_sqlite_to_nedb.py — SQLite → embedded-NEDB domain migrator.

The companion to migrate_sqlite_to_nedb.py: that script copies the
KV/zset/set layer to the legacy nedbd HTTP store; this one copies the
DOMAIN tables (the part that used to stay on SQLite) into the
embedded NEDB engine directory the backend now serves from:

  utxos                        -> utxo          (UTXO ledger)
  address_txs                  -> addr_tx       (per-address history)
  utxo_undo                    -> utxo_undo     (reorg undo log)
  address_index_blocks         -> addr_block    (indexed block hashes)
  mining_pools                 -> pool
  pool_reward_snapshots        -> snapshot
  pool_reward_snapshot_entries -> entry
  pool_reward_blocks           -> reward_block

It also seeds the id-allocation counters (vision:seq:*) to each
table's max id so post-cutover creates never collide with migrated
rows, and carries over the address indexer's KV control keys
(``vision:address_index:last_height`` / ``:status``) so the writer
resumes at the exact height SQLite left off instead of rebuilding
from genesis. Document shapes are imported from
app.embedded_domain — the exact builders the live store uses — so
migrated docs are indistinguishable from natively-written ones.

Streaming + resumable, following the KV migrator's pattern: rows are
read via LIMIT/OFFSET (peak memory ~ --batch-size rows), each batch
lands as one deferred-sync bulk put, and a state file next to the
embedded dir records per-table offsets after every batch. All writes
are idempotent puts keyed by the row's primary key, so an interrupted
run can simply be re-run. The SQLite file is opened READ-ONLY and is
never modified — after cutover it stays as the cold backup.

Usage
-----
  # Full migration (auto-resumes):
  python migrate_domain_sqlite_to_nedb.py --sqlite ../data/vision.db \
      --embedded-dir ../data/vision-embedded

  # Dry run (counts rows, no writes):
  python migrate_domain_sqlite_to_nedb.py --dry-run

  # Reset saved progress and start fresh:
  python migrate_domain_sqlite_to_nedb.py --reset

Environment variables (override CLI defaults):
  SQLITE_PATH, NEDB_EMBEDDED_DIR
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

try:
    import aiosqlite
except ImportError:
    print("ERROR: aiosqlite is required.")
    print("  pip install aiosqlite")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.embedded_domain import (  # noqa: E402
    COLL_ADDR_BLOCK, COLL_ADDR_TX, COLL_ENTRY, COLL_POOL,
    COLL_REWARD_BLOCK, COLL_SNAPSHOT, COLL_UNDO, COLL_UTXO,
    SEQ_ENTRY, SEQ_POOL, SEQ_REWARD_BLOCK, SEQ_SNAPSHOT,
    addr_block_doc, addr_tx_doc, entry_doc, pool_doc,
    reward_block_doc, snapshot_doc, undo_doc, utxo_doc,
)
from app.embedded_store import EmbeddedNedbStore  # noqa: E402
from app.sqlite_store import Keys  # noqa: E402  (constants only — no store is constructed)

# Address-indexer control keys that live in the SQLite kv table but
# belong to the domain layer: without them the embedded writer would
# restart the address index from genesis after cutover.
CONTROL_KEYS = (Keys.ADDRESS_INDEX_LAST_HEIGHT, Keys.ADDRESS_INDEX_STATUS)

__version__ = "1.0.0"

# (sqlite table, order column, counter key or None, row -> (coll, doc))
def _utxo_row(r):
    d = utxo_doc(r["txid"], r["vout"], r["address"], r["value"], r["height"])
    return COLL_UTXO, d


def _addr_tx_row(r):
    d = addr_tx_doc(r["address"], r["txid"], r["height"], r["direction"],
                    r["value"], r["block_time"])
    return COLL_ADDR_TX, d


def _undo_row(r):
    d = undo_doc(r["height"], r["txid"], r["vout"], r["address"],
                 r["value"], r["orig_height"])
    return COLL_UNDO, d


def _addr_block_row(r):
    d = addr_block_doc(r["height"], r["hash"])
    return COLL_ADDR_BLOCK, d


def _pool_row(r):
    return COLL_POOL, pool_doc(dict(r))


def _snapshot_row(r):
    return COLL_SNAPSHOT, snapshot_doc(dict(r))


def _entry_row(r):
    return COLL_ENTRY, entry_doc(dict(r))


def _reward_block_row(r):
    return COLL_REWARD_BLOCK, reward_block_doc(dict(r))


TABLES = [
    # (table, order_by, id counter key, builder)
    ("utxos", "rowid", None, _utxo_row),
    ("address_txs", "rowid", None, _addr_tx_row),
    ("utxo_undo", "rowid", None, _undo_row),
    ("address_index_blocks", "height", None, _addr_block_row),
    ("mining_pools", "id", SEQ_POOL, _pool_row),
    ("pool_reward_snapshots", "id", SEQ_SNAPSHOT, _snapshot_row),
    ("pool_reward_snapshot_entries", "id", SEQ_ENTRY, _entry_row),
    ("pool_reward_blocks", "id", SEQ_REWARD_BLOCK, _reward_block_row),
]


# ---------------------------------------------------------------------------
# State file (resume)
# ---------------------------------------------------------------------------

def _state_path(embedded_dir: Path) -> Path:
    return embedded_dir.parent / ".domain-migrate-state.json"


def _load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(path)


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

async def _table_count(conn, table: str) -> int:
    """Row count for ``table``; -1 ONLY when the table genuinely does
    not exist in this vintage of vision.db. Any other failure (wrong
    file, unreadable database, lock, bad URI) raises — a migrator must
    never disguise a broken source as an empty one."""
    try:
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cur:
            row = await cur.fetchone()
        return int(row[0] or 0)
    except Exception as e:
        if "no such table" in str(e):
            return -1
        raise RuntimeError(f"COUNT failed for table {table!r}: {e}") from e


async def _migrate_table(conn, store, table, order_by, builder,
                         batch_size, state, state_file, dry_run) -> None:
    total = await _table_count(conn, table)
    if total < 0:
        print(f"  {table:<32} (table absent — skipped)")
        return 0
    offset = 0 if dry_run else int(state.get(table, 0))
    if offset >= total and total > 0:
        print(f"  {table:<32} {total:,} rows — already migrated")
        return
    while offset < total:
        async with conn.execute(
            f"SELECT * FROM {table} ORDER BY {order_by} "
            f"LIMIT ? OFFSET ?",
            (batch_size, offset),
        ) as cur:
            rows = await cur.fetchall()
        if not rows:
            break
        if not dry_run:
            ops = []
            for r in rows:
                coll, doc = builder(dict(r))
                ops.append((coll, doc["_id"], doc))
            await store.bulk_puts(ops)
        offset += len(rows)
        if not dry_run:
            state[table] = offset
            _save_state(state_file, state)
        print(f"  {table:<32} {offset:,}/{total:,}", end="\r")
    print(f"  {table:<32} {total:,} rows migrated          ")


async def _run(args) -> int:
    sqlite_path = Path(args.sqlite).resolve()
    if not sqlite_path.exists():
        print(f"ERROR: SQLite not found: {sqlite_path}")
        return 1
    embedded_dir = Path(args.embedded_dir)
    state_file = _state_path(embedded_dir)
    if args.reset and state_file.exists():
        state_file.unlink()
        print("State file reset.")
    state = {} if args.dry_run else _load_state(state_file)

    print(f"\ndomain-migrator v{__version__} — SQLite → embedded NEDB\n")
    print(f"  sqlite        {sqlite_path} "
          f"({sqlite_path.stat().st_size:,} bytes)")
    print(f"  embedded dir  {embedded_dir}")
    if args.dry_run:
        print("  mode          DRY RUN (no writes)")

    conn = await aiosqlite.connect(f"file:{sqlite_path}?mode=ro",
                                   uri=True)
    conn.row_factory = aiosqlite.Row
    try:
        # Preflight BEFORE the (potentially expensive) embedded open:
        # prove the source really is a Vision database. If none of the
        # domain tables exist, this is the wrong file — say so loudly
        # instead of "migrating" an empty database in 0.1s.
        async with conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ) as cur:
            found_tables = {r[0] for r in await cur.fetchall()}
        expected_tables = {t for t, *_ in TABLES}
        if not (expected_tables & found_tables):
            print(f"\nERROR: no Vision domain tables in {sqlite_path}")
            print(f"  tables present: {sorted(found_tables) or 'none'}")
            print("  This is not the production vision.db — point "
                  "--sqlite at the real file (see SQLITE_DB_PATH in "
                  "the backend .env). Nothing was written.")
            return 2

        store = None
        if not args.dry_run:
            embedded_dir.mkdir(parents=True, exist_ok=True)
            store = EmbeddedNedbStore(str(embedded_dir))
            await store.open()

        t0 = time.time()
        try:
            for table, order_by, counter_key, builder in TABLES:
                await _migrate_table(
                    conn, store, table, order_by, builder,
                    args.batch_size, state, state_file, args.dry_run,
                )
            if store is not None:
                # Carry over the address indexer's KV control keys
                # (they live in the kv table, which the domain owns
                # for these two keys).
                for key in CONTROL_KEYS:
                    try:
                        async with conn.execute(
                            "SELECT value FROM kv WHERE key = ?", (key,)
                        ) as cur:
                            row = await cur.fetchone()
                    except Exception:
                        row = None
                    if row and row[0] is not None:
                        await store.set(key, row[0])
                        print(f"  control key carried over: {key}")
                # Seed each id counter from the table's true MAX(id)
                # (raise-only) so post-cutover creates never collide
                # with migrated rows — correct on resumed runs too.
                for table, _ob, counter_key, _b in TABLES:
                    if not counter_key:
                        continue
                    try:
                        async with conn.execute(
                            f"SELECT MAX(id) FROM {table}"
                        ) as cur:
                            row = await cur.fetchone()
                        max_id = int(row[0] or 0) if row else 0
                    except Exception:
                        max_id = 0
                    if max_id:
                        cur_raw = await store.get(counter_key)
                        cur = int(cur_raw) if cur_raw else 0
                        if max_id > cur:
                            await store.set(counter_key, str(max_id))
        finally:
            if store is not None:
                await store.aclose()
    finally:
        await conn.close()

    print(f"\nDone in {time.time() - t0:.1f}s. "
          f"{sqlite_path} is untouched and remains the cold backup.")
    return 0


def main() -> None:
    p = argparse.ArgumentParser(
        description="SQLite → embedded-NEDB domain migrator (v1)")
    p.add_argument("--sqlite",
                   default=os.getenv("SQLITE_PATH", "../data/vision.db"))
    p.add_argument("--embedded-dir",
                   default=os.getenv("NEDB_EMBEDDED_DIR",
                                     "../data/vision-embedded"))
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--dry-run", action="store_true",
                   help="count rows only; write nothing")
    p.add_argument("--reset", action="store_true",
                   help="discard saved progress and start fresh")
    args = p.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
