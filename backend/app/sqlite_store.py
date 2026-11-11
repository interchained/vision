"""Async SQLite store — drop-in replacement for the Redis client.

Provides the same async method surface used throughout the Vision backend
so that consumer modules only need to change their import line.

Tables
------
kv     – key/value pairs with optional TTL (expires_at).
zsets   – sorted sets (ZADD / ZREVRANGE / ZREMRANGEBYRANK).
sets    – unordered sets (SADD / SMEMBERS / SREM).

Pub/sub is intentionally a no-op here; the in-process ``EventBus``
already handles local fanout, which is sufficient for single-instance
deployments.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
import time
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple, Union

import aiosqlite

from .config import settings

logger = logging.getLogger(__name__)

_db: Optional[aiosqlite.Connection] = None
_lock = asyncio.Lock()


def _assert_sqlite_enabled() -> None:
    """Production kill switch (DISABLE_SQLITE=1): the live store is
    the embedded NEDB engine, and SQLite survives only as the
    offline backup / migration source. Any app-code path that tries
    to construct or open the SQLite store then fails loudly instead
    of silently falling back to the file. (Offline scripts read the
    file via aiosqlite directly and never trip this guard.)"""
    if settings.sqlite_disabled:
        raise RuntimeError(
            "SQLiteStore is disabled (DISABLE_SQLITE=1): the live "
            "store is the embedded NEDB engine; SQLite is backup / "
            "migration-source only."
        )

# ── Write coordinator ─────────────────────────────────────────────────────────
# All SQLite mutations — both on the shared KV connection and on the dedicated
# AddressIndexWriter connection — acquire this lock before touching the file.
# This serialises writers at the Python/asyncio layer, making busy_timeout a
# last resort rather than the first line of defence against "database is locked".
_WRITE_LOCK: asyncio.Lock  # initialised lazily (needs a running event loop)
_WRITE_LOCK_READY = False

def _get_write_lock() -> asyncio.Lock:
    global _WRITE_LOCK, _WRITE_LOCK_READY
    if not _WRITE_LOCK_READY:
        _WRITE_LOCK = asyncio.Lock()
        _WRITE_LOCK_READY = True
    return _WRITE_LOCK

_LOCK_MAX_RETRIES = 12         # attempts before giving up (includes first try)
_LOCK_BASE_DELAY  = 0.15       # seconds — doubles each attempt + jitter, capped below
_LOCK_MAX_DELAY   = 8.0        # per-attempt cap so total wait stays under ~30 s

# Re-export the Keys namespace so consumers can keep using it.
class Keys:
    TIP_HEIGHT = "vision:tip:height"
    TIP_HASH = "vision:tip:hash"
    TIP_BLOCK_JSON = "vision:tip:block"
    BLOCK_BY_HEIGHT = "vision:block:height:{height}"   # hash
    BLOCK_BY_HASH = "vision:block:hash:{hash}"         # JSON
    TX_BY_TXID = "vision:tx:{txid}"                    # JSON
    RECENT_BLOCKS = "vision:recent:blocks"             # ZSET (height -> hash)
    RECENT_TXS = "vision:recent:txs"                   # ZSET (timestamp -> txid)
    MEMPOOL_SUMMARY = "vision:mempool:summary"         # JSON
    MEMPOOL_TXS = "vision:mempool:txs"                 # ZSET (fee_rate -> txid)
    MEMPOOL_FEE_HISTOGRAM = "vision:mempool:fee_hist"  # JSON
    NETWORK_STATS = "vision:stats:network"             # JSON
    SUPPLY_INFO = "vision:stats:supply"                # JSON {circulating_sats, height, computed_at, source}
    PRICE_ITC_USD = "vision:price:itc_usd"             # JSON
    INDEXER_LAST_HEIGHT = "vision:indexer:last_height"
    INDEXER_STATUS = "vision:indexer:status"            # JSON
    EVENT_STREAM = "vision:events"                     # (no-op for SQLite)
    ADDRESS_STATS = "vision:address:{addr}:stats"      # JSON
    ADDRESS_TXS = "vision:address:{addr}:txs"          # ZSET
    SPECIAL_ADDRESSES = "vision:special_addresses"     # SET
    TOKEN_REGISTRY = "vision:tokens:registry"          # JSON list
    TOKEN_META = "vision:token:{id}:meta"              # JSON
    TOKEN_HISTORY = "vision:token:{id}:history"        # JSON list
    POOL_DETECTOR_PATTERNS = "vision:pools:patterns"   # JSON
    RATE_LIMIT = "vision:rl:{ip}:{minute}"             # counter
    WEBHOOK_SUBSCRIBERS = "vision:webhooks"            # SET
    SEARCH_RECENT = "vision:search:recent"             # LIST
    ADDRESS_INDEX_LAST_HEIGHT = "vision:address_index:last_height"
    ADDRESS_INDEX_STATUS = "vision:address_index:status"  # JSON


_SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    expires_at REAL          -- unix timestamp; NULL = never expires
);

CREATE TABLE IF NOT EXISTS zsets (
    name   TEXT NOT NULL,
    member TEXT NOT NULL,
    score  REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (name, member)
);

CREATE TABLE IF NOT EXISTS sets (
    name   TEXT NOT NULL,
    member TEXT NOT NULL,
    PRIMARY KEY (name, member)
);

-- Unspent transaction outputs. The authoritative ledger we maintain so we can
-- answer "what's the balance of address X" without an external indexer.
CREATE TABLE IF NOT EXISTS utxos (
    txid    TEXT NOT NULL,
    vout    INTEGER NOT NULL,
    address TEXT NOT NULL,
    value   INTEGER NOT NULL,   -- sats
    height  INTEGER NOT NULL,
    PRIMARY KEY (txid, vout)
);

-- Per-address transaction history. One row per (address, txid, direction)
-- so a tx that both sends and receives for the same address gets two rows
-- (rare for non-self-spend, but cheap and correct).
CREATE TABLE IF NOT EXISTS address_txs (
    address   TEXT NOT NULL,
    txid      TEXT NOT NULL,
    height    INTEGER NOT NULL,
    direction TEXT NOT NULL,    -- 'in' (received) or 'out' (spent)
    value     INTEGER NOT NULL, -- sats moved in this leg
    block_time INTEGER,
    PRIMARY KEY (address, txid, direction)
);

-- Indexed block hashes — used by the address indexer to self-detect reorgs.
-- A separate row per indexed height, dropped during rollback_to().
CREATE TABLE IF NOT EXISTS address_index_blocks (
    height INTEGER PRIMARY KEY,
    hash   TEXT NOT NULL
);

-- Spend undo log — one row per UTXO consumed at each indexed block.
-- On reorg rollback we re-insert UTXOs whose ORIGINAL creation height is
-- <= the rollback height so the canonical post-rollback UTXO set is
-- recovered exactly. Pruned during rollback for the rolled-back range.
CREATE TABLE IF NOT EXISTS utxo_undo (
    height      INTEGER NOT NULL,   -- block in which this utxo was spent
    txid        TEXT    NOT NULL,   -- original outpoint txid
    vout        INTEGER NOT NULL,   -- original outpoint index
    address     TEXT    NOT NULL,
    value       INTEGER NOT NULL,
    orig_height INTEGER NOT NULL,   -- height at which the utxo was originally created
    PRIMARY KEY (height, txid, vout)
);
CREATE INDEX IF NOT EXISTS idx_utxo_undo_height ON utxo_undo(height);

CREATE INDEX IF NOT EXISTS idx_kv_expires ON kv(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_zsets_score ON zsets(name, score);
CREATE INDEX IF NOT EXISTS idx_utxos_address ON utxos(address);
CREATE INDEX IF NOT EXISTS idx_utxos_height ON utxos(height);
CREATE INDEX IF NOT EXISTS idx_addrtx_addr_height ON address_txs(address, height DESC);
CREATE INDEX IF NOT EXISTS idx_addrtx_height ON address_txs(height);

-- ── Pool Operator Snapshot Rewards ─────────────────────────────────────────
-- A snapshot-based airdrop program (separate from block subsidy) that pays
-- mining pool operators a fixed ITC reward per block they mined during a
-- weekly height range. Pools are matched primarily by their coinbase PAYOUT
-- ADDRESS (registered by an admin), with the coinbase tag as an optional
-- secondary signal. All ITC amounts are stored as TEXT (decimal strings with
-- 8 dp) so reward arithmetic never touches floating point.

CREATE TABLE IF NOT EXISTS mining_pools (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_name      TEXT NOT NULL,
    coinbase_tag   TEXT,                       -- optional secondary match signal
    payout_address TEXT,                       -- primary match key
    website        TEXT,
    contact_email  TEXT,
    discord        TEXT,
    telegram       TEXT,
    status         TEXT NOT NULL DEFAULT 'active',  -- active | disabled
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mining_pools_payout ON mining_pools(payout_address);
CREATE INDEX IF NOT EXISTS idx_mining_pools_status ON mining_pools(status);
-- DB-enforced invariant: at most one ACTIVE pool per payout address. Prevents
-- ambiguous block attribution even under concurrent admin writes.
CREATE UNIQUE INDEX IF NOT EXISTS idx_mining_pools_active_payout
    ON mining_pools(payout_address)
    WHERE status = 'active' AND payout_address IS NOT NULL;

CREATE TABLE IF NOT EXISTS pool_reward_snapshots (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_name        TEXT NOT NULL,
    start_height         INTEGER NOT NULL,
    end_height           INTEGER NOT NULL,
    reward_per_block     TEXT NOT NULL,        -- decimal string, 8 dp
    total_blocks_scanned INTEGER NOT NULL DEFAULT 0,
    total_blocks_matched INTEGER NOT NULL DEFAULT 0,
    total_reward         TEXT NOT NULL DEFAULT '0.00000000',
    status               TEXT NOT NULL DEFAULT 'generated',  -- draft|generated|approved|paid|rejected
    created_at           REAL NOT NULL,
    approved_at          REAL,
    paid_at              REAL,
    notes                TEXT
);
CREATE INDEX IF NOT EXISTS idx_pool_snap_range ON pool_reward_snapshots(start_height, end_height);
CREATE INDEX IF NOT EXISTS idx_pool_snap_status ON pool_reward_snapshots(status);

"""

# Reward entries + per-block rows are address-centric: a "winner" is a REGISTERED
# payout address (a miner OR a node operator who registered on the treasury
# grants page) that received a coinbase output (>0) in the snapshot range. Hence
# the entry key is the payout ADDRESS (UNIQUE per snapshot), pool_id carries the
# registered pool id (nullable only for legacy rows), and a single block can
# appear once per recipient (UNIQUE includes payout_address).
#
# These two tables hold *derived* data (rebuildable by re-running a snapshot), so
# the migration in ``_migrate_pool_reward_tables`` may drop+recreate them when an
# older (pool_id-keyed) shape is detected.
_ENTRIES_DDL = """
CREATE TABLE IF NOT EXISTS pool_reward_snapshot_entries (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id      INTEGER NOT NULL,
    pool_id          INTEGER,                  -- registered pool id, else NULL
    pool_name        TEXT NOT NULL,            -- pool name if registered, else the address
    payout_address   TEXT NOT NULL,            -- the coinbase recipient (match key)
    blocks_found     INTEGER NOT NULL DEFAULT 0,
    reward_per_block TEXT NOT NULL,            -- decimal string, 8 dp
    total_reward     TEXT NOT NULL,            -- decimal string, 8 dp
    status           TEXT NOT NULL DEFAULT 'pending',  -- pending|approved|paid|rejected
    txid             TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    UNIQUE (snapshot_id, payout_address)
);
CREATE INDEX IF NOT EXISTS idx_pool_entry_snap ON pool_reward_snapshot_entries(snapshot_id);
"""

_BLOCKS_DDL = """
CREATE TABLE IF NOT EXISTS pool_reward_blocks (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id           INTEGER NOT NULL,
    pool_id               INTEGER,
    block_height          INTEGER NOT NULL,
    block_hash            TEXT NOT NULL,
    coinbase_tag_detected TEXT,
    payout_address        TEXT,
    reward_amount         TEXT NOT NULL,        -- decimal string, 8 dp
    created_at            REAL NOT NULL,
    UNIQUE (snapshot_id, block_height, payout_address)
);
CREATE INDEX IF NOT EXISTS idx_pool_blocks_snap ON pool_reward_blocks(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_pool_blocks_pool ON pool_reward_blocks(snapshot_id, pool_id);
"""


class SQLiteStore:
    """Async facade that mirrors the subset of the Redis API used by Vision."""

    def __init__(self, conn: aiosqlite.Connection):
        _assert_sqlite_enabled()
        self._conn = conn

    # ── internal write helper ─────────────────────────────────────────────

    async def _locked_write(self, coro):
        """Acquire the global write lock then execute *coro*.

        Retries up to _LOCK_MAX_RETRIES times on transient "database is locked"
        errors (belt-and-suspenders on top of the Python-level lock, handles
        any external writer or checkpoint race).
        """
        lock = _get_write_lock()
        for attempt in range(_LOCK_MAX_RETRIES):
            try:
                async with lock:
                    return await coro()
            except Exception as exc:
                msg = str(exc).lower()
                if "database is locked" in msg and attempt < _LOCK_MAX_RETRIES - 1:
                    delay = min(
                        _LOCK_MAX_DELAY,
                        _LOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, _LOCK_BASE_DELAY),
                    )
                    logger.debug(
                        "SQLite write locked (attempt %d/%d), retrying in %.3fs: %s",
                        attempt + 1, _LOCK_MAX_RETRIES, delay, exc,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

    # ── key / value ──────────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[str]:
        now = time.time()
        async with self._conn.execute(
            "SELECT value FROM kv WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            (key, now),
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:  # noqa: A003
        expires = (time.time() + ex) if ex else None

        async def _do():
            await self._conn.execute(
                "INSERT OR REPLACE INTO kv (key, value, expires_at) VALUES (?, ?, ?)",
                (key, str(value), expires),
            )
            await self._conn.commit()

        await self._locked_write(_do)

    async def delete(self, *keys: str) -> int:
        async def _do():
            count = 0
            for k in keys:
                cur = await self._conn.execute("DELETE FROM kv WHERE key = ?", (k,))
                count += cur.rowcount
            for k in keys:
                await self._conn.execute("DELETE FROM sets WHERE name = ?", (k,))
                await self._conn.execute("DELETE FROM zsets WHERE name = ?", (k,))
            await self._conn.commit()
            return count

        return await self._locked_write(_do)

    async def incr(self, key: str) -> int:
        now = time.time()

        async def _do():
            await self._conn.execute(
                """INSERT INTO kv (key, value, expires_at) VALUES (?, '1', NULL)
                   ON CONFLICT(key) DO UPDATE
                   SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)
                   WHERE expires_at IS NULL OR expires_at > ?""",
                (key, now),
            )
            await self._conn.commit()

        await self._locked_write(_do)
        row = await self.get(key)
        return int(row) if row else 1

    async def expire(self, key: str, seconds: int) -> None:
        expires = time.time() + seconds

        async def _do():
            await self._conn.execute(
                "UPDATE kv SET expires_at = ? WHERE key = ?", (expires, key)
            )
            await self._conn.commit()

        await self._locked_write(_do)

    # ── sorted sets ──────────────────────────────────────────────────────

    async def zadd(self, name: str, mapping: dict) -> int:
        items = list(mapping.items())

        async def _do():
            for member, score in items:
                await self._conn.execute(
                    "INSERT OR REPLACE INTO zsets (name, member, score) VALUES (?, ?, ?)",
                    (name, str(member), float(score)),
                )
            await self._conn.commit()
            return len(items)

        return await self._locked_write(_do)

    async def zrevrange(
        self, name: str, start: int, stop: int, withscores: bool = False
    ) -> list:
        # Redis zrevrange is inclusive [start, stop], 0-indexed.
        limit = stop - start + 1
        async with self._conn.execute(
            "SELECT member, score FROM zsets WHERE name = ? ORDER BY score DESC LIMIT ? OFFSET ?",
            (name, limit, start),
        ) as cur:
            rows = await cur.fetchall()
        if withscores:
            return [(r[0], r[1]) for r in rows]
        return [r[0] for r in rows]

    async def zremrangebyrank(self, name: str, start: int, stop: int) -> int:
        """Remove members by rank (0-indexed, ascending score order).

        Negative stop means 'keep the top N'; e.g. zremrangebyrank(k, 0, -201)
        keeps the 200 highest-scored entries.
        """
        async def _do():
            if stop < 0:
                async with self._conn.execute(
                    "SELECT COUNT(*) FROM zsets WHERE name = ?", (name,)
                ) as cur:
                    total = (await cur.fetchone())[0]
                keep = abs(stop) - 1
                delete_count = total - keep
                if delete_count <= 0:
                    return 0
                await self._conn.execute(
                    """DELETE FROM zsets WHERE rowid IN (
                           SELECT rowid FROM zsets WHERE name = ?
                           ORDER BY score ASC LIMIT ?
                       )""",
                    (name, delete_count),
                )
            else:
                limit = stop - start + 1
                await self._conn.execute(
                    """DELETE FROM zsets WHERE rowid IN (
                           SELECT rowid FROM zsets WHERE name = ?
                           ORDER BY score ASC LIMIT ? OFFSET ?
                       )""",
                    (name, limit, start),
                )
            await self._conn.commit()
            return 0

        return await self._locked_write(_do)

    # ── sets ─────────────────────────────────────────────────────────────

    async def sadd(self, name: str, *members: str) -> int:
        mlist = [str(m) for m in members]

        async def _do():
            count = 0
            for m in mlist:
                try:
                    await self._conn.execute(
                        "INSERT OR IGNORE INTO sets (name, member) VALUES (?, ?)",
                        (name, m),
                    )
                    count += 1
                except Exception:
                    pass
            await self._conn.commit()
            return count

        return await self._locked_write(_do)

    async def smembers(self, name: str) -> set:
        async with self._conn.execute(
            "SELECT member FROM sets WHERE name = ?", (name,)
        ) as cur:
            rows = await cur.fetchall()
        return {r[0] for r in rows}

    async def srem(self, name: str, *members: str) -> int:
        mlist = [str(m) for m in members]

        async def _do():
            count = 0
            for m in mlist:
                cur = await self._conn.execute(
                    "DELETE FROM sets WHERE name = ? AND member = ?", (name, m)
                )
                count += cur.rowcount
            await self._conn.commit()
            return count

        return await self._locked_write(_do)

    # ── address index ────────────────────────────────────────────────────
    #
    # The indexer maintains a UTXO ledger and per-address tx history so the
    # explorer can answer balance/history queries without an external server.

    async def utxo_add_batch(self, rows: Sequence[Tuple[str, int, str, int, int]]) -> None:
        """Bulk-insert UTXOs. ``rows`` items: (txid, vout, address, value, height)."""
        if not rows:
            return
        await self._conn.executemany(
            "INSERT OR REPLACE INTO utxos (txid, vout, address, value, height) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )

    async def utxo_spend_batch(self, outpoints: Sequence[Tuple[str, int]]) -> List[Tuple[str, int, str, int, int]]:
        """Resolve and remove UTXOs by (txid, vout). Returns the rows that
        were deleted so callers know which addresses & values to credit on
        the spend side."""
        if not outpoints:
            return []
        # Look up first so we can return them; SQLite has no RETURNING in
        # older versions we want to support.
        placeholders = ",".join("(?,?)" for _ in outpoints)
        params: list = []
        for tx, vo in outpoints:
            params.extend((tx, vo))
        async with self._conn.execute(
            f"SELECT txid, vout, address, value, height FROM utxos "
            f"WHERE (txid, vout) IN (VALUES {placeholders})",
            params,
        ) as cur:
            found = await cur.fetchall()
        if found:
            await self._conn.executemany(
                "DELETE FROM utxos WHERE txid = ? AND vout = ?",
                [(r[0], r[1]) for r in found],
            )
        return [(r[0], r[1], r[2], r[3], r[4]) for r in found]

    async def address_tx_add_batch(
        self,
        rows: Sequence[Tuple[str, str, int, str, int, Optional[int]]],
    ) -> None:
        """Bulk-insert address-tx history. items: (address, txid, height, direction, value, block_time)."""
        if not rows:
            return
        await self._conn.executemany(
            "INSERT OR REPLACE INTO address_txs "
            "(address, txid, height, direction, value, block_time) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

    async def commit(self) -> None:
        """Flush a batch. Call after a series of utxo_/address_tx_ writes."""
        await self._conn.commit()

    async def address_balance(self, address: str) -> Tuple[int, int]:
        """Return (confirmed_balance_sats, total_received_sats) for address."""
        async with self._conn.execute(
            "SELECT COALESCE(SUM(value), 0) FROM utxos WHERE address = ?",
            (address,),
        ) as cur:
            row = await cur.fetchone()
        balance = int(row[0] or 0)
        async with self._conn.execute(
            "SELECT COALESCE(SUM(value), 0) FROM address_txs WHERE address = ? AND direction = 'in'",
            (address,),
        ) as cur:
            row = await cur.fetchone()
        received = int(row[0] or 0)
        return balance, received

    async def address_tx_count(self, address: str) -> int:
        """Distinct txids touching this address (in or out)."""
        async with self._conn.execute(
            "SELECT COUNT(DISTINCT txid) FROM address_txs WHERE address = ?",
            (address,),
        ) as cur:
            row = await cur.fetchone()
        return int(row[0] or 0)

    async def address_first_last_height(self, address: str) -> Tuple[Optional[int], Optional[int]]:
        async with self._conn.execute(
            "SELECT MIN(height), MAX(height) FROM address_txs WHERE address = ?",
            (address,),
        ) as cur:
            row = await cur.fetchone()
        if not row or row[0] is None:
            return None, None
        return int(row[0]), int(row[1])

    async def address_txs(
        self, address: str, limit: int = 50, offset: int = 0
    ) -> List[dict]:
        """Most-recent-first list of {txid, height, block_time, in_value, out_value}."""
        async with self._conn.execute(
            """
            SELECT txid,
                   MAX(height) AS h,
                   MAX(block_time) AS bt,
                   COALESCE(SUM(CASE WHEN direction = 'in'  THEN value END), 0),
                   COALESCE(SUM(CASE WHEN direction = 'out' THEN value END), 0)
              FROM address_txs
             WHERE address = ?
             GROUP BY txid
             ORDER BY h DESC, txid
             LIMIT ? OFFSET ?
            """,
            (address, limit, offset),
        ) as cur:
            rows = await cur.fetchall()
        return [
            {
                "txid": r[0],
                "height": r[1],
                "block_time": r[2],
                "in_sats": int(r[3] or 0),
                "out_sats": int(r[4] or 0),
            }
            for r in rows
        ]

    async def address_utxos(self, address: str) -> List[dict]:
        async with self._conn.execute(
            "SELECT txid, vout, value, height FROM utxos "
            "WHERE address = ? ORDER BY height DESC, txid",
            (address,),
        ) as cur:
            rows = await cur.fetchall()
        return [
            {"txid": r[0], "vout": int(r[1]), "value_sats": int(r[2]), "height": int(r[3])}
            for r in rows
        ]

    async def address_index_rollback(self, from_height: int) -> None:
        """Drop everything strictly above ``from_height`` so the indexer
        can replay after a reorg.

        Wraps both DELETEs in an explicit transaction — under autocommit a
        failure between the two deletes would otherwise leave the DB
        inconsistent (utxos gone, address_txs intact).
        """
        async def _do():
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                await self._conn.execute(
                    "DELETE FROM utxos       WHERE height > ?", (from_height,)
                )
                await self._conn.execute(
                    "DELETE FROM address_txs WHERE height > ?", (from_height,)
                )
                await self._conn.execute("COMMIT")
            except Exception:
                try:
                    await self._conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

        await self._locked_write(_do)

    # ── pool operator snapshot rewards ────────────────────────────────────
    #
    # All ITC amounts are persisted as decimal strings (8 dp); callers do the
    # Decimal arithmetic. Writes go through _locked_write like the rest of the
    # store so they serialise against the indexer's writers.

    @staticmethod
    def _row_to_dict(cur, row) -> Optional[dict]:
        if row is None:
            return None
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))

    @staticmethod
    def _rows_to_dicts(cur, rows) -> List[dict]:
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in rows]

    # -- pools --
    async def pool_create(self, data: dict) -> int:
        now = time.time()

        async def _do():
            cur = await self._conn.execute(
                "INSERT INTO mining_pools "
                "(pool_name, coinbase_tag, payout_address, website, contact_email, "
                " discord, telegram, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data.get("pool_name"),
                    data.get("coinbase_tag"),
                    data.get("payout_address"),
                    data.get("website"),
                    data.get("contact_email"),
                    data.get("discord"),
                    data.get("telegram"),
                    data.get("status", "active"),
                    now,
                    now,
                ),
            )
            await self._conn.commit()
            return cur.lastrowid

        return await self._locked_write(_do)

    async def pool_update(self, pool_id: int, data: dict) -> bool:
        fields = [
            "pool_name", "coinbase_tag", "payout_address", "website",
            "contact_email", "discord", "telegram", "status",
        ]
        sets = []
        params: list = []
        for f in fields:
            if f in data:
                sets.append(f"{f} = ?")
                params.append(data[f])
        if not sets:
            return False
        sets.append("updated_at = ?")
        params.append(time.time())
        params.append(pool_id)

        async def _do():
            cur = await self._conn.execute(
                f"UPDATE mining_pools SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            await self._conn.commit()
            return cur.rowcount > 0

        return await self._locked_write(_do)

    async def pool_get(self, pool_id: int) -> Optional[dict]:
        async with self._conn.execute(
            "SELECT * FROM mining_pools WHERE id = ?", (pool_id,)
        ) as cur:
            row = await cur.fetchone()
            return self._row_to_dict(cur, row)

    async def pool_find_by_payout_address(
        self, payout_address: str, *, exclude_id: Optional[int] = None,
        active_only: bool = True, statuses: Optional[Sequence[str]] = None,
    ) -> Optional[dict]:
        """Find a pool already using ``payout_address`` (optionally restricted to
        active pools, optionally excluding one id). Used to enforce that no two
        active pools share a payout address — otherwise block attribution and
        payouts would be ambiguous.

        ``statuses`` (if given) overrides ``active_only`` and restricts the match
        to the given status set — used by the public apply endpoint to treat both
        ``active`` and ``pending`` as "already taken"."""
        sql = "SELECT * FROM mining_pools WHERE payout_address = ?"
        args: list = [payout_address]
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            sql += f" AND status IN ({placeholders})"
            args.extend(statuses)
        elif active_only:
            sql += " AND status = 'active'"
        if exclude_id is not None:
            sql += " AND id != ?"
            args.append(exclude_id)
        sql += " LIMIT 1"
        async with self._conn.execute(sql, tuple(args)) as cur:
            row = await cur.fetchone()
            return self._row_to_dict(cur, row)

    async def pool_list(self, *, status: Optional[str] = None) -> List[dict]:
        if status:
            sql = "SELECT * FROM mining_pools WHERE status = ? ORDER BY pool_name COLLATE NOCASE"
            args: tuple = (status,)
        else:
            sql = "SELECT * FROM mining_pools ORDER BY pool_name COLLATE NOCASE"
            args = ()
        async with self._conn.execute(sql, args) as cur:
            rows = await cur.fetchall()
            return self._rows_to_dicts(cur, rows)

    # -- snapshots --
    async def snapshot_range_exists(self, start_height: int, end_height: int) -> bool:
        # Failed/rejected snapshots don't count as occupying the range — they can
        # be re-run, so they must not block a fresh attempt at the same heights.
        async with self._conn.execute(
            "SELECT 1 FROM pool_reward_snapshots WHERE start_height = ? AND end_height = ? "
            "AND status NOT IN ('failed', 'rejected') LIMIT 1",
            (start_height, end_height),
        ) as cur:
            return await cur.fetchone() is not None

    async def snapshot_create(self, data: dict) -> int:
        now = time.time()

        async def _do():
            cur = await self._conn.execute(
                "INSERT INTO pool_reward_snapshots "
                "(snapshot_name, start_height, end_height, reward_per_block, "
                " total_blocks_scanned, total_blocks_matched, total_reward, status, "
                " created_at, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data.get("snapshot_name"),
                    int(data.get("start_height")),
                    int(data.get("end_height")),
                    data.get("reward_per_block"),
                    int(data.get("total_blocks_scanned", 0)),
                    int(data.get("total_blocks_matched", 0)),
                    data.get("total_reward", "0.00000000"),
                    data.get("status", "generated"),
                    now,
                    data.get("notes"),
                ),
            )
            await self._conn.commit()
            return cur.lastrowid

        return await self._locked_write(_do)

    async def snapshot_create_guarded(self, data: dict) -> Optional[int]:
        """Insert a snapshot only if no snapshot with the same (start_height,
        end_height) already exists. Returns the new id, or ``None`` if the range
        is already taken.

        The existence check and the insert run inside the same ``_locked_write``
        critical section, so concurrent requests for the same range can never
        both succeed (race-safe duplicate guard without a schema constraint,
        which would otherwise break ``allow_duplicate``).
        """
        now = time.time()

        async def _do():
            async with self._conn.execute(
                "SELECT 1 FROM pool_reward_snapshots WHERE start_height = ? "
                "AND end_height = ? AND status NOT IN ('failed', 'rejected') LIMIT 1",
                (int(data.get("start_height")), int(data.get("end_height"))),
            ) as cur:
                if await cur.fetchone() is not None:
                    return None
            cur = await self._conn.execute(
                "INSERT INTO pool_reward_snapshots "
                "(snapshot_name, start_height, end_height, reward_per_block, "
                " total_blocks_scanned, total_blocks_matched, total_reward, status, "
                " created_at, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data.get("snapshot_name"),
                    int(data.get("start_height")),
                    int(data.get("end_height")),
                    data.get("reward_per_block"),
                    int(data.get("total_blocks_scanned", 0)),
                    int(data.get("total_blocks_matched", 0)),
                    data.get("total_reward", "0.00000000"),
                    data.get("status", "generated"),
                    now,
                    data.get("notes"),
                ),
            )
            await self._conn.commit()
            return cur.lastrowid

        return await self._locked_write(_do)

    async def snapshot_delete(self, snapshot_id: int) -> bool:
        """Delete a snapshot and all of its entries/blocks. Used to roll back a
        partially-persisted snapshot when the run fails mid-write."""
        async def _do():
            await self._conn.execute(
                "DELETE FROM pool_reward_blocks WHERE snapshot_id = ?", (snapshot_id,)
            )
            await self._conn.execute(
                "DELETE FROM pool_reward_snapshot_entries WHERE snapshot_id = ?", (snapshot_id,)
            )
            cur = await self._conn.execute(
                "DELETE FROM pool_reward_snapshots WHERE id = ?", (snapshot_id,)
            )
            await self._conn.commit()
            return cur.rowcount > 0

        return await self._locked_write(_do)

    async def snapshots_delete_by_status(self, statuses: Sequence[str]) -> int:
        """Delete all snapshots whose status is in ``statuses`` plus their
        derived entries/blocks. Returns the number of snapshots removed. Used by
        the admin cleanup endpoint to purge leftover ``draft``/``failed`` rows."""
        statuses = [s for s in statuses if s]
        if not statuses:
            return 0
        ph = ",".join("?" for _ in statuses)

        async def _do():
            async with self._conn.execute(
                f"SELECT id FROM pool_reward_snapshots WHERE status IN ({ph})",
                tuple(statuses),
            ) as cur:
                ids = [r[0] for r in await cur.fetchall()]
            if not ids:
                return 0
            idph = ",".join("?" for _ in ids)
            await self._conn.execute(
                f"DELETE FROM pool_reward_blocks WHERE snapshot_id IN ({idph})", tuple(ids)
            )
            await self._conn.execute(
                f"DELETE FROM pool_reward_snapshot_entries WHERE snapshot_id IN ({idph})", tuple(ids)
            )
            cur = await self._conn.execute(
                f"DELETE FROM pool_reward_snapshots WHERE id IN ({idph})", tuple(ids)
            )
            await self._conn.commit()
            return cur.rowcount

        return await self._locked_write(_do)

    async def address_index_last_height(self) -> int:
        """Highest block height the address/UTXO index has fully processed.

        Coinbase recipients are read from the address index, so a snapshot whose
        end_height exceeds this would undercount — callers must check coverage."""
        async with self._conn.execute(
            "SELECT value FROM kv WHERE key = ?", (Keys.ADDRESS_INDEX_LAST_HEIGHT,)
        ) as cur:
            row = await cur.fetchone()
        return int(row[0]) if row and row[0] is not None else -1

    async def coinbase_reward_scan(
        self, start_height: int, end_height: int
    ) -> tuple[list[dict], list[int]]:
        """Find every coinbase output recipient in ``[start_height, end_height]``.

        Pure local-DB scan — no RPC (this is what avoids the node's nginx 413 on
        a large getblockhash batch). For each height we read the cached block
        JSON for its coinbase txid (``txids[0]``, present even in 'lite' blocks),
        then join those txids to ``address_txs`` (direction 'in', value > 0) to
        enumerate every address credited by each coinbase transaction — miners
        and node operators alike.

        Returns ``(rows, missing_heights)`` where each row is
        ``{height, block_hash, coinbase_txid, address, value}``. ``missing_heights``
        lists heights with no cached block or no extractable coinbase txid, so
        the caller can fail loudly rather than silently undercount."""
        import json as _json

        heights = list(range(start_height, end_height + 1))
        CHUNK = 800

        # 1. height -> block hash (kv: vision:block:height:{h})
        height_to_hash: dict[int, str] = {}
        for i in range(0, len(heights), CHUNK):
            chunk = heights[i:i + CHUNK]
            keys = [Keys.BLOCK_BY_HEIGHT.format(height=h) for h in chunk]
            ph = ",".join("?" for _ in keys)
            async with self._conn.execute(
                f"SELECT key, value FROM kv WHERE key IN ({ph})", tuple(keys)
            ) as cur:
                for k, v in await cur.fetchall():
                    try:
                        h = int(k.rsplit(":", 1)[-1])
                    except ValueError:
                        continue
                    if v:
                        height_to_hash[h] = v
        missing = [h for h in heights if h not in height_to_hash]

        # 2. block hash -> coinbase txid (tx[0]) from the cached block JSON
        hash_to_height = {bh: h for h, bh in height_to_hash.items()}
        hashes = list(hash_to_height.keys())
        cbtxid_to_height: dict[str, int] = {}
        height_to_cbtxid: dict[int, str] = {}
        for i in range(0, len(hashes), CHUNK):
            chunk = hashes[i:i + CHUNK]
            keys = [Keys.BLOCK_BY_HASH.format(hash=bh) for bh in chunk]
            ph = ",".join("?" for _ in keys)
            async with self._conn.execute(
                f"SELECT key, value FROM kv WHERE key IN ({ph})", tuple(keys)
            ) as cur:
                for k, v in await cur.fetchall():
                    bh = k.rsplit(":", 1)[-1]
                    h = hash_to_height.get(bh)
                    if h is None or not v:
                        continue
                    try:
                        txids = _json.loads(v).get("txids") or []
                    except Exception:
                        continue
                    if txids:
                        cb = txids[0]
                        cbtxid_to_height[cb] = h
                        height_to_cbtxid[h] = cb
        # Block JSON present but no extractable coinbase txid → unscannable.
        for h in height_to_hash:
            if h not in height_to_cbtxid:
                missing.append(h)
        missing = sorted(set(missing))

        # 3. coinbase txid -> credited addresses (value>0) from the address index
        rows: list[dict] = []
        cbtxids = list(cbtxid_to_height.keys())
        for i in range(0, len(cbtxids), CHUNK):
            chunk = cbtxids[i:i + CHUNK]
            ph = ",".join("?" for _ in chunk)
            async with self._conn.execute(
                f"SELECT txid, address, value FROM address_txs "
                f"WHERE direction = 'in' AND value > 0 AND txid IN ({ph})",
                tuple(chunk),
            ) as cur:
                for txid, address, value in await cur.fetchall():
                    h = cbtxid_to_height.get(txid)
                    if h is None or not address:
                        continue
                    rows.append({
                        "height": h,
                        "block_hash": height_to_hash.get(h),
                        "coinbase_txid": txid,
                        "address": address,
                        "value": int(value),
                    })
        return rows, missing

    async def snapshot_get(self, snapshot_id: int) -> Optional[dict]:
        async with self._conn.execute(
            "SELECT * FROM pool_reward_snapshots WHERE id = ?", (snapshot_id,)
        ) as cur:
            row = await cur.fetchone()
            return self._row_to_dict(cur, row)

    async def snapshot_list(self, *, limit: int = 100, offset: int = 0) -> List[dict]:
        async with self._conn.execute(
            "SELECT * FROM pool_reward_snapshots ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ) as cur:
            rows = await cur.fetchall()
            return self._rows_to_dicts(cur, rows)

    async def snapshot_set_totals(
        self, snapshot_id: int, *, total_blocks_scanned: int,
        total_blocks_matched: int, total_reward: str,
    ) -> bool:
        async def _do():
            cur = await self._conn.execute(
                "UPDATE pool_reward_snapshots SET total_blocks_scanned = ?, "
                "total_blocks_matched = ?, total_reward = ? WHERE id = ?",
                (total_blocks_scanned, total_blocks_matched, total_reward, snapshot_id),
            )
            await self._conn.commit()
            return cur.rowcount > 0

        return await self._locked_write(_do)

    async def snapshot_set_status(self, snapshot_id: int, status: str) -> bool:
        now = time.time()
        ts_col = {"approved": "approved_at", "paid": "paid_at"}.get(status)

        async def _do():
            if ts_col:
                cur = await self._conn.execute(
                    f"UPDATE pool_reward_snapshots SET status = ?, {ts_col} = ? WHERE id = ?",
                    (status, now, snapshot_id),
                )
            else:
                cur = await self._conn.execute(
                    "UPDATE pool_reward_snapshots SET status = ? WHERE id = ?",
                    (status, snapshot_id),
                )
            await self._conn.commit()
            return cur.rowcount > 0

        return await self._locked_write(_do)

    async def snapshot_mark_failed(self, snapshot_id: int, error: str) -> bool:
        """Mark a still-``draft`` snapshot ``failed`` and store the error in
        ``notes``. Guarded on ``status = 'draft'`` so a background scan never
        clobbers a status an admin may have set in the meantime."""
        async def _do():
            cur = await self._conn.execute(
                "UPDATE pool_reward_snapshots SET status = 'failed', notes = ? "
                "WHERE id = ? AND status = 'draft'",
                (error, snapshot_id),
            )
            await self._conn.commit()
            return cur.rowcount > 0

        return await self._locked_write(_do)

    async def snapshot_finalize(
        self, snapshot_id: int, *, total_blocks_scanned: int,
        total_blocks_matched: int, total_reward: str,
    ) -> bool:
        """Atomically write totals and flip ``draft → generated`` in one commit.

        Compare-and-set on ``status = 'draft'``: returns ``False`` (without
        touching the row) if the snapshot is no longer a draft, so a concurrent
        admin status change is never silently overwritten."""
        async def _do():
            cur = await self._conn.execute(
                "UPDATE pool_reward_snapshots SET total_blocks_scanned = ?, "
                "total_blocks_matched = ?, total_reward = ?, status = 'generated' "
                "WHERE id = ? AND status = 'draft'",
                (total_blocks_scanned, total_blocks_matched, total_reward, snapshot_id),
            )
            await self._conn.commit()
            return cur.rowcount > 0

        return await self._locked_write(_do)

    async def snapshot_clear_results(self, snapshot_id: int) -> None:
        """Delete a snapshot's entries + per-block rows (keeps the snapshot row).

        Used to scrub partial writes before marking a failed scan, so a failed
        snapshot never carries half-written entries/blocks."""
        async def _do():
            await self._conn.execute(
                "DELETE FROM pool_reward_blocks WHERE snapshot_id = ?", (snapshot_id,)
            )
            await self._conn.execute(
                "DELETE FROM pool_reward_snapshot_entries WHERE snapshot_id = ?", (snapshot_id,)
            )
            await self._conn.commit()
            return True

        return await self._locked_write(_do)

    # -- entries --
    async def entries_insert_batch(self, rows: Sequence[dict]) -> None:
        if not rows:
            return
        now = time.time()
        payload = [
            (
                r["snapshot_id"], r.get("pool_id"), r["pool_name"], r.get("payout_address"),
                int(r["blocks_found"]), r["reward_per_block"], r["total_reward"],
                r.get("status", "pending"), r.get("txid"), now, now,
            )
            for r in rows
        ]

        async def _do():
            await self._conn.executemany(
                "INSERT INTO pool_reward_snapshot_entries "
                "(snapshot_id, pool_id, pool_name, payout_address, blocks_found, "
                " reward_per_block, total_reward, status, txid, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                payload,
            )
            await self._conn.commit()

        await self._locked_write(_do)

    async def entries_list(self, snapshot_id: int) -> List[dict]:
        async with self._conn.execute(
            "SELECT * FROM pool_reward_snapshot_entries WHERE snapshot_id = ? "
            "ORDER BY blocks_found DESC, pool_name COLLATE NOCASE",
            (snapshot_id,),
        ) as cur:
            rows = await cur.fetchall()
            return self._rows_to_dicts(cur, rows)

    async def entry_get(self, entry_id: int) -> Optional[dict]:
        async with self._conn.execute(
            "SELECT * FROM pool_reward_snapshot_entries WHERE id = ?", (entry_id,)
        ) as cur:
            row = await cur.fetchone()
            return self._row_to_dict(cur, row)

    async def entry_update(self, entry_id: int, *, status: Optional[str] = None, txid: Optional[str] = None) -> bool:
        sets = []
        params: list = []
        if status is not None:
            sets.append("status = ?")
            params.append(status)
        if txid is not None:
            sets.append("txid = ?")
            params.append(txid)
        if not sets:
            return False
        sets.append("updated_at = ?")
        params.append(time.time())
        params.append(entry_id)

        async def _do():
            cur = await self._conn.execute(
                f"UPDATE pool_reward_snapshot_entries SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            await self._conn.commit()
            return cur.rowcount > 0

        return await self._locked_write(_do)

    async def entries_set_status_all(self, snapshot_id: int, status: str) -> int:
        async def _do():
            cur = await self._conn.execute(
                "UPDATE pool_reward_snapshot_entries SET status = ?, updated_at = ? WHERE snapshot_id = ?",
                (status, time.time(), snapshot_id),
            )
            await self._conn.commit()
            return cur.rowcount

        return await self._locked_write(_do)

    # -- matched blocks --
    async def reward_blocks_insert_batch(self, rows: Sequence[dict]) -> None:
        if not rows:
            return
        now = time.time()
        payload = [
            (
                r["snapshot_id"], r.get("pool_id"), int(r["block_height"]), r["block_hash"],
                r.get("coinbase_tag_detected"), r.get("payout_address"),
                r["reward_amount"], now,
            )
            for r in rows
        ]

        async def _do():
            await self._conn.executemany(
                "INSERT OR IGNORE INTO pool_reward_blocks "
                "(snapshot_id, pool_id, block_height, block_hash, "
                " coinbase_tag_detected, payout_address, reward_amount, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                payload,
            )
            await self._conn.commit()

        await self._locked_write(_do)

    async def reward_blocks_list(self, snapshot_id: int, *, pool_id: Optional[int] = None) -> List[dict]:
        if pool_id is not None:
            sql = "SELECT * FROM pool_reward_blocks WHERE snapshot_id = ? AND pool_id = ? ORDER BY block_height"
            args: tuple = (snapshot_id, pool_id)
        else:
            sql = "SELECT * FROM pool_reward_blocks WHERE snapshot_id = ? ORDER BY block_height"
            args = (snapshot_id,)
        async with self._conn.execute(sql, args) as cur:
            rows = await cur.fetchall()
            return self._rows_to_dicts(cur, rows)

    # ── misc ─────────────────────────────────────────────────────────────

    async def ping(self) -> bool:
        async with self._conn.execute("SELECT 1") as cur:
            await cur.fetchone()
        return True

    async def publish(self, channel: str, message: str) -> int:
        """No-op — cross-instance fanout isn't needed with SQLite."""
        return 0

    async def aclose(self) -> None:
        try:
            await self._conn.close()
        except Exception:
            pass


# ── singleton ────────────────────────────────────────────────────────────

async def _migrate_pool_reward_tables(conn: aiosqlite.Connection) -> None:
    """Drop legacy pool-reward entry/block tables so the new address-centric
    shape can be recreated.

    The original tables were keyed by ``pool_id`` (entries
    ``UNIQUE(snapshot_id, pool_id)`` and blocks ``UNIQUE(snapshot_id,
    block_height)``). The reward program now credits *any* coinbase recipient
    (miner or node operator), keyed by payout address, and a single block can
    pay multiple recipients — so both unique constraints changed. These tables
    hold only derived data (rebuilt by re-running a snapshot), so dropping a
    legacy table is safe. Detection is by table DDL signature, so this is a
    no-op once migrated (idempotent)."""
    async def _table_sql(name: str) -> str:
        async with conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ) as cur:
            row = await cur.fetchone()
        return " ".join((row[0] or "").lower().split()) if row else ""

    entries_sql = await _table_sql("pool_reward_snapshot_entries")
    if entries_sql and "unique (snapshot_id, payout_address)" not in entries_sql:
        logger.warning("Migrating legacy pool_reward_snapshot_entries → address-centric shape (dropping derived rows)")
        await conn.execute("DROP TABLE pool_reward_snapshot_entries")

    blocks_sql = await _table_sql("pool_reward_blocks")
    if blocks_sql and "unique (snapshot_id, block_height, payout_address)" not in blocks_sql:
        logger.warning("Migrating legacy pool_reward_blocks → per-recipient shape (dropping derived rows)")
        await conn.execute("DROP TABLE pool_reward_blocks")


async def _ensure_db() -> aiosqlite.Connection:
    _assert_sqlite_enabled()
    global _db
    if _db is not None:
        return _db

    async with _lock:
        if _db is not None:
            return _db

        db_path = Path(settings.SQLITE_DB_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Opening SQLite store at %s", db_path)
        # isolation_level=None  → autocommit. We manage transactions explicitly
        # in AddressIndexWriter; for KV writes each statement commits on its
        # own. This avoids Python's legacy DEFERRED wrapper that silently
        # opens a BEGIN before each DML statement and can defeat busy_timeout.
        conn = await aiosqlite.connect(
            str(db_path), timeout=60, isolation_level=None
        )
        # Set busy_timeout FIRST so every subsequent statement (including the
        # WAL switch and the schema apply) respects it.
        await conn.execute("PRAGMA busy_timeout=60000")  # wait up to 60s
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA cache_size=-8000")  # 8 MB
        await conn.execute("PRAGMA wal_autocheckpoint=1000")
        await conn.executescript(_SCHEMA)
        # Pool-reward entries/blocks are derived data with an evolving shape.
        # Migrate any old (pool_id-keyed) tables, then (re)create the current
        # address-centric shape. Order matters: drop-old BEFORE create-new so a
        # stale table doesn't satisfy CREATE ... IF NOT EXISTS.
        await _migrate_pool_reward_tables(conn)
        await conn.executescript(_ENTRIES_DDL)
        await conn.executescript(_BLOCKS_DDL)
        # Sanity-check that busy_timeout actually stuck.
        async with conn.execute("PRAGMA busy_timeout") as cur:
            row = await cur.fetchone()
        logger.info("SQLite shared connection busy_timeout=%sms", row[0] if row else "?")
        _db = conn
    return _db


_store_override = None


def set_store_override(store) -> None:
    """Replace get_db() with a custom store (e.g. DualStore).

    Called by main.py when NEDB_URL is configured. All Vision code that
    calls get_db() will receive the override without any import changes.
    Pass None to restore the default SQLiteStore behaviour.
    """
    global _store_override
    _store_override = store


def get_db() -> SQLiteStore:
    """Return the active store singleton.

    Returns the DualStore when NEDB_URL is configured (set via
    set_store_override), otherwise returns a lazy SQLiteStore proxy.
    """
    if _store_override is not None:
        return _store_override
    return _LazyStore()


class _LazyStore(SQLiteStore):
    """Subclass that lazily opens the DB connection on first method call."""

    def __init__(self):
        # Don't call super().__init__ — we'll set _conn later.
        self.__conn: Optional[aiosqlite.Connection] = None

    @property
    def _conn(self):  # type: ignore[override]
        if self.__conn is None:
            raise RuntimeError("DB not initialised — call await init_db() first")
        return self.__conn

    @_conn.setter
    def _conn(self, v):
        self.__conn = v

    async def _ensure(self):
        if self.__conn is None:
            self.__conn = await _ensure_db()

    # Override every public method to auto-init.
    async def get(self, key):
        await self._ensure()
        return await super().get(key)

    async def set(self, key, value, ex=None):
        await self._ensure()
        return await super().set(key, value, ex=ex)

    async def delete(self, *keys):
        await self._ensure()
        return await super().delete(*keys)

    async def incr(self, key):
        await self._ensure()
        return await super().incr(key)

    async def expire(self, key, seconds):
        await self._ensure()
        return await super().expire(key, seconds)

    async def zadd(self, name, mapping):
        await self._ensure()
        return await super().zadd(name, mapping)

    async def zrevrange(self, name, start, stop, withscores=False):
        await self._ensure()
        return await super().zrevrange(name, start, stop, withscores=withscores)

    async def zremrangebyrank(self, name, start, stop):
        await self._ensure()
        return await super().zremrangebyrank(name, start, stop)

    async def sadd(self, name, *members):
        await self._ensure()
        return await super().sadd(name, *members)

    async def smembers(self, name):
        await self._ensure()
        return await super().smembers(name)

    async def srem(self, name, *members):
        await self._ensure()
        return await super().srem(name, *members)

    # ── address index proxies ────────────────────────────────────────────
    async def utxo_add_batch(self, rows):
        await self._ensure()
        return await super().utxo_add_batch(rows)

    async def utxo_spend_batch(self, outpoints):
        await self._ensure()
        return await super().utxo_spend_batch(outpoints)

    async def address_tx_add_batch(self, rows):
        await self._ensure()
        return await super().address_tx_add_batch(rows)

    async def commit(self):
        await self._ensure()
        return await super().commit()

    async def address_balance(self, address):
        await self._ensure()
        return await super().address_balance(address)

    async def address_tx_count(self, address):
        await self._ensure()
        return await super().address_tx_count(address)

    async def address_first_last_height(self, address):
        await self._ensure()
        return await super().address_first_last_height(address)

    async def address_txs(self, address, limit=50, offset=0):
        await self._ensure()
        return await super().address_txs(address, limit=limit, offset=offset)

    async def address_utxos(self, address):
        await self._ensure()
        return await super().address_utxos(address)

    async def address_index_rollback(self, from_height):
        await self._ensure()
        return await super().address_index_rollback(from_height)

    async def ping(self):
        await self._ensure()
        return await super().ping()

    # ── pool reward proxies ──────────────────────────────────────────────
    async def pool_create(self, data):
        await self._ensure()
        return await super().pool_create(data)

    async def pool_update(self, pool_id, data):
        await self._ensure()
        return await super().pool_update(pool_id, data)

    async def pool_get(self, pool_id):
        await self._ensure()
        return await super().pool_get(pool_id)

    async def pool_find_by_payout_address(self, payout_address, *, exclude_id=None, active_only=True, statuses=None):
        await self._ensure()
        return await super().pool_find_by_payout_address(
            payout_address, exclude_id=exclude_id, active_only=active_only, statuses=statuses,
        )

    async def pool_list(self, *, status=None):
        await self._ensure()
        return await super().pool_list(status=status)

    async def snapshot_range_exists(self, start_height, end_height):
        await self._ensure()
        return await super().snapshot_range_exists(start_height, end_height)

    async def snapshot_create(self, data):
        await self._ensure()
        return await super().snapshot_create(data)

    async def snapshot_create_guarded(self, data):
        await self._ensure()
        return await super().snapshot_create_guarded(data)

    async def snapshot_delete(self, snapshot_id):
        await self._ensure()
        return await super().snapshot_delete(snapshot_id)

    async def snapshots_delete_by_status(self, statuses):
        await self._ensure()
        return await super().snapshots_delete_by_status(statuses)

    async def address_index_last_height(self):
        await self._ensure()
        return await super().address_index_last_height()

    async def coinbase_reward_scan(self, start_height, end_height):
        await self._ensure()
        return await super().coinbase_reward_scan(start_height, end_height)

    async def snapshot_get(self, snapshot_id):
        await self._ensure()
        return await super().snapshot_get(snapshot_id)

    async def snapshot_list(self, *, limit=100, offset=0):
        await self._ensure()
        return await super().snapshot_list(limit=limit, offset=offset)

    async def snapshot_set_totals(self, snapshot_id, *, total_blocks_scanned, total_blocks_matched, total_reward):
        await self._ensure()
        return await super().snapshot_set_totals(
            snapshot_id, total_blocks_scanned=total_blocks_scanned,
            total_blocks_matched=total_blocks_matched, total_reward=total_reward,
        )

    async def snapshot_set_status(self, snapshot_id, status):
        await self._ensure()
        return await super().snapshot_set_status(snapshot_id, status)

    async def snapshot_mark_failed(self, snapshot_id, error):
        await self._ensure()
        return await super().snapshot_mark_failed(snapshot_id, error)

    async def snapshot_finalize(self, snapshot_id, *, total_blocks_scanned, total_blocks_matched, total_reward):
        await self._ensure()
        return await super().snapshot_finalize(
            snapshot_id, total_blocks_scanned=total_blocks_scanned,
            total_blocks_matched=total_blocks_matched, total_reward=total_reward,
        )

    async def snapshot_clear_results(self, snapshot_id):
        await self._ensure()
        return await super().snapshot_clear_results(snapshot_id)

    async def entries_insert_batch(self, rows):
        await self._ensure()
        return await super().entries_insert_batch(rows)

    async def entries_list(self, snapshot_id):
        await self._ensure()
        return await super().entries_list(snapshot_id)

    async def entry_get(self, entry_id):
        await self._ensure()
        return await super().entry_get(entry_id)

    async def entry_update(self, entry_id, *, status=None, txid=None):
        await self._ensure()
        return await super().entry_update(entry_id, status=status, txid=txid)

    async def entries_set_status_all(self, snapshot_id, status):
        await self._ensure()
        return await super().entries_set_status_all(snapshot_id, status)

    async def reward_blocks_insert_batch(self, rows):
        await self._ensure()
        return await super().reward_blocks_insert_batch(rows)

    async def reward_blocks_list(self, snapshot_id, *, pool_id=None):
        await self._ensure()
        return await super().reward_blocks_list(snapshot_id, pool_id=pool_id)

    async def publish(self, channel, message):
        return 0  # no-op

    async def aclose(self):
        if self.__conn is not None:
            await self.__conn.close()
            self.__conn = None


async def init_db() -> SQLiteStore:
    """Explicitly open the database (call during lifespan startup)."""
    conn = await _ensure_db()
    return SQLiteStore(conn)


async def close_db() -> None:
    """Close the database (call during lifespan shutdown)."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None


# ── Address-index writer ────────────────────────────────────────────────
#
# The address indexer needs strict per-block atomicity (UTXO writes,
# address_tx writes, the per-height block-hash record AND the
# last_height pointer all commit or roll back together). Because the
# rest of the app shares a single aiosqlite connection on which many
# code paths call ``commit()`` for their own writes, a coroutine yield
# inside a multi-step write batch could let an unrelated commit fire
# halfway through and persist a partial block.
#
# To make per-block atomicity unconditionally safe we give the address
# indexer its OWN aiosqlite connection. Reads from the rest of the app
# still go through the shared connection — WAL mode allows them to
# proceed concurrently with the writer's transaction.

_writer: Optional["AddressIndexWriter"] = None
_writer_lock = asyncio.Lock()


class AddressIndexWriter:
    """Owns a dedicated aiosqlite connection for atomic address-index writes."""

    # Last-height kv key (mirrors Keys.ADDRESS_INDEX_LAST_HEIGHT but kept
    # here as a literal so this module has no upward import).
    _LAST_HEIGHT_KEY = "vision:address_index:last_height"

    def __init__(self, conn: aiosqlite.Connection):
        _assert_sqlite_enabled()
        self._conn = conn
        # Serialize concurrent callers (e.g. backfill batch + tip handler).
        self._lock = asyncio.Lock()

    async def get_last_height(self) -> int:
        async with self._conn.execute(
            "SELECT value FROM kv WHERE key = ?", (self._LAST_HEIGHT_KEY,)
        ) as cur:
            row = await cur.fetchone()
        return int(row[0]) if row and row[0] is not None else -1

    async def get_indexed_hashes(
        self, heights: Sequence[int]
    ) -> dict:
        """Return {height: hash} for the requested heights (missing → absent)."""
        if not heights:
            return {}
        placeholders = ",".join("?" for _ in heights)
        async with self._conn.execute(
            f"SELECT height, hash FROM address_index_blocks WHERE height IN ({placeholders})",
            tuple(heights),
        ) as cur:
            rows = await cur.fetchall()
        return {int(r[0]): r[1] for r in rows}

    async def apply_block(
        self,
        height: int,
        block_hash: str,
        utxo_inserts: Sequence[Tuple[str, int, str, int, int]],
        spend_outpoints: Sequence[Tuple[str, int]],
        in_rows: Sequence[Tuple[str, str, int, str, int, Optional[int]]],
        spend_owner: dict,
        block_time: Optional[int],
    ) -> None:
        """Apply one block atomically.

        ``spend_owner`` maps (prev_txid, prev_vout) → spending txid for this block.
        Resolution of spent UTXOs (lookup + delete) happens *inside* the
        transaction so a self-spend within this same block resolves correctly,
        and so the read-and-delete pair is itself atomic.
        """
        async with self._lock:
            async with _get_write_lock():
                try:
                    await self._conn.execute("BEGIN IMMEDIATE")

                    # 1. Insert outputs first so any in-block self-spend can resolve.
                    if utxo_inserts:
                        await self._conn.executemany(
                            "INSERT OR REPLACE INTO utxos "
                            "(txid, vout, address, value, height) VALUES (?, ?, ?, ?, ?)",
                            utxo_inserts,
                        )

                    # 2. Resolve & delete spent UTXOs, building 'out' history rows.
                    #    Also record an undo entry for each spent UTXO so a future
                    #    reorg can restore the canonical UTXO set.
                    out_rows: list = []
                    if spend_outpoints:
                        placeholders = ",".join("(?,?)" for _ in spend_outpoints)
                        params: list = []
                        for tx, vo in spend_outpoints:
                            params.extend((tx, vo))
                        async with self._conn.execute(
                            f"SELECT txid, vout, address, value, height FROM utxos "
                            f"WHERE (txid, vout) IN (VALUES {placeholders})",
                            params,
                        ) as cur:
                            spent = await cur.fetchall()
                        if spent:
                            await self._conn.executemany(
                                "DELETE FROM utxos WHERE txid = ? AND vout = ?",
                                [(r[0], r[1]) for r in spent],
                            )
                            await self._conn.executemany(
                                "INSERT OR REPLACE INTO utxo_undo "
                                "(height, txid, vout, address, value, orig_height) "
                                "VALUES (?, ?, ?, ?, ?, ?)",
                                [
                                    (height, r[0], int(r[1]), r[2], int(r[3]), int(r[4]))
                                    for r in spent
                                ],
                            )
                        for prev_txid, prev_vout, addr, value_sats, _orig_h in spent:
                            spending_txid = spend_owner.get((prev_txid, int(prev_vout)))
                            if not spending_txid:
                                continue
                            out_rows.append(
                                (addr, spending_txid, height, "out", int(value_sats), block_time)
                            )

                    # 3. Insert all address_tx rows ('in' from caller, 'out' built above).
                    all_rows = list(in_rows) + out_rows
                    if all_rows:
                        await self._conn.executemany(
                            "INSERT OR REPLACE INTO address_txs "
                            "(address, txid, height, direction, value, block_time) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            all_rows,
                        )

                    # 4. Record the per-height block hash for reorg detection.
                    await self._conn.execute(
                        "INSERT OR REPLACE INTO address_index_blocks (height, hash) VALUES (?, ?)",
                        (height, block_hash),
                    )

                    # 5. Bump the last-height pointer — same transaction.
                    await self._conn.execute(
                        "INSERT OR REPLACE INTO kv (key, value, expires_at) VALUES (?, ?, NULL)",
                        (self._LAST_HEIGHT_KEY, str(height)),
                    )

                    await self._conn.commit()
                except Exception:
                    try:
                        await self._conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

    async def checkpoint(self) -> None:
        """Run a passive WAL checkpoint to keep the WAL file from growing unbounded."""
        try:
            await self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception:
            pass

    async def rollback_to(self, height: int) -> None:
        """Reorg-safe rollback to the given height.

        Restores the canonical UTXO set by:

        1. Deleting UTXOs created at heights > N (orphaned outputs).
        2. Re-inserting UTXOs from the undo log whose ORIGINAL creation
           height is <= N (outputs that existed before the reorg and were
           spent in the now-orphaned blocks).
        3. Pruning the undo log, address_txs, and per-height hash records
           for everything > N.
        4. Rewinding the last_height pointer.

        All steps run in a single transaction. If anything fails, the DB
        is left in its pre-rollback state.
        """
        async with self._lock:
            async with _get_write_lock():
                try:
                    await self._conn.execute("BEGIN IMMEDIATE")

                    # 1. Drop UTXOs created in orphaned blocks.
                    await self._conn.execute(
                        "DELETE FROM utxos WHERE height > ?", (height,)
                    )

                    # 2. Restore UTXOs that existed before the reorg and were
                    #    spent in now-orphaned blocks.
                    await self._conn.execute(
                        """
                        INSERT OR REPLACE INTO utxos (txid, vout, address, value, height)
                        SELECT txid, vout, address, value, orig_height
                          FROM utxo_undo
                         WHERE height > ? AND orig_height <= ?
                        """,
                        (height, height),
                    )

                    # 3. Prune everything strictly above N.
                    await self._conn.execute(
                        "DELETE FROM utxo_undo WHERE height > ?", (height,)
                    )
                    await self._conn.execute(
                        "DELETE FROM address_txs WHERE height > ?", (height,)
                    )
                    await self._conn.execute(
                        "DELETE FROM address_index_blocks WHERE height > ?", (height,)
                    )

                    # 4. Rewind pointer.
                    await self._conn.execute(
                        "INSERT OR REPLACE INTO kv (key, value, expires_at) VALUES (?, ?, NULL)",
                        (self._LAST_HEIGHT_KEY, str(height)),
                    )
                    await self._conn.commit()
                except Exception:
                    try:
                        await self._conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

    async def aclose(self) -> None:
        try:
            await self._conn.close()
        except Exception:
            pass


async def init_address_index_writer() -> AddressIndexWriter:
    """Open the dedicated address-index writer connection."""
    global _writer
    async with _writer_lock:
        if _writer is not None:
            return _writer
        # Make sure the schema is in place — opens the shared conn if needed.
        await _ensure_db()
        db_path = Path(settings.SQLITE_DB_PATH)
        # isolation_level=None → autocommit; apply_block / rollback_to issue
        # their own explicit BEGIN IMMEDIATE…COMMIT. This is REQUIRED for
        # busy_timeout to work reliably on this connection.
        conn = await aiosqlite.connect(
            str(db_path), timeout=60, isolation_level=None
        )
        await conn.execute("PRAGMA busy_timeout=60000")  # wait up to 60s
        await conn.execute("PRAGMA synchronous=NORMAL")
        async with conn.execute("PRAGMA busy_timeout") as cur:
            row = await cur.fetchone()
        logger.info("SQLite writer connection busy_timeout=%sms", row[0] if row else "?")
        _writer = AddressIndexWriter(conn)
    return _writer


def get_address_index_writer() -> AddressIndexWriter:
    if _writer is None:
        raise RuntimeError(
            "Address-index writer not initialised — "
            "call await init_address_index_writer() first"
        )
    return _writer


async def close_address_index_writer() -> None:
    global _writer
    if _writer is not None:
        await _writer.aclose()
        _writer = None
