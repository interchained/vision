"""embedded_domain.py — Vision's domain layer on the embedded NEDB engine.

Implements the full domain surface that used to live on SQLite
(``sqlite_store.SQLiteStore`` lines 472–1207: UTXO ledger, per-address
tx history, mining pools, reward snapshots/entries/blocks) plus the
address-index writer (the old dedicated-SQLite-connection
``AddressIndexWriter``), entirely on the in-process NEDB engine.

Data model — one engine collection per SQLite table, one doc per row,
``_id`` derived from the table's primary key:

===========  ====================  =====================================
collection   _id                   source table
===========  ====================  =====================================
utxo         ``{txid}:{vout}``     utxos
addr_tx      ``{addr}::{txid}::{direction}``  address_txs
utxo_undo    ``{height}:{txid}:{vout}``       utxo_undo
addr_block   ``{height}``          address_index_blocks
pool         ``{id}``              mining_pools
snapshot     ``{id}``              pool_reward_snapshots
entry        ``{id}``              pool_reward_snapshot_entries
reward_block ``{id}``              pool_reward_blocks
===========  ====================  =====================================

Auto-increment ids come from KV counters (``vision:seq:*``), allocated
inside the same storage-thread job as the insert, so allocation +
insert are atomic in practice (the engine facade serialises every
call through a single-worker executor — see embedded_store.py).

Semantics mirror the SQLite implementation exactly, including its
warts: ``sqlite3.IntegrityError`` is raised for the two uniqueness
violations SQLite enforced (one ACTIVE pool per payout address;
entries UNIQUE(snapshot_id, payout_address)) because admin routes
catch that exact type, and reward-block inserts keep SQL's
INSERT OR IGNORE behaviour (NULL payout addresses never collide).

All multi-step writes run as a single ``bulk()`` job on the storage
thread: check-then-write sequences (guarded snapshot create, pool
uniqueness) cannot race, and the batch lands under one fsync — the
same durability the SQLite version got from its per-call COMMIT.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from typing import Any, List, Optional, Sequence, Tuple

from .sqlite_store import Keys

logger = logging.getLogger(__name__)

# ── collections ──────────────────────────────────────────────────────

COLL_UTXO = "utxo"
COLL_ADDR_TX = "addr_tx"
COLL_UNDO = "utxo_undo"
COLL_ADDR_BLOCK = "addr_block"
COLL_POOL = "pool"
COLL_SNAPSHOT = "snapshot"
COLL_ENTRY = "entry"
COLL_REWARD_BLOCK = "reward_block"

# KV counters for id allocation (SQLite AUTOINCREMENT equivalents).
SEQ_POOL = "vision:seq:pool_id"
SEQ_SNAPSHOT = "vision:seq:snapshot_id"
SEQ_ENTRY = "vision:seq:entry_id"
SEQ_REWARD_BLOCK = "vision:seq:reward_block_id"

_BIG = 999999999  # "no practical limit" for NQL LIMIT clauses

_META_KEYS = ("_seq", "_hash", "_coll")

# Column sets — the exact dict keys the SQLite version returns.
POOL_FIELDS = (
    "id", "pool_name", "coinbase_tag", "payout_address", "website",
    "contact_email", "discord", "telegram", "status",
    "created_at", "updated_at",
)
SNAPSHOT_FIELDS = (
    "id", "snapshot_name", "start_height", "end_height",
    "reward_per_block", "total_blocks_scanned", "total_blocks_matched",
    "total_reward", "status", "created_at", "approved_at", "paid_at",
    "notes",
)
ENTRY_FIELDS = (
    "id", "snapshot_id", "pool_id", "pool_name", "payout_address",
    "blocks_found", "reward_per_block", "total_reward", "status",
    "txid", "created_at", "updated_at",
)
REWARD_BLOCK_FIELDS = (
    "id", "snapshot_id", "pool_id", "block_height", "block_hash",
    "coinbase_tag_detected", "payout_address", "reward_amount",
    "created_at",
)


def _esc(s: str) -> str:
    """Escape a string for an NQL double-quoted literal."""
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


def _clean(row: Optional[dict]) -> Optional[dict]:
    """Strip engine metadata keys from a returned doc."""
    if row is None:
        return None
    return {k: v for k, v in row.items() if k not in _META_KEYS}


def _fmt_value(v: Any) -> str:
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return repr(v)
    return f'"{_esc(v)}"'


def _qsync(db, coll: str, clauses: Sequence[Tuple[str, str, Any]] = (),
           order: Optional[Tuple[str, str]] = None,
           limit: int = _BIG, offset: int = 0) -> List[dict]:
    """Synchronous NQL query for use inside storage-thread jobs.

    ``clauses`` are (field, op, value) triples ANDed together; field
    names and ops are internal constants at every call site (never
    user input), values are escaped here.
    """
    nql = f"FROM {coll}"
    if clauses:
        nql += " WHERE " + " AND ".join(
            f"{f} {op} {_fmt_value(v)}" for f, op, v in clauses
        )
    if order is not None:
        nql += f" ORDER BY {order[0]} {order[1]}"
    nql += f" LIMIT {int(limit)}"
    if offset:
        nql += f" OFFSET {int(offset)}"
    rows = db.query(nql) or []
    return [_clean(r) for r in rows]


def _alloc_id(db, counter_key: str) -> int:
    """Next id from a KV counter — call only inside a storage job."""
    row = db.get("kv", counter_key)
    cur = 0
    if row and row.get("value") is not None:
        try:
            cur = int(row["value"])
        except (TypeError, ValueError):
            cur = 0
    new = cur + 1
    db.put("kv", counter_key,
           {"_id": counter_key, "value": str(new), "expires_at": None})
    return new


# ── doc builders (shared with the offline domain migrator) ───────────

def utxo_doc(txid: str, vout: int, address: str, value: int,
             height: int) -> dict:
    return {
        "_id": f"{txid}:{vout}", "txid": txid, "vout": int(vout),
        "address": address, "value": int(value), "height": int(height),
    }


def addr_tx_doc(address: str, txid: str, height: int, direction: str,
                value: int, block_time: Optional[int]) -> dict:
    return {
        "_id": f"{address}::{txid}::{direction}",
        "address": address, "txid": txid, "height": int(height),
        "direction": direction, "value": int(value),
        "block_time": block_time,
    }


def undo_doc(height: int, txid: str, vout: int, address: str,
             value: int, orig_height: int) -> dict:
    return {
        "_id": f"{height}:{txid}:{vout}", "height": int(height),
        "txid": txid, "vout": int(vout), "address": address,
        "value": int(value), "orig_height": int(orig_height),
    }


def addr_block_doc(height: int, block_hash: str) -> dict:
    return {"_id": str(height), "height": int(height), "hash": block_hash}


def _project(doc: dict, fields: Sequence[str]) -> dict:
    return {f: doc.get(f) for f in fields}


def pool_doc(row: dict) -> dict:
    doc = _project(row, POOL_FIELDS)
    doc["_id"] = str(doc["id"])
    return doc


def snapshot_doc(row: dict) -> dict:
    doc = _project(row, SNAPSHOT_FIELDS)
    doc["_id"] = str(doc["id"])
    return doc


def entry_doc(row: dict) -> dict:
    doc = _project(row, ENTRY_FIELDS)
    doc["_id"] = str(doc["id"])
    return doc


def reward_block_doc(row: dict) -> dict:
    doc = _project(row, REWARD_BLOCK_FIELDS)
    doc["_id"] = str(doc["id"])
    return doc


# ── the domain mixin (methods land on EmbeddedNedbStore) ─────────────

class EmbeddedDomainMixin:
    """Domain methods for EmbeddedNedbStore.

    Duck-typed on the host class: uses ``self._run``, ``self.bulk``,
    ``self.get``/``self.set`` (the KV facade) — all provided by
    EmbeddedNedbStore in embedded_store.py.
    """

    # ── helpers ──────────────────────────────────────────────────────

    async def _query(self, coll, clauses=(), order=None,
                     limit=_BIG, offset=0) -> List[dict]:
        db = self._engine()
        return await self._run(
            lambda: _qsync(db, coll, clauses, order, limit, offset))

    # ── address index: UTXO ledger ───────────────────────────────────

    async def utxo_add_batch(
        self, rows: Sequence[Tuple[str, int, str, int, int]]
    ) -> None:
        """Bulk-insert UTXOs. ``rows`` items: (txid, vout, address, value, height)."""
        if not rows:
            return

        def _job(db):
            for txid, vout, address, value, height in rows:
                doc = utxo_doc(txid, vout, address, value, height)
                db.put(COLL_UTXO, doc["_id"], doc)

        await self.bulk(_job)

    async def utxo_spend_batch(
        self, outpoints: Sequence[Tuple[str, int]]
    ) -> List[Tuple[str, int, str, int, int]]:
        """Resolve and remove UTXOs by (txid, vout). Returns the rows that
        were deleted (in input order) so callers know which addresses &
        values to credit on the spend side."""
        if not outpoints:
            return []

        def _job(db):
            found = []
            for txid, vout in outpoints:
                row = db.get(COLL_UTXO, f"{txid}:{vout}")
                if row is not None:
                    db.delete(COLL_UTXO, row["_id"])
                    found.append((
                        row["txid"], int(row["vout"]), row["address"],
                        int(row["value"]), int(row["height"]),
                    ))
            return found

        return await self.bulk(_job)

    async def address_tx_add_batch(
        self,
        rows: Sequence[Tuple[str, str, int, str, int, Optional[int]]],
    ) -> None:
        """Bulk-insert address-tx history. items: (address, txid, height, direction, value, block_time)."""
        if not rows:
            return

        def _job(db):
            for address, txid, height, direction, value, block_time in rows:
                doc = addr_tx_doc(address, txid, height, direction,
                                  value, block_time)
                db.put(COLL_ADDR_TX, doc["_id"], doc)

        await self.bulk(_job)

    async def commit(self) -> None:
        """Flush a batch. No-op: every embedded write call already
        lands under its own flush (see ``bulk()``). Kept for surface
        parity with the SQLite store."""

    async def address_balance(self, address: str) -> Tuple[int, int]:
        """Return (confirmed_balance_sats, total_received_sats) for address."""
        utxos = await self._query(
            COLL_UTXO, [("address", "=", address)])
        balance = sum(int(r["value"]) for r in utxos)
        ins = await self._query(
            COLL_ADDR_TX,
            [("address", "=", address), ("direction", "=", "in")])
        received = sum(int(r["value"]) for r in ins)
        return balance, received

    async def address_tx_count(self, address: str) -> int:
        """Distinct txids touching this address (in or out)."""
        rows = await self._query(
            COLL_ADDR_TX, [("address", "=", address)])
        return len({r["txid"] for r in rows})

    async def address_first_last_height(
        self, address: str
    ) -> Tuple[Optional[int], Optional[int]]:
        rows = await self._query(
            COLL_ADDR_TX, [("address", "=", address)])
        if not rows:
            return None, None
        heights = [int(r["height"]) for r in rows]
        return min(heights), max(heights)

    async def address_txs(
        self, address: str, limit: int = 50, offset: int = 0
    ) -> List[dict]:
        """Most-recent-first list of {txid, height, block_time, in_sats, out_sats}.

        Mirrors the SQLite GROUP BY txid aggregation: height and
        block_time are MAX over the address's rows for the tx,
        in/out are per-direction sums; ordering is height DESC with
        txid as the ascending tiebreak."""
        rows = await self._query(
            COLL_ADDR_TX, [("address", "=", address)])
        by_txid: dict = {}
        for r in rows:
            g = by_txid.get(r["txid"])
            if g is None:
                g = by_txid[r["txid"]] = {
                    "txid": r["txid"], "height": int(r["height"]),
                    "block_time": r.get("block_time"),
                    "in_sats": 0, "out_sats": 0,
                }
            g["height"] = max(g["height"], int(r["height"]))
            bt = r.get("block_time")
            if bt is not None:
                g["block_time"] = (
                    bt if g["block_time"] is None
                    else max(g["block_time"], bt)
                )
            if r["direction"] == "in":
                g["in_sats"] += int(r["value"])
            elif r["direction"] == "out":
                g["out_sats"] += int(r["value"])
        grouped = sorted(
            by_txid.values(), key=lambda g: (-g["height"], g["txid"]))
        return grouped[offset: offset + limit]

    async def address_utxos(self, address: str) -> List[dict]:
        rows = await self._query(
            COLL_UTXO, [("address", "=", address)])
        rows.sort(key=lambda r: (-int(r["height"]), r["txid"]))
        return [
            {
                "txid": r["txid"], "vout": int(r["vout"]),
                "value_sats": int(r["value"]), "height": int(r["height"]),
            }
            for r in rows
        ]

    async def address_index_rollback(self, from_height: int) -> None:
        """Drop everything strictly above ``from_height`` so the indexer
        can replay after a reorg. (Simple store-level rollback — the
        writer's undo-restoring rollback_to() is the reorg path the
        address indexer itself uses.)"""
        def _job(db):
            for r in _qsync(db, COLL_UTXO,
                            [("height", ">", int(from_height))]):
                db.delete(COLL_UTXO, r["_id"])
            for r in _qsync(db, COLL_ADDR_TX,
                            [("height", ">", int(from_height))]):
                db.delete(COLL_ADDR_TX, r["_id"])

        await self.bulk(_job)

    async def address_index_last_height(self) -> int:
        """Highest block height the address/UTXO index has fully processed."""
        raw = await self.get(Keys.ADDRESS_INDEX_LAST_HEIGHT)
        return int(raw) if raw is not None else -1

    async def coinbase_reward_scan(
        self, start_height: int, end_height: int
    ) -> tuple[list[dict], list[int]]:
        """Find every coinbase output recipient in ``[start_height, end_height]``.

        Same pure-local scan as the SQLite version: cached block JSON
        gives each height's coinbase txid (``txids[0]``), which joins
        to the address index (direction 'in', value > 0). Returns
        ``(rows, missing_heights)`` — see sqlite_store for the full
        contract."""
        heights = list(range(start_height, end_height + 1))
        CHUNK = 800
        db = self._engine()

        def _kv_values(keys):
            out = {}
            for k in keys:
                row = db.get("kv", k)
                if row is not None:
                    out[k] = row.get("value")
            return out

        # 1. height -> block hash (kv: vision:block:height:{h})
        height_to_hash: dict[int, str] = {}
        for i in range(0, len(heights), CHUNK):
            chunk = heights[i:i + CHUNK]
            keys = [Keys.BLOCK_BY_HEIGHT.format(height=h) for h in chunk]
            vals = await self._run(lambda: _kv_values(keys))
            for k, v in vals.items():
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
            vals = await self._run(lambda: _kv_values(keys))
            for k, v in vals.items():
                bh = k.rsplit(":", 1)[-1]
                h = hash_to_height.get(bh)
                if h is None or not v:
                    continue
                try:
                    txids = json.loads(v).get("txids") or []
                except Exception:
                    continue
                if txids:
                    cb = txids[0]
                    cbtxid_to_height[cb] = h
                    height_to_cbtxid[h] = cb
        for h in height_to_hash:
            if h not in height_to_cbtxid:
                missing.append(h)
        missing = sorted(set(missing))

        # 3. coinbase txid -> credited addresses (value>0) from the index
        rows: list[dict] = []
        cbtxids = list(cbtxid_to_height.keys())
        for i in range(0, len(cbtxids), CHUNK):
            chunk = cbtxids[i:i + CHUNK]

            def _scan(txids=chunk):
                out = []
                for txid in txids:
                    for r in _qsync(
                        db, COLL_ADDR_TX,
                        [("txid", "=", txid), ("direction", "=", "in")],
                    ):
                        if int(r["value"]) > 0:
                            out.append(r)
                return out

            for r in await self._run(_scan):
                h = cbtxid_to_height.get(r["txid"])
                if h is None or not r.get("address"):
                    continue
                rows.append({
                    "height": h,
                    "block_hash": height_to_hash.get(h),
                    "coinbase_txid": r["txid"],
                    "address": r["address"],
                    "value": int(r["value"]),
                })
        return rows, missing

    # ── pool operator snapshot rewards ───────────────────────────────
    #
    # All ITC amounts are persisted as decimal strings (8 dp); callers
    # do the Decimal arithmetic. Same contract as the SQLite version.

    # -- pools --
    async def pool_create(self, data: dict) -> int:
        now = time.time()

        def _job(db):
            addr = data.get("payout_address")
            status = data.get("status", "active")
            if addr is not None and status == "active":
                # Mirror the SQLite partial unique index: at most one
                # ACTIVE pool per payout address.
                for row in _qsync(db, COLL_POOL,
                                  [("payout_address", "=", addr)]):
                    if row.get("status") == "active":
                        raise sqlite3.IntegrityError(
                            "UNIQUE constraint failed: "
                            "mining_pools.payout_address (active)")
            pid = _alloc_id(db, SEQ_POOL)
            doc = pool_doc({
                "id": pid,
                "pool_name": data.get("pool_name"),
                "coinbase_tag": data.get("coinbase_tag"),
                "payout_address": addr,
                "website": data.get("website"),
                "contact_email": data.get("contact_email"),
                "discord": data.get("discord"),
                "telegram": data.get("telegram"),
                "status": status,
                "created_at": now,
                "updated_at": now,
            })
            db.put(COLL_POOL, doc["_id"], doc)
            return pid

        return await self.bulk(_job)

    async def pool_update(self, pool_id: int, data: dict) -> bool:
        fields = [
            "pool_name", "coinbase_tag", "payout_address", "website",
            "contact_email", "discord", "telegram", "status",
        ]
        updates = {f: data[f] for f in fields if f in data}
        if not updates:
            return False

        def _job(db):
            row = db.get(COLL_POOL, str(pool_id))
            if row is None:
                return False
            doc = _project(_clean(row), POOL_FIELDS)
            new_addr = updates.get("payout_address", doc.get("payout_address"))
            new_status = updates.get("status", doc.get("status"))
            if new_addr is not None and new_status == "active":
                for other in _qsync(
                    db, COLL_POOL, [("payout_address", "=", new_addr)]
                ):
                    if (int(other["id"]) != int(pool_id)
                            and other.get("status") == "active"):
                        raise sqlite3.IntegrityError(
                            "UNIQUE constraint failed: "
                            "mining_pools.payout_address (active)")
            doc.update(updates)
            doc["updated_at"] = time.time()
            full = pool_doc(doc)
            db.put(COLL_POOL, full["_id"], full)
            return True

        return await self.bulk(_job)

    async def pool_get(self, pool_id: int) -> Optional[dict]:
        db = self._engine()
        row = await self._run(db.get, COLL_POOL, str(pool_id))
        if row is None:
            return None
        return _project(_clean(row), POOL_FIELDS)

    async def pool_find_by_payout_address(
        self, payout_address: str, *, exclude_id: Optional[int] = None,
        active_only: bool = True, statuses: Optional[Sequence[str]] = None,
    ) -> Optional[dict]:
        """Find a pool already using ``payout_address`` — same filtering
        contract as the SQLite version (``statuses`` overrides
        ``active_only``). Lowest id wins, matching SQLite's rowid-order
        LIMIT 1 in practice."""
        rows = await self._query(
            COLL_POOL, [("payout_address", "=", payout_address)])
        matches = []
        for r in rows:
            if statuses:
                if r.get("status") not in statuses:
                    continue
            elif active_only:
                if r.get("status") != "active":
                    continue
            if exclude_id is not None and int(r["id"]) == int(exclude_id):
                continue
            matches.append(r)
        if not matches:
            return None
        matches.sort(key=lambda r: int(r["id"]))
        return _project(matches[0], POOL_FIELDS)

    async def pool_list(self, *, status: Optional[str] = None) -> List[dict]:
        rows = await self._query(COLL_POOL)
        if status:
            rows = [r for r in rows if r.get("status") == status]
        # SQLite: ORDER BY pool_name COLLATE NOCASE (ties → rowid).
        rows.sort(key=lambda r: (
            (r.get("pool_name") or "").lower(), int(r["id"])))
        return [_project(r, POOL_FIELDS) for r in rows]

    # -- snapshots --
    async def snapshot_range_exists(self, start_height: int, end_height: int) -> bool:
        # Failed/rejected snapshots don't count as occupying the range.
        rows = await self._query(
            COLL_SNAPSHOT, [("start_height", "=", int(start_height))])
        return any(
            int(r["end_height"]) == int(end_height)
            and r.get("status") not in ("failed", "rejected")
            for r in rows
        )

    @staticmethod
    def _snapshot_insert_doc(data: dict, sid: int, now: float) -> dict:
        return snapshot_doc({
            "id": sid,
            "snapshot_name": data.get("snapshot_name"),
            "start_height": int(data.get("start_height")),
            "end_height": int(data.get("end_height")),
            "reward_per_block": data.get("reward_per_block"),
            "total_blocks_scanned": int(data.get("total_blocks_scanned", 0)),
            "total_blocks_matched": int(data.get("total_blocks_matched", 0)),
            "total_reward": data.get("total_reward", "0.00000000"),
            "status": data.get("status", "generated"),
            "created_at": now,
            "approved_at": None,
            "paid_at": None,
            "notes": data.get("notes"),
        })

    async def snapshot_create(self, data: dict) -> int:
        now = time.time()

        def _job(db):
            sid = _alloc_id(db, SEQ_SNAPSHOT)
            doc = self._snapshot_insert_doc(data, sid, now)
            db.put(COLL_SNAPSHOT, doc["_id"], doc)
            return sid

        return await self.bulk(_job)

    async def snapshot_create_guarded(self, data: dict) -> Optional[int]:
        """Insert a snapshot only if no snapshot with the same
        (start_height, end_height) already exists (failed/rejected
        don't count). Check + insert run inside one storage-thread
        job, so concurrent requests for the same range can never both
        succeed — the race-safety the SQLite version got from its
        write-lock critical section."""
        now = time.time()

        def _job(db):
            for r in _qsync(
                db, COLL_SNAPSHOT,
                [("start_height", "=", int(data.get("start_height")))],
            ):
                if (int(r["end_height"]) == int(data.get("end_height"))
                        and r.get("status") not in ("failed", "rejected")):
                    return None
            sid = _alloc_id(db, SEQ_SNAPSHOT)
            doc = self._snapshot_insert_doc(data, sid, now)
            db.put(COLL_SNAPSHOT, doc["_id"], doc)
            return sid

        return await self.bulk(_job)

    async def snapshot_delete(self, snapshot_id: int) -> bool:
        """Delete a snapshot and all of its entries/blocks."""
        def _job(db):
            self._delete_snapshot_children(db, snapshot_id)
            row = db.get(COLL_SNAPSHOT, str(snapshot_id))
            if row is None:
                return False
            db.delete(COLL_SNAPSHOT, str(snapshot_id))
            return True

        return await self.bulk(_job)

    @staticmethod
    def _delete_snapshot_children(db, snapshot_id: int) -> None:
        for coll in (COLL_ENTRY, COLL_REWARD_BLOCK):
            for r in _qsync(db, coll,
                            [("snapshot_id", "=", int(snapshot_id))]):
                db.delete(coll, r["_id"])

    async def snapshots_delete_by_status(self, statuses: Sequence[str]) -> int:
        """Delete all snapshots whose status is in ``statuses`` plus
        their derived entries/blocks. Returns snapshots removed."""
        statuses = [s for s in statuses if s]
        if not statuses:
            return 0

        def _job(db):
            victims = [
                r for r in _qsync(db, COLL_SNAPSHOT)
                if r.get("status") in statuses
            ]
            for r in victims:
                self._delete_snapshot_children(db, int(r["id"]))
                db.delete(COLL_SNAPSHOT, r["_id"])
            return len(victims)

        return await self.bulk(_job)

    async def snapshot_get(self, snapshot_id: int) -> Optional[dict]:
        db = self._engine()
        row = await self._run(db.get, COLL_SNAPSHOT, str(snapshot_id))
        if row is None:
            return None
        return _project(_clean(row), SNAPSHOT_FIELDS)

    async def snapshot_list(self, *, limit: int = 100, offset: int = 0) -> List[dict]:
        rows = await self._query(COLL_SNAPSHOT)
        rows.sort(key=lambda r: -int(r["id"]))
        return [
            _project(r, SNAPSHOT_FIELDS)
            for r in rows[offset: offset + limit]
        ]

    async def snapshot_set_totals(
        self, snapshot_id: int, *, total_blocks_scanned: int,
        total_blocks_matched: int, total_reward: str,
    ) -> bool:
        def _job(db):
            row = db.get(COLL_SNAPSHOT, str(snapshot_id))
            if row is None:
                return False
            doc = _project(_clean(row), SNAPSHOT_FIELDS)
            doc["total_blocks_scanned"] = int(total_blocks_scanned)
            doc["total_blocks_matched"] = int(total_blocks_matched)
            doc["total_reward"] = total_reward
            full = snapshot_doc(doc)
            db.put(COLL_SNAPSHOT, full["_id"], full)
            return True

        return await self.bulk(_job)

    async def snapshot_set_status(self, snapshot_id: int, status: str) -> bool:
        def _job(db):
            row = db.get(COLL_SNAPSHOT, str(snapshot_id))
            if row is None:
                return False
            doc = _project(_clean(row), SNAPSHOT_FIELDS)
            doc["status"] = status
            ts_col = {"approved": "approved_at", "paid": "paid_at"}.get(status)
            if ts_col:
                doc[ts_col] = time.time()
            full = snapshot_doc(doc)
            db.put(COLL_SNAPSHOT, full["_id"], full)
            return True

        return await self.bulk(_job)

    async def snapshot_mark_failed(self, snapshot_id: int, error: str) -> bool:
        """Mark a still-``draft`` snapshot ``failed`` and store the error
        in ``notes``. Compare-and-set on status='draft' (single storage
        job), so a background scan never clobbers an admin's status."""
        def _job(db):
            row = db.get(COLL_SNAPSHOT, str(snapshot_id))
            if row is None:
                return False
            doc = _project(_clean(row), SNAPSHOT_FIELDS)
            if doc.get("status") != "draft":
                return False
            doc["status"] = "failed"
            doc["notes"] = error
            full = snapshot_doc(doc)
            db.put(COLL_SNAPSHOT, full["_id"], full)
            return True

        return await self.bulk(_job)

    async def snapshot_finalize(
        self, snapshot_id: int, *, total_blocks_scanned: int,
        total_blocks_matched: int, total_reward: str,
    ) -> bool:
        """Atomically write totals and flip ``draft → generated``.
        Compare-and-set on status='draft': returns False (without
        touching the doc) if the snapshot is no longer a draft."""
        def _job(db):
            row = db.get(COLL_SNAPSHOT, str(snapshot_id))
            if row is None:
                return False
            doc = _project(_clean(row), SNAPSHOT_FIELDS)
            if doc.get("status") != "draft":
                return False
            doc["total_blocks_scanned"] = int(total_blocks_scanned)
            doc["total_blocks_matched"] = int(total_blocks_matched)
            doc["total_reward"] = total_reward
            doc["status"] = "generated"
            full = snapshot_doc(doc)
            db.put(COLL_SNAPSHOT, full["_id"], full)
            return True

        return await self.bulk(_job)

    async def snapshot_clear_results(self, snapshot_id: int) -> None:
        """Delete a snapshot's entries + per-block rows (keeps the
        snapshot row itself)."""
        def _job(db):
            self._delete_snapshot_children(db, snapshot_id)
            return True

        await self.bulk(_job)

    # -- entries --
    async def entries_insert_batch(self, rows: Sequence[dict]) -> None:
        if not rows:
            return
        now = time.time()

        def _job(db):
            # UNIQUE(snapshot_id, payout_address): verify the whole
            # batch (against the store and within itself) before any
            # write, then raise the same error SQLite raised.
            seen: dict[int, set] = {}
            for r in rows:
                sid = int(r["snapshot_id"])
                if sid not in seen:
                    seen[sid] = {
                        e.get("payout_address")
                        for e in _qsync(db, COLL_ENTRY,
                                        [("snapshot_id", "=", sid)])
                    }
                addr = r.get("payout_address")
                if addr in seen[sid]:
                    raise sqlite3.IntegrityError(
                        "UNIQUE constraint failed: "
                        "pool_reward_snapshot_entries.snapshot_id, "
                        "pool_reward_snapshot_entries.payout_address")
                seen[sid].add(addr)
            for r in rows:
                eid = _alloc_id(db, SEQ_ENTRY)
                doc = entry_doc({
                    "id": eid,
                    "snapshot_id": int(r["snapshot_id"]),
                    "pool_id": r.get("pool_id"),
                    "pool_name": r["pool_name"],
                    "payout_address": r.get("payout_address"),
                    "blocks_found": int(r["blocks_found"]),
                    "reward_per_block": r["reward_per_block"],
                    "total_reward": r["total_reward"],
                    "status": r.get("status", "pending"),
                    "txid": r.get("txid"),
                    "created_at": now,
                    "updated_at": now,
                })
                db.put(COLL_ENTRY, doc["_id"], doc)

        await self.bulk(_job)

    async def entries_list(self, snapshot_id: int) -> List[dict]:
        rows = await self._query(
            COLL_ENTRY, [("snapshot_id", "=", int(snapshot_id))])
        # SQLite: ORDER BY blocks_found DESC, pool_name COLLATE NOCASE.
        rows.sort(key=lambda r: (
            -int(r["blocks_found"]),
            (r.get("pool_name") or "").lower(),
            int(r["id"]),
        ))
        return [_project(r, ENTRY_FIELDS) for r in rows]

    async def entry_get(self, entry_id: int) -> Optional[dict]:
        db = self._engine()
        row = await self._run(db.get, COLL_ENTRY, str(entry_id))
        if row is None:
            return None
        return _project(_clean(row), ENTRY_FIELDS)

    async def entry_update(
        self, entry_id: int, *, status: Optional[str] = None,
        txid: Optional[str] = None,
    ) -> bool:
        updates: dict = {}
        if status is not None:
            updates["status"] = status
        if txid is not None:
            updates["txid"] = txid
        if not updates:
            return False

        def _job(db):
            row = db.get(COLL_ENTRY, str(entry_id))
            if row is None:
                return False
            doc = _project(_clean(row), ENTRY_FIELDS)
            doc.update(updates)
            doc["updated_at"] = time.time()
            full = entry_doc(doc)
            db.put(COLL_ENTRY, full["_id"], full)
            return True

        return await self.bulk(_job)

    async def entries_set_status_all(self, snapshot_id: int, status: str) -> int:
        now = time.time()

        def _job(db):
            rows = _qsync(db, COLL_ENTRY,
                          [("snapshot_id", "=", int(snapshot_id))])
            for r in rows:
                doc = _project(r, ENTRY_FIELDS)
                doc["status"] = status
                doc["updated_at"] = now
                full = entry_doc(doc)
                db.put(COLL_ENTRY, full["_id"], full)
            return len(rows)

        return await self.bulk(_job)

    # -- matched blocks --
    async def reward_blocks_insert_batch(self, rows: Sequence[dict]) -> None:
        if not rows:
            return
        now = time.time()

        def _job(db):
            # INSERT OR IGNORE on UNIQUE(snapshot_id, block_height,
            # payout_address); SQL NULL semantics: a NULL payout
            # address never collides, so those rows always insert.
            seen: dict[int, set] = {}
            for r in rows:
                sid = int(r["snapshot_id"])
                if sid not in seen:
                    seen[sid] = {
                        (int(b["block_height"]), b.get("payout_address"))
                        for b in _qsync(
                            db, COLL_REWARD_BLOCK,
                            [("snapshot_id", "=", sid)])
                    }
                key = (int(r["block_height"]), r.get("payout_address"))
                if key[1] is not None and key in seen[sid]:
                    continue
                seen[sid].add(key)
                bid = _alloc_id(db, SEQ_REWARD_BLOCK)
                doc = reward_block_doc({
                    "id": bid,
                    "snapshot_id": sid,
                    "pool_id": r.get("pool_id"),
                    "block_height": int(r["block_height"]),
                    "block_hash": r["block_hash"],
                    "coinbase_tag_detected": r.get("coinbase_tag_detected"),
                    "payout_address": r.get("payout_address"),
                    "reward_amount": r["reward_amount"],
                    "created_at": now,
                })
                db.put(COLL_REWARD_BLOCK, doc["_id"], doc)

        await self.bulk(_job)

    async def reward_blocks_list(
        self, snapshot_id: int, *, pool_id: Optional[int] = None
    ) -> List[dict]:
        rows = await self._query(
            COLL_REWARD_BLOCK, [("snapshot_id", "=", int(snapshot_id))])
        if pool_id is not None:
            rows = [r for r in rows if r.get("pool_id") == pool_id]
        rows.sort(key=lambda r: (int(r["block_height"]), int(r["id"])))
        return [_project(r, REWARD_BLOCK_FIELDS) for r in rows]


# ── address-index writer ─────────────────────────────────────────────
#
# Embedded replacement for sqlite_store.AddressIndexWriter. Same
# interface and same per-block atomicity contract: one apply_block()
# call is one storage-thread job (see EmbeddedDomainMixin notes), so
# the block's UTXO mutations, address-tx rows, undo entries, indexed
# block-hash record AND the last_height pointer land together under a
# single flush — or, on failure, not at all beyond an idempotent
# prefix the next apply of the same block simply rewrites (every op
# is keyed put/delete, and the pointer write is the job's last op).

class EmbeddedAddressIndexWriter:
    """Per-block atomic address-index writes on the embedded engine."""

    _LAST_HEIGHT_KEY = Keys.ADDRESS_INDEX_LAST_HEIGHT

    def __init__(self, store) -> None:
        self._store = store
        # Serialize concurrent callers (backfill batch + tip handler),
        # mirroring the SQLite writer's lock.
        self._lock = asyncio.Lock()

    async def get_last_height(self) -> int:
        raw = await self._store.get(self._LAST_HEIGHT_KEY)
        return int(raw) if raw is not None else -1

    async def get_indexed_hashes(
        self, heights: Sequence[int]
    ) -> dict:
        """Return {height: hash} for the requested heights (missing → absent)."""
        if not heights:
            return {}
        db = self._store._engine()

        def _job():
            out = {}
            for h in heights:
                row = db.get(COLL_ADDR_BLOCK, str(int(h)))
                if row is not None:
                    out[int(h)] = row["hash"]
            return out

        return await self._store._run(_job)

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
        """Apply one block atomically. Same contract as the SQLite
        writer: outputs are inserted first so an in-block self-spend
        resolves; spent UTXOs are resolved + deleted inside the same
        job and recorded in the undo log; the pointer bump is last."""
        store = self._store

        def _job(db):
            # 1. Insert outputs first so any in-block self-spend resolves.
            for txid, vout, address, value, h in utxo_inserts:
                doc = utxo_doc(txid, vout, address, value, h)
                db.put(COLL_UTXO, doc["_id"], doc)

            # 2. Resolve & delete spent UTXOs, building 'out' history
            #    rows and undo entries.
            out_rows: list = []
            for prev_txid, prev_vout in spend_outpoints:
                row = db.get(COLL_UTXO, f"{prev_txid}:{prev_vout}")
                if row is None:
                    continue
                db.delete(COLL_UTXO, row["_id"])
                undo = undo_doc(
                    height, row["txid"], int(row["vout"]),
                    row["address"], int(row["value"]),
                    int(row["height"]),
                )
                db.put(COLL_UNDO, undo["_id"], undo)
                spending_txid = spend_owner.get(
                    (prev_txid, int(prev_vout)))
                if spending_txid:
                    out_rows.append((
                        row["address"], spending_txid, height, "out",
                        int(row["value"]), block_time,
                    ))

            # 3. Insert all address_tx rows ('in' + 'out').
            for address, txid, h, direction, value, bt in (
                list(in_rows) + out_rows
            ):
                doc = addr_tx_doc(address, txid, h, direction, value, bt)
                db.put(COLL_ADDR_TX, doc["_id"], doc)

            # 4. Per-height block hash for reorg detection.
            doc = addr_block_doc(height, block_hash)
            db.put(COLL_ADDR_BLOCK, doc["_id"], doc)

            # 5. Bump the last-height pointer — same job, last op.
            db.put("kv", self._LAST_HEIGHT_KEY, {
                "_id": self._LAST_HEIGHT_KEY, "value": str(height),
                "expires_at": None,
            })

        async with self._lock:
            await store.bulk(_job)

    async def checkpoint(self) -> None:
        """No-op — the SQLite WAL checkpoint has no embedded analogue;
        kept for interface parity with the old writer."""

    async def rollback_to(self, height: int) -> None:
        """Reorg-safe rollback to the given height — same steps as the
        SQLite writer, in one storage-thread job:

        1. Delete UTXOs created at heights > N.
        2. Re-insert UTXOs from the undo log whose ORIGINAL creation
           height is <= N (spent in the now-orphaned blocks).
        3. Prune the undo log, address_txs and per-height hash records
           for everything > N.
        4. Rewind the last_height pointer.
        """
        store = self._store

        def _job(db):
            for r in _qsync(db, COLL_UTXO, [("height", ">", height)]):
                db.delete(COLL_UTXO, r["_id"])
            for r in _qsync(db, COLL_UNDO, [("height", ">", height)]):
                if int(r["orig_height"]) <= height:
                    doc = utxo_doc(
                        r["txid"], int(r["vout"]), r["address"],
                        int(r["value"]), int(r["orig_height"]),
                    )
                    db.put(COLL_UTXO, doc["_id"], doc)
            for coll in (COLL_UNDO, COLL_ADDR_TX, COLL_ADDR_BLOCK):
                for r in _qsync(db, coll, [("height", ">", height)]):
                    db.delete(coll, r["_id"])
            db.put("kv", self._LAST_HEIGHT_KEY, {
                "_id": self._LAST_HEIGHT_KEY, "value": str(height),
                "expires_at": None,
            })

        async with self._lock:
            await store.bulk(_job)

    async def aclose(self) -> None:
        """No-op — the engine is owned (and closed) by the store."""


# ── writer singleton (same names as the sqlite_store versions) ───────

_writer: Optional[EmbeddedAddressIndexWriter] = None


def init_address_index_writer(store) -> EmbeddedAddressIndexWriter:
    """Bind the address-index writer to the live embedded store."""
    global _writer
    if _writer is None:
        _writer = EmbeddedAddressIndexWriter(store)
    return _writer


def get_address_index_writer() -> EmbeddedAddressIndexWriter:
    if _writer is None:
        raise RuntimeError(
            "Address-index writer not initialised — "
            "call init_address_index_writer() first"
        )
    return _writer


async def close_address_index_writer() -> None:
    global _writer
    if _writer is not None:
        await _writer.aclose()
        _writer = None
