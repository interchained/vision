"""embedded_store.py — in-process NEDB store for Vision's KV layer.

Replaces the DualStore architecture (SQLite primary + nedbd-over-HTTP
mirror) with a single embedded NEDB engine (the ``nedb-engine`` Python
package) opened on a local data directory inside the backend process.

Why: the HTTP hop to a separate nedbd process was the fragile part —
under indexer write pressure its listen backlog saturated and most
calls died with ConnectTimeout/WriteTimeout, so the "secondary" store
was effectively down while still costing seconds per call. In-process
calls cannot time out on a socket; the engine's group-commit
sequencer batches fsyncs internally.

Semantics deliberately mirror ``nedb_store.NedbStore`` (the HTTP
client this replaces) so every caller sees identical shapes:

* ``kv``    – ``{_id: key, value: str, expires_at: float|null}``
* ``zset``  – ``{_id: f"{name}::{member}", _name, _member, score}``
* ``set``   – ``{_id: f"{name}::{member}", _name, _member}``

All engine calls are serialized through a single-worker executor:
the engine never sees concurrent calls, the event loop never blocks
on storage I/O, and read-modify-write operations (incr) are atomic
in practice.

The domain surface (utxo_*, address_*, pool_*, snapshot_*) stays on
SQLite — see ``EmbeddedStore``, the drop-in ``get_db()`` override
that routes KV methods here and domain methods to SQLiteStore.
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# Sync-state keys, stored in the embedded store itself so the sync
# checkpoint survives restarts and is readable by any consumer.
SYNC_CHECKPOINT_KEY = "vision:sync:checkpoint_height"
SYNC_LOWEST_KEY = "vision:sync:lowest_synced"
SYNC_COMPLETE_KEY = "vision:sync:complete"


def _nql_escape(s: str) -> str:
    """Escape a string for embedding in an NQL double-quoted literal."""
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


class EmbeddedNedbStore:
    """Async facade over the embedded NEDB engine (KV/zset/set subset)."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._db = None                     # nedb.NEDB, opened lazily
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="nedb-embedded"
        )

    # ── engine plumbing ──────────────────────────────────────────────

    async def _run(self, fn, *args):
        """Run a blocking engine call on the single storage thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn, *args)

    async def bulk(self, fn):
        """Run ``fn(db)`` on the storage thread with fsync deferred,
        then a single flush at the end.

        The engine fsyncs the AOF on every put by default (~30/s on
        commodity disks). With ``_defer_sync`` set, puts buffer in
        the AOF writer and one ``flush()`` makes the whole batch
        durable (~9,000/s measured). Bulk paths (tip-window seed,
        history backfill, multi-member zadd/delete) use this; the
        batch is idempotent, so losing an unflushed tail to a crash
        just means the batch is redone.
        """
        def _job():
            db = self._engine()
            prev = getattr(db, "_defer_sync", False)
            db._defer_sync = True
            try:
                return fn(db)
            finally:
                try:
                    db.flush()
                finally:
                    db._defer_sync = prev

        return await self._run(_job)

    async def bulk_puts(self, ops) -> int:
        """Apply many ``(coll, id, doc)`` puts as one deferred-sync
        batch with a single fsync. Returns the number of puts."""
        def _job(db):
            for coll, doc_id, doc in ops:
                db.put(coll, doc_id, doc)
            return len(ops)

        return await self.bulk(_job)

    async def open(self) -> None:
        """Open (creating if needed) the embedded database."""
        if self._db is not None:
            return

        def _open():
            from nedb import NEDB
            return NEDB(self._path)

        self._db = await self._run(_open)
        logger.info("EmbeddedNedbStore open at %s", self._path)

    async def aclose(self) -> None:
        if self._db is not None:
            db, self._db = self._db, None
            try:
                await self._run(db.close)
            except Exception:
                pass
        self._executor.shutdown(wait=False)

    def _engine(self):
        if self._db is None:
            raise RuntimeError("EmbeddedNedbStore is not open")
        return self._db

    # ── key / value ──────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[str]:
        db = self._engine()
        row = await self._run(db.get, "kv", key)
        if not row:
            return None
        expires_at = row.get("expires_at")
        if expires_at is not None and expires_at < time.time():
            return None
        v = row.get("value")
        return None if v is None else str(v)

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:
        db = self._engine()
        expires = (time.time() + ex) if ex else None
        doc = {"_id": key, "value": str(value), "expires_at": expires}
        await self._run(db.put, "kv", key, doc)

    async def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        def _delete_all(db) -> int:
            count = 0
            for k in keys:
                if db.get("kv", k) is not None:
                    db.delete("kv", k)
                    count += 1
                # Mirror NedbStore.delete: also clear any zset/set
                # group stored under this name.
                for coll in ("zset", "set"):
                    rows = db.query(
                        f'FROM {coll} WHERE _name = "{_nql_escape(k)}" '
                        "LIMIT 99999"
                    )
                    for row in rows or []:
                        if row.get("_id"):
                            db.delete(coll, row["_id"])
            return count

        return await self.bulk(_delete_all)

    async def incr(self, key: str) -> int:
        # Serialized through the single storage thread, so this
        # read-modify-write cannot race (the HTTP version could).
        current = await self.get(key)
        try:
            new_val = (int(current) if current is not None else 0) + 1
        except (TypeError, ValueError):
            new_val = 1
        await self.set(key, new_val)
        return new_val

    async def expire(self, key: str, seconds: int) -> None:
        existing = await self.get(key)
        if existing is None:
            return
        db = self._engine()
        doc = {
            "_id": key,
            "value": existing,
            "expires_at": time.time() + seconds,
        }
        await self._run(db.put, "kv", key, doc)

    # ── sorted sets ──────────────────────────────────────────────────

    @staticmethod
    def _zset_id(name: str, member: str) -> str:
        return f"{name}::{member}"

    async def zadd(self, name: str, mapping: dict) -> int:
        if not mapping:
            return 0

        def _zadd_all(db) -> int:
            for member, score in mapping.items():
                m = str(member)
                doc = {
                    "_id": self._zset_id(name, m),
                    "_name": name,
                    "_member": m,
                    "score": float(score),
                }
                db.put("zset", doc["_id"], doc)
            return len(mapping)

        return await self.bulk(_zadd_all)

    async def zrevrange(
        self, name: str, start: int, stop: int, withscores: bool = False
    ) -> list:
        db = self._engine()
        esc = _nql_escape(name)
        if stop < 0:
            offset = max(start, 0)
            fetch_limit = 99999
        else:
            offset = max(start, 0)
            fetch_limit = max(0, stop + 1)
        nql = (
            f'FROM zset WHERE _name = "{esc}" '
            f"ORDER BY score DESC LIMIT {fetch_limit}"
        )
        rows = await self._run(db.query, nql) or []
        sliced = rows[offset:] if stop < 0 else rows[offset: stop + 1]
        if withscores:
            return [(r.get("_member"), float(r.get("score", 0))) for r in sliced]
        return [r.get("_member") for r in sliced]

    async def zremrangebyrank(self, name: str, start: int, stop: int) -> int:
        """Remove members by rank (0-indexed, ascending score order).

        Negative stop keeps the top N: zremrangebyrank(k, 0, -201)
        keeps the 200 highest-scored entries. Mirrors NedbStore.
        """
        esc = _nql_escape(name)

        def _trim(db) -> int:
            rows = db.query(
                f'FROM zset WHERE _name = "{esc}" '
                "ORDER BY score ASC LIMIT 99999"
            ) or []
            total = len(rows)
            if stop < 0:
                keep = abs(stop) - 1
                delete_count = total - keep
                if delete_count <= 0:
                    return 0
                victims = rows[:delete_count]
            else:
                victims = rows[start: stop + 1]
            n = 0
            for r in victims:
                if r.get("_id"):
                    db.delete("zset", r["_id"])
                    n += 1
            return n

        return await self.bulk(_trim)

    # ── sets ─────────────────────────────────────────────────────────

    @staticmethod
    def _set_id(name: str, member: str) -> str:
        return f"{name}::{member}"

    async def sadd(self, name: str, *members: str) -> int:
        if not members:
            return 0

        def _sadd_all(db) -> int:
            for m in members:
                ms = str(m)
                doc = {
                    "_id": self._set_id(name, ms),
                    "_name": name,
                    "_member": ms,
                }
                db.put("set", doc["_id"], doc)
            return len(members)

        return await self.bulk(_sadd_all)

    async def smembers(self, name: str) -> set:
        db = self._engine()
        esc = _nql_escape(name)
        rows = await self._run(
            db.query, f'FROM set WHERE _name = "{esc}" LIMIT 99999'
        ) or []
        return {r.get("_member") for r in rows if r.get("_member") is not None}

    async def srem(self, name: str, *members: str) -> int:
        if not members:
            return 0

        def _srem_all(db) -> int:
            n = 0
            for m in members:
                doc_id = self._set_id(name, str(m))
                if db.get("set", doc_id) is not None:
                    db.delete("set", doc_id)
                    n += 1
            return n

        return await self.bulk(_srem_all)

    # ── misc ─────────────────────────────────────────────────────────

    async def ping(self) -> bool:
        db = self._engine()
        await self._run(db.collections)
        return True

    async def publish(self, channel: str, message: str) -> int:
        return 0

    # ── sync state ───────────────────────────────────────────────────

    async def get_sync_state(self) -> dict:
        """Checkpoint / backfill progress, stored in the store itself."""
        checkpoint = await self.get(SYNC_CHECKPOINT_KEY)
        lowest = await self.get(SYNC_LOWEST_KEY)
        complete = await self.get(SYNC_COMPLETE_KEY)
        cp = int(checkpoint) if checkpoint else 0
        lo = int(lowest) if lowest is not None else cp
        pct = 0.0
        if cp > 0:
            pct = round(max(0, cp - lo) / cp * 100, 2)
        return {
            "checkpoint_height": cp,
            "lowest_synced": lo,
            "pct": 100.0 if complete == "1" else pct,
            "complete": complete == "1",
        }


class EmbeddedStore:
    """Drop-in ``get_db()`` override: KV layer → embedded NEDB,
    domain layer → SQLite. The DualStore's replacement — one store
    per concern, no mirroring, no circuit breaker, no HTTP.
    """

    def __init__(self, sqlite, embedded: EmbeddedNedbStore) -> None:
        self._sq = sqlite
        self._nd = embedded

    def stats(self) -> dict:
        return {"active": True, "store": "embedded"}

    async def sync_state(self) -> dict:
        return await self._nd.get_sync_state()

    # ── KV / zset / set → embedded NEDB ─────────────────────────────

    async def get(self, key: str) -> Optional[str]:
        return await self._nd.get(key)

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:
        return await self._nd.set(key, value, ex)

    async def delete(self, *keys: str) -> int:
        return await self._nd.delete(*keys)

    async def incr(self, key: str) -> int:
        return await self._nd.incr(key)

    async def expire(self, key: str, seconds: int) -> None:
        return await self._nd.expire(key, seconds)

    async def zadd(self, name: str, mapping: dict) -> int:
        return await self._nd.zadd(name, mapping)

    async def zrevrange(
        self, name: str, start: int, stop: int, withscores: bool = False
    ) -> list:
        return await self._nd.zrevrange(name, start, stop, withscores)

    async def zremrangebyrank(self, name: str, start: int, stop: int) -> int:
        return await self._nd.zremrangebyrank(name, start, stop)

    async def sadd(self, name: str, *members: str) -> int:
        return await self._nd.sadd(name, *members)

    async def smembers(self, name: str) -> set:
        return await self._nd.smembers(name)

    async def srem(self, name: str, *members: str) -> int:
        return await self._nd.srem(name, *members)

    # ── domain methods → SQLite ──────────────────────────────────────

    async def utxo_add_batch(self, rows: Sequence[Tuple[str, int, str, int, int]]) -> None:
        return await self._sq.utxo_add_batch(rows)

    async def utxo_spend_batch(self, outpoints: Sequence[Tuple[str, int]]) -> List[Tuple[str, int, str, int, int]]:
        return await self._sq.utxo_spend_batch(outpoints)

    async def address_tx_add_batch(self, *args, **kwargs) -> None:
        return await self._sq.address_tx_add_batch(*args, **kwargs)

    async def commit(self) -> None:
        return await self._sq.commit()

    async def address_balance(self, address: str) -> Tuple[int, int]:
        return await self._sq.address_balance(address)

    async def address_tx_count(self, address: str) -> int:
        return await self._sq.address_tx_count(address)

    async def address_first_last_height(self, address: str) -> Tuple[Optional[int], Optional[int]]:
        return await self._sq.address_first_last_height(address)

    async def address_txs(self, *args, **kwargs) -> list:
        return await self._sq.address_txs(*args, **kwargs)

    async def address_utxos(self, address: str) -> list:
        return await self._sq.address_utxos(address)

    async def address_index_rollback(self, from_height: int) -> None:
        return await self._sq.address_index_rollback(from_height)

    async def address_index_last_height(self) -> int:
        return await self._sq.address_index_last_height()

    async def coinbase_reward_scan(self, *args, **kwargs):
        return await self._sq.coinbase_reward_scan(*args, **kwargs)

    async def pool_create(self, data: dict) -> int:
        return await self._sq.pool_create(data)

    async def pool_update(self, pool_id: int, data: dict) -> bool:
        return await self._sq.pool_update(pool_id, data)

    async def pool_get(self, pool_id: int) -> Optional[dict]:
        return await self._sq.pool_get(pool_id)

    async def pool_find_by_payout_address(self, *args, **kwargs) -> Optional[dict]:
        return await self._sq.pool_find_by_payout_address(*args, **kwargs)

    async def pool_list(self, *args, **kwargs) -> list:
        return await self._sq.pool_list(*args, **kwargs)

    async def snapshot_create(self, data: dict) -> int:
        return await self._sq.snapshot_create(data)

    async def snapshot_create_guarded(self, data: dict) -> Optional[int]:
        return await self._sq.snapshot_create_guarded(data)

    async def snapshot_delete(self, snapshot_id: int) -> bool:
        return await self._sq.snapshot_delete(snapshot_id)

    async def snapshots_delete_by_status(self, statuses: Sequence[str]) -> int:
        return await self._sq.snapshots_delete_by_status(statuses)

    async def snapshot_get(self, snapshot_id: int) -> Optional[dict]:
        return await self._sq.snapshot_get(snapshot_id)

    async def snapshot_list(self, *args, **kwargs) -> list:
        return await self._sq.snapshot_list(*args, **kwargs)

    async def snapshot_set_totals(self, *args, **kwargs) -> None:
        return await self._sq.snapshot_set_totals(*args, **kwargs)

    async def snapshot_set_status(self, *args, **kwargs) -> bool:
        return await self._sq.snapshot_set_status(*args, **kwargs)

    async def snapshot_range_exists(self, start_height: int, end_height: int) -> bool:
        return await self._sq.snapshot_range_exists(start_height, end_height)


# ── module-level singleton ───────────────────────────────────────────

_instance: Optional[EmbeddedStore] = None


def set_embedded_store_instance(store: "EmbeddedStore") -> None:
    global _instance
    _instance = store


def get_embedded_store() -> Optional["EmbeddedStore"]:
    return _instance
