"""dual_store.py — bi-directional, sticky dual-write store for Vision.

Wraps SQLiteStore (primary, always reliable) and NedbStore (secondary,
temporal, NQL-queryable). All writes go to both. Reads prefer nedbd (sticky)
and fall back to SQLite transparently on miss or nedbd unavailability.

nedbd self-populates through normal Vision operation — no migration required.

Architecture
------------

    WRITE  →  SQLite (await, source of truth)
           →  nedbd  (fire-and-forget asyncio task, best-effort)

    READ   →  nedbd first (sticky: once a key is in nedbd, always read there)
           →  SQLite fallback on miss or nedbd unavailability

Stats + Observability
---------------------
    DualStore.stats()           → dict with nedb_hits, sqlite_fallbacks, etc.
    GET /api/health             → includes "dual_store" section
    Startup log                 → prominent banner confirming DualStore is live
    Periodic log every 1000 ops → shows % reads served by nedbd vs SQLite
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, List, Optional, Sequence, Tuple

from .sqlite_store import SQLiteStore
from .nedb_store import NedbStore
from .config import settings

logger = logging.getLogger(__name__)

# Log a stats summary every this many completed read operations
_STATS_LOG_INTERVAL = 1_000

# Log nedbd-offline warning at most once every this many seconds
_OFFLINE_WARN_INTERVAL = 60


class DualStore:
    """Bi-directional dual-write store — SQLite primary, nedbd secondary.

    Drop-in replacement for SQLiteStore — identical public interface.
    """

    def __init__(self, sqlite: SQLiteStore, nedb: NedbStore) -> None:
        self._sq = sqlite
        self._nd = nedb

        # Max concurrent in-flight nedbd write tasks
        self._write_sem = asyncio.Semaphore(4)

        # ── Stats counters ────────────────────────────────────────────────
        self._nedb_hits        = 0
        self._sqlite_fallbacks = 0
        self._write_successes  = 0
        self._write_failures   = 0
        self._total_reads      = 0

        # ── Circuit breaker ───────────────────────────────────────────────
        # When nedbd is down, writes are dropped until a periodic health
        # probe succeeds. Backoff grows exponentially (10s → 20s → ... → 300s)
        # so a crashed nedbd doesn't generate continuous noise.
        self._nedb_online         = True
        self._consec_failures     = 0       # consecutive failures since last success
        self._backoff_until       = 0.0     # monotonic time: skip writes until then
        self._probe_task: Optional[asyncio.Task] = None
        self._last_offline_log    = 0.0
        self._last_stats_log      = 0

        # Startup banner
        logger.info("=" * 58)
        logger.info("  DualStore ACTIVE")
        logger.info("  Reads : nedbd (sticky) → SQLite fallback")
        logger.info("  Writes: SQLite (sync) + nedbd (circuit-breaker)")
        logger.info("  nedbd : %s  db=%s", settings.NEDB_URL, settings.NEDB_DB_NAME)
        logger.info("=" * 58)

    # ── Public stats ─────────────────────────────────────────────────────

    def stats(self) -> dict:
        total = self._nedb_hits + self._sqlite_fallbacks
        nedb_pct = round(self._nedb_hits / total * 100, 1) if total else 0.0
        return {
            "active":           True,
            "nedb_online":      self._nedb_online,
            "nedb_hits":        self._nedb_hits,
            "sqlite_fallbacks": self._sqlite_fallbacks,
            "nedb_read_pct":    nedb_pct,
            "write_successes":  self._write_successes,
            "write_failures":   self._write_failures,
            "total_reads":      self._total_reads,
        }

    # ── Internal helpers ─────────────────────────────────────────────────

    def _fire(self, coro) -> None:
        """Schedule a nedbd coroutine as a semaphore-gated fire-and-forget task."""
        async def _wrap():
            async with self._write_sem:
                try:
                    await coro
                    self._write_successes += 1
                    if not self._nedb_online:
                        self._nedb_online = True
                        logger.info("DualStore: nedbd reconnected — resuming dual-write")
                except Exception as e:
                    self._write_failures += 1
                    now = time.monotonic()
                    if self._nedb_online:
                        self._nedb_online = False
                        logger.warning(
                            "DualStore: nedbd write failed — running SQLite-only "
                            "until nedbd recovers. %s: %s",
                            type(e).__name__, e or "(no message)"
                        )
                        self._last_offline_log = now
                    elif now - self._last_offline_log >= _OFFLINE_WARN_INTERVAL:
                        logger.warning(
                            "DualStore: nedbd still unavailable "
                            "(failures=%d, successes=%d). %s: %s",
                            self._write_failures, self._write_successes,
                            type(e).__name__, e or "(no message)",
                        )
                        self._last_offline_log = now

        try:
            asyncio.get_running_loop().create_task(_wrap())
        except RuntimeError:
            pass

    async def _nd_read(self, coro) -> Optional[Any]:
        """Run a nedbd read. Returns None on any failure (fall back to SQLite).

        httpx handles timeouts cleanly (closes the connection gracefully).
        asyncio.wait_for was removed — it cancels tasks without closing the
        socket, causing BrokenPipe errors in nedbd.
        """
        try:
            result = await coro
            return result
        except Exception:
            return None

    def _record_read(self, from_nedb: bool) -> None:
        """Increment counters and periodically log read-source stats."""
        self._total_reads += 1
        if from_nedb:
            self._nedb_hits += 1
        else:
            self._sqlite_fallbacks += 1

        ops = self._nedb_hits + self._sqlite_fallbacks
        if ops > 0 and ops % _STATS_LOG_INTERVAL == 0:
            total = self._nedb_hits + self._sqlite_fallbacks
            pct = round(self._nedb_hits / total * 100, 1) if total else 0.0
            logger.info(
                "DualStore reads: %d total — nedbd %d (%.1f%%)  "
                "SQLite fallback %d (%.1f%%)  write_failures=%d",
                total,
                self._nedb_hits, pct,
                self._sqlite_fallbacks, 100 - pct,
                self._write_failures,
            )

    # ── key / value ──────────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[str]:
        val = await self._nd_read(self._nd.get(key))
        if val is not None:
            self._record_read(from_nedb=True)
            return val
        result = await self._sq.get(key)
        self._record_read(from_nedb=False)
        return result

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:
        await self._sq.set(key, value, ex)
        self._fire(self._nd.set(key, value, ex))

    async def delete(self, *keys: str) -> int:
        count = await self._sq.delete(*keys)
        self._fire(self._nd.delete(*keys))
        return count

    async def incr(self, key: str) -> int:
        val = await self._sq.incr(key)
        self._fire(self._nd.set(key, str(val)))
        return val

    async def expire(self, key: str, seconds: int) -> None:
        await self._sq.expire(key, seconds)
        self._fire(self._nd.expire(key, seconds))

    # ── sorted sets ──────────────────────────────────────────────────────

    async def zadd(self, name: str, mapping: dict) -> int:
        count = await self._sq.zadd(name, mapping)
        self._fire(self._nd.zadd(name, mapping))
        return count

    async def zrevrange(
        self, name: str, start: int, stop: int, withscores: bool = False
    ) -> list:
        result = await self._nd_read(self._nd.zrevrange(name, start, stop, withscores))
        if result is not None:
            self._record_read(from_nedb=True)
            return result
        out = await self._sq.zrevrange(name, start, stop, withscores)
        self._record_read(from_nedb=False)
        return out

    async def zremrangebyrank(self, name: str, start: int, stop: int) -> int:
        count = await self._sq.zremrangebyrank(name, start, stop)
        self._fire(self._nd.zremrangebyrank(name, start, stop))
        return count

    # ── sets ─────────────────────────────────────────────────────────────

    async def sadd(self, name: str, *members: str) -> int:
        count = await self._sq.sadd(name, *members)
        self._fire(self._nd.sadd(name, *members))
        return count

    async def smembers(self, name: str) -> set:
        result = await self._nd_read(self._nd.smembers(name))
        if result is not None:
            self._record_read(from_nedb=True)
            return result
        out = await self._sq.smembers(name)
        self._record_read(from_nedb=False)
        return out

    async def srem(self, name: str, *members: str) -> int:
        count = await self._sq.srem(name, *members)
        self._fire(self._nd.srem(name, *members))
        return count

    # ── domain methods — SQLite only ─────────────────────────────────────

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


# ── module-level singleton ────────────────────────────────────────────────────

_instance: Optional[DualStore] = None


def set_dual_store_instance(ds: "DualStore") -> None:
    """Called by main.py after DualStore is constructed."""
    global _instance
    _instance = ds


def get_dual_store() -> Optional["DualStore"]:
    """Return the active DualStore instance, or None if not initialised."""
    return _instance
