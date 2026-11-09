"""embedded_backfill.py — tip-first history sync for the embedded store.

The serving model (replaces the DualStore + NedbBackfillTask design):

1. **Checkpoint at tip, serve immediately.** At startup the embedded
   store is seeded with the tip window (tip keys + the most recent
   ~100 blocks) copied from SQLite's KV cache, and the current tip
   height is recorded as the sync checkpoint. The explorer serves
   live data from the first second.
2. **Sync backwards in the background.** This task walks from the
   checkpoint down to genesis, copying each block's KV docs
   (``BLOCK_BY_HEIGHT`` / ``BLOCK_BY_HASH`` / ``RECENT_BLOCKS`` —
   exactly the writes the block indexer makes per block) into the
   embedded store. Sources, in order: SQLite's KV cache (always
   complete, local, byte-identical docs), the legacy nedbd HTTP
   store (the data also lives there from the DualStore era, but it
   is a remote round-trip per block — as primary source it throttled
   the production descent to ~100 blocks/min), and finally live RPC
   in the indexer's own "lite" serialization.
3. **Progress is public.** The contiguous synced floor
   (``vision:sync:lowest_synced``) lives in the embedded store and is
   surfaced via /api/health, so the UI can show sync progress and
   label not-yet-synced heights instead of hard-gating on them.

The cursor is the store itself: on restart the task resumes from the
persisted floor, and once it reaches genesis it sets
``vision:sync:complete`` and never runs again.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, Protocol, Tuple

from ..embedded_store import (
    EmbeddedNedbStore,
    SYNC_CHECKPOINT_KEY,
    SYNC_COMPLETE_KEY,
    SYNC_LOWEST_KEY,
)
from ..sqlite_store import Keys

logger = logging.getLogger(__name__)


def serialize_lite(height: int, bhash: str, block_raw: dict) -> str:
    """Byte-identical copy of the block indexer's lite serialization
    (indexer/main_loop.py ``_serialize_lite``) so RPC-sourced backfill
    blocks are indistinguishable from indexer-written ones."""
    return json.dumps({
        "height": height,
        "hash": bhash,
        "confirmations": block_raw.get("confirmations", 0),
        "time": block_raw.get("time", 0),
        "n_tx": block_raw.get("nTx", 0),
        "size": block_raw.get("size", 0),
        "weight": block_raw.get("weight"),
        "difficulty": block_raw.get("difficulty", 0.0),
        "version": block_raw.get("version", 0),
        "merkleroot": block_raw.get("merkleroot", ""),
        "bits": block_raw.get("bits", ""),
        "nonce": block_raw.get("nonce", 0),
        "previousblockhash": block_raw.get("previousblockhash"),
        "nextblockhash": block_raw.get("nextblockhash"),
        "txids": block_raw.get("tx", []),
    })


# ── Sources ──────────────────────────────────────────────────────────

class KvBlockSource(Protocol):
    async def get(self, height: int) -> Optional[Tuple[str, str]]: ...
    @property
    def name(self) -> str: ...


class HttpKvBlockSource:
    """Read block KV docs from the legacy nedbd HTTP store.

    This is a one-era bridge: the DualStore mirrored every KV write
    there for months, so most history can be copied across without
    touching the daemon. Best-effort — any failure is a miss.
    """

    name = "http"

    def __init__(self, http_store) -> None:
        self._http = http_store

    async def get(self, height: int) -> Optional[Tuple[str, str]]:
        try:
            bhash = await self._http.get(Keys.BLOCK_BY_HEIGHT.format(height=height))
            if not bhash:
                return None
            raw = await self._http.get(Keys.BLOCK_BY_HASH.format(hash=bhash))
            if not raw:
                return None
            return bhash, raw
        except Exception:
            return None


class SqliteKvBlockSource:
    """Read block KV docs from SQLite's KV cache (raw store, never
    through the get_db() override). Always complete for every block
    the indexer has ever processed."""

    name = "sqlite"

    def __init__(self, sqlite_store) -> None:
        self._sq = sqlite_store

    async def get(self, height: int) -> Optional[Tuple[str, str]]:
        bhash = await self._sq.get(Keys.BLOCK_BY_HEIGHT.format(height=height))
        if not bhash:
            return None
        raw = await self._sq.get(Keys.BLOCK_BY_HASH.format(hash=bhash))
        if not raw:
            return None
        return bhash, raw


class RpcLiteBlockSource:
    """Last resort: fetch from the daemon and serialize lite, exactly
    as the indexer's own fallback path does."""

    name = "rpc"

    def __init__(self, rpc_client) -> None:
        self._rpc_client = rpc_client

    async def get(self, height: int) -> Optional[Tuple[str, str]]:
        from ..rpc.methods import BlockchainRPC
        rpc = BlockchainRPC(self._rpc_client)
        bhash = await rpc.get_block_hash(height)
        block_raw = await rpc.get_block(bhash, verbosity=1)
        return bhash, serialize_lite(height, bhash, block_raw)


# ── Tip window seed ──────────────────────────────────────────────────

async def seed_tip_window(sqlite, embedded: EmbeddedNedbStore,
                          window: int = 100) -> dict:
    """Copy the live tip state + the most recent ``window`` blocks
    from SQLite's KV cache into the embedded store, and record the
    sync checkpoint. Idempotent; safe on every boot.

    Returns {"tip": int, "seeded": int, "lowest": int}.
    """
    tip_raw = await sqlite.get(Keys.TIP_HEIGHT)
    tip = int(tip_raw) if tip_raw else 0

    def _kv(key, value):
        return ("kv", key, {"_id": key, "value": str(value),
                            "expires_at": None})

    ops: list = []
    for key in (Keys.TIP_HEIGHT, Keys.TIP_HASH, Keys.INDEXER_STATUS,
                Keys.INDEXER_LAST_HEIGHT):
        val = await sqlite.get(key)
        if val is not None:
            ops.append(_kv(key, val))

    recent = await sqlite.zrevrange(
        Keys.RECENT_BLOCKS, 0, window - 1, withscores=True
    )
    seeded = 0
    lowest = tip
    for bhash, score in recent or []:
        h = int(score)
        raw = await sqlite.get(Keys.BLOCK_BY_HASH.format(hash=bhash))
        if raw is None:
            continue
        by_height = Keys.BLOCK_BY_HEIGHT.format(height=h)
        by_hash = Keys.BLOCK_BY_HASH.format(hash=bhash)
        zid = f"{Keys.RECENT_BLOCKS}::{bhash}"
        ops.append(_kv(by_height, bhash))
        ops.append(_kv(by_hash, raw))
        ops.append(("zset", zid, {
            "_id": zid, "_name": Keys.RECENT_BLOCKS,
            "_member": bhash, "score": float(h),
        }))
        seeded += 1
        lowest = min(lowest, h)

    existing_cp = await embedded.get(SYNC_CHECKPOINT_KEY)
    if existing_cp is None and tip > 0:
        ops.append(_kv(SYNC_CHECKPOINT_KEY, tip))
    existing_lowest = await embedded.get(SYNC_LOWEST_KEY)
    if existing_lowest is None:
        ops.append(_kv(SYNC_LOWEST_KEY, lowest if seeded else tip))
    elif seeded and lowest < int(existing_lowest):
        # A previous run may have synced lower; never raise the floor.
        ops.append(_kv(SYNC_LOWEST_KEY, lowest))

    if ops:
        await embedded.bulk_puts(ops)

    logger.info(
        "Embedded tip window seeded: tip=%d seeded=%d lowest=%d",
        tip, seeded, lowest,
    )
    _publish_status(
        checkpoint_height=tip,
        lowest_synced=lowest if seeded else tip,
    )
    return {"tip": tip, "seeded": seeded, "lowest": lowest}


# ── Backfill task ────────────────────────────────────────────────────

class EmbeddedBackfillTask:
    """Descends from the synced floor to genesis, copying block KV
    docs into the embedded store from the first source that has them.
    """

    def __init__(
        self,
        embedded: EmbeddedNedbStore,
        sources: list,
        batch_size: int = 100,
        sleep_ms: int = 50,
    ) -> None:
        self._nd = embedded
        self._sources = sources
        self._batch_size = batch_size
        self._sleep = sleep_ms / 1000.0
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self.running = False
        self.blocks_written = 0

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._on_done)
        logger.info(
            "EmbeddedBackfillTask started — sources=%s batch=%d",
            [s.name for s in self._sources], self._batch_size,
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    def _on_done(self, task: asyncio.Task) -> None:
        self.running = False
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("EmbeddedBackfillTask crashed: %s", exc, exc_info=exc)

    async def status(self) -> dict:
        state = await self._nd.get_sync_state()
        return {
            "running": self.running,
            "blocks_written": self.blocks_written,
            **state,
        }

    async def _fetch(self, height: int) -> Optional[Tuple[str, str]]:
        for src in self._sources:
            try:
                got = await src.get(height)
            except Exception:
                got = None
            if got is not None:
                return got
        return None

    async def _run(self) -> None:
        self.running = True
        _publish_status(running=True)
        try:
            state = await self._nd.get_sync_state()
            if state["complete"]:
                logger.info("Embedded backfill already complete — nothing to do")
                _publish_status(
                    checkpoint_height=state["checkpoint_height"],
                    lowest_synced=state["lowest_synced"],
                    pct=100.0, complete=True,
                )
                return
            cursor = state["lowest_synced"] - 1
            if cursor < 0:
                await self._nd.set(SYNC_COMPLETE_KEY, "1")
                _publish_status(pct=100.0, complete=True, running=False)
                return
            logger.info(
                "Embedded backfill: checkpoint=%d resuming at %d",
                state["checkpoint_height"], cursor,
            )
            checkpoint = state["checkpoint_height"]
            while cursor >= 0 and not self._stop_event.is_set():
                batch_low = max(0, cursor - self._batch_size + 1)
                heights = list(range(cursor, batch_low - 1, -1))
                fetched = await asyncio.gather(
                    *(self._fetch(h) for h in heights)
                )
                # Take the contiguous prefix (fetched is descending);
                # a hole stops the descent — contiguity is the
                # contract the UI's "synced range" relies on.
                items = []
                hole = False
                for h, got in zip(heights, fetched):
                    if got is None:
                        logger.warning(
                            "Embedded backfill: no source has block %d — "
                            "stopping (will resume here next start)", h,
                        )
                        hole = True
                        break
                    items.append((h, got[0], got[1]))
                if items:
                    # One deferred-sync batch per block batch: all
                    # block docs + the new floor land under one fsync.
                    ops = []
                    for h, bhash, raw in items:
                        by_height = Keys.BLOCK_BY_HEIGHT.format(height=h)
                        by_hash = Keys.BLOCK_BY_HASH.format(hash=bhash)
                        zid = f"{Keys.RECENT_BLOCKS}::{bhash}"
                        ops.append(("kv", by_height, {
                            "_id": by_height, "value": bhash,
                            "expires_at": None}))
                        ops.append(("kv", by_hash, {
                            "_id": by_hash, "value": raw,
                            "expires_at": None}))
                        ops.append(("zset", zid, {
                            "_id": zid, "_name": Keys.RECENT_BLOCKS,
                            "_member": bhash, "score": float(h)}))
                    floor = items[-1][0]
                    ops.append(("kv", SYNC_LOWEST_KEY, {
                        "_id": SYNC_LOWEST_KEY, "value": str(floor),
                        "expires_at": None}))
                    await self._nd.bulk_puts(ops)
                    self.blocks_written += len(items)
                if hole:
                    return
                cursor = batch_low - 1
                pct = (
                    round(max(0, checkpoint - cursor - 1) / checkpoint * 100, 2)
                    if checkpoint > 0 else 100.0
                )
                _publish_status(
                    checkpoint_height=checkpoint,
                    lowest_synced=cursor + 1,
                    pct=pct,
                    blocks_written=self.blocks_written,
                )
                if self._sleep:
                    await asyncio.sleep(self._sleep)
            if cursor < 0:
                await self._nd.set(SYNC_COMPLETE_KEY, "1")
                _publish_status(pct=100.0, complete=True, running=False)
                logger.info(
                    "Embedded backfill COMPLETE — %d blocks written",
                    self.blocks_written,
                )
        finally:
            self.running = False
            _publish_status(running=False, blocks_written=self.blocks_written)


# ── sync status cache (sync-readable for the health endpoint) ───────

_status_cache: dict = {
    "checkpoint_height": 0,
    "lowest_synced": 0,
    "pct": 0.0,
    "complete": False,
    "running": False,
    "blocks_written": 0,
}


def get_sync_status() -> dict:
    return dict(_status_cache)


def _publish_status(**kw) -> None:
    _status_cache.update(kw)


# ── module-level singleton ───────────────────────────────────────────

_task: Optional[EmbeddedBackfillTask] = None


def get_backfill_task() -> Optional[EmbeddedBackfillTask]:
    return _task


def set_backfill_task(task: Optional[EmbeddedBackfillTask]) -> None:
    global _task
    _task = task
