"""nedb_backfill.py — Bi-directional block backfill into nedbd.

Reads lean block headers from Vision's SQLite cache (the historical source)
and writes them to nedbd's ``blocks`` collection — walking backwards from
the current tip to genesis while simultaneously tracking new blocks forward.

Design principles
-----------------
- Reads: direct to the underlying SQLiteStore (not through DualStore — the
  DualStore tries nedbd first which has nothing during backfill, causing
  a 3s timeout per read × 50k blocks = hours of timeouts).
- Writes: direct to nedbd via NedbStore._put() — no fallbacks, no silent
  swallowing. If nedbd is down, the write fails loudly and the block is
  retried on the next pass.
- Errors: logged at WARNING with the actual exception. The error counter is
  a real diagnostic, not a catch-all to hide problems.
- Cursor: persisted in nedbd's ``_backfill`` collection so restarts resume.

BlockSource protocol
--------------------
One async method: ``get(height: int) -> Optional[dict]``
Any object implementing it can be used as a source.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import List, Optional, Protocol

logger = logging.getLogger(__name__)

# Lean fields written to nedbd
LEAN_FIELDS = ("height", "hash", "prev_hash", "timestamp",
               "n_tx", "difficulty", "size", "weight")


def _lean(raw: dict, height: int, bhash: str) -> dict:
    doc: dict = {}
    for k in LEAN_FIELDS:
        if k in raw and raw[k] is not None:
            doc[k] = raw[k]
    # Normalise naming differences between Vision's cached block shapes
    if "height" not in doc:
        doc["height"] = height
    if "hash" not in doc:
        doc["hash"] = bhash
    if "prev_hash" not in doc and "previousblockhash" in raw:
        doc["prev_hash"] = raw["previousblockhash"]
    if "n_tx" not in doc and "nTx" in raw:
        doc["n_tx"] = raw["nTx"]
    return doc


# ── BlockSource protocol ─────────────────────────────────────────────────────

class BlockSource(Protocol):
    async def get(self, height: int) -> Optional[dict]: ...
    @property
    def name(self) -> str: ...


# ── SqliteBlockSource ────────────────────────────────────────────────────────

class SqliteBlockSource:
    """Read block headers directly from Vision's SQLite KV block cache.

    Always reads from the raw SQLiteStore — never through the DualStore.
    The DualStore would try nedbd first on every read, but nedbd has no
    historical blocks during backfill. That causes a 3s timeout per read
    which, at 50 blocks/batch, makes each batch take 5+ minutes.
    """
    name = "sqlite"

    def __init__(self, store) -> None:
        # Unwrap DualStore → underlying SQLiteStore
        try:
            from ..dual_store import DualStore
            self._sq = store._sq if isinstance(store, DualStore) else store
        except Exception:
            self._sq = store

    async def get(self, height: int) -> Optional[dict]:
        from ..sqlite_store import Keys
        bhash = await self._sq.get(Keys.BLOCK_BY_HEIGHT.format(height=height))
        if not bhash:
            return None
        raw_json = await self._sq.get(Keys.BLOCK_BY_HASH.format(hash=bhash))
        if not raw_json:
            return None
        raw = json.loads(raw_json)
        doc = _lean(raw, height, bhash)
        return doc if doc.get("hash") else None


# ── RpcBlockSource ───────────────────────────────────────────────────────────

class RpcBlockSource:
    """Fetch block headers directly from the ITC node JSON-RPC.

    Used as the fallback when SQLite doesn't have the block (e.g. during
    the initial sync window before the block indexer has cached it).
    """
    name = "rpc"

    def __init__(self, rpc_client) -> None:
        self._rpc = rpc_client

    async def get(self, height: int) -> Optional[dict]:
        from ..rpc.methods import BlockchainRPC
        rpc = BlockchainRPC(self._rpc)
        bhash = await rpc.get_block_hash(height)
        block = await rpc.get_block(bhash, verbosity=1)
        return {
            "height":     height,
            "hash":       bhash,
            "prev_hash":  block.get("previousblockhash"),
            "timestamp":  block.get("time", 0),
            "n_tx":       block.get("nTx", 0),
            "difficulty": float(block.get("difficulty", 0.0)),
            "size":       block.get("size", 0),
            "weight":     block.get("weight"),
        }


# ── NedbBackfillTask ─────────────────────────────────────────────────────────

class NedbBackfillTask:
    """Bi-directional block header backfill into nedbd's ``blocks`` collection.

    - Backward pass: tip → genesis, 50 blocks per batch, 200ms sleep.
    - Forward pass: picks up any new blocks since the task started.
    - Cursor persisted in nedbd so restarts resume without reprocessing.
    - Write errors logged at WARNING with the actual exception.
    """

    CURSOR_COLL = "_backfill"
    CURSOR_ID   = "blocks"

    def __init__(
        self,
        nedb,
        db:          str,
        sources:     List[BlockSource],
        *,
        batch_size:  int = 50,
        sleep_ms:    int = 200,
        collection:  str = "blocks",
    ) -> None:
        self._nd         = nedb
        self._db         = db
        self._sources    = sources
        self._batch_size = batch_size
        self._sleep_s    = sleep_ms / 1000.0
        self._collection = collection
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

        self.lowest_done:    int  = 0
        self.highest_done:   int  = 0
        self.blocks_written: int  = 0
        self.read_errors:    int  = 0
        self.write_errors:   int  = 0
        self.running:        bool = False
        self.complete:       bool = False

    # ── Public ───────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._on_done)
        logger.info("NedbBackfillTask started — db=%s col=%s batch=%d",
                    self._db, self._collection, self._batch_size)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    def status(self) -> dict:
        tip = self.highest_done
        pct = round((tip - self.lowest_done) / tip * 100, 2) if tip > 0 else 0.0
        return {
            "running":       self.running,
            "complete":      self.complete,
            "lowest_done":   self.lowest_done,
            "highest_done":  self.highest_done,
            "blocks_written": self.blocks_written,
            "backfill_pct":  pct,
            "read_errors":   self.read_errors,
            "write_errors":  self.write_errors,
        }

    # ── Internal ─────────────────────────────────────────────────────────────

    def _on_done(self, task: asyncio.Task) -> None:
        self.running = False
        if task.cancelled():
            logger.info("NedbBackfillTask cancelled")
        elif (exc := task.exception()):
            logger.error("NedbBackfillTask crashed: %s", exc, exc_info=exc)
        else:
            logger.info("NedbBackfillTask complete — %d blocks written to nedbd",
                        self.blocks_written)

    async def _load_cursor(self) -> dict:
        try:
            rows = await self._nd._query(
                f'FROM {self.CURSOR_COLL} WHERE _id = "{self.CURSOR_ID}" LIMIT 1'
            )
            if rows.get("rows"):
                return rows["rows"][0]
        except Exception as e:
            logger.debug("backfill cursor load failed (first run?): %s", e)
        return {"lowest_done": -1, "highest_done": -1, "total": 0}

    async def _save_cursor(self) -> None:
        try:
            await self._nd._put(
                self.CURSOR_COLL, self.CURSOR_ID,
                {
                    "lowest_done":  self.lowest_done,
                    "highest_done": self.highest_done,
                    "total":        self.blocks_written,
                },
            )
        except Exception as e:
            logger.warning("backfill cursor save failed: %s", e)

    async def _fetch_block(self, height: int) -> Optional[dict]:
        """Try each source in priority order; log on all failures."""
        last_exc = None
        for source in self._sources:
            try:
                block = await source.get(height)
                if block:
                    return block
            except Exception as e:
                last_exc = e
                logger.debug("backfill source=%s h=%d: %s", source.name, height, e)
        if last_exc:
            logger.warning("backfill: could not fetch h=%d from any source: %s",
                           height, last_exc)
        self.read_errors += 1
        return None

    async def _write_block(self, block: dict) -> bool:
        height = block.get("height", 0)
        # caused_by must be a list of doc IDs — nedbd does list(caused_by)
        # so passing a bare string would split it into individual characters.
        caused_by = [str(height - 1)] if height > 0 else None
        try:
            await self._nd._put(
                self._collection,
                str(height),
                block,
                caused_by=caused_by,
            )
            return True
        except Exception as e:
            # Log exception TYPE to prove causality — ReadTimeout means the
            # response read timed out (nedbd fsync too slow for the 3s read
            # timeout on a shared client). Fix: write client uses read=30s.
            logger.warning(
                "backfill write h=%d failed: %s: %s",
                height, type(e).__name__, e or "(no message)",
            )
            self.write_errors += 1
            return False

    async def _get_tip(self) -> int:
        """Get tip from SQLite directly (fastest, no nedbd round-trip)."""
        for source in self._sources:
            if isinstance(source, SqliteBlockSource):
                try:
                    from ..sqlite_store import Keys
                    val = await source._sq.get(Keys.TIP_HEIGHT)
                    if val:
                        return int(val)
                except Exception:
                    pass
        return 0

    async def _run(self) -> None:
        self.running = True

        cursor = await self._load_cursor()
        self.lowest_done  = cursor.get("lowest_done",  -1)
        self.highest_done = cursor.get("highest_done", -1)
        self.blocks_written = cursor.get("total",       0)

        tip = await self._get_tip()
        if tip == 0:
            logger.error("NedbBackfillTask: cannot determine tip height — aborting")
            return

        if self.highest_done < 0:
            self.highest_done = tip
            self.lowest_done  = tip

        logger.info("NedbBackfillTask resuming — tip=%d lowest_done=%d "
                    "already_written=%d", tip, self.lowest_done, self.blocks_written)

        current = self.lowest_done - 1

        while not self._stop_event.is_set() and current >= 0:
            batch_written = 0
            batch_start   = current

            for h in range(current, max(current - self._batch_size, -1), -1):
                if self._stop_event.is_set():
                    break

                block = await self._fetch_block(h)
                if block:
                    if await self._write_block(block):
                        batch_written    += 1
                        self.blocks_written += 1
                self.lowest_done = h  # advance cursor regardless of outcome

            current = self.lowest_done - 1

            if batch_written > 0:
                await self._save_cursor()

            if batch_written > 0 or (self.blocks_written % 500 == 0 and self.blocks_written > 0):
                logger.info(
                    "NedbBackfillTask: batch h=%d→%d wrote=%d total=%d "
                    "read_err=%d write_err=%d",
                    batch_start, self.lowest_done,
                    batch_written, self.blocks_written,
                    self.read_errors, self.write_errors,
                )

            # Forward pass: pick up any new blocks since we started
            new_tip = await self._get_tip()
            if new_tip > self.highest_done:
                for h in range(self.highest_done + 1, new_tip + 1):
                    if self._stop_event.is_set():
                        break
                    block = await self._fetch_block(h)
                    if block and await self._write_block(block):
                        self.blocks_written += 1
                        self.highest_done = h

            await asyncio.sleep(self._sleep_s)

        self.complete = (current < 0)
        await self._save_cursor()
        logger.info(
            "NedbBackfillTask finished — complete=%s lowest=%d "
            "written=%d read_err=%d write_err=%d",
            self.complete, self.lowest_done,
            self.blocks_written, self.read_errors, self.write_errors,
        )


# ── Singleton ────────────────────────────────────────────────────────────────

_backfill_task: Optional[NedbBackfillTask] = None


def get_backfill_task() -> Optional[NedbBackfillTask]:
    return _backfill_task


def set_backfill_task(task: NedbBackfillTask) -> None:
    global _backfill_task
    _backfill_task = task
