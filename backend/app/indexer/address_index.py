"""Address index — UTXO ledger and per-address tx history.

Walks blocks (verbosity=2 from the node), extracts addresses from output
``scriptPubKey`` entries, and resolves input addresses by looking up the
spent UTXO in our local table. The output is a self-sufficient address
database that the explorer queries directly — no ElectrumX required.

Per-block writes go through ``EmbeddedAddressIndexWriter`` on the
embedded NEDB engine so each block's UTXO mutations, address-tx
rows, indexed block-hash record, and ``last_height`` pointer land
together in a single storage job (or not at all). A crash mid-block
leaves the database exactly as it was before that block was
attempted; restart-resume is therefore safe and never requires a
rebuild.

Reorg handling: the loop self-detects drift on every iteration by
comparing its recorded per-height hashes (in ``address_index_blocks``)
against the chain's current hashes, and atomically rolls back to the
common ancestor before re-applying.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Iterable, Optional

from ..config import settings
from ..embedded_domain import get_address_index_writer
from ..rpc.client import RPCConnectionError, RPCError, get_rpc
from ..rpc.methods import BlockchainRPC
from ..sqlite_store import Keys, get_db

logger = logging.getLogger(__name__)


# Convert "value" (BTC float) → integer sats. We use round() to absorb the
# floating-point noise that bitcoind's verbose JSON inevitably introduces.
def _to_sats(value: float) -> int:
    return int(round(float(value) * 100_000_000))


def _vout_address(vout: dict) -> Optional[str]:
    """Return the canonical address for a vout, or None for nulldata/non-standard."""
    spk = vout.get("scriptPubKey") or {}
    # Bitcoin Core 0.21 emits both ``address`` (singular, new) and
    # ``addresses`` (legacy list). We prefer the singular form.
    addr = spk.get("address")
    if addr:
        return addr
    addrs = spk.get("addresses") or []
    if addrs:
        return addrs[0]
    return None


class AddressIndexer:
    """Maintains the UTXO + per-address-history tables."""

    def __init__(self):
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        # Read-only access to the shared kv connection (for status/INDEXER_STATUS).
        self._db = get_db()

    # ── public lifecycle ────────────────────────────────────────────────
    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run_backfill_loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    # ── status helpers ──────────────────────────────────────────────────
    async def _set_status(self, **fields) -> None:
        existing: dict = {}
        try:
            raw = await self._db.get(Keys.ADDRESS_INDEX_STATUS)
            if raw:
                existing = json.loads(raw)
        except Exception:
            pass
        existing.update(fields)
        try:
            await self._db.set(Keys.ADDRESS_INDEX_STATUS, json.dumps(existing))
        except Exception as e:
            logger.warning("Address index status update skipped (non-fatal): %s", e)

    async def get_last_height(self) -> int:
        return await get_address_index_writer().get_last_height()

    async def rollback_to(self, height: int) -> None:
        await get_address_index_writer().rollback_to(height)
        await self._set_status(rolled_back_to=height)

    # ── core: prepare and apply one block atomically ─────────────────────
    async def process_block(self, block: dict) -> None:
        """Extract address-level events from a verbosity=2 block and apply
        them atomically via the writer."""
        height = int(block["height"])
        block_hash = block["hash"]
        block_time = int(block.get("time", 0)) or None
        txs = block.get("tx") or []

        utxo_inserts: list = []
        spend_outpoints: list = []
        spend_owner: dict[tuple, str] = {}
        # The 'in' (received) history rows we can determine purely from this
        # block. The matching 'out' (spent) rows are constructed inside the
        # writer's transaction once spent UTXOs are looked up.
        in_rows: list = []

        for tx in txs:
            txid = tx["txid"]

            # Outputs → new UTXOs + 'in' history.
            for vout in tx.get("vout", []):
                addr = _vout_address(vout)
                if not addr:
                    continue  # OP_RETURN / non-standard — no address to credit
                value_sats = _to_sats(vout.get("value", 0))
                if value_sats <= 0:
                    continue
                utxo_inserts.append((txid, int(vout["n"]), addr, value_sats, height))
                in_rows.append(
                    (addr, txid, height, "in", value_sats, block_time)
                )

            # Inputs → outpoints to spend, plus a (prev outpoint → spending tx)
            # map so the writer can build the matching 'out' history rows.
            for vin in tx.get("vin", []):
                if "coinbase" in vin:
                    continue
                prev_txid = vin.get("txid")
                prev_vout = vin.get("vout")
                if prev_txid is None or prev_vout is None:
                    continue
                key = (prev_txid, int(prev_vout))
                spend_outpoints.append(key)
                spend_owner[key] = txid

        await get_address_index_writer().apply_block(
            height=height,
            block_hash=block_hash,
            utxo_inserts=utxo_inserts,
            spend_outpoints=spend_outpoints,
            in_rows=in_rows,
            spend_owner=spend_owner,
            block_time=block_time,
        )

    # ── reorg self-detection ────────────────────────────────────────────
    async def _check_for_reorg(self, rpc_client) -> None:
        """Compare our recorded hashes for the last REORG_DEPTH heights
        against the chain. On mismatch, roll back to the common ancestor.

        This piggybacks on its own per-height hash records (see
        ``address_index_blocks``) — no dependency on the block indexer's
        reorg path, so we self-correct even if anything else gets out of
        sync.
        """
        writer = get_address_index_writer()
        last = await writer.get_last_height()
        if last < 0:
            return
        depth = min(int(settings.REORG_DEPTH), last + 1)
        if depth <= 0:
            return
        heights = list(range(last - depth + 1, last + 1))

        try:
            on_chain = await rpc_client.call_batch(
                [("getblockhash", [h]) for h in heights]
            )
        except Exception as e:
            logger.warning("address_index reorg check: getblockhash batch failed: %s", e)
            return

        ours = await writer.get_indexed_hashes(heights)

        # Walk lowest → highest. Heights without a recorded hash are
        # *trusted* — they pre-date this table (legacy data) or were
        # written by an older code path. Reorg protection ramps back to
        # full strength once REORG_DEPTH new blocks have been applied.
        for h, on_chain_hash in sorted(zip(heights, on_chain), key=lambda x: x[0]):
            if isinstance(on_chain_hash, RPCError):
                continue
            our_hash = ours.get(h)
            if our_hash is None:
                continue  # legacy / migration window
            if our_hash != on_chain_hash:
                rollback_to = max(-1, h - 1)
                logger.warning(
                    "address_index: reorg detected at h=%d (had %s, now %s) — rolling back to %d",
                    h, our_hash, on_chain_hash, rollback_to,
                )
                await self.rollback_to(rollback_to)
                return

    # ── backfill loop ───────────────────────────────────────────────────
    async def run_backfill_loop(self) -> None:
        """Walk from last indexed height up to the chain tip."""
        rpc_client = get_rpc()
        rpc = BlockchainRPC(rpc_client)
        writer = get_address_index_writer()

        await self._set_status(phase="starting")

        # Wait until the block indexer has made progress so we don't double
        # the RPC load during a fresh sync.
        while not self._stop.is_set():
            try:
                raw = await self._db.get(Keys.INDEXER_STATUS)
                phase = json.loads(raw).get("phase") if raw else None
                if phase in ("live", "syncing"):
                    break
            except Exception:
                pass
            await asyncio.sleep(2)

        last = await writer.get_last_height()
        logger.info("Address index starting at height %d", last + 1)
        await self._set_status(phase="backfilling", last_height=last)

        progress_t0 = asyncio.get_event_loop().time()
        progress_h0 = last

        # On boot, do one reorg check before resuming. Cheap and worth it.
        await self._check_for_reorg(rpc_client)
        last = await writer.get_last_height()

        while not self._stop.is_set():
            try:
                tip_raw = await self._db.get(Keys.TIP_HEIGHT)
                tip = int(tip_raw) if tip_raw else 0
                if tip <= last:
                    # Caught up — verify recent hashes and idle a tick.
                    await self._check_for_reorg(rpc_client)
                    new_last = await writer.get_last_height()
                    if new_last < last:
                        # Reorg rolled us back; resume from new pointer.
                        last = new_last
                        continue
                    await self._set_status(phase="live", last_height=last, tip=tip)
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(),
                            timeout=settings.INDEXER_TICK_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue

                # Fetch blocks in a small concurrent batch. Verbosity=2 is
                # heavy (full tx data) so use a smaller concurrency than
                # the block indexer's lite fetches.
                batch_size = min(50, tip - last)
                heights = list(range(last + 1, last + 1 + batch_size))

                hashes = await rpc_client.call_batch(
                    [("getblockhash", [h]) for h in heights]
                )
                # Stop at first error — never advance past a hole.
                valid: list[tuple[int, str]] = []
                for h, res in zip(heights, hashes):
                    if isinstance(res, RPCError):
                        logger.warning(
                            "address_index: getblockhash(%d) failed: %s", h, res
                        )
                        break
                    valid.append((h, res))
                if not valid:
                    await asyncio.sleep(1)
                    continue

                sem = asyncio.Semaphore(4)

                async def _fetch(h: int, bh: str):
                    async with sem:
                        try:
                            blk = await rpc.get_block(bh, verbosity=2)
                            return h, blk, None
                        except Exception as e:
                            return h, None, e

                results = await asyncio.gather(*(_fetch(h, bh) for h, bh in valid))
                results.sort(key=lambda x: x[0])

                advanced = False
                for h, blk, err in results:
                    if err is not None:
                        logger.warning(
                            "address_index halting at h=%d: %s", h, err
                        )
                        break
                    try:
                        await self.process_block(blk)
                        advanced = True
                    except Exception as e:
                        logger.exception(
                            "address_index: process_block(%d) failed: %s", h, e
                        )
                        break

                if advanced:
                    # Re-read the authoritative pointer from the writer's
                    # connection (the same transaction that wrote the data).
                    last = await writer.get_last_height()
                    # Keep the WAL file from growing unbounded; a passive
                    # checkpoint lets readers finish before flushing pages.
                    await writer.checkpoint()
                    await self._set_status(
                        phase="backfilling" if last < tip else "live",
                        last_height=last,
                        tip=tip,
                    )
                    now = asyncio.get_event_loop().time()
                    elapsed = now - progress_t0
                    if elapsed >= 5.0:
                        rate = (last - progress_h0) / elapsed if elapsed else 0
                        remaining = max(0, tip - last)
                        eta = (remaining / rate) if rate > 0 else 0
                        logger.info(
                            "Address index %d/%d (%.2f%%) %.1f blk/s, ETA %ds",
                            last, tip,
                            (last / tip * 100) if tip else 0,
                            rate, int(eta),
                        )
                        progress_t0 = now
                        progress_h0 = last
                else:
                    # Made no progress; back off briefly.
                    await asyncio.sleep(1)

            except RPCConnectionError as e:
                await self._set_status(phase="rpc_offline")
                logger.warning("address_index: RPC offline: %s", e)
                try:
                    await rpc_client.reconnect()
                except Exception:
                    pass
                await asyncio.sleep(5)
            except Exception as e:
                logger.exception("address_index loop error: %s", e)
                await asyncio.sleep(2)


_singleton: Optional[AddressIndexer] = None


def get_address_indexer() -> AddressIndexer:
    global _singleton
    if _singleton is None:
        _singleton = AddressIndexer()
    return _singleton
