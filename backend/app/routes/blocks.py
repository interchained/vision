from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query

from ..models.block import Block
from ..sqlite_store import Keys, get_db as get_redis
from ..rpc.client import get_rpc, RPCConnectionError, RPCError
from ..rpc.methods import BlockchainRPC
from ..services.blocks import shape_block

router = APIRouter()


def _is_block_hash(s: str) -> bool:
    return len(s) == 64 and all(c in "0123456789abcdefABCDEF" for c in s)


@router.get("/blocks/tip")
async def tip():
    rpc = BlockchainRPC(get_rpc())
    redis = get_redis()
    # Always prefer the live node — it's the source of truth regardless of
    # whether the local block indexer has caught up.
    try:
        height = await rpc.get_block_count()
        bhash = await rpc.get_best_block_hash()
        return {"height": height, "hash": bhash}
    except Exception:
        pass
    # Fallback: DB cache (e.g., node temporarily unreachable)
    h = await redis.get(Keys.TIP_HEIGHT)
    if h is not None:
        return {"height": int(h), "hash": await redis.get(Keys.TIP_HASH)}
    raise HTTPException(503, "Chain tip unavailable")


@router.get("/blocks")
async def list_blocks(
    limit: int = Query(20, ge=1, le=100),
    before_height: int | None = Query(None),
):
    import asyncio

    rpc = BlockchainRPC(get_rpc())
    redis = get_redis()
    tip_height_raw = await redis.get(Keys.TIP_HEIGHT)
    tip_height = int(tip_height_raw) if tip_height_raw else await rpc.get_block_count()

    end = before_height - 1 if before_height else tip_height
    start = max(0, end - limit + 1)
    heights = list(range(end, start - 1, -1))

    # Fetch all height→hash lookups concurrently instead of sequentially.
    # Sequential reads through DualStore cost one nedbd round-trip per block
    # (60+ HTTP calls at limit=30). Gather collapses them in parallel.
    hashes = await asyncio.gather(
        *[redis.get(Keys.BLOCK_BY_HEIGHT.format(height=h)) for h in heights],
        return_exceptions=True,
    )

    # Fetch full block JSON for whichever heights returned a hash, concurrently.
    async def _fetch_block_data(h: int, cached_hash):
        if isinstance(cached_hash, Exception) or not cached_hash:
            # Fall back to RPC for uncached blocks
            try:
                bhash = await rpc.get_block_hash(h)
                block_raw = await rpc.get_block(bhash, verbosity=1)
                return {
                    "height": h,
                    "hash": bhash,
                    "time": block_raw.get("time"),
                    "tx_count": block_raw.get("nTx"),
                    "size": block_raw.get("size"),
                    "weight": block_raw.get("weight"),
                    "miner": None,
                    "miner_address": None,
                }
            except Exception:
                return None

        cached = await redis.get(Keys.BLOCK_BY_HASH.format(hash=cached_hash))
        if not cached:
            try:
                bhash = await rpc.get_block_hash(h)
                block_raw = await rpc.get_block(bhash, verbosity=1)
                return {
                    "height": h,
                    "hash": bhash,
                    "time": block_raw.get("time"),
                    "tx_count": block_raw.get("nTx"),
                    "size": block_raw.get("size"),
                    "weight": block_raw.get("weight"),
                    "miner": None,
                    "miner_address": None,
                }
            except Exception:
                return None

        b = json.loads(cached)
        cb = b.get("coinbase") or {}
        return {
            "height": b["height"],
            "hash": b["hash"],
            "time": b["time"],
            "tx_count": b["n_tx"],
            "size": b["size"],
            "weight": b.get("weight"),
            "miner": cb.get("miner"),
            "miner_address": cb.get("address"),
        }

    results = await asyncio.gather(
        *[_fetch_block_data(h, hsh) for h, hsh in zip(heights, hashes)],
        return_exceptions=True,
    )

    items = [r for r in results if r and not isinstance(r, Exception)]
    return {"items": items, "tip_height": tip_height, "next_before_height": start - 1 if start > 0 else None}


@router.get("/block/{id}", response_model=Block)
async def get_block(id: str):
    rpc = BlockchainRPC(get_rpc())
    redis = get_redis()
    bhash: str
    if _is_block_hash(id):
        bhash = id
    else:
        try:
            height = int(id)
        except ValueError:
            raise HTTPException(400, "Block id must be a height (int) or hash (64-hex)")
        cached_hash = await redis.get(Keys.BLOCK_BY_HEIGHT.format(height=height))
        try:
            bhash = cached_hash or await rpc.get_block_hash(height)
        except RPCConnectionError as e:
            raise HTTPException(503, f"Node unreachable: {e}") from e
        except RPCError as e:
            raise HTTPException(503, f"Node error: {e}") from e

    cached = await redis.get(Keys.BLOCK_BY_HASH.format(hash=bhash))
    if cached:
        return json.loads(cached)

    try:
        block_raw = await rpc.get_block(bhash, verbosity=2)
    except RPCConnectionError as e:
        raise HTTPException(503, f"Node unreachable: {e}") from e
    except RPCError as e:
        raise HTTPException(503, f"Node error: {e}") from e

    tip_raw = await redis.get(Keys.TIP_HEIGHT)
    tip = int(tip_raw) if tip_raw else block_raw.get("height", 0)
    block = await shape_block(rpc, block_raw, tip)
    await redis.set(Keys.BLOCK_BY_HASH.format(hash=bhash), block.model_dump_json(), ex=300)
    return block
