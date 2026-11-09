"""Route test: /block/{id} must serve lite-shaped cache docs.

While the block indexer is catching up it caches "lite" block docs
that lack version_hex; the detail route used to 500 on them via
response-model validation. The full (shaped) doc must also pass
through unchanged.

Run from backend/:  python -m pytest tests/test_block_route.py -q
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.indexer.embedded_backfill import serialize_lite
from app.routes import blocks as blocks_route
from app.sqlite_store import Keys


class FakeStore:
    def __init__(self, kv):
        self.kv = kv

    async def get(self, key):
        return self.kv.get(key)

    async def set(self, key, value, ex=None):
        self.kv[key] = str(value)


def _get_block(store, ident):
    blocks_route.get_redis = lambda: store

    async def _no_rpc(*a, **k):
        raise AssertionError("RPC must not be called for cached blocks")

    orig_fetch = blocks_route.get_block
    return asyncio.run(orig_fetch(ident))


def test_block_route_serves_lite_doc():
    h, bhash = 17, f"{17:064x}"
    raw = {"confirmations": 5, "time": 1700001020, "nTx": 2, "size": 285,
           "weight": 1140, "difficulty": 1.5, "version": 0x20004000,
           "merkleroot": "ab", "bits": "1e0ffff0", "nonce": 7,
           "previousblockhash": "p", "nextblockhash": None,
           "tx": ["t1", "t2"]}
    lite = serialize_lite(h, bhash, raw)
    assert "version_hex" not in json.loads(lite)   # the gap being fixed
    store = FakeStore({
        Keys.BLOCK_BY_HEIGHT.format(height=h): bhash,
        Keys.BLOCK_BY_HASH.format(hash=bhash): lite,
    })
    block = _get_block(store, str(h))
    assert block.height == h and block.hash == bhash
    assert block.version_hex == "20004000"
    assert block.n_tx == 2 and block.txids == ["t1", "t2"]


def test_block_route_serves_shaped_doc():
    h, bhash = 600, f"{600:064x}"
    shaped = {
        "height": h, "hash": bhash, "confirmations": 1,
        "version": 0x20004000, "version_hex": "20004000",
        "merkleroot": "cd", "time": 1700036000, "nonce": 9,
        "bits": "1e0ffff0", "difficulty": 2.5, "n_tx": 1,
        "size": 300, "weight": 1200, "txids": ["x"],
    }
    store = FakeStore({
        Keys.BLOCK_BY_HEIGHT.format(height=h): bhash,
        Keys.BLOCK_BY_HASH.format(hash=bhash): json.dumps(shaped),
    })
    block = _get_block(store, bhash)   # by hash this time
    assert block.height == h and block.version_hex == "20004000"
    assert block.difficulty == 2.5
