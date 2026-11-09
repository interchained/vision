"""Tests for the embedded NEDB store + tip-first backfill.

Run from backend/:  python -m pytest tests/test_embedded_store.py -q
"""
import asyncio
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.embedded_store import (
    EmbeddedNedbStore, EmbeddedStore,
    SYNC_CHECKPOINT_KEY, SYNC_COMPLETE_KEY, SYNC_LOWEST_KEY,
)
from app.indexer.embedded_backfill import (
    EmbeddedBackfillTask, SqliteKvBlockSource, seed_tip_window,
    serialize_lite,
)
from app.sqlite_store import Keys


@pytest.fixture()
def store(tmp_path):
    s = EmbeddedNedbStore(str(tmp_path / "embedded"))
    asyncio.run(s.open())
    yield s
    asyncio.run(s.aclose())


def run(coro):
    return asyncio.run(coro)


# ── KV semantics ─────────────────────────────────────────────────────

def test_kv_set_get_delete(store):
    async def main():
        assert await store.get("k1") is None
        await store.set("k1", "v1")
        assert await store.get("k1") == "v1"
        await store.set("k1", 42)              # values stringified
        assert await store.get("k1") == "42"
        assert await store.delete("k1") == 1
        assert await store.get("k1") is None
        assert await store.delete("k1") == 0    # already gone
    run(main())


def test_kv_ttl(store):
    async def main():
        await store.set("temp", "x", ex=1)
        assert await store.get("temp") == "x"
        # Force-expire by rewriting expires_at into the past.
        await store.expire("temp", -10)
        assert await store.get("temp") is None
        await store.expire("missing", 100)      # no-op, must not raise
    run(main())


def test_incr(store):
    async def main():
        assert await store.incr("n") == 1
        assert await store.incr("n") == 2
        await store.set("n", "notanint")
        assert await store.incr("n") == 1
    run(main())


def test_delete_sweeps_groups(store):
    async def main():
        await store.zadd("grp", {"a": 1})
        await store.sadd("grp", "a")
        await store.delete("grp")
        assert await store.zrevrange("grp", 0, -1) == []
        assert await store.smembers("grp") == set()
    run(main())


# ── zset / set semantics ─────────────────────────────────────────────

def test_zset_ordering_and_pagination(store):
    async def main():
        await store.zadd("recent", {"h1": 10, "h2": 30, "h3": 20})
        assert await store.zrevrange("recent", 0, -1) == ["h2", "h3", "h1"]
        assert await store.zrevrange("recent", 0, 1) == ["h2", "h3"]
        assert await store.zrevrange("recent", 1, -1) == ["h3", "h1"]
        ws = await store.zrevrange("recent", 0, 0, withscores=True)
        assert ws == [("h2", 30.0)]
        # re-adding a member updates its score
        await store.zadd("recent", {"h1": 99})
        assert await store.zrevrange("recent", 0, 0) == ["h1"]
    run(main())


def test_zremrangebyrank_keeps_top_n(store):
    async def main():
        await store.zadd("recent", {f"h{i}": i for i in range(10)})
        removed = await store.zremrangebyrank("recent", 0, -4)  # keep top 3
        assert removed == 7
        assert await store.zrevrange("recent", 0, -1) == ["h9", "h8", "h7"]
    run(main())


def test_set_ops(store):
    async def main():
        assert await store.sadd("s", "a", "b") == 2
        assert await store.smembers("s") == {"a", "b"}
        assert await store.srem("s", "a") == 1
        assert await store.smembers("s") == {"b"}
        assert await store.srem("s", "zzz") == 0
    run(main())


# ── sync state ───────────────────────────────────────────────────────

def test_sync_state_roundtrip(store):
    async def main():
        st = await store.get_sync_state()
        assert st["complete"] is False and st["checkpoint_height"] == 0
        await store.set(SYNC_CHECKPOINT_KEY, "1000")
        await store.set(SYNC_LOWEST_KEY, "250")
        st = await store.get_sync_state()
        assert st["checkpoint_height"] == 1000
        assert st["lowest_synced"] == 250
        assert st["pct"] == 75.0
        await store.set(SYNC_COMPLETE_KEY, "1")
        assert (await store.get_sync_state())["pct"] == 100.0
    run(main())


# ── fake KV source (stands in for SQLite / old nedbd) ────────────────

class FakeKv:
    """Minimal get/zrevrange store with the same shapes as the real
    SQLiteStore KV surface, for seed/backfill tests."""

    def __init__(self):
        self.kv = {}
        self.zsets = {}

    async def get(self, key):
        return self.kv.get(key)

    async def set(self, key, value, ex=None):
        self.kv[key] = str(value)

    async def zadd(self, name, mapping):
        self.zsets.setdefault(name, {}).update(
            {str(k): float(v) for k, v in mapping.items()})
        return len(mapping)

    async def zrevrange(self, name, start, stop, withscores=False):
        items = sorted(self.zsets.get(name, {}).items(),
                       key=lambda kv: kv[1], reverse=True)
        sliced = items[start:] if stop < 0 else items[start:stop + 1]
        if withscores:
            return [(m, s) for m, s in sliced]
        return [m for m, _ in sliced]


def _seed_chain(fake: FakeKv, tip: int, genesis: int = 0):
    for h in range(genesis, tip + 1):
        bhash = f"hash{h:04d}"
        fake.kv[Keys.BLOCK_BY_HEIGHT.format(height=h)] = bhash
        fake.kv[Keys.BLOCK_BY_HASH.format(hash=bhash)] = json.dumps(
            {"height": h, "hash": bhash, "time": 1000 + h})
        fake.zsets.setdefault(Keys.RECENT_BLOCKS, {})[bhash] = float(h)
    fake.kv[Keys.TIP_HEIGHT] = str(tip)
    fake.kv[Keys.TIP_HASH] = f"hash{tip:04d}"


def test_seed_tip_window(store, tmp_path):
    async def main():
        fake = FakeKv()
        _seed_chain(fake, tip=50)
        out = await seed_tip_window(fake, store, window=10)
        assert out["tip"] == 50 and out["seeded"] == 10
        # tip keys live immediately
        assert await store.get(Keys.TIP_HEIGHT) == "50"
        assert await store.get(
            Keys.BLOCK_BY_HEIGHT.format(height=50)) == "hash0050"
        # height 40 is below the seeded window — not present yet
        assert await store.get(
            Keys.BLOCK_BY_HEIGHT.format(height=40)) is None
        state = await store.get_sync_state()
        assert state["checkpoint_height"] == 50
        assert state["lowest_synced"] == 41
    run(main())


def test_backfill_descends_and_completes(store):
    async def main():
        fake = FakeKv()
        _seed_chain(fake, tip=30)
        await seed_tip_window(fake, store, window=5)   # floor = 26
        task = EmbeddedBackfillTask(
            store, [SqliteKvBlockSource(fake)], batch_size=10, sleep_ms=0)
        await task.start()
        assert task._task is not None
        await asyncio.wait_for(task._task, timeout=30)
        state = await store.get_sync_state()
        assert state["complete"] is True
        assert state["lowest_synced"] == 0
        # every height now served from the embedded store
        for h in (0, 17, 25, 30):
            assert await store.get(
                Keys.BLOCK_BY_HEIGHT.format(height=h)) == f"hash{h:04d}"
        raw = await store.get(Keys.BLOCK_BY_HASH.format(hash="hash0017"))
        assert json.loads(raw)["height"] == 17
    run(main())


def test_backfill_stops_at_hole(store):
    async def main():
        fake = FakeKv()
        _seed_chain(fake, tip=20)
        # Punch a hole: height 12 missing from every source.
        del fake.kv[Keys.BLOCK_BY_HEIGHT.format(height=12)]
        await seed_tip_window(fake, store, window=3)   # floor = 18
        task = EmbeddedBackfillTask(
            store, [SqliteKvBlockSource(fake)], batch_size=5, sleep_ms=0)
        await task.start()
        await asyncio.wait_for(task._task, timeout=30)
        state = await store.get_sync_state()
        assert state["complete"] is False
        assert state["lowest_synced"] == 13   # stopped above the hole
    run(main())


def test_bulk_puts(store):
    async def main():
        ops = [
            ("kv", f"k{i}", {"_id": f"k{i}", "value": str(i),
                             "expires_at": None})
            for i in range(50)
        ]
        assert await store.bulk_puts(ops) == 50
        assert await store.get("k49") == "49"
        # durable across reopen
    run(main())
    async def again():
        s2 = EmbeddedNedbStore(store._path)
        await s2.open()
        assert await s2.get("k0") == "0"
        await s2.aclose()
    run(again())


def test_peek_sync_read(store):
    """peek(): the synchronous engine read /api/health relies on."""
    async def main():
        assert store.peek("nope") is None
        await store.set("vision:indexer:last_height", "914655")
        assert store.peek("vision:indexer:last_height") == "914655"
        await store.set("temp", "x", ex=1)
        assert store.peek("temp") == "x"
        await store.expire("temp", -5)
        assert store.peek("temp") is None
    run(main())


def test_serialize_lite_shape():
    raw = {"confirmations": 3, "time": 99, "nTx": 2, "size": 250,
           "weight": 900, "difficulty": 1.5, "version": 0x20004000,
           "merkleroot": "ab", "bits": "1e0ffff0", "nonce": 7,
           "previousblockhash": "p", "nextblockhash": None,
           "tx": ["t1", "t2"]}
    doc = json.loads(serialize_lite(123, "deadbeef", raw))
    assert doc["height"] == 123 and doc["hash"] == "deadbeef"
    assert doc["n_tx"] == 2 and doc["txids"] == ["t1", "t2"]
    assert doc["version"] == 0x20004000


def test_embedded_store_delegation(tmp_path):
    """EmbeddedStore: KV → embedded NEDB, domain → sqlite side."""
    async def main():
        emb = EmbeddedNedbStore(str(tmp_path / "embedded"))
        await emb.open()

        class FakeSqlite:
            async def address_balance(self, address):
                return (123, 1)

            async def pool_list(self, *a, **k):
                return [{"id": 1}]

        store = EmbeddedStore(FakeSqlite(), emb)
        await store.set("k", "v")
        assert await emb.get("k") == "v"          # KV landed embedded-side
        assert await store.address_balance("itc1") == (123, 1)
        assert await store.pool_list() == [{"id": 1}]
        assert store.stats() == {"active": True, "store": "embedded"}
        await emb.aclose()
    run(main())
