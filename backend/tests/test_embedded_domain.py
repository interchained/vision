"""Parity tests for the embedded-NEDB domain layer.

The domain surface (UTXO ledger, address history, pools, reward
snapshots/entries/blocks, address-index writer) used to live on
SQLite; these tests pin the embedded implementation
(app/embedded_domain.py) to the same semantics the SQLite store had.

Run from backend/:  python -m pytest tests/test_embedded_domain.py -q
"""
import asyncio
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.embedded_domain import (
    EmbeddedAddressIndexWriter, close_address_index_writer,
    get_address_index_writer, init_address_index_writer,
)
from app.embedded_store import EmbeddedNedbStore
from app.sqlite_store import Keys


@pytest.fixture()
def store(tmp_path):
    s = EmbeddedNedbStore(str(tmp_path / "embedded"))
    asyncio.run(s.open())
    yield s
    asyncio.run(s.aclose())


def run(coro):
    return asyncio.run(coro)


# ── UTXO ledger ──────────────────────────────────────────────────────

def test_utxo_add_spend(store):
    async def main():
        await store.utxo_add_batch([
            ("txA", 0, "addr1", 1000, 10),
            ("txA", 1, "addr2", 2000, 10),
            ("txB", 0, "addr1", 500, 11),
        ])
        # spend one existing + one unknown outpoint
        spent = await store.utxo_spend_batch([("txA", 0), ("txZZ", 9)])
        assert spent == [("txA", 0, "addr1", 1000, 10)]
        # re-spend is a no-op miss
        assert await store.utxo_spend_batch([("txA", 0)]) == []
        # REPLACE semantics on re-add (same outpoint, new value)
        await store.utxo_add_batch([("txA", 1, "addr2", 2500, 10)])
        utxos = await store.address_utxos("addr2")
        assert utxos == [
            {"txid": "txA", "vout": 1, "value_sats": 2500, "height": 10}]
        await store.commit()  # no-op, must not raise
    run(main())


def test_address_utxos_ordering(store):
    async def main():
        await store.utxo_add_batch([
            ("txB", 0, "addr1", 1, 20),
            ("txA", 0, "addr1", 2, 20),
            ("txC", 0, "addr1", 3, 30),
        ])
        got = await store.address_utxos("addr1")
        # height DESC, txid ASC tiebreak (SQLite ORDER BY)
        assert [(g["txid"], g["height"]) for g in got] == [
            ("txC", 30), ("txA", 20), ("txB", 20)]
    run(main())


# ── address history ──────────────────────────────────────────────────

def _seed_history(store):
    async def main():
        await store.address_tx_add_batch([
            # (address, txid, height, direction, value, block_time)
            ("addr1", "tx1", 100, "in", 700, 1000),
            ("addr1", "tx2", 101, "in", 300, 1001),
            ("addr1", "tx2", 101, "out", 100, 1001),   # self-spend leg
            ("addr1", "tx3", 102, "out", 50, None),
            ("addr2", "tx1", 100, "in", 999, 1000),    # other address
        ])
        await store.utxo_add_batch([
            ("tx1", 0, "addr1", 700, 100),
            ("tx2", 1, "addr1", 200, 101),
        ])
    run(main())


def test_address_balance_and_count(store):
    _seed_history(store)

    async def main():
        # balance = live UTXOs (700+200); received = sum of 'in' legs
        assert await store.address_balance("addr1") == (900, 1000)
        assert await store.address_balance("nobody") == (0, 0)
        # distinct txids: tx1, tx2, tx3
        assert await store.address_tx_count("addr1") == 3
        assert await store.address_first_last_height("addr1") == (100, 102)
        assert await store.address_first_last_height("nobody") == (None, None)
    run(main())


def test_address_txs_grouping_and_pagination(store):
    _seed_history(store)

    async def main():
        txs = await store.address_txs("addr1")
        assert [t["txid"] for t in txs] == ["tx3", "tx2", "tx1"]  # height DESC
        by_id = {t["txid"]: t for t in txs}
        assert by_id["tx2"]["in_sats"] == 300
        assert by_id["tx2"]["out_sats"] == 100
        assert by_id["tx2"]["block_time"] == 1001
        assert by_id["tx3"]["block_time"] is None   # MAX over all-NULL
        page = await store.address_txs("addr1", limit=1, offset=1)
        assert [t["txid"] for t in page] == ["tx2"]
    run(main())


def test_store_level_rollback(store):
    _seed_history(store)

    async def main():
        await store.address_index_rollback(100)
        # height 101/102 history gone, height-100 rows survive
        assert await store.address_tx_count("addr1") == 1
        assert await store.address_balance("addr1") == (700, 700)
    run(main())


# ── address-index writer (apply_block / rollback_to) ─────────────────

def _writer_for(store):
    return EmbeddedAddressIndexWriter(store)


def test_writer_apply_and_rollback_restores_utxo(store):
    async def main():
        w = _writer_for(store)
        assert await w.get_last_height() == -1
        # Block 10: coinbase pays addr1 500.
        await w.apply_block(
            height=10, block_hash="hash10",
            utxo_inserts=[("cb10", 0, "addr1", 500, 10)],
            spend_outpoints=[], in_rows=[("addr1", "cb10", 10, "in", 500, 100)],
            spend_owner={}, block_time=100,
        )
        assert await w.get_last_height() == 10
        assert await store.address_index_last_height() == 10
        assert await w.get_indexed_hashes([10, 11]) == {10: "hash10"}
        # Block 11: addr1 spends cb10:0, receives 300 change.
        await w.apply_block(
            height=11, block_hash="hash11",
            utxo_inserts=[("tx11", 1, "addr1", 300, 11)],
            spend_outpoints=[("cb10", 0)],
            in_rows=[("addr1", "tx11", 11, "in", 300, 101)],
            spend_owner={("cb10", 0): "tx11"}, block_time=101,
        )
        assert await store.address_balance("addr1") == (300, 800)
        txs = await store.address_txs("addr1")
        assert {t["txid"] for t in txs} == {"cb10", "tx11"}
        # Reorg: roll back to 10 — the spent UTXO must be restored
        # from the undo log, block-11 effects pruned, pointer rewound.
        await w.rollback_to(10)
        assert await w.get_last_height() == 10
        assert await store.address_balance("addr1") == (500, 500)
        assert await store.address_tx_count("addr1") == 1
        assert await w.get_indexed_hashes([11]) == {}
        await w.checkpoint()  # no-op parity call
    run(main())


def test_writer_singleton_roundtrip(store):
    async def main():
        w = init_address_index_writer(store)
        assert get_address_index_writer() is w
        await close_address_index_writer()
        with pytest.raises(RuntimeError):
            get_address_index_writer()
    run(main())


# ── pools ────────────────────────────────────────────────────────────

def test_pool_crud_and_find(store):
    async def main():
        a = await store.pool_create({
            "pool_name": "beta Pool", "payout_address": "itc1beta",
            "coinbase_tag": "/beta/", "status": "active",
        })
        b = await store.pool_create({
            "pool_name": "Alpha pool", "payout_address": "itc1alpha",
            "status": "active",
        })
        assert b == a + 1  # sequential ids
        got = await store.pool_get(a)
        assert got["pool_name"] == "beta Pool"
        assert got["coinbase_tag"] == "/beta/"
        assert got["created_at"] == got["updated_at"]
        assert set(got.keys()) == {
            "id", "pool_name", "coinbase_tag", "payout_address",
            "website", "contact_email", "discord", "telegram",
            "status", "created_at", "updated_at",
        }
        # list: pool_name COLLATE NOCASE ordering
        names = [p["pool_name"] for p in await store.pool_list()]
        assert names == ["Alpha pool", "beta Pool"]
        assert [p["pool_name"] for p in
                await store.pool_list(status="disabled")] == []
        # find by payout address
        hit = await store.pool_find_by_payout_address("itc1beta")
        assert hit["id"] == a
        assert await store.pool_find_by_payout_address(
            "itc1beta", exclude_id=a) is None
        # update
        assert await store.pool_update(a, {"status": "disabled"}) is True
        assert (await store.pool_get(a))["status"] == "disabled"
        assert await store.pool_update(9999, {"status": "active"}) is False
        assert await store.pool_update(a, {"bogus": 1}) is False
        # disabled pools don't match active_only lookups...
        assert await store.pool_find_by_payout_address("itc1beta") is None
        # ...but do match an explicit status set
        hit = await store.pool_find_by_payout_address(
            "itc1beta", statuses=["active", "disabled"])
        assert hit["id"] == a
    run(main())


def test_pool_active_payout_uniqueness(store):
    async def main():
        await store.pool_create({
            "pool_name": "One", "payout_address": "itc1dup",
            "status": "active",
        })
        # second ACTIVE pool with the same payout → IntegrityError,
        # exactly what the SQLite partial unique index raised
        with pytest.raises(sqlite3.IntegrityError):
            await store.pool_create({
                "pool_name": "Two", "payout_address": "itc1dup",
                "status": "active",
            })
        # a DISABLED pool may share the address
        await store.pool_create({
            "pool_name": "Three", "payout_address": "itc1dup",
            "status": "disabled",
        })
        # update path: activating pool Three must also collide
        three = await store.pool_find_by_payout_address(
            "itc1dup", statuses=["disabled"])
        with pytest.raises(sqlite3.IntegrityError):
            await store.pool_update(three["id"], {"status": "active"})
    run(main())


# ── snapshots ────────────────────────────────────────────────────────

def _snap_data(**kw):
    d = {
        "snapshot_name": "Week 1", "start_height": 100,
        "end_height": 200, "reward_per_block": "0.10301990",
        "status": "draft", "notes": None,
    }
    d.update(kw)
    return d


def test_snapshot_lifecycle(store):
    async def main():
        sid = await store.snapshot_create_guarded(_snap_data())
        assert sid is not None
        # guarded create: same live range rejected
        assert await store.snapshot_create_guarded(_snap_data()) is None
        assert await store.snapshot_range_exists(100, 200) is True
        assert await store.snapshot_range_exists(100, 201) is False
        snap = await store.snapshot_get(sid)
        assert snap["status"] == "draft"
        assert snap["total_reward"] == "0.00000000"
        assert snap["approved_at"] is None

        # mark_failed is CAS on draft
        assert await store.snapshot_mark_failed(sid, "boom") is True
        assert await store.snapshot_mark_failed(sid, "again") is False
        snap = await store.snapshot_get(sid)
        assert snap["status"] == "failed" and snap["notes"] == "boom"
        # failed snapshots free the range again
        assert await store.snapshot_range_exists(100, 200) is False
        sid2 = await store.snapshot_create_guarded(_snap_data())
        assert sid2 is not None and sid2 != sid

        # finalize CAS: draft → generated with totals
        assert await store.snapshot_finalize(
            sid2, total_blocks_scanned=101, total_blocks_matched=42,
            total_reward="4.32683580") is True
        assert await store.snapshot_finalize(
            sid2, total_blocks_scanned=1, total_blocks_matched=1,
            total_reward="0.1") is False   # no longer draft — untouched
        snap = await store.snapshot_get(sid2)
        assert snap["status"] == "generated"
        assert snap["total_blocks_matched"] == 42

        # set_status stamps approved_at / paid_at
        assert await store.snapshot_set_status(sid2, "approved") is True
        snap = await store.snapshot_get(sid2)
        assert snap["approved_at"] is not None and snap["paid_at"] is None
        assert await store.snapshot_set_totals(
            sid2, total_blocks_scanned=101, total_blocks_matched=43,
            total_reward="4.42985570") is True
        assert (await store.snapshot_get(sid2))["total_blocks_matched"] == 43
        assert await store.snapshot_set_status(9999, "paid") is False

        # list: newest first
        ids = [s["id"] for s in await store.snapshot_list()]
        assert ids == sorted(ids, reverse=True)

        # delete_by_status removes rows + children
        n = await store.snapshots_delete_by_status(["failed"])
        assert n == 1
        assert await store.snapshot_get(sid) is None
        assert await store.snapshot_delete(sid2) is True
        assert await store.snapshot_delete(sid2) is False
    run(main())


# ── entries + reward blocks ──────────────────────────────────────────

def test_entries_and_reward_blocks(store):
    async def main():
        sid = await store.snapshot_create(_snap_data())
        await store.entries_insert_batch([
            {"snapshot_id": sid, "pool_id": 1, "pool_name": "Beta",
             "payout_address": "itc1b", "blocks_found": 5,
             "reward_per_block": "0.10301990", "total_reward": "0.51509950"},
            {"snapshot_id": sid, "pool_id": 2, "pool_name": "alpha",
             "payout_address": "itc1a", "blocks_found": 9,
             "reward_per_block": "0.10301990", "total_reward": "0.92717910"},
        ])
        entries = await store.entries_list(sid)
        # blocks_found DESC, then pool_name NOCASE
        assert [e["pool_name"] for e in entries] == ["alpha", "Beta"]
        assert entries[0]["status"] == "pending"
        eid = entries[0]["id"]
        assert (await store.entry_get(eid))["payout_address"] == "itc1a"
        assert await store.entry_update(eid, status="approved") is True
        assert (await store.entry_get(eid))["status"] == "approved"
        assert await store.entry_update(eid) is False  # nothing to set
        assert await store.entries_set_status_all(sid, "paid") == 2
        assert {e["status"] for e in await store.entries_list(sid)} == {"paid"}

        # duplicate (snapshot_id, payout_address) → IntegrityError
        with pytest.raises(sqlite3.IntegrityError):
            await store.entries_insert_batch([
                {"snapshot_id": sid, "pool_id": 3, "pool_name": "Copy",
                 "payout_address": "itc1a", "blocks_found": 1,
                 "reward_per_block": "0.10301990",
                 "total_reward": "0.10301990"},
            ])

        await store.reward_blocks_insert_batch([
            {"snapshot_id": sid, "pool_id": 2, "block_height": 150,
             "block_hash": "h150", "coinbase_tag_detected": "/a/",
             "payout_address": "itc1a", "reward_amount": "0.10301990"},
            {"snapshot_id": sid, "pool_id": 1, "block_height": 120,
             "block_hash": "h120", "coinbase_tag_detected": None,
             "payout_address": "itc1b", "reward_amount": "0.10301990"},
        ])
        # INSERT OR IGNORE: the same triple again is dropped silently
        await store.reward_blocks_insert_batch([
            {"snapshot_id": sid, "pool_id": 2, "block_height": 150,
             "block_hash": "h150", "coinbase_tag_detected": "/a/",
             "payout_address": "itc1a", "reward_amount": "0.10301990"},
        ])
        blocks = await store.reward_blocks_list(sid)
        assert [b["block_height"] for b in blocks] == [120, 150]
        only_a = await store.reward_blocks_list(sid, pool_id=2)
        assert [b["block_height"] for b in only_a] == [150]

        # clear_results wipes children, keeps the snapshot
        await store.snapshot_clear_results(sid)
        assert await store.entries_list(sid) == []
        assert await store.reward_blocks_list(sid) == []
        assert await store.snapshot_get(sid) is not None
        # delete cascades too
        await store.entries_insert_batch([
            {"snapshot_id": sid, "pool_id": 1, "pool_name": "Beta",
             "payout_address": "itc1b", "blocks_found": 1,
             "reward_per_block": "0.10301990", "total_reward": "0.10301990"},
        ])
        assert await store.snapshot_delete(sid) is True
        assert await store.entries_list(sid) == []
    run(main())


# ── coinbase reward scan ─────────────────────────────────────────────

def test_coinbase_reward_scan(store):
    async def main():
        # Cached block docs for heights 10..12 (12 has no coinbase txid)
        for h, txids in ((10, ["cb10"]), (11, ["cb11"]), (12, [])):
            bhash = f"hash{h}"
            await store.set(Keys.BLOCK_BY_HEIGHT.format(height=h), bhash)
            await store.set(
                Keys.BLOCK_BY_HASH.format(hash=bhash),
                json.dumps({"height": h, "txids": txids}))
        await store.address_tx_add_batch([
            ("miner1", "cb10", 10, "in", 500, 100),
            ("miner2", "cb10", 10, "in", 250, 100),
            ("miner1", "cb11", 11, "in", 500, 101),
            ("miner1", "cb11", 11, "out", 500, 101),  # 'out' ignored
            ("miner3", "cb10", 10, "in", 0, 100),     # value 0 ignored
        ])
        rows, missing = await store.coinbase_reward_scan(10, 13)
        assert missing == [12, 13]   # 12: no txid; 13: no cached block
        got = {(r["height"], r["address"]): r["value"] for r in rows}
        assert got == {
            (10, "miner1"): 500, (10, "miner2"): 250, (11, "miner1"): 500,
        }
        assert all(r["coinbase_txid"].startswith("cb") for r in rows)
    run(main())


# ── config hardening ─────────────────────────────────────────────────

def test_store_mode_validation():
    from pydantic import ValidationError
    from app.config import Settings
    assert Settings().STORE_MODE == "embedded"
    with pytest.raises(ValidationError):
        Settings(STORE_MODE="sqlite")
    with pytest.raises(ValidationError):
        Settings(STORE_MODE="dual")


def test_disable_sqlite_kill_switch(monkeypatch):
    from app import sqlite_store
    from app.config import settings
    monkeypatch.setattr(settings, "DISABLE_SQLITE", "1")
    assert settings.sqlite_disabled is True
    with pytest.raises(RuntimeError):
        sqlite_store.SQLiteStore(None)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError):
        asyncio.run(sqlite_store.init_db())
    monkeypatch.setattr(settings, "DISABLE_SQLITE", "0")
    assert settings.sqlite_disabled is False
