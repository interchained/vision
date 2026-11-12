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

The domain surface (utxo_*, address_*, pool_*, snapshot_*,
entries_*, reward_blocks_*) also lives on the embedded engine —
see ``embedded_domain.EmbeddedDomainMixin``, which
``EmbeddedNedbStore`` inherits. SQLite is no longer in the live
path at all: it remains only as the offline backup / migration
source (scripts/migrate_sqlite_to_nedb.py,
scripts/migrate_domain_sqlite_to_nedb.py).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional, Sequence, Tuple

from .embedded_domain import EmbeddedDomainMixin

logger = logging.getLogger(__name__)

# Sync-state keys, stored in the embedded store itself so the sync
# checkpoint survives restarts and is readable by any consumer.
SYNC_CHECKPOINT_KEY = "vision:sync:checkpoint_height"
SYNC_LOWEST_KEY = "vision:sync:lowest_synced"
SYNC_COMPLETE_KEY = "vision:sync:complete"


def _nql_escape(s: str) -> str:
    """Escape a string for embedding in an NQL double-quoted literal."""
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


# ── bounded open ─────────────────────────────────────────────────────
#
# Why this exists: the engine's stock NEDB(path) open is O(history).
# _load() parses EVERY AOF line into an in-memory Op (payload dict
# included, kept forever in log.ops) and folds every op into the
# in-memory MVCC store; then _open() unconditionally runs
# _self_heal_if_needed(), whose log.verify() re-canonicalizes and
# re-hashes EVERY op. None of that is skipped when a snapshot
# exists — the stock snapshot branch still parses the whole AOF and
# still verifies the whole chain. Measured on a 200k-doc repro dir
# (183 MB AOF): stock open 13.9 s / 566 MB, and WITH a checkpoint
# snapshot present 13.3 s / 720 MB. Production holds ~9x that
# (~1.8M docs), which is how the backend OOM-killed at ~5 GB RSS on
# the 16 GB production box while "just opening" its data dir.
#
# The fix: checkpoint_embedded_db() takes an engine checkpoint and
# records a sidecar (vision-open.json) beside snapshot.json with
# the checkpoint seq, the head hash, the AOF byte offset just past
# the checkpoint op, and the snapshot size. _BoundedNEDB._load(),
# when the sidecar is provably consistent (snapshot size matches,
# AOF only grew, and the first delta op chains from the recorded
# head — a hash check, not a guess), restores state via the
# engine's own load_snapshot(), seeks the AOF to the recorded
# offset, and parses ONLY the delta ops. The in-memory log is
# padded with one shared inert placeholder per pre-checkpoint op
# so len(log) — hence the seq of every future append, and db.seq —
# stays globally correct; nonce/idem tables are merged back from
# the snapshot so replay protection survives intact.
#
# Bounded open cost tracks LIVE STATE (the snapshot) + ops since
# the checkpoint, not total history: stock open grows with HISTORY
# (every op ever written, incl. rewrites of the same keys), the
# bounded open only with the state that history produced. Same
# repro dir after conversion: 3.4 s / 573 MB (vs 13.9 s / 566 MB
# stock at this scale — RSS is state-dominated here; at
# production's ~9x the stock open reaches the observed ~5 GB
# while the bounded open stays state-sized).
#
# Semantics kept: every read/write Vision issues, MVCC head state,
# seq allocation, idempotency, replay protection. Semantics
# narrowed — the same narrowing the engine's own snapshot format
# already documents ("pre-checkpoint time-travel is not supported
# without the full AOF"): in this runtime instance the
# pre-checkpoint op payloads are not resident, so AS OF /
# changefeed reads over pre-checkpoint history need an offline
# stock open. Vision never issues those against the embedded
# store (its AS OF routes proxy to a separate nedbd over HTTP).
#
# _self_heal_if_needed() is disabled in the subclass: it exists to
# repair a historical ENCRYPT-backfill chain break, Vision's dirs
# are unencrypted, and its unconditional full-chain verify() is
# the most expensive single phase of stock open. Full integrity
# verification remains one command away — scripts/
# checkpoint_embedded.py opens with the STOCK engine (heal +
# verify included) and is the audited conversion path.

_OPEN_SIDECAR = "vision-open.json"

_bounded_cls = None


def checkpoint_embedded_db(db, path: str) -> dict:
    """Take an engine checkpoint on ``db`` and record the
    bounded-open sidecar next to it. Returns the sidecar dict.

    The AOF offset is captured after db.checkpoint() returns (the
    engine flushes the checkpoint op to disk first), so it points
    exactly at the first post-checkpoint op line.
    """
    head = db.checkpoint()
    sidecar = {
        "version": 1,
        "checkpoint_seq": len(db.log.ops) - 1,
        "head": head,
        "aof_offset": os.path.getsize(os.path.join(path, "log.aof")),
        "snapshot_bytes": os.path.getsize(
            os.path.join(path, "snapshot.json")),
    }
    tmp = os.path.join(path, _OPEN_SIDECAR + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(sidecar, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, os.path.join(path, _OPEN_SIDECAR))
    return sidecar


def _bounded_nedb_class():
    """The NEDB subclass with the bounded _load, built lazily so
    importing this module stays free of the engine import."""
    global _bounded_cls
    if _bounded_cls is not None:
        return _bounded_cls

    import re

    from nedb import NEDB
    from nedb import crypto as _crypto
    from nedb import snapshot as _snap
    from nedb.engine import apply_op
    from nedb.log import Op

    # One shared inert Op stands in for every pre-checkpoint op:
    # OpLog.load() only reads .nonce/.client/.idem off each entry
    # (all inert here) and .hash off the LAST entry (overridden
    # explicitly when the delta is empty), while len() keeps seq
    # allocation globally correct. 8 bytes of pointer per op
    # instead of a full payload dict.
    _PAD = Op(0, "", 0, "checkpoint", {}, 0.0, None, "", "")

    class _BoundedNEDB(NEDB):
        _open_info: dict = {"mode": "fresh"}

        def _self_heal_if_needed(self) -> None:
            # Disabled — see the bounded-open notes above. The
            # stock version runs a full-chain verify() here on
            # every open; integrity audits run offline via
            # scripts/checkpoint_embedded.py (stock open).
            return

        def _load(self) -> None:
            info = self._bounded_load()
            if info is None:
                super()._load()
                self._open_info = {"mode": "full-replay"}
            else:
                self._open_info = info

        def _bounded_load(self):
            """Snapshot + delta-only load. Returns the open-info
            dict, or None when the bounded path is not provably
            safe and the caller must use the stock loader. Once
            load_snapshot() has run this never returns None — it
            degrades to the stock snapshot branch instead."""
            aof_path = self._aof_path
            snap_path = _snap._snap_path(self.path)
            sidecar_path = os.path.join(self.path, _OPEN_SIDECAR)
            try:
                if not (os.path.exists(sidecar_path)
                        and os.path.exists(snap_path)
                        and os.path.exists(aof_path)):
                    return None
                with open(sidecar_path, encoding="utf-8") as fh:
                    sidecar = json.load(fh)
                if sidecar.get("version") != 1:
                    return None
                if os.path.getsize(snap_path) != sidecar["snapshot_bytes"]:
                    return None
                if os.path.getsize(aof_path) < sidecar["aof_offset"]:
                    return None
                # The sidecar head must be the head the snapshot
                # itself anchored (snapshot.json v1 writes it in
                # the first bytes). This is what makes the
                # empty-delta case sound: with no delta ops to
                # chain-check, the recorded head is trusted only
                # because the snapshot agrees with it.
                with open(snap_path, "rb") as fh:
                    prefix = fh.read(1024).decode("utf-8", "ignore")
                m = re.search(r'"head"\s*:\s*"([0-9a-f]+)"', prefix)
                if not m or m.group(1) != sidecar["head"]:
                    return None
            except Exception:
                return None

            snap_seq = _snap.load_snapshot(self)
            if snap_seq < 0:
                # load_snapshot bails before mutating state on an
                # unreadable/foreign snapshot, so the stock loader
                # is still safe here.
                return None
            if snap_seq != sidecar["checkpoint_seq"]:
                return self._snapshot_full_parse(snap_seq)

            snap_nonces = dict(self.log._last_nonce)
            snap_idem = dict(self.log._idem)
            delta: list = []
            try:
                with open(aof_path, encoding="utf-8") as fh:
                    fh.seek(sidecar["aof_offset"])
                    for raw in fh:
                        line = _crypto.aof_decode(raw, self._dek)
                        if line:
                            delta.append(Op.from_dict(json.loads(line)))
            except Exception:
                return self._snapshot_full_parse(snap_seq)

            # The delta must chain, link by link, from the head the
            # checkpoint anchored. Field comparison only — no
            # re-hashing — but a foreign/replaced snapshot or a
            # truncated AOF cannot pass it.
            prev = sidecar["head"]
            for op in delta:
                if op.seq <= snap_seq or op.prev_hash != prev:
                    return self._snapshot_full_parse(snap_seq)
                prev = op.hash

            self.log.load([_PAD] * (snap_seq + 1) + delta)
            if not delta:
                self.log._head = sidecar["head"]
            for client, nonce in snap_nonces.items():
                if nonce > self.log._last_nonce.get(client, 0):
                    self.log._last_nonce[client] = nonce
            for key, seq in snap_idem.items():
                self.log._idem.setdefault(key, seq)
            for op in delta:
                apply_op(self.store, self.relations, self.indexes,
                         op, self.cause_map)
            self._nonce = dict(self.log._last_nonce)
            return {"mode": "snapshot-bounded",
                    "checkpoint_seq": snap_seq,
                    "delta_ops": len(delta)}

        def _snapshot_full_parse(self, snap_seq: int) -> dict:
            """The engine's stock snapshot branch: state already
            restored from the snapshot, the full AOF is parsed to
            rebuild the log, only post-checkpoint ops are folded.
            Correct but O(history) — the degraded fallback when the
            sidecar cannot be trusted."""
            ops: list = []
            if os.path.exists(self._aof_path):
                with open(self._aof_path, encoding="utf-8") as fh:
                    for raw in fh:
                        line = _crypto.aof_decode(raw, self._dek)
                        if line:
                            ops.append(Op.from_dict(json.loads(line)))
            self.log.load(ops)
            for op in self.log.ops:
                if op.seq > snap_seq:
                    apply_op(self.store, self.relations, self.indexes,
                             op, self.cause_map)
            self._nonce = dict(self.log._last_nonce)
            return {"mode": "snapshot-full-parse",
                    "checkpoint_seq": snap_seq}

    _bounded_cls = _BoundedNEDB
    return _bounded_cls


_native_cls = None


def _native_nedb_class():
    """NEDB-shaped adapter over the Rust NedbCore (``nedb._native``).

    Selected with NEDB_ENGINE=native. The v2 store lives in the
    ``rust/`` subdirectory of the embedded data dir; the v1 files
    (snapshot.json + log.aof) stay untouched beside it as the archive
    and the rollback path. Only the engine surface the facade and the
    domain mixin actually call is implemented: put / get / delete /
    query / create_index / collections / checkpoint / flush / close.

    Conversion from a v1 store is convert_store.py's job (it streams
    the live state through the bounded reader above and verifies
    counts + a content digest + verify() before writing
    conversion.json); this adapter seeds collections() from that
    marker and tracks new collections as they are written.
    """
    global _native_cls
    if _native_cls is not None:
        return _native_cls

    from nedb._native import NedbCore

    class _NativeNEDB:
        _open_info = {"mode": "native-rust"}
        _is_native = True

        def __init__(self, path: str) -> None:
            self._path = path
            # The v2 store is segment-backed (NEDB_DAG_V3) and opens in
            # portal mode (NEDB_PORTAL_READS): sealed segments are read
            # through their on-disk indexes instead of being indexed in
            # memory. Both default on here; an explicit env value set
            # before boot is respected.
            os.environ.setdefault("NEDB_DAG_V3", "1")
            os.environ.setdefault("NEDB_PORTAL_READS", "1")
            self._core = NedbCore.open(os.path.join(path, "rust"))
            self._colls = set()
            marker = os.path.join(path, "conversion.json")
            if os.path.exists(marker):
                try:
                    with open(marker, encoding="utf-8") as fh:
                        self._colls.update(
                            json.load(fh).get("counts", {}).keys())
                except Exception:
                    pass

        # ── engine surface used by the facade / domain mixin ──────
        def put(self, coll, doc_id, doc):
            self._colls.add(coll)
            return self._core.put(coll, str(doc_id), json.dumps(doc))

        def get(self, coll, doc_id):
            raw = self._core.get(coll, str(doc_id))
            return json.loads(raw) if raw is not None else None

        def delete(self, coll, doc_id):
            return self._core.delete(coll, str(doc_id))

        def query(self, nql):
            return [json.loads(r) for r in self._core.query(nql)]

        def create_index(self, coll, field, kind="eq"):
            return self._core.create_index(coll, field, kind)

        def collections(self):
            return sorted(self._colls)

        def checkpoint(self):
            self._core.flush()
            return self._core.head()

        def flush(self):
            return self._core.flush()

        def close(self):
            try:
                self._core.flush()
            except Exception:
                pass

        @property
        def seq(self):
            return self._core.seq()

    _native_cls = _NativeNEDB
    return _native_cls


class EmbeddedNedbStore(EmbeddedDomainMixin):
    """Async facade over the embedded NEDB engine.

    KV/zset/set primitives are defined here; the domain surface
    (utxo/address/pool/snapshot/entries/reward-blocks) comes from
    EmbeddedDomainMixin, implemented on the same primitives.
    """

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
            if os.environ.get("NEDB_ENGINE") == "native":
                db = _native_nedb_class()(self._path)
            else:
                db = _bounded_nedb_class()(self._path)
            # Domain-layer indexes: equality lookups on these fields
            # are the hot paths (address pages, snapshot scans).
            # create_index is idempotent and cheap on an indexed
            # collection; failures are non-fatal (queries fall back
            # to scans, still correct).
            for coll, field in (
                ("utxo", "address"), ("utxo", "height"),
                ("addr_tx", "address"), ("addr_tx", "txid"),
                ("addr_tx", "height"),
                ("utxo_undo", "height"),
                ("pool", "payout_address"),
                ("snapshot", "start_height"),
                ("entry", "snapshot_id"),
                ("reward_block", "snapshot_id"),
            ):
                try:
                    db.create_index(coll, field)
                except Exception:
                    pass
            return db

        self._db = await self._run(_open)
        logger.info(
            "EmbeddedNedbStore open at %s (%s)",
            self._path, getattr(self._db, "_open_info", {"mode": "?"}),
        )

    async def aclose(self) -> None:
        if self._db is not None:
            db, self._db = self._db, None
            # Checkpoint on clean close so the NEXT open takes the
            # bounded path (snapshot + small delta). Best-effort: a
            # failed checkpoint never loses data (the AOF is the
            # source of truth) — it only means the next open
            # replays more. Disable with
            # VISION_EMBEDDED_CHECKPOINT_ON_CLOSE=0.
            if os.environ.get(
                    "VISION_EMBEDDED_CHECKPOINT_ON_CLOSE", "1") != "0":
                try:
                    if getattr(db, "_is_native", False):
                        # The v2 store has no AOF/snapshot to checkpoint;
                        # a flush is the durable close. The bounded-open
                        # sidecar is a v1 concept and is not written.
                        await self._run(db.flush)
                    else:
                        await self._run(checkpoint_embedded_db, db, self._path)
                except Exception as e:
                    logger.warning(
                        "embedded checkpoint on close failed: %s", e)
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

    def peek(self, key: str) -> Optional[str]:
        """Synchronous read straight off the engine (no executor hop).

        For sync contexts that cannot await — notably /api/health,
        which is deliberately a sync endpoint. Engine reads are
        in-memory MVCC lookups; a single dict read racing the storage
        thread's writes is safe for status purposes.
        """
        if self._db is None:
            return None
        row = self._db.get("kv", key)
        if not row:
            return None
        expires_at = row.get("expires_at")
        if expires_at is not None and expires_at < time.time():
            return None
        v = row.get("value")
        return None if v is None else str(v)

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

    def peek_ping(self) -> bool:
        """Synchronous liveness check for /api/health: True when the
        engine is open and answering. Never raises."""
        if self._db is None:
            return False
        try:
            self._db.collections()
            return True
        except Exception:
            return False

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
    """Drop-in ``get_db()`` override backed entirely by the embedded
    NEDB engine: KV/zset/set AND the domain layer (UTXO ledger,
    address history, pools, snapshots) all route to
    EmbeddedNedbStore. SQLite is not constructed anywhere in the
    live path — it survives only as the offline backup / migration
    source.
    """

    def __init__(self, embedded: EmbeddedNedbStore) -> None:
        self._nd = embedded

    def stats(self) -> dict:
        return {"active": True, "store": "embedded"}

    def peek_kv(self, key: str) -> Optional[str]:
        return self._nd.peek(key)

    def peek_ping(self) -> bool:
        return self._nd.peek_ping()

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

    async def ping(self) -> bool:
        return await self._nd.ping()

    async def publish(self, channel: str, message: str) -> int:
        return await self._nd.publish(channel, message)

    async def aclose(self) -> None:
        return await self._nd.aclose()

    # ── domain methods → embedded NEDB ───────────────────────────────

    async def utxo_add_batch(self, rows: Sequence[Tuple[str, int, str, int, int]]) -> None:
        return await self._nd.utxo_add_batch(rows)

    async def utxo_spend_batch(self, outpoints: Sequence[Tuple[str, int]]) -> List[Tuple[str, int, str, int, int]]:
        return await self._nd.utxo_spend_batch(outpoints)

    async def address_tx_add_batch(self, *args, **kwargs) -> None:
        return await self._nd.address_tx_add_batch(*args, **kwargs)

    async def commit(self) -> None:
        return await self._nd.commit()

    async def address_balance(self, address: str) -> Tuple[int, int]:
        return await self._nd.address_balance(address)

    async def address_tx_count(self, address: str) -> int:
        return await self._nd.address_tx_count(address)

    async def address_first_last_height(self, address: str) -> Tuple[Optional[int], Optional[int]]:
        return await self._nd.address_first_last_height(address)

    async def address_txs(self, *args, **kwargs) -> list:
        return await self._nd.address_txs(*args, **kwargs)

    async def address_utxos(self, address: str) -> list:
        return await self._nd.address_utxos(address)

    async def address_index_rollback(self, from_height: int) -> None:
        return await self._nd.address_index_rollback(from_height)

    async def address_index_last_height(self) -> int:
        return await self._nd.address_index_last_height()

    async def coinbase_reward_scan(self, *args, **kwargs):
        return await self._nd.coinbase_reward_scan(*args, **kwargs)

    async def pool_create(self, data: dict) -> int:
        return await self._nd.pool_create(data)

    async def pool_update(self, pool_id: int, data: dict) -> bool:
        return await self._nd.pool_update(pool_id, data)

    async def pool_get(self, pool_id: int) -> Optional[dict]:
        return await self._nd.pool_get(pool_id)

    async def pool_find_by_payout_address(self, *args, **kwargs) -> Optional[dict]:
        return await self._nd.pool_find_by_payout_address(*args, **kwargs)

    async def pool_list(self, *args, **kwargs) -> list:
        return await self._nd.pool_list(*args, **kwargs)

    async def snapshot_create(self, data: dict) -> int:
        return await self._nd.snapshot_create(data)

    async def snapshot_create_guarded(self, data: dict) -> Optional[int]:
        return await self._nd.snapshot_create_guarded(data)

    async def snapshot_delete(self, snapshot_id: int) -> bool:
        return await self._nd.snapshot_delete(snapshot_id)

    async def snapshots_delete_by_status(self, statuses: Sequence[str]) -> int:
        return await self._nd.snapshots_delete_by_status(statuses)

    async def snapshot_get(self, snapshot_id: int) -> Optional[dict]:
        return await self._nd.snapshot_get(snapshot_id)

    async def snapshot_list(self, *args, **kwargs) -> list:
        return await self._nd.snapshot_list(*args, **kwargs)

    async def snapshot_set_totals(self, *args, **kwargs) -> None:
        return await self._nd.snapshot_set_totals(*args, **kwargs)

    async def snapshot_set_status(self, *args, **kwargs) -> bool:
        return await self._nd.snapshot_set_status(*args, **kwargs)

    async def snapshot_mark_failed(self, *args, **kwargs) -> bool:
        return await self._nd.snapshot_mark_failed(*args, **kwargs)

    async def snapshot_finalize(self, *args, **kwargs) -> bool:
        return await self._nd.snapshot_finalize(*args, **kwargs)

    async def snapshot_clear_results(self, snapshot_id: int) -> None:
        return await self._nd.snapshot_clear_results(snapshot_id)

    async def snapshot_range_exists(self, start_height: int, end_height: int) -> bool:
        return await self._nd.snapshot_range_exists(start_height, end_height)

    async def entries_insert_batch(self, rows) -> None:
        return await self._nd.entries_insert_batch(rows)

    async def entries_list(self, snapshot_id: int) -> list:
        return await self._nd.entries_list(snapshot_id)

    async def entry_get(self, entry_id: int) -> Optional[dict]:
        return await self._nd.entry_get(entry_id)

    async def entry_update(self, *args, **kwargs) -> bool:
        return await self._nd.entry_update(*args, **kwargs)

    async def entries_set_status_all(self, snapshot_id: int, status: str) -> int:
        return await self._nd.entries_set_status_all(snapshot_id, status)

    async def reward_blocks_insert_batch(self, rows) -> None:
        return await self._nd.reward_blocks_insert_batch(rows)

    async def reward_blocks_list(self, *args, **kwargs) -> list:
        return await self._nd.reward_blocks_list(*args, **kwargs)


# ── module-level singleton ───────────────────────────────────────────

_instance: Optional[EmbeddedStore] = None


def set_embedded_store_instance(store: "EmbeddedStore") -> None:
    global _instance
    _instance = store


def get_embedded_store() -> Optional["EmbeddedStore"]:
    return _instance
