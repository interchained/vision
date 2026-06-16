"""Async NEDB store — drop-in NQL-first replacement for the SQLite KV layer.

This module mirrors the public method surface of ``sqlite_store.SQLiteStore``
for the KV / sorted-set / set operations Vision relies on, but talks to a
remote ``nedbd`` HTTP server instead of a local SQLite file.

Collections used inside the nedbd database (typically ``vision``):

* ``kv``    – ``{_id: key, value: str, expires_at: float|null}``
* ``zset``  – ``{_id: f"{name}::{member}", _name: name, _member: member, score: float}``
* ``set``   – ``{_id: f"{name}::{member}", _name: name, _member: member}``

TTL semantics
-------------
``set(key, value, ex=N)`` stores ``expires_at = time.time() + N``. Reads
filter through NQL with ``WHERE _id = "{key}" AND (expires_at IS NULL OR
expires_at > {now})``. Expired rows are simply not returned; a background
sweep is *not* required for correctness.

Domain-specific methods (utxo_*, address_*, pool_*, snapshot_*,
``coinbase_reward_scan``) are NOT supported here — they require relational
joins / indices that are owned by ``sqlite_store``. Calling them raises
``NotImplementedError`` so the caller can fall back to ``sqlite_store``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, List, Optional

import httpx

from .config import settings

logger = logging.getLogger(__name__)

# Module-level singleton (parallels sqlite_store._db).
_store: Optional["NedbStore"] = None
_lock = asyncio.Lock()


# Re-export the Keys namespace so consumers can keep using it.
class Keys:
    TIP_HEIGHT = "vision:tip:height"
    TIP_HASH = "vision:tip:hash"
    RECENT_BLOCKS = "vision:blocks:recent"
    BLOCK_BY_HEIGHT = "vision:block:height:{height}"
    BLOCK_BY_HASH = "vision:block:hash:{hash}"
    INDEXER_LAST_HEIGHT = "vision:indexer:last_height"
    INDEXER_STATUS = "vision:indexer:status"
    MEMPOOL_SUMMARY = "vision:mempool:summary"
    TOKEN_REGISTRY = "vision:token:registry"
    SPECIAL_ADDRESSES = "vision:special:addresses"
    ADDRESS_INDEX_STATUS = "vision:address_index:status"


# ── NQL helpers ─────────────────────────────────────────────────────────────

def _nql_escape(s: str) -> str:
    """Escape a string for embedding in an NQL double-quoted literal."""
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


def _not_implemented(method: str) -> NotImplementedError:
    return NotImplementedError(
        f"NedbStore.{method} is not supported — this method requires SQLite. "
        "Use sqlite_store.get_db() for address/pool/snapshot operations."
    )


class NedbStore:
    """Async facade over the nedbd HTTP API, exposing the Redis-shaped subset
    of the API that Vision actually uses (KV, sorted sets, sets)."""

    def __init__(
        self,
        base_url: str,
        db_name: str,
        token: str = "",
        client: Optional[httpx.AsyncClient] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._db = db_name
        self._token = token
        # Allow tests to inject a pre-built client; otherwise create one lazily.
        self._client: Optional[httpx.AsyncClient] = client
        self._client_lock = asyncio.Lock()

    # ── client management ────────────────────────────────────────────────
    # Two separate httpx clients with different timeouts:
    #
    #   _read_client:  read=3s  — queries fail fast, DualStore falls back to SQLite
    #   _write_client: read=30s — PUTs wait for nedbd's encrypted AOF fsync
    #
    # Root cause of write_errors: a single client with read=3s was timing out
    # while nedbd fsynced the AOF under write pressure (backfill + DualStore
    # concurrent writes). Splitting clients removes the contention entirely.

    _write_client: Optional[httpx.AsyncClient] = None
    _write_client_lock: Optional[asyncio.Lock] = None

    def _ensure_write_lock(self) -> asyncio.Lock:
        if self._write_client_lock is None:
            self._write_client_lock = asyncio.Lock()
        return self._write_client_lock

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the read client (short 3s read timeout)."""
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._client is None:
                headers = {}
                if self._token:
                    headers["Authorization"] = f"Bearer {self._token}"
                self._client = httpx.AsyncClient(
                    base_url=self._base_url,
                    headers=headers,
                    timeout=httpx.Timeout(connect=2.0, read=3.0, write=10.0, pool=2.0),
                )
        return self._client

    async def _get_write_client(self) -> httpx.AsyncClient:
        """Return the write client (long 30s read timeout for fsync waits)."""
        if self._write_client is not None:
            return self._write_client
        async with self._ensure_write_lock():
            if self._write_client is None:
                headers = {}
                if self._token:
                    headers["Authorization"] = f"Bearer {self._token}"
                self._write_client = httpx.AsyncClient(
                    base_url=self._base_url,
                    headers=headers,
                    timeout=httpx.Timeout(connect=2.0, read=30.0, write=30.0, pool=2.0),
                )
        return self._write_client

    async def aclose(self) -> None:
        for attr in ("_client", "_write_client"):
            c = getattr(self, attr, None)
            if c is not None:
                try:
                    await c.aclose()
                except Exception:
                    pass
                setattr(self, attr, None)

    # ── low-level HTTP helpers ───────────────────────────────────────────

    async def _query(self, nql: str) -> dict:
        """Run an NQL query and return the raw response payload.

        Treats 400/404 as an empty result rather than raising — this keeps
        the DualStore resilient when collections don't exist yet (e.g. while
        the migrator is still populating nedbd for the first time).
        """
        client = await self._get_client()
        resp = await client.post(
            f"/v1/databases/{self._db}/query",
            json={"nql": nql},
        )
        if resp.status_code in (400, 404):
            # Collection not found or NQL error on an empty collection —
            # treat as empty result so DualStore falls back to SQLite.
            return {"rows": [], "count": 0}
        resp.raise_for_status()
        return resp.json()

    async def _put(
        self,
        coll: str,
        doc_id: str,
        doc: dict,
        *,
        caused_by: Optional[str] = None,
        valid_from: Optional[str] = None,
        valid_to: Optional[str] = None,
        evidence: Optional[str] = None,
        confidence: Optional[float] = None,
    ) -> dict:
        client = await self._get_write_client()   # write client: 30s read timeout
        payload: dict = {"coll": coll, "id": doc_id, "doc": doc}
        if caused_by is not None:
            payload["caused_by"] = caused_by
        if valid_from is not None:
            payload["valid_from"] = valid_from
        if valid_to is not None:
            payload["valid_to"] = valid_to
        if evidence is not None:
            payload["evidence"] = evidence
        if confidence is not None:
            payload["confidence"] = confidence
        resp = await client.post(
            f"/v1/databases/{self._db}/put",
            json=payload,
        )
        if resp.status_code == 404:
            # Database doesn't exist yet — create it and retry once.
            await client.post("/v1/databases", json={"name": self._db})
            resp = await client.post(
                f"/v1/databases/{self._db}/put",
                json=payload,
            )
        resp.raise_for_status()
        return resp.json()

    async def _del(self, coll: str, doc_id: str) -> dict:
        client = await self._get_write_client()
        resp = await client.delete(
            f"/v1/databases/{self._db}/rows/{coll}/{doc_id}",
        )
        resp.raise_for_status()
        return resp.json()

    async def _batch(self, ops: List[dict]) -> dict:
        client = await self._get_write_client()   # write client
        resp = await client.post(
            f"/v1/databases/{self._db}/batch",
            json={"ops": ops},
        )
        resp.raise_for_status()
        return resp.json()

    # ── key / value ──────────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[str]:
        # TTL filtering done in Python, not NQL — avoids IS NULL which is
        # not guaranteed to be supported in all NQL implementations.
        nql = f'FROM kv WHERE _id = "{_nql_escape(key)}" LIMIT 1'
        data = await self._query(nql)
        rows = data.get("rows") or []
        if not rows:
            return None
        row = rows[0]
        expires_at = row.get("expires_at")
        if expires_at is not None and expires_at < time.time():
            return None  # expired — treat as miss, SQLite fallback handles it
        v = row.get("value")
        return None if v is None else str(v)

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:  # noqa: A003
        expires = (time.time() + ex) if ex else None
        await self._put(
            "kv",
            key,
            {"_id": key, "value": str(value), "expires_at": expires},
        )

    async def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        ops: List[dict] = []
        for k in keys:
            ops.append({"op": "del", "coll": "kv", "id": k})

        # Best-effort: also clear any matching zset/set group with this name.
        # We discover member ids via NQL and queue dels for them.
        for k in keys:
            esc = _nql_escape(k)
            for coll in ("zset", "set"):
                try:
                    data = await self._query(
                        f'FROM {coll} WHERE _name = "{esc}" LIMIT 99999'
                    )
                    for row in data.get("rows") or []:
                        _id = row.get("_id")
                        if _id:
                            ops.append({"op": "del", "coll": coll, "id": _id})
                except Exception:
                    # Don't fail the kv delete if the auxiliary sweep blows up.
                    pass

        if not ops:
            return 0
        result = await self._batch(ops)
        # Count only successful kv deletes (mirrors sqlite_store.delete semantics
        # which returns rowcount from the kv DELETEs).
        kv_count = 0
        for item, op in zip(result.get("results") or [], ops):
            if op["coll"] == "kv" and (item.get("ok") if isinstance(item, dict) else True):
                kv_count += 1
        return kv_count

    async def incr(self, key: str) -> int:
        # NEDB has no atomic increment over HTTP — read, increment, write.
        # Two concurrent writers can race; acceptable for the counters
        # (rate-limit buckets, etc.) we use this for.
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
        expires = time.time() + seconds
        await self._put(
            "kv",
            key,
            {"_id": key, "value": existing, "expires_at": expires},
        )

    # ── sorted sets ──────────────────────────────────────────────────────

    @staticmethod
    def _zset_id(name: str, member: str) -> str:
        return f"{name}::{member}"

    async def zadd(self, name: str, mapping: dict) -> int:
        if not mapping:
            return 0
        ops: List[dict] = []
        for member, score in mapping.items():
            m = str(member)
            doc = {
                "_id": self._zset_id(name, m),
                "_name": name,
                "_member": m,
                "score": float(score),
            }
            ops.append({"op": "put", "coll": "zset", "id": doc["_id"], "doc": doc})
        await self._batch(ops)
        return len(ops)

    async def zrevrange(
        self, name: str, start: int, stop: int, withscores: bool = False
    ) -> list:
        esc = _nql_escape(name)
        # ``stop = -1`` means "all". Otherwise use Redis-inclusive [start, stop].
        if stop < 0:
            limit_clause = "LIMIT 99999"
            offset = max(start, 0)
        else:
            limit = max(0, stop - start + 1)
            limit_clause = f"LIMIT {limit}"
            offset = max(start, 0)

        # NQL doesn't document OFFSET — fetch [0, stop] then slice client-side.
        # (offset is small in practice: 0 for "top N" queries.)
        fetch_limit = 99999 if stop < 0 else max(0, stop + 1)
        nql = (
            f'FROM zset WHERE _name = "{esc}" '
            f"ORDER BY score DESC LIMIT {fetch_limit}"
        )
        data = await self._query(nql)
        rows = data.get("rows") or []
        sliced = rows[offset:] if stop < 0 else rows[offset: stop + 1]

        if withscores:
            return [(r.get("_member"), float(r.get("score", 0))) for r in sliced]
        return [r.get("_member") for r in sliced]

    async def zremrangebyrank(self, name: str, start: int, stop: int) -> int:
        """Remove members by rank (0-indexed, ascending score order).

        Negative stop means 'keep the top N'; e.g. zremrangebyrank(k, 0, -201)
        keeps the 200 highest-scored entries.
        """
        esc = _nql_escape(name)
        data = await self._query(
            f'FROM zset WHERE _name = "{esc}" ORDER BY score ASC LIMIT 99999'
        )
        rows = data.get("rows") or []
        total = len(rows)
        if stop < 0:
            keep = abs(stop) - 1
            delete_count = total - keep
            if delete_count <= 0:
                return 0
            victims = rows[:delete_count]
        else:
            victims = rows[start: stop + 1]
        if not victims:
            return 0
        ops = [
            {"op": "del", "coll": "zset", "id": r["_id"]}
            for r in victims
            if r.get("_id")
        ]
        if ops:
            await self._batch(ops)
        return len(ops)

    # ── sets ─────────────────────────────────────────────────────────────

    @staticmethod
    def _set_id(name: str, member: str) -> str:
        return f"{name}::{member}"

    async def sadd(self, name: str, *members: str) -> int:
        if not members:
            return 0
        ops: List[dict] = []
        for m in members:
            ms = str(m)
            doc = {
                "_id": self._set_id(name, ms),
                "_name": name,
                "_member": ms,
            }
            ops.append({"op": "put", "coll": "set", "id": doc["_id"], "doc": doc})
        await self._batch(ops)
        return len(ops)

    async def smembers(self, name: str) -> set:
        esc = _nql_escape(name)
        data = await self._query(
            f'FROM set WHERE _name = "{esc}" LIMIT 99999'
        )
        rows = data.get("rows") or []
        return {r.get("_member") for r in rows if r.get("_member") is not None}

    async def srem(self, name: str, *members: str) -> int:
        if not members:
            return 0
        ops = [
            {"op": "del", "coll": "set", "id": self._set_id(name, str(m))}
            for m in members
        ]
        await self._batch(ops)
        return len(ops)

    # ── misc ─────────────────────────────────────────────────────────────

    async def ping(self) -> bool:
        client = await self._get_client()
        resp = await client.get("/health")
        resp.raise_for_status()
        body = resp.json()
        return bool(body.get("ok"))

    async def publish(self, channel: str, message: str) -> int:
        """No-op — cross-instance fanout isn't part of NEDB's surface here."""
        return 0

    # ── domain methods (not supported — fall back to sqlite_store) ───────
    #
    # These all raise NotImplementedError. We intentionally enumerate every
    # method on SQLiteStore that callers may invoke so a typo or untested
    # code path fails loudly with a clear message instead of silently
    # returning None.

    async def utxo_add_batch(self, *args, **kwargs):
        raise _not_implemented("utxo_add_batch")

    async def utxo_spend_batch(self, *args, **kwargs):
        raise _not_implemented("utxo_spend_batch")

    async def address_tx_add_batch(self, *args, **kwargs):
        raise _not_implemented("address_tx_add_batch")

    async def commit(self, *args, **kwargs):
        raise _not_implemented("commit")

    async def address_balance(self, *args, **kwargs):
        raise _not_implemented("address_balance")

    async def address_tx_count(self, *args, **kwargs):
        raise _not_implemented("address_tx_count")

    async def address_first_last_height(self, *args, **kwargs):
        raise _not_implemented("address_first_last_height")

    async def address_txs(self, *args, **kwargs):
        raise _not_implemented("address_txs")

    async def address_utxos(self, *args, **kwargs):
        raise _not_implemented("address_utxos")

    async def address_index_rollback(self, *args, **kwargs):
        raise _not_implemented("address_index_rollback")

    async def address_index_last_height(self, *args, **kwargs):
        raise _not_implemented("address_index_last_height")

    async def coinbase_reward_scan(self, *args, **kwargs):
        raise _not_implemented("coinbase_reward_scan")

    # Pool reward methods
    async def pool_create(self, *args, **kwargs):
        raise _not_implemented("pool_create")

    async def pool_update(self, *args, **kwargs):
        raise _not_implemented("pool_update")

    async def pool_get(self, *args, **kwargs):
        raise _not_implemented("pool_get")

    async def pool_find_by_payout_address(self, *args, **kwargs):
        raise _not_implemented("pool_find_by_payout_address")

    async def pool_list(self, *args, **kwargs):
        raise _not_implemented("pool_list")

    # Snapshot methods
    async def snapshot_range_exists(self, *args, **kwargs):
        raise _not_implemented("snapshot_range_exists")

    async def snapshot_create(self, *args, **kwargs):
        raise _not_implemented("snapshot_create")

    async def snapshot_create_guarded(self, *args, **kwargs):
        raise _not_implemented("snapshot_create_guarded")

    async def snapshot_delete(self, *args, **kwargs):
        raise _not_implemented("snapshot_delete")

    async def snapshots_delete_by_status(self, *args, **kwargs):
        raise _not_implemented("snapshots_delete_by_status")

    async def snapshot_get(self, *args, **kwargs):
        raise _not_implemented("snapshot_get")

    async def snapshot_list(self, *args, **kwargs):
        raise _not_implemented("snapshot_list")

    async def snapshot_set_totals(self, *args, **kwargs):
        raise _not_implemented("snapshot_set_totals")

    async def snapshot_set_status(self, *args, **kwargs):
        raise _not_implemented("snapshot_set_status")

    async def snapshot_mark_failed(self, *args, **kwargs):
        raise _not_implemented("snapshot_mark_failed")

    async def snapshot_finalize(self, *args, **kwargs):
        raise _not_implemented("snapshot_finalize")

    async def snapshot_clear_results(self, *args, **kwargs):
        raise _not_implemented("snapshot_clear_results")

    async def entries_insert_batch(self, *args, **kwargs):
        raise _not_implemented("entries_insert_batch")

    async def entries_list(self, *args, **kwargs):
        raise _not_implemented("entries_list")

    async def entry_get(self, *args, **kwargs):
        raise _not_implemented("entry_get")

    async def entry_update(self, *args, **kwargs):
        raise _not_implemented("entry_update")

    async def entries_set_status_all(self, *args, **kwargs):
        raise _not_implemented("entries_set_status_all")

    async def reward_blocks_insert_batch(self, *args, **kwargs):
        raise _not_implemented("reward_blocks_insert_batch")

    async def reward_blocks_list(self, *args, **kwargs):
        raise _not_implemented("reward_blocks_list")


# ── singleton plumbing (mirrors sqlite_store) ──────────────────────────────

async def _ensure_store() -> "NedbStore":
    global _store
    if _store is not None:
        return _store
    async with _lock:
        if _store is not None:
            return _store
        if not settings.NEDB_URL:
            raise RuntimeError(
                "NEDB_URL is not configured — cannot initialise NedbStore. "
                "Set NEDB_URL in the environment or use sqlite_store instead."
            )
        logger.info(
            "Opening NEDB store at %s (db=%s, auth=%s)",
            settings.NEDB_URL, settings.NEDB_DB_NAME,
            "yes" if settings.NEDBD_TOKEN else "no",
        )
        store = NedbStore(
            base_url=settings.NEDB_URL,
            db_name=settings.NEDB_DB_NAME,
            token=settings.NEDBD_TOKEN,
        )
        _store = store
    return _store


class _LazyStore(NedbStore):
    """Subclass that lazily initialises the HTTP client on first method call.

    Mirrors ``sqlite_store._LazyStore``: ``get_db()`` returns an instance
    immediately so callers can hold the reference, and the underlying
    connection materialises on first method invocation.
    """

    def __init__(self):
        # Don't call super().__init__ — defer to _ensure().
        self.__inner: Optional[NedbStore] = None

    async def _ensure(self) -> NedbStore:
        if self.__inner is None:
            self.__inner = await _ensure_store()
        return self.__inner

    # KV ----------------------------------------------------------------
    async def get(self, key):
        return await (await self._ensure()).get(key)

    async def set(self, key, value, ex=None):
        return await (await self._ensure()).set(key, value, ex=ex)

    async def delete(self, *keys):
        return await (await self._ensure()).delete(*keys)

    async def incr(self, key):
        return await (await self._ensure()).incr(key)

    async def expire(self, key, seconds):
        return await (await self._ensure()).expire(key, seconds)

    # Sorted sets -------------------------------------------------------
    async def zadd(self, name, mapping):
        return await (await self._ensure()).zadd(name, mapping)

    async def zrevrange(self, name, start, stop, withscores=False):
        return await (await self._ensure()).zrevrange(name, start, stop, withscores=withscores)

    async def zremrangebyrank(self, name, start, stop):
        return await (await self._ensure()).zremrangebyrank(name, start, stop)

    # Sets --------------------------------------------------------------
    async def sadd(self, name, *members):
        return await (await self._ensure()).sadd(name, *members)

    async def smembers(self, name):
        return await (await self._ensure()).smembers(name)

    async def srem(self, name, *members):
        return await (await self._ensure()).srem(name, *members)

    # Misc --------------------------------------------------------------
    async def ping(self):
        return await (await self._ensure()).ping()

    async def publish(self, channel, message):
        return 0

    async def aclose(self):
        if self.__inner is not None:
            await self.__inner.aclose()
            self.__inner = None


def get_db() -> NedbStore:
    """Return a lazy singleton ``NedbStore``.

    Mirrors ``sqlite_store.get_db()``: synchronous return so callers can do
    ``store = get_db()`` at module load; the HTTP client is created on the
    first awaited method call. Importing this module while nedbd is offline
    is safe — only method calls will fail.
    """
    return _LazyStore()


async def init_db() -> "NedbStore":
    """Open the NEDB HTTP client and create the database in the background.

    The database creation is fire-and-forget so startup never blocks.
    If the database already exists, nedbd returns a non-2xx that we ignore.
    """
    store = await _ensure_store()
    # Kick off DB creation as a background task — don't await it.
    asyncio.get_event_loop().create_task(_ensure_db_exists(store))
    return store


async def _ensure_db_exists(store: "NedbStore") -> None:
    """POST to create the database. Idempotent — safe if it already exists."""
    try:
        client = await store._get_client()
        await client.post("/v1/databases", json={"name": store._db})
        # nedbd returns 201 on create, 409/4xx if already exists — both fine.
    except Exception:
        pass  # will retry naturally on next write attempt


async def close_db() -> None:
    """Close the NEDB HTTP client (call during lifespan shutdown)."""
    global _store
    if _store is not None:
        try:
            await _store.aclose()
        except Exception:
            pass
        _store = None
