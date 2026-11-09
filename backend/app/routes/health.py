from __future__ import annotations

import json
import logging
import sqlite3

from fastapi import APIRouter

from .. import __version__
from ..config import settings
from ..indexer.main_loop import get_indexer

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
def health():
    """Fully synchronous health check — reads SQLite directly with sqlite3,
    bypassing the async event loop so it always responds instantly."""
    rpc_ok = False
    rpc_height = None
    electrum_ok = False
    db_ok = False
    indexer_height = None

    # Use the indexer's cached state
    indexer = get_indexer()
    if indexer is not None:
        rpc_ok = indexer._rpc_online

    # Read directly from SQLite (synchronous, no event loop)
    try:
        conn = sqlite3.connect(str(settings.SQLITE_DB_PATH), timeout=2)
        # Test DB is alive
        conn.execute("SELECT 1")
        db_ok = True

        cur = conn.execute("SELECT value FROM kv WHERE key = ?", ("vision:indexer:last_height",))
        row = cur.fetchone()
        if row and row[0]:
            indexer_height = int(row[0])

        cur = conn.execute("SELECT value FROM kv WHERE key = ?", ("vision:indexer:status",))
        row = cur.fetchone()
        if row and row[0]:
            status = json.loads(row[0])
            rpc_height = status.get("tip")
            if rpc_height and rpc_height > 0:
                rpc_ok = True

        conn.close()
    except Exception:
        db_ok = False

    # Embedded store active: the indexer writes its status to the
    # embedded KV now, NOT to SQLite — the SQLite copies froze at the
    # moment of the switch. Read the live values from the engine.
    try:
        from ..embedded_store import get_embedded_store
        _emb = get_embedded_store()
        if _emb is not None:
            _lh = _emb.peek_kv("vision:indexer:last_height")
            if _lh:
                indexer_height = int(_lh)
            _st = _emb.peek_kv("vision:indexer:status")
            if _st:
                _status = json.loads(_st)
                _tip = _status.get("tip")
                if _tip:
                    rpc_height = _tip
                    if _tip > 0:
                        rpc_ok = True
    except Exception:
        pass

    # ElectrumX — check if the persistent TCP connection is alive
    try:
        from ..electrumx.client import get_electrumx
        ex = get_electrumx()
        electrum_ok = not ex._closed
    except Exception:
        electrum_ok = False

    # Embedded-store sync progress — tip checkpoint + backwards
    # history sync. Read from an in-memory cache the backfill task
    # publishes to, so this endpoint stays fully synchronous.
    sync_stats: dict = {"active": False}
    try:
        from ..embedded_store import get_embedded_store
        from ..indexer.embedded_backfill import get_sync_status
        if get_embedded_store() is not None:
            sync_stats = {
                "active": True,
                "store": "embedded",
                **get_sync_status(),
            }
    except Exception:
        pass

    overall = "ok" if (rpc_ok and db_ok) else "degraded"
    return {
        "status":         overall,
        "rpc":            rpc_ok,
        "rpc_height":     rpc_height,
        "electrumx":      electrum_ok,
        "db":             db_ok,
        "indexer_height": indexer_height,
        "version":        __version__,
        "sync":           sync_stats,
    }
