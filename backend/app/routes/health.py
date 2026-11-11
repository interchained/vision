from __future__ import annotations

import json
import logging

from fastapi import APIRouter

from .. import __version__
from ..indexer.main_loop import get_indexer

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
def health():
    """Fully synchronous health check — reads the embedded NEDB
    engine directly (in-memory MVCC peeks), bypassing the async
    event loop so it always responds instantly. SQLite is not
    consulted: it is a cold backup, not a live store."""
    rpc_ok = False
    rpc_height = None
    electrum_ok = False
    db_ok = False
    indexer_height = None

    # Use the indexer's cached state
    indexer = get_indexer()
    if indexer is not None:
        rpc_ok = indexer._rpc_online

    # The embedded store is the live DB: liveness + the indexer's
    # status keys all come from it.
    try:
        from ..embedded_store import get_embedded_store
        _emb = get_embedded_store()
        if _emb is not None:
            db_ok = _emb.peek_ping()
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
        db_ok = False

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
