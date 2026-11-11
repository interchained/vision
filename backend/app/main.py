"""FastAPI entrypoint for Interchained Vision backend."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .electrumx.client import close_electrumx
from .indexer.main_loop import get_indexer
from .indexer.address_index import get_address_indexer
from .middleware.errors import register_exception_handlers
from .middleware.rate_limit import rate_limit_middleware
from . import nedb_store
from .embedded_domain import (
    close_address_index_writer, init_address_index_writer,
)
from .embedded_store import (
    EmbeddedNedbStore, EmbeddedStore, set_embedded_store_instance,
)
from .sqlite_store import set_store_override
from .indexer.embedded_backfill import (
    EmbeddedBackfillTask, HttpKvBlockSource,
    RpcLiteBlockSource, seed_tip_window_from_rpc,
    get_backfill_task, set_backfill_task,
)
from .routes import (
    addresses,
    admin_pools,
    blocks,
    compat,
    deploy,
    health,
    mempool,
    nedb as nedb_routes,
    pool_rewards,
    rss,
    search,
    sse,
    stats,
    tokens,
    transactions,
    webhooks,
    ws,
)
from .rpc.client import close_rpc
from .services.price import price_loop
from .services.events import get_event_bus

logging.basicConfig(level=settings.LOG_LEVEL.upper(), format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
# httpx logs every RPC POST at INFO — way too noisy during sync.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("vision")

_background_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    ex_host, ex_port = settings.electrumx_endpoint
    logger.info("Vision backend starting (RPC=%s wallet=%s, ElectrumX=%s:%s)",
                settings.rpc_base_url, settings.ITC_WALLET_NAME,
                ex_host, ex_port)

    # The embedded NEDB engine is the ONLY live store (KV + domain).
    # Open it and install the get_db() override BEFORE anything else
    # touches storage, and bind the address-index writer to it.
    #
    # Any failure here is FATAL and propagates: the old behaviour —
    # log "non-fatal" and silently serve from SQLite — is precisely
    # what this hardening removes. A half-up backend quietly serving
    # a stale file is worse than a loud crash at boot. SQLite is
    # never constructed in the live path; it remains only as the
    # offline backup / migration source (see DEPLOY.md).
    embedded = EmbeddedNedbStore(settings.NEDB_EMBEDDED_DIR)
    await embedded.open()
    store = EmbeddedStore(embedded)
    set_store_override(store)
    set_embedded_store_instance(store)
    init_address_index_writer(embedded)
    logger.info("Embedded NEDB store ready at %s", settings.NEDB_EMBEDDED_DIR)

    indexer = get_indexer()
    address_indexer = get_address_indexer()

    async def _deferred_start():
        """Run after uvicorn is already accepting connections.

        Wraps every step in try/except + explicit logging so a silent
        exception or hang can never hide which step failed.
        """
        await asyncio.sleep(1)  # Let uvicorn settle
        logger.info("Deferred startup: begin")

        # 0. Seed the tip window from the daemon and start the
        #    backwards history sync into the embedded store. Sources,
        #    in order: live RPC in the indexer's own lite
        #    serialization, then the legacy nedbd HTTP store when
        #    NEDB_URL is set. SQLite is NOT a live source any more.
        try:
            from .rpc.client import get_rpc as _get_rpc
            rpc_client = _get_rpc()
            seed = await seed_tip_window_from_rpc(embedded, rpc_client)
            sources: list = [RpcLiteBlockSource(rpc_client)]
            if settings.NEDB_URL:
                try:
                    await nedb_store.init_db()
                    sources.append(HttpKvBlockSource(nedb_store.get_db()))
                except Exception as e:
                    logger.warning(
                        "Legacy nedbd backfill source unavailable "
                        "(non-fatal): %s", e,
                    )
            backfill = EmbeddedBackfillTask(embedded, sources)
            set_backfill_task(backfill)
            await backfill.start()
            logger.info(
                "Embedded history sync running — tip=%d seeded=%d "
                "sources=%s",
                seed["tip"], seed["seeded"],
                [s.name for s in sources],
            )
        except Exception as e:
            logger.error(
                "Embedded seed/backfill failed: %s", e, exc_info=True,
            )

        # 1. ElectrumX first — its handshake is independent of the DB
        try:
            from .electrumx.client import get_electrumx
            ex = get_electrumx()
            await asyncio.wait_for(ex._ensure_connected(), timeout=10)
            logger.info("ElectrumX connected to %s:%s", ex.host, ex.port)
        except asyncio.TimeoutError:
            logger.warning("ElectrumX pre-connect timed out after 10s (non-fatal)")
        except Exception as e:
            logger.warning("ElectrumX pre-connect failed (non-fatal): %s", e)

        # 2. Warm special wallets (writes to KV via the store).
        try:
            await asyncio.wait_for(indexer.warm_special_wallets(), timeout=15)
            logger.info("Special wallets warmed")
        except asyncio.TimeoutError:
            logger.warning("warm_special_wallets timed out after 15s (non-fatal)")
        except Exception as e:
            logger.warning("warm_special_wallets failed (non-fatal): %s", e)

        # 3. Start the block indexer task.
        try:
            await indexer.start()
            logger.info("Block indexer task started")
        except Exception as e:
            logger.error("indexer.start failed: %s", e, exc_info=True)

        # 4. Start the address indexer task.
        try:
            await address_indexer.start()
            logger.info("Address indexer task started")
        except Exception as e:
            logger.error("address_indexer.start failed: %s", e, exc_info=True)

        logger.info("Deferred startup: complete")

    def _on_deferred_done(task: asyncio.Task):
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("Deferred startup task crashed: %s", exc, exc_info=exc)

    _deferred_task = asyncio.create_task(_deferred_start())
    _deferred_task.add_done_callback(_on_deferred_done)
    _background_tasks.append(_deferred_task)

    if settings.PRICE_API_URL:
        _background_tasks.append(asyncio.create_task(price_loop()))

    _background_tasks.append(asyncio.create_task(webhooks.webhook_dispatcher_loop()))

    yield

    logger.info("Vision backend stopping")
    for t in _background_tasks:
        t.cancel()

    # Stop backfill gracefully
    _bt = get_backfill_task()
    if _bt:
        await _bt.stop()

    await indexer.stop()
    await address_indexer.stop()
    await close_address_index_writer()
    await close_electrumx()
    await close_rpc()
    set_store_override(None)
    set_embedded_store_instance(None)
    try:
        await embedded.aclose()
    except Exception as e:
        logger.warning("embedded store close failed: %s", e)
    if settings.NEDB_URL:
        try:
            await nedb_store.close_db()
        except Exception as e:
            logger.warning("nedb_store.close_db failed: %s", e)


app = FastAPI(
    title="Interchained Vision API",
    description="Production-grade ITC blockchain explorer + ITSL token registry & deployer.",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)
app.middleware("http")(rate_limit_middleware)

register_exception_handlers(app)

# Mount all routes under /api so a frontend can proxy easily.
prefix = "/api"
app.include_router(health.router, prefix=prefix, tags=["health"])
app.include_router(stats.router, prefix=prefix, tags=["stats"])
app.include_router(compat.router, prefix=prefix, tags=["compat"])
app.include_router(blocks.router, prefix=prefix, tags=["blocks"])
app.include_router(transactions.router, prefix=prefix, tags=["transactions"])
app.include_router(addresses.router, prefix=prefix, tags=["addresses"])
app.include_router(mempool.router, prefix=prefix, tags=["mempool"])
app.include_router(tokens.router, prefix=prefix, tags=["tokens"])
app.include_router(deploy.router, prefix=prefix, tags=["deploy"])
app.include_router(search.router, prefix=prefix, tags=["search"])
app.include_router(sse.router, prefix=prefix, tags=["realtime"])
app.include_router(ws.router, prefix=prefix, tags=["realtime"])
app.include_router(rss.router, prefix=prefix, tags=["feeds"])
app.include_router(webhooks.router, prefix=prefix, tags=["webhooks"])
app.include_router(admin_pools.router, prefix=prefix, tags=["admin"])
app.include_router(pool_rewards.admin_router, prefix=prefix, tags=["admin"])
app.include_router(pool_rewards.public_router, prefix=prefix, tags=["pools"])
app.include_router(nedb_routes.router, prefix=prefix, tags=["nedb"])


@app.get("/")
async def root():
    return {
        "name": "Interchained Vision API",
        "version": "0.1.0",
        "docs": "/api/docs",
        "openapi": "/api/openapi.json",
    }
