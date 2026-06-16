# Interchained Vision

Production-grade explorer for the Interchained (ITC) blockchain, with a built-in
ITSL token registry and self-serve token deployer. 100% open source, self-hostable,
zero analytics, zero accounts.

## Features

- **Real-time blocks & mempool** — server-sent events fanned out from a single async indexer.
- **Bloomberg-grade address pages** — txs, UTXOs, ITSL holdings, stats, private notes.
- **Mempool intelligence** — fee categories, fee histogram, projected upcoming blocks.
- **ITSL token registry** — sortable, searchable, with verification badges.
- **Self-serve deploy** — fee estimate → review → broadcast, WIF never persisted.
- **Real-time API** — REST + Server-Sent Events + WebSocket + Atom feed + webhooks.
- **TypeScript SDK** at `sdk/` (`@interchained/vision-sdk`).
- **PWA** — installable, offline-tolerant, mobile-first.

## Stack

| Layer       | Technology                                         |
|-------------|----------------------------------------------------|
| Frontend    | Next.js 15 · React 19 RC · Tailwind v4 · TypeScript |
| Backend     | FastAPI · httpx · Redis (async)                    |
| Address idx | Any ElectrumX server (yours)                       |
| Indexer     | Single-process async, Redis-backed, reorg-safe     |
| Realtime    | SSE (browser) + WebSocket (SDK) + Redis pub/sub    |

## Run in this Replit workspace

This project lives next to **iNEWS**. To avoid port conflicts:

| Service          | Port  | Workflow              |
|------------------|-------|------------------------|
| iNEWS frontend   | 5000  | `Frontend`             |
| iNEWS backend    | 8000  | `Backend`              |
| **Vision web**   | 8099  | `Vision Web`           |
| **Vision API**   | 8080  | `Vision Backend`       |

To preview Vision in the Replit pane, switch the preview port to **8099**.

### Local dev (outside Replit)

```bash
cp .env.example .env  # fill in ITC_RPC_*, ELECTRUMX_*
# Backend
cd backend && pip install -r requirements.txt && bash start.sh
# Frontend
cd web && pnpm install && pnpm run dev
```

### Docker (production)

```bash
docker compose up -d
# UI on http://localhost:5000, API on http://localhost:8000
```

## Required environment

- `ITC_RPC_HOST`, `ITC_RPC_PORT`, `ITC_RPC_USER`, `ITC_RPC_PASS`, `ITC_WALLET_NAME`
- `ELECTRUMX_HOST`, `ELECTRUMX_PORT` (your VPS)
- Optional: `PRICE_API_URL`, `START_FROM_HEIGHT`, `RATE_LIMIT_PER_MIN`

## NEDB Integration

Vision is wired into [NEDB](https://github.com/Eth-Interchained/nedb) — an
append-only, hash-chained, time-traveling embedded database built by
Interchained LLC and Claude Sonnet 4.6. Standard explorers answer "what is the
state now?" Vision-on-NEDB answers "what was the state at any past sequence,
who caused it, and can you prove the log wasn't tampered with?" Every block
header, every ITSL token operation, and every coinbase reward split is written
to nedbd with causal links — the explorer is now a tamper-evident, replayable
chain mirror, not just a read-through cache.

### Three-layer architecture

1. **DualStore** (`backend/app/store/dualstore.py`) — operational KV. Every
   write Vision makes to its hot-path key/value store goes to SQLite (sync,
   source of truth) AND nedbd (fire-and-forget) simultaneously. Reads prefer
   nedbd with sticky fallback to SQLite. In production, `nedb_read_pct`
   reached **81.4%** within hours.
2. **`blocks` collection** — populated by `NedbBackfillTask`, an asyncio
   background worker that walks backwards from chain tip to genesis at
   ~250 blocks/second, writing lean headers (`height`, `hash`, `prev_hash`,
   `timestamp`, `n_tx`, `difficulty`) with `caused_by: [parent_height]` so the
   entire chain becomes a TRACE-able causal graph.
3. **`itsl_ops` + `reward_splits`** — written by `itsl_mirror.py`, a sidecar
   daemon that bridges the ITC node into nedbd. Every ITSL CREATE / TRANSFER /
   APPROVE / BURN lands in `itsl_ops` with causal links back through the
   token's history; per-block coinbase splits (miner 44% / governance 51% /
   operator 5%) land in `reward_splits`.

### Enabling NEDB

1. Run `nedbd` (the NEDB HTTP daemon) somewhere Vision can reach it. Default
   port is `:7070`.
2. Set in `.env`:
   ```
   NEDB_URL=http://127.0.0.1:7070
   ```
3. Restart Vision. The backfill task starts on boot; DualStore picks up nedbd
   on the next write. Watch `/api/nedb/backfill-status` to track progress.

### NQL showcase routes

Vision exposes NEDB's query language ([NQL](https://github.com/Eth-Interchained/nedb))
directly to the explorer:

| Route | Purpose |
|---|---|
| `POST /api/nedb/query` | Arbitrary NQL — drives the `/nedb` console |
| `GET  /api/nedb/token-history/{id}?as_of=N` | Time-travel a token to sequence `N` |
| `GET  /api/nedb/trace/{id}` | Walk `caused_by` graph from any block or op |
| `GET  /api/nedb/verify` | Tamper-evidence check — returns `{ok, seq, head}` |
| `GET  /api/nedb/backfill-status` | Backfill progress, throughput, last height written |

Example NQL queries (try them in `/nedb`):

```nql
-- What was the chain tip 2 hours ago?
FROM kv WHERE _id = "vision:tip:height" AS OF 50000

-- Full causal ancestry of a block back to genesis
FROM blocks WHERE _id = "619960" TRACE caused_by

-- Every ITSL token operation for a token, in causal order
FROM itsl_ops WHERE token = "0x...tok" TRACE caused_by

-- Blocks with the most transactions
FROM blocks ORDER BY n_tx DESC LIMIT 10
```

See [NEDB.md](./NEDB.md) for the full story — architecture diagram, design
rationale, and the numbers from launch day.

## License

MIT
