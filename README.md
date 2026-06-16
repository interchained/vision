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

## License

MIT
