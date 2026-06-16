# NEDB Inside Vision

On June 16, 2026, Interchained Vision stopped being a read-through cache and
became something genuinely new: a live, tamper-evident, time-traveling chain
explorer powered by a causally-linked append-only database. This document is
the honest account of what got built, why, and what it now lets you ask.

Built by Mark Allen Evans Jr. (Interchained LLC) and Claude Sonnet 4.6.

---

## What NEDB Is

NEDB is an embedded database. Append-only log on disk. BLAKE2b hash chain over
every write, so the log is tamper-evident by construction — any byte flipped
anywhere in history breaks the head hash and `/verify` returns false. MVCC
under the hood, which means every read can specify `AS OF seq` and get the
exact state the database had at that sequence number. It's bi-temporal too:
`VALID AS OF "date"` separates *transaction time* (when the row was written)
from *valid time* (when the fact was true in the world). Every write can carry
a `caused_by` pointer, an `evidence` blob, and a `confidence` score — which
turns the database into a causal graph you can walk with `TRACE caused_by`.
At-rest encryption is AES-256-GCM. The query language is NQL:

```
FROM coll [AS OF seq] [WHERE ...] [TRACE caused_by] [ORDER BY ...] [LIMIT n]
```

The daemon, `nedbd`, runs on `:7070` and exposes all of this over HTTP.

The reason any of this matters: most databases answer the question *"what is
the state now?"* NEDB answers *"what was the state at any point, who caused
it, and can you prove it wasn't tampered with?"* Those are different
questions, and on a blockchain explorer the second one turns out to be the
interesting one.

---

## Why Vision Uses It

Vision was originally a read-through cache over an ITC node. Block hashes in
SQLite. Tip height in a Redis-shaped KV store. ITSL token registry polled
every 30 seconds and held in memory. It worked. It was fast. It served real
traffic.

But it had no memory of itself. Query the chain tip and you got the current
value, not the history of how it got there. Query a token's metadata and you
got whatever the last poll captured, not what the token looked like at the
moment block 500,000 was mined. If a row in SQLite got silently corrupted —
bad disk, bad migration, bad operator — there was no cryptographic way to
know. The explorer trusted itself, and asked you to trust it too.

NEDB changes the questions Vision can answer:

- *"What was the chain tip 6 hours ago?"* — answered by `AS OF`.
- *"Show me every ITSL operation causally linked back to the genesis of this
  token."* — answered by `TRACE caused_by`.
- *"Is any of this data tampered with?"* — answered by `GET /api/nedb/verify`,
  which validates the entire BLAKE2b chain head.

None of those were possible against the SQLite-only cache. All three are now
production routes.

---

## The Integration

Three components were added. Each does one thing.

### 1. DualStore

`backend/app/store/dualstore.py`. The operational KV layer Vision uses for
hot-path state — chain tip, mempool snapshot, recent block index, per-address
counters. Every write is fanned out to both SQLite (synchronous, the source
of truth) and nedbd (fire-and-forget, async). Reads prefer nedbd, fall back
to SQLite on miss or error, and stick to whichever store answered first for
the rest of the request. SQLite never goes away — it's the safety net if
nedbd is restarting, slow, or unreachable.

The fire-and-forget write path is what made this safe to ship. nedbd can be
down for an hour and Vision keeps serving traffic; when it comes back, the
backfill task and the live write stream rebuild what was missed. No
distributed transaction, no two-phase commit, no operational complexity that
would have made the project unshippable.

### 2. NedbBackfillTask

The chain didn't start today. There were 619,960 blocks of history when
Vision wired in NEDB, and all of them needed to land in the `blocks`
collection with proper causal links if `TRACE caused_by` was going to be
useful. The backfill task is an asyncio background worker that walks
*backwards* from the current tip toward genesis, writing lean block headers
— `height`, `hash`, `prev_hash`, `timestamp`, `n_tx`, `difficulty` — and
setting `caused_by: [parent_height]` on every block so the entire chain is a
causal graph anchored at block 0.

It sustained roughly **250 blocks/second** on the production node. Tip-down
ordering means the explorer is useful immediately for recent activity, and
the deep history fills in behind it.

### 3. itsl_mirror.py

A standalone sidecar daemon, separate from Vision's main process. It tails
the ITC node and writes three collections into nedbd:

- **`itsl_ops`** — every ITSL token operation: `CREATE`, `TRANSFER`,
  `APPROVE`, `BURN`. Each op is causally linked to the previous op for that
  token, so `FROM itsl_ops WHERE token = "0x..." TRACE caused_by` returns the
  token's entire history in causal order back to its CREATE.
- **`reward_splits`** — the per-block coinbase split: miner 44%, governance
  51%, operator 5%. This is the row that lets you ask *"how much has the
  governance treasury accumulated in the last 1,000 blocks?"* without
  scanning the chain yourself.
- **`blocks`** — full block headers, written live as the node mines or
  receives. These overlap with the backfill task on purpose; nedbd
  deduplicates by `_id`.

Running it as a sidecar instead of inside Vision means the explorer process
stays narrow and the bridge can be restarted independently when the ITC node
rotates RPC creds or upgrades.

---

## What You Can Ask Now

These were impossible against the old cache. They all work today:

```nql
-- Time-travel: what was the chain tip 2 hours ago?
FROM kv WHERE _id = "vision:tip:height" AS OF 50000

-- Full causal ancestry of a block back to genesis
FROM blocks WHERE _id = "619960" TRACE caused_by

-- Every ITSL token operation for a token, in causal order
FROM itsl_ops WHERE token = "0x...tok" TRACE caused_by

-- Governance treasury inflow over the last 1,000 blocks
FROM reward_splits WHERE height > 619000 GROUP BY governance_address SUM governance_reward

-- Blocks with the most transactions
FROM blocks ORDER BY n_tx DESC LIMIT 10

-- Tamper-evidence check (HTTP, not NQL):
-- GET /api/nedb/verify  ->  {ok: true, seq: N, head: "blake2b..."}
```

The `/nedb` page in the Vision frontend wires these together: an NQL console,
a time-travel explorer, and a tamper-verify panel.

---

## The Numbers

Within hours of going live on June 16, 2026:

| Metric | Value |
|---|---|
| Reads served by nedbd | **85,032** |
| `nedb_read_pct` | **81.4%** |
| Dual-writes (SQLite + nedbd) | **63,951** |
| Chain tip confirmed round-trip | **619960** |
| Backfill throughput | **~250 blocks/sec** |

This is not a demo database running alongside the real system. It *is* the
system, serving the majority of Vision's production reads from a
tamper-evident, time-traveling, causally-linked chain log.

---

## Architecture

```
                          +---------------------+
                          |     ITC node        |
                          |  (RPC + ZMQ feed)   |
                          +----------+----------+
                                     |
                +--------------------+--------------------+
                |                                         |
                v                                         v
       +-----------------+                        +-----------------+
       | Vision indexer  |                        | itsl_mirror.py  |
       |  (asyncio)      |                        |   (sidecar)     |
       +--------+--------+                        +--------+--------+
                |                                          |
                v                                          |
       +-----------------+                                 |
       |    DualStore    |                                 |
       +---+---------+---+                                 |
           |         |                                     |
   sync    |         | fire-and-forget                     |
           v         v                                     v
     +---------+   +------------------------------------------------+
     | SQLite  |   |                    nedbd  :7070                 |
     | (truth) |   |   blocks  |  itsl_ops  |  reward_splits  |  kv  |
     +---------+   +------------------------------------------------+
                                       ^
                                       |
              +------------------------+------------------------+
              |                                                 |
   reads (nedb first, SQLite fallback)              NQL showcase routes:
                                                    /api/nedb/query
                                                    /api/nedb/token-history/{id}?as_of=N
                                                    /api/nedb/trace/{id}
                                                    /api/nedb/verify
                                                    /api/nedb/backfill-status
                                                              |
                                                              v
                                                       +-------------+
                                                       |  Vision API |
                                                       +------+------+
                                                              |
                                                              v
                                                       +-------------+
                                                       |  /nedb page |
                                                       |  (frontend) |
                                                       +-------------+

  NedbBackfillTask (asyncio): walks tip -> genesis, writes lean block
  headers to nedbd with caused_by: [parent_height].
```

---

## Credit

Built by Interchained LLC and Claude Sonnet 4.6.

- nedb-engine: https://github.com/Eth-Interchained/nedb
- Vision: https://github.com/Eth-Interchained/vision
