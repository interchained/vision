#!/usr/bin/env python3
"""checkpoint_embedded.py — bounded-open conversion + integrity audit.

Opens an embedded NEDB data dir with the STOCK engine — the
knowingly slow, audited open: full AOF replay plus the engine's
full hash-chain verify — then takes a checkpoint and records
Vision's bounded-open sidecar (vision-open.json) so every later
backend start opens in O(live state + ops since checkpoint)
instead of O(total history). See the bounded-open notes in
app/embedded_store.py for why the stock open is O(history) and
why a checkpoint alone does not fix it.

Run it ONCE per existing data dir before (or right after)
deploying the bounded open, backend stopped, and any time you
want a full integrity audit:

  python scripts/checkpoint_embedded.py --dir ../data/vision-embedded
  python scripts/checkpoint_embedded.py --dir ../data/vision-embedded --verify-only

Expect the stock open to take minutes and multiple GB of RAM on
a production-sized dir — that is the problem this tool converts
away, so give it a maintenance window. Every subsequent open is
bounded. The script finishes by reopening the dir through
Vision's bounded path to prove it engages.
"""
import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Convert/audit an embedded NEDB dir for bounded opens")
    ap.add_argument("--dir", default="../data/vision-embedded",
                    help="embedded data dir (default ../data/vision-embedded)")
    ap.add_argument("--verify-only", action="store_true",
                    help="stock open + full chain verify, no checkpoint")
    args = ap.parse_args()
    path = args.dir

    from nedb import NEDB

    t0 = time.time()
    db = NEDB(path)  # stock: full replay + self-heal verify inside
    print(f"stock open: {time.time() - t0:.1f}s  ops={len(db.log.ops)}  "
          f"head={db.head[:12]}…")
    ok = db.log.verify()
    print(f"chain verify: {'OK' if ok else 'FAILED'}")
    if not ok:
        db.close()
        return 1
    if not args.verify_only:
        from app.embedded_store import checkpoint_embedded_db
        info = checkpoint_embedded_db(db, path)
        print(f"checkpoint recorded: seq={info['checkpoint_seq']} "
              f"aof_offset={info['aof_offset']} "
              f"snapshot_bytes={info['snapshot_bytes']}")
    db.close()
    if args.verify_only:
        return 0

    # Prove the bounded path engages on the very next open.
    os.environ["VISION_EMBEDDED_CHECKPOINT_ON_CLOSE"] = "0"
    from app.embedded_store import EmbeddedNedbStore

    async def reopen() -> None:
        store = EmbeddedNedbStore(path)
        t = time.time()
        await store.open()
        print(f"bounded reopen: {time.time() - t:.2f}s  "
              f"mode={store._db._open_info}")
        await store.aclose()

    asyncio.run(reopen())
    return 0


if __name__ == "__main__":
    sys.exit(main())
