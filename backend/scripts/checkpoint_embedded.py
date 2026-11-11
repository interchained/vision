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

Progress: the long phases block inside single engine calls, so a
heartbeat thread reports a live status line while they run —
phase, AOF bytes read (the replay's real position, from this
process's own file offset), RSS, and elapsed time.
"""
import argparse
import asyncio
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class _Progress:
    """One-line live status while a blocking engine phase runs."""

    _SPIN = "|/-\\"

    def __init__(self, aof_path: str) -> None:
        self._aof = os.path.realpath(aof_path)
        try:
            self._aof_size = os.path.getsize(self._aof)
        except OSError:
            self._aof_size = 0
        self.phase = "starting"
        self._t0 = time.time()
        self._tty = sys.stdout.isatty()
        self._stop = threading.Event()
        self._thread = None
        self._tick = 0

    # -- signals from /proc (Linux) --------------------------------------
    def _aof_pos(self):
        try:
            for fd in os.listdir("/proc/self/fd"):
                try:
                    if os.readlink(f"/proc/self/fd/{fd}") != self._aof:
                        continue
                    with open(f"/proc/self/fdinfo/{fd}") as fh:
                        for line in fh:
                            if line.startswith("pos:"):
                                return int(line.split()[1])
                except OSError:
                    continue
        except OSError:
            pass
        return None

    def _rss_mb(self) -> int:
        try:
            with open("/proc/self/status") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) // 1024
        except OSError:
            pass
        return 0

    # -- rendering ---------------------------------------------------------
    def _line(self) -> str:
        elapsed = int(time.time() - self._t0)
        parts = [f"{self._SPIN[self._tick % 4]} {self.phase}"]
        pos = self._aof_pos()
        if pos is not None and self._aof_size:
            parts.append(
                f"AOF read {pos / 1e9:.2f}/{self._aof_size / 1e9:.2f} GB "
                f"({pos * 100 // self._aof_size}%)")
        rss = self._rss_mb()
        if rss:
            parts.append(f"RSS {rss:,} MB")
        parts.append(f"{elapsed // 60}:{elapsed % 60:02d}")
        return "  " + " · ".join(parts)

    def _run(self) -> None:
        interval = 1.0 if self._tty else 15.0
        while not self._stop.wait(interval):
            self._tick += 1
            line = self._line()
            if self._tty:
                print(f"\r{line:<100}", end="", flush=True)
            else:
                print(line, flush=True)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def set_phase(self, label: str) -> None:
        if self._tty:
            print(f"\r{'':<100}\r", end="", flush=True)
        self.phase = label
        print(f"▸ {label}", flush=True)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if self._tty:
            print(f"\r{'':<100}\r", end="", flush=True)


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

    prog = _Progress(os.path.join(path, "log.aof"))
    prog.start()
    try:
        prog.set_phase("phase 1/4: stock open — replaying the full AOF "
                       "(the slow one, one time only)")
        t0 = time.time()
        db = NEDB(path)  # stock: full replay + self-heal verify inside
        print(f"stock open: {time.time() - t0:.1f}s  ops={len(db.log.ops)}  "
              f"head={db.head[:12]}…")
        prog.set_phase("phase 2/4: verifying the full hash chain")
        ok = db.log.verify()
        print(f"chain verify: {'OK' if ok else 'FAILED'}")
        if not ok:
            db.close()
            return 1
        if not args.verify_only:
            prog.set_phase("phase 3/4: writing checkpoint + bounded-open "
                           "sidecar")
            from app.embedded_store import checkpoint_embedded_db
            info = checkpoint_embedded_db(db, path)
            print(f"checkpoint recorded: seq={info['checkpoint_seq']} "
                  f"aof_offset={info['aof_offset']} "
                  f"snapshot_bytes={info['snapshot_bytes']}")
        db.close()
        if args.verify_only:
            return 0

        prog.set_phase("phase 4/4: reopening through the bounded path "
                       "(proof it engages)")
    finally:
        prog.stop()

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
