#!/usr/bin/env python3
"""stream_checkpoint_embedded.py — bounded-memory checkpoint converter.

Fallback for data dirs whose stock conversion no longer fits in
RAM. scripts/checkpoint_embedded.py opens the dir with the STOCK
engine: _load() parses EVERY AOF line into an in-memory Op kept
forever in log.ops, folds them all, then self-heal verify()
re-hashes every op. On the production dir (2.90 GB AOF) that open
OOM-killed at ~63% / 5.3 GB RSS and climbing — the conversion
could never finish, so the bounded open could never engage.

This converter reaches the SAME end state — engine snapshot.json
plus the vision-open.json sidecar written by
app.embedded_store.checkpoint_embedded_db — while holding memory
at LIVE-STATE size instead of HISTORY size:

  * it streams log.aof line by line through the engine's own
    parsing (crypto.aof_decode + Op.from_dict) and folds each op
    with the engine's own nedb.engine.apply_op, exactly like the
    stock loader's no-snapshot branch;
  * it never retains an op: per-op bookkeeping mirrors
    OpLog.load() (max nonce per client, first-seq idem map, head
    = last op's hash) and log.ops becomes a list of references to
    ONE shared inert placeholder — the same padding trick
    _BoundedNEDB uses — so len(log) and therefore the checkpoint
    op's seq stay globally correct at 8 bytes per historical op;
  * the checkpoint itself is taken by the engine's own
    db.checkpoint() (nedb.snapshot.save_snapshot: appends the
    checkpoint op to the AOF through _log_append, then serializes
    HEAD state), and the sidecar by checkpoint_embedded_db, so
    the artifacts are byte-for-byte the stock converter's shape.

What it deliberately does NOT do, per operator directive (the
dir is ours and trusted): no verify() pass — neither the explicit
chain verify nor the stock open's self-heal (overridden off,
same as _BoundedNEDB). Structural soundness is still enforced
while streaming, at zero hashing cost: every op's seq must be
the next expected seq and its prev_hash must equal the running
head (field comparison only). Any mismatch — or any line that
fails to parse — aborts the conversion BEFORE anything is
written; this tool never rewrites the AOF. The audited path
remains scripts/checkpoint_embedded.py (stock open + full
verify) whenever the dir is small enough to afford it.

Memory note: the fold keeps the engine's MVCC version chains,
exactly as the stock open does, so peak RSS tracks live state +
version history — the same state the bounded open itself loads
from the snapshot. What is gone is the second full copy of
history the stock loader keeps as parsed Op objects.

  python scripts/stream_checkpoint_embedded.py --dir ../data/vision-embedded

Progress: phase banners plus a ticking status line (AOF read %,
RSS, elapsed), same as checkpoint_embedded.py. Finishes by
reopening the dir through Vision's bounded path to prove it
engages (mode must be snapshot-bounded).
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


class _StreamError(Exception):
    """AOF is structurally unsound; abort before writing anything."""


def _stream_nedb_class():
    """NEDB subclass whose _load streams instead of retaining ops.

    Built lazily so importing this module stays free of the engine
    import, mirroring app.embedded_store._bounded_nedb_class().
    """
    import json

    from nedb import NEDB
    from nedb import crypto as _crypto
    from nedb.engine import apply_op
    from nedb.log import GENESIS, Op

    # One shared inert Op pads log.ops — see the module docstring.
    # OpLog.load() semantics only ever read .nonce/.client/.idem off
    # padded entries, and the checkpoint append reads len() and
    # _head, both of which _load sets explicitly below.
    _PAD = Op(0, "", 0, "checkpoint", {}, 0.0, None, "", "")

    class _StreamNEDB(NEDB):
        _stream_stats: dict = {}

        def _self_heal_if_needed(self) -> None:
            # Disabled — the stock version runs a full-chain
            # verify() here (and would try to "heal" a log whose
            # ops are placeholders). Operator directive: trusted
            # dir, no verify pass. Same call _BoundedNEDB makes.
            return

        def _load(self) -> None:
            # 1) index configuration (stock loader, same order)
            if os.path.exists(self._meta_path):
                with open(self._meta_path, encoding="utf-8") as fh:
                    for coll, field, kind in json.load(fh).get("indexes", []):
                        self.indexes.ensure(coll, field, kind)
            # 2) stream the AOF: parse, check structure, fold,
            #    discard. Bookkeeping mirrors OpLog.load().
            count = 0
            head = GENESIS
            last_nonce: dict = {}
            idem: dict = {}
            if os.path.exists(self._aof_path):
                with open(self._aof_path, encoding="utf-8") as fh:
                    for raw_line in fh:
                        line = _crypto.aof_decode(raw_line, self._dek)
                        if not line:
                            continue
                        try:
                            op = Op.from_dict(json.loads(line))
                        except Exception as exc:
                            raise _StreamError(
                                f"AOF line at seq {count} does not "
                                f"parse ({type(exc).__name__}: {exc}); "
                                f"aborting, nothing written") from exc
                        if op.seq != count or op.prev_hash != head:
                            raise _StreamError(
                                f"AOF chain structure breaks at seq "
                                f"{count}: op.seq={op.seq} "
                                f"prev_hash={op.prev_hash[:12]}… != "
                                f"running head {head[:12]}…; "
                                f"aborting, nothing written")
                        apply_op(self.store, self.relations,
                                 self.indexes, op, self.cause_map)
                        if op.nonce > last_nonce.get(op.client, 0):
                            last_nonce[op.client] = op.nonce
                        if op.idem is not None and op.idem not in idem:
                            idem[op.idem] = op.seq
                        head = op.hash
                        count += 1
            # 3) install the padded log: correct length for seq
            #    allocation, head/nonce/idem as OpLog.load() sets.
            self.log.ops = [_PAD] * count
            self.log._last_nonce = last_nonce
            self.log._idem = idem
            self.log._head = head
            self._nonce = dict(last_nonce)
            self._stream_stats = {"ops": count, "head": head}

    return _StreamNEDB


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Streaming bounded-memory conversion of an "
                    "embedded NEDB dir for bounded opens")
    ap.add_argument("--dir", default="../data/vision-embedded",
                    help="embedded data dir (default ../data/vision-embedded)")
    args = ap.parse_args()
    path = args.dir

    aof_path = os.path.join(path, "log.aof")
    if not os.path.exists(aof_path):
        print(f"ERROR: no log.aof in {path!r} — nothing to convert")
        return 2

    prog = _Progress(aof_path)
    prog.start()
    try:
        prog.set_phase("phase 1/3: streaming the full AOF — folding "
                       "every op once, retaining none")
        t0 = time.time()
        db = _stream_nedb_class()(path)
        stats = db._stream_stats
        print(f"streamed fold: {time.time() - t0:.1f}s  "
              f"ops={stats['ops']:,}  head={stats['head'][:12]}…")
        prog.set_phase("phase 2/3: writing checkpoint + bounded-open "
                       "sidecar (engine checkpoint, stock shape)")
        from app.embedded_store import checkpoint_embedded_db
        info = checkpoint_embedded_db(db, path)
        print(f"checkpoint recorded: seq={info['checkpoint_seq']} "
              f"aof_offset={info['aof_offset']} "
              f"snapshot_bytes={info['snapshot_bytes']}")
        db.close()
        # Release the folded state before the reopen proof so the
        # proof's own state doesn't stack on top of it in-process.
        del db
        import gc
        gc.collect()

        prog.set_phase("phase 3/3: reopening through the bounded path "
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
        mode = store._db._open_info
        print(f"bounded reopen: {time.time() - t:.2f}s  mode={mode}")
        if mode.get("mode") != "snapshot-bounded" or mode.get("delta_ops") != 0:
            raise SystemExit(
                f"ERROR: bounded open did not engage cleanly: {mode}")
        await store.aclose()

    asyncio.run(reopen())
    print("conversion complete: bounded open engages, delta_ops=0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
