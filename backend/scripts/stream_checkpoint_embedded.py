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

RESUMABLE: the fold persists its own progress as it goes, so a
kill (OOM or otherwise) costs at most one segment of work instead
of the whole fold — on production the first version of this tool
folded 3,901,568 ops in 498 s and then died in the checkpoint
phase with nothing saved. Every --segment-ops ops (default
400,000) the fold writes an intermediate snapshot plus a marker
(stream-progress.json: ops folded, AOF byte offset, head, and the
snapshot's size + SHA-256). The intermediate snapshot is the
engine's snapshot format written by a local mirror of
save_snapshot's serializer — save_snapshot itself cannot run
mid-fold (it anchors by appending a checkpoint op through
_log_append, whose seq comes from the not-yet-built log, and the
append handle only opens after _load returns), so the mirror
emits the identical dict minus the anchor op, with
checkpoint_seq = ops folded. On startup, a marker is trusted
only after the snapshot's size + SHA-256 match it, the AOF size
is exactly what the marker recorded, and the snapshot's own
head/checkpoint_seq agree with the marker; the state is then
restored through the engine's own load_snapshot() and the first
op streamed must chain (seq + prev_hash) from the recorded head
— any mismatch and the fold starts fresh rather than trusting a
torn or foreign resume point. --restart forces a fresh fold.
The marker is deleted once the real checkpoint lands; the final
checkpoint overwrites the intermediate snapshot with the
canonical anchored one.

Snapshot-restore note: snapshots keep HEAD values only, so a
resumed fold continues from collapsed version chains. That is
sound here for the same reason the engine's own snapshot load is:
apply_op only ever reads the HEAD value (index maintenance), and
the final artifact is a HEAD-only snapshot either way.

What it deliberately does NOT do, per operator directive (the
dir is ours and trusted): no verify() pass — neither the explicit
chain verify nor the stock open's self-heal (overridden off,
same as _BoundedNEDB). Structural soundness is still enforced
while streaming, at zero hashing cost: every op's seq must be
the next expected seq and its prev_hash must equal the running
head (field comparison only). Any mismatch — or any line that
fails to parse — aborts the conversion; this tool never rewrites
the AOF. The audited path remains scripts/checkpoint_embedded.py
(stock open + full verify) whenever the dir is small enough to
afford it.

Memory note: the fold keeps the engine's MVCC version chains,
exactly as the stock open does, so peak RSS tracks live state +
version history — the same state the bounded open itself loads
from the snapshot. What is gone is the second full copy of
history the stock loader keeps as parsed Op objects.

  python scripts/stream_checkpoint_embedded.py --dir ../data/vision-embedded
  python scripts/stream_checkpoint_embedded.py --dir ../data/vision-embedded --segment-ops 50000
  python scripts/stream_checkpoint_embedded.py --dir ../data/vision-embedded --restart

Progress: phase banners plus a ticking status line (AOF read %,
current segment, RSS, elapsed), same as checkpoint_embedded.py.
Finishes by reopening the dir through Vision's bounded path to
prove it engages (mode must be snapshot-bounded).
"""
import argparse
import asyncio
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_MARKER = "stream-progress.json"
_MARKER_VERSION = 1
_DEFAULT_SEGMENT_OPS = 400_000


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
        self.segment = 0
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
        if self.segment:
            parts.append(f"seg {self.segment}")
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

    def note(self, msg: str) -> None:
        """A durable line that does not fight the ticker line."""
        if self._tty:
            print(f"\r{'':<100}\r", end="", flush=True)
        print(msg, flush=True)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if self._tty:
            print(f"\r{'':<100}\r", end="", flush=True)


class _StreamError(Exception):
    """AOF is structurally unsound; abort before writing anything."""


# ── resumable-fold state ─────────────────────────────────────────────
#
# The marker is the commit point: it is replaced (atomically) only
# AFTER the intermediate snapshot it describes is fully written, and
# it carries that snapshot's size + SHA-256, so a half-written or
# superseded snapshot can never pass as a valid resume point.

def _sha256_file(path: str) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_bytes(path: str, raw: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(raw)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _write_resume_snapshot(db, *, ops: int, head: str,
                           nonces: dict, idem: dict) -> None:
    """Serialize HEAD state to <db.path>/snapshot.json in the
    engine's snapshot format — nedb.snapshot.save_snapshot steps
    2–6, minus step 1 (the AOF-anchoring checkpoint op, which
    cannot be appended mid-load; see the module docstring).
    ``seq``/``checkpoint_seq`` are the number of ops folded."""
    import base64
    import json

    from nedb import crypto as _crypto
    from nedb.snapshot import SNAP_VERSION
    from nedb.store import TOMB

    store_data = {}
    for key, versions in db.store._v.items():
        last_seq, last_val = versions[-1]
        store_data[key] = {
            "v": None if last_val is TOMB else last_val,
            "seq": last_seq,
            "deleted": last_val is TOMB,
        }
    rel_data = {}
    for (frm, rel), edges in db.relations._adj.items():
        rel_data[f"{frm}|{rel}"] = [
            {"to": to, "added": added, "removed": removed}
            for to, added, removed in edges
        ]
    dek = getattr(db, "_dek", None)
    blobs_data = {}
    for tier_name, bs in db.blobs.items():
        blobs_data[tier_name] = {
            "chunks": {
                h: base64.b64encode(
                    _crypto.chunk_encode(data, dek) if dek else data
                ).decode()
                for h, data in bs.chunks.items()
            },
            "files": bs.files,
            "logical_bytes": bs.logical_bytes,
            "dedup_hits": bs.dedup_hits,
        }
    snap = {
        "version": SNAP_VERSION,
        "seq": ops,
        "head": head,
        "checkpoint_seq": ops,
        "store": store_data,
        "relations": rel_data,
        "index_config": [list(t) for t in db.indexes.config],
        "nonces": dict(nonces),
        "idem": {k: v for k, v in idem.items()},
        "blobs": blobs_data,
    }
    raw = json.dumps(snap, separators=(",", ":")).encode()
    _atomic_write_bytes(
        os.path.join(db.path, "snapshot.json"),
        _crypto.snapshot_encode(raw, dek))


def _persist_segment(db, path: str, *, ops: int, aof_offset: int,
                     aof_size: int, head: str, nonces: dict,
                     idem: dict) -> None:
    """Make the fold-so-far durable: intermediate snapshot first,
    then the marker that vouches for it (in that order, always)."""
    import json

    _write_resume_snapshot(db, ops=ops, head=head,
                           nonces=nonces, idem=idem)
    snap_path = os.path.join(path, "snapshot.json")
    marker = {
        "version": _MARKER_VERSION,
        "ops": ops,
        "aof_offset": aof_offset,
        "aof_size": aof_size,
        "head": head,
        "snapshot_bytes": os.path.getsize(snap_path),
        "snapshot_sha256": _sha256_file(snap_path),
    }
    _atomic_write_bytes(
        os.path.join(path, _MARKER),
        json.dumps(marker).encode())


def _read_resume_state(path: str, aof_path: str, dek):
    """Return (marker, None) when a saved fold is provably
    consistent and safe to resume, (None, None) when there is no
    saved fold, or (None, reason) when saved state exists but must
    not be trusted — the caller then folds fresh. Nothing is
    loaded or mutated here; trust is established from bytes."""
    import json
    import re

    marker_path = os.path.join(path, _MARKER)
    if not os.path.exists(marker_path):
        return None, None
    try:
        with open(marker_path, encoding="utf-8") as fh:
            marker = json.load(fh)
        if marker.get("version") != _MARKER_VERSION:
            return None, "marker version is not understood"
        ops = int(marker["ops"])
        aof_offset = int(marker["aof_offset"])
        aof_size = int(marker["aof_size"])
        head = str(marker["head"])
    except Exception as exc:
        return None, f"marker unreadable ({type(exc).__name__})"
    snap_path = os.path.join(path, "snapshot.json")
    if not os.path.exists(snap_path):
        return None, "marker present but snapshot.json is missing"
    if os.path.getsize(snap_path) != marker["snapshot_bytes"]:
        return None, "snapshot.json size does not match the marker"
    if _sha256_file(snap_path) != marker["snapshot_sha256"]:
        return None, ("snapshot.json hash does not match the marker "
                      "(torn or replaced write)")
    try:
        actual_aof_size = os.path.getsize(aof_path)
    except OSError:
        actual_aof_size = -1
    if actual_aof_size != aof_size or aof_offset > aof_size:
        return None, "log.aof size changed since the marker was written"
    if dek is None:
        # Plain-JSON snapshot (Vision's dirs are unencrypted): the
        # snapshot's own header must agree with the marker — the
        # same prefix cross-check _BoundedNEDB performs.
        try:
            with open(snap_path, "rb") as fh:
                prefix = fh.read(1024).decode("utf-8", "ignore")
            m_head = re.search(r'"head"\s*:\s*"([0-9a-f]+)"', prefix)
            m_seq = re.search(r'"checkpoint_seq"\s*:\s*(\d+)', prefix)
            if (not m_head or m_head.group(1) != head
                    or not m_seq or int(m_seq.group(1)) != ops):
                return None, ("snapshot header (head/checkpoint_seq) "
                              "disagrees with the marker")
        except OSError:
            return None, "snapshot.json unreadable"
    marker.update({"ops": ops, "aof_offset": aof_offset,
                   "aof_size": aof_size, "head": head})
    return marker, None


def _discard_marker(path: str) -> bool:
    marker_path = os.path.join(path, _MARKER)
    if os.path.exists(marker_path):
        os.remove(marker_path)
        return True
    return False


def _stream_nedb_class(prog=None, segment_ops: int = _DEFAULT_SEGMENT_OPS,
                       restart: bool = False):
    """NEDB subclass whose _load streams instead of retaining ops,
    persisting resumable fold state every ``segment_ops`` ops.

    Built lazily so importing this module stays free of the engine
    import, mirroring app.embedded_store._bounded_nedb_class().
    """
    import json

    from nedb import NEDB
    from nedb import crypto as _crypto
    from nedb import snapshot as _snap
    from nedb.engine import apply_op
    from nedb.log import GENESIS, Op

    # One shared inert Op pads log.ops — see the module docstring.
    # OpLog.load() semantics only ever read .nonce/.client/.idem off
    # padded entries, and the checkpoint append reads len() and
    # _head, both of which _load sets explicitly below.
    _PAD = Op(0, "", 0, "checkpoint", {}, 0.0, None, "", "")

    def _note(msg: str) -> None:
        if prog is not None:
            prog.note(msg)
        else:
            print(msg, flush=True)

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
            # 2) resume an earlier fold when its saved state is
            #    provably consistent (see _read_resume_state);
            #    otherwise fold from genesis.
            try:
                aof_size = os.path.getsize(self._aof_path)
            except OSError:
                aof_size = 0
            marker = None
            if restart:
                if _discard_marker(self.path):
                    _note("--restart: discarded saved fold state — "
                          "folding from scratch")
            else:
                marker, reason = _read_resume_state(
                    self.path, self._aof_path, self._dek)
                if reason is not None:
                    _note(f"saved fold state rejected ({reason}) — "
                          f"folding from scratch")
                    _discard_marker(self.path)
            count = 0
            head = GENESIS
            last_nonce: dict = {}
            idem: dict = {}
            start_offset = 0
            resumed_from = None
            seg_no = 0
            if marker is not None:
                snap_seq = _snap.load_snapshot(self)
                if snap_seq != marker["ops"]:
                    raise _StreamError(
                        f"resume aborted: snapshot checkpoint_seq "
                        f"{snap_seq} != marker ops {marker['ops']}; "
                        f"rerun with --restart")
                count = marker["ops"]
                head = marker["head"]
                start_offset = marker["aof_offset"]
                last_nonce = dict(self.log._last_nonce)
                idem = dict(self.log._idem)
                resumed_from = count
                seg_no = count // segment_ops if segment_ops else 0
                if prog is not None:
                    prog.segment = seg_no
                pct = (start_offset * 100 // aof_size) if aof_size else 100
                _note(f"resuming at op {count:,} ({pct}% of the AOF) — "
                      f"saved state verified, continuing the fold")
            # 3) stream the AOF from start_offset: parse, check
            #    structure, fold, discard. Bookkeeping mirrors
            #    OpLog.load(). Byte offsets are tracked off the raw
            #    lines so a resume point is an exact file position.
            seg_since = 0
            if os.path.exists(self._aof_path):
                with open(self._aof_path, "rb") as fh:
                    fh.seek(start_offset)
                    offset = start_offset
                    while offset < aof_size:
                        raw = fh.readline()
                        if not raw:
                            break
                        offset += len(raw)
                        try:
                            text = raw.decode("utf-8")
                        except UnicodeDecodeError as exc:
                            raise _StreamError(
                                f"AOF line at seq {count} is not "
                                f"UTF-8 ({exc}); aborting, fold state "
                                f"on disk is untouched") from exc
                        line = _crypto.aof_decode(text, self._dek)
                        if not line:
                            continue
                        try:
                            op = Op.from_dict(json.loads(line))
                        except Exception as exc:
                            raise _StreamError(
                                f"AOF line at seq {count} does not "
                                f"parse ({type(exc).__name__}: {exc}); "
                                f"aborting, fold state on disk is "
                                f"untouched") from exc
                        if op.seq != count or op.prev_hash != head:
                            raise _StreamError(
                                f"AOF chain structure breaks at seq "
                                f"{count}: op.seq={op.seq} "
                                f"prev_hash={op.prev_hash[:12]}… != "
                                f"running head {head[:12]}…; "
                                f"aborting, fold state on disk is "
                                f"untouched")
                        apply_op(self.store, self.relations,
                                 self.indexes, op, self.cause_map)
                        if op.nonce > last_nonce.get(op.client, 0):
                            last_nonce[op.client] = op.nonce
                        if op.idem is not None and op.idem not in idem:
                            idem[op.idem] = op.seq
                        head = op.hash
                        count += 1
                        seg_since += 1
                        if seg_since >= segment_ops:
                            seg_no += 1
                            _persist_segment(
                                self, self.path, ops=count,
                                aof_offset=offset, aof_size=aof_size,
                                head=head, nonces=last_nonce, idem=idem)
                            seg_since = 0
                            if prog is not None:
                                prog.segment = seg_no
                            pct = (offset * 100 // aof_size
                                   ) if aof_size else 100
                            _note(f"segment {seg_no} durable — "
                                  f"{count:,} ops folded ({pct}%), "
                                  f"resume point saved")
            # 4) install the padded log: correct length for seq
            #    allocation, head/nonce/idem as OpLog.load() sets.
            self.log.ops = [_PAD] * count
            self.log._last_nonce = last_nonce
            self.log._idem = idem
            self.log._head = head
            self._nonce = dict(last_nonce)
            self._stream_stats = {"ops": count, "head": head,
                                  "resumed_from": resumed_from,
                                  "segments": seg_no}

    return _StreamNEDB


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Streaming bounded-memory conversion of an "
                    "embedded NEDB dir for bounded opens")
    ap.add_argument("--dir", default="../data/vision-embedded",
                    help="embedded data dir (default ../data/vision-embedded)")
    ap.add_argument("--segment-ops", type=int,
                    default=_DEFAULT_SEGMENT_OPS,
                    help="fold ops between durable resume points "
                         f"(default {_DEFAULT_SEGMENT_OPS})")
    ap.add_argument("--restart", action="store_true",
                    help="ignore any saved fold state and fold "
                         "from genesis")
    args = ap.parse_args()
    path = args.dir
    if args.segment_ops < 1:
        print("ERROR: --segment-ops must be >= 1")
        return 2

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
        db = _stream_nedb_class(prog=prog,
                                segment_ops=args.segment_ops,
                                restart=args.restart)(path)
        stats = db._stream_stats
        resumed = (f"  (resumed at op {stats['resumed_from']:,})"
                   if stats.get("resumed_from") else "")
        print(f"streamed fold: {time.time() - t0:.1f}s  "
              f"ops={stats['ops']:,}  head={stats['head'][:12]}…{resumed}")
        prog.set_phase("phase 2/3: writing checkpoint + bounded-open "
                       "sidecar (engine checkpoint, stock shape)")
        from app.embedded_store import checkpoint_embedded_db
        info = checkpoint_embedded_db(db, path)
        print(f"checkpoint recorded: seq={info['checkpoint_seq']} "
              f"aof_offset={info['aof_offset']} "
              f"snapshot_bytes={info['snapshot_bytes']}")
        db.close()
        # The fold is now anchored by the real checkpoint; the
        # resume marker has served its purpose.
        _discard_marker(path)
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
