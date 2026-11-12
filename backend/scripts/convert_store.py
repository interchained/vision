#!/usr/bin/env python3
"""convert_store.py — migrate a Vision embedded store from the Python NEDB
engine (v1: snapshot.json + log.aof) to the Rust NedbCore engine (v2:
segments + portals), in the SAME data directory.

Layout produced:
    <data_dir>/                 v1 files left untouched (archive / rollback)
    <data_dir>/rust/            the v2 store (NedbCore)
    <data_dir>/conversion.json  the conversion marker + verification record

Why not the Rust engine's built-in v1->v2 auto-migration? It reads only
log.aof. A checkpointed store keeps its folded state in snapshot.json and
only a tail in the AOF, so the auto-migrator would silently drop the
folded state. This converter streams the LIVE state (snapshot + tail,
composed by the engine itself) and writes it into NedbCore doc by doc.

Source reads go through Vision's own _BoundedNEDB when --vision-backend
is given (its offset-indexed snapshot load is what keeps the source open
bounded on a production-size store); otherwise the stock NEDB class.

Verification (all inside the run, all must pass):
  * per-collection live-doc counts: source == target
  * a rolling SHA-256 over canonical (coll, id, body) of every live doc:
    source digest == target read-back digest
  * NedbCore.verify() is True on the converted store

Progress: phase banners + a ticking per-page line (docs, rate, elapsed,
RSS) on stdout. Nothing here writes to the v1 files.
"""

import argparse
import hashlib
import json
import os
import resource
import sys
import time

PAGE = 2000
META_KEYS = {"_hash", "_seq", "_coll"}


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def nql_escape(s):
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


def clean_body(doc):
    """Strip engine-owned metadata; map v1 provenance fields to the names
    NedbCore.put reads. Returns (doc_id, body)."""
    body = {}
    for k, v in doc.items():
        if k in META_KEYS or k == "_id":
            continue
        if k == "_valid_from":
            body["valid_from"] = v
        elif k == "_valid_to":
            body["valid_to"] = v
        elif k == "_caused_by":
            continue  # v1 seq references are meaningless across the boundary
        else:
            body[k] = v
    return doc["_id"], body


def canon_digest(h, coll, doc_id, body):
    h.update(coll.encode())
    h.update(b"\x00")
    h.update(str(doc_id).encode())
    h.update(b"\x00")
    h.update(json.dumps(body, sort_keys=True, separators=(",", ":")).encode())
    h.update(b"\x01")


def page_query(db, coll, last_id, page):
    if last_id is None:
        nql = f'FROM {coll} ORDER BY _id LIMIT {page}'
    else:
        nql = (f'FROM {coll} WHERE _id > "{nql_escape(last_id)}" '
               f'ORDER BY _id LIMIT {page}')
    return db.query(nql)


def open_source(src, engine, vision_backend):
    if engine == "bounded":
        sys.path.insert(0, vision_backend)
        # app.embedded_store imports its siblings relatively; import as part
        # of the app package the way the backend itself does.
        parent = os.path.dirname(vision_backend.rstrip("/"))
        if parent not in sys.path:
            sys.path.insert(0, parent)
        from app.embedded_store import _bounded_nedb_class
        cls = _bounded_nedb_class()
        return cls(src)
    from nedb import NEDB
    return NEDB(src)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="the v1 data directory")
    ap.add_argument("--engine", choices=["bounded", "stock"], default="bounded")
    ap.add_argument("--vision-backend",
                    default=os.path.dirname(os.path.dirname(
                        os.path.abspath(__file__))),
                    help="path to Vision's backend/ dir (for the bounded "
                         "reader); defaults to this script's backend/")
    ap.add_argument("--page", type=int, default=PAGE)
    args = ap.parse_args()

    src = os.path.abspath(args.src)
    dst = os.path.join(src, "rust")
    marker = os.path.join(src, "conversion.json")
    if os.path.exists(marker):
        print("!! conversion.json already exists — refusing to re-convert. "
              "Remove it (and rust/) to redo.")
        return 1
    if args.engine == "bounded" and not args.vision_backend:
        print("!! --engine bounded needs --vision-backend")
        return 1

    t0 = time.time()
    print(f"== phase 1/4: open source store ({args.engine}) ==", flush=True)
    db = open_source(src, args.engine, args.vision_backend)
    colls = sorted(db.collections())
    print(f"   collections: {colls} | RSS {rss_mb():.0f} MB", flush=True)

    print("== phase 2/4: stream live docs into NedbCore ==", flush=True)
    from nedb._native import NedbCore
    os.makedirs(dst, exist_ok=True)
    core = NedbCore.open(dst)
    counts = {}
    digest = hashlib.sha256()
    total = 0
    for coll in colls:
        last = None
        n = 0
        while True:
            rows = page_query(db, coll, last, args.page)
            if not rows:
                break
            for doc in rows:
                doc_id, body = clean_body(doc)
                core.put(coll, str(doc_id),
                         json.dumps(body, separators=(",", ":")))
                canon_digest(digest, coll, str(doc_id), body)
                n += 1
                last = doc["_id"]
            total += len(rows)
            print(f"   [{coll}] {n} docs | total {total} | "
                  f"{total / max(time.time() - t0, 0.01):.0f} docs/s | "
                  f"RSS {rss_mb():.0f} MB | elapsed {time.time() - t0:.0f}s",
                  flush=True)
            if len(rows) < args.page:
                break
        counts[coll] = n
        print(f"   [{coll}] done: {n} live docs", flush=True)
    core.flush()
    src_digest = digest.hexdigest()
    print(f"   source digest {src_digest[:16]}… | core seq {core.seq()} | "
          f"verify {core.verify()}", flush=True)

    print("== phase 3/4: read back and compare ==", flush=True)
    digest2 = hashlib.sha256()
    counts2 = {}
    for coll in colls:
        last = None
        n = 0
        while True:
            rows = core.query(
                f'FROM {coll} ORDER BY _id LIMIT {args.page}'
                if last is None else
                f'FROM {coll} WHERE _id > "{nql_escape(last)}" '
                f'ORDER BY _id LIMIT {args.page}')
            if not rows:
                break
            for raw in rows:
                doc = json.loads(raw)
                doc_id, body = clean_body(doc)
                canon_digest(digest2, coll, str(doc_id), body)
                n += 1
                last = doc["_id"]
            if len(rows) < args.page:
                break
        counts2[coll] = n
    ok = (counts == counts2 and digest2.hexdigest() == src_digest
          and core.verify())
    print(f"   counts source {counts}", flush=True)
    print(f"   counts target {counts2}", flush=True)
    print(f"   digest match: {digest2.hexdigest() == src_digest} | "
          f"verify: {core.verify()}", flush=True)
    if not ok:
        print("!! VERIFICATION FAILED — rust/ is left in place for "
              "inspection; the v1 store is untouched and still live.")
        return 1

    print("== phase 4/4: write marker ==", flush=True)
    record = {
        "converted_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "source_engine": args.engine,
        "counts": counts,
        "total_docs": total,
        "digest_sha256": src_digest,
        "core_seq": core.seq(),
        "core_head": core.head(),
        "elapsed_s": round(time.time() - t0, 1),
    }
    with open(marker, "w", encoding="utf-8") as fh:
        json.dump(record, fh, indent=2)
    print(f"   DONE in {record['elapsed_s']}s — {total} docs, "
          f"digest {src_digest[:16]}…, head {record['core_head'][:16]}…",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
