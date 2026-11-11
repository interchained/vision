"""Regression tests for the bounded embedded open.

The stock engine open is O(history) (full AOF parse + fold +
full-chain verify on every NEDB(path)); the bounded open restores
a checkpoint snapshot and parses only the delta ops after it.
These tests pin the properties that make the bounded path safe:

* data written before the checkpoint reads back identically,
* the open actually takes the bounded path (open-info mode),
* writes made AFTER a bounded reopen survive the NEXT reopen —
  the seq-continuation property (a new op must land after the
  checkpoint seq, never be mistaken for pre-checkpoint history),
* a tampered sidecar degrades to a correct fallback open, never
  to wrong data.

Run from backend/:  python -m pytest tests/test_embedded_open_bounded.py -q
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.embedded_store import EmbeddedNedbStore


def run(coro):
    return asyncio.run(coro)


def _fill(store, n=200):
    async def go():
        for i in range(n):
            await store.set(f"key:{i}", f"value-{i}")
        await store.zadd("recent", {str(i): float(i) for i in range(20)})
    run(go())


def _mode(store):
    return store._db._open_info.get("mode")


def test_bounded_reopen_roundtrip(tmp_path):
    path = str(tmp_path / "embedded")
    s = EmbeddedNedbStore(path)
    run(s.open())
    _fill(s)
    run(s.aclose())  # clean close checkpoints + writes the sidecar
    assert (tmp_path / "embedded" / "snapshot.json").exists()
    assert (tmp_path / "embedded" / "vision-open.json").exists()

    s2 = EmbeddedNedbStore(path)
    run(s2.open())
    assert _mode(s2) == "snapshot-bounded"
    assert run(s2.get("key:0")) == "value-0"
    assert run(s2.get("key:199")) == "value-199"
    assert run(s2.zrevrange("recent", 0, 2)) == ["19", "18", "17"]
    run(s2.aclose())


def test_write_after_bounded_reopen_survives(tmp_path):
    path = str(tmp_path / "embedded")
    s = EmbeddedNedbStore(path)
    run(s.open())
    _fill(s, n=50)
    run(s.aclose())

    s2 = EmbeddedNedbStore(path)
    run(s2.open())
    assert _mode(s2) == "snapshot-bounded"
    run(s2.set("key:after", "written-post-checkpoint"))
    seq_after_write = s2._db.seq
    run(s2.aclose())

    s3 = EmbeddedNedbStore(path)
    run(s3.open())
    assert _mode(s3) == "snapshot-bounded"
    # The post-checkpoint write must NOT be skipped as history.
    assert run(s3.get("key:after")) == "written-post-checkpoint"
    assert run(s3.get("key:49")) == "value-49"
    # Seq continued globally, never restarted or reused.
    assert s3._db.seq >= seq_after_write
    run(s3.aclose())


def test_tampered_sidecar_falls_back_safely(tmp_path):
    path = tmp_path / "embedded"
    s = EmbeddedNedbStore(str(path))
    run(s.open())
    _fill(s, n=50)
    run(s.aclose())

    sidecar = path / "vision-open.json"
    data = json.loads(sidecar.read_text())
    data["head"] = "0" * 64  # delta can no longer chain from it
    sidecar.write_text(json.dumps(data))

    s2 = EmbeddedNedbStore(str(path))
    run(s2.open())
    assert _mode(s2) != "snapshot-bounded"
    assert run(s2.get("key:0")) == "value-0"
    assert run(s2.get("key:49")) == "value-49"
    run(s2.aclose())


def test_missing_sidecar_falls_back_safely(tmp_path):
    path = tmp_path / "embedded"
    s = EmbeddedNedbStore(str(path))
    run(s.open())
    _fill(s, n=50)
    run(s.aclose())
    (path / "vision-open.json").unlink()

    s2 = EmbeddedNedbStore(str(path))
    run(s2.open())
    assert _mode(s2) != "snapshot-bounded"
    assert run(s2.get("key:25")) == "value-25"
    run(s2.aclose())
