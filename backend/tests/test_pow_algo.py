"""Unit tests for app.utils.pow.algo_for_block — the tri-PoW era mapping.

Runnable with plain python (python3 backend/tests/test_pow_algo.py) or
pytest. No third-party deps required.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.pow import algo_for_block  # noqa: E402


def test_genesis_era_is_sha256():
    assert algo_for_block(0x20000000, 0) == "sha256"
    assert algo_for_block(1, 0) == "sha256"


def test_yespower_era():
    # yespowerForkHeight = 1
    assert algo_for_block(0x20000000, 1) == "yespower"
    assert algo_for_block(0x20000000, 300_000) == "yespower"
    assert algo_for_block(0x20000000, 571_798) == "yespower"


def test_sha256_reactivation_era():
    # sha256ReactivationHeight = 571799
    assert algo_for_block(0x20000000, 571_799) == "sha256"
    assert algo_for_block(0x20000000, 913_981) == "sha256"


def test_prefork_tag_bits_are_never_read():
    # The trap: mainnet block 900000 has version 0x20004000, whose bits
    # 13-14 raw-decode as the ethash tag — but pre-fork it is sha256.
    assert algo_for_block(0x20004000, 900_000) == "sha256"
    assert algo_for_block(0x20002000, 900_000) == "sha256"
    assert algo_for_block(0x20006000, 571_799) == "sha256"


def test_tripow_fork_tags():
    # triPowForkHeight = 913982; tag = (version >> 13) & 0x3
    assert algo_for_block(0x20000000, 913_982) == "sha256"    # tag 0
    assert algo_for_block(0x20002000, 913_982) == "yespower"  # tag 1
    assert algo_for_block(0x20004000, 913_982) == "ethash"    # tag 2
    assert algo_for_block(0x20006000, 913_982) == "sha256"    # tag 3 reserved


def test_real_postfork_blocks():
    # Block 914572 has version 0x20002000 -> yespower; heights past the
    # fork keep reading the tag.
    assert algo_for_block(0x20002000, 914_572) == "yespower"
    assert algo_for_block(0x20004000, 914_600) == "ethash"
    assert algo_for_block(0x20000000, 1_000_000) == "sha256"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"PASS {t.__name__}")
    print(f"ALL {len(tests)} POW ALGO TESTS PASSED")
