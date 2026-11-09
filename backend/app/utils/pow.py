"""Tri-PoW block algorithm classification — mirror of the daemon's
GetBlockPowAlgo (chainparams / validation era rules).

Interchained mainnet eras:

  height < 1                          -> sha256   (genesis era)
  1 <= height < 571799                -> yespower (yespowerForkHeight = 1)
  571799 <= height < 913982           -> sha256   (sha256 reactivation)
  height >= 913982 (triPowForkHeight) -> the block's version carries a
      2-bit lane tag in bits 13-14:  tag = (version >> 13) & 0x3
      0 = sha256, 1 = yespower, 2 = ethash, 3 = sha256 (reserved -> sha256)

The tag bits MUST NOT be read before the tri-PoW fork: pre-fork versions
reuse those bits for ordinary version rolling (e.g. mainnet block 900000
has version 0x20004000, which raw-decodes as "ethash" but is a sha256-era
block).
"""
from __future__ import annotations

YESPOWER_FORK_HEIGHT = 1
SHA256_REACTIVATION_HEIGHT = 571_799
TRIPOW_FORK_HEIGHT = 913_982

ALGO_SHA256 = "sha256"
ALGO_YESPOWER = "yespower"
ALGO_ETHASH = "ethash"

ALGOS = (ALGO_SHA256, ALGO_YESPOWER, ALGO_ETHASH)

_TAG_TO_ALGO = {
    0: ALGO_SHA256,
    1: ALGO_YESPOWER,
    2: ALGO_ETHASH,
    3: ALGO_SHA256,  # reserved — daemon treats it as sha256
}


def algo_for_block(version: int, height: int) -> str:
    """Return the PoW lane ('sha256' | 'yespower' | 'ethash') for a block
    with the given nVersion and height."""
    if height >= TRIPOW_FORK_HEIGHT:
        tag = (int(version) >> 13) & 0x3
        return _TAG_TO_ALGO[tag]
    if height < YESPOWER_FORK_HEIGHT:
        return ALGO_SHA256
    if height < SHA256_REACTIVATION_HEIGHT:
        return ALGO_YESPOWER
    return ALGO_SHA256
