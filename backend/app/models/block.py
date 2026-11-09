from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class BlockSummary(BaseModel):
    height: int
    hash: str
    time: int
    tx_count: int
    size: int
    weight: Optional[int] = None
    algo: Optional[str] = None
    miner: Optional[dict] = None
    fee_reward_sats: Optional[int] = None
    subsidy_sats: Optional[int] = None


class TxSummary(BaseModel):
    txid: str
    vsize: int
    fee_sats: int
    fee_rate_sat_vbyte: float
    is_coinbase: bool = False


class CoinbaseDetail(BaseModel):
    address: Optional[str] = None
    scriptsig_hex: str
    scriptsig_text: str
    miner: Optional[dict] = None
    subsidy_sats: int
    fee_sats: int
    total_sats: int
    maturity: dict


class Block(BaseModel):
    height: int
    hash: str
    confirmations: int
    version: int
    version_hex: str
    merkleroot: str
    time: int
    mediantime: Optional[int] = None
    nonce: int
    bits: str
    difficulty: float
    chainwork: Optional[str] = None
    n_tx: int
    previousblockhash: Optional[str] = None
    nextblockhash: Optional[str] = None
    size: int
    strippedsize: Optional[int] = None
    weight: Optional[int] = None
    coinbase: Optional[CoinbaseDetail] = None
    txids: List[str] = []
