"""Centralized configuration via pydantic-settings.

All environment variables are read once at import time and made available as a
typed `settings` singleton. Defaults match the interchainedd defaults.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ── ITC Core JSON-RPC ──
    # `ITC_RPC_HOST` is flexible:
    #   • bare hostname  → "10.0.0.1"        (port supplied separately)
    #   • host:port      → "10.0.0.1:17100"  (port may be omitted)
    #   • full URL       → "https://node.example.com"  (proxied; port ignored)
    # `ITC_RPC_PORT` is optional — accepts empty/non-numeric strings as "unset".
    ITC_RPC_HOST: str = "127.0.0.1"
    ITC_RPC_PORT: Optional[int] = None
    ITC_RPC_USER: str = Field(
        default="",
        validation_alias=AliasChoices("ITC_RPC_USER", "ITC_RPC_USERNAME"),
    )
    ITC_RPC_PASS: str = Field(
        default="",
        validation_alias=AliasChoices("ITC_RPC_PASS", "ITC_RPC_PASSWORD"),
    )
    ITC_WALLET_NAME: str = "bulk_payout_wallet"
    ITC_RPC_TIMEOUT: int = 30
    # Optional: full broadcast node URL (auth embedded). If set, takes priority
    # over the host/port/user/pass quartet for `sendrawtransaction` calls.
    ITC_BROADCAST_NODE: str = ""

    # ── ElectrumX ──
    # Same flexible host/port handling as the RPC.
    ELECTRUMX_HOST: str = "127.0.0.1"
    ELECTRUMX_PORT: Optional[int] = None
    ELECTRUMX_TLS: bool = False
    ELECTRUMX_TIMEOUT: int = 5

    @field_validator("ITC_RPC_PORT", "ELECTRUMX_PORT", mode="before")
    @classmethod
    def _coerce_optional_int(cls, v):
        """Treat empty / non-numeric strings as `None` so a proxied host:port
        URL doesn't require a separate port secret."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            v = v.strip()
            if not v.isdigit():
                return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    # ── SQLite (backup / migration source ONLY) ──
    # The live store is the embedded NEDB engine; SQLITE_DB_PATH now
    # names only the cold backup file the offline migrators read.
    # Set DISABLE_SQLITE=1 in production: any attempt to construct
    # the SQLite store from app code then raises RuntimeError, so a
    # regression can never silently fall back to the file.
    SQLITE_DB_PATH: str = "../data/vision.db"
    DISABLE_SQLITE: str = "0"

    @property
    def sqlite_disabled(self) -> bool:
        return str(self.DISABLE_SQLITE).strip().lower() in (
            "1", "true", "yes", "on",
        )

    # ── KV store ──
    # STORE_MODE accepts exactly one value: "embedded" — the KV and
    # domain layers are served by an in-process embedded NEDB engine
    # on NEDB_EMBEDDED_DIR, no HTTP hop, no separate nedbd process,
    # and no SQLite in the serving path. Any other value fails
    # startup with a clear error (see the validator below).
    STORE_MODE: str = "embedded"
    NEDB_EMBEDDED_DIR: str = "../data/vision-embedded"

    @field_validator("STORE_MODE", mode="before")
    @classmethod
    def _validate_store_mode(cls, v):
        if str(v).strip().lower() != "embedded":
            raise ValueError(
                f"STORE_MODE={v!r} is not supported: the only live "
                "store is the embedded NEDB engine "
                "(STORE_MODE=embedded). SQLite is backup / "
                "migration-source only — see DEPLOY.md."
            )
        return "embedded"

    # ── NEDB over HTTP (legacy — backfill source only) ──
    # When NEDB_URL is set (e.g. "http://127.0.0.1:7070"), the
    # one-time backwards history sync may copy block KV docs
    # from the old nedbd store as a secondary source (after live RPC).
    # It is never used for serving reads or writes any more.
    NEDB_URL: str = ""
    NEDB_DB_NAME: str = "vision"
    NEDBD_TOKEN: str = ""

    # ── Backend ──
    BACKEND_HOST: str = "0.0.0.0"
    BACKEND_PORT: int = 8080
    RATE_LIMIT_PER_MIN: int = 300
    ALLOWED_ORIGINS: str = "*"
    LOG_LEVEL: str = "info"

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        return str(v).lower()

    # ── Indexer ──
    REORG_DEPTH: int = 6
    INDEXER_TICK_SECONDS: int = 5
    INDEXER_BATCH_SIZE: int = 200
    INDEXER_FETCH_CONCURRENCY: int = 8
    START_FROM_HEIGHT: int = 0

    # ── Token registry ──
    TOKEN_REGISTRY_REFRESH_SECONDS: int = 30

    # ── Price oracle (optional) ──
    PRICE_API_URL: str = "https://vwap.interchained.org/data/markets"
    PRICE_REFRESH_SECONDS: int = 15

    # ── Verification API (optional) ──
    VERIFICATION_API_URL: str = ""
    VERIFICATION_API_KEY: str = ""

    # ── Webhooks ──
    WEBHOOK_MAX_RETRIES: int = 5
    WEBHOOK_TIMEOUT: int = 10

    # ── Admin (Pool Operator Snapshot Rewards) ──
    # Shared secret guarding all /api/admin/* writes. If empty, admin routes
    # return 503 (locked) rather than silently allowing unauthenticated writes.
    ADMIN_TOKEN: str = ""
    # Maximum block span a single snapshot may scan (guards against accidental
    # full-chain scans). ~3 weeks of blocks at 60s spacing.
    SNAPSHOT_MAX_SPAN: int = 100_000
    # Default reward per block for the program (decimal string, 8 dp).
    POOL_REWARD_PER_BLOCK: str = "0.10301990"

    # ── Data files ──
    DATA_DIR: str = "../data"

    @property
    def allowed_origins_list(self) -> List[str]:
        if self.ALLOWED_ORIGINS == "*":
            return ["*"]
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]

    @property
    def rpc_base_url(self) -> str:
        """JSON-RPC base URL without wallet prefix.

        Handles host formats:
          • full URL with scheme        → returned as-is
          • bare host:port embedded     → wrapped in http://
          • bare host, explicit port    → http://host:port
          • bare host, NO port (proxy)  → http://host  (proxy owns routing)
        """
        h = self.ITC_RPC_HOST.strip().rstrip("/")
        if h.startswith(("http://", "https://")):
            return h  # already a full URL
        if ":" in h and not h.startswith("["):
            return f"http://{h}"  # host:port embedded
        if self.ITC_RPC_PORT:
            return f"http://{h}:{self.ITC_RPC_PORT}"
        # Port is handled by the proxy — use host on standard HTTP
        return f"http://{h}"

    @property
    def rpc_wallet_url(self) -> str:
        """JSON-RPC URL with /wallet/{name} prefix (for token RPCs)."""
        if self.ITC_WALLET_NAME:
            return f"{self.rpc_base_url}/wallet/{self.ITC_WALLET_NAME}"
        return self.rpc_base_url

    @property
    def electrumx_endpoint(self) -> tuple[str, int]:
        """Resolve the ElectrumX (host, port) from a flexible host string."""
        h = self.ELECTRUMX_HOST.strip()
        if "://" in h:
            # Strip scheme — ElectrumX uses raw TCP/SSL.
            h = h.split("://", 1)[1]
        if ":" in h and not h.startswith("["):
            host, _, port_s = h.rpartition(":")
            try:
                return host, int(port_s)
            except ValueError:
                pass
        return h, self.ELECTRUMX_PORT or 50001

    @property
    def rpc_configured(self) -> bool:
        return bool(self.ITC_RPC_USER and self.ITC_RPC_PASS)

    def load_special_wallets(self) -> List[dict]:
        path = Path(__file__).parent.parent / self.DATA_DIR / "special_wallets.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
            return data.get("wallets", [])
        except Exception:
            return []

    def load_known_pools(self) -> List[dict]:
        path = Path(__file__).parent.parent / self.DATA_DIR / "known_pools.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
            return data.get("pools", [])
        except Exception:
            return []


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
