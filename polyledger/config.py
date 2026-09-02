"""Static configuration and runtime settings for PolyLedger."""

from __future__ import annotations 

import os
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------
# Chain / contract constants
# --------------------------------------------------------------------------

POLYGON_CHAIN_ID = 137
HYPERSYNC_URL = "https://polygon.hypersync.xyz"

# CTF Exchange V2 — live since the April 2026 contract migration.
CTF_EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310F59"
NEG_RISK_CTF_EXCHANGE_V2_B = "0xe2222d002000ba0053cef3375333610f64600036"

# CTF Exchange V1 — frozen, kept so historical fills stay queryable.
CTF_EXCHANGE_V1 = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE_V1 = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

EXCHANGES_V2 = [CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2_B]
EXCHANGES_V1 = [CTF_EXCHANGE_V1, NEG_RISK_CTF_EXCHANGE_V1]

# Human-readable contract labels, joined onto every fill row.
EXCHANGE_NAMES = {
    CTF_EXCHANGE_V2.lower(): "ctf_exchange_v2",
    NEG_RISK_CTF_EXCHANGE_V2.lower(): "neg_risk_ctf_exchange_v2",
    NEG_RISK_CTF_EXCHANGE_V2_B.lower(): "neg_risk_ctf_exchange_v2_b",
    CTF_EXCHANGE_V1.lower(): "ctf_exchange_v1",
    NEG_RISK_CTF_EXCHANGE_V1.lower(): "neg_risk_ctf_exchange_v1",
}

# V2: OrderFilled(bytes32 orderHash, address maker, address taker, uint8 side,
#                 uint256 tokenId, uint256 makerAmountFilled,
#                 uint256 takerAmountFilled, uint256 fee,
#                 bytes32 builder, bytes32 metadata)
ORDER_FILLED_V2_SIG = (
    "OrderFilled(bytes32,address,address,uint8,uint256,"
    "uint256,uint256,uint256,bytes32,bytes32)"
)
ORDER_FILLED_V2_TOPIC0 = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)

# V1: OrderFilled(bytes32 orderHash, address maker, address taker,
#                 uint256 makerAssetId, uint256 takerAssetId,
#                 uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)
ORDER_FILLED_V1_SIG = (
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
)
ORDER_FILLED_V1_TOPIC0 = (
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
)

# Both pUSD/USDC collateral and CTF outcome shares use 6 decimals.
TOKEN_DECIMALS = 6
UNIT = 10**TOKEN_DECIMALS

CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# Base64("0") — the cursor the CLOB expects for the first page.
CLOB_FIRST_CURSOR = "MA=="
# Base64("-1") — the cursor the CLOB returns once you hit the last page.
CLOB_LAST_CURSOR = "LTE="


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass
class Settings:
    """Everything tunable at runtime. Env vars take precedence over defaults."""

    db_path: Path = field(
        default_factory=lambda: Path(
            os.environ.get("POLYLEDGER_DB", "data/polyledger.duckdb")
        )
    )
    export_dir: Path = field(
        default_factory=lambda: Path(
            os.environ.get("POLYLEDGER_EXPORT_DIR", "data/parquet")
        )
    )

    hypersync_url: str = field(
        default_factory=lambda: os.environ.get("HYPERSYNC_URL", HYPERSYNC_URL)
    )
    hypersync_token: str | None = field(
        default_factory=lambda: os.environ.get("HYPERSYNC_BEARER_TOKEN")
        or os.environ.get("HYPERSYNC_API_TOKEN")
    )

    # Which contract generation to index. "v2" (default), "v1", or "all".
    contracts: str = field(
        default_factory=lambda: os.environ.get("POLYLEDGER_CONTRACTS", "v2")
    )
    # Explicit starting block. 0 means "probe the chain for the first fill".
    from_block: int = field(
        default_factory=lambda: _env_int("POLYLEDGER_FROM_BLOCK", 0)
    )
    # How many blocks behind the tip we stop, to stay clear of reorgs.
    reorg_buffer: int = field(default_factory=lambda: _env_int("POLYLEDGER_REORG_BUFFER", 64))
    # Fills buffered in memory before a checkpointed flush to DuckDB.
    flush_every: int = field(default_factory=lambda: _env_int("POLYLEDGER_FLUSH_EVERY", 50_000))

    # Shared HTTP politeness knobs for the CLOB and Gamma REST APIs.
    http_rate_limit: float = field(
        default_factory=lambda: _env_float("POLYLEDGER_HTTP_RPS", 8.0)
    )
    http_concurrency: int = field(
        default_factory=lambda: _env_int("POLYLEDGER_HTTP_CONCURRENCY", 8)
    )
    http_max_retries: int = field(
        default_factory=lambda: _env_int("POLYLEDGER_HTTP_RETRIES", 6)
    )
    http_timeout: float = field(
        default_factory=lambda: _env_float("POLYLEDGER_HTTP_TIMEOUT", 30.0)
    )

    def exchange_addresses(self) -> list[str]:
        mode = self.contracts.lower()
        if mode == "v1":
            return list(EXCHANGES_V1)
        if mode in ("all", "both"):
            return EXCHANGES_V2 + EXCHANGES_V1
        return list(EXCHANGES_V2)

    def topics(self) -> list[str]:
        mode = self.contracts.lower()
        if mode == "v1":
            return [ORDER_FILLED_V1_TOPIC0]
        if mode in ("all", "both"):
            return [ORDER_FILLED_V2_TOPIC0, ORDER_FILLED_V1_TOPIC0]
        return [ORDER_FILLED_V2_TOPIC0]

    def stream_key(self) -> str:
        """Checkpoint identity — changing `contracts` starts a separate cursor."""
        return f"order_filled:{self.contracts.lower()}"
