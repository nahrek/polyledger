"""Stage 1 — market metadata from the CLOB, plus Gamma gap-filling."""

from __future__ import annotations

import logging

from pydantic import ValidationError

from ..clients.clob import ClobClient
from ..clients.gamma import GammaClient
from ..config import Settings
from ..http import HttpSession
from ..models import Market, Token, market_from_clob, market_from_gamma
from ..storage import Store

log = logging.getLogger(__name__)


async def sync_markets(store: Store, settings: Settings) -> dict[str, int]:
    """Pull every market the CLOB knows about into `markets` and `tokens`."""
    stats = {"pages": 0, "markets": 0, "tokens": 0, "invalid": 0}
    async with HttpSession(
        rate=settings.http_rate_limit,
        concurrency=settings.http_concurrency,
        max_retries=settings.http_max_retries,
        timeout=settings.http_timeout,
    ) as session:
        client = ClobClient(session)
        async for page in client.iter_markets():
            markets: list[Market] = []
            tokens: list[Token] = []
            for raw in page:
                try:
                    market, market_tokens = market_from_clob(raw)
                except ValidationError as exc:
                    stats["invalid"] += 1
                    log.warning(
                        "skipping malformed market %s: %s",
                        raw.get("condition_id"), exc.errors()[:2],
                    )
                    continue
                markets.append(market)
                tokens.extend(market_tokens)

            store.upsert_markets(markets)
            store.upsert_tokens(tokens)
            stats["pages"] += 1
            stats["markets"] += len(markets)
            stats["tokens"] += len(tokens)
            if stats["pages"] % 10 == 0:
                log.info(
                    "markets: %s pages, %s markets, %s tokens",
                    stats["pages"], stats["markets"], stats["tokens"],
                )

    log.info(
        "markets sync done: %s markets, %s tokens, %s skipped",
        stats["markets"], stats["tokens"], stats["invalid"],
    )
    return stats


async def backfill_missing_markets(store: Store, settings: Settings) -> dict[str, int]:
    """Resolve token ids seen on chain that the CLOB listing didn't cover."""
    missing = store.unmatched_token_ids()
    if not missing:
        log.info("no unmatched token ids — nothing to backfill")
        return {"requested": 0, "resolved": 0, "still_missing": 0}

    async with HttpSession(
        rate=settings.http_rate_limit,
        concurrency=settings.http_concurrency,
        max_retries=settings.http_max_retries,
        timeout=settings.http_timeout,
    ) as session:
        payloads = await GammaClient(session).markets_by_token_ids(missing)

    markets: list[Market] = []
    tokens: list[Token] = []
    for raw in payloads:
        try:
            market, market_tokens = market_from_gamma(raw)
        except ValidationError as exc:
            log.warning("skipping malformed gamma market: %s", exc.errors()[:2])
            continue
        markets.append(market)
        tokens.extend(market_tokens)

    store.upsert_markets(markets)
    store.upsert_tokens(tokens)

    still_missing = len(store.unmatched_token_ids())
    log.info(
        "gamma backfill: %s requested, %s resolved, %s still unmatched",
        len(missing), len(missing) - still_missing, still_missing,
    )
    return {
        "requested": len(missing),
        "resolved": len(missing) - still_missing,
        "still_missing": still_missing,
    }
