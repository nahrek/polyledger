"""Gamma API client.

Used only to fill gaps: the CLOB `/markets` listing drops some resolved and
archived markets, but their token ids still appear in old chain fills. Gamma
can look those up by `clob_token_ids`, so anything the CLOB missed gets
recovered here rather than being left as an unjoinable trade row.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..config import GAMMA_BASE_URL
from ..http import HttpSession

log = logging.getLogger(__name__)

# Gamma caps `limit` at 100, and long query strings get rejected, so keep the
# id batches well under that.
BATCH_SIZE = 20


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _extract(payload: Any) -> list[dict[str, Any]]:
    """Gamma returns a bare list on some endpoints, `{"markets": [...]}` on others."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("markets", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


class GammaClient:
    def __init__(self, session: HttpSession, base_url: str = GAMMA_BASE_URL) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")

    async def _fetch_batch(self, token_ids: list[str]) -> list[dict[str, Any]]:
        params = [("clob_token_ids", tid) for tid in token_ids]
        params.append(("limit", str(len(token_ids) * 2)))
        try:
            payload = await self.session.get_json(
                f"{self.base_url}/markets/keyset", params=params
            )
            return _extract(payload)
        except Exception as exc:  # noqa: BLE001 - fall through to the legacy path
            # `/markets/keyset` is the documented replacement for the legacy
            # listing, but it has been flaky; an empty result is a real answer,
            # an error is not.
            log.debug("gamma keyset lookup failed (%s); trying /markets", exc)

        payload = await self.session.get_json(f"{self.base_url}/markets", params=params)
        return _extract(payload)

    async def markets_by_token_ids(self, token_ids: list[str]) -> list[dict[str, Any]]:
        """Look up markets for arbitrarily many token ids, batched in parallel.

        Concurrency is bounded by the shared `HttpSession` limiter, so this
        cannot outrun the global rate budget no matter how many batches exist.
        """
        batches = _chunks(sorted(set(token_ids)), BATCH_SIZE)
        if not batches:
            return []
        log.info("gamma backfill: %s ids in %s batches", len(token_ids), len(batches))
        results = await asyncio.gather(
            *(self._fetch_batch(b) for b in batches), return_exceptions=True
        )
        markets: list[dict[str, Any]] = []
        failures = 0
        for res in results:
            if isinstance(res, BaseException):
                failures += 1
                log.warning("gamma batch failed: %s", res)
                continue
            markets.extend(res)
        if failures:
            log.warning("%s/%s gamma batches failed", failures, len(batches))
        return markets
