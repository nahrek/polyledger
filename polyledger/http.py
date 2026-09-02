"""One HTTP layer shared by every REST source.

Two things live here that the original poly_data pipeline lacked: a global
token-bucket limiter (so parallel Gamma backfill can't stampede the API) and
retries with exponential backoff plus jitter (so a 12-hour backfill survives a
transient 502 instead of dying at hour nine).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Self

import httpx

log = logging.getLogger(__name__)

RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


class RateLimiter:
    """Token bucket. `rate` requests per second, burst capped at `rate`."""

    def __init__(self, rate: float) -> None:
        self.rate = max(rate, 0.1)
        self._tokens = self.rate
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self.rate, self._tokens + (now - self._updated) * self.rate
                )
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self.rate)


class HttpSession:
    """An httpx client wrapped in a limiter, a semaphore and retry logic."""

    def __init__(
        self,
        *,
        rate: float = 8.0,
        concurrency: int = 8,
        max_retries: int = 6,
        timeout: float = 30.0,
        user_agent: str = "polyledger/0.1 (+https://github.com/)",
    ) -> None:
        self._limiter = RateLimiter(rate)
        self._sem = asyncio.Semaphore(concurrency)
        self._max_retries = max_retries
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _backoff(attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, 60.0)
        # 0.5, 1, 2, 4, ... capped at 30s, with full jitter on top.
        base = min(0.5 * (2**attempt), 30.0)
        return base * (0.5 + random.random() / 2)

    async def get_json(self, url: str, params: Any = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                await self._limiter.acquire()
                async with self._sem:
                    resp = await self._client.get(url, params=params)
            except (httpx.TransportError, httpx.HTTPError) as exc:
                last_error = exc
                if attempt == self._max_retries:
                    break
                delay = self._backoff(attempt, None)
                log.warning("GET %s failed (%s); retrying in %.1fs", url, exc, delay)
                await asyncio.sleep(delay)
                continue

            if resp.status_code in RETRY_STATUS:
                if attempt == self._max_retries:
                    resp.raise_for_status()
                retry_after = None
                header = resp.headers.get("Retry-After")
                if header:
                    try:
                        retry_after = float(header)
                    except ValueError:
                        retry_after = None
                delay = self._backoff(attempt, retry_after)
                log.warning(
                    "GET %s -> HTTP %s; retrying in %.1fs",
                    url, resp.status_code, delay,
                )
                await asyncio.sleep(delay)
                continue

            resp.raise_for_status()
            return resp.json()

        assert last_error is not None
        raise last_error
