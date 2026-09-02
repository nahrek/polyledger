"""Polymarket CLOB REST client — market metadata only."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from ..config import CLOB_BASE_URL, CLOB_FIRST_CURSOR, CLOB_LAST_CURSOR
from ..http import HttpSession

log = logging.getLogger(__name__)


class ClobClient:
    def __init__(self, session: HttpSession, base_url: str = CLOB_BASE_URL) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")

    async def iter_markets(self) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield pages of markets, following `next_cursor` to the end.

        The CLOB signals the last page with a `next_cursor` of "LTE=". We also
        stop if the cursor stops advancing, which is how the API has failed in
        the past — without that guard the loop spins forever on page one.
        """
        cursor = CLOB_FIRST_CURSOR
        seen: set[str] = set()
        page = 0
        while True:
            payload = await self.session.get_json(
                f"{self.base_url}/markets", params={"next_cursor": cursor}
            )
            data = payload.get("data") or []
            if data:
                page += 1
                log.debug("clob /markets page %s: %s rows", page, len(data))
                yield data

            nxt = payload.get("next_cursor")
            if not nxt or nxt == CLOB_LAST_CURSOR or not data:
                return
            if nxt in seen:
                log.warning("CLOB cursor stopped advancing at %r; stopping", nxt)
                return
            seen.add(nxt)
            cursor = nxt
