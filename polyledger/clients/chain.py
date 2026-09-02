"""HyperSync wrapper for streaming `OrderFilled` logs off Polygon.

HyperSync returns logs *and* the block timestamps they belong to in a single
response, which is the whole reason this is viable — the equivalent over plain
JSON-RPC would be one `eth_getBlockByNumber` per block.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass

import hypersync
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    LogField,
    LogSelection,
    Query,
)

from ..config import Settings

log = logging.getLogger(__name__)

LOG_FIELDS = [
    LogField.BLOCK_NUMBER,
    LogField.TRANSACTION_HASH,
    LogField.LOG_INDEX,
    LogField.ADDRESS,
    LogField.DATA,
    LogField.TOPIC0,
    LogField.TOPIC1,
    LogField.TOPIC2,
    LogField.TOPIC3,
    LogField.REMOVED,
]
BLOCK_FIELDS = [BlockField.NUMBER, BlockField.TIMESTAMP]


@dataclass
class LogPage:
    logs: list
    timestamps: dict[int, int]
    next_block: int
    archive_height: int | None


def _to_int(value) -> int | None:
    """Block timestamps come back as hex strings on some chains, ints on others."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value)
    return int(text, 16) if text.startswith("0x") else int(text)


class ChainClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = hypersync.HypersyncClient(
            ClientConfig(
                url=settings.hypersync_url,
                api_token=settings.hypersync_token,
                max_num_retries=8,
                retry_base_ms=500,
                proactive_rate_limit_sleep=True,
            )
        )
        self.addresses = [a.lower() for a in settings.exchange_addresses()]
        self.topic0 = settings.topics()

    def _query(self, from_block: int, to_block: int | None = None,
               max_num_logs: int | None = None) -> Query:
        return Query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(address=self.addresses, topics=[self.topic0])],
            field_selection=FieldSelection(log=LOG_FIELDS, block=BLOCK_FIELDS),
            max_num_logs=max_num_logs,
        )

    async def height(self) -> int:
        return await self.client.get_height()

    async def find_first_fill_block(self, start: int = 0) -> int:
        """Locate the first block containing a matching log.

        HyperSync skips empty ranges server-side, so asking for a single log
        from block 0 costs a handful of round trips rather than a scan.
        """
        cursor = start
        while True:
            res = await self.client.get(self._query(cursor, max_num_logs=1))
            if res.data.logs:
                found = min(int(l.block_number) for l in res.data.logs)
                log.info("first OrderFilled log found at block %s", found)
                return found
            if res.next_block <= cursor:
                raise RuntimeError("no OrderFilled logs found on this chain")
            cursor = res.next_block

    async def iter_logs(
        self, from_block: int, to_block: int | None = None
    ) -> AsyncIterator[LogPage]:
        """Page through logs from `from_block` until `to_block` (exclusive)."""
        cursor = from_block
        while to_block is None or cursor < to_block:
            res = await self.client.get(self._query(cursor, to_block))
            timestamps = {
                int(b.number): ts
                for b in (res.data.blocks or [])
                if (ts := _to_int(b.timestamp)) is not None and b.number is not None
            }
            yield LogPage(
                logs=list(res.data.logs or []),
                timestamps=timestamps,
                next_block=res.next_block,
                archive_height=res.archive_height,
            )
            if res.next_block <= cursor:
                # Caught up with the archive; nothing further to read right now.
                return
            cursor = res.next_block

    def rate_limit(self) -> str:
        info = self.client.rate_limit_info()
        if not info:
            return "n/a"
        return f"{info.remaining}/{info.limit} (reset in {info.reset_secs}s)"
