"""Stage 2 — stream `OrderFilled` logs from Polygon into `order_fills`.

Resumable by construction: rows and the block cursor are committed in one
transaction, so an interrupted run picks up exactly where it stopped and never
double-writes.
"""

from __future__ import annotations

import logging
import time

from ..clients.chain import ChainClient
from ..config import Settings
from ..decode import decode_batch
from ..models import OrderFill
from ..storage import Store

log = logging.getLogger(__name__)


async def resolve_start_block(
    store: Store, client: ChainClient, settings: Settings
) -> int:
    stream = settings.stream_key()
    checkpoint = store.get_checkpoint(stream)
    if checkpoint is not None:
        log.info("resuming from checkpoint at block %s", checkpoint)
        return checkpoint
    if settings.from_block > 0:
        log.info("starting from configured block %s", settings.from_block)
        return settings.from_block
    log.info("no checkpoint — probing chain for the first fill")
    return await client.find_first_fill_block()


async def sync_chain(
    store: Store,
    settings: Settings,
    *,
    to_block: int | None = None,
    max_blocks: int | None = None,
) -> dict[str, int]:
    client = ChainClient(settings)
    stream = settings.stream_key()

    start = await resolve_start_block(store, client, settings)
    height = await client.height()
    target = to_block if to_block is not None else height - settings.reorg_buffer
    if max_blocks is not None:
        target = min(target, start + max_blocks)

    if target <= start:
        log.info("already up to date (block %s, tip %s)", start, height)
        return {"inserted": 0, "decoded": 0, "skipped": 0, "last_block": start}

    log.info(
        "indexing blocks %s..%s (tip %s, %s blocks)",
        start, target, height, target - start,
    )

    buffer: list[OrderFill] = []
    totals = {"inserted": 0, "decoded": 0, "skipped": 0, "last_block": start}
    started = time.monotonic()

    async for page in client.iter_logs(start, target):
        fills, skipped = decode_batch(page.logs, page.timestamps)
        buffer.extend(fills)
        totals["decoded"] += len(fills)
        totals["skipped"] += skipped
        totals["last_block"] = page.next_block

        if len(buffer) >= settings.flush_every:
            totals["inserted"] += store.commit_batch(stream, buffer, page.next_block)
            buffer.clear()
            elapsed = max(time.monotonic() - started, 1e-6)
            log.info(
                "block %s/%s | %s fills | %.0f blocks/s | quota %s",
                page.next_block, target, totals["inserted"],
                (page.next_block - start) / elapsed, client.rate_limit(),
            )

    # Final flush: always advance the cursor, even if the tail had no fills.
    totals["inserted"] += store.commit_batch(stream, buffer, totals["last_block"])

    log.info(
        "chain sync done: %s new fills (%s decoded, %s skipped) up to block %s in %.1fs",
        totals["inserted"], totals["decoded"], totals["skipped"],
        totals["last_block"], time.monotonic() - started,
    )
    return totals
