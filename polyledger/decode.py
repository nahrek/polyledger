"""Hand-rolled decoding of `OrderFilled` logs.

The event has a fixed, all-static layout, so a full ABI decoder would be dead
weight — slicing 32-byte words out of the data blob is both faster and one
fewer dependency. Kept pure and free of network calls so it can be tested
against fixtures offline.
"""

from __future__ import annotations

from typing import Any, Protocol

from .config import (
    EXCHANGE_NAMES,
    ORDER_FILLED_V1_TOPIC0,
    ORDER_FILLED_V2_TOPIC0,
)
from .models import OrderFill


class LogLike(Protocol):
    """The shape we need from a HyperSync log (or a test fixture)."""

    topics: Any
    data: Any
    address: Any
    block_number: Any
    transaction_hash: Any
    log_index: Any


class DecodeError(ValueError):
    """Raised when a log does not match the expected OrderFilled layout."""


def _strip(hex_str: str) -> str:
    s = hex_str.lower()
    return s.removeprefix("0x")


def _word(body: str, index: int) -> str:
    start = index * 64
    end = start + 64
    if end > len(body):
        raise DecodeError(f"data too short: need word {index}, have {len(body) // 64}")
    return body[start:end]


def _uint(body: str, index: int) -> int:
    return int(_word(body, index), 16)


def _topic_address(topic: str) -> str:
    """An indexed address is right-aligned in its 32-byte topic."""
    return "0x" + _strip(topic)[-40:]


def decode_order_filled(log: LogLike) -> OrderFill:
    """Turn one raw log into a validated `OrderFill`.

    Raises `DecodeError` if the topic0 is unknown or the payload is malformed.
    """
    topics = [t for t in (log.topics or []) if t]
    if len(topics) < 4:
        raise DecodeError(f"expected 4 topics, got {len(topics)}")

    topic0 = "0x" + _strip(topics[0])
    body = _strip(log.data or "")
    address = (log.address or "").lower()

    order_hash = "0x" + _strip(topics[1])
    maker = _topic_address(topics[2])
    taker = _topic_address(topics[3])

    if topic0 == ORDER_FILLED_V2_TOPIC0:
        # side, tokenId, makerAmountFilled, takerAmountFilled, fee, builder, metadata
        if len(body) < 7 * 64:
            raise DecodeError(f"v2 payload too short: {len(body) // 64} words")
        version = "v2"
        maker_side = _uint(body, 0)
        token_id = str(_uint(body, 1))
        maker_amount = _uint(body, 2)
        taker_amount = _uint(body, 3)
        fee = _uint(body, 4)

    elif topic0 == ORDER_FILLED_V1_TOPIC0:
        # makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee
        if len(body) < 5 * 64:
            raise DecodeError(f"v1 payload too short: {len(body) // 64} words")
        version = "v1"
        maker_asset = _uint(body, 0)
        taker_asset = _uint(body, 1)
        maker_amount = _uint(body, 2)
        taker_amount = _uint(body, 3)
        fee = _uint(body, 4)
        # V1 has no explicit side. Asset id 0 is collateral, so whichever leg
        # is 0 tells you which way the maker went.
        if maker_asset == 0:
            maker_side, token_id = 0, str(taker_asset)
        elif taker_asset == 0:
            maker_side, token_id = 1, str(maker_asset)
        else:
            # Token-for-token fill (complementary outcomes). No collateral leg,
            # so there is no meaningful USD price to record.
            raise DecodeError("v1 fill with no collateral leg")
    else:
        raise DecodeError(f"unrecognised topic0 {topic0}")

    if maker_side not in (0, 1):
        raise DecodeError(f"invalid side {maker_side}")

    return OrderFill(
        transaction_hash="0x" + _strip(log.transaction_hash or ""),
        log_index=int(log.log_index),
        block_number=int(log.block_number),
        block_timestamp=None,
        exchange=address,
        exchange_version=version,
        order_hash=order_hash,
        maker=maker,
        taker=taker,
        maker_side=maker_side,
        token_id=token_id,
        maker_amount_filled=maker_amount,
        taker_amount_filled=taker_amount,
        fee=fee,
    )


def exchange_label(address: str) -> str:
    return EXCHANGE_NAMES.get((address or "").lower(), "unknown")


def decode_batch(
    logs: list[LogLike],
    timestamps: dict[int, int] | None = None,
    *,
    strict: bool = False,
) -> tuple[list[OrderFill], int]:
    """Decode a page of logs, returning `(fills, skipped_count)`.

    Reorg-removed logs and token-for-token V1 fills are skipped rather than
    fatal; set `strict=True` to make any failure raise instead.
    """
    fills: list[OrderFill] = []
    skipped = 0
    for entry in logs:
        if getattr(entry, "removed", False):
            skipped += 1
            continue
        try:
            fill = decode_order_filled(entry)
        except DecodeError:
            if strict:
                raise
            skipped += 1
            continue
        if timestamps:
            fill.block_timestamp = timestamps.get(fill.block_number)
        fills.append(fill)
    return fills, skipped
