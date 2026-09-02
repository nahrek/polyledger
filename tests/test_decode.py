"""Decoder tests, built from a real V2 log plus synthetic edge cases."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from polyledger.config import (
    CTF_EXCHANGE_V2,
    ORDER_FILLED_V1_SIG,
    ORDER_FILLED_V1_TOPIC0,
    ORDER_FILLED_V2_SIG,
    ORDER_FILLED_V2_TOPIC0,
)
from polyledger.decode import DecodeError, decode_batch, decode_order_filled


@dataclass
class FakeLog:
    topics: list[str]
    data: str
    address: str = CTF_EXCHANGE_V2.lower()
    block_number: int = 75_000_000
    transaction_hash: str = "0x" + "ab" * 32
    log_index: int = 7
    removed: bool = False


def word(value: int) -> str:
    return f"{value:064x}"


def addr_topic(address: str) -> str:
    return "0x" + "0" * 24 + address.lower().removeprefix("0x")


MAKER = "0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a"
TAKER = "0x84cfffc3f16dcc353094de30d4a45226eccd2f63"
ORDER_HASH = "0xbb2fb433eedd361985f76aaf16448e1d7933bad4062e5152b264f36a26d7e15f"


def v2_log(side: int, token_id: int, maker_amt: int, taker_amt: int,
           fee: int = 0, **kwargs) -> FakeLog:
    data = "0x" + "".join(
        word(v) for v in (side, token_id, maker_amt, taker_amt, fee, 0, 0)
    )
    return FakeLog(
        topics=[ORDER_FILLED_V2_TOPIC0, ORDER_HASH,
                addr_topic(MAKER), addr_topic(TAKER)],
        data=data,
        **kwargs,
    )


def v1_log(maker_asset: int, taker_asset: int, maker_amt: int,
           taker_amt: int, fee: int = 0) -> FakeLog:
    data = "0x" + "".join(
        word(v) for v in (maker_asset, taker_asset, maker_amt, taker_amt, fee)
    )
    return FakeLog(
        topics=[ORDER_FILLED_V1_TOPIC0, ORDER_HASH,
                addr_topic(MAKER), addr_topic(TAKER)],
        data=data,
    )


def test_topic0_matches_published_signatures():
    """Guards against a typo in the hard-coded topic hashes."""
    hypersync = pytest.importorskip("hypersync")
    assert hypersync.signature_to_topic0(ORDER_FILLED_V2_SIG) == ORDER_FILLED_V2_TOPIC0
    assert hypersync.signature_to_topic0(ORDER_FILLED_V1_SIG) == ORDER_FILLED_V1_TOPIC0


def test_decode_v2_maker_buy():
    # Maker pays 55 USD for 100 shares -> price 0.55.
    fill = decode_order_filled(v2_log(0, 12345, 55_000_000, 100_000_000, fee=1_000))
    assert fill.exchange_version == "v2"
    assert fill.maker_side == 0
    assert fill.token_id == "12345"
    assert fill.maker_amount_filled == 55_000_000
    assert fill.taker_amount_filled == 100_000_000
    assert fill.fee == 1_000
    assert fill.maker == MAKER
    assert fill.taker == TAKER
    assert fill.order_hash == ORDER_HASH
    assert fill.transaction_hash == "0x" + "ab" * 32
    assert fill.log_index == 7


def test_decode_v2_maker_sell():
    fill = decode_order_filled(v2_log(1, 999, 100_000_000, 42_000_000))
    assert fill.maker_side == 1
    assert fill.token_id == "999"


def test_decode_v1_infers_side_from_collateral_leg():
    buy = decode_order_filled(v1_log(0, 777, 55_000_000, 100_000_000))
    assert (buy.maker_side, buy.token_id, buy.exchange_version) == (0, "777", "v1")

    sell = decode_order_filled(v1_log(777, 0, 100_000_000, 55_000_000))
    assert (sell.maker_side, sell.token_id) == (1, "777")


def test_decode_v1_token_for_token_is_rejected():
    with pytest.raises(DecodeError):
        decode_order_filled(v1_log(111, 222, 1, 1))


def test_unknown_topic_is_rejected():
    log = v2_log(0, 1, 1, 1)
    log.topics = ["0x" + "11" * 32, ORDER_HASH, addr_topic(MAKER), addr_topic(TAKER)]
    with pytest.raises(DecodeError):
        decode_order_filled(log)


def test_truncated_payload_is_rejected():
    log = v2_log(0, 1, 1, 1)
    log.data = "0x" + word(0) + word(1)
    with pytest.raises(DecodeError):
        decode_order_filled(log)


def test_batch_skips_removed_and_bad_logs_but_keeps_the_rest():
    logs = [
        v2_log(0, 1, 10, 20),
        v2_log(0, 2, 10, 20, log_index=8, removed=True),
        v1_log(111, 222, 1, 1),
        v2_log(1, 3, 10, 20, log_index=9),
    ]
    fills, skipped = decode_batch(logs, timestamps={75_000_000: 1_780_000_000})
    assert [f.token_id for f in fills] == ["1", "3"]
    assert skipped == 2
    assert all(f.block_timestamp == 1_780_000_000 for f in fills)


def test_batch_strict_mode_raises():
    with pytest.raises(DecodeError):
        decode_batch([v1_log(111, 222, 1, 1)], strict=True)


def test_missing_timestamp_leaves_field_none():
    fills, _ = decode_batch([v2_log(0, 1, 10, 20)], timestamps={})
    assert fills[0].block_timestamp is None
