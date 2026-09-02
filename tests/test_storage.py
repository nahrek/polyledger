"""Storage tests: idempotency, checkpointing, and the price/side SQL."""

from __future__ import annotations

import duckdb
import pytest

from polyledger.config import CTF_EXCHANGE_V2, Settings
from polyledger.models import Market, OrderFill, Token, market_from_clob
from polyledger.pipeline.trades import build_trades
from polyledger.storage import Store


@pytest.fixture()
def store(tmp_path):
    settings = Settings()
    settings.db_path = tmp_path / "test.duckdb"
    with Store(settings) as s:
        yield s


def make_fill(log_index: int, *, maker_side: int = 0, token_id: str = "1001",
              maker_amount: int = 55_000_000, taker_amount: int = 100_000_000,
              tx: str | None = None) -> OrderFill:
    return OrderFill(
        transaction_hash=tx or ("0x" + "cd" * 32),
        log_index=log_index,
        block_number=75_000_000 + log_index,
        block_timestamp=1_780_000_000 + log_index,
        exchange=CTF_EXCHANGE_V2.lower(),
        exchange_version="v2",
        order_hash="0x" + "ef" * 32,
        maker="0x" + "11" * 20,
        taker="0x" + "22" * 20,
        maker_side=maker_side,
        token_id=token_id,
        maker_amount_filled=maker_amount,
        taker_amount_filled=taker_amount,
        fee=1_000,
    )


def seed_market(store: Store) -> None:
    market, tokens = market_from_clob(
        {
            "condition_id": "0xcond",
            "question": "Will it rain?",
            "market_slug": "will-it-rain",
            "active": "true",
            "closed": False,
            "neg_risk": False,
            "minimum_tick_size": "0.01",
            "tags": ["weather"],
            "tokens": [
                {"token_id": "1001", "outcome": "Yes", "price": 0.55, "winner": False},
                {"token_id": "1002", "outcome": "No", "price": 0.45, "winner": False},
                {"token_id": "", "outcome": "Broken"},
            ],
        }
    )
    store.upsert_markets([market])
    store.upsert_tokens(tokens)


def test_empty_token_ids_are_dropped(store):
    seed_market(store)
    assert store.con.execute("SELECT count(*) FROM tokens").fetchone()[0] == 2


def test_insert_is_idempotent(store):
    fills = [make_fill(1), make_fill(2)]
    assert store.insert_fills(fills) == 2
    assert store.insert_fills(fills) == 0
    assert store.con.execute("SELECT count(*) FROM order_fills").fetchone()[0] == 2


def test_duplicates_within_one_batch_are_collapsed(store):
    assert store.insert_fills([make_fill(1), make_fill(1), make_fill(2)]) == 2


def test_same_log_index_in_different_tx_is_kept(store):
    a = make_fill(1, tx="0x" + "aa" * 32)
    b = make_fill(1, tx="0x" + "bb" * 32)
    assert store.insert_fills([a, b]) == 2


def test_market_upsert_replaces_on_conflict(store):
    store.upsert_markets([Market(condition_id="0xcond", question="old")])
    store.upsert_markets([Market(condition_id="0xcond", question="new")])
    rows = store.con.execute("SELECT question FROM markets").fetchall()
    assert rows == [("new",)]


def test_commit_batch_advances_checkpoint(store):
    stream = "order_filled:v2"
    assert store.get_checkpoint(stream) is None
    store.commit_batch(stream, [make_fill(1)], next_block=75_000_100)
    assert store.get_checkpoint(stream) == 75_000_100
    store.commit_batch(stream, [make_fill(2)], next_block=75_000_200)
    assert store.get_checkpoint(stream) == 75_000_200
    rows = store.con.execute(
        "SELECT rows_seen FROM checkpoints WHERE stream = ?", [stream]
    ).fetchone()[0]
    assert rows == 2


def test_commit_batch_rolls_back_on_failure(store):
    stream = "order_filled:v2"
    store.commit_batch(stream, [make_fill(1)], next_block=100)
    bad = make_fill(2)
    bad.token_id = None  # type: ignore[assignment]  - forced write failure
    with pytest.raises((TypeError, AttributeError, duckdb.Error)):
        store.commit_batch(stream, [bad, "not-a-fill"], next_block=200)  # type: ignore[list-item]
    assert store.get_checkpoint(stream) == 100
    assert store.con.execute("SELECT count(*) FROM order_fills").fetchone()[0] == 1


def test_trade_price_and_sides_for_maker_buy(store):
    seed_market(store)
    store.insert_fills([make_fill(1, maker_side=0)])
    row = store.con.execute(
        "SELECT maker_side, taker_side, shares, usd_size, price, outcome, question "
        "FROM trades"
    ).fetchone()
    assert row[0] == "BUY" and row[1] == "SELL"
    assert row[2] == pytest.approx(100.0)
    assert row[3] == pytest.approx(55.0)
    assert row[4] == pytest.approx(0.55)
    assert row[5] == "Yes"
    assert row[6] == "Will it rain?"


def test_trade_price_for_maker_sell(store):
    seed_market(store)
    store.insert_fills(
        [make_fill(1, maker_side=1, maker_amount=100_000_000, taker_amount=42_000_000)]
    )
    row = store.con.execute(
        "SELECT maker_side, shares, usd_size, price FROM trades"
    ).fetchone()
    assert row[0] == "SELL"
    assert row[1] == pytest.approx(100.0)
    assert row[2] == pytest.approx(42.0)
    assert row[3] == pytest.approx(0.42)


def test_zero_share_fill_yields_null_price_not_a_crash(store):
    seed_market(store)
    store.insert_fills([make_fill(1, maker_amount=10, taker_amount=0)])
    assert store.con.execute("SELECT price FROM trades").fetchone()[0] is None


def test_unmatched_token_is_flagged_and_reported(store):
    seed_market(store)
    store.insert_fills([make_fill(1, token_id="9999")])
    assert store.unmatched_token_ids() == ["9999"]
    assert store.con.execute(
        "SELECT unmatched_market FROM trades"
    ).fetchone()[0] is True
    assert store.stats()["unmatched_fills"] == 1


def test_build_trades_can_drop_unmatched(store):
    seed_market(store)
    store.insert_fills([make_fill(1), make_fill(2, token_id="9999")])
    assert build_trades(store)["rows"] == 2
    assert build_trades(store, drop_unmatched=True)["rows"] == 1


def test_export_parquet_writes_every_table(store, tmp_path):
    seed_market(store)
    store.insert_fills([make_fill(1)])
    paths = store.export_parquet(tmp_path / "out")
    assert {p.stem for p in paths} == {"markets", "tokens", "order_fills", "trades"}
    assert all(p.exists() and p.stat().st_size > 0 for p in paths)


def test_reopening_the_database_preserves_state(store, tmp_path):
    seed_market(store)
    store.commit_batch("order_filled:v2", [make_fill(1)], next_block=123)
    path = store.path
    store.close()

    settings = Settings()
    settings.db_path = path
    with Store(settings) as reopened:
        assert reopened.get_checkpoint("order_filled:v2") == 123
        assert reopened.con.execute(
            "SELECT count(*) FROM order_fills"
        ).fetchone()[0] == 1


def test_large_amounts_survive_the_roundtrip(store):
    """uint256 amounts can exceed BIGINT; the column must be HUGEINT."""
    big = 2**70
    store.insert_fills([make_fill(1, maker_amount=big, taker_amount=big)])
    assert store.con.execute(
        "SELECT maker_amount_filled FROM order_fills"
    ).fetchone()[0] == big


def test_token_model_coerces_string_booleans():
    token = Token.model_validate(
        {"token_id": 1001, "condition_id": "0xcond", "winner": "true"}
    )
    assert token.token_id == "1001"
    assert token.winner is True
