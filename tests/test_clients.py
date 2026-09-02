"""Client tests. No network: every session is a stub.""" 

from __future__ import annotations

import asyncio
import time

import pytest

from polyledger.clients.clob import ClobClient
from polyledger.clients.gamma import GammaClient, _chunks, _extract
from polyledger.config import CLOB_LAST_CURSOR
from polyledger.http import RateLimiter
from polyledger.models import market_from_gamma


class FakeSession:
    """Returns queued responses and records the params it was called with."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def get_json(self, url, params=None):
        self.calls.append((url, params))
        if not self.responses:
            raise AssertionError("more requests than queued responses")
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def run(coro):
    return asyncio.run(coro)


async def collect_markets(client):
    pages = []
    async for page in client.iter_markets():
        pages.append(page)
    return pages


def test_clob_follows_cursor_to_the_last_page():
    session = FakeSession([
        {"data": [{"condition_id": "a"}], "next_cursor": "Mg=="},
        {"data": [{"condition_id": "b"}], "next_cursor": CLOB_LAST_CURSOR},
    ])
    pages = run(collect_markets(ClobClient(session)))
    assert [m["condition_id"] for page in pages for m in page] == ["a", "b"]
    assert len(session.calls) == 2


def test_clob_stops_when_the_cursor_stops_advancing():
    """The keyset endpoints have shipped a looping cursor before."""
    stuck = {"data": [{"condition_id": "a"}], "next_cursor": "SAME"}
    session = FakeSession([stuck, stuck, stuck])
    pages = run(collect_markets(ClobClient(session)))
    assert len(pages) == 2  # first page, then the repeat that trips the guard
    assert len(session.calls) == 2


def test_clob_stops_on_empty_page():
    session = FakeSession([{"data": [], "next_cursor": "Mg=="}])
    assert run(collect_markets(ClobClient(session))) == []


def test_gamma_falls_back_to_legacy_endpoint():
    session = FakeSession([
        RuntimeError("keyset down"),
        [{"conditionId": "0xc", "clobTokenIds": '["1","2"]'}],
    ])
    result = run(GammaClient(session).markets_by_token_ids(["1"]))
    assert len(result) == 1
    assert session.calls[0][0].endswith("/markets/keyset")
    assert session.calls[1][0].endswith("/markets")


def test_gamma_batches_and_deduplicates_ids():
    ids = [str(i) for i in range(45)] + ["0"]
    session = FakeSession([[] for _ in range(3)])
    run(GammaClient(session).markets_by_token_ids(ids))
    assert len(session.calls) == 3  # 45 unique ids, batches of 20


def test_gamma_survives_a_failed_batch():
    session = FakeSession([[{"conditionId": "0xc"}], RuntimeError("boom"),
                           RuntimeError("boom")])
    result = run(GammaClient(session).markets_by_token_ids(
        [str(i) for i in range(40)]
    ))
    assert len(result) == 1


def test_extract_handles_both_response_shapes():
    assert _extract([{"a": 1}]) == [{"a": 1}]
    assert _extract({"markets": [{"a": 1}]}) == [{"a": 1}]
    assert _extract({"data": [{"a": 1}]}) == [{"a": 1}]
    assert _extract({"error": "nope"}) == []


def test_chunks():
    assert _chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert _chunks([], 2) == []


def test_gamma_market_parses_json_encoded_arrays():
    market, tokens = market_from_gamma({
        "conditionId": "0xc",
        "question": "Will it?",
        "slug": "will-it",
        "negRisk": "true",
        "clobTokenIds": '["1001", "1002"]',
        "outcomes": '["Yes", "No"]',
    })
    assert market.neg_risk is True
    assert market.source == "gamma"
    assert [(t.token_id, t.outcome) for t in tokens] == [("1001", "Yes"), ("1002", "No")]


def test_gamma_market_tolerates_broken_json():
    _, tokens = market_from_gamma({"conditionId": "0xc", "clobTokenIds": "not json"})
    assert tokens == []


def test_rate_limiter_enforces_the_configured_rate():
    async def hammer():
        limiter = RateLimiter(rate=20.0)
        start = time.monotonic()
        for _ in range(30):
            await limiter.acquire()
        return time.monotonic() - start

    # 20 tokens of burst, then 10 more at 20/s -> at least ~0.5s.
    assert run(hammer()) >= 0.4


def test_backoff_is_bounded_and_jittered():
    from polyledger.http import HttpSession

    delays = [HttpSession._backoff(i, None) for i in range(12)]
    assert all(0 < d <= 30.0 for d in delays)
    assert HttpSession._backoff(0, retry_after=5.0) == 5.0
    assert HttpSession._backoff(0, retry_after=999.0) == 60.0
    assert len({round(HttpSession._backoff(6, None), 6) for _ in range(20)}) > 1


@pytest.mark.parametrize("contracts,expected", [
    ("v2", 3), ("v1", 2), ("all", 5),
])
def test_settings_select_the_right_contract_set(contracts, expected):
    from polyledger.config import Settings

    settings = Settings()
    settings.contracts = contracts
    assert len(settings.exchange_addresses()) == expected
    assert settings.stream_key() == f"order_filled:{contracts}"
