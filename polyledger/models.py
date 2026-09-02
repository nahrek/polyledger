"""Validated row schemas.

Everything that crosses the boundary from an external API or the chain into
storage goes through one of these models first. When Polymarket changes a
response shape (as it did in April 2026), the pipeline fails loudly on a
`ValidationError` instead of silently writing nulls for the next six hours.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _as_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


class Token(BaseModel):
    """One outcome of a market. `token_id` is the join key against chain fills."""

    model_config = ConfigDict(extra="ignore")

    token_id: str
    condition_id: str
    outcome: str | None = None
    outcome_index: int | None = None
    price: float | None = None
    winner: bool | None = None

    @field_validator("token_id", "condition_id", mode="before")
    @classmethod
    def _stringify(cls, v: Any) -> str:
        return str(v)

    @field_validator("winner", mode="before")
    @classmethod
    def _coerce_winner(cls, v: Any) -> bool | None:
        return _as_bool(v)


class Market(BaseModel):
    """A CLOB market, flattened. Outcome tokens live in the `tokens` table."""

    model_config = ConfigDict(extra="ignore")

    condition_id: str
    question_id: str | None = None
    question: str | None = None
    description: str | None = None
    market_slug: str | None = None
    end_date_iso: str | None = None
    game_start_time: str | None = None
    active: bool | None = None
    closed: bool | None = None
    archived: bool | None = None
    accepting_orders: bool | None = None
    neg_risk: bool | None = None
    enable_order_book: bool | None = None
    minimum_tick_size: float | None = None
    minimum_order_size: float | None = None
    maker_base_fee: float | None = None
    taker_base_fee: float | None = None
    icon: str | None = None
    tags: list[str] = Field(default_factory=list)
    source: str = "clob"

    @field_validator("active", "closed", "archived", "accepting_orders",
                     "neg_risk", "enable_order_book", mode="before")
    @classmethod
    def _coerce_bools(cls, v: Any) -> bool | None:
        return _as_bool(v)

    @field_validator("minimum_tick_size", "minimum_order_size",
                     "maker_base_fee", "taker_base_fee", mode="before")
    @classmethod
    def _coerce_floats(cls, v: Any) -> float | None:
        if v is None or v == "":
            return None
        return float(v)

    @field_validator("tags", mode="before")
    @classmethod
    def _coerce_tags(cls, v: Any) -> list[str]:
        if not v:
            return []
        if isinstance(v, str):
            return [v]
        return [str(x) for x in v]


class OrderFill(BaseModel):
    """A decoded `OrderFilled` log — raw chain truth, no market context yet.

    `(transaction_hash, log_index)` is the natural primary key and is what
    makes re-running a batch idempotent.
    """

    model_config = ConfigDict(extra="forbid")

    transaction_hash: str
    log_index: int
    block_number: int
    block_timestamp: int | None = None
    exchange: str
    exchange_version: str
    order_hash: str
    maker: str
    taker: str
    maker_side: int          # 0 = maker BUY, 1 = maker SELL
    token_id: str
    maker_amount_filled: int  # raw base units (6 decimals)
    taker_amount_filled: int  # raw base units (6 decimals)
    fee: int


def market_from_clob(payload: dict[str, Any]) -> tuple[Market, list[Token]]:
    """Split one CLOB `/markets` entry into a market row plus its token rows."""
    market = Market.model_validate(payload)
    tokens: list[Token] = []
    for idx, raw in enumerate(payload.get("tokens") or []):
        if not raw or not raw.get("token_id"):
            # The CLOB emits placeholder tokens with an empty id for markets
            # whose book was never created. Nothing can ever join to them.
            continue
        tokens.append(
            Token.model_validate(
                {**raw, "condition_id": market.condition_id, "outcome_index": idx}
            )
        )
    return market, tokens


def market_from_gamma(payload: dict[str, Any]) -> tuple[Market, list[Token]]:
    """Same, for Gamma's differently-shaped market object."""
    import json

    condition_id = payload.get("conditionId") or payload.get("condition_id")
    market = Market.model_validate(
        {
            "condition_id": condition_id,
            "question_id": payload.get("questionID") or payload.get("question_id"),
            "question": payload.get("question"),
            "description": payload.get("description"),
            "market_slug": payload.get("slug") or payload.get("market_slug"),
            "end_date_iso": payload.get("endDate") or payload.get("end_date_iso"),
            "game_start_time": payload.get("gameStartTime"),
            "active": payload.get("active"),
            "closed": payload.get("closed"),
            "archived": payload.get("archived"),
            "accepting_orders": payload.get("acceptingOrders"),
            "neg_risk": payload.get("negRisk"),
            "enable_order_book": payload.get("enableOrderBook"),
            "minimum_tick_size": payload.get("orderPriceMinTickSize"),
            "minimum_order_size": payload.get("orderMinSize"),
            "icon": payload.get("icon"),
            "source": "gamma",
        }
    )

    raw_ids = payload.get("clobTokenIds") or payload.get("clob_token_ids") or []
    if isinstance(raw_ids, str):
        try:
            raw_ids = json.loads(raw_ids)
        except json.JSONDecodeError:
            raw_ids = []
    raw_outcomes = payload.get("outcomes") or []
    if isinstance(raw_outcomes, str):
        try:
            raw_outcomes = json.loads(raw_outcomes)
        except json.JSONDecodeError:
            raw_outcomes = []

    tokens = [
        Token(
            token_id=str(tid),
            condition_id=market.condition_id,
            outcome=str(raw_outcomes[i]) if i < len(raw_outcomes) else None,
            outcome_index=i,
        )
        for i, tid in enumerate(raw_ids)
        if tid
    ]
    return market, tokens
