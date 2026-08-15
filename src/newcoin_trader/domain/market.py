"""Normalized market-data records."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator

from newcoin_trader.domain.enums import Side, Venue
from newcoin_trader.domain.types import require_utc


class PriceSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    timestamp: datetime
    price: Decimal
    volume: Decimal | None = None
    liquidity: Decimal | None = None
    market_cap: Decimal | None = None
    buy_count: int | None = None
    sell_count: int | None = None
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class TradeTick(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    timestamp: datetime
    side: Side
    amount: Decimal
    price: Decimal
    external_trade_id: str | None = None
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class Kline(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None
    trade_count: int | None = None
    interval: str
    source: str
    venue: Venue | None = None

    @field_validator("open_time", "close_time")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class OrderBookLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal
    quantity: Decimal


class OrderBookL2(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    timestamp: datetime
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    last_update_id: int | None = None
    source: str

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class Ticker24h(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    timestamp: datetime
    last_price: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None
    price_change: Decimal | None = None
    trade_count: int | None = None
    source: str

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class PoolSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    pool_address: str
    chain: str
    base_mint: str
    quote_mint: str
    timestamp: datetime
    price: Decimal | None = None
    liquidity: Decimal | None = None
    volume_24h: Decimal | None = None
    name: str | None = None
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class PoolQuote(BaseModel):
    """Read-only swap quote. Never an executable transaction."""

    model_config = ConfigDict(frozen=True)

    input_mint: str
    output_mint: str
    input_amount: Decimal
    output_amount: Decimal
    other_amount_threshold: Decimal | None = None
    slippage_bps: int
    price_impact_pct: Decimal | None = None
    source: str
    quote_id: str | None = None
    provenance: dict[str, str] | None = None
