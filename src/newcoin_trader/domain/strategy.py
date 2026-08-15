"""Strategy interface records. Strategies are pure and deterministic."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator

from newcoin_trader.domain.enums import SignalKind
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.domain.types import require_utc


class Signal(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: SignalKind
    token_address: str
    timestamp: datetime
    price: Decimal
    qty: Decimal
    reason: str

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class StrategyContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    listing_time: datetime
    evaluation_time: datetime
    snapshots: tuple[PriceSnapshot, ...]
    parameters: dict[str, str | float | int | bool]

    @field_validator("listing_time", "evaluation_time")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class StrategyRunResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    strategy_name: str
    strategy_version: str
    parameters: dict[str, str | float | int | bool]
    signals: tuple[Signal, ...]
    metrics: dict[str, str]
    window_start: datetime | None = None
    window_end: datetime | None = None
