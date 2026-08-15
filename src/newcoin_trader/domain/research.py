"""Research output records. Candidate windows are not trading advice."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator

from newcoin_trader.domain.types import require_utc


class WindowStats(BaseModel):
    model_config = ConfigDict(frozen=True)

    window: str
    window_delta: timedelta
    simple_return: Decimal | None
    volatility: Decimal | None
    max_drawdown: Decimal | None
    mean_liquidity: Decimal | None
    mean_volume: Decimal | None
    n_observations: int


class CandidateWindow(BaseModel):
    """Deterministic research label inferred from historical returns.

    This is research output, not trading advice.
    """

    model_config = ConfigDict(frozen=True)

    kind: str
    start: datetime
    end: datetime
    metric: str
    value: Decimal
    label: str = "research_output_not_trading_advice"

    @field_validator("start", "end")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class ListingAnalysis(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    listing_time: datetime
    windows: tuple[WindowStats, ...]
    candidates: tuple[CandidateWindow, ...]
    disclaimer: str = "research_output_not_trading_advice"

    @field_validator("listing_time")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)
