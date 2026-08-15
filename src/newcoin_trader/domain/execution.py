"""Paper order / fill records. No live-order types exist."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator

from newcoin_trader.domain.enums import PaperStatus, RejectReason, Side
from newcoin_trader.domain.types import require_utc


class PaperOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    side: Side
    requested_qty: Decimal
    limit_price: Decimal
    signal_ts: datetime
    run_id: str | None = None

    @field_validator("signal_ts")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("requested_qty", "limit_price")
    @classmethod
    def _positive(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("must be positive")
        return value


class PaperFill(BaseModel):
    model_config = ConfigDict(frozen=True)

    order: PaperOrder
    status: PaperStatus
    fill_qty: Decimal
    fill_price: Decimal
    fee: Decimal
    slippage_bps: Decimal
    mode: str = "paper"
    reject_reason: RejectReason | None = None


class RejectedOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    order: PaperOrder
    reason: RejectReason
    status: PaperStatus = PaperStatus.REJECTED
    mode: str = "paper"
    detail: str | None = None


class PortfolioState(BaseModel):
    model_config = ConfigDict(frozen=True)

    open_positions: int
    gross_notional: Decimal
    position_size: Decimal
    drawdown: Decimal
    observed_liquidity: Decimal
