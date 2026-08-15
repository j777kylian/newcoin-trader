"""Risk limit configuration."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class RiskLimits(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_notional: Decimal = Field(default=Decimal("1000"))
    max_position_size: Decimal = Field(default=Decimal("500"))
    max_open_positions: int = 3
    max_drawdown: Decimal = Field(default=Decimal("0.25"))
    min_liquidity: Decimal = Field(default=Decimal("5000"))
