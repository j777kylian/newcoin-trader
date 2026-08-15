"""Token and listing discovery records."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator

from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.types import require_utc


class TokenRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: Chain
    symbol: str
    venue: Venue | None = None


class NewListingEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: Chain
    symbol: str
    name: str | None = None
    created_time: datetime | None = None
    first_seen_time: datetime
    source: str
    venue: Venue | None = None
    liquidity: Decimal | None = None
    pair_address: str | None = None
    provenance: dict[str, str] | None = None

    @field_validator("created_time", "first_seen_time")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)
