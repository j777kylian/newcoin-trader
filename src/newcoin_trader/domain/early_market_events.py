"""Phase 8A.1 early-market-event domain contracts (types/data only)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from newcoin_trader.domain.types import require_utc


class EarlyMarketEventKind(StrEnum):
    BINANCE_ALPHA_AVAILABLE = "BINANCE_ALPHA_AVAILABLE"
    DEX_FIRST_LIQUIDITY = "DEX_FIRST_LIQUIDITY"
    DEX_FIRST_TRADE = "DEX_FIRST_TRADE"
    BINANCE_SPOT_LISTING = "BINANCE_SPOT_LISTING"


class EventTimeSemantics(StrEnum):
    ANNOUNCED = "announced"
    OBSERVED = "observed"
    EFFECTIVE = "effective"
    UNKNOWN = "unknown"


class EventQualityStatus(StrEnum):
    ACCEPTED = "accepted"
    DEGRADED = "degraded"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class EventClockQuality(StrEnum):
    EXACT = "exact"
    BOUNDED = "bounded"
    ESTIMATED = "estimated"
    UNKNOWN = "unknown"


class AssetIdentity(BaseModel):
    """Canonical asset identity; symbol is optional annotation only."""

    model_config = ConfigDict(frozen=True)

    chain: str
    asset_key: str
    symbol: str | None = None


class MarketIdentity(BaseModel):
    """Market / pool / pair identity; structurally distinct from AssetIdentity."""

    model_config = ConfigDict(frozen=True)

    chain: str
    venue_or_protocol: str
    market_key: str
    pool_or_pair_address: str | None = None
    base_asset_key: str | None = None
    quote_asset_key: str | None = None
    symbol: str | None = None


class EarlyMarketEvent(BaseModel):
    """Immutable early-market event with explicit multi-clock provenance."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    event_kind: EarlyMarketEventKind
    event_definition_version: str
    source: str
    venue_or_protocol: str
    chain: str
    asset_identity: AssetIdentity
    market_identity: MarketIdentity | None = None
    source_event_time: datetime
    received_time: datetime
    decision_available_time: datetime
    first_market_data_time: datetime | None = None
    first_liquidity_time: datetime | None = None
    first_trade_time: datetime | None = None
    event_time_semantics: EventTimeSemantics
    event_quality_status: EventQualityStatus
    event_clock_quality: EventClockQuality
    provenance_ref: str

    @field_validator(
        "source_event_time",
        "received_time",
        "decision_available_time",
        "first_market_data_time",
        "first_liquidity_time",
        "first_trade_time",
    )
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)

    @model_validator(mode="after")
    def _event_clock_order(self) -> EarlyMarketEvent:
        if self.received_time < self.source_event_time:
            raise ValueError("received_time must not precede source_event_time")
        if self.decision_available_time < self.received_time:
            raise ValueError("decision_available_time must not precede received_time")
        return self


class CapabilityProfile(BaseModel):
    """Capability skeleton: types and data only (no behavior)."""

    model_config = ConfigDict(frozen=True)

    profile_id: str
    venue_or_protocol: str
    chain: str
    supported_event_kinds: tuple[EarlyMarketEventKind, ...]
    event_definition_version: str
    notes: str | None = None
