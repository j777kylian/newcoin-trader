"""Phase 8A.1 early-market-event domain contracts (types/data only)."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Any

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


class EventAvailabilityStatus(StrEnum):
    SOURCE_TIME_ONLY = "source_time_only"
    RECEIPT_VERIFIED = "receipt_verified"
    DECISION_AVAILABLE_VERIFIED = "decision_available_verified"


class EventAvailability(BaseModel):
    """Explicit availability topology for early-market event clocks."""

    model_config = ConfigDict(frozen=True)

    status: EventAvailabilityStatus
    source_event_time: datetime
    received_time: datetime | None = None
    decision_available_time: datetime | None = None
    availability_policy_version: str
    availability_provenance_ref: str

    @field_validator("source_event_time", "received_time", "decision_available_time")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)

    @model_validator(mode="after")
    def _status_field_topology(self) -> EventAvailability:
        if self.status is EventAvailabilityStatus.SOURCE_TIME_ONLY:
            if self.received_time is not None or self.decision_available_time is not None:
                raise ValueError("SOURCE_TIME_ONLY requires received_time and decision_available_time to be absent")
            return self

        if self.status is EventAvailabilityStatus.RECEIPT_VERIFIED:
            if self.received_time is None:
                raise ValueError("RECEIPT_VERIFIED requires received_time")
            if self.decision_available_time is not None:
                raise ValueError("RECEIPT_VERIFIED requires decision_available_time to be absent")
            if self.received_time < self.source_event_time:
                raise ValueError("received_time must not precede source_event_time")
            return self

        if self.status is EventAvailabilityStatus.DECISION_AVAILABLE_VERIFIED:
            if self.received_time is None:
                raise ValueError("DECISION_AVAILABLE_VERIFIED requires received_time")
            if self.decision_available_time is None:
                raise ValueError("DECISION_AVAILABLE_VERIFIED requires decision_available_time")
            if self.received_time < self.source_event_time:
                raise ValueError("received_time must not precede source_event_time")
            if self.decision_available_time < self.received_time:
                raise ValueError("decision_available_time must not precede received_time")
            return self

        raise ValueError(f"unsupported EventAvailabilityStatus: {self.status!r}")


class HistoricalEarlyMarketEventFact(BaseModel):
    """Immutable historical fact constrained to SOURCE_TIME_ONLY availability."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    event_kind: EarlyMarketEventKind
    event_definition_version: str
    source: str
    venue_or_protocol: str
    chain: str
    asset_identity: AssetIdentity
    market_identity: MarketIdentity | None = None
    source_event_time: datetime
    availability: EventAvailability
    first_market_data_time: datetime | None = None
    first_liquidity_time: datetime | None = None
    first_trade_time: datetime | None = None
    event_time_semantics: EventTimeSemantics
    event_quality_status: EventQualityStatus
    event_clock_quality: EventClockQuality
    provenance_ref: str

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> HistoricalEarlyMarketEventFact:
        """Copy through validation so updates cannot bypass historical availability rules."""
        if update is None:
            return super().model_copy(deep=deep)
        return type(self).model_validate({**self.model_dump(), **update})

    @field_validator(
        "source_event_time",
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
    def _historical_source_time_only(self) -> HistoricalEarlyMarketEventFact:
        if self.availability.status is not EventAvailabilityStatus.SOURCE_TIME_ONLY:
            raise ValueError("HistoricalEarlyMarketEventFact requires SOURCE_TIME_ONLY availability")
        if self.source_event_time != self.availability.source_event_time:
            raise ValueError("source_event_time must equal availability.source_event_time")
        return self
