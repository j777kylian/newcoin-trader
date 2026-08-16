"""Phase 3 descriptive event-study domain records (gross market returns only)."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.types import require_utc

DISCLAIMER = "descriptive_gross_market_return_research_not_trading_advice"
WARNING_NO_PNL = "not_executable_pnl_not_strategy_optimization"


class ObservationResolution(StrEnum):
    """Temporal resolution of a market observation product."""

    POINT = "point"  # trade / tick / point quote: supports sub-minute delays
    MINUTE = "minute"  # one-minute OHLCV / minute snapshots
    COARSE = "coarse"  # coarser than one minute (e.g. 1h bars)


class CellOutcomeStatus(StrEnum):
    COMPLETE = "complete"
    RIGHT_CENSORED = "right_censored"
    MISSING_ENTRY = "missing_entry"
    MISSING_EXIT = "missing_exit"
    UNSUPPORTED_RESOLUTION = "unsupported_resolution"
    INVALID_MARKET_DATA = "invalid_market_data"
    NOT_DECISION_AVAILABLE = "not_decision_available"


class TokenListingEvent(BaseModel):
    """Frozen listing event with distinct time semantics (not universal launch).

    ``source_event_time`` is the stored venue/source event clock (typically
    ``created_time`` when present). It is intentionally distinct from
    ``first_seen_time``, ``first_market_data_time``, and
    ``decision_available_time``.
    """

    model_config = ConfigDict(frozen=True)

    event_id: str
    venue: Venue
    chain: Chain
    token_address: str
    pair_address: str | None = None
    symbol: str
    source: str
    source_event_time: datetime
    first_seen_time: datetime
    first_market_data_time: datetime | None = None
    decision_available_time: datetime
    provenance: dict[str, str] = Field(default_factory=dict)

    @field_validator(
        "source_event_time",
        "first_seen_time",
        "first_market_data_time",
        "decision_available_time",
    )
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class MarketObservation(BaseModel):
    """Point market observation for event-study pricing (research only)."""

    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    venue: Venue
    timestamp: datetime
    price: Decimal
    resolution: ObservationResolution
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class PathStats(BaseModel):
    """Path extrema from observed prices only (never invented intra-bar OHLC)."""

    model_config = ConfigDict(frozen=True)

    mfe: Decimal | None = None
    mae: Decimal | None = None
    peak_price: Decimal | None = None
    trough_price: Decimal | None = None
    time_to_peak: timedelta | None = None
    time_to_trough: timedelta | None = None
    path_observation_count: int = 0
    path_available: bool = False


class EventStudyCellResult(BaseModel):
    """One venue × entry-delay × holding outcome for a single listing event."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    venue: Venue
    token_address: str
    chain: Chain
    source_event_time: datetime
    first_seen_time: datetime
    first_market_data_time: datetime | None = None
    decision_available_time: datetime
    entry_delay: timedelta
    holding_period: timedelta
    entry_time: datetime
    exit_time: datetime
    status: CellOutcomeStatus
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    simple_return: Decimal | None = None
    log_return: Decimal | None = None
    path: PathStats = Field(default_factory=PathStats)
    event_source: str
    event_provenance: dict[str, str] = Field(default_factory=dict)
    entry_source: str | None = None
    entry_provenance: dict[str, str] | None = None
    exit_source: str | None = None
    exit_provenance: dict[str, str] | None = None
    label: str = DISCLAIMER
    warning: str = WARNING_NO_PNL

    @field_validator(
        "source_event_time",
        "first_seen_time",
        "first_market_data_time",
        "decision_available_time",
        "entry_time",
        "exit_time",
    )
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class CellAggregate(BaseModel):
    """Deterministic distribution stats for one venue × delay × holding cell."""

    model_config = ConfigDict(frozen=True)

    venue: Venue
    entry_delay: timedelta
    holding_period: timedelta
    samples: int
    complete_count: int
    valid_return_count: int
    censored_count: int
    status_counts: dict[str, int]
    mean_simple_return: Decimal | None = None
    median_simple_return: Decimal | None = None
    std_simple_return: Decimal | None = None
    win_rate: Decimal | None = None
    p10: Decimal | None = None
    p25: Decimal | None = None
    p75: Decimal | None = None
    p90: Decimal | None = None
    min_simple_return: Decimal | None = None
    max_simple_return: Decimal | None = None
    mean_mfe: Decimal | None = None
    mean_mae: Decimal | None = None
    median_mfe: Decimal | None = None
    median_mae: Decimal | None = None
    mfe_available_count: int = 0
    mae_available_count: int = 0
    label: str = DISCLAIMER
    warning: str = WARNING_NO_PNL


class EventStudyRunMeta(BaseModel):
    """Reproducible run identity and explicit data/config boundaries."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    config_id: str
    phase: str = "phase_3_event_study"
    study_kind: str = "descriptive_gross_market_return"
    venue: str
    start: datetime
    end: datetime
    max_events: int
    entry_delays: tuple[timedelta, ...]
    holding_periods: tuple[timedelta, ...]
    eligibility_rules: tuple[str, ...]
    seed: int | None = None
    git_identity: str | None = None
    event_count: int
    observation_count: int
    data_horizon_end: datetime | None = None
    warnings: tuple[str, ...] = (
        DISCLAIMER,
        WARNING_NO_PNL,
        "venues_never_pooled",
        "no_forward_fill_exit",
        "subminute_requires_point_resolution",
    )

    @field_validator("start", "end", "data_horizon_end")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class EventStudyReport(BaseModel):
    """Self-contained Phase 3 summary payload."""

    model_config = ConfigDict(frozen=True)

    meta: EventStudyRunMeta
    aggregates: tuple[CellAggregate, ...]
    cell_results: tuple[EventStudyCellResult, ...] = ()
    extras: dict[str, Any] = Field(default_factory=dict)
