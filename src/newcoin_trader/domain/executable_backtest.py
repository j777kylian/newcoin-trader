"""Phase 5 executable historical-backtest domain (research simulation only)."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from newcoin_trader.domain.enums import Side, Venue
from newcoin_trader.domain.event_study import ObservationResolution
from newcoin_trader.domain.feature_research import RuleCondition
from newcoin_trader.domain.types import require_utc

DISCLAIMER = "historical_executable_simulation_not_live_trading_advice"
WARNING_RESEARCH_ONLY = "research_only_no_orders_no_wallets_no_capital_deployment"
WARNING_MODELED = "fills_may_be_modeled_not_amm_exact_or_live_book"


class AvailabilityLevel(StrEnum):
    SUPPORTED = "supported"
    PARTIAL = "partial"
    UNSUPPORTED = "unsupported"


class ExecutableBacktestStatus(StrEnum):
    FULLY_FILLED = "fully_filled"
    PARTIAL = "partial"
    UNFILLED = "unfilled"
    UNSUPPORTED_RESOLUTION = "unsupported_resolution"
    NO_ENTRY = "no_entry"
    NO_EXIT = "no_exit"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"
    INVALID_MARKET_DATA = "invalid_market_data"
    NOT_DECISION_AVAILABLE = "not_decision_available"
    RULE_NOT_MATCHED = "rule_not_matched"


class ExecutionConfidence(StrEnum):
    EXACT_DEPTH = "exact_depth"
    EXACT_TRADE = "exact_trade"
    MODELED_PRICE = "modeled_price"
    MODELED_LIQUIDITY_IMPACT = "modeled_liquidity_impact"
    ASSUMED_FEE = "assumed_fee"
    UNSUPPORTED = "unsupported"


class SimulatedFillMode(StrEnum):
    EXACT_DEPTH = "exact_depth"
    EXACT_TRADE = "exact_trade"
    MODELED_PRICE = "modeled_price"
    MODELED_LIQUIDITY = "modeled_liquidity"


class DepthLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal
    quantity: Decimal


class HistoricalDepthBook(BaseModel):
    """Supplied historical L2 only — not loaded from DB (no depth table)."""

    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    venue: Venue
    timestamp: datetime
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class ExecutionMarketObservation(BaseModel):
    """Timestamped price/liquidity observation for modeled or exact-trade fills."""

    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    venue: Venue
    timestamp: datetime
    price: Decimal
    liquidity: Decimal | None = None
    volume: Decimal | None = None
    resolution: ObservationResolution
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class ExecutionTradeTick(BaseModel):
    """Persisted historical trade tick (Binance/CEX when available)."""

    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    venue: Venue
    timestamp: datetime
    side: str
    amount: Decimal
    price: Decimal
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class FrozenCandidateIdentity(BaseModel):
    """Frozen Phase 4 candidate-rule identity — never rediscovered in Phase 5."""

    model_config = ConfigDict(frozen=True)

    rule_id: str
    conditions: tuple[RuleCondition, ...]
    human_readable: str
    phase4_config_id: str
    split_label: str = "test"
    fold_index: int | None = None
    provenance: dict[str, str] = Field(default_factory=dict)


class SimulatedFill(BaseModel):
    model_config = ConfigDict(frozen=True)

    side: Side
    status: ExecutableBacktestStatus
    mode: SimulatedFillMode
    confidence: ExecutionConfidence
    request_time: datetime
    fill_time: datetime
    requested_qty: Decimal
    fill_qty: Decimal
    fill_price: Decimal
    notional: Decimal
    fee_cost: Decimal
    spread_cost: Decimal
    slippage_cost: Decimal
    impact_cost: Decimal
    assumed_fee_bps: Decimal
    label: str = WARNING_MODELED
    source: str | None = None

    @field_validator("request_time", "fill_time")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class ExecutableTradeResult(BaseModel):
    """One frozen-rule × scenario × event executable simulation outcome."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    venue: Venue
    token_address: str
    chain: str
    frozen_rule_id: str
    phase4_config_id: str
    split_label: str
    fold_index: int | None = None
    source_event_time: datetime
    first_seen_time: datetime
    decision_available_time: datetime
    configured_decision_time: datetime
    signal_time: datetime
    request_time: datetime
    fill_time: datetime
    exit_signal_time: datetime
    exit_request_time: datetime
    exit_fill_time: datetime
    status: ExecutableBacktestStatus
    side: Side
    position_notional: Decimal
    holding_period: timedelta
    entry_fill: SimulatedFill | None = None
    exit_fill: SimulatedFill | None = None
    phase4_gross_return: Decimal | None = None
    gross_return: Decimal | None = None
    net_return: Decimal | None = None
    total_fee_cost: Decimal | None = None
    total_spread_cost: Decimal | None = None
    total_slippage_cost: Decimal | None = None
    total_impact_cost: Decimal | None = None
    edge_retention: Decimal | None = None
    edge_retention_semantics: str | None = None
    confidence: ExecutionConfidence | None = None
    label: str = DISCLAIMER
    warning: str = WARNING_RESEARCH_ONLY

    @field_validator(
        "source_event_time",
        "first_seen_time",
        "decision_available_time",
        "configured_decision_time",
        "signal_time",
        "request_time",
        "fill_time",
        "exit_signal_time",
        "exit_request_time",
        "exit_fill_time",
    )
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class ExecutableBacktestRunMeta(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    config_id: str
    phase: str = "phase_5_executable_backtest"
    study_kind: str = "historical_executable_simulation"
    venue: str
    start: datetime
    end: datetime
    max_events: int
    max_trades: int
    max_execution_inputs: int
    latencies: tuple[timedelta, ...]
    holding_periods: tuple[timedelta, ...]
    position_notionals: tuple[Decimal, ...]
    max_participation: Decimal
    assumed_fee_bps: Decimal
    event_count: int
    trade_count: int
    frozen_rule_ids: tuple[str, ...]
    git_identity: str | None = None
    warnings: tuple[str, ...] = (
        DISCLAIMER,
        WARNING_RESEARCH_ONLY,
        WARNING_MODELED,
        "no_db_historical_depth_table",
        "dex_impact_modeled_not_amm_exact",
        "fees_assumed_when_historical_unavailable",
        "frozen_phase4_identity_no_rediscovery",
        "subminute_requires_point_resolution",
    )

    @field_validator("start", "end")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class ExecutableBacktestReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    meta: ExecutableBacktestRunMeta
    capabilities: dict[str, dict[str, str]]
    trades: tuple[ExecutableTradeResult, ...] = ()
    aggregates: dict[str, Any] = Field(default_factory=dict)
    frozen_identities: tuple[FrozenCandidateIdentity, ...] = ()
    extras: dict[str, Any] = Field(default_factory=dict)
